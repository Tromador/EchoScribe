//! Authoritative capture boundary and consumer.
//!
//! Songbird callbacks enqueue bounded records without waiting. One consumer
//! owns journal ordering, identity routing, durability checkpoints, and
//! non-blocking fan-out into lower-priority live FLAC and diagnostic stages.

use std::{
    collections::{HashMap, HashSet},
    fs::{self, File, OpenOptions},
    io::{self, BufWriter, Write},
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use tokio::{
    sync::{mpsc, oneshot},
    task::JoinHandle,
    time::{self, MissedTickBehavior},
};

use crate::{
    artifacts::{EVENT_JOURNAL_FILE_NAME, PACKET_JOURNAL_FILE_NAME, PLAYOUT_JOURNAL_FILE_NAME},
    diagnostics::{DecodedFrame, OptionalDiagnosticWriter, TrackSummary},
    identity::{IdentityRouter, RoutingAction, UnresolvedSsrcAbandonment, UserIdentity},
    journal::{self, PacketRecord},
    live_flac::{LiveFlacStage, StageReport as LiveFlacReport, TrackAbandonment},
    participants::ParticipantContext,
    playout::{self, OpusPayloadBounds, PlayoutDecision, PlayoutRecord},
    session::{self, NewSession, SessionEvent},
};

const QUEUE_CAPACITY: usize = 4096;
const WRITER_BUFFER_CAPACITY: usize = 256 * 1024;
const CHECKPOINT_INTERVAL: Duration = Duration::from_secs(5);
const DURABILITY_SYNC_INTERVAL: Duration = Duration::from_secs(30);

/// Decrypted RTP evidence copied out of a Songbird callback.
///
/// Payload bounds locate the Opus frame within `packet`; the complete packet is
/// retained because it is part of the recoverable recording authority.
pub(crate) struct CapturedPacket {
    pub(crate) ssrc: u32,
    pub(crate) sequence: u16,
    pub(crate) timestamp: u32,
    pub(crate) payload_start: u32,
    pub(crate) payload_end: u32,
    pub(crate) packet: Vec<u8>,
}

#[derive(Clone)]
/// Non-blocking callback-side handle for the authoritative capture queue.
pub(crate) struct CaptureSender {
    sender: mpsc::Sender<CaptureRecord>,
    metrics: Arc<ProducerMetrics>,
    session_start: Instant,
}

/// Owns capture shutdown and the consumer task for one recording session.
///
/// Dropping a sender is not the normal stop protocol: `stop` closes the queue,
/// drains accepted records, and finalises the downstream stages deterministically.
pub(crate) struct CaptureDrain {
    stop: oneshot::Sender<()>,
    task: JoinHandle<io::Result<ConsumerSummary>>,
    metrics: Arc<ProducerMetrics>,
    session_directory: PathBuf,
}

#[derive(Default)]
/// Callback-side counters are atomic because several Songbird event handlers
/// may submit records concurrently.
struct ProducerMetrics {
    accepted: AtomicU64,
    full_drops: AtomicU64,
    closed_drops: AtomicU64,
    event_drops: AtomicU64,
    playout_drops: AtomicU64,
    audio_drops: AtomicU64,
    high_water: AtomicUsize,
}

#[derive(Default)]
/// Consumer-owned accounting used for the shutdown integrity report.
struct ConsumerSummary {
    records: u64,
    packet_records: u64,
    event_records: u64,
    generated_event_records: u64,
    routing_ticks: u64,
    playout_records: u64,
    audio_frames: u64,
    audio_samples: u64,
    packet_bytes: u64,
    stream_tails: HashMap<u32, (u16, u32)>,
    diagnostic_tracks: Vec<TrackSummary>,
    live_flac: Option<LiveFlacReport>,
    resolved_frames: u64,
    resolved_samples: u64,
    identity_updates: u64,
    unresolved_abandonments: u64,
    abandoned_users: HashSet<u64>,
    missing_participant_warnings: u64,
    diagnostic_failures: u64,
    reported_flac_failure_users: HashSet<u64>,
    checkpoints: u64,
    durability_syncs: u64,
}

enum CaptureRecord {
    // Authoritative and derived inputs deliberately share this ingress queue at
    // the Songbird boundary. Derived FLAC work is separated after consumption.
    Packet(PacketRecord),
    Event(SessionEvent),
    Playout(PlayoutRecord),
    Audio(DecodedFrame),
    RoutingTick { tick: u64, elapsed_nanos: u64 },
}

struct SessionDirectoryGuard {
    path: Option<PathBuf>,
}

struct DurableFailure {
    elapsed_nanos: u64,
    kind: &'static str,
    message: String,
}

pub(crate) fn start(
    output_directory: &Path,
    guild_id: &str,
    channel_id: &str,
    configuration_version: u32,
    participants: &ParticipantContext,
    diagnostic_wav: bool,
) -> io::Result<(CaptureSender, CaptureDrain)> {
    let (session_directory, started_at_unix_millis) = create_session_directory(output_directory)?;
    start_in_session_directory(
        SessionDirectoryGuard::new(session_directory),
        started_at_unix_millis,
        guild_id,
        channel_id,
        configuration_version,
        participants,
        diagnostic_wav,
    )
}

fn start_in_session_directory(
    session_directory: SessionDirectoryGuard,
    started_at_unix_millis: u64,
    guild_id: &str,
    channel_id: &str,
    configuration_version: u32,
    participants: &ParticipantContext,
    diagnostic_wav: bool,
) -> io::Result<(CaptureSender, CaptureDrain)> {
    // Session publication is the startup commit point. Everything which can
    // fail synchronously is initialised first; the armed guard removes the
    // directory if publication never succeeds.
    let session_id = session_directory
        .path()
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| io::Error::other("capture session directory has no valid UTF-8 name"))?
        .to_owned();

    let packets_path = session_directory.path().join(PACKET_JOURNAL_FILE_NAME);
    let packets_file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&packets_path)?;
    let mut packet_writer = BufWriter::with_capacity(WRITER_BUFFER_CAPACITY, packets_file);
    journal::write_file_header(&mut packet_writer)?;
    let events_file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(session_directory.path().join(EVENT_JOURNAL_FILE_NAME))?;
    let event_writer = BufWriter::new(events_file);
    let playout_file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(session_directory.path().join(PLAYOUT_JOURNAL_FILE_NAME))?;
    let mut playout_writer = BufWriter::with_capacity(WRITER_BUFFER_CAPACITY, playout_file);
    playout::write_file_header(&mut playout_writer)?;
    let diagnostic_writer =
        OptionalDiagnosticWriter::new(session_directory.path(), diagnostic_wav)?;
    fs::create_dir(session_directory.path().join("tracks"))?;

    packet_writer.flush()?;
    packet_writer.get_ref().sync_data()?;
    event_writer.get_ref().sync_data()?;
    playout_writer.flush()?;
    playout_writer.get_ref().sync_data()?;

    let session_store = session::SessionStore::create(
        session_directory.path(),
        NewSession {
            session_id: &session_id,
            started_at_unix_millis,
            configuration_version,
            guild_id,
            channel_id,
            participants,
        },
    )?;
    let identity_router = IdentityRouter::new(participants.discord_user_ids());
    let live_flac = LiveFlacStage::start(session_directory.path());

    let (sender, receiver) = mpsc::channel(QUEUE_CAPACITY);
    let (stop, stop_receiver) = oneshot::channel();
    let metrics = Arc::new(ProducerMetrics::default());
    let task = tokio::spawn(consume(
        receiver,
        stop_receiver,
        packet_writer,
        event_writer,
        playout_writer,
        diagnostic_writer,
        identity_router,
        live_flac,
        session_store,
        started_at_unix_millis,
    ));
    let session_directory = session_directory.disarm();

    Ok((
        CaptureSender {
            sender,
            metrics: Arc::clone(&metrics),
            session_start: Instant::now(),
        },
        CaptureDrain {
            stop,
            task,
            metrics,
            session_directory,
        },
    ))
}

impl SessionDirectoryGuard {
    fn new(path: PathBuf) -> Self {
        Self { path: Some(path) }
    }

    fn path(&self) -> &Path {
        self.path
            .as_deref()
            .expect("session directory guard is always armed while borrowed")
    }

    fn disarm(mut self) -> PathBuf {
        self.path
            .take()
            .expect("session directory guard can only be disarmed once")
    }
}

impl Drop for SessionDirectoryGuard {
    fn drop(&mut self) {
        let Some(path) = self.path.take() else {
            return;
        };
        if let Err(error) = fs::remove_dir_all(&path) {
            eprintln!(
                "failed to remove incomplete capture session directory {}: {error}",
                path.display()
            );
        }
    }
}

impl CaptureSender {
    /// Submit decrypted packet evidence without waiting for queue capacity.
    pub(crate) fn try_send(&self, packet: CapturedPacket) {
        let arrival_nanos_since_session_start =
            u64::try_from(self.session_start.elapsed().as_nanos()).unwrap_or(u64::MAX);
        self.try_send_record(CaptureRecord::Packet(PacketRecord {
            arrival_nanos_since_session_start,
            ssrc: packet.ssrc,
            sequence: packet.sequence,
            timestamp: packet.timestamp,
            payload_start: packet.payload_start,
            payload_end: packet.payload_end,
            packet: packet.packet,
        }));
    }

    /// Record Songbird's transport-to-user mapping evidence.
    pub(crate) fn try_send_speaker_mapping(
        &self,
        ssrc: u32,
        user_id: Option<String>,
        speaking_bits: u8,
    ) {
        let elapsed_nanos =
            u64::try_from(self.session_start.elapsed().as_nanos()).unwrap_or(u64::MAX);
        self.try_send_record(CaptureRecord::Event(SessionEvent::speaker_mapping(
            elapsed_nanos,
            ssrc,
            user_id,
            speaking_bits,
        )));
    }

    /// Record gateway identity evidence independently of SSRC mapping timing.
    pub(crate) fn try_send_user_identity(
        &self,
        discord_user_id: u64,
        server_display_name: Option<String>,
        global_display_name: Option<String>,
        username: String,
    ) {
        let elapsed_nanos =
            u64::try_from(self.session_start.elapsed().as_nanos()).unwrap_or(u64::MAX);
        self.try_send_record(CaptureRecord::Event(SessionEvent::user_identity(
            elapsed_nanos,
            discord_user_id.to_string(),
            server_display_name,
            global_display_name,
            username,
        )));
    }

    /// Revoke every live SSRC mapping for a user who left the voice channel.
    pub(crate) fn try_send_user_disconnected(&self, discord_user_id: u64) {
        let elapsed_nanos =
            u64::try_from(self.session_start.elapsed().as_nanos()).unwrap_or(u64::MAX);
        self.try_send_record(CaptureRecord::Event(SessionEvent::user_disconnected(
            elapsed_nanos,
            discord_user_id.to_string(),
        )));
    }

    /// Advance pending-mapping expiry even when an unresolved SSRC is silent.
    pub(crate) fn try_advance_routing_tick(&self, tick: u64) {
        let elapsed_nanos =
            u64::try_from(self.session_start.elapsed().as_nanos()).unwrap_or(u64::MAX);
        self.try_send_record(CaptureRecord::RoutingTick {
            tick,
            elapsed_nanos,
        });
    }

    /// Submit decoded mono PCM still keyed by its transport SSRC.
    pub(crate) fn try_send_audio(&self, tick: u64, ssrc: u32, samples: Vec<i16>) {
        let elapsed_nanos =
            u64::try_from(self.session_start.elapsed().as_nanos()).unwrap_or(u64::MAX);
        self.try_send_record(CaptureRecord::Audio(DecodedFrame {
            elapsed_nanos,
            tick,
            ssrc,
            samples,
        }));
    }

    pub(crate) fn try_send_playout(
        &self,
        tick: u64,
        ssrc: u32,
        packet: Option<(u16, u32, Option<OpusPayloadBounds>)>,
        decoded_samples: u32,
    ) {
        let decision = match packet {
            Some((sequence, timestamp, opus_payload)) => PlayoutDecision::Packet {
                sequence,
                timestamp,
                opus_payload,
            },
            None => PlayoutDecision::Loss,
        };
        self.try_send_record(CaptureRecord::Playout(PlayoutRecord {
            tick,
            ssrc,
            decision,
            decoded_samples,
        }));
    }

    fn try_send_record(&self, record: CaptureRecord) {
        // Classify before moving the record so pressure telemetry can distinguish
        // authoritative event/playout loss from derived decoded-audio loss.
        let is_event = matches!(&record, CaptureRecord::Event(_));
        let is_playout = matches!(&record, CaptureRecord::Playout(_));
        let is_audio = matches!(&record, CaptureRecord::Audio(_));
        let depth_after_send = self.sender.max_capacity() - self.sender.capacity() + 1;

        match self.sender.try_send(record) {
            Ok(()) => {
                self.metrics.accepted.fetch_add(1, Ordering::Relaxed);
                self.metrics
                    .high_water
                    .fetch_max(depth_after_send, Ordering::Relaxed);
            }
            Err(mpsc::error::TrySendError::Full(_)) => {
                self.metrics.full_drops.fetch_add(1, Ordering::Relaxed);
                if is_event {
                    self.metrics.event_drops.fetch_add(1, Ordering::Relaxed);
                }
                if is_playout {
                    self.metrics.playout_drops.fetch_add(1, Ordering::Relaxed);
                }
                if is_audio {
                    self.metrics.audio_drops.fetch_add(1, Ordering::Relaxed);
                }
                self.metrics
                    .high_water
                    .fetch_max(self.sender.max_capacity(), Ordering::Relaxed);
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                self.metrics.closed_drops.fetch_add(1, Ordering::Relaxed);
                if is_event {
                    self.metrics.event_drops.fetch_add(1, Ordering::Relaxed);
                }
                if is_playout {
                    self.metrics.playout_drops.fetch_add(1, Ordering::Relaxed);
                }
                if is_audio {
                    self.metrics.audio_drops.fetch_add(1, Ordering::Relaxed);
                }
            }
        }
    }
}

impl CaptureDrain {
    pub(crate) fn session_directory(&self) -> &Path {
        &self.session_directory
    }

    /// Stop capture and report failures rather than disguising them as a clean
    /// shutdown. Slice 5 will promote this result into workflow finalisation.
    pub(crate) async fn stop(self) {
        let Self {
            stop,
            task,
            metrics,
            session_directory,
        } = self;
        let _ = stop.send(());

        match task.await {
            Ok(Ok(summary)) => Self::report(&metrics, &summary, &session_directory),
            Ok(Err(error)) => eprintln!("capture writer failed: {error}"),
            Err(error) => eprintln!("capture consumer task failed: {error}"),
        }
    }

    fn report(metrics: &ProducerMetrics, summary: &ConsumerSummary, session_directory: &Path) {
        println!(
            "Capture queue: {} records accepted, {} consumed ({} packets, {} events, \
             {} playout decisions, {} audio frames, {} routing ticks), {} full drops, {} closed drops, \
             {} event drops, {} playout drops, {} audio drops, high-water {}/{}, \
             {} packet bytes and {} audio samples consumed.",
            metrics.accepted.load(Ordering::Relaxed),
            summary.records,
            summary.packet_records,
            summary.event_records,
            summary.playout_records,
            summary.audio_frames,
            summary.routing_ticks,
            metrics.full_drops.load(Ordering::Relaxed),
            metrics.closed_drops.load(Ordering::Relaxed),
            metrics.event_drops.load(Ordering::Relaxed),
            metrics.playout_drops.load(Ordering::Relaxed),
            metrics.audio_drops.load(Ordering::Relaxed),
            metrics.high_water.load(Ordering::Relaxed),
            QUEUE_CAPACITY,
            summary.packet_bytes,
            summary.audio_samples,
        );
        println!(
            "Capture-generated events: {}.",
            summary.generated_event_records
        );
        println!("Session files written to {}.", session_directory.display());
        println!(
            "Capture durability: {} structural checkpoints, {} storage syncs.",
            summary.checkpoints, summary.durability_syncs
        );
        println!(
            "Identity routing: {} identity updates, {} resolved frames ({} samples), \
             {} unresolved SSRC abandonments, {} abandoned users, {} missing-participant warnings.",
            summary.identity_updates,
            summary.resolved_frames,
            summary.resolved_samples,
            summary.unresolved_abandonments,
            summary.abandoned_users.len(),
            summary.missing_participant_warnings,
        );
        if summary.diagnostic_failures > 0 {
            eprintln!(
                "Diagnostic WAV was abandoned after {} isolated failures.",
                summary.diagnostic_failures
            );
        }
        if let Some(flac) = &summary.live_flac {
            println!(
                "Live FLAC queue: {} frames accepted, {} enqueue failures, \
                 high-water {}/{}, {} warning crossings, {} abandoned users.",
                flac.accepted,
                flac.enqueue_failures,
                flac.high_water,
                crate::live_flac::QUEUE_CAPACITY,
                flac.warning_crossings,
                summary.abandoned_users.len(),
            );
            for track in &flac.summary.tracks {
                println!(
                    "Live FLAC user {}: {}, first tick {}, {} frames, {} source samples, \
                     {} inserted silence samples, {} nonstandard frames, source SSRCs {:?}.",
                    track.discord_user_id,
                    track.path.display(),
                    track.first_tick,
                    track.frames,
                    track.source_samples,
                    track.inserted_silence_samples,
                    track.nonstandard_frames,
                    track.source_ssrcs,
                );
            }
            for failure in &flac.summary.failures {
                eprintln!(
                    "Live FLAC user {} abandoned at tick {} ({}): {}",
                    failure.discord_user_id, failure.tick, failure.reason, failure.message
                );
            }
        }

        let mut streams = summary.stream_tails.iter().collect::<Vec<_>>();
        streams.sort_unstable_by_key(|(ssrc, _)| **ssrc);

        for (ssrc, (sequence, timestamp)) in streams {
            println!(
                "Capture consumer SSRC {ssrc}: last sequence {sequence}, \
                 last RTP timestamp {timestamp}."
            );
        }

        for track in &summary.diagnostic_tracks {
            println!(
                "Diagnostic WAV SSRC {}: {}, first tick {}, {} frames, {} source samples, \
                 {} inserted silence samples, {} nonstandard frames.",
                track.ssrc,
                track.path.display(),
                track.first_tick,
                track.frames,
                track.source_samples,
                track.inserted_silence_samples,
                track.nonstandard_frames,
            );
        }
    }
}

async fn consume(
    mut receiver: mpsc::Receiver<CaptureRecord>,
    mut stop: oneshot::Receiver<()>,
    mut packet_writer: BufWriter<File>,
    mut event_writer: BufWriter<File>,
    mut playout_writer: BufWriter<File>,
    mut diagnostic_writer: OptionalDiagnosticWriter,
    mut identity_router: IdentityRouter,
    mut live_flac: LiveFlacStage,
    session_store: session::SessionStore,
    started_at_unix_millis: u64,
) -> io::Result<ConsumerSummary> {
    // The session-state writer has its own task because an atomic JSON
    // replacement must not delay journal consumption after a FLAC fault.
    let mut summary = ConsumerSummary::default();
    let (failure_sender, failure_receiver) = mpsc::unbounded_channel();
    let failure_task = tokio::spawn(record_session_failures(
        failure_receiver,
        session_store,
        started_at_unix_millis,
    ));
    let mut checkpoint_interval = time::interval_at(
        time::Instant::now() + CHECKPOINT_INTERVAL,
        CHECKPOINT_INTERVAL,
    );
    checkpoint_interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut durability_sync_interval = time::interval_at(
        time::Instant::now() + DURABILITY_SYNC_INTERVAL,
        DURABILITY_SYNC_INTERVAL,
    );
    durability_sync_interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            // Shutdown wins once requested, then closes and drains the accepted
            // record set before derived stages are stopped.
            biased;

            _ = &mut stop => {
                receiver.close();

                while let Some(record) = receiver.recv().await {
                    write_and_observe(
                        &mut packet_writer,
                        &mut event_writer,
                        &mut playout_writer,
                        &mut diagnostic_writer,
                        &mut identity_router,
                        &mut live_flac,
                        &failure_sender,
                        &mut summary,
                        record,
                    )?;
                }

                finish_identity_routing(
                    &mut event_writer,
                    &mut identity_router,
                    &mut live_flac,
                    &failure_sender,
                    &mut summary,
                )?;
                let live_report = live_flac.stop().await;
                record_live_flac_failures(&failure_sender, &summary, &live_report);
                drop(failure_sender);
                await_failure_recorder(failure_task).await?;
                return finish(
                    packet_writer,
                    event_writer,
                    playout_writer,
                    diagnostic_writer,
                    live_report,
                    summary,
                );
            }
            _ = checkpoint_interval.tick() => {
                checkpoint(
                    &mut packet_writer,
                    &mut event_writer,
                    &mut playout_writer,
                    &mut diagnostic_writer,
                )?;
                summary.checkpoints += 1;
            }
            _ = durability_sync_interval.tick() => {
                sync_data(
                    &packet_writer,
                    &event_writer,
                    &playout_writer,
                    &mut diagnostic_writer,
                )?;
                summary.durability_syncs += 1;
            }
            record = receiver.recv() => {
                match record {
                    Some(record) => write_and_observe(
                        &mut packet_writer,
                        &mut event_writer,
                        &mut playout_writer,
                        &mut diagnostic_writer,
                        &mut identity_router,
                        &mut live_flac,
                        &failure_sender,
                        &mut summary,
                        record,
                    )?,
                    None => {
                        finish_identity_routing(
                            &mut event_writer,
                            &mut identity_router,
                            &mut live_flac,
                            &failure_sender,
                            &mut summary,
                        )?;
                        let live_report = live_flac.stop().await;
                        record_live_flac_failures(&failure_sender, &summary, &live_report);
                        drop(failure_sender);
                        await_failure_recorder(failure_task).await?;
                        return finish(
                            packet_writer,
                            event_writer,
                            playout_writer,
                            diagnostic_writer,
                            live_report,
                            summary,
                        );
                    }
                }
            }
        }
    }
}

fn write_and_observe(
    packet_writer: &mut BufWriter<File>,
    event_writer: &mut BufWriter<File>,
    playout_writer: &mut BufWriter<File>,
    diagnostic_writer: &mut OptionalDiagnosticWriter,
    identity_router: &mut IdentityRouter,
    live_flac: &mut LiveFlacStage,
    failure_sender: &mpsc::UnboundedSender<DurableFailure>,
    summary: &mut ConsumerSummary,
    record: CaptureRecord,
) -> io::Result<()> {
    // This function is the ordering point: each record reaches its durable
    // journal before any resulting identity or derived-audio action is applied.
    summary.records += 1;

    match record {
        CaptureRecord::Packet(record) => {
            journal::write_record(packet_writer, &record)?;
            summary.observe_packet(record);
        }
        CaptureRecord::Event(event) => {
            session::write_event(event_writer, &event)?;
            summary.event_records += 1;
            let actions = match event {
                SessionEvent::SpeakerMapping { ssrc, user_id, .. } => {
                    let user_id = user_id
                        .map(|user_id| {
                            user_id.parse::<u64>().map_err(|_| {
                                io::Error::new(
                                    io::ErrorKind::InvalidData,
                                    format!(
                                        "speaker mapping contains invalid Discord user ID \
                                         {user_id:?}"
                                    ),
                                )
                            })
                        })
                        .transpose()?;
                    identity_router.observe_mapping(ssrc, user_id)
                }
                SessionEvent::UserIdentity {
                    user_id,
                    server_display_name,
                    global_display_name,
                    username,
                    ..
                } => {
                    let discord_user_id = user_id.parse::<u64>().map_err(|_| {
                        io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!("identity event contains invalid Discord user ID {user_id:?}"),
                        )
                    })?;
                    identity_router.observe_identity(UserIdentity {
                        discord_user_id,
                        server_display_name,
                        global_display_name,
                        username,
                    })
                }
                SessionEvent::UserDisconnected { user_id, .. } => {
                    let discord_user_id = user_id.parse::<u64>().map_err(|_| {
                        io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!(
                                "disconnect event contains invalid Discord user ID {user_id:?}"
                            ),
                        )
                    })?;
                    identity_router.observe_disconnect(discord_user_id);
                    Vec::new()
                }
                SessionEvent::UnresolvedSsrcAbandoned { .. } => Vec::new(),
            };
            apply_routing_actions(event_writer, live_flac, failure_sender, summary, actions)?;
        }
        CaptureRecord::Playout(record) => {
            playout::write_record(playout_writer, &record)?;
            summary.playout_records += 1;
        }
        CaptureRecord::Audio(frame) => {
            summary.audio_samples += frame.samples.len() as u64;
            if let Err(error) = diagnostic_writer.write_frame(&frame) {
                summary.diagnostic_failures += 1;
                eprintln!("Diagnostic WAV abandoned; authoritative capture continues: {error}");
            }
            summary.audio_frames += 1;
            let actions = identity_router.route_frame(frame);
            apply_routing_actions(event_writer, live_flac, failure_sender, summary, actions)?;
        }
        CaptureRecord::RoutingTick {
            tick,
            elapsed_nanos,
        } => {
            summary.routing_ticks += 1;
            let actions = identity_router.advance_tick(tick, elapsed_nanos);
            apply_routing_actions(event_writer, live_flac, failure_sender, summary, actions)?;
        }
    }

    Ok(())
}

fn finish_identity_routing(
    event_writer: &mut BufWriter<File>,
    identity_router: &mut IdentityRouter,
    live_flac: &mut LiveFlacStage,
    failure_sender: &mpsc::UnboundedSender<DurableFailure>,
    summary: &mut ConsumerSummary,
) -> io::Result<()> {
    apply_routing_actions(
        event_writer,
        live_flac,
        failure_sender,
        summary,
        identity_router.finish(),
    )
}

fn apply_routing_actions(
    event_writer: &mut BufWriter<File>,
    live_flac: &mut LiveFlacStage,
    failure_sender: &mpsc::UnboundedSender<DurableFailure>,
    summary: &mut ConsumerSummary,
    actions: Vec<RoutingAction>,
) -> io::Result<()> {
    // Encoder feedback is observed before routing more PCM, preventing a failed
    // user writer from being recreated or fed indefinitely.
    for failure in live_flac.take_encoder_failures() {
        summary.abandoned_users.insert(failure.discord_user_id);
        summary
            .reported_flac_failure_users
            .insert(failure.discord_user_id);
        let _ = failure_sender.send(DurableFailure {
            elapsed_nanos: failure.elapsed_nanos,
            kind: "live_flac_encoder",
            message: format!(
                "live FLAC track for Discord user {} abandoned at tick {}: reason {}: {}",
                failure.discord_user_id, failure.tick, failure.reason, failure.message
            ),
        });
    }
    for action in actions {
        match action {
            RoutingAction::Frame(frame) => {
                summary.resolved_frames += 1;
                summary.resolved_samples += frame.samples.len() as u64;
                if let Some(abandonment) = live_flac.try_send(frame) {
                    record_queue_abandonment(failure_sender, summary, &abandonment);
                }
            }
            RoutingAction::IdentityUpdated(_) => {
                summary.identity_updates += 1;
            }
            RoutingAction::UnresolvedSsrcAbandoned(abandonment) => {
                write_abandonment_event(event_writer, &abandonment)?;
                summary.generated_event_records += 1;
                summary.unresolved_abandonments += 1;
                eprintln!(
                    "Unresolved SSRC {} abandoned at tick {}: {} frames and {} samples \
                     discarded ({})",
                    abandonment.ssrc,
                    abandonment.last_tick,
                    abandonment.discarded_frames,
                    abandonment.discarded_samples,
                    abandonment.reason.as_str(),
                );
            }
            RoutingAction::UserTrackAbandoned(abandonment) => {
                summary.abandoned_users.insert(abandonment.discord_user_id);
                live_flac.abandon_user(
                    abandonment.discord_user_id,
                    abandonment.source.last_tick,
                    abandonment.source.elapsed_nanos,
                    "unresolved_ssrc",
                );
                eprintln!(
                    "Discord user {} has incomplete live continuity because SSRC {} \
                     was abandoned before mapping.",
                    abandonment.discord_user_id, abandonment.source.ssrc
                );
            }
            RoutingAction::MissingParticipantContext { discord_user_id } => {
                summary.missing_participant_warnings += 1;
                eprintln!(
                    "Discord user {discord_user_id} is missing from participant context; \
                     recording continues with role player and no character."
                );
            }
        }
    }
    Ok(())
}

fn write_abandonment_event(
    event_writer: &mut BufWriter<File>,
    abandonment: &UnresolvedSsrcAbandonment,
) -> io::Result<()> {
    session::write_event(
        event_writer,
        &SessionEvent::unresolved_ssrc_abandoned(
            abandonment.elapsed_nanos,
            abandonment.ssrc,
            abandonment.first_tick,
            abandonment.last_tick,
            abandonment.discarded_frames,
            abandonment.discarded_samples,
            abandonment.reason.as_str().to_owned(),
        ),
    )
}

fn record_queue_abandonment(
    failure_sender: &mpsc::UnboundedSender<DurableFailure>,
    summary: &mut ConsumerSummary,
    abandonment: &TrackAbandonment,
) {
    // Mark locally before queueing durable metadata so no later frame can race
    // into a replacement writer for the same logical user.
    summary.abandoned_users.insert(abandonment.discord_user_id);
    let message = format!(
        "live FLAC track for Discord user {} abandoned at tick {}: reason {}, \
         queue depth {}/{}, high-water {}",
        abandonment.discord_user_id,
        abandonment.tick,
        abandonment.reason,
        abandonment.queue_depth,
        abandonment.queue_capacity,
        abandonment.queue_high_water,
    );
    let _ = failure_sender.send(DurableFailure {
        elapsed_nanos: abandonment.elapsed_nanos,
        kind: if abandonment.reason == "queue_full" {
            "live_flac_queue_full"
        } else {
            "live_flac_queue_closed"
        },
        message: message.clone(),
    });
    eprintln!("{message}");
}

fn record_live_flac_failures(
    failure_sender: &mpsc::UnboundedSender<DurableFailure>,
    summary: &ConsumerSummary,
    report: &LiveFlacReport,
) {
    for failure in report.summary.failures.iter().filter(|failure| {
        !summary
            .reported_flac_failure_users
            .contains(&failure.discord_user_id)
    }) {
        let _ = failure_sender.send(DurableFailure {
            elapsed_nanos: failure.elapsed_nanos,
            kind: "live_flac_encoder",
            message: format!(
                "live FLAC track for Discord user {} abandoned at tick {}: reason {}: {}",
                failure.discord_user_id, failure.tick, failure.reason, failure.message
            ),
        });
    }
}

async fn record_session_failures(
    mut receiver: mpsc::UnboundedReceiver<DurableFailure>,
    mut session_store: session::SessionStore,
    started_at_unix_millis: u64,
) -> io::Result<()> {
    // Durable metadata writes are kept off the authoritative consumer's hot
    // path. Input is semantically bounded to one terminal failure per track.
    while let Some(failure) = receiver.recv().await {
        let elapsed_millis = failure.elapsed_nanos / 1_000_000;
        let recorded_at = started_at_unix_millis.saturating_add(elapsed_millis);
        session_store.record_failure(recorded_at, failure.kind, failure.message)?;
    }
    Ok(())
}

async fn await_failure_recorder(task: JoinHandle<io::Result<()>>) -> io::Result<()> {
    task.await
        .map_err(|error| io::Error::other(format!("session failure recorder failed: {error}")))?
}

fn checkpoint(
    packet_writer: &mut BufWriter<File>,
    event_writer: &mut BufWriter<File>,
    playout_writer: &mut BufWriter<File>,
    diagnostic_writer: &mut OptionalDiagnosticWriter,
) -> io::Result<()> {
    // Checkpoints flush language/runtime buffers; the less frequent sync pass
    // separately asks storage to make those bytes durable.
    packet_writer.flush()?;
    event_writer.flush()?;
    playout_writer.flush()?;
    if let Err(error) = diagnostic_writer.checkpoint() {
        eprintln!("Diagnostic WAV checkpoint failed and diagnostics were abandoned: {error}");
    }
    Ok(())
}

fn sync_data(
    packet_writer: &BufWriter<File>,
    event_writer: &BufWriter<File>,
    playout_writer: &BufWriter<File>,
    diagnostic_writer: &mut OptionalDiagnosticWriter,
) -> io::Result<()> {
    packet_writer.get_ref().sync_data()?;
    event_writer.get_ref().sync_data()?;
    playout_writer.get_ref().sync_data()?;
    if let Err(error) = diagnostic_writer.sync_data() {
        eprintln!("Diagnostic WAV sync failed and diagnostics were abandoned: {error}");
    }
    Ok(())
}

fn finish(
    mut packet_writer: BufWriter<File>,
    mut event_writer: BufWriter<File>,
    mut playout_writer: BufWriter<File>,
    diagnostic_writer: OptionalDiagnosticWriter,
    live_report: LiveFlacReport,
    mut summary: ConsumerSummary,
) -> io::Result<ConsumerSummary> {
    flush_and_sync(&mut packet_writer)?;
    flush_and_sync(&mut event_writer)?;
    flush_and_sync(&mut playout_writer)?;
    match diagnostic_writer.finalize() {
        Ok(tracks) => summary.diagnostic_tracks = tracks,
        Err(error) => {
            summary.diagnostic_failures += 1;
            eprintln!(
                "Diagnostic WAV finalisation failed; authoritative capture remains valid: {error}"
            );
        }
    }
    summary.abandoned_users.extend(
        live_report
            .abandoned_users
            .iter()
            .chain(
                live_report
                    .summary
                    .failures
                    .iter()
                    .filter(|failure| failure.discord_user_id != 0)
                    .map(|failure| &failure.discord_user_id),
            )
            .copied(),
    );
    summary.live_flac = Some(live_report);
    Ok(summary)
}

fn flush_and_sync(writer: &mut BufWriter<File>) -> io::Result<()> {
    writer.flush()?;
    writer.get_ref().sync_data()
}

impl ConsumerSummary {
    fn observe_packet(&mut self, record: PacketRecord) {
        self.packet_records += 1;
        self.packet_bytes += record.packet.len() as u64;
        self.stream_tails
            .insert(record.ssrc, (record.sequence, record.timestamp));
    }
}

fn create_session_directory(output_directory: &Path) -> io::Result<(PathBuf, u64)> {
    // Millisecond IDs are readable but not assumed unique. Bounded suffixing
    // handles simultaneous/repeated allocation without overwriting a session.
    fs::create_dir_all(output_directory)?;

    let unix_millis = u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|error| {
                io::Error::other(format!("system clock precedes Unix epoch: {error}"))
            })?
            .as_millis(),
    )
    .map_err(|_| io::Error::other("current Unix timestamp does not fit in u64"))?;

    for suffix in 0..1000 {
        let name = if suffix == 0 {
            format!("session-{unix_millis}")
        } else {
            format!("session-{unix_millis}-{suffix}")
        };
        let session_directory = output_directory.join(name);

        match fs::create_dir(&session_directory) {
            Ok(()) => return Ok((session_directory, unix_millis)),
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
            Err(error) => return Err(error),
        }
    }

    Err(io::Error::new(
        io::ErrorKind::AlreadyExists,
        "could not allocate a unique capture session directory",
    ))
}

#[cfg(test)]
mod tests {
    use std::{env, process};

    use crate::{
        journal::{ReadRecord, read_file_header, read_record},
        playout::{
            PlayoutDecision, ReadRecord as ReadPlayoutRecord,
            read_file_header as read_playout_file_header, read_record as read_playout_record,
        },
    };

    use super::*;

    fn packet(sequence: u16) -> CapturedPacket {
        CapturedPacket {
            ssrc: 123,
            sequence,
            timestamp: u32::from(sequence) * 960,
            payload_start: 12,
            payload_end: 16,
            packet: vec![
                0x80, 0x78, 0, 0, 0, 0, 0, 0, 0, 0, 0, 123, 0x01, 0x02, 0x03, 0x04,
            ],
        }
    }

    #[test]
    fn full_queue_is_counted_without_blocking() {
        let (sender, _receiver) = mpsc::channel(1);
        let metrics = Arc::new(ProducerMetrics::default());
        let sender = CaptureSender {
            sender,
            metrics: Arc::clone(&metrics),
            session_start: Instant::now(),
        };

        sender.try_send(packet(1));
        sender.try_send(packet(2));

        assert_eq!(metrics.accepted.load(Ordering::Relaxed), 1);
        assert_eq!(metrics.full_drops.load(Ordering::Relaxed), 1);
        assert_eq!(metrics.high_water.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn closed_queue_is_counted_without_panicking() {
        let (sender, receiver) = mpsc::channel(1);
        let metrics = Arc::new(ProducerMetrics::default());
        let sender = CaptureSender {
            sender,
            metrics: Arc::clone(&metrics),
            session_start: Instant::now(),
        };
        drop(receiver);

        sender.try_send(packet(1));

        assert_eq!(metrics.accepted.load(Ordering::Relaxed), 0);
        assert_eq!(metrics.closed_drops.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn journal_initialisation_failure_removes_unpublished_session() {
        let output_directory = env::temp_dir().join(format!(
            "echoscribe-capture-start-failure-{}-{}",
            process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let session_directory = output_directory.join("session-1000");
        fs::create_dir_all(&session_directory).unwrap();
        fs::write(
            session_directory.join(PLAYOUT_JOURNAL_FILE_NAME),
            b"collision",
        )
        .unwrap();
        let participants = ParticipantContext::empty_for_test();

        let error = start_in_session_directory(
            SessionDirectoryGuard::new(session_directory.clone()),
            1000,
            "123",
            "456",
            1,
            &participants,
            false,
        )
        .err()
        .expect("journal initialisation should fail");

        assert_eq!(error.kind(), io::ErrorKind::AlreadyExists);
        assert!(!session_directory.exists());
        assert!(!output_directory.join("session-1000/session.json").exists());
        fs::remove_dir_all(output_directory).unwrap();
    }

    #[tokio::test]
    async fn writer_creates_readable_session_files() {
        let output_directory = env::temp_dir().join(format!(
            "echoscribe-capture-test-{}-{}",
            process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let participants = ParticipantContext::empty_for_test();
        let (sender, drain) =
            start(&output_directory, "123", "456", 1, &participants, true).unwrap();
        let session_path = drain.session_directory().to_path_buf();
        let packets_path = drain.session_directory().join(PACKET_JOURNAL_FILE_NAME);
        let playout_path = drain.session_directory().join(PLAYOUT_JOURNAL_FILE_NAME);

        sender.try_send(packet(42));
        sender.try_send_user_identity(
            789,
            Some("Server name".into()),
            Some("Global name".into()),
            "username".into(),
        );
        sender.try_send_speaker_mapping(123, Some("789".into()), 1);
        sender.try_send_playout(
            10,
            123,
            Some((42, 42 * 960, Some(OpusPayloadBounds { start: 12, end: 16 }))),
            960,
        );
        sender.try_send_audio(
            10,
            123,
            vec![321; crate::diagnostics::SAMPLES_PER_TICK as usize],
        );
        sender.try_send_user_disconnected(789);
        drain.stop().await;

        let bytes = fs::read(&packets_path).unwrap();
        let mut reader = bytes.as_slice();
        read_file_header(&mut reader).unwrap();

        let ReadRecord::Record(record) = read_record(&mut reader).unwrap() else {
            panic!("expected one packet record");
        };

        assert_eq!(record.ssrc, 123);
        assert_eq!(record.sequence, 42);
        assert_eq!(record.timestamp, 42 * 960);
        assert_eq!(record.payload_start, 12);
        assert_eq!(record.payload_end, 16);
        assert_eq!(record.packet, packet(42).packet);
        assert_eq!(read_record(&mut reader).unwrap(), ReadRecord::EndOfFile);

        let bytes = fs::read(&playout_path).unwrap();
        let mut reader = bytes.as_slice();
        let format_version = read_playout_file_header(&mut reader).unwrap();
        let ReadPlayoutRecord::Record(record) =
            read_playout_record(&mut reader, format_version).unwrap()
        else {
            panic!("expected one playout record");
        };
        assert_eq!(record.tick, 10);
        assert_eq!(record.ssrc, 123);
        assert_eq!(
            record.decision,
            PlayoutDecision::Packet {
                sequence: 42,
                timestamp: 42 * 960,
                opus_payload: Some(OpusPayloadBounds { start: 12, end: 16 }),
            }
        );
        assert_eq!(record.decoded_samples, 960);
        assert_eq!(
            read_playout_record(&mut reader, format_version).unwrap(),
            ReadPlayoutRecord::EndOfFile
        );

        let session: serde_json::Value =
            serde_json::from_slice(&fs::read(session_path.join("session.json")).unwrap()).unwrap();
        assert_eq!(session["format"], 3);
        assert_eq!(session["state"], "recording");
        assert_eq!(session["discord"]["guild_id"], "123");
        assert_eq!(session["discord"]["channel_id"], "456");
        assert_eq!(session["files"]["packets"]["path"], "packets.dat");
        assert_eq!(session["files"]["playout"]["path"], "playout.dat");
        assert_eq!(session["files"]["events"]["path"], "events.ndjson");
        assert_eq!(
            session["files"]["events"]["format"],
            crate::session::EVENT_FORMAT_VERSION
        );
        assert_eq!(
            session["files"]["participants"]["path"],
            "participants.toml"
        );
        assert_eq!(session["files"]["tracks"]["path"], "tracks.json");
        assert!(session_path.join("participants.toml").is_file());

        let events = fs::read_to_string(session_path.join("events.ndjson")).unwrap();
        let events = events
            .lines()
            .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(events.len(), 3);
        assert_eq!(events[0]["event"], "user_identity");
        assert_eq!(events[0]["user_id"], "789");
        assert_eq!(events[0]["server_display_name"], "Server name");
        assert_eq!(events[0]["global_display_name"], "Global name");
        assert_eq!(events[0]["username"], "username");
        assert_eq!(events[1]["event"], "speaker_mapping");
        assert_eq!(events[1]["ssrc"], 123);
        assert_eq!(events[1]["user_id"], "789");
        assert_eq!(events[1]["speaking_bits"], 1);
        assert_eq!(events[2]["event"], "user_disconnected");
        assert_eq!(events[2]["user_id"], "789");

        let mut wav =
            hound::WavReader::open(session_path.join("diagnostics/ssrc-123.wav")).unwrap();
        assert_eq!(wav.spec().channels, 1);
        assert_eq!(wav.spec().sample_rate, 48_000);
        let samples = wav.samples::<i16>().collect::<Result<Vec<_>, _>>().unwrap();
        assert_eq!(
            samples,
            vec![321; crate::diagnostics::SAMPLES_PER_TICK as usize]
        );
        let live_flac_path = session_path.join("tracks/user-789.flac.part");
        assert!(live_flac_path.is_file());
        assert!(!session_path.join("tracks/user-789.flac").exists());
        let mut live_flac = flac_codec::decode::FlacSampleReader::open(live_flac_path).unwrap();
        let mut live_samples = Vec::new();
        live_flac.read_to_end(&mut live_samples).unwrap();
        assert_eq!(
            live_samples.len(),
            crate::diagnostics::SAMPLES_PER_TICK as usize * 11
        );
        assert!(
            live_samples[..crate::diagnostics::SAMPLES_PER_TICK as usize * 10]
                .iter()
                .all(|sample| *sample == 0)
        );
        assert!(
            live_samples[crate::diagnostics::SAMPLES_PER_TICK as usize * 10..]
                .iter()
                .all(|sample| *sample == 321)
        );

        crate::inspect::run(&session_path).unwrap();

        fs::remove_dir_all(output_directory).unwrap();
    }

    #[tokio::test]
    async fn global_routing_tick_abandons_a_silent_unresolved_ssrc() {
        let output_directory = env::temp_dir().join(format!(
            "echoscribe-routing-expiry-test-{}-{}",
            process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let participants = ParticipantContext::empty_for_test();
        let (sender, drain) =
            start(&output_directory, "123", "456", 1, &participants, false).unwrap();
        let session_path = drain.session_directory().to_path_buf();

        sender.try_send_audio(
            10,
            999,
            vec![1; crate::diagnostics::SAMPLES_PER_TICK as usize],
        );
        sender.try_advance_routing_tick(260);
        drain.stop().await;

        let events = fs::read_to_string(session_path.join(EVENT_JOURNAL_FILE_NAME)).unwrap();
        let events = events
            .lines()
            .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0]["event"], "unresolved_ssrc_abandoned");
        assert_eq!(events[0]["ssrc"], 999);
        assert_eq!(events[0]["first_tick"], 10);
        assert_eq!(events[0]["last_tick"], 260);
        assert_eq!(events[0]["discarded_frames"], 1);
        assert_eq!(events[0]["discarded_samples"], 960);
        assert_eq!(events[0]["reason"], "age_limit");
        assert!(!session_path.join("diagnostics").exists());

        fs::remove_dir_all(output_directory).unwrap();
    }

    #[tokio::test]
    async fn live_encoder_error_is_durable_and_does_not_create_a_replacement() {
        let output_directory = env::temp_dir().join(format!(
            "echoscribe-live-flac-failure-test-{}-{}",
            process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let participants = ParticipantContext::empty_for_test();
        let (sender, drain) =
            start(&output_directory, "123", "456", 1, &participants, false).unwrap();
        let session_path = drain.session_directory().to_path_buf();
        let part_path = session_path.join("tracks/user-789.flac.part");
        fs::create_dir(&part_path).unwrap();

        sender.try_send_speaker_mapping(123, Some("789".into()), 1);
        sender.try_send_audio(
            10,
            123,
            vec![1; crate::diagnostics::SAMPLES_PER_TICK as usize],
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
        sender.try_send_audio(
            11,
            123,
            vec![2; crate::diagnostics::SAMPLES_PER_TICK as usize],
        );
        drain.stop().await;

        let session = session::SessionStore::load(&session_path).unwrap();
        assert_eq!(session.record().failures.len(), 1);
        assert_eq!(session.record().failures[0].kind, "live_flac_encoder");
        assert!(
            session.record().failures[0]
                .message
                .contains("Discord user 789")
        );
        assert!(part_path.is_dir());
        assert!(!session_path.join("tracks/user-789.flac").exists());

        fs::remove_dir_all(output_directory).unwrap();
    }

    #[tokio::test]
    async fn queue_full_abandonment_records_required_durable_evidence() {
        let session_path = env::temp_dir().join(format!(
            "echoscribe-live-flac-queue-failure-test-{}-{}",
            process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir(&session_path).unwrap();
        let participants = ParticipantContext::empty_for_test();
        let session_store = session::SessionStore::create(
            &session_path,
            NewSession {
                session_id: "session-test",
                started_at_unix_millis: 1_000,
                configuration_version: 1,
                guild_id: "123",
                channel_id: "456",
                participants: &participants,
            },
        )
        .unwrap();
        let (failure_sender, failure_receiver) = mpsc::unbounded_channel();
        let task = tokio::spawn(record_session_failures(
            failure_receiver,
            session_store,
            1_000,
        ));
        let mut summary = ConsumerSummary::default();

        record_queue_abandonment(
            &failure_sender,
            &mut summary,
            &TrackAbandonment {
                discord_user_id: 789,
                tick: 42,
                elapsed_nanos: 200_000_000,
                reason: "queue_full",
                queue_depth: 1024,
                queue_capacity: 1024,
                queue_high_water: 1024,
            },
        );
        drop(failure_sender);
        await_failure_recorder(task).await.unwrap();

        let session = session::SessionStore::load(&session_path).unwrap();
        assert_eq!(session.record().failures.len(), 1);
        let failure = &session.record().failures[0];
        assert_eq!(failure.recorded_at_unix_millis, 1_200);
        assert_eq!(failure.kind, "live_flac_queue_full");
        for evidence in [
            "Discord user 789",
            "tick 42",
            "reason queue_full",
            "queue depth 1024/1024",
            "high-water 1024",
        ] {
            assert!(failure.message.contains(evidence), "{evidence:?} missing");
        }
        assert!(summary.abandoned_users.contains(&789));

        fs::remove_dir_all(session_path).unwrap();
    }
}
