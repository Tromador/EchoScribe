use std::{
    collections::{HashMap, HashSet},
    fs::{File, OpenOptions},
    io::{self, BufWriter},
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
};

use flac_codec::encode::{FlacSampleWriter, Options};
use tokio::{
    sync::{mpsc, oneshot},
    task::JoinHandle,
};

use crate::{
    diagnostics::{CHANNELS, SAMPLE_RATE, SAMPLES_PER_TICK},
    identity::ResolvedFrame,
};

pub(crate) const QUEUE_CAPACITY: usize = 1024;
const BITS_PER_SAMPLE: u32 = 16;
const SILENCE_BLOCK_SAMPLES: usize = 4096;
const SILENCE_BLOCK: [i32; SILENCE_BLOCK_SAMPLES] = [0; SILENCE_BLOCK_SAMPLES];

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct TrackAbandonment {
    pub(crate) discord_user_id: u64,
    pub(crate) tick: u64,
    pub(crate) elapsed_nanos: u64,
    pub(crate) reason: &'static str,
    pub(crate) queue_depth: usize,
    pub(crate) queue_capacity: usize,
    pub(crate) queue_high_water: usize,
}

#[derive(Default)]
pub(crate) struct QueueMetrics {
    accepted: AtomicU64,
    enqueue_failures: AtomicU64,
    high_water: AtomicUsize,
    warning_crossings: AtomicU64,
}

pub(crate) struct LiveFlacStage {
    sender: mpsc::Sender<ResolvedFrame>,
    stop: oneshot::Sender<()>,
    task: JoinHandle<StageSummary>,
    failure_receiver: mpsc::UnboundedReceiver<TrackFailure>,
    metrics: Arc<QueueMetrics>,
    abandoned_users: HashSet<u64>,
    warning_armed: bool,
}

#[derive(Default)]
pub(crate) struct StageSummary {
    pub(crate) tracks: Vec<LiveTrackSummary>,
    pub(crate) failures: Vec<TrackFailure>,
}

pub(crate) struct LiveTrackSummary {
    pub(crate) discord_user_id: u64,
    pub(crate) path: PathBuf,
    pub(crate) first_tick: u64,
    pub(crate) frames: u64,
    pub(crate) source_samples: u64,
    pub(crate) inserted_silence_samples: u64,
    pub(crate) nonstandard_frames: u64,
    pub(crate) source_ssrcs: Vec<u32>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct TrackFailure {
    pub(crate) discord_user_id: u64,
    pub(crate) tick: u64,
    pub(crate) elapsed_nanos: u64,
    pub(crate) reason: &'static str,
    pub(crate) message: String,
}

impl LiveFlacStage {
    pub(crate) fn start(session_directory: &Path) -> Self {
        Self::start_with_capacity(session_directory, QUEUE_CAPACITY)
    }

    fn start_with_capacity(session_directory: &Path, capacity: usize) -> Self {
        let directory = session_directory.join("tracks");
        let (sender, receiver) = mpsc::channel(capacity);
        let (stop, stop_receiver) = oneshot::channel();
        let (failure_sender, failure_receiver) = mpsc::unbounded_channel();
        let metrics = Arc::new(QueueMetrics::default());
        let task = tokio::spawn(run(receiver, stop_receiver, failure_sender, directory));

        Self {
            sender,
            stop,
            task,
            failure_receiver,
            metrics,
            abandoned_users: HashSet::new(),
            warning_armed: true,
        }
    }

    pub(crate) fn try_send(&mut self, frame: ResolvedFrame) -> Option<TrackAbandonment> {
        if self.abandoned_users.contains(&frame.discord_user_id) {
            return None;
        }

        let depth_before = self.depth();
        self.observe_depth(depth_before);

        let user_id = frame.discord_user_id;
        let tick = frame.tick;
        let elapsed_nanos = frame.elapsed_nanos;
        match self.sender.try_send(frame) {
            Ok(()) => {
                self.metrics.accepted.fetch_add(1, Ordering::Relaxed);
                let depth = depth_before + 1;
                self.metrics.high_water.fetch_max(depth, Ordering::Relaxed);
                self.observe_depth(depth);
                None
            }
            Err(mpsc::error::TrySendError::Full(_)) => {
                self.metrics
                    .enqueue_failures
                    .fetch_add(1, Ordering::Relaxed);
                self.metrics
                    .high_water
                    .fetch_max(self.sender.max_capacity(), Ordering::Relaxed);
                self.abandon(
                    user_id,
                    tick,
                    elapsed_nanos,
                    "queue_full",
                    self.sender.max_capacity(),
                )
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                self.metrics
                    .enqueue_failures
                    .fetch_add(1, Ordering::Relaxed);
                self.abandon(user_id, tick, elapsed_nanos, "queue_closed", depth_before)
            }
        }
    }

    pub(crate) fn abandon_user(
        &mut self,
        discord_user_id: u64,
        tick: u64,
        elapsed_nanos: u64,
        reason: &'static str,
    ) -> Option<TrackAbandonment> {
        self.abandon(discord_user_id, tick, elapsed_nanos, reason, self.depth())
    }

    pub(crate) fn take_encoder_failures(&mut self) -> Vec<TrackFailure> {
        let mut failures = Vec::new();
        while let Ok(failure) = self.failure_receiver.try_recv() {
            if failure.discord_user_id != 0 {
                self.abandoned_users.insert(failure.discord_user_id);
            }
            failures.push(failure);
        }
        failures
    }

    fn abandon(
        &mut self,
        discord_user_id: u64,
        tick: u64,
        elapsed_nanos: u64,
        reason: &'static str,
        queue_depth: usize,
    ) -> Option<TrackAbandonment> {
        self.abandoned_users
            .insert(discord_user_id)
            .then(|| TrackAbandonment {
                discord_user_id,
                tick,
                elapsed_nanos,
                reason,
                queue_depth,
                queue_capacity: self.sender.max_capacity(),
                queue_high_water: self.metrics.high_water.load(Ordering::Relaxed),
            })
    }

    pub(crate) fn depth(&self) -> usize {
        self.sender.max_capacity() - self.sender.capacity()
    }

    fn observe_depth(&mut self, depth: usize) {
        let capacity = self.sender.max_capacity();
        let warning_depth = capacity * 3 / 4;
        let warning_rearm_depth = capacity / 2;
        if depth < warning_rearm_depth {
            self.warning_armed = true;
        } else if self.warning_armed && depth >= warning_depth {
            self.warning_armed = false;
            self.metrics
                .warning_crossings
                .fetch_add(1, Ordering::Relaxed);
            eprintln!(
                "Live FLAC queue reached {depth}/{capacity} frames; \
                 warning re-arms below {warning_rearm_depth}."
            );
        }
    }

    pub(crate) async fn stop(self) -> StageReport {
        let Self {
            sender,
            stop,
            task,
            metrics,
            abandoned_users,
            failure_receiver: _,
            ..
        } = self;
        drop(sender);
        let _ = stop.send(());
        let summary = match task.await {
            Ok(summary) => summary,
            Err(error) => StageSummary {
                failures: vec![TrackFailure {
                    discord_user_id: 0,
                    tick: 0,
                    elapsed_nanos: 0,
                    reason: "worker_failed",
                    message: error.to_string(),
                }],
                ..StageSummary::default()
            },
        };

        StageReport {
            accepted: metrics.accepted.load(Ordering::Relaxed),
            enqueue_failures: metrics.enqueue_failures.load(Ordering::Relaxed),
            high_water: metrics.high_water.load(Ordering::Relaxed),
            warning_crossings: metrics.warning_crossings.load(Ordering::Relaxed),
            abandoned_users,
            summary,
        }
    }
}

pub(crate) struct StageReport {
    pub(crate) accepted: u64,
    pub(crate) enqueue_failures: u64,
    pub(crate) high_water: usize,
    pub(crate) warning_crossings: u64,
    pub(crate) abandoned_users: HashSet<u64>,
    pub(crate) summary: StageSummary,
}

async fn run(
    mut receiver: mpsc::Receiver<ResolvedFrame>,
    mut stop: oneshot::Receiver<()>,
    failure_sender: mpsc::UnboundedSender<TrackFailure>,
    directory: PathBuf,
) -> StageSummary {
    let mut writers = HashMap::<u64, Track>::new();
    let mut abandoned = HashSet::new();
    let mut failures = Vec::new();

    loop {
        tokio::select! {
            biased;
            _ = &mut stop => {
                receiver.close();
                while let Some(frame) = receiver.recv().await {
                    write_frame(
                        &directory,
                        &mut writers,
                        &mut abandoned,
                        &mut failures,
                        &failure_sender,
                        frame,
                    );
                }
                break;
            }
            frame = receiver.recv() => {
                match frame {
                    Some(frame) => write_frame(
                        &directory,
                        &mut writers,
                        &mut abandoned,
                        &mut failures,
                        &failure_sender,
                        frame,
                    ),
                    None => break,
                }
            }
        }
    }

    let mut tracks = Vec::new();
    for (user_id, track) in writers {
        match track.finalize(user_id) {
            Ok(summary) => tracks.push(summary),
            Err(failure) => failures.push(failure),
        }
    }
    tracks.sort_unstable_by_key(|track| track.discord_user_id);
    failures.sort_unstable_by_key(|failure| failure.discord_user_id);
    StageSummary { tracks, failures }
}

fn write_frame(
    directory: &Path,
    writers: &mut HashMap<u64, Track>,
    abandoned: &mut HashSet<u64>,
    failures: &mut Vec<TrackFailure>,
    failure_sender: &mpsc::UnboundedSender<TrackFailure>,
    frame: ResolvedFrame,
) {
    if abandoned.contains(&frame.discord_user_id) {
        return;
    }

    let result = match writers.entry(frame.discord_user_id) {
        std::collections::hash_map::Entry::Occupied(mut entry) => {
            entry.get_mut().write_frame(&frame)
        }
        std::collections::hash_map::Entry::Vacant(entry) => {
            match Track::create(directory, frame.discord_user_id, frame.tick) {
                Ok(mut track) => {
                    let result = track.write_frame(&frame);
                    entry.insert(track);
                    result
                }
                Err(error) => Err(error),
            }
        }
    };

    if let Err(error) = result {
        abandoned.insert(frame.discord_user_id);
        writers.remove(&frame.discord_user_id);
        let failure = TrackFailure {
            discord_user_id: frame.discord_user_id,
            tick: frame.tick,
            elapsed_nanos: frame.elapsed_nanos,
            reason: "encoder_error",
            message: error.to_string(),
        };
        let _ = failure_sender.send(failure.clone());
        failures.push(failure);
    }
}

struct Track {
    writer: FlacSampleWriter<BufWriter<File>>,
    path: PathBuf,
    first_tick: u64,
    next_tick: u64,
    frames: u64,
    source_samples: u64,
    inserted_silence_samples: u64,
    nonstandard_frames: u64,
    last_elapsed_nanos: u64,
    source_ssrcs: HashSet<u32>,
    sample_buffer: Vec<i32>,
}

impl Track {
    fn create(directory: &Path, discord_user_id: u64, first_tick: u64) -> io::Result<Self> {
        let path = directory.join(format!("user-{discord_user_id}.flac.part"));
        let writer = FlacSampleWriter::create(
            &path,
            Options::default(),
            SAMPLE_RATE,
            BITS_PER_SAMPLE,
            CHANNELS as u8,
            None,
        )
        .map_err(flac_error)?;

        Ok(Self {
            writer,
            path,
            first_tick,
            next_tick: 0,
            frames: 0,
            source_samples: 0,
            inserted_silence_samples: 0,
            nonstandard_frames: 0,
            last_elapsed_nanos: 0,
            source_ssrcs: HashSet::new(),
            sample_buffer: Vec::with_capacity(SAMPLES_PER_TICK as usize),
        })
    }

    fn write_frame(&mut self, frame: &ResolvedFrame) -> io::Result<()> {
        if frame.tick < self.next_tick {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "audio tick {} for user {} arrived after tick {}",
                    frame.tick,
                    frame.discord_user_id,
                    self.next_tick - 1
                ),
            ));
        }

        let missing_ticks = frame.tick - self.next_tick;
        let silence_samples = missing_ticks
            .checked_mul(SAMPLES_PER_TICK)
            .ok_or_else(|| io::Error::other("live FLAC silence length overflow"))?;
        self.write_silence(silence_samples)?;
        self.sample_buffer.clear();
        self.sample_buffer
            .extend(frame.samples.iter().map(|sample| i32::from(*sample)));
        self.writer.write(&self.sample_buffer).map_err(flac_error)?;

        self.next_tick = frame
            .tick
            .checked_add(1)
            .ok_or_else(|| io::Error::other("live FLAC tick counter overflow"))?;
        self.frames += 1;
        self.source_samples += frame.samples.len() as u64;
        self.inserted_silence_samples += silence_samples;
        self.source_ssrcs.insert(frame.source_ssrc);
        self.last_elapsed_nanos = frame.elapsed_nanos;
        if frame.samples.len() as u64 != SAMPLES_PER_TICK {
            self.nonstandard_frames += 1;
        }
        Ok(())
    }

    fn write_silence(&mut self, mut samples: u64) -> io::Result<()> {
        while samples > 0 {
            let length = usize::try_from(samples.min(SILENCE_BLOCK_SAMPLES as u64))
                .expect("silence block length always fits usize");
            self.writer
                .write(&SILENCE_BLOCK[..length])
                .map_err(flac_error)?;
            samples -= length as u64;
        }
        Ok(())
    }

    fn finalize(self, discord_user_id: u64) -> Result<LiveTrackSummary, TrackFailure> {
        let tick = self.next_tick.saturating_sub(1);
        let path = self.path.clone();
        if let Err(error) = self
            .writer
            .finalize()
            .map_err(flac_error)
            .and_then(|()| OpenOptions::new().write(true).open(&path)?.sync_data())
        {
            return Err(TrackFailure {
                discord_user_id,
                tick,
                elapsed_nanos: self.last_elapsed_nanos,
                reason: "encoder_error",
                message: error.to_string(),
            });
        }

        let mut source_ssrcs = self.source_ssrcs.into_iter().collect::<Vec<_>>();
        source_ssrcs.sort_unstable();
        Ok(LiveTrackSummary {
            discord_user_id,
            path: self.path,
            first_tick: self.first_tick,
            frames: self.frames,
            source_samples: self.source_samples,
            inserted_silence_samples: self.inserted_silence_samples,
            nonstandard_frames: self.nonstandard_frames,
            source_ssrcs,
        })
    }
}

fn flac_error(error: flac_codec::Error) -> io::Error {
    io::Error::other(error)
}

#[cfg(test)]
mod tests {
    use std::{
        env, fs, process,
        time::{SystemTime, UNIX_EPOCH},
    };

    use flac_codec::decode::FlacSampleReader;

    use super::*;

    #[tokio::test]
    async fn one_user_track_merges_ssrcs_and_preserves_alignment() {
        let session = test_directory("aligned");
        fs::create_dir(session.join("tracks")).unwrap();
        let mut stage = LiveFlacStage::start(&session);

        assert!(stage.try_send(frame(11, 100, 2, 960, 100)).is_none());
        assert!(stage.try_send(frame(11, 200, 4, 480, 200)).is_none());
        let report = stage.stop().await;

        assert!(report.summary.failures.is_empty());
        assert_eq!(report.summary.tracks.len(), 1);
        let track = &report.summary.tracks[0];
        assert_eq!(track.discord_user_id, 11);
        assert_eq!(track.first_tick, 2);
        assert_eq!(track.frames, 2);
        assert_eq!(track.source_samples, 1440);
        assert_eq!(track.inserted_silence_samples, 3 * SAMPLES_PER_TICK);
        assert_eq!(track.nonstandard_frames, 1);
        assert_eq!(track.source_ssrcs, [100, 200]);
        assert!(track.path.ends_with("tracks/user-11.flac.part"));
        assert!(!session.join("tracks/user-11.flac").exists());

        let mut reader = FlacSampleReader::open(&track.path).unwrap();
        let mut samples = Vec::new();
        reader.read_to_end(&mut samples).unwrap();
        assert_eq!(samples.len(), 5 * SAMPLES_PER_TICK as usize - 480);
        assert!(samples[..1920].iter().all(|sample| *sample == 0));
        assert!(samples[1920..2880].iter().all(|sample| *sample == 100));
        assert!(samples[2880..3840].iter().all(|sample| *sample == 0));
        assert!(samples[3840..].iter().all(|sample| *sample == 200));

        fs::remove_dir_all(session).unwrap();
    }

    #[tokio::test]
    async fn queue_full_abandons_only_rejected_user_without_blocking() {
        let session = test_directory("queue-full");
        fs::create_dir(session.join("tracks")).unwrap();
        let directory = session.join("tracks");
        let (sender, receiver) = mpsc::channel(1);
        let (stop, stop_receiver) = oneshot::channel();
        let metrics = Arc::new(QueueMetrics::default());
        let task = tokio::spawn(async move {
            let _receiver = receiver;
            let _ = stop_receiver.await;
            StageSummary::default()
        });
        let (_failure_sender, failure_receiver) = mpsc::unbounded_channel();
        let mut stage = LiveFlacStage {
            sender,
            stop,
            task,
            failure_receiver,
            metrics: Arc::clone(&metrics),
            abandoned_users: HashSet::new(),
            warning_armed: true,
        };

        assert!(stage.try_send(frame(11, 100, 10, 960, 1)).is_none());
        let abandonment = stage
            .try_send(frame(22, 200, 10, 960, 2))
            .expect("second frame should find the queue full");
        assert_eq!(abandonment.discord_user_id, 22);
        assert_eq!(abandonment.reason, "queue_full");
        assert_eq!(abandonment.queue_depth, 1);
        assert_eq!(abandonment.queue_capacity, 1);
        assert_eq!(abandonment.queue_high_water, 1);

        assert!(stage.try_send(frame(22, 200, 11, 960, 3)).is_none());
        assert_eq!(metrics.accepted.load(Ordering::Relaxed), 1);
        assert_eq!(metrics.enqueue_failures.load(Ordering::Relaxed), 1);
        assert_eq!(metrics.high_water.load(Ordering::Relaxed), 1);
        assert!(!stage.abandoned_users.contains(&11));
        assert!(stage.abandoned_users.contains(&22));

        let report = stage.stop().await;
        assert_eq!(report.abandoned_users, HashSet::from([22]));
        fs::remove_dir_all(directory.parent().unwrap()).unwrap();
    }

    #[tokio::test]
    async fn encoder_error_abandons_track_without_replacement() {
        let session = test_directory("encoder-error");
        let tracks = session.join("tracks");
        fs::create_dir_all(tracks.join("user-11.flac.part")).unwrap();
        let mut stage = LiveFlacStage::start(&session);

        assert!(stage.try_send(frame(11, 100, 10, 960, 1)).is_none());
        tokio::task::yield_now().await;
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        let failures = stage.take_encoder_failures();
        assert_eq!(failures.len(), 1);
        assert_eq!(failures[0].discord_user_id, 11);
        assert_eq!(failures[0].reason, "encoder_error");

        assert!(stage.try_send(frame(11, 200, 11, 960, 2)).is_none());
        let report = stage.stop().await;
        assert_eq!(report.accepted, 1);
        assert_eq!(report.summary.failures.len(), 1);
        assert!(tracks.join("user-11.flac.part").is_dir());
        assert!(!tracks.join("user-11.flac").exists());

        fs::remove_dir_all(session).unwrap();
    }

    #[tokio::test]
    async fn backlog_warning_rearms_only_below_half_capacity() {
        let session = test_directory("warning");
        fs::create_dir(session.join("tracks")).unwrap();
        let mut stage = LiveFlacStage::start_with_capacity(&session, 4);

        stage.observe_depth(3);
        stage.observe_depth(4);
        assert_eq!(stage.metrics.warning_crossings.load(Ordering::Relaxed), 1);
        stage.observe_depth(2);
        stage.observe_depth(3);
        assert_eq!(stage.metrics.warning_crossings.load(Ordering::Relaxed), 1);
        stage.observe_depth(1);
        stage.observe_depth(3);
        assert_eq!(stage.metrics.warning_crossings.load(Ordering::Relaxed), 2);

        stage.stop().await;
        fs::remove_dir_all(session).unwrap();
    }

    fn frame(
        discord_user_id: u64,
        source_ssrc: u32,
        tick: u64,
        samples: usize,
        value: i16,
    ) -> ResolvedFrame {
        ResolvedFrame {
            discord_user_id,
            display_name: discord_user_id.to_string(),
            source_ssrc,
            elapsed_nanos: tick * 20_000_000,
            tick,
            samples: vec![value; samples],
        }
    }

    fn test_directory(label: &str) -> PathBuf {
        let directory = env::temp_dir().join(format!(
            "echoscribe-live-flac-{label}-{}-{}",
            process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir(&directory).unwrap();
        directory
    }
}
