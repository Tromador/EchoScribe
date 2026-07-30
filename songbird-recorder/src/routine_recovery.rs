//! Operator-controlled reconstruction of routine Discord-user FLAC tracks.
//!
//! Recovery replays authoritative journals, applies timestamped transport
//! identity evidence, and publishes only explicitly selected derived tracks.
//! It never advances workflow state; `continue` owns that separate decision.

use std::{
    collections::{HashMap, HashSet},
    fs::{self, File, OpenOptions},
    io::{self, BufWriter},
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, anyhow, bail};
use flac_codec::{
    decode::{Verified, verify},
    encode::{FlacSampleWriter, Options},
};

use crate::{
    artifacts::TRACK_DIRECTORY_NAME,
    diagnostics::{CHANNELS, DecodedFrame, SAMPLE_RATE, SAMPLES_PER_TICK},
    recover,
    session::{
        EVENT_FORMAT_VERSION, LEGACY_EVENT_FORMAT_VERSION, SessionEvent, SessionFiles,
        SessionStore, WorkflowState,
    },
    track_manifest::{TrackManifest, TrackState},
};

const BITS_PER_SAMPLE: u32 = 16;
const SILENCE_BLOCK_SAMPLES: usize = 4096;
const SILENCE_BLOCK: [i32; SILENCE_BLOCK_SAMPLES] = [0; SILENCE_BLOCK_SAMPLES];
pub(crate) const RECOVERY_STARTED_PREFIX: &str = "track_recovery_started_user_";
pub(crate) const RECOVERY_COMPLETED_PREFIX: &str = "track_recovery_completed_user_";

/// Recover all incomplete tracks, or exactly the explicitly named users.
pub(crate) fn run(session_directory: &Path, requested_users: &[u64]) -> Result<()> {
    let mut session = SessionStore::load(session_directory).with_context(|| {
        format!(
            "failed to load workflow state from {}",
            session_directory.display()
        )
    })?;
    if session.record().state != WorkflowState::AwaitingOperator {
        bail!(
            "track recovery requires session state awaiting_operator; found {}",
            session.record().state.as_str()
        );
    }

    let manifest_path = session_directory.join(&session.record().files.tracks.path);
    let mut manifest = TrackManifest::read(&manifest_path)
        .with_context(|| format!("failed to read track manifest {}", manifest_path.display()))?;
    if manifest.session_id != session.record().session_id {
        bail!(
            "track manifest session ID {:?} does not match session.json ID {:?}",
            manifest.session_id,
            session.record().session_id
        );
    }

    let targets = select_targets(&manifest, requested_users)?;
    let attempted_at = unix_millis_now()?;
    session.record_checkpoints(
        attempted_at,
        targets
            .iter()
            .map(|user_id| format!("{RECOVERY_STARTED_PREFIX}{user_id}")),
    )?;

    let result = recover_selected(
        session_directory,
        &session.record().files,
        &mut manifest,
        &targets,
    );
    if let Err(error) = result {
        let message = format!(
            "routine track recovery for Discord users {} failed: {error:#}",
            display_user_ids(&targets)
        );
        if let Err(record_error) =
            session.record_failure(unix_millis_now()?, "track_recovery", &message)
        {
            return Err(anyhow!(
                "{message}; additionally failed to record that recovery failure: {record_error}"
            ));
        }
        return Err(anyhow!(message));
    }

    if let Err(error) = session.record_checkpoints(
        unix_millis_now()?,
        targets
            .iter()
            .map(|user_id| format!("{RECOVERY_COMPLETED_PREFIX}{user_id}")),
    ) {
        let message = format!(
            "recovered routine tracks for Discord users {}, but failed to record the durable recovery result: {error}",
            display_user_ids(&targets)
        );
        let _ = session.record_failure(unix_millis_now()?, "track_recovery", &message);
        return Err(anyhow!(message));
    }

    println!(
        "Recovered routine FLAC tracks for Discord users {}. Session remains awaiting operator action.",
        display_user_ids(&targets)
    );
    Ok(())
}

fn select_targets(manifest: &TrackManifest, requested_users: &[u64]) -> Result<Vec<u64>> {
    let available = manifest
        .tracks
        .iter()
        .map(|track| {
            track
                .discord_user_id
                .parse::<u64>()
                .expect("validated track manifest contains numeric Discord IDs")
        })
        .collect::<HashSet<_>>();

    let mut targets = if requested_users.is_empty() {
        manifest
            .tracks
            .iter()
            .filter(|track| track.state == TrackState::Incomplete)
            .map(|track| {
                track
                    .discord_user_id
                    .parse::<u64>()
                    .expect("validated track manifest contains numeric Discord IDs")
            })
            .collect::<Vec<_>>()
    } else {
        requested_users.to_vec()
    };
    targets.sort_unstable();
    targets.dedup();

    if targets.is_empty() {
        bail!("session has no incomplete routine tracks to recover");
    }
    if let Some(missing) = targets.iter().find(|user_id| !available.contains(user_id)) {
        bail!("Discord user {missing} has no entry in tracks.json");
    }
    Ok(targets)
}

fn recover_selected(
    session_directory: &Path,
    files: &SessionFiles,
    manifest: &mut TrackManifest,
    targets: &[u64],
) -> Result<()> {
    let timeline = MappingTimeline::read(&session_directory.join(&files.events.path))?;
    let target_set = targets.iter().copied().collect::<HashSet<_>>();
    let track_directory = session_directory.join(TRACK_DIRECTORY_NAME);
    fs::create_dir_all(&track_directory)?;
    let mut output = RecoveredTracks::new(&track_directory, target_set);

    let summary = recover::replay_session_files(session_directory, files, |frame| {
        let Some(user_id) = timeline.user_at(frame.ssrc, frame.elapsed_nanos) else {
            return Ok(());
        };
        output.write_frame(user_id, frame)
    })?;

    if summary.truncated_packet_tail || summary.truncated_playout_tail {
        bail!("authoritative packet or playout journal has a truncated tail");
    }
    if summary.skipped_undecoded > 0 {
        bail!(
            "playout journal contains {} decisions without decoded-sample evidence",
            summary.skipped_undecoded
        );
    }

    let recovered = output.finalize(targets)?;
    publish_recovered_tracks(session_directory, manifest, recovered)?;
    Ok(())
}

fn publish_recovered_tracks(
    session_directory: &Path,
    manifest: &mut TrackManifest,
    recovered: Vec<RecoveredTrack>,
) -> Result<()> {
    let track_directory = session_directory.join(TRACK_DIRECTORY_NAME);

    for track in &recovered {
        let part_path = track_directory.join(format!("user-{}.flac.part", track.user_id));
        let final_path = track_directory.join(format!("user-{}.flac", track.user_id));

        // The verified temporary file atomically replaces any old partial
        // track before following the normal `.part` -> `.flac` publication.
        fs::rename(&track.temporary_path, &part_path).with_context(|| {
            format!(
                "failed to publish recovered partial track {}",
                part_path.display()
            )
        })?;
        File::open(&track_directory)?.sync_all()?;
        fs::rename(&part_path, &final_path).with_context(|| {
            format!(
                "failed to finalise recovered track {}",
                final_path.display()
            )
        })?;
        File::open(&track_directory)?.sync_all()?;
    }

    for track in recovered {
        let description = manifest
            .tracks
            .iter_mut()
            .find(|description| description.discord_user_id == track.user_id.to_string())
            .expect("selected recovery user was validated against the manifest");
        description.path = format!("{TRACK_DIRECTORY_NAME}/user-{}.flac", track.user_id);
        description.state = TrackState::Complete;
        description.length_samples = track.length_samples;
        description.source_ssrcs.clone_from(&track.source_ssrcs);
        description.abandonment_reason = None;
        description.last_contiguous_sample = None;
    }
    manifest.write(session_directory)?;
    Ok(())
}

#[derive(Clone, Copy)]
struct MappingInterval {
    start_nanos: u64,
    end_nanos: u64,
    user_id: u64,
}

#[derive(Default)]
pub(crate) struct MappingTimeline {
    intervals: HashMap<u32, Vec<MappingInterval>>,
    unresolved_ssrcs: HashSet<u32>,
}

impl MappingTimeline {
    pub(crate) fn read(path: &Path) -> Result<Self> {
        let bytes = fs::read(path)
            .with_context(|| format!("failed to read event journal {}", path.display()))?;
        let mut builder = MappingBuilder::default();

        for (line_index, line) in bytes.split_inclusive(|byte| *byte == b'\n').enumerate() {
            if !line.ends_with(b"\n") {
                bail!(
                    "event journal {} has a truncated final record",
                    path.display()
                );
            }
            let event: SessionEvent =
                serde_json::from_slice(line.strip_suffix(b"\n").expect("line ending checked"))
                    .with_context(|| {
                        format!(
                            "invalid event record on line {} in {}",
                            line_index + 1,
                            path.display()
                        )
                    })?;
            builder.observe(event, line_index + 1, path)?;
        }

        Ok(builder.finish())
    }

    fn user_at(&self, ssrc: u32, elapsed_nanos: u64) -> Option<u64> {
        self.intervals.get(&ssrc).and_then(|intervals| {
            intervals
                .iter()
                .find(|interval| {
                    interval.start_nanos <= elapsed_nanos && elapsed_nanos < interval.end_nanos
                })
                .map(|interval| interval.user_id)
        })
    }

    pub(crate) fn unresolved_ssrcs(&self) -> &HashSet<u32> {
        &self.unresolved_ssrcs
    }
}

#[derive(Default)]
struct MappingBuilder {
    timeline: MappingTimeline,
    current: HashMap<u32, MappingInterval>,
    unmapped_since: HashMap<u32, u64>,
}

impl MappingBuilder {
    fn observe(&mut self, event: SessionEvent, line: usize, path: &Path) -> Result<()> {
        match event {
            SessionEvent::SpeakerMapping {
                format,
                elapsed_nanos,
                ssrc,
                user_id,
                ..
            } => {
                if !matches!(format, LEGACY_EVENT_FORMAT_VERSION | EVENT_FORMAT_VERSION) {
                    bail!(
                        "unsupported event format {format} on line {line} in {}",
                        path.display()
                    );
                }
                let Some(user_id) = user_id else {
                    return Ok(());
                };
                let user_id = parse_user_id(&user_id)?;
                self.timeline.unresolved_ssrcs.remove(&ssrc);
                if self
                    .current
                    .get(&ssrc)
                    .is_some_and(|current| current.user_id == user_id)
                {
                    return Ok(());
                }

                let start_nanos = if let Some(current) = self.current.remove(&ssrc) {
                    self.close(ssrc, current, elapsed_nanos);
                    elapsed_nanos
                } else {
                    self.unmapped_since.remove(&ssrc).unwrap_or(0)
                };
                self.current.insert(
                    ssrc,
                    MappingInterval {
                        start_nanos,
                        end_nanos: u64::MAX,
                        user_id,
                    },
                );
            }
            SessionEvent::UserDisconnected {
                format,
                elapsed_nanos,
                user_id,
            } => {
                require_event_format(format, line, path)?;
                let user_id = parse_user_id(&user_id)?;
                let ssrcs = self
                    .current
                    .iter()
                    .filter_map(|(ssrc, interval)| (interval.user_id == user_id).then_some(*ssrc))
                    .collect::<Vec<_>>();
                for ssrc in ssrcs {
                    let interval = self
                        .current
                        .remove(&ssrc)
                        .expect("mapped SSRC was collected above");
                    self.close(ssrc, interval, elapsed_nanos);
                    self.unmapped_since.insert(ssrc, elapsed_nanos);
                }
            }
            SessionEvent::UserIdentity { format, .. } => {
                require_event_format(format, line, path)?;
            }
            SessionEvent::UnresolvedSsrcAbandoned { format, ssrc, .. } => {
                require_event_format(format, line, path)?;
                self.timeline.unresolved_ssrcs.insert(ssrc);
            }
        }
        Ok(())
    }

    fn close(&mut self, ssrc: u32, mut interval: MappingInterval, end_nanos: u64) {
        interval.end_nanos = end_nanos.max(interval.start_nanos);
        self.timeline
            .intervals
            .entry(ssrc)
            .or_default()
            .push(interval);
    }

    fn finish(mut self) -> MappingTimeline {
        for (ssrc, interval) in self.current {
            self.timeline
                .intervals
                .entry(ssrc)
                .or_default()
                .push(interval);
        }
        self.timeline
    }
}

fn require_event_format(format: u16, line: usize, path: &Path) -> Result<()> {
    if format != EVENT_FORMAT_VERSION {
        bail!(
            "unsupported event format {format} on line {line} in {}",
            path.display()
        );
    }
    Ok(())
}

fn parse_user_id(value: &str) -> Result<u64> {
    value
        .parse::<u64>()
        .ok()
        .filter(|user_id| *user_id != 0)
        .ok_or_else(|| anyhow!("invalid Discord user ID {value:?} in event journal"))
}

struct RecoveredTracks {
    directory: PathBuf,
    targets: HashSet<u64>,
    tracks: HashMap<u64, RecoveryTrackWriter>,
}

impl RecoveredTracks {
    fn new(directory: &Path, targets: HashSet<u64>) -> Self {
        Self {
            directory: directory.to_path_buf(),
            targets,
            tracks: HashMap::new(),
        }
    }

    fn write_frame(&mut self, user_id: u64, frame: DecodedFrame) -> Result<()> {
        if !self.targets.contains(&user_id) {
            return Ok(());
        }
        if !self.tracks.contains_key(&user_id) {
            self.tracks.insert(
                user_id,
                RecoveryTrackWriter::create(&self.directory, user_id)?,
            );
        }
        self.tracks
            .get_mut(&user_id)
            .expect("recovery writer was inserted above")
            .write_frame(frame)?;
        Ok(())
    }

    fn finalize(mut self, targets: &[u64]) -> Result<Vec<RecoveredTrack>> {
        for user_id in targets {
            if !self.tracks.contains_key(user_id) {
                bail!(
                    "authoritative journals contain no attributed PCM for Discord user {user_id}"
                );
            }
        }

        let mut recovered = Vec::with_capacity(self.tracks.len());
        for (user_id, writer) in self.tracks.drain() {
            recovered.push(writer.finalize(user_id)?);
        }
        recovered.sort_unstable_by_key(|track| track.user_id);
        Ok(recovered)
    }
}

struct RecoveryTrackWriter {
    writer: Option<FlacSampleWriter<BufWriter<File>>>,
    temporary_path: PathBuf,
    keep_temporary: bool,
    next_tick: u64,
    length_samples: u64,
    source_ssrcs: HashSet<u32>,
    sample_buffer: Vec<i32>,
}

impl RecoveryTrackWriter {
    fn create(directory: &Path, user_id: u64) -> Result<Self> {
        let temporary_path = directory.join(format!("user-{user_id}.flac.part.recovering"));
        if temporary_path.exists() {
            bail!(
                "stale recovery output exists at {}; inspect or remove it before retrying",
                temporary_path.display()
            );
        }
        let writer = FlacSampleWriter::create(
            &temporary_path,
            Options::default(),
            SAMPLE_RATE,
            BITS_PER_SAMPLE,
            CHANNELS as u8,
            None,
        )
        .map_err(io::Error::other)?;
        Ok(Self {
            writer: Some(writer),
            temporary_path,
            keep_temporary: false,
            next_tick: 0,
            length_samples: 0,
            source_ssrcs: HashSet::new(),
            sample_buffer: Vec::with_capacity(SAMPLES_PER_TICK as usize),
        })
    }

    fn write_frame(&mut self, frame: DecodedFrame) -> Result<()> {
        if frame.tick < self.next_tick {
            bail!(
                "recovered audio tick {} for SSRC {} overlaps an earlier frame ending at tick {}",
                frame.tick,
                frame.ssrc,
                self.next_tick.saturating_sub(1)
            );
        }

        let silence_samples = frame
            .tick
            .saturating_sub(self.next_tick)
            .checked_mul(SAMPLES_PER_TICK)
            .ok_or_else(|| anyhow!("recovered FLAC silence length overflow"))?;
        self.write_silence(silence_samples)?;
        self.sample_buffer.clear();
        self.sample_buffer
            .extend(frame.samples.iter().map(|sample| i32::from(*sample)));
        self.writer
            .as_mut()
            .expect("recovery writer exists until finalisation")
            .write(&self.sample_buffer)
            .map_err(io::Error::other)?;

        self.length_samples = self
            .length_samples
            .checked_add(silence_samples)
            .and_then(|length| length.checked_add(frame.samples.len() as u64))
            .ok_or_else(|| anyhow!("recovered FLAC sample count overflow"))?;
        self.next_tick = frame
            .tick
            .checked_add(1)
            .ok_or_else(|| anyhow!("recovered FLAC tick counter overflow"))?;
        self.source_ssrcs.insert(frame.ssrc);
        Ok(())
    }

    fn write_silence(&mut self, mut samples: u64) -> Result<()> {
        while samples > 0 {
            let length = usize::try_from(samples.min(SILENCE_BLOCK_SAMPLES as u64))
                .expect("fixed silence block length fits usize");
            self.writer
                .as_mut()
                .expect("recovery writer exists until finalisation")
                .write(&SILENCE_BLOCK[..length])
                .map_err(io::Error::other)?;
            samples -= length as u64;
        }
        Ok(())
    }

    fn finalize(mut self, user_id: u64) -> Result<RecoveredTrack> {
        let path = self.temporary_path.clone();
        self.writer
            .take()
            .expect("recovery writer exists until finalisation")
            .finalize()
            .map_err(io::Error::other)?;
        OpenOptions::new().write(true).open(&path)?.sync_all()?;
        match verify(&path).map_err(io::Error::other)? {
            Verified::MD5Match => {}
            Verified::MD5Mismatch => bail!("FLAC PCM MD5 mismatch in {}", path.display()),
            Verified::NoMD5 => bail!("FLAC contains no PCM MD5 in {}", path.display()),
        }

        let mut source_ssrcs = self.source_ssrcs.iter().copied().collect::<Vec<_>>();
        source_ssrcs.sort_unstable();
        self.keep_temporary = true;
        Ok(RecoveredTrack {
            user_id,
            temporary_path: path,
            length_samples: self.length_samples,
            source_ssrcs,
        })
    }
}

impl Drop for RecoveryTrackWriter {
    fn drop(&mut self) {
        // Failed attempts must not leave a file which looks like the normal
        // incomplete-track artefact.
        if !self.keep_temporary {
            let _ = fs::remove_file(&self.temporary_path);
        }
    }
}

struct RecoveredTrack {
    user_id: u64,
    temporary_path: PathBuf,
    length_samples: u64,
    source_ssrcs: Vec<u32>,
}

impl Drop for RecoveredTrack {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.temporary_path);
    }
}

fn display_user_ids(user_ids: &[u64]) -> String {
    user_ids
        .iter()
        .map(u64::to_string)
        .collect::<Vec<_>>()
        .join(", ")
}

fn unix_millis_now() -> Result<u64> {
    Ok(u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .context("system clock precedes Unix epoch")?
            .as_millis(),
    )
    .map_err(|_| anyhow!("current Unix timestamp does not fit in u64"))?)
}

#[cfg(test)]
mod tests {
    use std::{
        env, process,
        time::{SystemTime, UNIX_EPOCH},
    };

    use super::*;
    use crate::{
        artifacts::{
            EVENT_JOURNAL_FILE_NAME, PACKET_JOURNAL_FILE_NAME, PARTICIPANT_SNAPSHOT_FILE_NAME,
            PLAYOUT_JOURNAL_FILE_NAME, TRACK_MANIFEST_FILE_NAME,
        },
        journal,
        participants::ParticipantContext,
        playout::{self, PlayoutDecision, PlayoutRecord},
        session::{NewSession, SessionStore, write_event},
        track_manifest::TrackDescription,
    };

    #[test]
    fn selected_recovery_rebuilds_only_the_named_user_and_still_waits() {
        let directory = fixture("selected", &[(11, 100), (22, 200)]);

        run(&directory, &[11]).unwrap();

        let manifest = TrackManifest::read(&directory.join(TRACK_MANIFEST_FILE_NAME)).unwrap();
        assert_eq!(manifest.tracks[0].discord_user_id, "11");
        assert_eq!(manifest.tracks[0].state, TrackState::Complete);
        assert_eq!(manifest.tracks[0].source_ssrcs, [100]);
        assert!(directory.join("tracks/user-11.flac").is_file());
        assert!(!directory.join("tracks/user-11.flac.part").exists());
        assert_eq!(manifest.tracks[1].discord_user_id, "22");
        assert_eq!(manifest.tracks[1].state, TrackState::Incomplete);
        assert!(directory.join("tracks/user-22.flac.part").is_file());

        let session = SessionStore::load(&directory).unwrap();
        assert_eq!(session.record().state, WorkflowState::AwaitingOperator);
        assert!(
            session
                .record()
                .checkpoints
                .iter()
                .any(|checkpoint| { checkpoint.stage == format!("{RECOVERY_STARTED_PREFIX}11") })
        );
        assert!(
            session
                .record()
                .checkpoints
                .iter()
                .any(|checkpoint| { checkpoint.stage == format!("{RECOVERY_COMPLETED_PREFIX}11") })
        );
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn default_recovery_rebuilds_every_incomplete_user() {
        let directory = fixture("all-incomplete", &[(11, 100), (22, 200)]);

        run(&directory, &[]).unwrap();

        let manifest = TrackManifest::read(&directory.join(TRACK_MANIFEST_FILE_NAME)).unwrap();
        assert!(
            manifest
                .tracks
                .iter()
                .all(|track| track.state == TrackState::Complete)
        );
        assert!(directory.join("tracks/user-11.flac").is_file());
        assert!(directory.join("tracks/user-22.flac").is_file());
        assert_eq!(
            SessionStore::load(&directory).unwrap().record().state,
            WorkflowState::AwaitingOperator
        );
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn recovery_merges_replacement_ssrcs_into_one_user_track() {
        let directory = fixture("replacement-ssrc", &[(11, 100)]);
        let mut playout_bytes = Vec::new();
        playout::write_file_header(&mut playout_bytes).unwrap();
        let mut event_bytes = Vec::new();
        for (index, ssrc) in [100, 200].into_iter().enumerate() {
            playout::write_record(
                &mut playout_bytes,
                &PlayoutRecord {
                    tick: 10 + index as u64 * 2,
                    ssrc,
                    decision: PlayoutDecision::Loss,
                    decoded_samples: 1_920,
                },
            )
            .unwrap();
            write_event(
                &mut event_bytes,
                &SessionEvent::speaker_mapping(
                    1_000_000 + index as u64,
                    ssrc,
                    Some("11".to_owned()),
                    1,
                ),
            )
            .unwrap();
        }
        fs::write(directory.join(PLAYOUT_JOURNAL_FILE_NAME), playout_bytes).unwrap();
        fs::write(directory.join(EVENT_JOURNAL_FILE_NAME), event_bytes).unwrap();

        run(&directory, &[]).unwrap();

        let manifest = TrackManifest::read(&directory.join(TRACK_MANIFEST_FILE_NAME)).unwrap();
        assert_eq!(manifest.tracks.len(), 1);
        assert_eq!(manifest.tracks[0].source_ssrcs, [100, 200]);
        assert_eq!(manifest.tracks[0].state, TrackState::Complete);
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn healthy_tracks_are_selected_only_when_named_explicitly() {
        let manifest = TrackManifest::new(
            "session-test".to_owned(),
            vec![
                description(11, 100, TrackState::Incomplete),
                description(22, 200, TrackState::Complete),
            ],
        );

        assert_eq!(select_targets(&manifest, &[]).unwrap(), [11]);
        assert_eq!(select_targets(&manifest, &[22]).unwrap(), [22]);
    }

    #[test]
    fn completed_track_requires_an_explicit_user_id_to_rebuild() {
        let directory = fixture("explicit-healthy", &[(11, 100)]);
        run(&directory, &[]).unwrap();
        let first_bytes = fs::read(directory.join("tracks/user-11.flac")).unwrap();

        let error = run(&directory, &[]).unwrap_err();
        assert!(error.to_string().contains("no incomplete routine tracks"));
        assert_eq!(
            fs::read(directory.join("tracks/user-11.flac")).unwrap(),
            first_bytes
        );

        run(&directory, &[11]).unwrap();
        let session = SessionStore::load(&directory).unwrap();
        assert_eq!(
            session
                .record()
                .checkpoints
                .iter()
                .filter(|checkpoint| {
                    checkpoint.stage == format!("{RECOVERY_COMPLETED_PREFIX}11")
                })
                .count(),
            2
        );
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn failed_recovery_records_failure_and_leaves_incomplete_state() {
        let directory = fixture("failure", &[(11, 100)]);
        fs::remove_file(directory.join(PACKET_JOURNAL_FILE_NAME)).unwrap();

        let error = run(&directory, &[]).unwrap_err();

        assert!(error.to_string().contains("routine track recovery"));
        let session = SessionStore::load(&directory).unwrap();
        assert_eq!(session.record().state, WorkflowState::AwaitingOperator);
        assert!(
            session
                .record()
                .failures
                .iter()
                .any(|failure| failure.kind == "track_recovery")
        );
        let manifest = TrackManifest::read(&directory.join(TRACK_MANIFEST_FILE_NAME)).unwrap();
        assert_eq!(manifest.tracks[0].state, TrackState::Incomplete);
        assert!(!directory.join("tracks/user-11.flac").exists());
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn mapping_timeline_revokes_departed_users_and_resolves_late_evidence() {
        let directory = test_directory("mapping-timeline");
        let path = directory.join(EVENT_JOURNAL_FILE_NAME);
        let mut events = Vec::new();
        write_event(
            &mut events,
            &SessionEvent::speaker_mapping(100, 500, Some("11".to_owned()), 1),
        )
        .unwrap();
        write_event(
            &mut events,
            &SessionEvent::user_disconnected(200, "11".to_owned()),
        )
        .unwrap();
        write_event(
            &mut events,
            &SessionEvent::unresolved_ssrc_abandoned(
                250,
                500,
                10,
                20,
                11,
                10_560,
                "age_limit".to_owned(),
            ),
        )
        .unwrap();
        write_event(
            &mut events,
            &SessionEvent::speaker_mapping(300, 500, Some("22".to_owned()), 1),
        )
        .unwrap();
        fs::write(&path, events).unwrap();

        let timeline = MappingTimeline::read(&path).unwrap();

        assert_eq!(timeline.user_at(500, 50), Some(11));
        assert_eq!(timeline.user_at(500, 199), Some(11));
        // Frames retained after the disconnect and before late mapping belong
        // to the replacement user, matching the live pending route.
        assert_eq!(timeline.user_at(500, 200), Some(22));
        assert_eq!(timeline.user_at(500, 350), Some(22));
        assert!(timeline.unresolved_ssrcs().is_empty());
        fs::remove_dir_all(directory).unwrap();
    }

    fn fixture(label: &str, users: &[(u64, u32)]) -> PathBuf {
        let directory = test_directory(label);
        fs::create_dir(directory.join(TRACK_DIRECTORY_NAME)).unwrap();
        let participants = ParticipantContext::empty_for_test();
        let mut session = SessionStore::create(
            &directory,
            NewSession {
                session_id: directory.file_name().unwrap().to_str().unwrap(),
                started_at_unix_millis: 1_000,
                configuration_version: 1,
                guild_id: "123",
                channel_id: "456",
                participants: &participants,
            },
        )
        .unwrap();
        session
            .transition(WorkflowState::RecordedIncomplete, 2_000)
            .unwrap();
        session
            .transition(WorkflowState::AwaitingOperator, 2_001)
            .unwrap();

        let mut packet_bytes = Vec::new();
        journal::write_file_header(&mut packet_bytes).unwrap();
        fs::write(directory.join(PACKET_JOURNAL_FILE_NAME), packet_bytes).unwrap();

        let mut playout_bytes = Vec::new();
        playout::write_file_header(&mut playout_bytes).unwrap();
        let mut event_bytes = Vec::new();
        for (index, (user_id, ssrc)) in users.iter().enumerate() {
            playout::write_record(
                &mut playout_bytes,
                &PlayoutRecord {
                    tick: 10 + index as u64,
                    ssrc: *ssrc,
                    decision: PlayoutDecision::Loss,
                    // A fresh Opus PLC decoder returns its initial 40 ms
                    // concealment frame; the journal remains authoritative
                    // about that nonstandard sample count.
                    decoded_samples: 1_920,
                },
            )
            .unwrap();
            write_event(
                &mut event_bytes,
                &SessionEvent::speaker_mapping(
                    1_000_000 + index as u64,
                    *ssrc,
                    Some(user_id.to_string()),
                    1,
                ),
            )
            .unwrap();
            fs::write(
                directory.join(format!("tracks/user-{user_id}.flac.part")),
                b"old incomplete track",
            )
            .unwrap();
        }
        fs::write(directory.join(PLAYOUT_JOURNAL_FILE_NAME), playout_bytes).unwrap();
        fs::write(directory.join(EVENT_JOURNAL_FILE_NAME), event_bytes).unwrap();

        let manifest = TrackManifest::new(
            directory.file_name().unwrap().to_str().unwrap().to_owned(),
            users
                .iter()
                .map(|(user_id, ssrc)| description(*user_id, *ssrc, TrackState::Incomplete))
                .collect(),
        );
        manifest.write(&directory).unwrap();
        assert!(directory.join(PARTICIPANT_SNAPSHOT_FILE_NAME).is_file());
        directory
    }

    fn description(user_id: u64, ssrc: u32, state: TrackState) -> TrackDescription {
        TrackDescription::new(
            user_id,
            format!("User {user_id}"),
            "player".to_owned(),
            None,
            match state {
                TrackState::Complete => format!("tracks/user-{user_id}.flac"),
                TrackState::Incomplete => format!("tracks/user-{user_id}.flac.part"),
            },
            state,
            0,
            vec![ssrc],
            (state == TrackState::Incomplete).then(|| "encoder_error".to_owned()),
        )
    }

    fn test_directory(label: &str) -> PathBuf {
        let directory = env::temp_dir().join(format!(
            "echoscribe-routine-recovery-{label}-{}-{}",
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
