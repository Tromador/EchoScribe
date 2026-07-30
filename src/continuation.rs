//! Validation gate for explicit workflow continuation after operator action.
//!
//! Historical failures remain evidence. This gate asks whether their present
//! consequences have been repaired and whether authoritative evidence is still
//! healthy; it does not erase or generically mark old records “resolved”.

use std::{
    collections::HashSet,
    fs,
    path::Path,
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, anyhow, bail};

use crate::{
    operation_lease::SessionOperationLease,
    participants::ParticipantContext,
    recover,
    routine_recovery::{MappingTimeline, RECOVERY_COMPLETED_PREFIX, RECOVERY_STARTED_PREFIX},
    session::{
        FailureRecord, PREVIOUS_SESSION_FORMAT_VERSION, RECORDING_SESSION_FORMAT_VERSION,
        SessionStore, WorkflowState,
    },
    track_manifest::TrackManifest,
    verify_tracks,
};

pub(crate) fn run(session_directory: &Path) -> Result<()> {
    let session_directory = fs::canonicalize(session_directory).with_context(|| {
        format!(
            "failed to resolve session directory {}",
            session_directory.display()
        )
    })?;
    let _lease = SessionOperationLease::acquire(&session_directory)?;
    let mut session = SessionStore::load(&session_directory).with_context(|| {
        format!(
            "failed to load workflow state from {}",
            session_directory.display()
        )
    })?;
    if session.record().state != WorkflowState::AwaitingOperator
        || !matches!(
            session.record().format,
            PREVIOUS_SESSION_FORMAT_VERSION | RECORDING_SESSION_FORMAT_VERSION
        )
        || session.record().files.results.is_some()
    {
        bail!(
            "continue <session> requires a format-3 or format-4 awaiting_operator session \
             without transcription results; found format {} state {}",
            session.record().format,
            session.record().state.as_str(),
        );
    }

    let participants_path = session_directory.join(&session.record().files.participants.path);
    ParticipantContext::load(&participants_path).with_context(|| {
        format!(
            "failed to validate participant snapshot {}",
            participants_path.display()
        )
    })?;

    let manifest_path = session_directory.join(&session.record().files.tracks.path);
    let manifest = TrackManifest::read(&manifest_path)
        .with_context(|| format!("failed to read track manifest {}", manifest_path.display()))?;
    if manifest.session_id != session.record().session_id {
        bail!(
            "track manifest session ID {:?} does not match session.json ID {:?}",
            manifest.session_id,
            session.record().session_id
        );
    }

    let recovery = RecoveryEvidence::from_session(&session)?;
    recovery.validate_complete_attempts()?;

    let timeline =
        MappingTimeline::read(&session_directory.join(&session.record().files.events.path))?;
    if !timeline.unresolved_ssrcs().is_empty() {
        let mut unresolved = timeline
            .unresolved_ssrcs()
            .iter()
            .copied()
            .collect::<Vec<_>>();
        unresolved.sort_unstable();
        bail!(
            "cannot continue while SSRC mapping evidence remains unresolved for {}",
            unresolved
                .iter()
                .map(u32::to_string)
                .collect::<Vec<_>>()
                .join(", ")
        );
    }

    let mut replay_users = HashSet::new();
    let mut unattributed_ssrcs = HashSet::new();
    let replay =
        recover::replay_session_files(&session_directory, &session.record().files, |frame| {
            match timeline.user_at(frame.ssrc, frame.elapsed_nanos) {
                Some(user_id) => {
                    replay_users.insert(user_id);
                }
                None => {
                    unattributed_ssrcs.insert(frame.ssrc);
                }
            }
            Ok(())
        })
        .context("authoritative packet/playout journal validation failed")?;
    if replay.truncated_packet_tail || replay.truncated_playout_tail {
        bail!("cannot continue with a truncated authoritative journal tail");
    }
    if replay.skipped_undecoded > 0 {
        bail!(
            "cannot continue: {} playout decisions lack decoded-sample evidence",
            replay.skipped_undecoded
        );
    }
    if !unattributed_ssrcs.is_empty() {
        let mut ssrcs = unattributed_ssrcs.into_iter().collect::<Vec<_>>();
        ssrcs.sort_unstable();
        bail!(
            "cannot continue: decoded PCM cannot be attributed safely for SSRCs {}",
            ssrcs
                .iter()
                .map(u32::to_string)
                .collect::<Vec<_>>()
                .join(", ")
        );
    }

    // The derived manifest is not evidence that it lists every speaker.
    // Authoritative replay supplies the required user set.
    let complete_manifest_users = manifest
        .tracks
        .iter()
        .filter(|track| track.state == crate::track_manifest::TrackState::Complete)
        .map(|track| {
            track
                .discord_user_id
                .parse::<u64>()
                .expect("validated manifest contains numeric Discord IDs")
        })
        .collect::<HashSet<_>>();
    let mut missing_users = replay_users
        .difference(&complete_manifest_users)
        .copied()
        .collect::<Vec<_>>();
    missing_users.sort_unstable();
    if !missing_users.is_empty() {
        bail!(
            "cannot continue: attributable PCM has no complete routine track for Discord users {}",
            missing_users
                .iter()
                .map(u64::to_string)
                .collect::<Vec<_>>()
                .join(", ")
        );
    }

    validate_historical_failures(session.record().failures.as_slice(), &manifest, &recovery)?;
    let verified_tracks = verify_tracks::verify_complete_manifest(&session_directory, &manifest)?;

    let now = unix_millis_now()?;
    session.record_checkpoint(now, "recording_continuation_validated")?;
    if let Err(error) = session.transition(WorkflowState::ReadyForTranscription, now) {
        let message =
            format!("failed to publish ready_for_transcription after validation: {error}");
        let _ = session.record_failure(unix_millis_now()?, "recording_continuation", &message);
        return Err(anyhow!(message));
    }
    println!(
        "Validated {verified_tracks} routine track(s); session {} is ready for transcription.",
        session.record().session_id
    );
    Ok(())
}

#[derive(Default)]
struct RecoveryEvidence {
    started_users: HashSet<u64>,
    completed_users: HashSet<u64>,
}

impl RecoveryEvidence {
    fn from_session(session: &SessionStore) -> Result<Self> {
        let mut evidence = Self::default();
        for checkpoint in &session.record().checkpoints {
            if let Some(value) = checkpoint.stage.strip_prefix(RECOVERY_STARTED_PREFIX) {
                evidence.started_users.insert(parse_checkpoint_user(value)?);
            }
            if let Some(value) = checkpoint.stage.strip_prefix(RECOVERY_COMPLETED_PREFIX) {
                evidence
                    .completed_users
                    .insert(parse_checkpoint_user(value)?);
            }
        }
        Ok(evidence)
    }

    fn validate_complete_attempts(&self) -> Result<()> {
        let mut unfinished = self
            .started_users
            .difference(&self.completed_users)
            .copied()
            .collect::<Vec<_>>();
        unfinished.sort_unstable();
        if !unfinished.is_empty() {
            bail!(
                "recovery has no durable successful result for Discord users {}",
                unfinished
                    .iter()
                    .map(u64::to_string)
                    .collect::<Vec<_>>()
                    .join(", ")
            );
        }
        Ok(())
    }
}

fn validate_historical_failures(
    failures: &[FailureRecord],
    manifest: &TrackManifest,
    recovery: &RecoveryEvidence,
) -> Result<()> {
    let mut derived_track_fault = false;
    let mut all_tracks_must_be_recovered = false;

    for failure in failures {
        match failure.kind.as_str() {
            "capture_consumer" | "capture_consumer_task" | "capture_authoritative_drop" => {
                bail!(
                    "authoritative recording fault remains unresolved: {}",
                    failure.message
                )
            }
            "capture_queue_drop" => match legacy_capture_drop_counts(&failure.message) {
                Some((total, audio)) if total == audio && audio > 0 => {
                    derived_track_fault = true;
                    all_tracks_must_be_recovered = true;
                }
                _ => bail!(
                    "capture queue loss may include authoritative records and remains unresolved: {}",
                    failure.message
                ),
            },
            "capture_audio_drop" => {
                derived_track_fault = true;
                all_tracks_must_be_recovered = true;
            }
            "live_flac_encoder"
            | "live_flac_queue_full"
            | "live_flac_queue_closed"
            | "live_flac_finalization"
            | "live_track_incomplete"
            | "unresolved_ssrc" => derived_track_fault = true,
            _ => {}
        }
    }

    if derived_track_fault && recovery.completed_users.is_empty() {
        bail!("derived track failures remain without a durable successful recovery result");
    }
    if all_tracks_must_be_recovered {
        let unrecovered = manifest
            .tracks
            .iter()
            .filter_map(|track| {
                let user_id = track
                    .discord_user_id
                    .parse::<u64>()
                    .expect("validated manifest contains numeric Discord IDs");
                (!recovery.completed_users.contains(&user_id)).then_some(user_id)
            })
            .collect::<Vec<_>>();
        if !unrecovered.is_empty() {
            bail!(
                "decoded-audio ingress loss requires recovery of every routine track; missing users {}",
                unrecovered
                    .iter()
                    .map(u64::to_string)
                    .collect::<Vec<_>>()
                    .join(", ")
            );
        }
    }
    Ok(())
}

fn legacy_capture_drop_counts(message: &str) -> Option<(u64, u64)> {
    // Slice 5 recorded one stable aggregate sentence. Parsing it preserves
    // compatibility while newer code may use more specific failure kinds.
    let after_rejected = message.strip_prefix("capture queue rejected ")?;
    let (full, after_full) = after_rejected.split_once(" records while full and ")?;
    let (closed, after_closed) = after_full.split_once(" records after closure, including ")?;
    let (audio, suffix) = after_closed.split_once(" decoded-audio records")?;
    if !suffix.is_empty() {
        return None;
    }
    Some((
        full.parse::<u64>()
            .ok()?
            .checked_add(closed.parse().ok()?)?,
        audio.parse().ok()?,
    ))
}

fn parse_checkpoint_user(value: &str) -> Result<u64> {
    value
        .parse::<u64>()
        .ok()
        .filter(|user_id| *user_id != 0)
        .ok_or_else(|| anyhow!("invalid recovery checkpoint Discord user ID {value:?}"))
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
        env, fs, process,
        time::{SystemTime, UNIX_EPOCH},
    };

    use super::*;
    use crate::{
        artifacts::{
            EVENT_JOURNAL_FILE_NAME, PACKET_JOURNAL_FILE_NAME, PLAYOUT_JOURNAL_FILE_NAME,
            TRACK_DIRECTORY_NAME,
        },
        diagnostics::SAMPLES_PER_TICK,
        journal,
        participants::ParticipantContext,
        playout::{self, PlayoutDecision, PlayoutRecord},
        routine_recovery,
        session::{NewSession, SessionEvent, fail_record_write_after, write_event},
        track_manifest::{TrackDescription, TrackState},
    };

    #[test]
    fn continue_refuses_while_a_required_track_is_incomplete() {
        let directory = fixture("incomplete");

        let error = run(&directory).unwrap_err();

        assert!(
            error
                .to_string()
                .contains("no complete routine track for Discord users 11")
        );
        assert_eq!(
            SessionStore::load(&directory).unwrap().record().state,
            WorkflowState::AwaitingOperator
        );
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn recording_continuation_cannot_publish_while_session_lease_is_owned() {
        let directory = fixture("operation-lease");
        let lease = SessionOperationLease::acquire(&directory).unwrap();
        let session_before = fs::read(directory.join("session.json")).unwrap();
        let manifest_before =
            fs::read(directory.join(crate::artifacts::TRACK_MANIFEST_FILE_NAME)).unwrap();

        let error = run(&directory).unwrap_err();

        assert!(
            error
                .to_string()
                .contains("another mutating operation is already active")
        );
        assert_eq!(
            fs::read(directory.join("session.json")).unwrap(),
            session_before
        );
        assert_eq!(
            fs::read(directory.join(crate::artifacts::TRACK_MANIFEST_FILE_NAME)).unwrap(),
            manifest_before
        );
        drop(lease);
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn continue_accepts_a_healthy_recovered_recording() {
        let directory = fixture("healthy-recovered");
        routine_recovery::run(&directory, &[]).unwrap();

        run(&directory).unwrap();

        let session = SessionStore::load(&directory).unwrap();
        assert_eq!(session.record().state, WorkflowState::ReadyForTranscription);
        assert!(
            session
                .record()
                .checkpoints
                .iter()
                .any(|checkpoint| checkpoint.stage == "recording_continuation_validated")
        );
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn authoritative_capture_loss_still_blocks_after_track_recovery() {
        let directory = fixture("authoritative-loss");
        routine_recovery::run(&directory, &[]).unwrap();
        let mut session = SessionStore::load(&directory).unwrap();
        session
            .record_failure(
                3_000,
                "capture_queue_drop",
                "capture queue rejected 2 records while full and 0 records after closure, including 1 decoded-audio records",
            )
            .unwrap();

        let error = run(&directory).unwrap_err();

        assert!(error.to_string().contains("authoritative records"));
        assert_eq!(
            SessionStore::load(&directory).unwrap().record().state,
            WorkflowState::AwaitingOperator
        );
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn audio_only_capture_loss_requires_every_track_to_be_recovered() {
        let directory = fixture("audio-loss");
        routine_recovery::run(&directory, &[]).unwrap();
        let mut session = SessionStore::load(&directory).unwrap();
        session
            .record_failure(
                3_000,
                "capture_queue_drop",
                "capture queue rejected 1 records while full and 0 records after closure, including 1 decoded-audio records",
            )
            .unwrap();

        run(&directory).unwrap();

        assert_eq!(
            SessionStore::load(&directory).unwrap().record().state,
            WorkflowState::ReadyForTranscription
        );
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn failed_ready_publication_remains_awaiting_operator() {
        let directory = fixture("ready-persistence-failure");
        routine_recovery::run(&directory, &[]).unwrap();
        // Validation checkpoint persists, then the state transition receives
        // the one-shot failure.
        fail_record_write_after(&directory, 1);

        let error = run(&directory).unwrap_err();

        assert!(error.to_string().contains("ready_for_transcription"));
        let session = SessionStore::load(&directory).unwrap();
        assert_eq!(session.record().state, WorkflowState::AwaitingOperator);
        assert!(
            session
                .record()
                .failures
                .iter()
                .any(|failure| failure.kind == "recording_continuation")
        );
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn continuation_requires_manifest_coverage_for_every_replayed_user() {
        let directory = fixture("manifest-user-coverage");
        routine_recovery::run(&directory, &[]).unwrap();

        let mut playout_bytes = Vec::new();
        playout::write_file_header(&mut playout_bytes).unwrap();
        let mut event_bytes = Vec::new();
        for (index, (user_id, ssrc)) in [(11, 100), (22, 200)].into_iter().enumerate() {
            playout::write_record(
                &mut playout_bytes,
                &PlayoutRecord {
                    tick: 10 + index as u64,
                    ssrc,
                    decision: PlayoutDecision::Loss,
                    decoded_samples: SAMPLES_PER_TICK as u32,
                },
            )
            .unwrap();
            write_event(
                &mut event_bytes,
                &SessionEvent::speaker_mapping(
                    1_000_000 + index as u64,
                    ssrc,
                    Some(user_id.to_string()),
                    1,
                ),
            )
            .unwrap();
        }
        fs::write(directory.join(PLAYOUT_JOURNAL_FILE_NAME), playout_bytes).unwrap();
        fs::write(directory.join(EVENT_JOURNAL_FILE_NAME), event_bytes).unwrap();

        let error = run(&directory).unwrap_err();

        assert!(
            error
                .to_string()
                .contains("no complete routine track for Discord users 22")
        );
        assert_eq!(
            SessionStore::load(&directory).unwrap().record().state,
            WorkflowState::AwaitingOperator
        );
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn continuation_refuses_unattributed_replayed_pcm() {
        let directory = fixture("unattributed-pcm");
        routine_recovery::run(&directory, &[]).unwrap();
        let mut playout_bytes = Vec::new();
        playout::write_file_header(&mut playout_bytes).unwrap();
        playout::write_record(
            &mut playout_bytes,
            &PlayoutRecord {
                tick: 10,
                ssrc: 999,
                decision: PlayoutDecision::Loss,
                decoded_samples: SAMPLES_PER_TICK as u32,
            },
        )
        .unwrap();
        fs::write(directory.join(PLAYOUT_JOURNAL_FILE_NAME), playout_bytes).unwrap();

        let error = run(&directory).unwrap_err();

        assert!(
            error
                .to_string()
                .contains("attributed safely for SSRCs 999")
        );
        assert_eq!(
            SessionStore::load(&directory).unwrap().record().state,
            WorkflowState::AwaitingOperator
        );
        fs::remove_dir_all(directory).unwrap();
    }

    fn fixture(label: &str) -> std::path::PathBuf {
        let directory = env::temp_dir().join(format!(
            "echoscribe-continuation-{label}-{}-{}",
            process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir(&directory).unwrap();
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
        playout::write_record(
            &mut playout_bytes,
            &PlayoutRecord {
                tick: 10,
                ssrc: 100,
                decision: PlayoutDecision::Loss,
                decoded_samples: SAMPLES_PER_TICK as u32,
            },
        )
        .unwrap();
        fs::write(directory.join(PLAYOUT_JOURNAL_FILE_NAME), playout_bytes).unwrap();

        let mut event_bytes = Vec::new();
        write_event(
            &mut event_bytes,
            &SessionEvent::speaker_mapping(1_000_000, 100, Some("11".to_owned()), 1),
        )
        .unwrap();
        fs::write(directory.join(EVENT_JOURNAL_FILE_NAME), event_bytes).unwrap();
        fs::write(
            directory.join("tracks/user-11.flac.part"),
            b"old incomplete track",
        )
        .unwrap();

        TrackManifest::new(
            directory.file_name().unwrap().to_str().unwrap().to_owned(),
            vec![TrackDescription::new(
                11,
                "User 11".to_owned(),
                "player".to_owned(),
                None,
                "tracks/user-11.flac.part".to_owned(),
                TrackState::Incomplete,
                SAMPLES_PER_TICK,
                vec![100],
                Some("encoder_error".to_owned()),
            )],
        )
        .write(&directory)
        .unwrap();
        directory
    }
}
