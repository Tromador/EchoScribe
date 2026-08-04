//! Complete, atomic replacement transcription for a healthy completed session.
//!
//! A replacement is built in an unreferenced generation directory. Only a
//! final atomic `session.json` replacement makes the manifest, results and
//! readable transcript authoritative together.

use std::{
    fs::{self, File},
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, anyhow, bail};

use crate::{
    artifacts::{
        PARTIAL_TRANSCRIPT_FILE_NAME, TRANSCRIPTION_RESULTS_FILE_NAME, WORK_ITEM_MANIFEST_FILE_NAME,
    },
    config::SegmentationConfig,
    operation_lease::SessionOperationLease,
    session::SessionStore,
    transcription::{
        PreparedTranscription, TranscriptionResult, run_replacement_worker,
        validate_complete_transcription_authority, write_replacement_transcript,
    },
    work_items::{WorkItem, build_retranscription_items, write_retranscription_manifest},
};

const RETRANSCRIPTION_DIRECTORY: &str = "transcription/retranscriptions";
const FINAL_TRANSCRIPT_FILE_NAME: &str = "transcript.txt";

trait ReplacementTranscriber {
    #[allow(clippy::too_many_arguments)]
    fn transcribe(
        &self,
        session_directory: &Path,
        prepared: &PreparedTranscription,
        manifest_path: &Path,
        results_path: &Path,
        partial_transcript_path: &Path,
        work_items: &[WorkItem],
        lease: &SessionOperationLease,
    ) -> Result<Vec<TranscriptionResult>>;
}

struct SystemTranscriber;

impl ReplacementTranscriber for SystemTranscriber {
    fn transcribe(
        &self,
        session_directory: &Path,
        prepared: &PreparedTranscription,
        manifest_path: &Path,
        results_path: &Path,
        partial_transcript_path: &Path,
        work_items: &[WorkItem],
        lease: &SessionOperationLease,
    ) -> Result<Vec<TranscriptionResult>> {
        run_replacement_worker(
            session_directory,
            prepared,
            manifest_path,
            results_path,
            partial_transcript_path,
            work_items,
            lease,
        )
    }
}

pub(crate) fn run(session_directory: &Path, config_path: &Path) -> Result<()> {
    run_with_transcriber(session_directory, config_path, &SystemTranscriber)
}

fn run_with_transcriber(
    session_directory: &Path,
    config_path: &Path,
    transcriber: &dyn ReplacementTranscriber,
) -> Result<()> {
    let prepared = PreparedTranscription::load(config_path)?;
    let merge_gap_ms = SegmentationConfig::load_merge_gap_ms(config_path)?;
    let session_directory = fs::canonicalize(session_directory).with_context(|| {
        format!(
            "failed to resolve session directory {}",
            session_directory.display()
        )
    })?;
    let lease = SessionOperationLease::acquire(&session_directory)?;

    let result = run_owned(
        &session_directory,
        merge_gap_ms,
        &prepared,
        transcriber,
        &lease,
    );
    result.map_err(|error| {
        error.context(
            "retranscription failed; the previous complete transcription remains authoritative",
        )
    })
}

fn run_owned(
    session_directory: &Path,
    merge_gap_ms: u64,
    prepared: &PreparedTranscription,
    transcriber: &dyn ReplacementTranscriber,
    lease: &SessionOperationLease,
) -> Result<()> {
    let mut session = SessionStore::load(session_directory).with_context(|| {
        format!(
            "failed to load workflow state from {}",
            session_directory.display()
        )
    })?;
    validate_complete_transcription_authority(session_directory, session.record())?;

    // This invokes the same journal, mapping, participant snapshot, track and
    // range validation used by ordinary work-manifest generation.
    let work_items =
        build_retranscription_items(session_directory, session.record(), merge_gap_ms)?;

    let generation = next_generation_directory(session_directory)?;
    fs::create_dir_all(&generation).with_context(|| {
        format!(
            "failed to create retranscription generation {}",
            generation.display()
        )
    })?;
    synchronise_generation_parents(session_directory, &generation)?;

    let manifest_path = generation.join(WORK_ITEM_MANIFEST_FILE_NAME);
    let results_path = generation.join(TRANSCRIPTION_RESULTS_FILE_NAME);
    let partial_path = generation.join(PARTIAL_TRANSCRIPT_FILE_NAME);
    let transcript_path = generation.join(FINAL_TRANSCRIPT_FILE_NAME);
    write_retranscription_manifest(&manifest_path, &work_items)?;

    let results = transcriber.transcribe(
        session_directory,
        prepared,
        &manifest_path,
        &results_path,
        &partial_path,
        &work_items,
        lease,
    )?;
    write_replacement_transcript(&transcript_path, &results)?;
    if partial_path.exists() {
        fs::remove_file(&partial_path).with_context(|| {
            format!(
                "failed to remove completed staging transcript {}",
                partial_path.display()
            )
        })?;
    }
    File::open(&generation)?.sync_all()?;

    let work_items_relative = relative_path(session_directory, &manifest_path)?;
    let results_relative = relative_path(session_directory, &results_path)?;
    let transcript_relative = relative_path(session_directory, &transcript_path)?;
    session
        .publish_retranscription_complete(
            unix_millis_now()?,
            work_items_relative,
            results_relative,
            transcript_relative,
        )
        .context("staged retranscription was complete but session authority was not replaced")?;

    println!(
        "Retranscription completed for session {}; published {} work item(s) from {}.",
        session.record().session_id,
        work_items.len(),
        generation.display()
    );
    Ok(())
}

fn next_generation_directory(session_directory: &Path) -> Result<PathBuf> {
    let root = session_directory.join(RETRANSCRIPTION_DIRECTORY);
    fs::create_dir_all(&root).with_context(|| format!("failed to create {}", root.display()))?;
    let metadata = fs::symlink_metadata(&root)
        .with_context(|| format!("failed to inspect {}", root.display()))?;
    if !metadata.is_dir() || metadata.file_type().is_symlink() {
        bail!(
            "retranscription generation root {} is not a real directory",
            root.display()
        );
    }
    let canonical_root =
        fs::canonicalize(&root).with_context(|| format!("failed to resolve {}", root.display()))?;
    if !canonical_root.starts_with(session_directory) {
        bail!(
            "retranscription generation root {} escapes the session directory",
            canonical_root.display()
        );
    }
    let mut maximum = 0_u64;
    for entry in
        fs::read_dir(&root).with_context(|| format!("failed to inspect {}", root.display()))?
    {
        let entry = entry?;
        let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
            continue;
        };
        if let Ok(value) = name.parse::<u64>() {
            maximum = maximum.max(value);
        }
    }
    let next = maximum
        .checked_add(1)
        .ok_or_else(|| anyhow!("retranscription generation number overflow"))?;
    Ok(root.join(format!("{next:06}")))
}

fn synchronise_generation_parents(session_directory: &Path, generation: &Path) -> Result<()> {
    let generation_root = generation
        .parent()
        .ok_or_else(|| anyhow!("generation path has no parent"))?;
    let transcription_directory = generation_root
        .parent()
        .ok_or_else(|| anyhow!("generation root has no parent"))?;
    File::open(generation_root)?.sync_all()?;
    File::open(transcription_directory)?.sync_all()?;
    File::open(session_directory)?.sync_all()?;
    Ok(())
}

fn relative_path(session_directory: &Path, path: &Path) -> Result<String> {
    path.strip_prefix(session_directory)
        .with_context(|| {
            format!(
                "staged path {} is outside session {}",
                path.display(),
                session_directory.display()
            )
        })?
        .to_str()
        .map(str::to_owned)
        .ok_or_else(|| anyhow!("staged path is not valid UTF-8"))
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
        env,
        fs::OpenOptions,
        io::Write,
        process,
        sync::{Arc, Mutex},
        time::{SystemTime, UNIX_EPOCH},
    };

    use super::*;
    use crate::{
        artifacts::{
            EVENT_JOURNAL_FILE_NAME, FINAL_TRANSCRIPT_PATH, PACKET_JOURNAL_FILE_NAME,
            PLAYOUT_JOURNAL_FILE_NAME, TRACK_DIRECTORY_NAME, TRANSCRIPTION_RESULTS_PATH,
            WORK_ITEM_MANIFEST_PATH,
        },
        diagnostics::SAMPLES_PER_TICK,
        journal,
        participants::ParticipantContext,
        playout::{self, PlayoutDecision, PlayoutRecord},
        session::{
            NewSession, RETRANSCRIPTION_SESSION_FORMAT_VERSION, SessionEvent, WorkflowState,
            fail_record_write_after, write_event,
        },
        track_manifest::{TrackDescription, TrackManifest, TrackState},
    };

    struct FakeTranscriber {
        calls: Arc<Mutex<usize>>,
        fail: bool,
    }

    impl FakeTranscriber {
        fn succeeding() -> Self {
            Self {
                calls: Arc::new(Mutex::new(0)),
                fail: false,
            }
        }

        fn failing() -> Self {
            Self {
                calls: Arc::new(Mutex::new(0)),
                fail: true,
            }
        }
    }

    impl ReplacementTranscriber for FakeTranscriber {
        fn transcribe(
            &self,
            _session_directory: &Path,
            _prepared: &PreparedTranscription,
            _manifest_path: &Path,
            results_path: &Path,
            partial_transcript_path: &Path,
            work_items: &[WorkItem],
            _lease: &SessionOperationLease,
        ) -> Result<Vec<TranscriptionResult>> {
            *self.calls.lock().unwrap() += 1;
            if self.fail {
                bail!("injected worker failure");
            }
            assert_eq!(work_items.first().map(|item| item.sequence), Some(1));
            assert!(
                work_items
                    .iter()
                    .enumerate()
                    .all(|(index, item)| item.sequence == u64::try_from(index + 1).unwrap())
            );

            let results = work_items
                .iter()
                .map(|item| result_for(item, format!("replacement {}", item.sequence)))
                .collect::<Vec<_>>();
            let mut file = OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(results_path)?;
            for result in &results {
                serde_json::to_writer(&mut file, result)?;
                file.write_all(b"\n")?;
            }
            file.sync_all()?;
            fs::write(partial_transcript_path, b"worker partial\n")?;
            Ok(results)
        }
    }

    struct MalformedStagingTranscriber;

    impl ReplacementTranscriber for MalformedStagingTranscriber {
        fn transcribe(
            &self,
            _session_directory: &Path,
            _prepared: &PreparedTranscription,
            _manifest_path: &Path,
            results_path: &Path,
            _partial_transcript_path: &Path,
            _work_items: &[WorkItem],
            _lease: &SessionOperationLease,
        ) -> Result<Vec<TranscriptionResult>> {
            fs::write(results_path, b"{malformed staged result}\n")?;
            bail!("staged result validation failed")
        }
    }

    #[test]
    fn successful_retranscription_atomically_publishes_one_complete_generation() {
        let (directory, config_path) = complete_fixture("success");
        let old_session = fs::read(directory.join("session.json")).unwrap();
        let old_manifest = fs::read(directory.join(WORK_ITEM_MANIFEST_PATH)).unwrap();
        let old_results = fs::read(directory.join(TRANSCRIPTION_RESULTS_PATH)).unwrap();
        let old_transcript = fs::read(directory.join(FINAL_TRANSCRIPT_PATH)).unwrap();
        let transcriber = FakeTranscriber::succeeding();

        run_with_transcriber(&directory, &config_path, &transcriber).unwrap();

        assert_eq!(*transcriber.calls.lock().unwrap(), 1);
        let session = SessionStore::load(&directory).unwrap();
        assert_eq!(
            session.record().format,
            RETRANSCRIPTION_SESSION_FORMAT_VERSION
        );
        assert_eq!(session.record().state, WorkflowState::Complete);
        let work_path = &session.record().files.work_items.as_ref().unwrap().path;
        let results_path = &session.record().files.results.as_ref().unwrap().path;
        let transcript_path = &session.record().files.transcript.as_ref().unwrap().path;
        assert_eq!(
            Path::new(work_path).parent(),
            Path::new(results_path).parent()
        );
        assert_eq!(
            Path::new(work_path).parent(),
            Path::new(transcript_path).parent()
        );
        let items = read_items(&fs::read(directory.join(work_path)).unwrap());
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].discord_user_id, "11");
        assert_eq!(items[0].sequence, 1);
        assert_eq!(
            fs::read_to_string(directory.join(transcript_path)).unwrap(),
            "[00:00:00] Alice: replacement 1\n"
        );
        assert!(
            !directory
                .join(Path::new(transcript_path).parent().unwrap())
                .join(PARTIAL_TRANSCRIPT_FILE_NAME)
                .exists()
        );

        // Publication switches authority; it does not truncate or overwrite
        // the previous complete generation.
        assert_ne!(
            fs::read(directory.join("session.json")).unwrap(),
            old_session
        );
        assert_eq!(
            fs::read(directory.join(WORK_ITEM_MANIFEST_PATH)).unwrap(),
            old_manifest
        );
        assert_eq!(
            fs::read(directory.join(TRANSCRIPTION_RESULTS_PATH)).unwrap(),
            old_results
        );
        assert_eq!(
            fs::read(directory.join(FINAL_TRANSCRIPT_PATH)).unwrap(),
            old_transcript
        );
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn worker_failure_preserves_previous_authority_and_complete_state() {
        let (directory, config_path) = complete_fixture("worker-failure");
        let before = authority_bytes(&directory);

        let error = run_with_transcriber(&directory, &config_path, &FakeTranscriber::failing())
            .unwrap_err();

        assert!(
            error
                .to_string()
                .contains("previous complete transcription")
        );
        assert_eq!(authority_bytes(&directory), before);
        assert_eq!(
            SessionStore::load(&directory).unwrap().record().state,
            WorkflowState::Complete
        );
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn malformed_staged_results_do_not_replace_existing_authority() {
        let (directory, config_path) = complete_fixture("malformed-stage");
        let before = authority_bytes(&directory);

        let error = run_with_transcriber(&directory, &config_path, &MalformedStagingTranscriber)
            .unwrap_err();

        assert!(format!("{error:#}").contains("staged result validation failed"));
        assert_eq!(authority_bytes(&directory), before);
        assert_eq!(
            SessionStore::load(&directory).unwrap().record().state,
            WorkflowState::Complete
        );
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn failed_session_publication_cannot_mix_old_and_new_authority() {
        let (directory, config_path) = complete_fixture("publish-failure");
        let before = authority_bytes(&directory);
        fail_record_write_after(&directory, 0);

        let error = run_with_transcriber(&directory, &config_path, &FakeTranscriber::succeeding())
            .unwrap_err();

        assert!(format!("{error:#}").contains("session authority was not replaced"));
        assert_eq!(authority_bytes(&directory), before);
        assert_eq!(
            SessionStore::load(&directory).unwrap().record().state,
            WorkflowState::Complete
        );
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn non_complete_entry_is_refused_without_mutation_or_worker() {
        let (directory, config_path) = complete_fixture("non-complete");
        let mut record = SessionStore::load(&directory).unwrap().record().clone();
        record.state = WorkflowState::Transcribing;
        fs::write(
            directory.join("session.json"),
            serde_json::to_vec_pretty(&record).unwrap(),
        )
        .unwrap();
        let before = authority_bytes(&directory);
        let transcriber = FakeTranscriber::succeeding();

        let error = run_with_transcriber(&directory, &config_path, &transcriber).unwrap_err();

        assert!(format!("{error:#}").contains("requires a complete session"));
        assert_eq!(*transcriber.calls.lock().unwrap(), 0);
        assert_eq!(authority_bytes(&directory), before);
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn active_session_lease_refuses_retranscription_before_mutation() {
        let (directory, config_path) = complete_fixture("lease");
        let lease = SessionOperationLease::acquire(&directory).unwrap();
        let before = authority_bytes(&directory);

        let error = run_with_transcriber(&directory, &config_path, &FakeTranscriber::succeeding())
            .unwrap_err();

        assert!(error.to_string().contains("another mutating operation"));
        assert_eq!(authority_bytes(&directory), before);
        drop(lease);
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn repeated_retranscription_is_safe_and_rebuilds_from_sequence_one() {
        let (directory, config_path) = complete_fixture("repeated");

        run_with_transcriber(&directory, &config_path, &FakeTranscriber::succeeding()).unwrap();
        let first = SessionStore::load(&directory).unwrap();
        let first_path = first
            .record()
            .files
            .work_items
            .as_ref()
            .unwrap()
            .path
            .clone();
        let first_bytes = fs::read(directory.join(&first_path)).unwrap();

        run_with_transcriber(&directory, &config_path, &FakeTranscriber::succeeding()).unwrap();
        let second = SessionStore::load(&directory).unwrap();
        let second_path = second
            .record()
            .files
            .work_items
            .as_ref()
            .unwrap()
            .path
            .clone();

        assert_ne!(first_path, second_path);
        assert_eq!(first_bytes, fs::read(directory.join(second_path)).unwrap());
        assert_eq!(read_items(&first_bytes)[0].sequence, 1);
        fs::remove_dir_all(directory).unwrap();
    }

    fn complete_fixture(label: &str) -> (PathBuf, PathBuf) {
        let directory = test_directory(label);
        let participant_source = directory.join("source-participants.toml");
        fs::write(
            &participant_source,
            concat!(
                "version = 1\n",
                "[participants.\"11\"]\n",
                "character = \"Included\"\n",
                "role = \"gm\"\n",
                "[participants.\"22\"]\n",
                "transcribe = false\n",
            ),
        )
        .unwrap();
        let participants = ParticipantContext::load(&participant_source).unwrap();
        let mut session = SessionStore::create(
            &directory,
            NewSession {
                session_id: "session-retranscription",
                started_at_unix_millis: 1_000,
                configuration_version: 1,
                guild_id: "123",
                channel_id: "456",
                participants: &participants,
            },
        )
        .unwrap();
        session
            .transition(WorkflowState::RecordedClean, 2_000)
            .unwrap();
        session
            .transition(WorkflowState::ReadyForTranscription, 2_100)
            .unwrap();

        let mut packets = File::create(directory.join(PACKET_JOURNAL_FILE_NAME)).unwrap();
        journal::write_file_header(&mut packets).unwrap();
        packets.sync_all().unwrap();
        let mut playout_file = File::create(directory.join(PLAYOUT_JOURNAL_FILE_NAME)).unwrap();
        playout::write_file_header(&mut playout_file).unwrap();
        for (tick, ssrc) in [(10, 100), (20, 200)] {
            playout::write_record(
                &mut playout_file,
                &PlayoutRecord {
                    tick,
                    ssrc,
                    decision: PlayoutDecision::Loss,
                    decoded_samples: u32::try_from(SAMPLES_PER_TICK).unwrap(),
                },
            )
            .unwrap();
        }
        playout_file.sync_all().unwrap();
        let mut events = File::create(directory.join(EVENT_JOURNAL_FILE_NAME)).unwrap();
        for (ssrc, user_id) in [(100, "11"), (200, "22")] {
            write_event(
                &mut events,
                &SessionEvent::speaker_mapping(0, ssrc, Some(user_id.to_owned()), 1),
            )
            .unwrap();
        }
        events.sync_all().unwrap();

        let tracks_directory = directory.join(TRACK_DIRECTORY_NAME);
        fs::create_dir(&tracks_directory).unwrap();
        fs::write(tracks_directory.join("user-11.flac"), b"complete").unwrap();
        fs::write(tracks_directory.join("user-22.flac"), b"complete").unwrap();
        TrackManifest::new(
            "session-retranscription".to_owned(),
            vec![
                TrackDescription::new(
                    11,
                    "Alice".to_owned(),
                    "player".to_owned(),
                    None,
                    "tracks/user-11.flac".to_owned(),
                    TrackState::Complete,
                    11 * SAMPLES_PER_TICK,
                    vec![100],
                    None,
                ),
                TrackDescription::new(
                    22,
                    "Astra".to_owned(),
                    "player".to_owned(),
                    None,
                    "tracks/user-22.flac".to_owned(),
                    TrackState::Complete,
                    21 * SAMPLES_PER_TICK,
                    vec![200],
                    None,
                ),
            ],
        )
        .write(&directory)
        .unwrap();

        let old_items = build_retranscription_items(&directory, session.record(), 750).unwrap();
        write_retranscription_manifest(&directory.join(WORK_ITEM_MANIFEST_PATH), &old_items)
            .unwrap();
        session.publish_work_manifest(3_000).unwrap();
        let old_results = old_items
            .iter()
            .map(|item| result_for(item, "old transcript".to_owned()))
            .collect::<Vec<_>>();
        let mut results_file = File::create(directory.join(TRANSCRIPTION_RESULTS_PATH)).unwrap();
        for result in &old_results {
            serde_json::to_writer(&mut results_file, result).unwrap();
            results_file.write_all(b"\n").unwrap();
        }
        results_file.sync_all().unwrap();
        session.publish_transcription_start(3_100).unwrap();
        write_replacement_transcript(&directory.join(FINAL_TRANSCRIPT_PATH), &old_results).unwrap();
        session.publish_transcription_complete(3_200).unwrap();

        let config_path = directory.join("echoscribe.toml");
        fs::write(
            &config_path,
            concat!(
                "version = 1\n",
                "[discord]\n",
                "token = \"unused\"\n",
                "guild_id = \"bad-offline\"\n",
                "channel_id = \"bad-offline\"\n",
                "[recording]\n",
                "output_directory = \"recordings\"\n",
                "[participants]\n",
                "file = \"missing.toml\"\n",
                "[transcription]\n",
                "model = \"model\"\n",
                "language = \"en\"\n",
                "device = \"cpu\"\n",
                "compute_type = \"int8\"\n",
                "beam_size = 5\n",
                "vocabulary_file = \"missing-vocabulary.txt\"\n",
                "resume_rewind_seconds = 120\n",
                "lexical_no_speech_threshold = 0.60\n",
                "[segmentation]\n",
                "vad_enabled = true\n",
                "merge_gap_ms = 750\n",
            ),
        )
        .unwrap();
        (directory, config_path)
    }

    fn result_for(item: &WorkItem, text: String) -> TranscriptionResult {
        TranscriptionResult {
            format: 1,
            work_item_id: item.id.clone(),
            session_id: item.session_id.clone(),
            sequence: item.sequence,
            discord_user_id: item.discord_user_id.clone(),
            speaker: item.speaker.clone(),
            role: item.role.clone(),
            character: item.character.clone(),
            start_ms: item.start_ms,
            end_ms: item.end_ms,
            source: item.source.clone(),
            source_start_ms: item.source_start_ms,
            source_end_ms: item.source_end_ms,
            text,
            status: "complete".to_owned(),
        }
    }

    fn read_items(bytes: &[u8]) -> Vec<WorkItem> {
        bytes
            .split(|byte| *byte == b'\n')
            .filter(|line| !line.is_empty())
            .map(|line| serde_json::from_slice(line).unwrap())
            .collect()
    }

    fn authority_bytes(directory: &Path) -> (Vec<u8>, Vec<u8>, Vec<u8>, Vec<u8>) {
        (
            fs::read(directory.join("session.json")).unwrap(),
            fs::read(directory.join(WORK_ITEM_MANIFEST_PATH)).unwrap(),
            fs::read(directory.join(TRANSCRIPTION_RESULTS_PATH)).unwrap(),
            fs::read(directory.join(FINAL_TRANSCRIPT_PATH)).unwrap(),
        )
    }

    fn test_directory(label: &str) -> PathBuf {
        let directory = env::temp_dir().join(format!(
            "echoscribe-retranscription-{label}-{}-{}",
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
