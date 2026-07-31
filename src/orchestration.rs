//! One-stop post-recording orchestration and stage-aware continuation.
//!
//! Live capture finishes before this coordinator takes ownership. One shared
//! session lease then covers every derived publication through completion, so
//! another command cannot interleave authority between stages.

use std::{
    fs,
    path::Path,
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, anyhow, bail};

use crate::{
    config::SegmentationConfig,
    continuation,
    operation_lease::SessionOperationLease,
    session::{
        PREVIOUS_SESSION_FORMAT_VERSION, RECORDING_SESSION_FORMAT_VERSION, SESSION_FORMAT_VERSION,
        SessionRecord, SessionStore, WorkflowState,
    },
    stage::{StageError, StageResult},
    transcription::{self, PreparedTranscription},
    work_items,
};

struct PreparedPipeline {
    merge_gap_ms: u64,
    transcription: PreparedTranscription,
}

impl PreparedPipeline {
    fn load(config_path: &Path) -> Result<Self> {
        // Operator configuration is validated before the lease and before any
        // session mutation. A typo is a refusal, not a workflow failure.
        Ok(Self {
            merge_gap_ms: SegmentationConfig::load_merge_gap_ms(config_path)?,
            transcription: PreparedTranscription::load(config_path)?,
        })
    }
}

trait PipelineStages {
    fn validate_recording_continuation(
        &self,
        session_directory: &Path,
        lease: &SessionOperationLease,
    ) -> Result<()>;

    fn build_work_items(
        &self,
        session_directory: &Path,
        lease: &SessionOperationLease,
    ) -> StageResult<()>;

    fn transcribe(
        &self,
        session_directory: &Path,
        lease: &SessionOperationLease,
    ) -> StageResult<()>;

    fn continue_transcription(
        &self,
        session_directory: &Path,
        lease: &SessionOperationLease,
    ) -> StageResult<()>;
}

struct SystemStages<'a> {
    prepared: &'a PreparedPipeline,
}

impl PipelineStages for SystemStages<'_> {
    fn validate_recording_continuation(
        &self,
        session_directory: &Path,
        lease: &SessionOperationLease,
    ) -> Result<()> {
        continuation::run_with_lease(session_directory, lease)
    }

    fn build_work_items(
        &self,
        session_directory: &Path,
        lease: &SessionOperationLease,
    ) -> StageResult<()> {
        work_items::run_with_lease(
            session_directory,
            self.prepared.merge_gap_ms,
            unix_millis_now().map_err(StageError::refused)?,
            lease,
        )
    }

    fn transcribe(
        &self,
        session_directory: &Path,
        lease: &SessionOperationLease,
    ) -> StageResult<()> {
        transcription::run_with_lease(session_directory, &self.prepared.transcription, lease)
    }

    fn continue_transcription(
        &self,
        session_directory: &Path,
        lease: &SessionOperationLease,
    ) -> StageResult<()> {
        transcription::continue_with_lease(session_directory, &self.prepared.transcription, lease)
    }
}

pub(crate) fn run_after_recording(session_directory: &Path, config_path: &Path) -> Result<()> {
    let prepared = PreparedPipeline::load(config_path)?;
    let session_directory = canonical_session_directory(session_directory)?;
    let lease = SessionOperationLease::acquire(&session_directory)?;
    let stages = SystemStages {
        prepared: &prepared,
    };

    let record = load_record(&session_directory)?;
    if record.format != RECORDING_SESSION_FORMAT_VERSION
        || record.state != WorkflowState::ReadyForTranscription
        || record.files.results.is_some()
    {
        bail!(
            "one-stop post-recording orchestration requires a format-4 \
             ready_for_transcription session without results; found format {} state {}",
            record.format,
            record.state.as_str()
        );
    }
    run_ready_pipeline(&session_directory, &lease, &stages)
}

pub(crate) fn continue_stage_aware(session_directory: &Path, config_path: &Path) -> Result<()> {
    let prepared = PreparedPipeline::load(config_path)?;
    let session_directory = canonical_session_directory(session_directory)?;
    let lease = SessionOperationLease::acquire(&session_directory)?;
    let stages = SystemStages {
        prepared: &prepared,
    };
    continue_with_stages(&session_directory, &lease, &stages)
}

fn continue_with_stages(
    session_directory: &Path,
    lease: &SessionOperationLease,
    stages: &dyn PipelineStages,
) -> Result<()> {
    let record = load_record(session_directory)?;
    match (record.format, record.state) {
        (
            PREVIOUS_SESSION_FORMAT_VERSION | RECORDING_SESSION_FORMAT_VERSION,
            WorkflowState::AwaitingOperator,
        ) if record.files.results.is_none() => {
            stages.validate_recording_continuation(session_directory, lease)?;
            run_ready_pipeline(session_directory, lease, stages)
        }
        (
            PREVIOUS_SESSION_FORMAT_VERSION | RECORDING_SESSION_FORMAT_VERSION,
            WorkflowState::ReadyForTranscription,
        ) if record.files.results.is_none() => run_ready_pipeline(session_directory, lease, stages),
        (SESSION_FORMAT_VERSION, WorkflowState::Transcribing)
            if has_transcription_authority(&record) =>
        {
            run_stage(
                session_directory,
                lease,
                "transcription_orchestration",
                stages.transcribe(session_directory, lease),
            )?;
            require_complete(session_directory, lease)
        }
        (
            SESSION_FORMAT_VERSION,
            WorkflowState::AwaitingOperator | WorkflowState::TranscriptionFailed,
        ) if has_transcription_authority(&record) => {
            run_stage(
                session_directory,
                lease,
                "transcription_orchestration",
                stages.continue_transcription(session_directory, lease),
            )?;
            require_complete(session_directory, lease)
        }
        _ => bail!(
            "continue <session> <config> cannot route format {} state {} with the current \
             work/results authority",
            record.format,
            record.state.as_str()
        ),
    }
}

fn run_ready_pipeline(
    session_directory: &Path,
    lease: &SessionOperationLease,
    stages: &dyn PipelineStages,
) -> Result<()> {
    let before_manifest = load_record(session_directory)?;
    if before_manifest.format > RECORDING_SESSION_FORMAT_VERSION
        || before_manifest.state != WorkflowState::ReadyForTranscription
        || before_manifest.files.results.is_some()
    {
        bail!(
            "post-recording pipeline requires ready_for_transcription without results; \
             found format {} state {}",
            before_manifest.format,
            before_manifest.state.as_str()
        );
    }

    if before_manifest.files.work_items.is_none() {
        run_stage(
            session_directory,
            lease,
            "work_manifest_build",
            stages.build_work_items(session_directory, lease),
        )?;
    }

    // Reload after the builder. The manifest description and checkpoint are
    // the durable boundary deciding whether this stage is reused or retried.
    let before_transcription = match load_record(session_directory) {
        Ok(record) => record,
        Err(error) => {
            return run_stage(
                session_directory,
                lease,
                "work_manifest_build",
                Err(StageError::accepted(error)),
            );
        }
    };
    if before_transcription.state != WorkflowState::ReadyForTranscription
        || before_transcription.files.work_items.is_none()
        || before_transcription.files.results.is_some()
    {
        return run_stage(
            session_directory,
            lease,
            "work_manifest_build",
            Err(StageError::accepted(anyhow!(
                "work-manifest stage did not leave a reusable ready_for_transcription authority"
            ))),
        );
    }
    run_stage(
        session_directory,
        lease,
        "transcription_orchestration",
        stages.transcribe(session_directory, lease),
    )?;
    require_complete(session_directory, lease)
}

fn require_complete(session_directory: &Path, lease: &SessionOperationLease) -> Result<()> {
    let record = match load_record(session_directory) {
        Ok(record) => record,
        Err(error) => {
            return run_stage(
                session_directory,
                lease,
                "transcription_orchestration",
                Err(StageError::accepted(error)),
            );
        }
    };
    if record.state != WorkflowState::Complete {
        return run_stage(
            session_directory,
            lease,
            "transcription_orchestration",
            Err(StageError::accepted(anyhow!(
                "transcription stage returned successfully without completing session {}; found {}",
                record.session_id,
                record.state.as_str()
            ))),
        );
    }
    Ok(())
}

fn run_stage(
    session_directory: &Path,
    _lease: &SessionOperationLease,
    failure_kind: &'static str,
    result: StageResult<()>,
) -> Result<()> {
    match result {
        Ok(()) => Ok(()),
        Err(error) if !error.was_accepted() => Err(error.into_anyhow()),
        Err(error) => {
            let source = error.into_anyhow();
            record_accepted_stage_failure(session_directory, failure_kind, &source)?;
            Err(source.context(
                "one-stop workflow stopped at a safe boundary; session is awaiting operator action",
            ))
        }
    }
}

fn record_accepted_stage_failure(
    session_directory: &Path,
    failure_kind: &'static str,
    source: &anyhow::Error,
) -> Result<()> {
    let mut session = SessionStore::load(session_directory).with_context(|| {
        format!("stage failed ({source:#}) and current workflow authority could not be reloaded")
    })?;
    let message = format!("{source:#}");
    match session.record().state {
        WorkflowState::ReadyForTranscription => session
            .publish_orchestration_failure(unix_millis_now()?, failure_kind, message)
            .context("stage failed and durable awaiting_operator publication also failed"),
        WorkflowState::Transcribing => {
            session
                .publish_transcription_failure(
                    unix_millis_now()?,
                    format!("process=orchestration_failure; stage={failure_kind}; error={message}"),
                )
                .context(
                    "stage failed and durable transcription failure publication also failed",
                )?;
            session
                .transition(WorkflowState::AwaitingOperator, unix_millis_now()?)
                .context(
                    "transcription failure is durable but awaiting_operator publication failed",
                )
        }
        // Slice 9 worker handling may already have published the complete
        // failure route before returning its operator-facing error.
        WorkflowState::AwaitingOperator | WorkflowState::TranscriptionFailed => Ok(()),
        _ => Err(anyhow!(
            "stage failed ({source:#}) after authority unexpectedly reached {}",
            session.record().state.as_str()
        )),
    }
}

fn has_transcription_authority(record: &SessionRecord) -> bool {
    record.files.work_items.is_some() && record.files.results.is_some()
}

fn load_record(session_directory: &Path) -> Result<SessionRecord> {
    Ok(SessionStore::load(session_directory)
        .with_context(|| {
            format!(
                "failed to load workflow state from {}",
                session_directory.display()
            )
        })?
        .record()
        .clone())
}

fn canonical_session_directory(session_directory: &Path) -> Result<std::path::PathBuf> {
    fs::canonicalize(session_directory).with_context(|| {
        format!(
            "failed to resolve session directory {}",
            session_directory.display()
        )
    })
}

fn unix_millis_now() -> Result<u64> {
    Ok(SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock is before Unix epoch")?
        .as_millis()
        .try_into()
        .context("Unix timestamp does not fit in u64")?)
}

#[cfg(test)]
mod tests {
    use std::{
        env, fs, process,
        sync::Mutex,
        time::{SystemTime, UNIX_EPOCH},
    };

    use super::*;
    use crate::{
        participants::ParticipantContext,
        session::{NewSession, WorkflowState},
    };

    #[derive(Clone, Copy)]
    enum BuildBehaviour {
        Publish,
        Refuse,
        FailAfterAcceptance,
    }

    #[derive(Clone, Copy)]
    enum TranscriptionBehaviour {
        Complete,
        FailWithPartialOutput,
    }

    struct FakeStages {
        build: BuildBehaviour,
        transcription: TranscriptionBehaviour,
        calls: Mutex<Vec<&'static str>>,
    }

    impl FakeStages {
        fn healthy() -> Self {
            Self {
                build: BuildBehaviour::Publish,
                transcription: TranscriptionBehaviour::Complete,
                calls: Mutex::new(Vec::new()),
            }
        }

        fn with_build(build: BuildBehaviour) -> Self {
            Self {
                build,
                transcription: TranscriptionBehaviour::Complete,
                calls: Mutex::new(Vec::new()),
            }
        }

        fn with_transcription(transcription: TranscriptionBehaviour) -> Self {
            Self {
                build: BuildBehaviour::Publish,
                transcription,
                calls: Mutex::new(Vec::new()),
            }
        }

        fn calls(&self) -> Vec<&'static str> {
            self.calls.lock().unwrap().clone()
        }
    }

    impl PipelineStages for FakeStages {
        fn validate_recording_continuation(
            &self,
            session_directory: &Path,
            _lease: &SessionOperationLease,
        ) -> Result<()> {
            self.calls.lock().unwrap().push("validate_recording");
            SessionStore::load(session_directory)?
                .transition(WorkflowState::ReadyForTranscription, 3_000)?;
            Ok(())
        }

        fn build_work_items(
            &self,
            session_directory: &Path,
            _lease: &SessionOperationLease,
        ) -> StageResult<()> {
            self.calls.lock().unwrap().push("build");
            match self.build {
                BuildBehaviour::Publish => {
                    let mut session = SessionStore::load(session_directory)
                        .map_err(anyhow::Error::new)
                        .map_err(StageError::accepted)?;
                    session
                        .publish_work_manifest(4_000)
                        .map_err(anyhow::Error::new)
                        .map_err(StageError::accepted)
                }
                BuildBehaviour::Refuse => {
                    Err(StageError::refused(anyhow!("manifest validation refused")))
                }
                BuildBehaviour::FailAfterAcceptance => {
                    Err(StageError::accepted(anyhow!("manifest publication failed")))
                }
            }
        }

        fn transcribe(
            &self,
            session_directory: &Path,
            _lease: &SessionOperationLease,
        ) -> StageResult<()> {
            self.calls.lock().unwrap().push("transcribe");
            let mut session = SessionStore::load(session_directory)
                .map_err(anyhow::Error::new)
                .map_err(StageError::accepted)?;
            if session.record().state == WorkflowState::ReadyForTranscription {
                session
                    .publish_transcription_start(5_000)
                    .map_err(anyhow::Error::new)
                    .map_err(StageError::accepted)?;
            }
            match self.transcription {
                TranscriptionBehaviour::Complete => session
                    .publish_transcription_complete(6_000)
                    .map_err(anyhow::Error::new)
                    .map_err(StageError::accepted),
                TranscriptionBehaviour::FailWithPartialOutput => {
                    fs::write(
                        session_directory.join("transcript.partial.txt"),
                        b"[00:00:00] Alice: retained partial\n",
                    )
                    .map_err(anyhow::Error::new)
                    .map_err(StageError::accepted)?;
                    fs::write(
                        session_directory.join("transcription/results.jsonl"),
                        b"{\"committed\":true}\n",
                    )
                    .map_err(anyhow::Error::new)
                    .map_err(StageError::accepted)?;
                    session
                        .publish_transcription_failure(6_000, "injected worker failure")
                        .map_err(anyhow::Error::new)
                        .map_err(StageError::accepted)?;
                    session
                        .transition(WorkflowState::AwaitingOperator, 6_100)
                        .map_err(anyhow::Error::new)
                        .map_err(StageError::accepted)?;
                    Err(StageError::accepted(anyhow!("worker failed")))
                }
            }
        }

        fn continue_transcription(
            &self,
            session_directory: &Path,
            _lease: &SessionOperationLease,
        ) -> StageResult<()> {
            self.calls.lock().unwrap().push("continue_transcription");
            let mut session = SessionStore::load(session_directory)
                .map_err(anyhow::Error::new)
                .map_err(StageError::accepted)?;
            if session.record().state == WorkflowState::TranscriptionFailed {
                session
                    .transition(WorkflowState::AwaitingOperator, 5_000)
                    .map_err(anyhow::Error::new)
                    .map_err(StageError::accepted)?;
            }
            session
                .transition(WorkflowState::Transcribing, 5_100)
                .map_err(anyhow::Error::new)
                .map_err(StageError::accepted)?;
            session
                .publish_transcription_complete(6_000)
                .map_err(anyhow::Error::new)
                .map_err(StageError::accepted)
        }
    }

    #[test]
    fn clean_pipeline_crosses_every_post_recording_state() {
        let directory = ready_fixture("clean");
        let lease = SessionOperationLease::acquire(&directory).unwrap();
        let stages = FakeStages::healthy();

        run_ready_pipeline(&directory, &lease, &stages).unwrap();

        assert_eq!(stages.calls(), ["build", "transcribe"]);
        assert_eq!(
            SessionStore::load(&directory).unwrap().record().state,
            WorkflowState::Complete
        );
        drop(lease);
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn published_work_manifest_is_reused() {
        let directory = ready_fixture("reuse-manifest");
        SessionStore::load(&directory)
            .unwrap()
            .publish_work_manifest(4_000)
            .unwrap();
        let lease = SessionOperationLease::acquire(&directory).unwrap();
        let stages = FakeStages::healthy();

        run_ready_pipeline(&directory, &lease, &stages).unwrap();

        assert_eq!(stages.calls(), ["transcribe"]);
        drop(lease);
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn format_three_ready_session_is_upgraded_then_completed() {
        let directory = ready_fixture("format-three-ready");
        let mut record = SessionStore::load(&directory).unwrap().record().clone();
        record.format = PREVIOUS_SESSION_FORMAT_VERSION;
        fs::write(
            directory.join("session.json"),
            serde_json::to_vec_pretty(&record).unwrap(),
        )
        .unwrap();
        let lease = SessionOperationLease::acquire(&directory).unwrap();
        let stages = FakeStages::healthy();

        continue_with_stages(&directory, &lease, &stages).unwrap();

        assert_eq!(stages.calls(), ["build", "transcribe"]);
        let completed = SessionStore::load(&directory).unwrap();
        assert_eq!(completed.record().format, SESSION_FORMAT_VERSION);
        assert_eq!(completed.record().state, WorkflowState::Complete);
        drop(completed);
        drop(lease);
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn accepted_manifest_failure_waits_without_starting_transcription() {
        let directory = ready_fixture("manifest-failure");
        let lease = SessionOperationLease::acquire(&directory).unwrap();
        let stages = FakeStages::with_build(BuildBehaviour::FailAfterAcceptance);

        let error = run_ready_pipeline(&directory, &lease, &stages).unwrap_err();

        assert!(error.to_string().contains("safe boundary"));
        assert_eq!(stages.calls(), ["build"]);
        let session = SessionStore::load(&directory).unwrap();
        assert_eq!(session.record().state, WorkflowState::AwaitingOperator);
        assert_eq!(
            session.record().failures.last().unwrap().kind,
            "work_manifest_build"
        );
        drop(session);
        drop(lease);
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn configured_continuation_retries_the_failed_manifest_boundary() {
        let directory = ready_fixture("manifest-retry");
        let lease = SessionOperationLease::acquire(&directory).unwrap();
        let failing = FakeStages::with_build(BuildBehaviour::FailAfterAcceptance);
        run_ready_pipeline(&directory, &lease, &failing).unwrap_err();
        let resumed = FakeStages::healthy();

        continue_with_stages(&directory, &lease, &resumed).unwrap();

        assert_eq!(
            resumed.calls(),
            ["validate_recording", "build", "transcribe"]
        );
        assert_eq!(
            SessionStore::load(&directory).unwrap().record().state,
            WorkflowState::Complete
        );
        drop(lease);
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn transcription_failure_stops_with_partial_outputs() {
        let directory = ready_fixture("transcription-failure");
        let lease = SessionOperationLease::acquire(&directory).unwrap();
        let stages = FakeStages::with_transcription(TranscriptionBehaviour::FailWithPartialOutput);

        let error = run_ready_pipeline(&directory, &lease, &stages).unwrap_err();

        assert!(error.to_string().contains("safe boundary"));
        assert_eq!(stages.calls(), ["build", "transcribe"]);
        assert_eq!(
            SessionStore::load(&directory).unwrap().record().state,
            WorkflowState::AwaitingOperator
        );
        assert!(directory.join("transcript.partial.txt").is_file());
        assert!(directory.join("transcription/results.jsonl").is_file());
        drop(lease);
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn validation_refusal_does_not_mutate_authority() {
        let directory = ready_fixture("manifest-refusal");
        let before = fs::read(directory.join("session.json")).unwrap();
        let lease = SessionOperationLease::acquire(&directory).unwrap();
        let stages = FakeStages::with_build(BuildBehaviour::Refuse);

        let error = run_ready_pipeline(&directory, &lease, &stages).unwrap_err();

        assert!(error.to_string().contains("validation refused"));
        assert_eq!(fs::read(directory.join("session.json")).unwrap(), before);
        assert_eq!(stages.calls(), ["build"]);
        drop(lease);
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn configured_continuation_validates_recovery_then_resumes_pipeline() {
        let directory = awaiting_recording_fixture("recording-continuation");
        let lease = SessionOperationLease::acquire(&directory).unwrap();
        let stages = FakeStages::healthy();

        continue_with_stages(&directory, &lease, &stages).unwrap();

        assert_eq!(
            stages.calls(),
            ["validate_recording", "build", "transcribe"]
        );
        assert_eq!(
            SessionStore::load(&directory).unwrap().record().state,
            WorkflowState::Complete
        );
        drop(lease);
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn configured_continuation_uses_controlled_restart_for_transcribing() {
        let directory = ready_fixture("controlled-restart");
        let mut session = SessionStore::load(&directory).unwrap();
        session.publish_work_manifest(4_000).unwrap();
        session.publish_transcription_start(5_000).unwrap();
        let lease = SessionOperationLease::acquire(&directory).unwrap();
        let stages = FakeStages::healthy();

        continue_with_stages(&directory, &lease, &stages).unwrap();

        assert_eq!(stages.calls(), ["transcribe"]);
        assert_eq!(
            SessionStore::load(&directory).unwrap().record().state,
            WorkflowState::Complete
        );
        drop(lease);
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn configured_continuation_routes_transcription_failure_to_rewind_stage() {
        let directory = ready_fixture("transcription-continuation");
        let mut session = SessionStore::load(&directory).unwrap();
        session.publish_work_manifest(4_000).unwrap();
        session.publish_transcription_start(5_000).unwrap();
        session
            .publish_transcription_failure(5_500, "injected transcription failure")
            .unwrap();
        session
            .transition(WorkflowState::AwaitingOperator, 5_600)
            .unwrap();
        let lease = SessionOperationLease::acquire(&directory).unwrap();
        let stages = FakeStages::healthy();

        continue_with_stages(&directory, &lease, &stages).unwrap();

        assert_eq!(stages.calls(), ["continue_transcription"]);
        assert_eq!(
            SessionStore::load(&directory).unwrap().record().state,
            WorkflowState::Complete
        );
        drop(lease);
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn completed_session_refuses_duplicate_normal_continuation() {
        let directory = ready_fixture("duplicate");
        let mut session = SessionStore::load(&directory).unwrap();
        session.publish_work_manifest(4_000).unwrap();
        session.publish_transcription_start(5_000).unwrap();
        session.publish_transcription_complete(6_000).unwrap();
        let before = fs::read(directory.join("session.json")).unwrap();
        let lease = SessionOperationLease::acquire(&directory).unwrap();
        let stages = FakeStages::healthy();

        let error = continue_with_stages(&directory, &lease, &stages).unwrap_err();

        assert!(error.to_string().contains("cannot route"));
        assert!(stages.calls().is_empty());
        assert_eq!(fs::read(directory.join("session.json")).unwrap(), before);
        drop(lease);
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn invalid_config_is_refused_before_session_mutation() {
        let directory = ready_fixture("invalid-config");
        let config_path = directory.join("invalid.toml");
        fs::write(&config_path, "this is not valid TOML =").unwrap();
        let before = fs::read(directory.join("session.json")).unwrap();

        let error = continue_stage_aware(&directory, &config_path).unwrap_err();

        assert!(error.to_string().contains("failed to parse configuration"));
        assert_eq!(fs::read(directory.join("session.json")).unwrap(), before);
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn lease_contention_is_refused_before_session_mutation() {
        let directory = ready_fixture("lease-contention");
        let config_path = write_offline_config(&directory);
        let before = fs::read(directory.join("session.json")).unwrap();
        let _lease = SessionOperationLease::acquire(&directory).unwrap();

        let error = continue_stage_aware(&directory, &config_path).unwrap_err();

        assert!(error.to_string().contains("another mutating operation"));
        assert_eq!(fs::read(directory.join("session.json")).unwrap(), before);
        drop(_lease);
        fs::remove_dir_all(directory).unwrap();
    }

    fn ready_fixture(label: &str) -> std::path::PathBuf {
        let directory = fixture_directory(label);
        let participants = ParticipantContext::empty_for_test();
        let mut session = SessionStore::create(
            &directory,
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
        session
            .transition(WorkflowState::RecordedClean, 2_000)
            .unwrap();
        session
            .transition(WorkflowState::ReadyForTranscription, 2_100)
            .unwrap();
        directory
    }

    fn awaiting_recording_fixture(label: &str) -> std::path::PathBuf {
        let directory = fixture_directory(label);
        let participants = ParticipantContext::empty_for_test();
        let mut session = SessionStore::create(
            &directory,
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
        session
            .transition(WorkflowState::RecordedIncomplete, 2_000)
            .unwrap();
        session
            .transition(WorkflowState::AwaitingOperator, 2_100)
            .unwrap();
        directory
    }

    fn fixture_directory(label: &str) -> std::path::PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let directory = env::temp_dir().join(format!(
            "echoscribe-orchestration-{label}-{}-{nonce}",
            process::id()
        ));
        fs::create_dir_all(&directory).unwrap();
        directory
    }

    fn write_offline_config(directory: &Path) -> std::path::PathBuf {
        let path = directory.join("echoscribe.toml");
        fs::write(
            &path,
            r#"
version = 1

[discord]
token = "not-used-offline"
guild_id = "not-validated-offline"
channel_id = "not-validated-offline"

[recording]
output_directory = "recordings"

[participants]
file = "not-read-offline.toml"

[transcription]
model = "test-model"
language = "en"
device = "cpu"
compute_type = "int8"
beam_size = 1
vocabulary_file = "missing-vocabulary.txt"
resume_rewind_seconds = 120

[segmentation]
vad_enabled = false
merge_gap_ms = 750
"#,
        )
        .unwrap();
        path
    }
}
