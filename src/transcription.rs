//! Offline transcription orchestration and durable result-prefix validation.
//!
//! Rust owns workflow authority and process lifetime. The Python worker owns
//! model loading, ranged audio extraction, and ordered result/text appends, but
//! never edits `session.json`.

use std::{
    collections::HashMap,
    env,
    ffi::OsString,
    fmt,
    fs::{self, File, OpenOptions},
    io::{BufWriter, Write},
    path::{Path, PathBuf},
    process::{Command, Stdio},
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, anyhow, bail};
use serde::{Deserialize, Serialize};

use crate::{
    artifacts::{
        FINAL_TRANSCRIPT_PATH, PARTIAL_TRANSCRIPT_FILE_NAME, TRANSCRIPTION_DIRECTORY_NAME,
        TRANSCRIPTION_RESULT_FORMAT_VERSION, TRANSCRIPTION_RESULTS_FILE_NAME,
        WORK_ITEM_MANIFEST_FORMAT_VERSION,
    },
    config::OfflineTranscriptionConfig,
    operation_lease::SessionOperationLease,
    participants::ParticipantContext,
    session::{
        RECORDING_SESSION_FORMAT_VERSION, SESSION_FORMAT_VERSION, SessionRecord, SessionStore,
        WorkflowState,
    },
    stage::{StageError, StageResult},
    track_manifest::{TrackManifest, TrackState},
    work_items::WorkItem,
};

const RESULTS_TEMP_FILE_NAME: &str = ".results.jsonl.tmp";
const RESULTS_RESUME_TEMP_FILE_NAME: &str = ".results.jsonl.resume.tmp";
const PARTIAL_TRANSCRIPT_TEMP_FILE_NAME: &str = ".transcript.partial.txt.tmp";
const FINAL_TRANSCRIPT_TEMP_FILE_NAME: &str = ".transcript.txt.tmp";
const RESUME_PREPARED_PREFIX: &str = "transcription_resume_prepared_";
const RESUME_APPLIED_PREFIX: &str = "transcription_resume_applied_";

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct TranscriptionResult {
    pub(crate) format: u16,
    pub(crate) work_item_id: String,
    pub(crate) session_id: String,
    pub(crate) sequence: u64,
    pub(crate) discord_user_id: String,
    pub(crate) speaker: String,
    pub(crate) role: String,
    pub(crate) character: Option<String>,
    pub(crate) start_ms: u64,
    pub(crate) end_ms: u64,
    pub(crate) source: String,
    pub(crate) source_start_ms: u64,
    pub(crate) source_end_ms: u64,
    pub(crate) text: String,
    pub(crate) status: String,
}

struct WorkerInvocation<'a> {
    config_path: &'a Path,
    session_directory: &'a Path,
    manifest_path: &'a Path,
    results_path: &'a Path,
    transcript_path: &'a Path,
    next_sequence: u64,
    settings: &'a OfflineTranscriptionConfig,
}

trait WorkerProcess {
    fn run(
        &self,
        invocation: &WorkerInvocation<'_>,
        lease: &SessionOperationLease,
    ) -> Result<WorkerExit>;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct WorkerExit {
    success: bool,
    code: Option<i32>,
}

struct SystemWorker;

impl WorkerProcess for SystemWorker {
    fn run(
        &self,
        invocation: &WorkerInvocation<'_>,
        lease: &SessionOperationLease,
    ) -> Result<WorkerExit> {
        let worker_path = worker_script_path();
        if !worker_path.is_file() {
            bail!(
                "Python transcription worker is missing at {}",
                worker_path.display()
            );
        }

        let interpreter = python_interpreter()?;
        let mut command = Command::new(&interpreter);
        command
            // The worker never reads stdin. Giving it a duplicate of the
            // locked handle makes the OS retain the lease if Rust is killed.
            .stdin(Stdio::from(lease.inherited_handle()?))
            .arg(&worker_path)
            .arg("--config")
            .arg(invocation.config_path)
            .arg("--session")
            .arg(invocation.session_directory)
            .arg("--manifest")
            .arg(invocation.manifest_path)
            .arg("--results")
            .arg(invocation.results_path)
            .arg("--transcript")
            .arg(invocation.transcript_path)
            .arg("--start-sequence")
            .arg(invocation.next_sequence.to_string())
            .arg("--model")
            .arg(&invocation.settings.model)
            .arg("--language")
            .arg(&invocation.settings.language)
            .arg("--device")
            .arg(&invocation.settings.device)
            .arg("--compute-type")
            .arg(&invocation.settings.compute_type)
            .arg("--beam-size")
            .arg(invocation.settings.beam_size.to_string())
            .args(vad_worker_arguments(invocation.settings.vad_enabled));
        for hotword in &invocation.settings.hotwords {
            command.arg("--hotword").arg(hotword);
        }

        let status = command.status().with_context(|| {
            format!(
                "failed to launch Python transcription worker {} with interpreter {:?}",
                worker_path.display(),
                interpreter
            )
        })?;
        Ok(WorkerExit {
            success: status.success(),
            code: status.code(),
        })
    }
}

fn vad_worker_arguments(enabled: bool) -> [OsString; 2] {
    [
        OsString::from("--vad-enabled"),
        OsString::from(enabled.to_string()),
    ]
}

pub(crate) struct PreparedTranscription {
    config_path: PathBuf,
    settings: OfflineTranscriptionConfig,
}

impl PreparedTranscription {
    /// Parse all operator-controlled configuration before session ownership is
    /// acquired, so a bad config refuses cleanly without changing authority.
    pub(crate) fn load(config_path: &Path) -> Result<Self> {
        let settings = OfflineTranscriptionConfig::load(config_path)?;
        if let Some(warning) = &settings.vocabulary_warning {
            eprintln!("Warning: {warning}.");
        }
        let config_path = fs::canonicalize(config_path).with_context(|| {
            format!(
                "failed to resolve configuration file {}",
                config_path.display()
            )
        })?;
        Ok(Self {
            config_path,
            settings,
        })
    }
}

pub(crate) fn run(session_directory: &Path, config_path: &Path) -> Result<()> {
    run_with_worker(session_directory, config_path, &SystemWorker)
}

pub(crate) fn rebuild_transcript(session_directory: &Path) -> Result<()> {
    let session_directory = canonical_session_directory(session_directory)?;
    let lease = SessionOperationLease::acquire(&session_directory)?;
    rebuild_transcript_with_lease(&session_directory, &lease)
}

fn rebuild_transcript_with_lease(
    session_directory: &Path,
    _lease: &SessionOperationLease,
) -> Result<()> {
    let session = SessionStore::load(session_directory).with_context(|| {
        format!(
            "failed to load workflow state from {}",
            session_directory.display()
        )
    })?;
    if session.record().format != SESSION_FORMAT_VERSION
        || session.record().state != WorkflowState::Complete
        || session.record().files.work_items.is_none()
        || session.record().files.results.is_none()
    {
        bail!(
            "rebuild-transcript requires a complete format-5 session with work and result authority; \
             found format {} state {}",
            session.record().format,
            session.record().state.as_str()
        );
    }

    let manifest_path = session_directory.join(
        &session
            .record()
            .files
            .work_items
            .as_ref()
            .expect("rebuild entry validation requires work_items")
            .path,
    );
    // Completed structured authority is sufficient to render display text.
    // Recording journals, participant context and source audio are deliberately
    // outside this recovery boundary.
    let work_items = read_work_manifest_authority(&manifest_path, session.record())?;
    let results_path = session_directory.join(
        &session
            .record()
            .files
            .results
            .as_ref()
            .expect("rebuild entry validation requires results")
            .path,
    );
    let bytes = fs::read(&results_path)
        .with_context(|| format!("failed to read results {}", results_path.display()))?;
    if !bytes.is_empty() && !bytes.ends_with(b"\n") {
        bail!(
            "cannot rebuild transcript from a truncated final result record in {}",
            results_path.display()
        );
    }
    let committed = validate_and_repair_result_prefix(&results_path, &work_items)?;
    if committed.len() != work_items.len() {
        bail!(
            "cannot rebuild transcript: committed {} of {} work items",
            committed.len(),
            work_items.len()
        );
    }

    write_final_transcript_atomically(session_directory, &committed)?;
    println!(
        "Rebuilt final transcript for session {} at {}.",
        session.record().session_id,
        session_directory.join(FINAL_TRANSCRIPT_PATH).display()
    );
    Ok(())
}

fn run_with_worker(
    session_directory: &Path,
    config_path: &Path,
    worker: &dyn WorkerProcess,
) -> Result<()> {
    run_with_worker_before_lease(session_directory, config_path, worker, || {})
}

fn run_with_worker_before_lease(
    session_directory: &Path,
    config_path: &Path,
    worker: &dyn WorkerProcess,
    before_lease: impl FnOnce(),
) -> Result<()> {
    let prepared = PreparedTranscription::load(config_path)?;
    let session_directory = canonical_session_directory(session_directory)?;
    before_lease();
    // Every session-derived read happens after ownership is exclusive. A
    // caller delayed before this point cannot carry stale authority forward.
    let lease = SessionOperationLease::acquire(&session_directory)?;
    run_prepared_with_worker(&session_directory, &prepared, worker, &lease)
        .map_err(StageError::into_anyhow)
}

pub(crate) fn run_with_lease(
    session_directory: &Path,
    prepared: &PreparedTranscription,
    lease: &SessionOperationLease,
) -> StageResult<()> {
    run_prepared_with_worker(session_directory, prepared, &SystemWorker, lease)
}

fn run_prepared_with_worker(
    session_directory: &Path,
    prepared: &PreparedTranscription,
    worker: &dyn WorkerProcess,
    lease: &SessionOperationLease,
) -> StageResult<()> {
    let (mut session, manifest_path, work_items) = (|| -> Result<_> {
        let session = SessionStore::load(session_directory).with_context(|| {
            format!(
                "failed to load workflow state from {}",
                session_directory.display()
            )
        })?;
        validate_entry_state(session.record())?;
        let manifest_path = session_directory.join(
            &session
                .record()
                .files
                .work_items
                .as_ref()
                .expect("entry validation requires work_items")
                .path,
        );
        let tracks = validate_session_artifacts(session_directory, session.record())?;
        let work_items = read_work_manifest(&manifest_path, session.record(), &tracks)?;
        Ok((session, manifest_path, work_items))
    })()
    .map_err(StageError::refused)?;

    (|| -> Result<()> {
        if session.record().state == WorkflowState::ReadyForTranscription {
            prepare_empty_results(session_directory)?;
            session
                .publish_transcription_start(unix_millis_now()?)
                .context(
                    "results authority was prepared but session.json could not start transcription",
                )?;
        }

        let results_path = session_directory.join(
            &session
                .record()
                .files
                .results
                .as_ref()
                .expect("format-5 transcribing session requires results")
                .path,
        );
        let committed = validate_and_repair_result_prefix(&results_path, &work_items)?;
        let transcript_path = session_directory.join(PARTIAL_TRANSCRIPT_FILE_NAME);
        rebuild_partial_transcript(session_directory, &transcript_path, &committed)?;
        let next_sequence = u64::try_from(committed.len())
            .ok()
            .and_then(|value| value.checked_add(1))
            .ok_or_else(|| anyhow!("transcription result sequence overflow"))?;

        run_worker_attempt(
            &mut session,
            session_directory,
            &prepared.config_path,
            &manifest_path,
            &results_path,
            &transcript_path,
            &work_items,
            next_sequence,
            &prepared.settings,
            worker,
            lease,
        )
    })()
    .map_err(StageError::accepted)
}

#[cfg(test)]
fn continue_with_worker(
    session_directory: &Path,
    config_path: &Path,
    worker: &dyn WorkerProcess,
) -> Result<()> {
    continue_with_worker_before_lease(session_directory, config_path, worker, || {})
}

#[cfg(test)]
fn continue_with_worker_before_lease(
    session_directory: &Path,
    config_path: &Path,
    worker: &dyn WorkerProcess,
    before_lease: impl FnOnce(),
) -> Result<()> {
    let prepared = PreparedTranscription::load(config_path)?;
    let session_directory = canonical_session_directory(session_directory)?;
    before_lease();
    // Continuation route selection and every path derived from session.json are
    // protected by the same lease as result repair and worker execution.
    let lease = SessionOperationLease::acquire(&session_directory)?;
    continue_prepared_with_worker(&session_directory, &prepared, worker, &lease)
        .map_err(StageError::into_anyhow)
}

pub(crate) fn continue_with_lease(
    session_directory: &Path,
    prepared: &PreparedTranscription,
    lease: &SessionOperationLease,
) -> StageResult<()> {
    continue_prepared_with_worker(session_directory, prepared, &SystemWorker, lease)
}

fn continue_prepared_with_worker(
    session_directory: &Path,
    prepared: &PreparedTranscription,
    worker: &dyn WorkerProcess,
    lease: &SessionOperationLease,
) -> StageResult<()> {
    let (mut session, manifest_path, work_items, results_path, committed, plan) =
        (|| -> Result<_> {
            let session = SessionStore::load(session_directory).with_context(|| {
                format!(
                    "failed to load workflow state from {}",
                    session_directory.display()
                )
            })?;
            validate_transcription_continuation_entry(session.record())?;
            let manifest_path = session_directory.join(
                &session
                    .record()
                    .files
                    .work_items
                    .as_ref()
                    .expect("continuation validation requires work_items")
                    .path,
            );
            let tracks = validate_session_artifacts(session_directory, session.record())?;
            let work_items = read_work_manifest(&manifest_path, session.record(), &tracks)?;
            let results_path = session_directory.join(
                &session
                    .record()
                    .files
                    .results
                    .as_ref()
                    .expect("continuation validation requires results")
                    .path,
            );
            let committed = validate_and_repair_result_prefix(&results_path, &work_items)?;
            let plan = select_resume_plan(
                session.record(),
                &committed,
                &work_items,
                &prepared.settings,
            )?;
            Ok((
                session,
                manifest_path,
                work_items,
                results_path,
                committed,
                plan,
            ))
        })()
        .map_err(StageError::refused)?;

    (|| -> Result<()> {
        if session.record().state == WorkflowState::TranscriptionFailed {
            session
                .transition(WorkflowState::AwaitingOperator, unix_millis_now()?)
                .context("failed to publish awaiting_operator for transcription continuation")?;
        }
        if !plan.previously_prepared {
            session
                .record_transcription_resume_prepared(unix_millis_now()?, plan.sequence)
                .with_context(|| {
                    format!(
                        "failed to record prepared transcription resume sequence {}",
                        plan.sequence
                    )
                })?;
        }

        let retained_count = usize::try_from(plan.sequence - 1)
            .map_err(|_| anyhow!("resume sequence does not fit in memory"))?;
        replace_results_prefix(&results_path, &committed[..retained_count])?;
        let retained = committed[..retained_count].to_vec();
        let transcript_path = session_directory.join(PARTIAL_TRANSCRIPT_FILE_NAME);
        rebuild_partial_transcript(session_directory, &transcript_path, &retained)?;
        session
            .apply_transcription_resume(unix_millis_now()?, plan.sequence)
            .with_context(|| {
                format!(
                    "results were rewound to sequence {} but the applied resume state was not published",
                    plan.sequence
                )
            })?;

        run_worker_attempt(
            &mut session,
            session_directory,
            &prepared.config_path,
            &manifest_path,
            &results_path,
            &transcript_path,
            &work_items,
            plan.sequence,
            &prepared.settings,
            worker,
            lease,
        )
    })()
    .map_err(StageError::accepted)
}

fn canonical_session_directory(session_directory: &Path) -> Result<PathBuf> {
    fs::canonicalize(session_directory).with_context(|| {
        format!(
            "failed to resolve session directory {}",
            session_directory.display()
        )
    })
}

#[allow(clippy::too_many_arguments)]
fn run_worker_attempt(
    session: &mut SessionStore,
    session_directory: &Path,
    config_path: &Path,
    manifest_path: &Path,
    results_path: &Path,
    transcript_path: &Path,
    work_items: &[WorkItem],
    attempted_start_sequence: u64,
    settings: &OfflineTranscriptionConfig,
    worker: &dyn WorkerProcess,
    lease: &SessionOperationLease,
) -> Result<()> {
    let invocation = WorkerInvocation {
        config_path,
        session_directory,
        manifest_path,
        results_path,
        transcript_path,
        next_sequence: attempted_start_sequence,
        settings,
    };
    let (worker_succeeded, process_diagnostic) = match worker.run(&invocation, lease) {
        Err(error) => (false, format!("launch_failure: {error:#}")),
        Ok(exit) if exit.success => (true, "zero_exit".to_owned()),
        Ok(exit) => (
            false,
            match exit.code {
                Some(code) => format!("non_zero_exit: status {code}"),
                None => "signal_termination: worker terminated without an exit status".to_owned(),
            },
        ),
    };

    let committed = match validate_and_repair_result_prefix_detailed(results_path, work_items) {
        Ok(committed) => committed,
        Err(integrity) => {
            return record_result_integrity_failure(
                session,
                attempted_start_sequence,
                &process_diagnostic,
                &integrity,
            );
        }
    };

    if !worker_succeeded {
        return record_worker_failure(
            session,
            work_items,
            &committed,
            attempted_start_sequence,
            process_diagnostic,
        );
    }
    if committed.len() != work_items.len() {
        return record_worker_failure(
            session,
            work_items,
            &committed,
            attempted_start_sequence,
            format!(
                "incomplete_zero_exit: committed {} of {} work items",
                committed.len(),
                work_items.len()
            ),
        );
    }

    rebuild_partial_transcript(session_directory, transcript_path, &committed)?;
    publish_final_transcript(session_directory, transcript_path)?;
    session
        .publish_transcription_complete(unix_millis_now()?)
        .context("final transcript was published but session completion state was not")?;
    println!(
        "Transcription completed for session {}; final transcript: {}.",
        session.record().session_id,
        session_directory.join(FINAL_TRANSCRIPT_PATH).display()
    );
    Ok(())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ResumePlan {
    sequence: u64,
    previously_prepared: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ResumeCheckpoint {
    Prepared(u64),
    Applied(u64),
}

fn validate_transcription_continuation_entry(record: &SessionRecord) -> Result<()> {
    if record.format != SESSION_FORMAT_VERSION
        || record.files.work_items.is_none()
        || record.files.results.is_none()
        || !matches!(
            record.state,
            WorkflowState::AwaitingOperator | WorkflowState::TranscriptionFailed
        )
    {
        bail!(
            "continue <session> <config> requires a format-5 awaiting_operator or \
             transcription_failed session with work_items and results; found format {} state {}",
            record.format,
            record.state.as_str()
        );
    }
    if !record.failures.iter().any(|failure| {
        failure.kind == "transcription_worker" && failure.state == WorkflowState::Transcribing
    }) {
        bail!("transcription continuation requires durable transcription-failure evidence");
    }
    Ok(())
}

fn record_worker_failure(
    session: &mut SessionStore,
    work_items: &[WorkItem],
    committed: &[TranscriptionResult],
    attempted_start_sequence: u64,
    process_diagnostic: String,
) -> Result<()> {
    let next_uncommitted_sequence = u64::try_from(committed.len())
        .ok()
        .and_then(|value| value.checked_add(1))
        .ok_or_else(|| anyhow!("transcription result sequence overflow"))?;
    let next_work_item_id = work_items
        .get(committed.len())
        .map_or("none", |item| item.id.as_str());
    let message = format!(
        "attempted_start_sequence={attempted_start_sequence}; \
         next_uncommitted_sequence={next_uncommitted_sequence}; \
         next_work_item_id={next_work_item_id}; process={process_diagnostic}"
    );
    publish_transcription_failure_and_wait(session, &message, &process_diagnostic)
}

fn record_result_integrity_failure(
    session: &mut SessionStore,
    attempted_start_sequence: u64,
    process_diagnostic: &str,
    integrity: &ResultAuthorityFailure,
) -> Result<()> {
    let message = format!(
        "attempted_start_sequence={attempted_start_sequence}; process={process_diagnostic}; \
         result_integrity_error={}; safely_validated_prefix_length={}; \
         earliest_unsafe_sequence={}; earliest_unsafe_work_item_id={}",
        integrity.message,
        integrity.safely_validated_prefix_length,
        integrity.earliest_unsafe_sequence,
        integrity.earliest_unsafe_work_item_id
    );
    publish_transcription_failure_and_wait(session, &message, process_diagnostic)
}

fn publish_transcription_failure_and_wait(
    session: &mut SessionStore,
    message: &str,
    process_diagnostic: &str,
) -> Result<()> {
    session
        .publish_transcription_failure(unix_millis_now()?, message)
        .context("failed to publish durable transcription failure")?;
    if let Err(error) = session.transition(WorkflowState::AwaitingOperator, unix_millis_now()?) {
        bail!(
            "transcription worker failed ({process_diagnostic}); failure is durable but the session \
             remains transcription_failed because awaiting_operator publication failed: {error}"
        );
    }
    bail!("transcription worker failed ({process_diagnostic}); session is awaiting operator action")
}

fn select_resume_plan(
    record: &SessionRecord,
    committed: &[TranscriptionResult],
    work_items: &[WorkItem],
    settings: &OfflineTranscriptionConfig,
) -> Result<ResumePlan> {
    let current_next = u64::try_from(committed.len())
        .ok()
        .and_then(|value| value.checked_add(1))
        .ok_or_else(|| anyhow!("transcription result sequence overflow"))?;
    let maximum_next = u64::try_from(work_items.len())
        .ok()
        .and_then(|value| value.checked_add(1))
        .ok_or_else(|| anyhow!("work-item sequence overflow"))?;

    let plan = match latest_resume_checkpoint(record)? {
        Some(ResumeCheckpoint::Prepared(sequence)) => ResumePlan {
            sequence,
            previously_prepared: true,
        },
        Some(ResumeCheckpoint::Applied(sequence)) if current_next == sequence => ResumePlan {
            // No forward progress: repeat the same boundary, not another rewind.
            sequence,
            previously_prepared: false,
        },
        Some(ResumeCheckpoint::Applied(sequence)) if current_next < sequence => {
            bail!(
                "result authority ends at sequence {} before the previously applied resume \
                 sequence {sequence}",
                current_next.saturating_sub(1)
            )
        }
        _ => ResumePlan {
            sequence: calculate_resume_sequence(committed, settings.resume_rewind_seconds)?,
            previously_prepared: false,
        },
    };

    if plan.sequence == 0 || plan.sequence > current_next || plan.sequence > maximum_next {
        bail!(
            "prepared transcription resume sequence {} is incompatible with committed prefix \
             ending at sequence {}",
            plan.sequence,
            current_next.saturating_sub(1)
        );
    }
    Ok(plan)
}

fn latest_resume_checkpoint(record: &SessionRecord) -> Result<Option<ResumeCheckpoint>> {
    for checkpoint in record.checkpoints.iter().rev() {
        if let Some(value) = checkpoint.stage.strip_prefix(RESUME_PREPARED_PREFIX) {
            return Ok(Some(ResumeCheckpoint::Prepared(parse_resume_sequence(
                value,
            )?)));
        }
        if let Some(value) = checkpoint.stage.strip_prefix(RESUME_APPLIED_PREFIX) {
            return Ok(Some(ResumeCheckpoint::Applied(parse_resume_sequence(
                value,
            )?)));
        }
    }
    Ok(None)
}

fn parse_resume_sequence(value: &str) -> Result<u64> {
    value
        .parse::<u64>()
        .ok()
        .filter(|sequence| *sequence != 0)
        .ok_or_else(|| anyhow!("invalid transcription resume checkpoint sequence {value:?}"))
}

fn calculate_resume_sequence(
    committed: &[TranscriptionResult],
    rewind_seconds: u64,
) -> Result<u64> {
    let current_next = u64::try_from(committed.len())
        .ok()
        .and_then(|value| value.checked_add(1))
        .ok_or_else(|| anyhow!("transcription result sequence overflow"))?;
    if rewind_seconds == 0 || committed.is_empty() {
        return Ok(current_next);
    }

    let committed_end = committed
        .last()
        .expect("non-empty committed prefix checked above")
        .end_ms;
    let boundary = committed_end.saturating_sub(rewind_seconds.saturating_mul(1_000));
    Ok(committed
        .iter()
        .find(|result| result.start_ms < committed_end && result.end_ms > boundary)
        .map_or(current_next, |result| result.sequence))
}

fn replace_results_prefix(path: &Path, results: &[TranscriptionResult]) -> Result<()> {
    let directory = path
        .parent()
        .ok_or_else(|| anyhow!("results path has no parent directory"))?;
    let temporary_path = directory.join(RESULTS_RESUME_TEMP_FILE_NAME);
    let file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&temporary_path)
        .with_context(|| format!("failed to create {}", temporary_path.display()))?;
    let mut writer = BufWriter::new(file);
    for result in results {
        serde_json::to_writer(&mut writer, result)?;
        writer.write_all(b"\n")?;
    }
    writer.flush()?;
    writer.get_ref().sync_all()?;
    drop(writer);
    fs::rename(&temporary_path, path)
        .with_context(|| format!("failed to replace results authority {}", path.display()))?;
    File::open(directory)?
        .sync_all()
        .context("failed to synchronise transcription directory")?;
    Ok(())
}

fn publish_final_transcript(session_directory: &Path, partial_path: &Path) -> Result<()> {
    let transcription_directory = session_directory.join(TRANSCRIPTION_DIRECTORY_NAME);
    let final_path = session_directory.join(FINAL_TRANSCRIPT_PATH);
    fs::rename(partial_path, &final_path).with_context(|| {
        format!(
            "failed to publish final transcript {}",
            final_path.display()
        )
    })?;
    File::open(&transcription_directory)?
        .sync_all()
        .context("failed to synchronise transcription directory")?;
    File::open(session_directory)?
        .sync_all()
        .context("failed to synchronise session directory")?;
    Ok(())
}

fn validate_entry_state(record: &SessionRecord) -> Result<()> {
    match record.state {
        WorkflowState::ReadyForTranscription => {
            if record.format != RECORDING_SESSION_FORMAT_VERSION
                || record.files.work_items.is_none()
                || record.files.results.is_some()
            {
                bail!(
                    "first transcription invocation requires a format-4 ready_for_transcription \
                     session with work_items and without results"
                );
            }
        }
        WorkflowState::Transcribing => {
            if record.format != SESSION_FORMAT_VERSION
                || record.files.work_items.is_none()
                || record.files.results.is_none()
            {
                bail!(
                    "controlled transcription restart requires a valid format-5 transcribing session"
                );
            }
        }
        state => bail!(
            "transcribe requires session state ready_for_transcription or transcribing; found {}",
            state.as_str()
        ),
    }
    Ok(())
}

fn validate_session_artifacts(
    session_directory: &Path,
    record: &SessionRecord,
) -> Result<TrackManifest> {
    for (name, relative_path) in [
        ("packet journal", record.files.packets.path.as_str()),
        ("playout journal", record.files.playout.path.as_str()),
        ("event journal", record.files.events.path.as_str()),
        (
            "participant snapshot",
            record.files.participants.path.as_str(),
        ),
        ("track manifest", record.files.tracks.path.as_str()),
    ] {
        let path = session_directory.join(relative_path);
        if !path
            .metadata()
            .with_context(|| format!("failed to inspect {name} {}", path.display()))?
            .is_file()
        {
            bail!("{name} {} is not a regular file", path.display());
        }
    }

    let participant_path = session_directory.join(&record.files.participants.path);
    ParticipantContext::load(&participant_path).with_context(|| {
        format!(
            "failed to validate participant snapshot {}",
            participant_path.display()
        )
    })?;

    let track_manifest_path = session_directory.join(&record.files.tracks.path);
    let tracks = TrackManifest::read(&track_manifest_path).with_context(|| {
        format!(
            "failed to validate routine track manifest {}",
            track_manifest_path.display()
        )
    })?;
    if tracks.session_id != record.session_id {
        bail!(
            "track manifest session ID {:?} does not match session.json ID {:?}",
            tracks.session_id,
            record.session_id
        );
    }
    let mut incomplete = Vec::new();
    for track in &tracks.tracks {
        if track.state != TrackState::Complete {
            incomplete.push(track.discord_user_id.clone());
            continue;
        }
        let path = session_directory.join(&track.path);
        if !path
            .metadata()
            .with_context(|| format!("failed to inspect routine track {}", path.display()))?
            .is_file()
        {
            bail!("routine track {} is not a regular file", path.display());
        }
    }
    if !incomplete.is_empty() {
        bail!(
            "cannot transcribe with incomplete routine tracks for Discord users {}",
            incomplete.join(", ")
        );
    }
    Ok(tracks)
}

fn read_work_manifest(
    path: &Path,
    record: &SessionRecord,
    tracks: &TrackManifest,
) -> Result<Vec<WorkItem>> {
    let items = read_work_manifest_authority(path, record)?;
    let tracks_by_user = tracks
        .tracks
        .iter()
        .map(|track| (track.discord_user_id.as_str(), track))
        .collect::<HashMap<_, _>>();
    for item in &items {
        validate_work_item_source(item, &tracks_by_user)?;
    }
    Ok(items)
}

fn read_work_manifest_authority(path: &Path, record: &SessionRecord) -> Result<Vec<WorkItem>> {
    let bytes = fs::read(path)
        .with_context(|| format!("failed to read work manifest {}", path.display()))?;
    if !bytes.is_empty() && !bytes.ends_with(b"\n") {
        bail!(
            "work manifest {} has a truncated final record",
            path.display()
        );
    }
    let mut items = Vec::new();
    let manifest_body = bytes.strip_suffix(b"\n").unwrap_or(&bytes);
    for (index, line) in manifest_body.split(|byte| *byte == b'\n').enumerate() {
        if line.is_empty() {
            if manifest_body.is_empty() {
                break;
            }
            bail!(
                "blank work manifest record at line {} in {}",
                index + 1,
                path.display()
            );
        }
        let item: WorkItem = serde_json::from_slice(line).with_context(|| {
            format!(
                "malformed work manifest record at line {} in {}",
                index + 1,
                path.display()
            )
        })?;
        validate_work_item_for_session(&item, record, items.last())?;
        items.push(item);
    }
    Ok(items)
}

fn validate_work_item_for_session(
    item: &WorkItem,
    record: &SessionRecord,
    previous: Option<&WorkItem>,
) -> Result<()> {
    let expected_sequence = previous.map_or(1, |value| value.sequence + 1);
    if item.format != WORK_ITEM_MANIFEST_FORMAT_VERSION
        || item.session_id != record.session_id
        || item.sequence != expected_sequence
        || item.id != format!("{}:{:06}", record.session_id, item.sequence)
        || item.start_ms >= item.end_ms
        || item.source_start_ms >= item.source_end_ms
        || item.speaker.trim().is_empty()
        || !matches!(item.role.as_str(), "player" | "gm")
        || item
            .discord_user_id
            .parse::<u64>()
            .ok()
            .is_none_or(|user_id| user_id == 0)
        || !is_safe_relative_path(&item.source)
    {
        bail!("invalid or out-of-order work item {:?}", item.id);
    }
    if let Some(previous) = previous {
        // The producer sorts sample-accurate ranges before format-1 publication
        // rounds their positions to milliseconds. Sequence therefore preserves
        // the deterministic producer order when displayed start times collide;
        // reconstructing another tie-break from lossy published fields would
        // falsely reject valid manifests.
        if item.start_ms < previous.start_ms {
            bail!("work manifest is not in deterministic chronological order");
        }
    }
    Ok(())
}

fn validate_work_item_source(
    item: &WorkItem,
    tracks: &HashMap<&str, &crate::track_manifest::TrackDescription>,
) -> Result<()> {
    let track = tracks.get(item.discord_user_id.as_str()).ok_or_else(|| {
        anyhow!(
            "work item {} has no complete routine track for Discord user {}",
            item.id,
            item.discord_user_id
        )
    })?;
    if item.source != track.path {
        bail!(
            "work item {} source {:?} does not match routine track {:?}",
            item.id,
            item.source,
            track.path
        );
    }
    let track_end_ms = track
        .length_samples
        .checked_add(47)
        .map(|samples| samples / 48)
        .ok_or_else(|| anyhow!("routine track duration overflow"))?;
    if item.source_end_ms > track_end_ms {
        bail!(
            "work item {} source range ends beyond its routine track",
            item.id
        );
    }
    Ok(())
}

fn is_safe_relative_path(value: &str) -> bool {
    !value.is_empty()
        && Path::new(value)
            .components()
            .all(|component| matches!(component, std::path::Component::Normal(_)))
}

fn prepare_empty_results(session_directory: &Path) -> Result<()> {
    let directory = session_directory.join(TRANSCRIPTION_DIRECTORY_NAME);
    fs::create_dir_all(&directory).with_context(|| {
        format!(
            "failed to create transcription directory {}",
            directory.display()
        )
    })?;
    File::open(session_directory)?
        .sync_all()
        .context("failed to synchronise session directory")?;

    let temporary_path = directory.join(RESULTS_TEMP_FILE_NAME);
    let final_path = directory.join(TRANSCRIPTION_RESULTS_FILE_NAME);
    let file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&temporary_path)
        .with_context(|| format!("failed to create {}", temporary_path.display()))?;
    file.sync_all()?;
    drop(file);
    fs::rename(&temporary_path, &final_path).with_context(|| {
        format!(
            "failed to publish results authority {}",
            final_path.display()
        )
    })?;
    File::open(&directory)?
        .sync_all()
        .context("failed to synchronise transcription directory")?;
    Ok(())
}

#[derive(Debug)]
struct ResultAuthorityFailure {
    message: String,
    safely_validated_prefix_length: usize,
    earliest_unsafe_sequence: u64,
    earliest_unsafe_work_item_id: String,
}

impl fmt::Display for ResultAuthorityFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{}; safely validated prefix length {}; earliest unsafe sequence {}; \
             earliest unsafe work item {}",
            self.message,
            self.safely_validated_prefix_length,
            self.earliest_unsafe_sequence,
            self.earliest_unsafe_work_item_id
        )
    }
}

impl std::error::Error for ResultAuthorityFailure {}

fn result_authority_failure(
    message: impl Into<String>,
    safely_validated_prefix_length: usize,
    work_items: &[WorkItem],
) -> ResultAuthorityFailure {
    let earliest_unsafe_sequence = u64::try_from(safely_validated_prefix_length)
        .ok()
        .and_then(|value| value.checked_add(1))
        .unwrap_or(u64::MAX);
    let earliest_unsafe_work_item_id = work_items
        .get(safely_validated_prefix_length)
        .map_or_else(|| "none".to_owned(), |item| item.id.clone());
    ResultAuthorityFailure {
        message: message.into(),
        safely_validated_prefix_length,
        earliest_unsafe_sequence,
        earliest_unsafe_work_item_id,
    }
}

fn validate_and_repair_result_prefix(
    path: &Path,
    work_items: &[WorkItem],
) -> Result<Vec<TranscriptionResult>> {
    validate_and_repair_result_prefix_detailed(path, work_items).map_err(anyhow::Error::new)
}

fn validate_and_repair_result_prefix_detailed(
    path: &Path,
    work_items: &[WorkItem],
) -> std::result::Result<Vec<TranscriptionResult>, ResultAuthorityFailure> {
    let bytes = fs::read(path).map_err(|error| {
        result_authority_failure(
            format!("failed to read results {}: {error}", path.display()),
            0,
            work_items,
        )
    })?;
    let committed_length = if bytes.is_empty() || bytes.ends_with(b"\n") {
        bytes.len()
    } else {
        bytes
            .iter()
            .rposition(|byte| *byte == b'\n')
            .map_or(0, |position| position + 1)
    };
    let committed = &bytes[..committed_length];
    let mut results = Vec::new();
    let result_body = committed.strip_suffix(b"\n").unwrap_or(committed);
    for (index, line) in result_body.split(|byte| *byte == b'\n').enumerate() {
        if line.is_empty() {
            if result_body.is_empty() {
                break;
            }
            return Err(result_authority_failure(
                format!(
                    "blank interior transcription result at line {} in {}",
                    index + 1,
                    path.display()
                ),
                results.len(),
                work_items,
            ));
        }
        let result: TranscriptionResult = serde_json::from_slice(line).map_err(|error| {
            result_authority_failure(
                format!(
                    "malformed interior transcription result at line {} in {}: {error}",
                    index + 1,
                    path.display()
                ),
                results.len(),
                work_items,
            )
        })?;
        let work_item = work_items.get(results.len()).ok_or_else(|| {
            result_authority_failure(
                format!(
                    "transcription results contain sequence {} beyond the work manifest",
                    result.sequence
                ),
                results.len(),
                work_items,
            )
        })?;
        validate_result_matches_work_item(&result, work_item).map_err(|error| {
            result_authority_failure(error.to_string(), results.len(), work_items)
        })?;
        results.push(result);
    }

    if committed_length != bytes.len() {
        let file = OpenOptions::new().write(true).open(path).map_err(|error| {
            result_authority_failure(
                format!(
                    "failed to open truncated results {} for repair: {error}",
                    path.display()
                ),
                results.len(),
                work_items,
            )
        })?;
        file.set_len(u64::try_from(committed_length).map_err(|_| {
            result_authority_failure(
                "result prefix length does not fit in u64",
                results.len(),
                work_items,
            )
        })?)
        .map_err(|error| {
            result_authority_failure(
                format!("failed to truncate result byte tail: {error}"),
                results.len(),
                work_items,
            )
        })?;
        file.sync_all().map_err(|error| {
            result_authority_failure(
                format!("failed to synchronise repaired results: {error}"),
                results.len(),
                work_items,
            )
        })?;
        if let Some(directory) = path.parent() {
            File::open(directory)
                .and_then(|directory| directory.sync_all())
                .map_err(|error| {
                    result_authority_failure(
                        format!("failed to synchronise repaired result directory: {error}"),
                        results.len(),
                        work_items,
                    )
                })?;
        }
    }
    Ok(results)
}

fn validate_result_matches_work_item(result: &TranscriptionResult, item: &WorkItem) -> Result<()> {
    if result.format != TRANSCRIPTION_RESULT_FORMAT_VERSION
        || result.status != "complete"
        || result.work_item_id != item.id
        || result.session_id != item.session_id
        || result.sequence != item.sequence
        || result.discord_user_id != item.discord_user_id
        || result.speaker != item.speaker
        || result.role != item.role
        || result.character != item.character
        || result.start_ms != item.start_ms
        || result.end_ms != item.end_ms
        || result.source != item.source
        || result.source_start_ms != item.source_start_ms
        || result.source_end_ms != item.source_end_ms
        || result.text.contains(['\n', '\r'])
    {
        bail!(
            "transcription result sequence {} does not match work item {}",
            result.sequence,
            item.id
        );
    }
    Ok(())
}

fn rebuild_partial_transcript(
    session_directory: &Path,
    path: &Path,
    results: &[TranscriptionResult],
) -> Result<()> {
    let temporary_path = session_directory.join(PARTIAL_TRANSCRIPT_TEMP_FILE_NAME);
    let file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&temporary_path)
        .with_context(|| format!("failed to create {}", temporary_path.display()))?;
    let mut writer = BufWriter::new(file);
    for result in results {
        if !result.text.trim().is_empty() {
            writer.write_all(transcript_line(result).as_bytes())?;
        }
    }
    writer.flush()?;
    writer.get_ref().sync_all()?;
    drop(writer);
    fs::rename(&temporary_path, path)
        .with_context(|| format!("failed to publish partial transcript {}", path.display()))?;
    File::open(session_directory)?
        .sync_all()
        .context("failed to synchronise session directory")?;
    Ok(())
}

fn write_final_transcript_atomically(
    session_directory: &Path,
    results: &[TranscriptionResult],
) -> Result<()> {
    let directory = session_directory.join(TRANSCRIPTION_DIRECTORY_NAME);
    let temporary_path = directory.join(FINAL_TRANSCRIPT_TEMP_FILE_NAME);
    let final_path = session_directory.join(FINAL_TRANSCRIPT_PATH);
    let file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&temporary_path)
        .with_context(|| format!("failed to create {}", temporary_path.display()))?;
    let mut writer = BufWriter::new(file);
    for result in results {
        if !result.text.trim().is_empty() {
            writer.write_all(transcript_line(result).as_bytes())?;
        }
    }
    writer.flush()?;
    writer.get_ref().sync_all()?;
    drop(writer);
    fs::rename(&temporary_path, &final_path).with_context(|| {
        format!(
            "failed to publish final transcript {}",
            final_path.display()
        )
    })?;
    File::open(&directory)?
        .sync_all()
        .context("failed to synchronise transcription directory")?;
    Ok(())
}

fn transcript_line(result: &TranscriptionResult) -> String {
    let elapsed_seconds = result.start_ms / 1_000;
    let hours = elapsed_seconds / 3_600;
    let minutes = (elapsed_seconds % 3_600) / 60;
    let seconds = elapsed_seconds % 60;
    format!(
        "[{hours:02}:{minutes:02}:{seconds:02}] {}: {}\n",
        result.speaker, result.text
    )
}

fn python_interpreter() -> Result<OsString> {
    select_python_interpreter(
        env::var_os("ECHOSCRIBE_PYTHON"),
        application_root(),
        cfg!(windows),
    )
}

fn select_python_interpreter(
    explicit: Option<OsString>,
    application_root: &Path,
    windows: bool,
) -> Result<OsString> {
    match explicit {
        Some(value) if !value.is_empty() => return Ok(value),
        Some(_) => bail!("ECHOSCRIBE_PYTHON is set but empty"),
        None => {}
    }

    let virtual_environment = if windows {
        application_root
            .join(".venv")
            .join("Scripts")
            .join("python.exe")
    } else {
        application_root.join(".venv").join("bin").join("python")
    };
    if virtual_environment.is_file() {
        return Ok(virtual_environment.into_os_string());
    }

    if windows {
        Ok(OsString::from("python"))
    } else {
        Ok(OsString::from("python3"))
    }
}

fn application_root() -> &'static Path {
    Path::new(env!("CARGO_MANIFEST_DIR"))
}

fn worker_script_path() -> PathBuf {
    application_root()
        .join("workers")
        .join("faster-whisper")
        .join("transcription_worker.py")
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
        sync::{Arc, Mutex, mpsc},
        thread,
        time::{Duration, SystemTime, UNIX_EPOCH},
    };

    use super::*;
    use crate::{
        artifacts::{
            EVENT_JOURNAL_FILE_NAME, PACKET_JOURNAL_FILE_NAME, PARTICIPANT_SNAPSHOT_FILE_NAME,
            PLAYOUT_JOURNAL_FILE_NAME, TRACK_DIRECTORY_NAME, TRACK_MANIFEST_FILE_NAME,
            WORK_ITEM_MANIFEST_PATH,
        },
        participants::ParticipantContext,
        session::{NewSession, fail_record_write_after},
        track_manifest::TrackDescription,
    };

    #[derive(Clone, Debug, Eq, PartialEq)]
    struct ObservedInvocation {
        next_sequence: u64,
        manifest_path: PathBuf,
        results_path: PathBuf,
        transcript_path: PathBuf,
        hotwords: Vec<String>,
    }

    struct FakeWorker {
        exit: WorkerExit,
        commit_all: bool,
        invocations: Mutex<Vec<ObservedInvocation>>,
    }

    impl FakeWorker {
        fn successful() -> Self {
            Self {
                exit: WorkerExit {
                    success: true,
                    code: Some(0),
                },
                commit_all: true,
                invocations: Mutex::new(Vec::new()),
            }
        }

        fn failed() -> Self {
            Self {
                exit: WorkerExit {
                    success: false,
                    code: Some(17),
                },
                commit_all: false,
                invocations: Mutex::new(Vec::new()),
            }
        }

        fn signal_failure() -> Self {
            Self {
                exit: WorkerExit {
                    success: false,
                    code: None,
                },
                commit_all: false,
                invocations: Mutex::new(Vec::new()),
            }
        }
    }

    impl WorkerProcess for FakeWorker {
        fn run(
            &self,
            invocation: &WorkerInvocation<'_>,
            _lease: &SessionOperationLease,
        ) -> Result<WorkerExit> {
            self.invocations.lock().unwrap().push(ObservedInvocation {
                next_sequence: invocation.next_sequence,
                manifest_path: invocation.manifest_path.to_owned(),
                results_path: invocation.results_path.to_owned(),
                transcript_path: invocation.transcript_path.to_owned(),
                hotwords: invocation.settings.hotwords.clone(),
            });
            if self.commit_all {
                commit_remaining_results(invocation, usize::MAX)?;
            }
            Ok(self.exit)
        }
    }

    struct LaunchFailureWorker;

    impl WorkerProcess for LaunchFailureWorker {
        fn run(
            &self,
            _invocation: &WorkerInvocation<'_>,
            _lease: &SessionOperationLease,
        ) -> Result<WorkerExit> {
            bail!("injected worker launch failure")
        }
    }

    struct ProgressThenFailureWorker {
        count: usize,
        invocations: Mutex<Vec<ObservedInvocation>>,
    }

    impl WorkerProcess for ProgressThenFailureWorker {
        fn run(
            &self,
            invocation: &WorkerInvocation<'_>,
            _lease: &SessionOperationLease,
        ) -> Result<WorkerExit> {
            self.invocations.lock().unwrap().push(ObservedInvocation {
                next_sequence: invocation.next_sequence,
                manifest_path: invocation.manifest_path.to_owned(),
                results_path: invocation.results_path.to_owned(),
                transcript_path: invocation.transcript_path.to_owned(),
                hotwords: invocation.settings.hotwords.clone(),
            });
            commit_remaining_results(invocation, self.count)?;
            Ok(WorkerExit {
                success: false,
                code: Some(23),
            })
        }
    }

    struct MalformedThenFailureWorker;

    impl WorkerProcess for MalformedThenFailureWorker {
        fn run(
            &self,
            invocation: &WorkerInvocation<'_>,
            _lease: &SessionOperationLease,
        ) -> Result<WorkerExit> {
            commit_remaining_results(invocation, 1)?;
            let mut results = OpenOptions::new()
                .append(true)
                .open(invocation.results_path)?;
            results.write_all(b"{malformed complete record}\n")?;
            results.sync_all()?;
            Ok(WorkerExit {
                success: false,
                code: Some(31),
            })
        }
    }

    struct MismatchedThenZeroWorker;

    impl WorkerProcess for MismatchedThenZeroWorker {
        fn run(
            &self,
            invocation: &WorkerInvocation<'_>,
            _lease: &SessionOperationLease,
        ) -> Result<WorkerExit> {
            let manifest = fs::read_to_string(invocation.manifest_path)?;
            let item: WorkItem = serde_json::from_str(
                manifest
                    .lines()
                    .next()
                    .ok_or_else(|| anyhow!("test manifest is empty"))?,
            )?;
            let mut result = complete_result(&item, "Mismatched");
            result.speaker = "Wrong speaker".to_owned();
            let mut results = OpenOptions::new()
                .append(true)
                .open(invocation.results_path)?;
            serde_json::to_writer(&mut results, &result)?;
            results.write_all(b"\n")?;
            results.sync_all()?;
            Ok(WorkerExit {
                success: true,
                code: Some(0),
            })
        }
    }

    struct BlockingWorker {
        started: mpsc::Sender<()>,
        release: Mutex<mpsc::Receiver<()>>,
    }

    impl WorkerProcess for BlockingWorker {
        fn run(
            &self,
            _invocation: &WorkerInvocation<'_>,
            _lease: &SessionOperationLease,
        ) -> Result<WorkerExit> {
            self.started.send(()).unwrap();
            self.release.lock().unwrap().recv().unwrap();
            Ok(WorkerExit {
                success: true,
                code: Some(0),
            })
        }
    }

    #[test]
    fn python_interpreter_prefers_override_then_root_virtual_environment() {
        let directory = test_directory("python-selection");
        let explicit = OsString::from("chosen-python");
        assert_eq!(
            select_python_interpreter(Some(explicit.clone()), &directory, false).unwrap(),
            explicit
        );
        assert!(
            select_python_interpreter(Some(OsString::new()), &directory, false)
                .unwrap_err()
                .to_string()
                .contains("set but empty")
        );

        assert_eq!(
            select_python_interpreter(None, &directory, false).unwrap(),
            OsString::from("python3")
        );
        assert_eq!(
            select_python_interpreter(None, &directory, true).unwrap(),
            OsString::from("python")
        );

        let posix_python = directory.join(".venv").join("bin").join("python");
        fs::create_dir_all(posix_python.parent().unwrap()).unwrap();
        fs::write(&posix_python, b"").unwrap();
        assert_eq!(
            select_python_interpreter(None, &directory, false).unwrap(),
            posix_python.as_os_str()
        );

        let windows_python = directory.join(".venv").join("Scripts").join("python.exe");
        fs::create_dir_all(windows_python.parent().unwrap()).unwrap();
        fs::write(&windows_python, b"").unwrap();
        assert_eq!(
            select_python_interpreter(None, &directory, true).unwrap(),
            windows_python.as_os_str()
        );
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn worker_path_is_rooted_at_the_application_manifest() {
        assert_eq!(
            worker_script_path(),
            application_root()
                .join("workers")
                .join("faster-whisper")
                .join("transcription_worker.py")
        );
    }

    #[test]
    fn worker_arguments_include_explicit_vad_setting() {
        assert_eq!(
            vad_worker_arguments(true),
            [OsString::from("--vad-enabled"), OsString::from("true")]
        );
        assert_eq!(
            vad_worker_arguments(false),
            [OsString::from("--vad-enabled"), OsString::from("false")]
        );
    }

    #[test]
    fn work_manifest_accepts_equal_start_with_numeric_id_order() {
        let (directory, _, mut first) = ready_session("manifest-numeric-id-order");
        first.discord_user_id = "333965420539150337".to_owned();
        first.start_ms = 646_300;
        first.end_ms = 647_000;
        first.source_start_ms = 646_300;
        first.source_end_ms = 647_000;
        let mut second = first.clone();
        second.sequence = 2;
        second.id = "session-transcription:000002".to_owned();
        second.discord_user_id = "1070186824502358139".to_owned();

        let items = read_test_work_manifest_authority(&directory, &[first, second]).unwrap();

        assert_eq!(items.len(), 2);
        assert_eq!(items[0].discord_user_id, "333965420539150337");
        assert_eq!(items[1].discord_user_id, "1070186824502358139");
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn work_manifest_equal_start_uses_sequence_for_either_id_order() {
        let (directory, _, mut first) = ready_session("manifest-either-id-order");
        first.start_ms = 500;
        first.end_ms = 700;
        first.source_start_ms = 500;
        first.source_end_ms = 700;

        for (first_id, second_id) in [
            ("333965420539150337", "1070186824502358139"),
            ("1070186824502358139", "333965420539150337"),
        ] {
            first.discord_user_id = first_id.to_owned();
            let mut second = first.clone();
            second.sequence = 2;
            second.id = "session-transcription:000002".to_owned();
            second.discord_user_id = second_id.to_owned();

            read_test_work_manifest_authority(&directory, &[first.clone(), second]).unwrap();
        }

        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn work_manifest_accepts_exact_sample_starts_rounded_to_same_millisecond() {
        const SAMPLES_PER_MILLISECOND: u64 = 48;
        let exact_first_start = 646_300 * SAMPLES_PER_MILLISECOND + 1;
        let exact_second_start = exact_first_start + 1;
        assert_ne!(exact_first_start, exact_second_start);
        assert_eq!(
            exact_first_start / SAMPLES_PER_MILLISECOND,
            exact_second_start / SAMPLES_PER_MILLISECOND
        );

        let (directory, _, mut first) = ready_session("manifest-rounded-samples");
        first.discord_user_id = "9".to_owned();
        first.start_ms = exact_first_start / SAMPLES_PER_MILLISECOND;
        first.end_ms = first.start_ms + 100;
        first.source_start_ms = first.start_ms;
        first.source_end_ms = first.end_ms;
        let mut second = first.clone();
        second.sequence = 2;
        second.id = "session-transcription:000002".to_owned();
        second.discord_user_id = "10".to_owned();
        second.start_ms = exact_second_start / SAMPLES_PER_MILLISECOND;
        second.source_start_ms = second.start_ms;

        read_test_work_manifest_authority(&directory, &[first, second]).unwrap();

        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn work_manifest_rejects_decreasing_start_time() {
        let (directory, _, mut first) = ready_session("manifest-decreasing-time");
        first.start_ms = 500;
        first.end_ms = 700;
        first.source_start_ms = 500;
        first.source_end_ms = 700;
        let mut second = test_item(2, 499, 701);
        second.source_start_ms = second.start_ms;
        second.source_end_ms = second.end_ms;

        let error = read_test_work_manifest_authority(&directory, &[first, second]).unwrap_err();

        assert!(
            error
                .to_string()
                .contains("not in deterministic chronological order")
        );
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn work_manifest_still_rejects_sequence_gap_and_duplicate() {
        let (directory, _, first) = ready_session("manifest-sequence-errors");
        for invalid_sequence in [1, 3] {
            let mut second = test_item(invalid_sequence, first.end_ms, first.end_ms + 100);
            second.id = format!("session-transcription:{invalid_sequence:06}");

            assert!(
                read_test_work_manifest_authority(&directory, &[first.clone(), second]).is_err()
            );
        }
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn work_manifest_still_validates_fields_ranges_session_and_source_path() {
        let (directory, _, item) = ready_session("manifest-field-validation");
        let mut invalid_items = Vec::new();

        let mut invalid = item.clone();
        invalid.format += 1;
        invalid_items.push(invalid);

        let mut invalid = item.clone();
        invalid.session_id = "another-session".to_owned();
        invalid_items.push(invalid);

        let mut invalid = item.clone();
        invalid.id = "not-canonical".to_owned();
        invalid_items.push(invalid);

        let mut invalid = item.clone();
        invalid.end_ms = invalid.start_ms;
        invalid_items.push(invalid);

        let mut invalid = item.clone();
        invalid.source_end_ms = invalid.source_start_ms;
        invalid_items.push(invalid);

        let mut invalid = item.clone();
        invalid.speaker = "   ".to_owned();
        invalid_items.push(invalid);

        let mut invalid = item.clone();
        invalid.role = "spectator".to_owned();
        invalid_items.push(invalid);

        let mut invalid = item.clone();
        invalid.discord_user_id = "0".to_owned();
        invalid_items.push(invalid);

        let mut invalid = item.clone();
        invalid.discord_user_id = "not-a-discord-id".to_owned();
        invalid_items.push(invalid);

        let mut invalid = item.clone();
        invalid.source = "/tmp/user-11.flac".to_owned();
        invalid_items.push(invalid);

        let mut invalid = item;
        invalid.source = "tracks/../../user-11.flac".to_owned();
        invalid_items.push(invalid);

        for invalid in invalid_items {
            assert!(read_test_work_manifest_authority(&directory, &[invalid]).is_err());
        }
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn first_invocation_publishes_results_and_state_before_one_worker() {
        let (directory, config_path, item) = ready_session("first");
        fs::write(
            directory.join("vocabulary.txt"),
            " Emperor Coaltongue \n\nDragon Lance\n",
        )
        .unwrap();
        let worker = FakeWorker::successful();

        run_with_worker(&directory, &config_path, &worker).unwrap();

        let session = SessionStore::load(&directory).unwrap();
        assert_eq!(session.record().format, SESSION_FORMAT_VERSION);
        assert_eq!(session.record().state, WorkflowState::Complete);
        assert_eq!(
            session.record().files.results.as_ref().unwrap().path,
            crate::artifacts::TRANSCRIPTION_RESULTS_PATH
        );
        assert_eq!(
            validate_and_repair_result_prefix(
                &directory.join(crate::artifacts::TRANSCRIPTION_RESULTS_PATH),
                &[item.clone()]
            )
            .unwrap()
            .len(),
            1
        );
        assert!(!directory.join(PARTIAL_TRANSCRIPT_FILE_NAME).exists());
        assert!(directory.join(FINAL_TRANSCRIPT_PATH).is_file());
        let invocations = worker.invocations.lock().unwrap();
        assert_eq!(invocations.len(), 1);
        assert_eq!(invocations[0].next_sequence, item.sequence);
        assert_eq!(
            invocations[0].hotwords,
            ["Emperor Coaltongue", "Dragon Lance"]
        );
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn rebuild_transcript_replaces_only_the_complete_display_artefact() {
        let (directory, config_path, _) = ready_session("rebuild-complete");
        run_with_worker(&directory, &config_path, &FakeWorker::successful()).unwrap();
        let results_path = directory.join(crate::artifacts::TRANSCRIPTION_RESULTS_PATH);
        let final_path = directory.join(FINAL_TRANSCRIPT_PATH);
        let results_before = fs::read(&results_path).unwrap();
        let session_before = fs::read(directory.join("session.json")).unwrap();
        for name in [
            PACKET_JOURNAL_FILE_NAME,
            PLAYOUT_JOURNAL_FILE_NAME,
            EVENT_JOURNAL_FILE_NAME,
            PARTICIPANT_SNAPSHOT_FILE_NAME,
            TRACK_MANIFEST_FILE_NAME,
        ] {
            fs::remove_file(directory.join(name)).unwrap();
        }
        fs::remove_dir_all(directory.join(TRACK_DIRECTORY_NAME)).unwrap();
        fs::write(&final_path, b"stale display text\n").unwrap();

        rebuild_transcript(&directory).unwrap();

        assert_eq!(
            fs::read_to_string(&final_path).unwrap(),
            "[00:00:00] Alice: Result 1\n"
        );
        assert_eq!(fs::read(&results_path).unwrap(), results_before);
        assert_eq!(
            fs::read(directory.join("session.json")).unwrap(),
            session_before
        );
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn rebuild_transcript_refuses_incomplete_or_truncated_authority_without_mutation() {
        let (directory, config_path, _) = ready_session("rebuild-refusal");
        run_with_worker(&directory, &config_path, &FakeWorker::successful()).unwrap();
        let results_path = directory.join(crate::artifacts::TRANSCRIPTION_RESULTS_PATH);
        let final_path = directory.join(FINAL_TRANSCRIPT_PATH);
        let mut truncated = fs::read(&results_path).unwrap();
        truncated.extend_from_slice(b"{\"format\":");
        fs::write(&results_path, &truncated).unwrap();
        fs::write(&final_path, b"retain me\n").unwrap();

        let error = rebuild_transcript(&directory).unwrap_err();

        assert!(error.to_string().contains("truncated final result"));
        assert_eq!(fs::read(&results_path).unwrap(), truncated);
        assert_eq!(fs::read(&final_path).unwrap(), b"retain me\n");
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn controlled_restart_repairs_tail_rebuilds_text_and_skips_prefix() {
        let (directory, config_path, item) = ready_session("restart");
        start_transcribing_session(&directory);
        let result = complete_result(&item, "All conversation stays here.");
        let mut bytes = serde_json::to_vec(&result).unwrap();
        bytes.extend_from_slice(b"\n{\"format\":");
        fs::write(
            directory.join(crate::artifacts::TRANSCRIPTION_RESULTS_PATH),
            bytes,
        )
        .unwrap();
        fs::write(
            directory.join(PARTIAL_TRANSCRIPT_FILE_NAME),
            "stale duplicate\n",
        )
        .unwrap();
        let worker = FakeWorker::successful();

        run_with_worker(&directory, &config_path, &worker).unwrap();

        let expected_json = format!("{}\n", serde_json::to_string(&result).unwrap());
        assert_eq!(
            fs::read_to_string(directory.join(crate::artifacts::TRANSCRIPTION_RESULTS_PATH))
                .unwrap(),
            expected_json
        );
        assert_eq!(
            fs::read_to_string(directory.join(FINAL_TRANSCRIPT_PATH)).unwrap(),
            "[00:00:00] Alice: All conversation stays here.\n"
        );
        let invocations = worker.invocations.lock().unwrap();
        assert_eq!(invocations.len(), 1);
        assert_eq!(invocations[0].next_sequence, 2);
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn active_worker_excludes_second_invocation_before_output_repair() {
        let (directory, config_path, _) = ready_session("exclusive-lease");
        let (started_sender, started_receiver) = mpsc::channel();
        let (release_sender, release_receiver) = mpsc::channel();
        let first_directory = directory.clone();
        let first_config = config_path.clone();
        let first = thread::spawn(move || {
            let worker = BlockingWorker {
                started: started_sender,
                release: Mutex::new(release_receiver),
            };
            run_with_worker(&first_directory, &first_config, &worker)
        });
        started_receiver
            .recv_timeout(Duration::from_secs(5))
            .expect("first worker did not start");

        let results_path = directory.join(crate::artifacts::TRANSCRIPTION_RESULTS_PATH);
        let transcript_path = directory.join(PARTIAL_TRANSCRIPT_FILE_NAME);
        fs::write(&results_path, b"{\"unfinished\":").unwrap();
        fs::write(&transcript_path, b"must not be rebuilt\n").unwrap();
        let second_worker = FakeWorker::successful();

        let error = run_with_worker(&directory, &config_path, &second_worker).unwrap_err();

        assert!(
            error
                .to_string()
                .contains("another mutating operation is already active")
        );
        assert_eq!(fs::read(&results_path).unwrap(), b"{\"unfinished\":");
        assert_eq!(
            fs::read(&transcript_path).unwrap(),
            b"must not be rebuilt\n"
        );
        assert!(second_worker.invocations.lock().unwrap().is_empty());

        release_sender.send(()).unwrap();
        assert!(first.join().unwrap().is_err());
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn active_worker_excludes_work_manifest_rebuild() {
        let (directory, config_path, _) = ready_session("worker-excludes-builder");
        let work_items_path = directory.join(WORK_ITEM_MANIFEST_PATH);
        let work_items_before = fs::read(&work_items_path).unwrap();
        let (started_sender, started_receiver) = mpsc::channel();
        let (release_sender, release_receiver) = mpsc::channel();
        let worker_directory = directory.clone();
        let worker_config = config_path.clone();
        let active = thread::spawn(move || {
            let worker = BlockingWorker {
                started: started_sender,
                release: Mutex::new(release_receiver),
            };
            run_with_worker(&worker_directory, &worker_config, &worker)
        });
        started_receiver
            .recv_timeout(Duration::from_secs(5))
            .expect("blocking worker did not start");

        let error = crate::work_items::run(&directory, &config_path).unwrap_err();

        assert!(
            error
                .to_string()
                .contains("another mutating operation is already active")
        );
        assert_eq!(fs::read(&work_items_path).unwrap(), work_items_before);

        release_sender.send(()).unwrap();
        assert!(active.join().unwrap().is_err());
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn delayed_invocation_reloads_authority_after_acquiring_lease() {
        let (directory, config_path, _) = ready_session("stale-authority");
        let (observed_sender, observed_receiver) = mpsc::channel();
        let (resume_sender, resume_receiver) = mpsc::channel();
        let delayed_directory = directory.clone();
        let delayed_config = config_path.clone();
        let delayed_worker = Arc::new(FakeWorker::successful());
        let delayed_worker_thread = Arc::clone(&delayed_worker);
        let delayed = thread::spawn(move || {
            run_with_worker_before_lease(
                &delayed_directory,
                &delayed_config,
                delayed_worker_thread.as_ref(),
                || {
                    let stale = SessionStore::load(&delayed_directory).unwrap();
                    assert_eq!(stale.record().state, WorkflowState::ReadyForTranscription);
                    observed_sender.send(()).unwrap();
                    resume_receiver.recv().unwrap();
                },
            )
        });
        observed_receiver
            .recv_timeout(Duration::from_secs(5))
            .expect("delayed invocation did not observe old authority");

        run_with_worker(&directory, &config_path, &FakeWorker::successful()).unwrap();
        let session_before = fs::read(directory.join("session.json")).unwrap();
        let results_before =
            fs::read(directory.join(crate::artifacts::TRANSCRIPTION_RESULTS_PATH)).unwrap();
        let final_before = fs::read(directory.join(FINAL_TRANSCRIPT_PATH)).unwrap();
        assert!(!directory.join(PARTIAL_TRANSCRIPT_FILE_NAME).exists());

        resume_sender.send(()).unwrap();
        let error = delayed.join().unwrap().unwrap_err();

        assert!(error.to_string().contains("found complete"));
        assert!(delayed_worker.invocations.lock().unwrap().is_empty());
        assert_eq!(
            fs::read(directory.join("session.json")).unwrap(),
            session_before
        );
        assert_eq!(
            fs::read(directory.join(crate::artifacts::TRANSCRIPTION_RESULTS_PATH)).unwrap(),
            results_before
        );
        assert_eq!(
            fs::read(directory.join(FINAL_TRANSCRIPT_PATH)).unwrap(),
            final_before
        );
        assert!(!directory.join(PARTIAL_TRANSCRIPT_FILE_NAME).exists());
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn delayed_configured_continue_reloads_authority_after_lease() {
        let (directory, config_path, _) =
            failed_session_with_results("stale-continue-authority", &[(0, 10_000)], 0);
        let (observed_sender, observed_receiver) = mpsc::channel();
        let (resume_sender, resume_receiver) = mpsc::channel();
        let delayed_directory = directory.clone();
        let delayed_config = config_path.clone();
        let delayed_worker = Arc::new(FakeWorker::successful());
        let delayed_worker_thread = Arc::clone(&delayed_worker);
        let delayed = thread::spawn(move || {
            continue_with_worker_before_lease(
                &delayed_directory,
                &delayed_config,
                delayed_worker_thread.as_ref(),
                || {
                    let stale = SessionStore::load(&delayed_directory).unwrap();
                    assert_eq!(stale.record().state, WorkflowState::AwaitingOperator);
                    observed_sender.send(()).unwrap();
                    resume_receiver.recv().unwrap();
                },
            )
        });
        observed_receiver
            .recv_timeout(Duration::from_secs(5))
            .expect("delayed continuation did not observe old authority");

        continue_with_worker(&directory, &config_path, &FakeWorker::successful()).unwrap();
        let session_before = fs::read(directory.join("session.json")).unwrap();
        let results_before =
            fs::read(directory.join(crate::artifacts::TRANSCRIPTION_RESULTS_PATH)).unwrap();
        let final_before = fs::read(directory.join(FINAL_TRANSCRIPT_PATH)).unwrap();

        resume_sender.send(()).unwrap();
        let error = delayed.join().unwrap().unwrap_err();

        assert!(error.to_string().contains("found format 5 state complete"));
        assert!(delayed_worker.invocations.lock().unwrap().is_empty());
        assert_eq!(
            fs::read(directory.join("session.json")).unwrap(),
            session_before
        );
        assert_eq!(
            fs::read(directory.join(crate::artifacts::TRANSCRIPTION_RESULTS_PATH)).unwrap(),
            results_before
        );
        assert_eq!(
            fs::read(directory.join(FINAL_TRANSCRIPT_PATH)).unwrap(),
            final_before
        );
        assert!(!directory.join(PARTIAL_TRANSCRIPT_FILE_NAME).exists());
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn duplicated_worker_handle_retains_lease_until_last_close() {
        let (directory, _, _) = ready_session("inherited-lease");
        let lease = SessionOperationLease::acquire(&directory).unwrap();
        let inherited = lease.inherited_handle().unwrap();
        drop(lease);

        let error = SessionOperationLease::acquire(&directory).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("another mutating operation is already active")
        );

        drop(inherited);
        let recovered = SessionOperationLease::acquire(&directory).unwrap();
        drop(recovered);
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn inherited_handle_excludes_restart_for_orphan_child_lifetime() {
        const CHILD_ENVIRONMENT: &str = "ECHOSCRIBE_TEST_LEASE_CHILD";
        const TEST_NAME: &str =
            "transcription::tests::inherited_handle_excludes_restart_for_orphan_child_lifetime";

        if env::var_os(CHILD_ENVIRONMENT).is_some() {
            thread::sleep(Duration::from_secs(30));
            return;
        }

        let (directory, _, _) = ready_session("orphan-child-lease");
        let lease = SessionOperationLease::acquire(&directory).unwrap();
        let mut child = process::Command::new(env::current_exe().unwrap())
            .arg("--exact")
            .arg(TEST_NAME)
            .arg("--nocapture")
            .env(CHILD_ENVIRONMENT, "1")
            .stdin(Stdio::from(lease.inherited_handle().unwrap()))
            .spawn()
            .unwrap();
        drop(lease);

        let conflict = SessionOperationLease::acquire(&directory);
        let child_status = child.try_wait().unwrap();
        child.kill().unwrap();
        child.wait().unwrap();

        assert!(child_status.is_none(), "lease child exited unexpectedly");
        let error = conflict.unwrap_err();
        assert!(
            error
                .to_string()
                .contains("another mutating operation is already active")
        );

        let recovered = SessionOperationLease::acquire(&directory).unwrap();
        drop(recovered);
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn malformed_interior_result_is_rejected_without_worker_launch() {
        let (directory, config_path, item) = ready_session("bad-interior");
        start_transcribing_session(&directory);
        let result = complete_result(&item, "Committed");
        fs::write(
            directory.join(crate::artifacts::TRANSCRIPTION_RESULTS_PATH),
            format!(
                "{}\n{{not-json}}\n",
                serde_json::to_string(&result).unwrap()
            ),
        )
        .unwrap();
        let worker = FakeWorker::successful();

        let error = run_with_worker(&directory, &config_path, &worker).unwrap_err();

        assert!(format!("{error:#}").contains("malformed interior"));
        assert!(worker.invocations.lock().unwrap().is_empty());
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn result_prefix_rejects_gaps_duplicates_and_mismatched_complete_records() {
        let directory = test_directory("invalid-prefixes");
        let path = directory.join("results.jsonl");
        let first = test_item(1, 0, 100);
        let second = test_item(2, 100, 200);
        let items = [first.clone(), second.clone()];

        let gap = complete_result(&second, "gap");
        fs::write(&path, format!("{}\n", serde_json::to_string(&gap).unwrap())).unwrap();
        assert!(
            validate_and_repair_result_prefix(&path, &items)
                .unwrap_err()
                .to_string()
                .contains("does not match")
        );

        let duplicate = complete_result(&first, "duplicate");
        fs::write(
            &path,
            format!(
                "{}\n{}\n",
                serde_json::to_string(&duplicate).unwrap(),
                serde_json::to_string(&duplicate).unwrap()
            ),
        )
        .unwrap();
        assert!(validate_and_repair_result_prefix(&path, &items).is_err());

        let mut mismatched = complete_result(&first, "mismatch");
        mismatched.speaker = "Someone else".to_owned();
        fs::write(
            &path,
            format!("{}\n", serde_json::to_string(&mismatched).unwrap()),
        )
        .unwrap();
        assert!(validate_and_repair_result_prefix(&path, &items).is_err());

        let mut incomplete = complete_result(&first, "incomplete");
        incomplete.status = "failed".to_owned();
        fs::write(
            &path,
            format!("{}\n", serde_json::to_string(&incomplete).unwrap()),
        )
        .unwrap();
        assert!(validate_and_repair_result_prefix(&path, &items).is_err());
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn failed_session_publication_does_not_launch_worker() {
        let (directory, config_path, _) = ready_session("publish-failure");
        fail_record_write_after(&directory, 0);
        let worker = FakeWorker::successful();

        let error = run_with_worker(&directory, &config_path, &worker).unwrap_err();

        assert!(format!("{error:#}").contains("could not start transcription"));
        assert!(worker.invocations.lock().unwrap().is_empty());
        let session = SessionStore::load(&directory).unwrap();
        assert_eq!(session.record().format, RECORDING_SESSION_FORMAT_VERSION);
        assert_eq!(session.record().state, WorkflowState::ReadyForTranscription);
        assert_eq!(
            fs::read(directory.join(crate::artifacts::TRANSCRIPTION_RESULTS_PATH)).unwrap(),
            b""
        );
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn missing_required_artifact_prevents_state_change_and_worker_launch() {
        let (directory, config_path, _) = ready_session("missing-artifact");
        fs::remove_file(directory.join(PACKET_JOURNAL_FILE_NAME)).unwrap();
        let worker = FakeWorker::successful();

        let error = run_with_worker(&directory, &config_path, &worker).unwrap_err();

        assert!(format!("{error:#}").contains("packet journal"));
        assert!(worker.invocations.lock().unwrap().is_empty());
        let session = SessionStore::load(&directory).unwrap();
        assert_eq!(session.record().state, WorkflowState::ReadyForTranscription);
        assert_eq!(session.record().format, RECORDING_SESSION_FORMAT_VERSION);
        assert!(
            !directory
                .join(crate::artifacts::TRANSCRIPTION_RESULTS_PATH)
                .exists()
        );
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn worker_failure_records_diagnostics_and_waits_for_operator() {
        let (directory, config_path, _) = ready_session("worker-failure");
        let worker = FakeWorker::failed();

        let error = run_with_worker(&directory, &config_path, &worker).unwrap_err();

        assert!(error.to_string().contains("status 17"));
        assert_eq!(worker.invocations.lock().unwrap().len(), 1);
        let session = SessionStore::load(&directory).unwrap();
        assert_eq!(session.record().state, WorkflowState::AwaitingOperator);
        assert_eq!(session.record().failures.len(), 1);
        assert_eq!(session.record().failures[0].kind, "transcription_worker");
        assert!(
            session.record().failures[0]
                .message
                .contains("attempted_start_sequence=1")
        );
        assert!(
            session.record().failures[0]
                .message
                .contains("next_uncommitted_sequence=1")
        );
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn failure_diagnostics_use_results_committed_after_worker_start() {
        let (directory, config_path, items) = session_with_items(
            "progress-failure",
            &[
                (0, 10_000),
                (20_000, 30_000),
                (40_000, 50_000),
                (60_000, 70_000),
            ],
        );
        let worker = ProgressThenFailureWorker {
            count: 3,
            invocations: Mutex::new(Vec::new()),
        };

        let error = run_with_worker(&directory, &config_path, &worker).unwrap_err();

        assert!(error.to_string().contains("status 23"));
        let session = SessionStore::load(&directory).unwrap();
        assert_eq!(session.record().state, WorkflowState::AwaitingOperator);
        let message = &session.record().failures.last().unwrap().message;
        assert!(message.contains("attempted_start_sequence=1"));
        assert!(message.contains("next_uncommitted_sequence=4"));
        assert!(message.contains(&format!("next_work_item_id={}", items[3].id)));
        assert_eq!(
            validate_and_repair_result_prefix(
                &directory.join(crate::artifacts::TRANSCRIPTION_RESULTS_PATH),
                &items
            )
            .unwrap()
            .len(),
            3
        );
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn malformed_complete_result_after_nonzero_exit_is_durable_and_untouched() {
        let (directory, config_path, items) = session_with_items(
            "malformed-post-worker",
            &[(0, 10_000), (20_000, 30_000), (40_000, 50_000)],
        );

        let error =
            run_with_worker(&directory, &config_path, &MalformedThenFailureWorker).unwrap_err();

        assert!(error.to_string().contains("status 31"));
        let results_path = directory.join(crate::artifacts::TRANSCRIPTION_RESULTS_PATH);
        let results = fs::read(&results_path).unwrap();
        assert!(results.ends_with(b"{malformed complete record}\n"));
        let session = SessionStore::load(&directory).unwrap();
        assert_eq!(session.record().state, WorkflowState::AwaitingOperator);
        let failure = &session.record().failures.last().unwrap().message;
        assert!(failure.contains("process=non_zero_exit: status 31"));
        assert!(failure.contains("result_integrity_error=malformed interior"));
        assert!(failure.contains("safely_validated_prefix_length=1"));
        assert!(failure.contains("earliest_unsafe_sequence=2"));
        assert!(failure.contains(&format!("earliest_unsafe_work_item_id={}", items[1].id)));
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn mismatched_result_after_zero_exit_is_durable_and_untouched() {
        let (directory, config_path, item) = ready_session("mismatched-post-worker");

        let error =
            run_with_worker(&directory, &config_path, &MismatchedThenZeroWorker).unwrap_err();

        assert!(error.to_string().contains("zero_exit"));
        let results_path = directory.join(crate::artifacts::TRANSCRIPTION_RESULTS_PATH);
        let results_before = fs::read(&results_path).unwrap();
        assert!(results_before.ends_with(b"\n"));
        let session = SessionStore::load(&directory).unwrap();
        assert_eq!(session.record().state, WorkflowState::AwaitingOperator);
        let failure = &session.record().failures.last().unwrap().message;
        assert!(failure.contains("process=zero_exit"));
        assert!(failure.contains("result_integrity_error=transcription result sequence 1"));
        assert!(failure.contains("safely_validated_prefix_length=0"));
        assert!(failure.contains("earliest_unsafe_sequence=1"));
        assert!(failure.contains(&format!("earliest_unsafe_work_item_id={}", item.id)));
        assert_eq!(fs::read(&results_path).unwrap(), results_before);
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn result_integrity_failure_can_remain_recoverably_transcription_failed() {
        let (directory, config_path, _) = ready_session("integrity-stranded");
        // Start and atomic failure publication succeed; awaiting_operator fails.
        fail_record_write_after(&directory, 2);

        let error =
            run_with_worker(&directory, &config_path, &MismatchedThenZeroWorker).unwrap_err();

        assert!(error.to_string().contains("remains transcription_failed"));
        let session = SessionStore::load(&directory).unwrap();
        assert_eq!(session.record().state, WorkflowState::TranscriptionFailed);
        assert!(
            session
                .record()
                .failures
                .last()
                .unwrap()
                .message
                .contains("result_integrity_error=transcription result sequence 1")
        );
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn launch_and_signal_failures_are_distinguished_durably() {
        for (label, worker, expected) in [
            (
                "launch-failure",
                &LaunchFailureWorker as &dyn WorkerProcess,
                "launch_failure",
            ),
            (
                "signal-failure",
                &FakeWorker::signal_failure() as &dyn WorkerProcess,
                "signal_termination",
            ),
        ] {
            let (directory, config_path, _) = ready_session(label);

            let error = run_with_worker(&directory, &config_path, worker).unwrap_err();

            assert!(error.to_string().contains(expected));
            let session = SessionStore::load(&directory).unwrap();
            assert_eq!(session.record().state, WorkflowState::AwaitingOperator);
            assert!(
                session
                    .record()
                    .failures
                    .last()
                    .unwrap()
                    .message
                    .contains(expected)
            );
            fs::remove_dir_all(directory).unwrap();
        }
    }

    #[test]
    fn stranded_transcription_failed_session_can_continue() {
        let (directory, config_path, _) = ready_session("stranded-failed");
        fail_record_write_after(&directory, 2);

        let error = run_with_worker(&directory, &config_path, &FakeWorker::failed()).unwrap_err();

        assert!(error.to_string().contains("remains transcription_failed"));
        assert_eq!(
            SessionStore::load(&directory).unwrap().record().state,
            WorkflowState::TranscriptionFailed
        );

        continue_with_worker(&directory, &config_path, &FakeWorker::successful()).unwrap();

        assert_eq!(
            SessionStore::load(&directory).unwrap().record().state,
            WorkflowState::Complete
        );
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn zero_rewind_retains_complete_prefix_without_duplicate_authority() {
        let (directory, config_path, items) = session_with_items(
            "zero-rewind",
            &[
                (0, 10_000),
                (20_000, 30_000),
                (40_000, 50_000),
                (60_000, 70_000),
            ],
        );
        set_rewind_seconds(&config_path, 0);
        let first = ProgressThenFailureWorker {
            count: 3,
            invocations: Mutex::new(Vec::new()),
        };
        run_with_worker(&directory, &config_path, &first).unwrap_err();
        let resumed = FakeWorker::successful();

        continue_with_worker(&directory, &config_path, &resumed).unwrap();

        assert_eq!(resumed.invocations.lock().unwrap()[0].next_sequence, 4);
        let results = validate_and_repair_result_prefix(
            &directory.join(crate::artifacts::TRANSCRIPTION_RESULTS_PATH),
            &items,
        )
        .unwrap();
        assert_eq!(results.len(), 4);
        assert_eq!(
            results
                .iter()
                .map(|result| result.sequence)
                .collect::<Vec<_>>(),
            [1, 2, 3, 4]
        );
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn positive_rewind_uses_earliest_intersection_and_contiguous_prefix() {
        let items = [
            test_item(1, 0, 10_000),
            test_item(2, 170_000, 190_000),
            test_item(3, 175_000, 400_000),
            test_item(4, 290_000, 300_000),
        ];
        let results = items
            .iter()
            .map(|item| complete_result(item, "overlap"))
            .collect::<Vec<_>>();

        assert_eq!(calculate_resume_sequence(&results, 120).unwrap(), 2);
        assert_eq!(calculate_resume_sequence(&results, 0).unwrap(), 5);
    }

    #[test]
    fn continuation_repairs_a_truncated_final_result_before_rewind() {
        let (directory, config_path, items) = failed_session_with_results(
            "continuation-tail",
            &[(0, 10_000), (130_000, 140_000), (260_000, 270_000)],
            2,
        );
        let results_path = directory.join(crate::artifacts::TRANSCRIPTION_RESULTS_PATH);
        let mut bytes = fs::read(&results_path).unwrap();
        bytes.extend_from_slice(b"{\"format\":");
        fs::write(&results_path, bytes).unwrap();
        let worker = FakeWorker::successful();

        continue_with_worker(&directory, &config_path, &worker).unwrap();

        assert_eq!(worker.invocations.lock().unwrap()[0].next_sequence, 2);
        assert_eq!(
            validate_and_repair_result_prefix(&results_path, &items)
                .unwrap()
                .len(),
            3
        );
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn prepared_checkpoint_is_reapplied_without_recalculating_rewind() {
        let (directory, config_path, items) = failed_session_with_results(
            "prepared-crash",
            &[(0, 10_000), (20_000, 30_000), (40_000, 50_000)],
            3,
        );
        SessionStore::load(&directory)
            .unwrap()
            .record_transcription_resume_prepared(3_000, 2)
            .unwrap();
        let worker = FakeWorker::successful();

        continue_with_worker(&directory, &config_path, &worker).unwrap();

        assert_eq!(worker.invocations.lock().unwrap()[0].next_sequence, 2);
        assert_eq!(
            validate_and_repair_result_prefix(
                &directory.join(crate::artifacts::TRANSCRIPTION_RESULTS_PATH),
                &items
            )
            .unwrap()
            .len(),
            3
        );
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn result_replacement_before_applied_checkpoint_is_idempotent() {
        let (directory, config_path, items) = failed_session_with_results(
            "replacement-crash",
            &[(0, 10_000), (20_000, 30_000), (40_000, 50_000)],
            3,
        );
        let results_path = directory.join(crate::artifacts::TRANSCRIPTION_RESULTS_PATH);
        let committed = validate_and_repair_result_prefix(&results_path, &items).unwrap();
        SessionStore::load(&directory)
            .unwrap()
            .record_transcription_resume_prepared(3_000, 2)
            .unwrap();
        replace_results_prefix(&results_path, &committed[..1]).unwrap();
        fs::write(
            directory.join(PARTIAL_TRANSCRIPT_FILE_NAME),
            b"stale text\n",
        )
        .unwrap();
        let worker = FakeWorker::successful();

        continue_with_worker(&directory, &config_path, &worker).unwrap();

        assert_eq!(worker.invocations.lock().unwrap()[0].next_sequence, 2);
        assert_eq!(
            fs::read_to_string(directory.join(FINAL_TRANSCRIPT_PATH)).unwrap(),
            "[00:00:00] Alice: Result 1\n\
             [00:00:20] Alice: Result 2\n\
             [00:00:40] Alice: Result 3\n"
        );
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn applied_checkpoint_failure_is_recoverable_without_second_rewind() {
        let (directory, config_path, _) = failed_session_with_results(
            "applied-publish-crash",
            &[(0, 10_000), (130_000, 140_000), (260_000, 270_000)],
            3,
        );
        // Prepared checkpoint succeeds; applied checkpoint/state publication fails.
        fail_record_write_after(&directory, 1);

        let error =
            continue_with_worker(&directory, &config_path, &FakeWorker::successful()).unwrap_err();

        assert!(error.to_string().contains("applied resume state"));
        let stranded = SessionStore::load(&directory).unwrap();
        assert_eq!(stranded.record().state, WorkflowState::AwaitingOperator);
        assert!(
            stranded
                .record()
                .checkpoints
                .last()
                .unwrap()
                .stage
                .starts_with(RESUME_PREPARED_PREFIX)
        );
        let worker = FakeWorker::successful();
        continue_with_worker(&directory, &config_path, &worker).unwrap();
        assert_eq!(worker.invocations.lock().unwrap()[0].next_sequence, 3);
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn repeated_failure_without_progress_reuses_attempt_boundary() {
        let (directory, config_path, _) =
            failed_session_with_results("no-progress", &[(0, 10_000), (130_000, 140_000)], 2);
        let first = FakeWorker::failed();
        continue_with_worker(&directory, &config_path, &first).unwrap_err();
        let second = FakeWorker::failed();

        continue_with_worker(&directory, &config_path, &second).unwrap_err();

        assert_eq!(first.invocations.lock().unwrap()[0].next_sequence, 2);
        assert_eq!(second.invocations.lock().unwrap()[0].next_sequence, 2);
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn new_progress_allows_one_new_rewind_calculation() {
        let (directory, config_path, _) = failed_session_with_results(
            "new-progress",
            &[
                (0, 10_000),
                (130_000, 140_000),
                (260_000, 270_000),
                (390_000, 400_000),
            ],
            2,
        );
        let progressing = ProgressThenFailureWorker {
            count: 2,
            invocations: Mutex::new(Vec::new()),
        };
        continue_with_worker(&directory, &config_path, &progressing).unwrap_err();
        let next = FakeWorker::failed();

        continue_with_worker(&directory, &config_path, &next).unwrap_err();

        assert_eq!(progressing.invocations.lock().unwrap()[0].next_sequence, 2);
        assert_eq!(next.invocations.lock().unwrap()[0].next_sequence, 3);
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn transcription_continue_rejects_recording_format_without_mutation() {
        let (directory, config_path, _) = ready_session("wrong-continue-route");
        let before = fs::read(directory.join("session.json")).unwrap();

        let error =
            continue_with_worker(&directory, &config_path, &FakeWorker::successful()).unwrap_err();

        assert!(error.to_string().contains("requires a format-5"));
        assert_eq!(fs::read(directory.join("session.json")).unwrap(), before);
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn recording_continue_rejects_format_five_without_mutation() {
        let (directory, _, _) =
            failed_session_with_results("wrong-recording-route", &[(0, 10_000)], 0);
        let before = fs::read(directory.join("session.json")).unwrap();

        let error = crate::continuation::run(&directory).unwrap_err();

        assert!(error.to_string().contains("format-3 or format-4"));
        assert_eq!(fs::read(directory.join("session.json")).unwrap(), before);
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn transcription_continue_rejects_transcribing_state() {
        let (directory, config_path, _) = ready_session("transcribing-is-transcribe-only");
        start_transcribing_session(&directory);
        SessionStore::load(&directory)
            .unwrap()
            .record_failure(2_400, "transcription_worker", "historical test evidence")
            .unwrap();
        let before = fs::read(directory.join("session.json")).unwrap();

        let error =
            continue_with_worker(&directory, &config_path, &FakeWorker::successful()).unwrap_err();

        assert!(error.to_string().contains("awaiting_operator or"));
        assert_eq!(fs::read(directory.join("session.json")).unwrap(), before);
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn partial_transcript_reconstruction_is_deterministic() {
        let directory = test_directory("deterministic-text");
        let path = directory.join(PARTIAL_TRANSCRIPT_FILE_NAME);
        let results = [
            complete_result(&test_item(1, 0, 10_000), "First"),
            complete_result(&test_item(2, 65_000, 70_000), "Second"),
        ];

        rebuild_partial_transcript(&directory, &path, &results).unwrap();
        let first = fs::read(&path).unwrap();
        fs::write(&path, b"stale\n").unwrap();
        rebuild_partial_transcript(&directory, &path, &results).unwrap();

        assert_eq!(fs::read(&path).unwrap(), first);
        assert_eq!(
            String::from_utf8(first).unwrap(),
            "[00:00:00] Alice: First\n[00:01:05] Alice: Second\n"
        );
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn partial_and_final_transcripts_omit_empty_results() {
        let directory = test_directory("empty-result-text");
        fs::create_dir(directory.join(TRANSCRIPTION_DIRECTORY_NAME)).unwrap();
        let partial_path = directory.join(PARTIAL_TRANSCRIPT_FILE_NAME);
        let first_item = test_item(1, 0, 10_000);
        let second_item = test_item(2, 10_000, 20_000);
        let results = [
            complete_result(&first_item, ""),
            complete_result(&second_item, "Spoken text"),
        ];

        validate_result_matches_work_item(&results[0], &first_item).unwrap();
        rebuild_partial_transcript(&directory, &partial_path, &results).unwrap();
        write_final_transcript_atomically(&directory, &results).unwrap();

        let expected = "[00:00:10] Alice: Spoken text\n";
        assert_eq!(fs::read_to_string(&partial_path).unwrap(), expected);
        assert_eq!(
            fs::read_to_string(directory.join(FINAL_TRANSCRIPT_PATH)).unwrap(),
            expected
        );
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn completion_state_failure_is_recoverable_by_explicit_transcribe_retry() {
        let (directory, config_path, item) = ready_session("completion-state-failure");
        // Transcription-start publication succeeds; completion publication fails.
        fail_record_write_after(&directory, 1);

        let error =
            run_with_worker(&directory, &config_path, &FakeWorker::successful()).unwrap_err();

        assert!(error.to_string().contains("completion state"));
        assert_eq!(
            SessionStore::load(&directory).unwrap().record().state,
            WorkflowState::Transcribing
        );
        assert!(directory.join(FINAL_TRANSCRIPT_PATH).is_file());

        run_with_worker(&directory, &config_path, &FakeWorker::successful()).unwrap();

        let session = SessionStore::load(&directory).unwrap();
        assert_eq!(session.record().state, WorkflowState::Complete);
        assert_eq!(
            validate_and_repair_result_prefix(
                &directory.join(crate::artifacts::TRANSCRIPTION_RESULTS_PATH),
                &[item]
            )
            .unwrap()
            .len(),
            1
        );
        fs::remove_dir_all(directory).unwrap();
    }

    fn start_transcribing_session(directory: &Path) {
        prepare_empty_results(directory).unwrap();
        SessionStore::load(directory)
            .unwrap()
            .publish_transcription_start(2_300)
            .unwrap();
    }

    fn commit_remaining_results(invocation: &WorkerInvocation<'_>, maximum: usize) -> Result<()> {
        let manifest = fs::read_to_string(invocation.manifest_path)?;
        let items = manifest
            .lines()
            .map(serde_json::from_str::<WorkItem>)
            .collect::<std::result::Result<Vec<_>, _>>()?;
        let mut results = OpenOptions::new()
            .append(true)
            .open(invocation.results_path)?;
        let mut transcript = OpenOptions::new()
            .append(true)
            .open(invocation.transcript_path)?;
        for item in items
            .iter()
            .filter(|item| item.sequence >= invocation.next_sequence)
            .take(maximum)
        {
            let result = complete_result(item, &format!("Result {}", item.sequence));
            serde_json::to_writer(&mut results, &result)?;
            results.write_all(b"\n")?;
            transcript.write_all(transcript_line(&result).as_bytes())?;
        }
        results.sync_all()?;
        transcript.sync_all()?;
        Ok(())
    }

    fn session_with_items(label: &str, ranges: &[(u64, u64)]) -> (PathBuf, PathBuf, Vec<WorkItem>) {
        let (directory, config_path, _) = ready_session(label);
        let items = ranges
            .iter()
            .enumerate()
            .map(|(index, (start_ms, end_ms))| {
                test_item(u64::try_from(index).unwrap() + 1, *start_ms, *end_ms)
            })
            .collect::<Vec<_>>();
        write_work_items_and_track_duration(&directory, &items);
        (directory, config_path, items)
    }

    fn failed_session_with_results(
        label: &str,
        ranges: &[(u64, u64)],
        committed_count: usize,
    ) -> (PathBuf, PathBuf, Vec<WorkItem>) {
        let (directory, config_path, items) = session_with_items(label, ranges);
        start_transcribing_session(&directory);
        let results = items
            .iter()
            .take(committed_count)
            .map(|item| complete_result(item, &format!("Result {}", item.sequence)))
            .collect::<Vec<_>>();
        replace_results_prefix(
            &directory.join(crate::artifacts::TRANSCRIPTION_RESULTS_PATH),
            &results,
        )
        .unwrap();
        rebuild_partial_transcript(
            &directory,
            &directory.join(PARTIAL_TRANSCRIPT_FILE_NAME),
            &results,
        )
        .unwrap();
        let mut session = SessionStore::load(&directory).unwrap();
        session
            .publish_transcription_failure(
                2_400,
                "attempted_start_sequence=1; next_uncommitted_sequence=1; \
                 next_work_item_id=test; process=test_failure",
            )
            .unwrap();
        session
            .transition(WorkflowState::AwaitingOperator, 2_500)
            .unwrap();
        (directory, config_path, items)
    }

    fn write_work_items_and_track_duration(directory: &Path, items: &[WorkItem]) {
        let body = items
            .iter()
            .map(|item| serde_json::to_string(item).unwrap())
            .collect::<Vec<_>>()
            .join("\n")
            + "\n";
        fs::write(directory.join(WORK_ITEM_MANIFEST_PATH), body).unwrap();
        let maximum_end_ms = items.iter().map(|item| item.end_ms).max().unwrap_or(1);
        TrackManifest::new(
            "session-transcription".to_owned(),
            vec![TrackDescription::new(
                11,
                "Alice".to_owned(),
                "player".to_owned(),
                None,
                "tracks/user-11.flac".to_owned(),
                TrackState::Complete,
                maximum_end_ms.saturating_mul(48),
                vec![100],
                None,
            )],
        )
        .write(directory)
        .unwrap();
    }

    fn set_rewind_seconds(config_path: &Path, seconds: u64) {
        let config = fs::read_to_string(config_path).unwrap();
        fs::write(
            config_path,
            config.replace(
                "resume_rewind_seconds = 120",
                &format!("resume_rewind_seconds = {seconds}"),
            ),
        )
        .unwrap();
    }

    fn read_test_work_manifest_authority(
        directory: &Path,
        items: &[WorkItem],
    ) -> Result<Vec<WorkItem>> {
        let body = items
            .iter()
            .map(|item| serde_json::to_string(item).unwrap())
            .collect::<Vec<_>>()
            .join("\n")
            + "\n";
        let path = directory.join(WORK_ITEM_MANIFEST_PATH);
        fs::write(&path, body).unwrap();
        let session = SessionStore::load(directory).unwrap();
        read_work_manifest_authority(&path, session.record())
    }

    fn ready_session(label: &str) -> (PathBuf, PathBuf, WorkItem) {
        let directory = test_directory(label);
        fs::create_dir(directory.join(TRACK_DIRECTORY_NAME)).unwrap();
        fs::create_dir(directory.join(TRANSCRIPTION_DIRECTORY_NAME)).unwrap();
        let participants = ParticipantContext::empty_for_test();
        let mut session = SessionStore::create(
            &directory,
            NewSession {
                session_id: "session-transcription",
                started_at_unix_millis: 1_000,
                configuration_version: 1,
                guild_id: "123",
                channel_id: "456",
                participants: &participants,
            },
        )
        .unwrap();
        for name in [
            PACKET_JOURNAL_FILE_NAME,
            PLAYOUT_JOURNAL_FILE_NAME,
            EVENT_JOURNAL_FILE_NAME,
        ] {
            fs::write(directory.join(name), b"").unwrap();
        }
        fs::write(
            directory.join(TRACK_DIRECTORY_NAME).join("user-11.flac"),
            b"test track",
        )
        .unwrap();
        TrackManifest::new(
            "session-transcription".to_owned(),
            vec![TrackDescription::new(
                11,
                "Alice".to_owned(),
                "player".to_owned(),
                None,
                "tracks/user-11.flac".to_owned(),
                TrackState::Complete,
                4_800,
                vec![100],
                None,
            )],
        )
        .write(&directory)
        .unwrap();
        session
            .transition(WorkflowState::RecordedClean, 2_000)
            .unwrap();
        session
            .transition(WorkflowState::ReadyForTranscription, 2_100)
            .unwrap();
        session.publish_work_manifest(2_200).unwrap();

        let item = WorkItem {
            format: WORK_ITEM_MANIFEST_FORMAT_VERSION,
            id: "session-transcription:000001".to_owned(),
            session_id: "session-transcription".to_owned(),
            sequence: 1,
            discord_user_id: "11".to_owned(),
            speaker: "Alice".to_owned(),
            role: "player".to_owned(),
            character: None,
            start_ms: 0,
            end_ms: 100,
            source: "tracks/user-11.flac".to_owned(),
            source_start_ms: 0,
            source_end_ms: 100,
        };
        fs::write(
            directory.join(WORK_ITEM_MANIFEST_PATH),
            format!("{}\n", serde_json::to_string(&item).unwrap()),
        )
        .unwrap();

        let config_path = directory.join("echoscribe.toml");
        fs::write(
            &config_path,
            r#"
version = 1

[discord]
token = ""
guild_id = "not-validated-offline"
channel_id = "also-not-validated"

[recording]
output_directory = "recordings"

[participants]
file = "missing-participants.toml"

[transcription]
model = "test-model"
language = "en"
device = "cpu"
compute_type = "int8"
beam_size = 1
vocabulary_file = "vocabulary.txt"
resume_rewind_seconds = 120

[segmentation]
vad_enabled = false
merge_gap_ms = 750
"#,
        )
        .unwrap();
        (directory, config_path, item)
    }

    fn complete_result(item: &WorkItem, text: &str) -> TranscriptionResult {
        TranscriptionResult {
            format: TRANSCRIPTION_RESULT_FORMAT_VERSION,
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
            text: text.to_owned(),
            status: "complete".to_owned(),
        }
    }

    fn test_item(sequence: u64, start_ms: u64, end_ms: u64) -> WorkItem {
        WorkItem {
            format: WORK_ITEM_MANIFEST_FORMAT_VERSION,
            id: format!("session-transcription:{sequence:06}"),
            session_id: "session-transcription".to_owned(),
            sequence,
            discord_user_id: "11".to_owned(),
            speaker: "Alice".to_owned(),
            role: "player".to_owned(),
            character: None,
            start_ms,
            end_ms,
            source: "tracks/user-11.flac".to_owned(),
            source_start_ms: start_ms,
            source_end_ms: end_ms,
        }
    }

    fn test_directory(label: &str) -> PathBuf {
        let directory = env::temp_dir().join(format!(
            "echoscribe-transcription-{label}-{}-{}",
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
