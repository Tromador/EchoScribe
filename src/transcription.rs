//! Offline transcription orchestration and durable result-prefix validation.
//!
//! Rust owns workflow authority and process lifetime. The Python worker owns
//! model loading, ranged audio extraction, and ordered result/text appends, but
//! never edits `session.json`.

use std::{
    collections::HashMap,
    env,
    ffi::OsString,
    fs::{self, File, OpenOptions},
    io::{BufWriter, Write},
    path::{Path, PathBuf},
    process::Command,
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, anyhow, bail};
use serde::{Deserialize, Serialize};

use crate::{
    artifacts::{
        PARTIAL_TRANSCRIPT_FILE_NAME, TRANSCRIPTION_DIRECTORY_NAME,
        TRANSCRIPTION_RESULT_FORMAT_VERSION, TRANSCRIPTION_RESULTS_FILE_NAME,
        WORK_ITEM_MANIFEST_FORMAT_VERSION,
    },
    config::OfflineTranscriptionConfig,
    participants::ParticipantContext,
    session::{
        RECORDING_SESSION_FORMAT_VERSION, SESSION_FORMAT_VERSION, SessionRecord, SessionStore,
        WorkflowState,
    },
    track_manifest::{TrackManifest, TrackState},
    work_items::WorkItem,
};

const RESULTS_TEMP_FILE_NAME: &str = ".results.jsonl.tmp";
const PARTIAL_TRANSCRIPT_TEMP_FILE_NAME: &str = ".transcript.partial.txt.tmp";

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
    fn run(&self, invocation: &WorkerInvocation<'_>) -> Result<WorkerExit>;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct WorkerExit {
    success: bool,
    code: Option<i32>,
}

struct SystemWorker;

impl WorkerProcess for SystemWorker {
    fn run(&self, invocation: &WorkerInvocation<'_>) -> Result<WorkerExit> {
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
            .arg(invocation.settings.beam_size.to_string());
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

pub(crate) fn run(session_directory: &Path, config_path: &Path) -> Result<()> {
    run_with_worker(session_directory, config_path, &SystemWorker)
}

fn run_with_worker(
    session_directory: &Path,
    config_path: &Path,
    worker: &dyn WorkerProcess,
) -> Result<()> {
    let settings = OfflineTranscriptionConfig::load(config_path)?;
    if let Some(warning) = &settings.vocabulary_warning {
        eprintln!("Warning: {warning}.");
    }

    let session_directory = fs::canonicalize(session_directory).with_context(|| {
        format!(
            "failed to resolve session directory {}",
            session_directory.display()
        )
    })?;
    let config_path = fs::canonicalize(config_path).with_context(|| {
        format!(
            "failed to resolve configuration file {}",
            config_path.display()
        )
    })?;
    let mut session = SessionStore::load(&session_directory).with_context(|| {
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
    let tracks = validate_session_artifacts(&session_directory, session.record())?;
    let work_items = read_work_manifest(&manifest_path, session.record(), &tracks)?;

    if session.record().state == WorkflowState::ReadyForTranscription {
        prepare_empty_results(&session_directory)?;
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
    rebuild_partial_transcript(&session_directory, &transcript_path, &committed)?;
    let next_sequence = u64::try_from(committed.len())
        .ok()
        .and_then(|value| value.checked_add(1))
        .ok_or_else(|| anyhow!("transcription result sequence overflow"))?;

    let exit = worker.run(&WorkerInvocation {
        config_path: &config_path,
        session_directory: &session_directory,
        manifest_path: &manifest_path,
        results_path: &results_path,
        transcript_path: &transcript_path,
        next_sequence,
        settings: &settings,
    })?;
    if !exit.success {
        match exit.code {
            Some(code) => bail!(
                "Python transcription worker exited with status {code}; session remains transcribing"
            ),
            None => bail!(
                "Python transcription worker terminated by signal; session remains transcribing"
            ),
        }
    }

    println!(
        "Python transcription worker completed for session {}; workflow remains transcribing.",
        session.record().session_id
    );
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
    let bytes = fs::read(path)
        .with_context(|| format!("failed to read work manifest {}", path.display()))?;
    if !bytes.is_empty() && !bytes.ends_with(b"\n") {
        bail!(
            "work manifest {} has a truncated final record",
            path.display()
        );
    }
    let tracks_by_user = tracks
        .tracks
        .iter()
        .map(|track| (track.discord_user_id.as_str(), track))
        .collect::<HashMap<_, _>>();
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
        validate_work_item_for_session(&item, record, &tracks_by_user, items.last())?;
        items.push(item);
    }
    Ok(items)
}

fn validate_work_item_for_session(
    item: &WorkItem,
    record: &SessionRecord,
    tracks: &HashMap<&str, &crate::track_manifest::TrackDescription>,
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
    {
        bail!("invalid or out-of-order work item {:?}", item.id);
    }
    if let Some(previous) = previous {
        let previous_key = (
            previous.start_ms,
            previous.discord_user_id.as_str(),
            previous.end_ms,
            previous.source_start_ms,
        );
        let item_key = (
            item.start_ms,
            item.discord_user_id.as_str(),
            item.end_ms,
            item.source_start_ms,
        );
        if item_key < previous_key {
            bail!("work manifest is not in deterministic chronological order");
        }
    }
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

fn validate_and_repair_result_prefix(
    path: &Path,
    work_items: &[WorkItem],
) -> Result<Vec<TranscriptionResult>> {
    let bytes =
        fs::read(path).with_context(|| format!("failed to read results {}", path.display()))?;
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
            bail!(
                "blank interior transcription result at line {} in {}",
                index + 1,
                path.display()
            );
        }
        let result: TranscriptionResult = serde_json::from_slice(line).with_context(|| {
            format!(
                "malformed interior transcription result at line {} in {}",
                index + 1,
                path.display()
            )
        })?;
        let work_item = work_items.get(results.len()).ok_or_else(|| {
            anyhow!(
                "transcription results contain sequence {} beyond the work manifest",
                result.sequence
            )
        })?;
        validate_result_matches_work_item(&result, work_item)?;
        results.push(result);
    }

    if committed_length != bytes.len() {
        let file = OpenOptions::new().write(true).open(path)?;
        file.set_len(
            u64::try_from(committed_length)
                .map_err(|_| anyhow!("result prefix length does not fit in u64"))?,
        )?;
        file.sync_all()?;
        if let Some(directory) = path.parent() {
            File::open(directory)?.sync_all()?;
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
        writer.write_all(transcript_line(result).as_bytes())?;
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
        sync::Mutex,
        time::{SystemTime, UNIX_EPOCH},
    };

    use super::*;
    use crate::{
        artifacts::{
            EVENT_JOURNAL_FILE_NAME, PACKET_JOURNAL_FILE_NAME, PLAYOUT_JOURNAL_FILE_NAME,
            TRACK_DIRECTORY_NAME, WORK_ITEM_MANIFEST_PATH,
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
        invocations: Mutex<Vec<ObservedInvocation>>,
    }

    impl FakeWorker {
        fn successful() -> Self {
            Self {
                exit: WorkerExit {
                    success: true,
                    code: Some(0),
                },
                invocations: Mutex::new(Vec::new()),
            }
        }

        fn failed() -> Self {
            Self {
                exit: WorkerExit {
                    success: false,
                    code: Some(17),
                },
                invocations: Mutex::new(Vec::new()),
            }
        }
    }

    impl WorkerProcess for FakeWorker {
        fn run(&self, invocation: &WorkerInvocation<'_>) -> Result<WorkerExit> {
            self.invocations.lock().unwrap().push(ObservedInvocation {
                next_sequence: invocation.next_sequence,
                manifest_path: invocation.manifest_path.to_owned(),
                results_path: invocation.results_path.to_owned(),
                transcript_path: invocation.transcript_path.to_owned(),
                hotwords: invocation.settings.hotwords.clone(),
            });
            Ok(self.exit)
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
        assert_eq!(session.record().state, WorkflowState::Transcribing);
        assert_eq!(
            session.record().files.results.as_ref().unwrap().path,
            crate::artifacts::TRANSCRIPTION_RESULTS_PATH
        );
        assert_eq!(
            fs::read(directory.join(crate::artifacts::TRANSCRIPTION_RESULTS_PATH)).unwrap(),
            b""
        );
        assert_eq!(
            fs::read(directory.join(PARTIAL_TRANSCRIPT_FILE_NAME)).unwrap(),
            b""
        );
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
    fn controlled_restart_repairs_tail_rebuilds_text_and_skips_prefix() {
        let (directory, config_path, item) = ready_session("restart");
        run_with_worker(&directory, &config_path, &FakeWorker::successful()).unwrap();
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
            fs::read_to_string(directory.join(PARTIAL_TRANSCRIPT_FILE_NAME)).unwrap(),
            "[00:00:00] Alice: All conversation stays here.\n"
        );
        let invocations = worker.invocations.lock().unwrap();
        assert_eq!(invocations.len(), 1);
        assert_eq!(invocations[0].next_sequence, 2);
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn malformed_interior_result_is_rejected_without_worker_launch() {
        let (directory, config_path, item) = ready_session("bad-interior");
        run_with_worker(&directory, &config_path, &FakeWorker::successful()).unwrap();
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
    fn worker_failure_returns_error_and_leaves_transcribing() {
        let (directory, config_path, _) = ready_session("worker-failure");
        let worker = FakeWorker::failed();

        let error = run_with_worker(&directory, &config_path, &worker).unwrap_err();

        assert!(error.to_string().contains("status 17"));
        assert_eq!(worker.invocations.lock().unwrap().len(), 1);
        let session = SessionStore::load(&directory).unwrap();
        assert_eq!(session.record().state, WorkflowState::Transcribing);
        assert!(session.record().failures.is_empty());
        fs::remove_dir_all(directory).unwrap();
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
