//! Durable session workflow metadata and event-journal records.
//!
//! `session.json` is workflow authority. Updates use write/synchronise/rename
//! replacement, while participant context remains a separate immutable TOML
//! artefact referenced by the manifest.

use std::{
    fs::{self, File, OpenOptions},
    io::{self, Write},
    path::{Component, Path, PathBuf},
};

#[cfg(test)]
use std::{
    collections::HashMap,
    sync::{Mutex, OnceLock},
};

use serde::{Deserialize, Serialize};

use crate::{
    artifacts::{
        EVENT_JOURNAL_FILE_NAME, PACKET_JOURNAL_FILE_NAME, PARTICIPANT_SNAPSHOT_FILE_NAME,
        PARTICIPANT_SNAPSHOT_FORMAT_VERSION, PLAYOUT_JOURNAL_FILE_NAME, TRACK_MANIFEST_FILE_NAME,
        TRACK_MANIFEST_FORMAT_VERSION, TRANSCRIPTION_RESULT_FORMAT_VERSION,
        TRANSCRIPTION_RESULTS_PATH, WORK_ITEM_MANIFEST_FORMAT_VERSION, WORK_ITEM_MANIFEST_PATH,
    },
    participants::ParticipantContext,
};

pub(crate) const SESSION_FORMAT_VERSION: u16 = 5;
pub(crate) const RECORDING_SESSION_FORMAT_VERSION: u16 = 4;
pub(crate) const PREVIOUS_SESSION_FORMAT_VERSION: u16 = 3;
pub(crate) const LEGACY_SESSION_FORMAT_VERSION: u16 = 2;
pub(crate) const LEGACY_EVENT_FORMAT_VERSION: u16 = 1;
pub(crate) const EVENT_FORMAT_VERSION: u16 = 2;

const SESSION_FILE_NAME: &str = "session.json";
const SESSION_TEMP_FILE_NAME: &str = ".session.json.tmp";
const PARTICIPANT_TEMP_FILE_NAME: &str = ".participants.toml.tmp";

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
/// Validated top-level workflow states persisted in `session.json`.
pub(crate) enum WorkflowState {
    Recording,
    RecordedClean,
    RecordedIncomplete,
    AwaitingOperator,
    ReadyForTranscription,
    Transcribing,
    TranscriptionFailed,
    Complete,
}

impl WorkflowState {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Recording => "recording",
            Self::RecordedClean => "recorded_clean",
            Self::RecordedIncomplete => "recorded_incomplete",
            Self::AwaitingOperator => "awaiting_operator",
            Self::ReadyForTranscription => "ready_for_transcription",
            Self::Transcribing => "transcribing",
            Self::TranscriptionFailed => "transcription_failed",
            Self::Complete => "complete",
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
/// Versioned durable authority for one session's processing state.
pub(crate) struct SessionRecord {
    pub(crate) format: u16,
    pub(crate) session_id: String,
    pub(crate) started_at_unix_millis: u64,
    pub(crate) stopped_at_unix_millis: Option<u64>,
    pub(crate) configuration_version: u32,
    pub(crate) state: WorkflowState,
    pub(crate) discord: DiscordSession,
    pub(crate) files: SessionFiles,
    pub(crate) failures: Vec<FailureRecord>,
    pub(crate) checkpoints: Vec<CheckpointRecord>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct DiscordSession {
    pub(crate) guild_id: String,
    pub(crate) channel_id: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
/// Internal artefact manifest; paths are relative to the session directory.
pub(crate) struct SessionFiles {
    pub(crate) packets: FileDescription,
    pub(crate) playout: FileDescription,
    pub(crate) events: FileDescription,
    pub(crate) participants: FileDescription,
    pub(crate) tracks: FileDescription,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) work_items: Option<FileDescription>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) results: Option<FileDescription>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
/// A path and an independently versioned on-disk format.
pub(crate) struct FileDescription {
    pub(crate) path: String,
    pub(crate) format: u16,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
/// Durable explanation of a fault while the record was in `state`.
pub(crate) struct FailureRecord {
    pub(crate) recorded_at_unix_millis: u64,
    pub(crate) state: WorkflowState,
    pub(crate) kind: String,
    pub(crate) message: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CheckpointRecord {
    pub(crate) completed_at_unix_millis: u64,
    pub(crate) stage: String,
}

/// Inputs required to allocate the immutable foundation of a new session.
pub(crate) struct NewSession<'a> {
    pub(crate) session_id: &'a str,
    pub(crate) started_at_unix_millis: u64,
    pub(crate) configuration_version: u32,
    pub(crate) guild_id: &'a str,
    pub(crate) channel_id: &'a str,
    pub(crate) participants: &'a ParticipantContext,
}

#[allow(dead_code)]
/// In-memory record paired with crash-safe persistence operations.
pub(crate) struct SessionStore {
    path: PathBuf,
    record: SessionRecord,
}

impl SessionRecord {
    fn new(input: &NewSession<'_>) -> Self {
        Self {
            // Recording starts in format 4. Format 5 is a transcription-stage
            // commitment and must not be claimed before results authority exists.
            format: RECORDING_SESSION_FORMAT_VERSION,
            session_id: input.session_id.to_owned(),
            started_at_unix_millis: input.started_at_unix_millis,
            stopped_at_unix_millis: None,
            configuration_version: input.configuration_version,
            state: WorkflowState::Recording,
            discord: DiscordSession {
                guild_id: input.guild_id.to_owned(),
                channel_id: input.channel_id.to_owned(),
            },
            files: SessionFiles {
                packets: FileDescription {
                    path: PACKET_JOURNAL_FILE_NAME.to_owned(),
                    format: crate::journal::FORMAT_VERSION,
                },
                playout: FileDescription {
                    path: PLAYOUT_JOURNAL_FILE_NAME.to_owned(),
                    format: crate::playout::FORMAT_VERSION,
                },
                events: FileDescription {
                    path: EVENT_JOURNAL_FILE_NAME.to_owned(),
                    format: EVENT_FORMAT_VERSION,
                },
                participants: FileDescription {
                    path: PARTICIPANT_SNAPSHOT_FILE_NAME.to_owned(),
                    format: PARTICIPANT_SNAPSHOT_FORMAT_VERSION,
                },
                tracks: FileDescription {
                    path: TRACK_MANIFEST_FILE_NAME.to_owned(),
                    format: TRACK_MANIFEST_FORMAT_VERSION,
                },
                work_items: None,
                results: None,
            },
            failures: Vec::new(),
            checkpoints: Vec::new(),
        }
    }

    pub(crate) fn validate(&self) -> io::Result<()> {
        // Validation is intentionally repeated on read and before every write;
        // malformed workflow authority must never be propagated.
        if !matches!(
            self.format,
            PREVIOUS_SESSION_FORMAT_VERSION
                | RECORDING_SESSION_FORMAT_VERSION
                | SESSION_FORMAT_VERSION
        ) {
            return Err(invalid_data(format!(
                "unsupported session format {}; expected {}, {}, or {}",
                self.format,
                PREVIOUS_SESSION_FORMAT_VERSION,
                RECORDING_SESSION_FORMAT_VERSION,
                SESSION_FORMAT_VERSION
            )));
        }
        if self.session_id.trim().is_empty() {
            return Err(invalid_data("session_id must not be empty"));
        }
        if self.configuration_version == 0 {
            return Err(invalid_data(
                "configuration_version must be greater than zero",
            ));
        }
        if self
            .stopped_at_unix_millis
            .is_some_and(|stopped| stopped < self.started_at_unix_millis)
        {
            return Err(invalid_data(
                "stopped_at_unix_millis precedes started_at_unix_millis",
            ));
        }
        if self
            .discord
            .guild_id
            .parse::<u64>()
            .ok()
            .filter(|id| *id != 0)
            .is_none()
        {
            return Err(invalid_data(
                "discord.guild_id must be a non-zero unsigned decimal ID string",
            ));
        }
        if self
            .discord
            .channel_id
            .parse::<u64>()
            .ok()
            .filter(|id| *id != 0)
            .is_none()
        {
            return Err(invalid_data(
                "discord.channel_id must be a non-zero unsigned decimal ID string",
            ));
        }

        for (name, description, expected_path, expected_format) in [
            (
                "packets",
                &self.files.packets,
                PACKET_JOURNAL_FILE_NAME,
                crate::journal::FORMAT_VERSION,
            ),
            (
                "playout",
                &self.files.playout,
                PLAYOUT_JOURNAL_FILE_NAME,
                crate::playout::FORMAT_VERSION,
            ),
            (
                "participants",
                &self.files.participants,
                PARTICIPANT_SNAPSHOT_FILE_NAME,
                PARTICIPANT_SNAPSHOT_FORMAT_VERSION,
            ),
            (
                "tracks",
                &self.files.tracks,
                TRACK_MANIFEST_FILE_NAME,
                TRACK_MANIFEST_FORMAT_VERSION,
            ),
        ] {
            validate_relative_artifact_path(name, &description.path)?;
            if description.path != expected_path || description.format != expected_format {
                return Err(invalid_data(format!(
                    "session file {name} must be {expected_path:?} format {expected_format}"
                )));
            }
        }
        validate_relative_artifact_path("events", &self.files.events.path)?;
        if self.files.events.path != EVENT_JOURNAL_FILE_NAME
            || !matches!(
                self.files.events.format,
                LEGACY_EVENT_FORMAT_VERSION | EVENT_FORMAT_VERSION
            )
        {
            return Err(invalid_data(format!(
                "session file events must be {EVENT_JOURNAL_FILE_NAME:?} format \
                 {LEGACY_EVENT_FORMAT_VERSION} or {EVENT_FORMAT_VERSION}"
            )));
        }
        if self.format == PREVIOUS_SESSION_FORMAT_VERSION && self.files.work_items.is_some() {
            return Err(invalid_data(
                "session format 3 must not contain a work_items description",
            ));
        }
        if let Some(description) = &self.files.work_items {
            validate_relative_artifact_path("work_items", &description.path)?;
            if description.path != WORK_ITEM_MANIFEST_PATH
                || description.format != WORK_ITEM_MANIFEST_FORMAT_VERSION
            {
                return Err(invalid_data(format!(
                    "session file work_items must be {WORK_ITEM_MANIFEST_PATH:?} format \
                     {WORK_ITEM_MANIFEST_FORMAT_VERSION}"
                )));
            }
        }
        if self.format < SESSION_FORMAT_VERSION && self.files.results.is_some() {
            return Err(invalid_data(
                "session formats 3 and 4 must not contain a results description",
            ));
        }
        if self.format == SESSION_FORMAT_VERSION {
            if self.files.work_items.is_none() || self.files.results.is_none() {
                return Err(invalid_data(
                    "session format 5 requires work_items and results descriptions",
                ));
            }
            let description = self
                .files
                .results
                .as_ref()
                .expect("format-5 results presence checked above");
            validate_relative_artifact_path("results", &description.path)?;
            if description.path != TRANSCRIPTION_RESULTS_PATH
                || description.format != TRANSCRIPTION_RESULT_FORMAT_VERSION
            {
                return Err(invalid_data(format!(
                    "session file results must be {TRANSCRIPTION_RESULTS_PATH:?} format \
                     {TRANSCRIPTION_RESULT_FORMAT_VERSION}"
                )));
            }
        }
        if matches!(
            self.state,
            WorkflowState::Transcribing
                | WorkflowState::TranscriptionFailed
                | WorkflowState::Complete
        ) && self.format != SESSION_FORMAT_VERSION
        {
            return Err(invalid_data(
                "transcription workflow states require session format 5",
            ));
        }
        if self.format == SESSION_FORMAT_VERSION
            && !matches!(
                self.state,
                WorkflowState::Transcribing
                    | WorkflowState::TranscriptionFailed
                    | WorkflowState::AwaitingOperator
                    | WorkflowState::Complete
            )
        {
            return Err(invalid_data(
                "session format 5 requires a transcription workflow state",
            ));
        }

        for failure in &self.failures {
            validate_label("failure kind", &failure.kind)?;
            if failure.message.trim().is_empty() {
                return Err(invalid_data("failure message must not be empty"));
            }
        }
        for checkpoint in &self.checkpoints {
            validate_label("checkpoint stage", &checkpoint.stage)?;
        }
        let has_work_manifest_checkpoint = self
            .checkpoints
            .iter()
            .any(|checkpoint| checkpoint.stage == "work_manifest_built");
        if self.files.work_items.is_some() != has_work_manifest_checkpoint {
            return Err(invalid_data(
                "work_items description and work_manifest_built checkpoint must appear together",
            ));
        }

        Ok(())
    }
}

impl SessionStore {
    /// Publish the participant snapshot and initial recording record.
    pub(crate) fn create(session_directory: &Path, input: NewSession<'_>) -> io::Result<Self> {
        let participant_snapshot = input
            .participants
            .canonical_toml()
            .map_err(io::Error::other)?;
        write_new_file_atomically(
            session_directory,
            PARTICIPANT_TEMP_FILE_NAME,
            PARTICIPANT_SNAPSHOT_FILE_NAME,
            participant_snapshot.as_bytes(),
        )?;

        let record = SessionRecord::new(&input);
        record.validate()?;
        let path = session_directory.join(SESSION_FILE_NAME);
        write_record_atomically(session_directory, &record)?;
        Ok(Self { path, record })
    }

    #[allow(dead_code)]
    pub(crate) fn load(session_directory: &Path) -> io::Result<Self> {
        let path = session_directory.join(SESSION_FILE_NAME);
        let record = read_record(&path)?;
        Ok(Self { path, record })
    }

    #[allow(dead_code)]
    pub(crate) fn record(&self) -> &SessionRecord {
        &self.record
    }

    #[allow(dead_code)]
    pub(crate) fn transition(
        &mut self,
        next: WorkflowState,
        at_unix_millis: u64,
    ) -> io::Result<()> {
        // State changes are an explicit table, not an ordinal progression. This
        // prevents commands skipping required operator-controlled boundaries.
        if !valid_transition(self.record.state, next) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "invalid session state transition from {:?} to {:?}",
                    self.record.state, next
                ),
            ));
        }

        let mut updated = self.record.clone();
        if updated.state == WorkflowState::Recording {
            updated.stopped_at_unix_millis = Some(at_unix_millis);
        }
        updated.state = next;
        self.persist(updated)
    }

    #[allow(dead_code)]
    pub(crate) fn record_failure(
        &mut self,
        recorded_at_unix_millis: u64,
        kind: impl Into<String>,
        message: impl Into<String>,
    ) -> io::Result<()> {
        // Failure evidence is append-only within the replaced session record.
        let mut updated = self.record.clone();
        updated.failures.push(FailureRecord {
            recorded_at_unix_millis,
            state: updated.state,
            kind: kind.into(),
            message: message.into(),
        });
        self.persist(updated)
    }

    #[allow(dead_code)]
    pub(crate) fn record_checkpoint(
        &mut self,
        completed_at_unix_millis: u64,
        stage: impl Into<String>,
    ) -> io::Result<()> {
        let mut updated = self.record.clone();
        updated.checkpoints.push(CheckpointRecord {
            completed_at_unix_millis,
            stage: stage.into(),
        });
        self.persist(updated)
    }

    /// Record a related set of stage checkpoints in one durable replacement.
    pub(crate) fn record_checkpoints(
        &mut self,
        completed_at_unix_millis: u64,
        stages: impl IntoIterator<Item = String>,
    ) -> io::Result<()> {
        let mut updated = self.record.clone();
        updated
            .checkpoints
            .extend(stages.into_iter().map(|stage| CheckpointRecord {
                completed_at_unix_millis,
                stage,
            }));
        self.persist(updated)
    }

    /// Publish the cross-stage work-manifest reference and its checkpoint as
    /// one workflow-authority replacement after the JSONL file is durable.
    pub(crate) fn publish_work_manifest(
        &mut self,
        completed_at_unix_millis: u64,
    ) -> io::Result<()> {
        if self.record.state != WorkflowState::ReadyForTranscription {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "work manifest requires ready_for_transcription; found {}",
                    self.record.state.as_str()
                ),
            ));
        }

        let mut updated = self.record.clone();
        updated.format = RECORDING_SESSION_FORMAT_VERSION;
        updated.files.work_items = Some(FileDescription {
            path: WORK_ITEM_MANIFEST_PATH.to_owned(),
            format: WORK_ITEM_MANIFEST_FORMAT_VERSION,
        });
        updated.checkpoints.push(CheckpointRecord {
            completed_at_unix_millis,
            stage: "work_manifest_built".to_owned(),
        });
        self.persist(updated)
    }

    /// Commit the results authority and workflow transition together. The
    /// caller must synchronise the empty results file before invoking this.
    pub(crate) fn publish_transcription_start(
        &mut self,
        started_at_unix_millis: u64,
    ) -> io::Result<()> {
        if self.record.format != RECORDING_SESSION_FORMAT_VERSION
            || self.record.state != WorkflowState::ReadyForTranscription
            || self.record.files.work_items.is_none()
            || self.record.files.results.is_some()
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "first transcription start requires a format-4 ready_for_transcription session \
                 with work_items and without results",
            ));
        }

        let mut updated = self.record.clone();
        updated.format = SESSION_FORMAT_VERSION;
        updated.state = WorkflowState::Transcribing;
        updated.files.results = Some(FileDescription {
            path: TRANSCRIPTION_RESULTS_PATH.to_owned(),
            format: TRANSCRIPTION_RESULT_FORMAT_VERSION,
        });
        updated.checkpoints.push(CheckpointRecord {
            completed_at_unix_millis: started_at_unix_millis,
            stage: "transcription_started".to_owned(),
        });
        self.persist(updated)
    }

    fn persist(&mut self, updated: SessionRecord) -> io::Result<()> {
        // Do not mutate the in-memory authority until the replacement is
        // validated and durably published.
        updated.validate()?;
        let session_directory = self
            .path
            .parent()
            .ok_or_else(|| io::Error::other("session path has no parent directory"))?;
        write_record_atomically(session_directory, &updated)?;
        self.record = updated;
        Ok(())
    }
}

pub(crate) fn read_record(path: &Path) -> io::Result<SessionRecord> {
    // A successfully parsed JSON object is still untrusted until every state,
    // path, format and identifier invariant has been checked.
    let bytes = fs::read(path)?;
    let value: serde_json::Value = serde_json::from_slice(&bytes).map_err(io::Error::other)?;
    if value.get("format").and_then(serde_json::Value::as_u64)
        == Some(u64::from(PREVIOUS_SESSION_FORMAT_VERSION))
        && value
            .get("files")
            .and_then(serde_json::Value::as_object)
            .is_some_and(|files| files.contains_key("work_items"))
    {
        return Err(invalid_data(
            "session format 3 must not contain a work_items field",
        ));
    }
    if value
        .get("format")
        .and_then(serde_json::Value::as_u64)
        .is_some_and(|format| format <= u64::from(RECORDING_SESSION_FORMAT_VERSION))
        && value
            .get("files")
            .and_then(serde_json::Value::as_object)
            .is_some_and(|files| files.contains_key("results"))
    {
        return Err(invalid_data(
            "session formats 3 and 4 must not contain a results field",
        ));
    }
    let record: SessionRecord = serde_json::from_value(value).map_err(io::Error::other)?;
    record.validate()?;
    Ok(record)
}

fn valid_transition(current: WorkflowState, next: WorkflowState) -> bool {
    matches!(
        (current, next),
        (WorkflowState::Recording, WorkflowState::RecordedClean)
            | (WorkflowState::Recording, WorkflowState::RecordedIncomplete)
            | (
                WorkflowState::RecordedClean,
                WorkflowState::ReadyForTranscription
            )
            | (
                WorkflowState::RecordedClean,
                WorkflowState::AwaitingOperator
            )
            | (
                WorkflowState::RecordedIncomplete,
                WorkflowState::AwaitingOperator
            )
            | (WorkflowState::Transcribing, WorkflowState::Complete)
            | (
                WorkflowState::Transcribing,
                WorkflowState::TranscriptionFailed
            )
            | (
                WorkflowState::TranscriptionFailed,
                WorkflowState::AwaitingOperator
            )
            | (
                WorkflowState::AwaitingOperator,
                WorkflowState::ReadyForTranscription
            )
            | (WorkflowState::AwaitingOperator, WorkflowState::Transcribing)
    )
}

fn write_record_atomically(session_directory: &Path, record: &SessionRecord) -> io::Result<()> {
    #[cfg(test)]
    inject_record_write_failure_if_due(session_directory)?;

    let mut bytes = serde_json::to_vec_pretty(record).map_err(io::Error::other)?;
    bytes.push(b'\n');
    write_replacing_file_atomically(
        session_directory,
        SESSION_TEMP_FILE_NAME,
        SESSION_FILE_NAME,
        &bytes,
    )
}

#[cfg(test)]
static RECORD_WRITE_FAILURES: OnceLock<Mutex<HashMap<PathBuf, usize>>> = OnceLock::new();

#[cfg(test)]
pub(crate) fn fail_record_write_after(
    session_directory: &Path,
    successful_writes_before_failure: usize,
) {
    RECORD_WRITE_FAILURES
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .insert(
            session_directory.to_path_buf(),
            successful_writes_before_failure,
        );
}

#[cfg(test)]
fn inject_record_write_failure_if_due(session_directory: &Path) -> io::Result<()> {
    let mut failures = RECORD_WRITE_FAILURES
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let Some(remaining) = failures.get_mut(session_directory) else {
        return Ok(());
    };
    if *remaining > 0 {
        *remaining -= 1;
        return Ok(());
    }

    failures.remove(session_directory);
    Err(io::Error::other(
        "injected one-shot session record persistence failure",
    ))
}

fn write_new_file_atomically(
    directory: &Path,
    temporary_name: &str,
    final_name: &str,
    bytes: &[u8],
) -> io::Result<()> {
    if directory.join(final_name).exists() {
        return Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            format!("{} already exists", directory.join(final_name).display()),
        ));
    }
    write_replacing_file_atomically(directory, temporary_name, final_name, bytes)
}

fn write_replacing_file_atomically(
    directory: &Path,
    temporary_name: &str,
    final_name: &str,
    bytes: &[u8],
) -> io::Result<()> {
    // Sync contents before rename and the directory afterwards so a reported
    // update survives the normal storage crash boundary.
    let temporary_path = directory.join(temporary_name);
    let final_path = directory.join(final_name);
    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&temporary_path)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    fs::rename(&temporary_path, &final_path)?;
    sync_directory(directory)
}

fn sync_directory(directory: &Path) -> io::Result<()> {
    File::open(directory)?.sync_all()
}

fn validate_relative_artifact_path(name: &str, value: &str) -> io::Result<()> {
    let path = Path::new(value);
    if value.is_empty() || path.is_absolute() {
        return Err(invalid_data(format!(
            "session file {name} path must be relative to the session directory"
        )));
    }
    if path
        .components()
        .any(|component| !matches!(component, Component::Normal(_)))
    {
        return Err(invalid_data(format!(
            "session file {name} path must not escape the session directory"
        )));
    }
    Ok(())
}

fn validate_label(name: &str, value: &str) -> io::Result<()> {
    if value.is_empty()
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
    {
        return Err(invalid_data(format!(
            "{name} must contain only lowercase ASCII letters, digits, and underscores"
        )));
    }
    Ok(())
}

fn invalid_data(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.into())
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "event", rename_all = "snake_case")]
/// Versioned identity and continuity evidence stored as NDJSON.
pub(crate) enum SessionEvent {
    SpeakerMapping {
        format: u16,
        elapsed_nanos: u64,
        ssrc: u32,
        user_id: Option<String>,
        speaking_bits: u8,
    },
    UserIdentity {
        format: u16,
        elapsed_nanos: u64,
        user_id: String,
        server_display_name: Option<String>,
        global_display_name: Option<String>,
        username: String,
    },
    UserDisconnected {
        format: u16,
        elapsed_nanos: u64,
        user_id: String,
    },
    UnresolvedSsrcAbandoned {
        format: u16,
        elapsed_nanos: u64,
        ssrc: u32,
        first_tick: u64,
        last_tick: u64,
        discarded_frames: u64,
        discarded_samples: u64,
        reason: String,
    },
}

impl SessionEvent {
    pub(crate) fn speaker_mapping(
        elapsed_nanos: u64,
        ssrc: u32,
        user_id: Option<String>,
        speaking_bits: u8,
    ) -> Self {
        Self::SpeakerMapping {
            format: EVENT_FORMAT_VERSION,
            elapsed_nanos,
            ssrc,
            user_id,
            speaking_bits,
        }
    }

    pub(crate) fn user_identity(
        elapsed_nanos: u64,
        user_id: String,
        server_display_name: Option<String>,
        global_display_name: Option<String>,
        username: String,
    ) -> Self {
        Self::UserIdentity {
            format: EVENT_FORMAT_VERSION,
            elapsed_nanos,
            user_id,
            server_display_name,
            global_display_name,
            username,
        }
    }

    pub(crate) fn unresolved_ssrc_abandoned(
        elapsed_nanos: u64,
        ssrc: u32,
        first_tick: u64,
        last_tick: u64,
        discarded_frames: u64,
        discarded_samples: u64,
        reason: String,
    ) -> Self {
        Self::UnresolvedSsrcAbandoned {
            format: EVENT_FORMAT_VERSION,
            elapsed_nanos,
            ssrc,
            first_tick,
            last_tick,
            discarded_frames,
            discarded_samples,
            reason,
        }
    }

    pub(crate) fn user_disconnected(elapsed_nanos: u64, user_id: String) -> Self {
        Self::UserDisconnected {
            format: EVENT_FORMAT_VERSION,
            elapsed_nanos,
            user_id,
        }
    }
}

pub(crate) fn write_event(writer: &mut impl Write, event: &SessionEvent) -> io::Result<()> {
    // One complete JSON object per line permits streaming append and inspection
    // of every complete prefix after a crash.
    serde_json::to_writer(&mut *writer, event).map_err(io::Error::other)?;
    writer.write_all(b"\n")
}

#[cfg(test)]
mod tests {
    use std::{
        env, process,
        time::{SystemTime, UNIX_EPOCH},
    };

    use super::*;

    #[test]
    fn new_session_is_recording_and_references_separate_participants() {
        let directory = test_directory("initial");
        let participants = ParticipantContext::empty_for_test();
        let store = create_store(&directory, &participants);

        assert_eq!(store.record().state, WorkflowState::Recording);
        assert_eq!(store.record().format, RECORDING_SESSION_FORMAT_VERSION);
        assert_eq!(store.record().stopped_at_unix_millis, None);
        assert_eq!(store.record().configuration_version, 1);
        assert_eq!(store.record().discord.guild_id, "123");
        assert_eq!(store.record().discord.channel_id, "456");
        assert_eq!(store.record().files.packets.format, 1);
        assert_eq!(store.record().files.playout.format, 2);
        assert_eq!(store.record().files.events.format, EVENT_FORMAT_VERSION);
        assert_eq!(store.record().files.participants.path, "participants.toml");
        assert_eq!(store.record().files.participants.format, 1);
        assert_eq!(store.record().files.tracks.path, "tracks.json");
        assert_eq!(store.record().files.tracks.format, 1);
        assert_eq!(store.record().files.work_items, None);
        assert_eq!(store.record().files.results, None);
        assert!(store.record().failures.is_empty());
        assert!(store.record().checkpoints.is_empty());

        let json = fs::read_to_string(directory.join("session.json")).unwrap();
        let value: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert!(value.get("participants").is_none());
        assert!(directory.join("participants.toml").is_file());

        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn format_three_session_remains_readable() {
        let directory = test_directory("format-three-event-one");
        let participants = ParticipantContext::empty_for_test();
        let store = create_store(&directory, &participants);
        let mut record = store.record().clone();
        record.format = PREVIOUS_SESSION_FORMAT_VERSION;
        record.files.events.format = LEGACY_EVENT_FORMAT_VERSION;
        fs::write(
            directory.join(SESSION_FILE_NAME),
            serde_json::to_vec_pretty(&record).unwrap(),
        )
        .unwrap();

        let reloaded = SessionStore::load(&directory).unwrap();

        assert_eq!(reloaded.record().format, PREVIOUS_SESSION_FORMAT_VERSION);
        assert_eq!(reloaded.record().files.work_items, None);
        assert_eq!(
            reloaded.record().files.events.format,
            LEGACY_EVENT_FORMAT_VERSION
        );
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn format_three_rejects_a_work_items_field_even_when_null() {
        let directory = test_directory("format-three-work-items");
        let participants = ParticipantContext::empty_for_test();
        let store = create_store(&directory, &participants);
        let mut value = serde_json::to_value(store.record()).unwrap();
        value["format"] = PREVIOUS_SESSION_FORMAT_VERSION.into();
        value["files"]["work_items"] = serde_json::Value::Null;
        fs::write(
            directory.join(SESSION_FILE_NAME),
            serde_json::to_vec_pretty(&value).unwrap(),
        )
        .unwrap();

        let error = SessionStore::load(&directory).err().unwrap();

        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(error.to_string().contains("format 3"));
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn publishing_work_manifest_upgrades_format_three_atomically() {
        let directory = test_directory("work-manifest-upgrade");
        let participants = ParticipantContext::empty_for_test();
        let mut store = create_store(&directory, &participants);
        store
            .transition(WorkflowState::RecordedClean, 2_000)
            .unwrap();
        store
            .transition(WorkflowState::ReadyForTranscription, 2_100)
            .unwrap();
        let mut record = store.record().clone();
        record.format = PREVIOUS_SESSION_FORMAT_VERSION;
        fs::write(
            directory.join(SESSION_FILE_NAME),
            serde_json::to_vec_pretty(&record).unwrap(),
        )
        .unwrap();
        let mut store = SessionStore::load(&directory).unwrap();

        store.publish_work_manifest(3_000).unwrap();

        let reloaded = SessionStore::load(&directory).unwrap();
        assert_eq!(reloaded.record().format, RECORDING_SESSION_FORMAT_VERSION);
        assert_eq!(
            reloaded.record().files.work_items,
            Some(FileDescription {
                path: WORK_ITEM_MANIFEST_PATH.to_owned(),
                format: WORK_ITEM_MANIFEST_FORMAT_VERSION,
            })
        );
        assert_eq!(
            reloaded.record().checkpoints.last().unwrap().stage,
            "work_manifest_built"
        );
        assert_eq!(
            reloaded.record().state,
            WorkflowState::ReadyForTranscription
        );
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn format_four_rejects_a_results_field_even_when_null() {
        let directory = test_directory("format-four-results");
        let participants = ParticipantContext::empty_for_test();
        let store = create_store(&directory, &participants);
        let mut value = serde_json::to_value(store.record()).unwrap();
        value["files"]["results"] = serde_json::Value::Null;
        fs::write(
            directory.join(SESSION_FILE_NAME),
            serde_json::to_vec_pretty(&value).unwrap(),
        )
        .unwrap();

        let error = SessionStore::load(&directory).err().unwrap();

        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(error.to_string().contains("formats 3 and 4"));
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn transcription_start_publishes_format_results_and_state_together() {
        let directory = test_directory("transcription-start");
        let participants = ParticipantContext::empty_for_test();
        let mut store = create_store(&directory, &participants);
        store
            .transition(WorkflowState::RecordedClean, 2_000)
            .unwrap();
        store
            .transition(WorkflowState::ReadyForTranscription, 2_100)
            .unwrap();
        store.publish_work_manifest(2_200).unwrap();

        store.publish_transcription_start(2_300).unwrap();

        let reloaded = SessionStore::load(&directory).unwrap();
        assert_eq!(reloaded.record().format, SESSION_FORMAT_VERSION);
        assert_eq!(reloaded.record().state, WorkflowState::Transcribing);
        assert_eq!(
            reloaded.record().files.results,
            Some(FileDescription {
                path: TRANSCRIPTION_RESULTS_PATH.to_owned(),
                format: TRANSCRIPTION_RESULT_FORMAT_VERSION,
            })
        );
        assert_eq!(
            reloaded.record().checkpoints.last().unwrap().stage,
            "transcription_started"
        );
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn work_items_reference_and_checkpoint_cannot_be_published_separately() {
        let directory = test_directory("work-manifest-pair");
        let participants = ParticipantContext::empty_for_test();
        let store = create_store(&directory, &participants);
        let mut record = store.record().clone();
        record.files.work_items = Some(FileDescription {
            path: WORK_ITEM_MANIFEST_PATH.to_owned(),
            format: WORK_ITEM_MANIFEST_FORMAT_VERSION,
        });

        let error = record.validate().unwrap_err();

        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(error.to_string().contains("must appear together"));
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn clean_and_incomplete_transitions_are_persisted() {
        for (label, state) in [
            ("clean", WorkflowState::RecordedClean),
            ("incomplete", WorkflowState::RecordedIncomplete),
        ] {
            let directory = test_directory(label);
            let participants = ParticipantContext::empty_for_test();
            let mut store = create_store(&directory, &participants);

            store.transition(state, 2000).unwrap();
            let reloaded = SessionStore::load(&directory).unwrap();

            assert_eq!(reloaded.record().state, state);
            assert_eq!(reloaded.record().stopped_at_unix_millis, Some(2000));
            fs::remove_dir_all(directory).unwrap();
        }
    }

    #[test]
    fn recorded_clean_failure_can_wait_for_operator() {
        let directory = test_directory("clean-to-operator");
        let participants = ParticipantContext::empty_for_test();
        let mut store = create_store(&directory, &participants);
        store
            .transition(WorkflowState::RecordedClean, 2_000)
            .unwrap();

        store
            .transition(WorkflowState::AwaitingOperator, 2_100)
            .unwrap();

        let reloaded = SessionStore::load(&directory).unwrap();
        assert_eq!(reloaded.record().state, WorkflowState::AwaitingOperator);
        assert_eq!(reloaded.record().stopped_at_unix_millis, Some(2_000));
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn failures_and_checkpoints_are_persisted() {
        let directory = test_directory("records");
        let participants = ParticipantContext::empty_for_test();
        let mut store = create_store(&directory, &participants);

        store
            .record_failure(1500, "capture_io", "failed to synchronise packets.dat")
            .unwrap();
        store.record_checkpoint(1600, "recording").unwrap();
        let reloaded = SessionStore::load(&directory).unwrap();

        assert_eq!(reloaded.record().failures.len(), 1);
        assert_eq!(
            reloaded.record().failures[0].state,
            WorkflowState::Recording
        );
        assert_eq!(reloaded.record().failures[0].kind, "capture_io");
        assert_eq!(reloaded.record().checkpoints[0].stage, "recording");
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn invalid_transition_is_rejected_without_changing_disk() {
        let directory = test_directory("invalid-transition");
        let participants = ParticipantContext::empty_for_test();
        let mut store = create_store(&directory, &participants);
        let original = fs::read(directory.join("session.json")).unwrap();

        let error = store.transition(WorkflowState::Complete, 2000).unwrap_err();

        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
        assert_eq!(store.record().state, WorkflowState::Recording);
        assert_eq!(fs::read(directory.join("session.json")).unwrap(), original);
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn replacement_leaves_complete_json_and_no_temporary_file() {
        let directory = test_directory("replacement");
        let participants = ParticipantContext::empty_for_test();
        let mut store = create_store(&directory, &participants);

        store
            .record_failure(1500, "capture_io", "first durable failure")
            .unwrap();
        store
            .record_failure(1600, "capture_io", "second durable failure")
            .unwrap();

        let reloaded = SessionStore::load(&directory).unwrap();
        assert_eq!(reloaded.record().failures.len(), 2);
        assert!(!directory.join(SESSION_TEMP_FILE_NAME).exists());
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn interrupted_temporary_write_does_not_replace_durable_state() {
        let directory = test_directory("interrupted-replacement");
        let participants = ParticipantContext::empty_for_test();
        let mut store = create_store(&directory, &participants);
        let durable = fs::read(directory.join("session.json")).unwrap();

        fs::write(directory.join(SESSION_TEMP_FILE_NAME), b"{\"incomplete\":").unwrap();

        let reloaded = SessionStore::load(&directory).unwrap();
        assert_eq!(reloaded.record().state, WorkflowState::Recording);
        assert_eq!(fs::read(directory.join("session.json")).unwrap(), durable);

        store
            .record_failure(1500, "capture_io", "durable update after interruption")
            .unwrap();
        let reloaded = SessionStore::load(&directory).unwrap();
        assert_eq!(reloaded.record().failures.len(), 1);
        assert!(!directory.join(SESSION_TEMP_FILE_NAME).exists());
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn absolute_and_escaping_artifact_paths_are_rejected() {
        let directory = test_directory("bad-paths");
        let participants = ParticipantContext::empty_for_test();
        let store = create_store(&directory, &participants);

        for invalid in [
            "/tmp/packets.dat",
            "../packets.dat",
            "journal/../packets.dat",
        ] {
            let mut record = store.record().clone();
            record.files.packets.path = invalid.to_owned();
            let mut bytes = serde_json::to_vec_pretty(&record).unwrap();
            bytes.push(b'\n');
            fs::write(directory.join("session.json"), bytes).unwrap();

            let error = SessionStore::load(&directory).err().unwrap();
            assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        }

        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn speaker_mapping_is_one_json_line() {
        let event =
            SessionEvent::speaker_mapping(123_456, 4326, Some("881203221593464864".into()), 1);
        let mut bytes = Vec::new();

        write_event(&mut bytes, &event).unwrap();

        let text = String::from_utf8(bytes).unwrap();
        assert_eq!(text.lines().count(), 1);
        let value: serde_json::Value = serde_json::from_str(&text).unwrap();
        assert_eq!(value["event"], "speaker_mapping");
        assert_eq!(value["format"], 2);
        assert_eq!(value["elapsed_nanos"], 123_456);
        assert_eq!(value["ssrc"], 4326);
        assert_eq!(value["user_id"], "881203221593464864");
        assert_eq!(value["speaking_bits"], 1);
    }

    #[test]
    fn format_one_speaker_mapping_remains_readable() {
        let event: SessionEvent = serde_json::from_str(concat!(
            "{\"event\":\"speaker_mapping\",\"format\":1,\"elapsed_nanos\":123,",
            "\"ssrc\":4326,\"user_id\":\"881203221593464864\",\"speaking_bits\":1}"
        ))
        .unwrap();

        assert!(matches!(
            event,
            SessionEvent::SpeakerMapping { format: 1, .. }
        ));
    }

    fn create_store<'a>(directory: &Path, participants: &'a ParticipantContext) -> SessionStore {
        SessionStore::create(
            directory,
            NewSession {
                session_id: "session-1000",
                started_at_unix_millis: 1000,
                configuration_version: 1,
                guild_id: "123",
                channel_id: "456",
                participants,
            },
        )
        .unwrap()
    }

    fn test_directory(label: &str) -> PathBuf {
        let directory = env::temp_dir().join(format!(
            "echoscribe-session-{label}-{}-{}",
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
