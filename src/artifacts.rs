//! Canonical names and format versions for session artefacts.
//!
//! Keeping these values central prevents producers, manifests, and readers from
//! silently developing different ideas about the same file.

pub(crate) const PACKET_JOURNAL_FILE_NAME: &str = "packets.dat";
pub(crate) const PLAYOUT_JOURNAL_FILE_NAME: &str = "playout.dat";
pub(crate) const EVENT_JOURNAL_FILE_NAME: &str = "events.ndjson";
pub(crate) const PARTICIPANT_SNAPSHOT_FILE_NAME: &str = "participants.toml";
pub(crate) const TRACK_MANIFEST_FILE_NAME: &str = "tracks.json";
pub(crate) const TRACK_DIRECTORY_NAME: &str = "tracks";
pub(crate) const TRANSCRIPTION_DIRECTORY_NAME: &str = "transcription";
pub(crate) const WORK_ITEM_MANIFEST_FILE_NAME: &str = "work-items.jsonl";
pub(crate) const WORK_ITEM_MANIFEST_PATH: &str = "transcription/work-items.jsonl";
pub(crate) const TRANSCRIPTION_RESULTS_FILE_NAME: &str = "results.jsonl";
pub(crate) const TRANSCRIPTION_RESULTS_PATH: &str = "transcription/results.jsonl";
pub(crate) const PARTIAL_TRANSCRIPT_FILE_NAME: &str = "transcript.partial.txt";
pub(crate) const FINAL_TRANSCRIPT_PATH: &str = "transcription/transcript.txt";

pub(crate) const LEGACY_PARTICIPANT_SNAPSHOT_FORMAT_VERSION: u16 = 1;
pub(crate) const PARTICIPANT_SNAPSHOT_FORMAT_VERSION: u16 = 2;
pub(crate) const LEGACY_TRACK_MANIFEST_FORMAT_VERSION: u16 = 1;
pub(crate) const TRACK_MANIFEST_FORMAT_VERSION: u16 = 2;
pub(crate) const LEGACY_WORK_ITEM_MANIFEST_FORMAT_VERSION: u16 = 1;
pub(crate) const WORK_ITEM_MANIFEST_FORMAT_VERSION: u16 = 2;
pub(crate) const LEGACY_TRANSCRIPTION_RESULT_FORMAT_VERSION: u16 = 1;
pub(crate) const TRANSCRIPTION_RESULT_FORMAT_VERSION: u16 = 2;
pub(crate) const LEGACY_TRANSCRIPT_FORMAT_VERSION: u16 = 1;
pub(crate) const TRANSCRIPT_FORMAT_VERSION: u16 = 2;
