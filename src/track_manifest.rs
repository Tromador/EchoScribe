//! Durable manifest for routine Discord-user tracks.
//!
//! The manifest is published only after encoder finalisation and the attempted
//! `.flac.part` to `.flac` lifecycle. It describes both usable routine tracks
//! and incomplete tracks which require operator-controlled recovery.

use std::{
    fs::{self, File, OpenOptions},
    io::{self, Write},
    path::{Component, Path},
};

use serde::{Deserialize, Serialize};

use crate::{
    artifacts::{
        LEGACY_TRACK_MANIFEST_FORMAT_VERSION, TRACK_MANIFEST_FILE_NAME,
        TRACK_MANIFEST_FORMAT_VERSION,
    },
    diagnostics::{CHANNELS, SAMPLE_RATE},
};

const TRACK_MANIFEST_TEMP_FILE_NAME: &str = ".tracks.json.tmp";
const BITS_PER_SAMPLE: u16 = 16;

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum TrackState {
    Complete,
    Incomplete,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct TrackDescription {
    pub(crate) discord_user_id: String,
    pub(crate) display_name: String,
    pub(crate) role: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) character: Option<String>,
    pub(crate) path: String,
    pub(crate) state: TrackState,
    pub(crate) sample_rate: u32,
    pub(crate) channels: u16,
    pub(crate) bits_per_sample: u16,
    pub(crate) start_sample: u64,
    pub(crate) length_samples: u64,
    pub(crate) source_ssrcs: Vec<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) abandonment_reason: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) last_contiguous_sample: Option<u64>,
}

impl TrackDescription {
    pub(crate) fn new(
        discord_user_id: u64,
        display_name: String,
        role: String,
        character: Option<String>,
        path: String,
        state: TrackState,
        length_samples: u64,
        source_ssrcs: Vec<u32>,
        abandonment_reason: Option<String>,
    ) -> Self {
        Self {
            discord_user_id: discord_user_id.to_string(),
            display_name,
            role,
            character,
            path,
            state,
            sample_rate: SAMPLE_RATE,
            channels: CHANNELS,
            bits_per_sample: BITS_PER_SAMPLE,
            start_sample: 0,
            length_samples,
            source_ssrcs,
            abandonment_reason,
            last_contiguous_sample: (state == TrackState::Incomplete).then_some(length_samples),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct TrackManifest {
    pub(crate) format: u16,
    pub(crate) session_id: String,
    pub(crate) tracks: Vec<TrackDescription>,
}

impl TrackManifest {
    #[cfg(test)]
    pub(crate) fn new(session_id: String, tracks: Vec<TrackDescription>) -> Self {
        Self::new_with_format(LEGACY_TRACK_MANIFEST_FORMAT_VERSION, session_id, tracks)
    }

    pub(crate) fn new_with_format(
        format: u16,
        session_id: String,
        mut tracks: Vec<TrackDescription>,
    ) -> Self {
        tracks.sort_unstable_by_key(|track| {
            track
                .discord_user_id
                .parse::<u64>()
                .expect("track descriptions are constructed from numeric Discord IDs")
        });
        Self {
            format,
            session_id,
            tracks,
        }
    }

    pub(crate) fn validate(&self) -> io::Result<()> {
        if !matches!(
            self.format,
            LEGACY_TRACK_MANIFEST_FORMAT_VERSION | TRACK_MANIFEST_FORMAT_VERSION
        ) {
            return Err(invalid_data(format!(
                "unsupported track manifest format {}; expected {} or {}",
                self.format, LEGACY_TRACK_MANIFEST_FORMAT_VERSION, TRACK_MANIFEST_FORMAT_VERSION
            )));
        }
        if self.session_id.trim().is_empty() {
            return Err(invalid_data("track manifest session_id must not be empty"));
        }

        let mut previous_user_id = None;
        for track in &self.tracks {
            let user_id = track
                .discord_user_id
                .parse::<u64>()
                .ok()
                .filter(|user_id| *user_id != 0)
                .ok_or_else(|| {
                    invalid_data(format!(
                        "track Discord user ID {:?} is invalid",
                        track.discord_user_id
                    ))
                })?;
            if previous_user_id.is_some_and(|previous| user_id <= previous) {
                return Err(invalid_data(
                    "track manifest entries must be unique and numerically ordered",
                ));
            }
            previous_user_id = Some(user_id);

            validate_relative_path(&track.path)?;
            let expected_path = match track.state {
                TrackState::Complete => format!("tracks/user-{user_id}.flac"),
                TrackState::Incomplete => format!("tracks/user-{user_id}.flac.part"),
            };
            if track.path != expected_path {
                return Err(invalid_data(format!(
                    "track path must be {expected_path:?} for Discord user {user_id}"
                )));
            }
            if track.display_name.trim().is_empty() {
                return Err(invalid_data("track display_name must not be empty"));
            }
            if self.format == LEGACY_TRACK_MANIFEST_FORMAT_VERSION
                && !matches!(track.role.as_str(), "player" | "gm")
            {
                return Err(invalid_data("format-1 track role must be player or gm"));
            }
            if track.role.trim().is_empty() || track.role.contains(['\n', '\r']) {
                return Err(invalid_data(
                    "track role must be non-empty single-line text",
                ));
            }
            if track
                .character
                .as_ref()
                .is_some_and(|value| value.trim().is_empty())
            {
                return Err(invalid_data("track character must not be empty"));
            }
            if self.format == TRACK_MANIFEST_FORMAT_VERSION && track.character.is_some() {
                return Err(invalid_data(
                    "format-2 track manifest must not contain character metadata",
                ));
            }
            if track.sample_rate != SAMPLE_RATE
                || track.channels != CHANNELS
                || track.bits_per_sample != BITS_PER_SAMPLE
                || track.start_sample != 0
            {
                return Err(invalid_data(
                    "track audio description does not match routine FLAC format",
                ));
            }
            match track.state {
                TrackState::Complete => {
                    if !track.path.ends_with(".flac")
                        || track.path.ends_with(".flac.part")
                        || track.abandonment_reason.is_some()
                        || track.last_contiguous_sample.is_some()
                    {
                        return Err(invalid_data(
                            "complete track must use .flac without abandonment fields",
                        ));
                    }
                }
                TrackState::Incomplete => {
                    if !track.path.ends_with(".flac.part")
                        || track
                            .abandonment_reason
                            .as_deref()
                            .is_none_or(str::is_empty)
                        || track.last_contiguous_sample != Some(track.length_samples)
                    {
                        return Err(invalid_data(
                            "incomplete track must use .flac.part with abandonment details",
                        ));
                    }
                }
            }
        }
        Ok(())
    }

    /// Atomically publish the manifest after all track file decisions are fixed.
    pub(crate) fn write(&self, session_directory: &Path) -> io::Result<()> {
        self.validate()?;
        let mut bytes = serde_json::to_vec_pretty(self).map_err(io::Error::other)?;
        bytes.push(b'\n');

        let temporary_path = session_directory.join(TRACK_MANIFEST_TEMP_FILE_NAME);
        let final_path = session_directory.join(TRACK_MANIFEST_FILE_NAME);
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&temporary_path)?;
        file.write_all(&bytes)?;
        file.sync_all()?;
        fs::rename(&temporary_path, &final_path)?;
        File::open(session_directory)?.sync_all()
    }

    pub(crate) fn read(path: &Path) -> io::Result<Self> {
        let bytes = fs::read(path)?;
        let manifest: Self = serde_json::from_slice(&bytes).map_err(io::Error::other)?;
        manifest.validate()?;
        Ok(manifest)
    }
}

fn validate_relative_path(value: &str) -> io::Result<()> {
    let path = Path::new(value);
    if value.is_empty()
        || path.is_absolute()
        || path
            .components()
            .any(|component| !matches!(component, Component::Normal(_)))
    {
        return Err(invalid_data(
            "track path must be relative to and contained by the session directory",
        ));
    }
    Ok(())
}

fn invalid_data(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.into())
}

#[cfg(test)]
mod tests {
    use std::{
        env, fs, process,
        time::{SystemTime, UNIX_EPOCH},
    };

    use super::*;

    #[test]
    fn complete_and_incomplete_tracks_round_trip() {
        let directory = test_directory("round-trip");
        let manifest = TrackManifest::new(
            "session-123".to_owned(),
            vec![
                TrackDescription::new(
                    22,
                    "Second player".to_owned(),
                    "player".to_owned(),
                    None,
                    "tracks/user-22.flac.part".to_owned(),
                    TrackState::Incomplete,
                    1_920,
                    vec![200],
                    Some("queue_full".to_owned()),
                ),
                TrackDescription::new(
                    11,
                    "GM Name".to_owned(),
                    "gm".to_owned(),
                    Some("Emperor Coaltongue".to_owned()),
                    "tracks/user-11.flac".to_owned(),
                    TrackState::Complete,
                    2_880,
                    vec![100, 101],
                    None,
                ),
            ],
        );

        manifest.write(&directory).unwrap();

        let bytes = fs::read(directory.join(TRACK_MANIFEST_FILE_NAME)).unwrap();
        let reloaded: TrackManifest = serde_json::from_slice(&bytes).unwrap();
        reloaded.validate().unwrap();
        assert_eq!(reloaded, manifest);
        assert_eq!(reloaded.tracks[0].discord_user_id, "11");
        assert_eq!(
            reloaded.tracks[0].character.as_deref(),
            Some("Emperor Coaltongue")
        );
        assert_eq!(reloaded.tracks[1].state, TrackState::Incomplete);
        assert_eq!(reloaded.tracks[1].last_contiguous_sample, Some(1_920));
        assert!(!directory.join(TRACK_MANIFEST_TEMP_FILE_NAME).exists());
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn escaping_track_path_is_rejected_without_publishing_manifest() {
        let directory = test_directory("escaping-path");
        let manifest = TrackManifest::new(
            "session-123".to_owned(),
            vec![TrackDescription::new(
                11,
                "Player".to_owned(),
                "player".to_owned(),
                None,
                "../user-11.flac".to_owned(),
                TrackState::Complete,
                960,
                vec![100],
                None,
            )],
        );

        let error = manifest.write(&directory).unwrap_err();

        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(!directory.join(TRACK_MANIFEST_FILE_NAME).exists());
        fs::remove_dir_all(directory).unwrap();
    }

    fn test_directory(label: &str) -> std::path::PathBuf {
        let directory = env::temp_dir().join(format!(
            "echoscribe-track-manifest-{label}-{}-{}",
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
