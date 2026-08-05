//! Participant-context TOML parsing and canonical session snapshots.
//!
//! Discord identity remains gateway evidence. This file adds optional speaker
//! context and materialises defaults into an immutable session-local snapshot.

use std::{
    collections::HashMap,
    fs,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Deserializer, Serialize, de::Error as _};

use crate::artifacts::{
    LEGACY_PARTICIPANT_SNAPSHOT_FORMAT_VERSION, PARTICIPANT_SNAPSHOT_FORMAT_VERSION,
};

const DEFAULT_CURRENT_ROLE: &str = "participant";

#[derive(Deserialize)]
struct ParticipantVersion {
    version: u32,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct LegacyParticipantFile {
    version: u32,
    #[serde(default)]
    participants: HashMap<String, LegacyFileParticipant>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct LegacyFileParticipant {
    character: Option<String>,
    #[serde(default)]
    role: LegacyParticipantRole,
    #[serde(default = "default_transcribe")]
    transcribe: bool,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CurrentParticipantFile {
    version: u32,
    #[serde(default)]
    transcript_name_source: TranscriptNameSource,
    #[serde(default)]
    participants: HashMap<String, CurrentFileParticipant>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CurrentFileParticipant {
    name: Option<String>,
    #[serde(default = "default_current_role")]
    role: String,
    #[serde(default = "default_transcribe")]
    transcribe: bool,
}

const fn default_transcribe() -> bool {
    true
}

fn default_current_role() -> String {
    DEFAULT_CURRENT_ROLE.to_owned()
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub(crate) enum TranscriptNameSource {
    #[default]
    Discord,
    Name,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
enum LegacyParticipantRole {
    #[default]
    Player,
    Gm,
}

impl<'de> Deserialize<'de> for LegacyParticipantRole {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        if value.eq_ignore_ascii_case("player") {
            Ok(Self::Player)
        } else if value.eq_ignore_ascii_case("gm") {
            Ok(Self::Gm)
        } else {
            Err(D::Error::unknown_variant(&value, &["player", "gm"]))
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
/// Validated speaker context keyed by a numeric Discord user ID.
pub(crate) struct Participant {
    pub(crate) discord_user_id: u64,
    pub(crate) name: Option<String>,
    /// Retained only when reading a format-1 participant snapshot.
    pub(crate) character: Option<String>,
    pub(crate) role: String,
    pub(crate) transcribe: bool,
}

#[derive(Debug)]
#[allow(dead_code)]
/// Source participant mapping plus canonical snapshot serialisation.
pub(crate) struct ParticipantContext {
    pub(crate) source_path: PathBuf,
    format_version: u16,
    transcript_name_source: TranscriptNameSource,
    participants: HashMap<u64, Participant>,
}

impl ParticipantContext {
    /// Read and validate the operator-maintained participant TOML.
    pub(crate) fn load(path: &Path) -> Result<Self> {
        let text = fs::read_to_string(path).with_context(|| {
            format!("failed to read participant context file {}", path.display())
        })?;
        Self::from_toml(&text, path)
    }

    pub(crate) fn from_toml(text: &str, path: &Path) -> Result<Self> {
        let version: ParticipantVersion = toml::from_str(text).with_context(|| {
            format!(
                "failed to parse participant context file {}",
                path.display()
            )
        })?;
        match u16::try_from(version.version).ok() {
            Some(LEGACY_PARTICIPANT_SNAPSHOT_FORMAT_VERSION) => Self::from_legacy_toml(text, path),
            Some(PARTICIPANT_SNAPSHOT_FORMAT_VERSION) => Self::from_current_toml(text, path),
            _ => bail!(
                "unsupported participant context version {}; expected {} or {}",
                version.version,
                LEGACY_PARTICIPANT_SNAPSHOT_FORMAT_VERSION,
                PARTICIPANT_SNAPSHOT_FORMAT_VERSION
            ),
        }
    }

    fn from_legacy_toml(text: &str, path: &Path) -> Result<Self> {
        let file: LegacyParticipantFile = toml::from_str(text).with_context(|| {
            format!(
                "failed to parse participant context file {}",
                path.display()
            )
        })?;
        let mut participants = HashMap::with_capacity(file.participants.len());
        for (user_id, participant) in file.participants {
            let discord_user_id = parse_discord_user_id(&user_id)?;
            let character = validate_optional_text(&user_id, "character", participant.character)?;
            participants.insert(
                discord_user_id,
                Participant {
                    discord_user_id,
                    name: None,
                    character,
                    role: match participant.role {
                        LegacyParticipantRole::Player => "player",
                        LegacyParticipantRole::Gm => "gm",
                    }
                    .to_owned(),
                    transcribe: participant.transcribe,
                },
            );
        }
        Ok(Self {
            source_path: path.to_path_buf(),
            format_version: u16::try_from(file.version).expect("legacy version fits in u16"),
            transcript_name_source: TranscriptNameSource::Discord,
            participants,
        })
    }

    fn from_current_toml(text: &str, path: &Path) -> Result<Self> {
        let file: CurrentParticipantFile = toml::from_str(text).with_context(|| {
            format!(
                "failed to parse participant context file {}",
                path.display()
            )
        })?;
        let mut participants = HashMap::with_capacity(file.participants.len());
        for (user_id, participant) in file.participants {
            let discord_user_id = parse_discord_user_id(&user_id)?;
            let name = validate_optional_text(&user_id, "name", participant.name)?;
            let role = validate_required_text(&user_id, "role", participant.role)?;
            participants.insert(
                discord_user_id,
                Participant {
                    discord_user_id,
                    name,
                    character: None,
                    role,
                    transcribe: participant.transcribe,
                },
            );
        }
        Ok(Self {
            source_path: path.to_path_buf(),
            format_version: u16::try_from(file.version).expect("current version fits in u16"),
            transcript_name_source: file.transcript_name_source,
            participants,
        })
    }

    pub(crate) const fn format_version(&self) -> u16 {
        self.format_version
    }

    pub(crate) const fn transcript_name_source(&self) -> TranscriptNameSource {
        self.transcript_name_source
    }

    pub(crate) fn default_role(&self) -> &'static str {
        if self.format_version == LEGACY_PARTICIPANT_SNAPSHOT_FORMAT_VERSION {
            "player"
        } else {
            DEFAULT_CURRENT_ROLE
        }
    }

    #[allow(dead_code)]
    pub(crate) fn get(&self, discord_user_id: u64) -> Option<&Participant> {
        self.participants.get(&discord_user_id)
    }

    pub(crate) fn discord_user_ids(&self) -> impl Iterator<Item = u64> + '_ {
        self.participants.keys().copied()
    }

    /// Apply an explicit operator policy migration to a session snapshot.
    pub(crate) fn set_transcribe(&mut self, discord_user_id: u64, transcribe: bool) {
        let default_role = self.default_role().to_owned();
        self.participants
            .entry(discord_user_id)
            .and_modify(|participant| participant.transcribe = transcribe)
            .or_insert(Participant {
                discord_user_id,
                name: None,
                character: None,
                role: default_role,
                transcribe,
            });
    }

    pub(crate) fn canonical_toml(&self) -> Result<String> {
        // Numeric ordering makes snapshots deterministic even though runtime
        // lookup uses a HashMap.
        let mut participants = self.participants.values().collect::<Vec<_>>();
        participants.sort_unstable_by_key(|participant| participant.discord_user_id);

        let mut text = format!("version = {}\n", self.format_version);
        if self.format_version == PARTICIPANT_SNAPSHOT_FORMAT_VERSION {
            let value = match self.transcript_name_source {
                TranscriptNameSource::Discord => "discord",
                TranscriptNameSource::Name => "name",
            };
            text.push_str(&format!("transcript_name_source = {value:?}\n"));
        }
        for participant in participants {
            text.push_str(&format!(
                "\n[participants.\"{}\"]\n",
                participant.discord_user_id
            ));
            if let Some(name) = &participant.name {
                let name = toml::Value::String(name.clone());
                text.push_str(&format!("name = {name}\n"));
            }
            if let Some(character) = &participant.character {
                let character = toml::Value::String(character.clone());
                text.push_str(&format!("character = {character}\n"));
            }
            let role = toml::Value::String(participant.role.clone());
            text.push_str(&format!("role = {role}\n"));
            text.push_str(&format!("transcribe = {}\n", participant.transcribe));
        }
        Ok(text)
    }

    #[cfg(test)]
    pub(crate) fn empty_for_test() -> Self {
        Self {
            source_path: PathBuf::from("participants.toml"),
            format_version: LEGACY_PARTICIPANT_SNAPSHOT_FORMAT_VERSION,
            transcript_name_source: TranscriptNameSource::Discord,
            participants: HashMap::new(),
        }
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.participants.len()
    }
}

fn validate_optional_text(
    user_id: &str,
    field: &str,
    value: Option<String>,
) -> Result<Option<String>> {
    value
        .map(|value| validate_required_text(user_id, field, value))
        .transpose()
}

fn validate_required_text(user_id: &str, field: &str, value: String) -> Result<String> {
    let value = value.trim();
    if value.is_empty() || value.contains(['\n', '\r']) {
        bail!("participant {user_id} {field} must be non-empty single-line text");
    }
    Ok(value.to_owned())
}

fn parse_discord_user_id(value: &str) -> Result<u64> {
    let id = value.parse::<u64>().with_context(|| {
        format!("participant key {value:?} must be an unsigned Discord user ID")
    })?;
    if id == 0 {
        bail!("participant Discord user ID must not be zero");
    }
    Ok(id)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn omitted_role_defaults_to_player_and_missing_mapping_is_nonfatal() {
        let context = ParticipantContext::from_toml(
            concat!(
                "version = 1\n",
                "[participants.\"881203221593464864\"]\n",
                "character = \"Example Character\"\n",
            ),
            Path::new("participants.toml"),
        )
        .unwrap();

        let participant = context.get(881203221593464864).unwrap();
        assert_eq!(participant.role, "player");
        assert!(participant.transcribe);
        assert_eq!(participant.character.as_deref(), Some("Example Character"));
        assert!(context.get(123456789012345678).is_none());
    }

    #[test]
    fn accepts_multiple_gms() {
        let context = ParticipantContext::from_toml(
            concat!(
                "version = 1\n",
                "[participants.\"111\"]\n",
                "role = \"gm\"\n",
                "[participants.\"222\"]\n",
                "role = \"gm\"\n",
            ),
            Path::new("participants.toml"),
        )
        .unwrap();

        assert_eq!(context.len(), 2);
        assert_eq!(context.get(111).unwrap().role, "gm");
        assert_eq!(context.get(222).unwrap().role, "gm");
    }

    #[test]
    fn roles_are_ascii_case_insensitive_and_snapshots_are_lowercase() {
        let context = ParticipantContext::from_toml(
            concat!(
                "version = 1\n",
                "[participants.\"111\"]\n",
                "role = \"GM\"\n",
                "[participants.\"222\"]\n",
                "role = \"PlAyEr\"\n",
            ),
            Path::new("participants.toml"),
        )
        .unwrap();

        assert_eq!(context.get(111).unwrap().role, "gm");
        assert_eq!(context.get(222).unwrap().role, "player");
        let snapshot = context.canonical_toml().unwrap();
        assert!(snapshot.contains("role = \"gm\""));
        assert!(snapshot.contains("role = \"player\""));
        assert!(!snapshot.contains("role = \"GM\""));
    }

    #[test]
    fn canonical_snapshot_is_numeric_id_ordered_and_materialises_roles() {
        let context = ParticipantContext::from_toml(
            concat!(
                "version = 1\n",
                "[participants.\"222\"]\n",
                "character = \"Second\"\n",
                "[participants.\"11\"]\n",
                "role = \"gm\"\n",
            ),
            Path::new("participants.toml"),
        )
        .unwrap();

        let snapshot = context.canonical_toml().unwrap();
        assert!(
            snapshot.find("[participants.\"11\"]").unwrap()
                < snapshot.find("[participants.\"222\"]").unwrap()
        );
        assert!(snapshot.contains("[participants.\"11\"]\nrole = \"gm\"\ntranscribe = true"));
        assert!(snapshot.contains(
            "[participants.\"222\"]\ncharacter = \"Second\"\nrole = \"player\"\ntranscribe = true"
        ));

        let reloaded =
            ParticipantContext::from_toml(&snapshot, Path::new("session/participants.toml"))
                .unwrap();
        assert_eq!(reloaded.get(222).unwrap().role, "player");
        assert!(reloaded.get(222).unwrap().transcribe);
    }

    #[test]
    fn explicit_transcription_exclusion_is_parsed_and_materialised() {
        let context = ParticipantContext::from_toml(
            concat!(
                "version = 1\n",
                "[participants.\"111\"]\n",
                "transcribe = false\n",
            ),
            Path::new("participants.toml"),
        )
        .unwrap();

        assert!(!context.get(111).unwrap().transcribe);
        assert!(
            context
                .canonical_toml()
                .unwrap()
                .contains("[participants.\"111\"]\nrole = \"player\"\ntranscribe = false")
        );
    }

    #[test]
    fn older_snapshot_without_transcribe_remains_compatible() {
        let context = ParticipantContext::from_toml(
            concat!(
                "version = 1\n",
                "[participants.\"111\"]\n",
                "role = \"gm\"\n",
            ),
            Path::new("session/participants.toml"),
        )
        .unwrap();

        assert!(context.get(111).unwrap().transcribe);
    }

    #[test]
    fn rejects_invalid_version() {
        let error = ParticipantContext::from_toml("version = 3\n", Path::new("participants.toml"))
            .err()
            .expect("unsupported version should fail");

        assert!(
            error
                .to_string()
                .contains("unsupported participant context version")
        );
    }

    #[test]
    fn rejects_invalid_discord_user_id() {
        let error = ParticipantContext::from_toml(
            concat!(
                "version = 1\n",
                "[participants.\"not-an-id\"]\n",
                "role = \"player\"\n",
            ),
            Path::new("participants.toml"),
        )
        .err()
        .expect("invalid Discord ID should fail");

        assert!(format!("{error:#}").contains("must be an unsigned Discord user ID"));
    }

    #[test]
    fn current_format_accepts_names_arbitrary_roles_and_name_attribution() {
        let context = ParticipantContext::from_toml(
            concat!(
                "version = 2\n",
                "transcript_name_source = \"name\"\n",
                "[participants.\"111\"]\n",
                "name = \" Professor Jane Smith \"\n",
                "role = \"session chair\"\n",
                "[participants.\"222\"]\n",
            ),
            Path::new("participants.toml"),
        )
        .unwrap();

        assert_eq!(
            context.format_version(),
            PARTICIPANT_SNAPSHOT_FORMAT_VERSION
        );
        assert_eq!(context.transcript_name_source(), TranscriptNameSource::Name);
        assert_eq!(
            context.get(111).unwrap().name.as_deref(),
            Some("Professor Jane Smith")
        );
        assert_eq!(context.get(111).unwrap().role, "session chair");
        assert_eq!(context.get(222).unwrap().role, "participant");
        assert!(context.get(222).unwrap().transcribe);

        let snapshot = context.canonical_toml().unwrap();
        assert!(snapshot.starts_with("version = 2\ntranscript_name_source = \"name\"\n"));
        assert!(snapshot.contains("name = \"Professor Jane Smith\""));
        assert!(!snapshot.contains("character"));
    }

    #[test]
    fn current_format_defaults_to_discord_attribution_and_rejects_character() {
        let context = ParticipantContext::from_toml(
            "version = 2\n[participants.\"111\"]\n",
            Path::new("participants.toml"),
        )
        .unwrap();
        assert_eq!(
            context.transcript_name_source(),
            TranscriptNameSource::Discord
        );

        let error = ParticipantContext::from_toml(
            "version = 2\n[participants.\"111\"]\ncharacter = \"Legacy\"\n",
            Path::new("participants.toml"),
        )
        .unwrap_err();
        assert!(format!("{error:#}").contains("unknown field"));
    }
}
