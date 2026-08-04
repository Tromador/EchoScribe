//! Participant-context TOML parsing and canonical session snapshots.
//!
//! Discord identity remains gateway evidence. This file adds optional campaign
//! context and materialises defaults into an immutable session-local snapshot.

use std::{
    collections::HashMap,
    fs,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Deserializer, Serialize, de::Error as _};

use crate::artifacts::PARTICIPANT_SNAPSHOT_FORMAT_VERSION;

const SUPPORTED_PARTICIPANT_VERSION: u32 = PARTICIPANT_SNAPSHOT_FORMAT_VERSION as u32;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ParticipantFile {
    version: u32,
    #[serde(default)]
    participants: HashMap<String, FileParticipant>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct FileParticipant {
    character: Option<String>,
    #[serde(default)]
    role: ParticipantRole,
    #[serde(default = "default_transcribe")]
    transcribe: bool,
}

const fn default_transcribe() -> bool {
    true
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
/// Session role used by later transcript processing, not speaker identity.
pub(crate) enum ParticipantRole {
    #[default]
    Player,
    Gm,
}

impl<'de> Deserialize<'de> for ParticipantRole {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        // Accept natural human casing, while `Serialize` emits the canonical
        // lowercase spelling used by session snapshots.
        if value.eq_ignore_ascii_case("player") {
            Ok(Self::Player)
        } else if value.eq_ignore_ascii_case("gm") {
            Ok(Self::Gm)
        } else {
            Err(D::Error::unknown_variant(&value, &["player", "gm"]))
        }
    }
}

impl ParticipantRole {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Player => "player",
            Self::Gm => "gm",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
/// Validated campaign context keyed by a numeric Discord user ID.
pub(crate) struct Participant {
    pub(crate) discord_user_id: u64,
    pub(crate) character: Option<String>,
    pub(crate) role: ParticipantRole,
    pub(crate) transcribe: bool,
}

#[allow(dead_code)]
/// Source participant mapping plus canonical snapshot serialisation.
pub(crate) struct ParticipantContext {
    pub(crate) source_path: PathBuf,
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
        let file: ParticipantFile = toml::from_str(text).with_context(|| {
            format!(
                "failed to parse participant context file {}",
                path.display()
            )
        })?;

        if file.version != SUPPORTED_PARTICIPANT_VERSION {
            bail!(
                "unsupported participant context version {}; expected {}",
                file.version,
                SUPPORTED_PARTICIPANT_VERSION
            );
        }

        let mut participants = HashMap::with_capacity(file.participants.len());
        for (user_id, participant) in file.participants {
            let discord_user_id = parse_discord_user_id(&user_id)?;
            let character = participant
                .character
                .map(|character| {
                    if character.trim().is_empty() {
                        bail!("participant {user_id} character must not be empty");
                    }
                    Ok(character)
                })
                .transpose()?;

            participants.insert(
                discord_user_id,
                Participant {
                    discord_user_id,
                    character,
                    role: participant.role,
                    transcribe: participant.transcribe,
                },
            );
        }

        Ok(Self {
            source_path: path.to_path_buf(),
            participants,
        })
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
        self.participants
            .entry(discord_user_id)
            .and_modify(|participant| participant.transcribe = transcribe)
            .or_insert(Participant {
                discord_user_id,
                character: None,
                role: ParticipantRole::Player,
                transcribe,
            });
    }

    pub(crate) fn canonical_toml(&self) -> Result<String> {
        // Numeric ordering makes snapshots deterministic even though runtime
        // lookup uses a HashMap.
        let mut participants = self.participants.values().collect::<Vec<_>>();
        participants.sort_unstable_by_key(|participant| participant.discord_user_id);

        let mut text = format!("version = {SUPPORTED_PARTICIPANT_VERSION}\n");
        for participant in participants {
            text.push_str(&format!(
                "\n[participants.\"{}\"]\n",
                participant.discord_user_id
            ));
            if let Some(character) = &participant.character {
                let character = toml::Value::String(character.clone());
                text.push_str(&format!("character = {character}\n"));
            }
            text.push_str(&format!("role = \"{}\"\n", participant.role.as_str()));
            text.push_str(&format!("transcribe = {}\n", participant.transcribe));
        }
        Ok(text)
    }

    #[cfg(test)]
    pub(crate) fn empty_for_test() -> Self {
        Self {
            source_path: PathBuf::from("participants.toml"),
            participants: HashMap::new(),
        }
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.participants.len()
    }
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
        assert_eq!(participant.role, ParticipantRole::Player);
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
        assert_eq!(context.get(111).unwrap().role, ParticipantRole::Gm);
        assert_eq!(context.get(222).unwrap().role, ParticipantRole::Gm);
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

        assert_eq!(context.get(111).unwrap().role, ParticipantRole::Gm);
        assert_eq!(context.get(222).unwrap().role, ParticipantRole::Player);
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
        assert_eq!(reloaded.get(222).unwrap().role, ParticipantRole::Player);
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
        let error = ParticipantContext::from_toml("version = 2\n", Path::new("participants.toml"))
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
}
