use std::{
    collections::HashMap,
    fs,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result, bail};
use serde::Deserialize;

const SUPPORTED_PARTICIPANT_VERSION: u32 = 1;

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
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) enum ParticipantRole {
    #[default]
    Player,
    Gm,
}

impl<'de> Deserialize<'de> for ParticipantRole {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        match value.as_str() {
            "player" => Ok(Self::Player),
            "gm" => Ok(Self::Gm),
            _ => Err(serde::de::Error::unknown_variant(&value, &["player", "gm"])),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct Participant {
    pub(crate) discord_user_id: u64,
    pub(crate) character: Option<String>,
    pub(crate) role: ParticipantRole,
}

#[allow(dead_code)]
pub(crate) struct ParticipantContext {
    pub(crate) source_path: PathBuf,
    participants: HashMap<u64, Participant>,
}

impl ParticipantContext {
    pub(crate) fn load(path: &Path) -> Result<Self> {
        let text = fs::read_to_string(path).with_context(|| {
            format!("failed to read participant context file {}", path.display())
        })?;
        Self::from_toml(&text, path)
    }

    fn from_toml(text: &str, path: &Path) -> Result<Self> {
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
