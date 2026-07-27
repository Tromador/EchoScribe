use std::{
    fs,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result, bail};
use serde::Deserialize;
use serenity::all::{ChannelId, GuildId};

const SUPPORTED_CONFIG_VERSION: u32 = 1;

#[derive(Deserialize)]
struct FileConfig {
    version: u32,
    discord: FileDiscordConfig,
    recording: FileRecordingConfig,
}

#[derive(Deserialize)]
struct FileDiscordConfig {
    token: String,
    guild_id: String,
    channel_id: String,
}

#[derive(Deserialize)]
struct FileRecordingConfig {
    output_directory: PathBuf,
}

pub(crate) struct Config {
    token: String,
    pub(crate) guild_id: GuildId,
    pub(crate) channel_id: ChannelId,
    pub(crate) output_directory: PathBuf,
}

impl Config {
    pub(crate) fn load(path: &Path) -> Result<Self> {
        let text = fs::read_to_string(path)
            .with_context(|| format!("failed to read configuration file {}", path.display()))?;

        Self::from_toml(&text, path)
    }

    fn from_toml(text: &str, path: &Path) -> Result<Self> {
        let file: FileConfig = toml::from_str(text)
            .with_context(|| format!("failed to parse configuration file {}", path.display()))?;

        if file.version != SUPPORTED_CONFIG_VERSION {
            bail!(
                "unsupported configuration version {}; expected {}",
                file.version,
                SUPPORTED_CONFIG_VERSION
            );
        }

        if file.discord.token.trim().is_empty() {
            bail!("discord.token must not be empty");
        }

        let guild_id = parse_discord_id("discord.guild_id", &file.discord.guild_id)?;
        let channel_id = parse_discord_id("discord.channel_id", &file.discord.channel_id)?;

        if file.recording.output_directory.as_os_str().is_empty() {
            bail!("recording.output_directory must not be empty");
        }

        let output_directory = if file.recording.output_directory.is_absolute() {
            file.recording.output_directory
        } else {
            path.parent()
                .unwrap_or_else(|| Path::new("."))
                .join(file.recording.output_directory)
        };

        Ok(Self {
            token: file.discord.token,
            guild_id: GuildId::new(guild_id),
            channel_id: ChannelId::new(channel_id),
            output_directory,
        })
    }

    pub(crate) fn token(&self) -> &str {
        &self.token
    }
}

fn parse_discord_id(field: &str, value: &str) -> Result<u64> {
    let id = value
        .parse::<u64>()
        .with_context(|| format!("{field} must be an unsigned decimal Discord ID"))?;

    if id == 0 {
        bail!("{field} must not be zero");
    }

    Ok(id)
}

#[cfg(test)]
mod tests {
    use super::*;

    const VALID_CONFIG: &str = r#"
version = 1

[discord]
token = "test-token"
guild_id = "123"
channel_id = "456"

[recording]
output_directory = "recordings"

[transcription]
model = "large-v3"
"#;

    #[test]
    fn loads_valid_config() {
        let config = Config::from_toml(VALID_CONFIG, Path::new("/srv/echoscribe/echoscribe.toml"))
            .expect("valid configuration should load");

        assert_eq!(config.token(), "test-token");
        assert_eq!(config.guild_id.get(), 123);
        assert_eq!(config.channel_id.get(), 456);
    }

    #[test]
    fn rejects_malformed_guild_id() {
        let input = VALID_CONFIG.replace(r#"guild_id = "123""#, r#"guild_id = "not-a-number""#);
        let error = parse_error(&input);

        assert!(error.contains("discord.guild_id must be an unsigned decimal Discord ID"));
    }

    #[test]
    fn rejects_unsupported_version() {
        let input = VALID_CONFIG.replace("version = 1", "version = 2");
        let error = parse_error(&input);

        assert!(error.contains("unsupported configuration version 2; expected 1"));
    }

    #[test]
    fn resolves_relative_output_directory_from_config_file() {
        let config = Config::from_toml(VALID_CONFIG, Path::new("/srv/echoscribe/echoscribe.toml"))
            .expect("valid configuration should load");

        assert_eq!(
            config.output_directory,
            PathBuf::from("/srv/echoscribe/recordings")
        );
    }

    fn parse_error(input: &str) -> String {
        match Config::from_toml(input, Path::new("echoscribe.toml")) {
            Ok(_) => panic!("configuration unexpectedly parsed successfully"),
            Err(error) => format!("{error:#}"),
        }
    }
}
