//! Human-edited configuration loading and validation.
//!
//! Paths in the main TOML are resolved relative to that file, so invoking the
//! recorder from another working directory does not change their meaning.

use std::{
    fs, io,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result, bail};
use serde::Deserialize;
use serenity::all::{ChannelId, GuildId};

use crate::participants::ParticipantContext;

const SUPPORTED_CONFIG_VERSION: u32 = 1;

// These private `File*` types describe the on-disk schema. The public crate
// types below contain validated and resolved values used by the application.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct FileConfig {
    version: u32,
    discord: FileDiscordConfig,
    recording: FileRecordingConfig,
    participants: FileParticipantsConfig,
    transcription: FileTranscriptionConfig,
    segmentation: FileSegmentationConfig,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct FileDiscordConfig {
    token: String,
    guild_id: String,
    channel_id: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct FileRecordingConfig {
    output_directory: PathBuf,
    #[serde(default)]
    diagnostic_wav: bool,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct FileParticipantsConfig {
    file: PathBuf,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct FileTranscriptionConfig {
    model: String,
    language: String,
    device: String,
    compute_type: String,
    beam_size: u32,
    vocabulary_file: PathBuf,
    resume_rewind_seconds: u64,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct FileSegmentationConfig {
    vad_enabled: bool,
    merge_gap_ms: u64,
}

#[allow(dead_code)]
/// Validated settings used by the live recording path.
pub(crate) struct RecordingConfig {
    pub(crate) output_directory: PathBuf,
    pub(crate) diagnostic_wav: bool,
}

#[allow(dead_code)]
/// Validated transcription settings retained for later implementation slices.
pub(crate) struct TranscriptionConfig {
    pub(crate) model: String,
    pub(crate) language: String,
    pub(crate) device: String,
    pub(crate) compute_type: String,
    pub(crate) beam_size: u32,
    pub(crate) vocabulary_file: PathBuf,
    pub(crate) resume_rewind_seconds: u64,
}

#[derive(Debug)]
/// Settings needed by the offline Python worker, deliberately detached from
/// Discord and the mutable participant source.
pub(crate) struct OfflineTranscriptionConfig {
    pub(crate) model: String,
    pub(crate) language: String,
    pub(crate) device: String,
    pub(crate) compute_type: String,
    pub(crate) beam_size: u32,
    pub(crate) hotwords: Vec<String>,
    pub(crate) vocabulary_warning: Option<String>,
}

impl OfflineTranscriptionConfig {
    pub(crate) fn load(path: &Path) -> Result<Self> {
        let text = fs::read_to_string(path)
            .with_context(|| format!("failed to read configuration file {}", path.display()))?;
        let file: FileConfig = toml::from_str(&text)
            .with_context(|| format!("failed to parse configuration file {}", path.display()))?;

        if file.version != SUPPORTED_CONFIG_VERSION {
            bail!(
                "unsupported configuration version {}; expected {}",
                file.version,
                SUPPORTED_CONFIG_VERSION
            );
        }
        validate_nonempty("transcription.model", &file.transcription.model)?;
        validate_nonempty("transcription.language", &file.transcription.language)?;
        validate_nonempty("transcription.device", &file.transcription.device)?;
        validate_nonempty(
            "transcription.compute_type",
            &file.transcription.compute_type,
        )?;
        if file.transcription.beam_size == 0 {
            bail!("transcription.beam_size must be greater than zero");
        }
        if file.transcription.vocabulary_file.as_os_str().is_empty() {
            bail!("transcription.vocabulary_file must not be empty");
        }

        let vocabulary_path = resolve_path(path, file.transcription.vocabulary_file);
        let (hotwords, vocabulary_warning) = load_vocabulary(&vocabulary_path)?;
        Ok(Self {
            model: file.transcription.model,
            language: file.transcription.language,
            device: file.transcription.device,
            compute_type: file.transcription.compute_type,
            beam_size: file.transcription.beam_size,
            hotwords,
            vocabulary_warning,
        })
    }
}

#[allow(dead_code)]
/// Segmentation settings are loaded now so the one configuration schema remains
/// stable as post-recording range generation is introduced.
pub(crate) struct SegmentationConfig {
    pub(crate) vad_enabled: bool,
    pub(crate) merge_gap_ms: u64,
}

impl SegmentationConfig {
    /// Load the one setting required by offline range generation. This
    /// deliberately avoids participant-file I/O, Discord construction, and
    /// token validation.
    pub(crate) fn load_merge_gap_ms(path: &Path) -> Result<u64> {
        let text = fs::read_to_string(path)
            .with_context(|| format!("failed to read configuration file {}", path.display()))?;
        let file: FileConfig = toml::from_str(&text)
            .with_context(|| format!("failed to parse configuration file {}", path.display()))?;

        if file.version != SUPPORTED_CONFIG_VERSION {
            bail!(
                "unsupported configuration version {}; expected {}",
                file.version,
                SUPPORTED_CONFIG_VERSION
            );
        }
        if file.segmentation.merge_gap_ms == 0 {
            bail!("segmentation.merge_gap_ms must be greater than zero");
        }

        Ok(file.segmentation.merge_gap_ms)
    }
}

#[allow(dead_code)]
/// Fully validated runtime configuration with all local paths resolved.
pub(crate) struct Config {
    pub(crate) configuration_version: u32,
    token: String,
    pub(crate) guild_id: GuildId,
    pub(crate) channel_id: ChannelId,
    pub(crate) recording: RecordingConfig,
    pub(crate) participants: ParticipantContext,
    pub(crate) transcription: TranscriptionConfig,
    pub(crate) segmentation: SegmentationConfig,
}

impl Config {
    /// Load and validate a main TOML configuration plus its participant file.
    pub(crate) fn load(path: &Path) -> Result<Self> {
        let text = fs::read_to_string(path)
            .with_context(|| format!("failed to read configuration file {}", path.display()))?;

        Self::from_toml(&text, path)
    }

    fn from_toml(text: &str, path: &Path) -> Result<Self> {
        // Rejecting unknown fields catches misspellings rather than silently
        // running with a default the operator did not intend.
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

        validate_nonempty("transcription.model", &file.transcription.model)?;
        validate_nonempty("transcription.language", &file.transcription.language)?;
        validate_nonempty("transcription.device", &file.transcription.device)?;
        validate_nonempty(
            "transcription.compute_type",
            &file.transcription.compute_type,
        )?;
        if file.transcription.beam_size == 0 {
            bail!("transcription.beam_size must be greater than zero");
        }
        if file.transcription.vocabulary_file.as_os_str().is_empty() {
            bail!("transcription.vocabulary_file must not be empty");
        }
        if file.participants.file.as_os_str().is_empty() {
            bail!("participants.file must not be empty");
        }
        if file.segmentation.merge_gap_ms == 0 {
            bail!("segmentation.merge_gap_ms must be greater than zero");
        }

        let output_directory = resolve_path(path, file.recording.output_directory);
        let participant_path = resolve_path(path, file.participants.file);
        let vocabulary_file = resolve_path(path, file.transcription.vocabulary_file);
        let participants = ParticipantContext::load(&participant_path)?;

        Ok(Self {
            configuration_version: file.version,
            token: file.discord.token,
            guild_id: GuildId::new(guild_id),
            channel_id: ChannelId::new(channel_id),
            recording: RecordingConfig {
                output_directory,
                diagnostic_wav: file.recording.diagnostic_wav,
            },
            participants,
            transcription: TranscriptionConfig {
                model: file.transcription.model,
                language: file.transcription.language,
                device: file.transcription.device,
                compute_type: file.transcription.compute_type,
                beam_size: file.transcription.beam_size,
                vocabulary_file,
                resume_rewind_seconds: file.transcription.resume_rewind_seconds,
            },
            segmentation: SegmentationConfig {
                vad_enabled: file.segmentation.vad_enabled,
                merge_gap_ms: file.segmentation.merge_gap_ms,
            },
        })
    }

    pub(crate) fn token(&self) -> &str {
        &self.token
    }
}

fn resolve_path(config_path: &Path, value: PathBuf) -> PathBuf {
    if value.is_absolute() {
        value
    } else {
        config_path
            .parent()
            .unwrap_or_else(|| Path::new("."))
            .join(value)
    }
}

fn load_vocabulary(path: &Path) -> Result<(Vec<String>, Option<String>)> {
    let bytes = match fs::read(path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            return Ok((
                Vec::new(),
                Some(format!(
                    "vocabulary file {} is missing; continuing without hotwords",
                    path.display()
                )),
            ));
        }
        Err(error) => {
            return Err(error)
                .with_context(|| format!("failed to read vocabulary file {}", path.display()));
        }
    };
    let text = String::from_utf8(bytes)
        .with_context(|| format!("vocabulary file {} is not valid UTF-8", path.display()))?;
    let hotwords = text
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(str::to_owned)
        .collect::<Vec<_>>();
    let warning = hotwords.is_empty().then(|| {
        format!(
            "vocabulary file {} is empty; continuing without hotwords",
            path.display()
        )
    });
    Ok((hotwords, warning))
}

fn validate_nonempty(field: &str, value: &str) -> Result<()> {
    if value.trim().is_empty() {
        bail!("{field} must not be empty");
    }
    Ok(())
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
    use std::{
        env, process,
        time::{SystemTime, UNIX_EPOCH},
    };

    use super::*;

    const VALID_CONFIG: &str = r#"
version = 1

[discord]
token = "test-token"
guild_id = "123"
channel_id = "456"

[recording]
output_directory = "recordings"

[participants]
file = "participants.toml"

[transcription]
model = "large-v3"
language = "en"
device = "cuda"
compute_type = "float16"
beam_size = 5
vocabulary_file = "vocabulary.txt"
resume_rewind_seconds = 120

[segmentation]
vad_enabled = false
merge_gap_ms = 750
"#;

    #[test]
    fn loads_valid_config() {
        let directory = test_directory("valid");
        write_participants(&directory, "version = 1\n");
        let config_path = directory.join("echoscribe.toml");
        let config =
            Config::from_toml(VALID_CONFIG, &config_path).expect("valid configuration should load");

        assert_eq!(config.token(), "test-token");
        assert_eq!(config.guild_id.get(), 123);
        assert_eq!(config.channel_id.get(), 456);
        assert!(!config.recording.diagnostic_wav);
        assert_eq!(config.transcription.resume_rewind_seconds, 120);
        assert!(!config.segmentation.vad_enabled);
        assert_eq!(config.segmentation.merge_gap_ms, 750);

        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn offline_merge_gap_load_does_not_read_participants_or_validate_discord() {
        let directory = test_directory("offline-segmentation");
        let config_path = directory.join("echoscribe.toml");
        let input = VALID_CONFIG
            .replace(r#"token = "test-token""#, r#"token = """#)
            .replace(r#"guild_id = "123""#, r#"guild_id = "not-a-number""#);
        fs::write(&config_path, input).unwrap();

        let merge_gap_ms = SegmentationConfig::load_merge_gap_ms(&config_path).unwrap();

        assert_eq!(merge_gap_ms, 750);
        assert!(!directory.join("participants.toml").exists());
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn offline_transcription_loads_trimmed_vocabulary_without_discord_or_participants() {
        let directory = test_directory("offline-transcription");
        let config_path = directory.join("echoscribe.toml");
        let input = VALID_CONFIG
            .replace(r#"token = "test-token""#, r#"token = """#)
            .replace(r#"guild_id = "123""#, r#"guild_id = "not-a-number""#);
        fs::write(&config_path, input).unwrap();
        fs::write(
            directory.join("vocabulary.txt"),
            " Emperor Coaltongue \n\nDragon Lance\n",
        )
        .unwrap();

        let config = OfflineTranscriptionConfig::load(&config_path).unwrap();

        assert_eq!(config.model, "large-v3");
        assert_eq!(config.hotwords, ["Emperor Coaltongue", "Dragon Lance"]);
        assert_eq!(config.vocabulary_warning, None);
        assert!(!directory.join("participants.toml").exists());
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn missing_and_empty_vocabulary_are_warning_only() {
        for (label, contents) in [("missing", None), ("empty", Some(" \n\t\n"))] {
            let directory = test_directory(label);
            let config_path = directory.join("echoscribe.toml");
            fs::write(&config_path, VALID_CONFIG).unwrap();
            if let Some(contents) = contents {
                fs::write(directory.join("vocabulary.txt"), contents).unwrap();
            }

            let config = OfflineTranscriptionConfig::load(&config_path).unwrap();

            assert!(config.hotwords.is_empty());
            assert!(config.vocabulary_warning.is_some());
            fs::remove_dir_all(directory).unwrap();
        }
    }

    #[test]
    fn invalid_utf8_vocabulary_fails_clearly() {
        let directory = test_directory("invalid-vocabulary");
        let config_path = directory.join("echoscribe.toml");
        fs::write(&config_path, VALID_CONFIG).unwrap();
        fs::write(directory.join("vocabulary.txt"), [0xff, 0xfe]).unwrap();

        let error = OfflineTranscriptionConfig::load(&config_path).unwrap_err();

        assert!(format!("{error:#}").contains("not valid UTF-8"));
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn rejects_malformed_guild_id() {
        let directory = test_directory("bad-guild");
        write_participants(&directory, "version = 1\n");
        let input = VALID_CONFIG.replace(r#"guild_id = "123""#, r#"guild_id = "not-a-number""#);
        let error = parse_error(&input, &directory.join("echoscribe.toml"));

        assert!(error.contains("discord.guild_id must be an unsigned decimal Discord ID"));
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn rejects_unsupported_version() {
        let directory = test_directory("bad-version");
        write_participants(&directory, "version = 1\n");
        let input = VALID_CONFIG.replace("version = 1", "version = 2");
        let error = parse_error(&input, &directory.join("echoscribe.toml"));

        assert!(error.contains("unsupported configuration version 2; expected 1"));
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn resolves_relative_paths_from_config_file() {
        let directory = test_directory("paths");
        write_participants(&directory, "version = 1\n");
        let config_path = directory.join("echoscribe.toml");
        let config =
            Config::from_toml(VALID_CONFIG, &config_path).expect("valid configuration should load");

        assert_eq!(
            config.recording.output_directory,
            directory.join("recordings")
        );
        assert_eq!(
            config.transcription.vocabulary_file,
            directory.join("vocabulary.txt")
        );
        assert_eq!(
            config.participants.source_path,
            directory.join("participants.toml")
        );
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn missing_participant_file_is_an_error() {
        let directory = test_directory("missing-participants");
        let error = parse_error(VALID_CONFIG, &directory.join("echoscribe.toml"));

        assert!(error.contains("failed to read participant context file"));
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn parses_resume_rewind_seconds() {
        let directory = test_directory("rewind");
        write_participants(&directory, "version = 1\n");
        let input =
            VALID_CONFIG.replace("resume_rewind_seconds = 120", "resume_rewind_seconds = 45");
        let config = Config::from_toml(&input, &directory.join("echoscribe.toml")).unwrap();

        assert_eq!(config.transcription.resume_rewind_seconds, 45);
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn diagnostic_wav_defaults_to_false() {
        let directory = test_directory("wav-default");
        write_participants(&directory, "version = 1\n");
        let config = Config::from_toml(VALID_CONFIG, &directory.join("echoscribe.toml")).unwrap();

        assert!(!config.recording.diagnostic_wav);
        fs::remove_dir_all(directory).unwrap();
    }

    fn parse_error(input: &str, path: &Path) -> String {
        match Config::from_toml(input, path) {
            Ok(_) => panic!("configuration unexpectedly parsed successfully"),
            Err(error) => format!("{error:#}"),
        }
    }

    fn test_directory(label: &str) -> PathBuf {
        let directory = env::temp_dir().join(format!(
            "echoscribe-config-{label}-{}-{}",
            process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir(&directory).unwrap();
        directory
    }

    fn write_participants(directory: &Path, contents: &str) {
        fs::write(directory.join("participants.toml"), contents).unwrap();
    }
}
