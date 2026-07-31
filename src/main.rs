//! EchoScribe command-line entry point and Discord lifecycle orchestration.
//!
//! Serenity owns gateway identity events, Songbird owns voice reception, and
//! both feed the capture boundary. Recovery and inspection avoid the live
//! Discord path entirely.

mod artifacts;
mod capture;
mod config;
mod continuation;
mod diagnostics;
mod flac_tracks;
mod identity;
mod inspect;
mod journal;
mod live_flac;
mod operation_lease;
mod orchestration;
mod participants;
mod playout;
mod recover;
mod routine_recovery;
mod session;
mod stage;
mod telemetry;
mod track_manifest;
mod transcription;
mod verify_tracks;
mod work_items;

use std::{
    collections::HashSet,
    env,
    path::PathBuf,
    sync::{Arc, Mutex},
};

use anyhow::{Context as AnyhowContext, Result, bail};
use capture::{CaptureDrain, RecordingOutcome};
use config::Config;
use serenity::{
    Client, async_trait,
    client::{Context, EventHandler},
    model::gateway::{GatewayIntents, Ready},
    model::guild::{Guild, Member},
    model::id::{ChannelId, GuildId},
    model::voice::VoiceState,
};
use songbird::{
    Config as SongbirdConfig, SerenityInit, Songbird,
    driver::{Channels, DecodeConfig, DecodeMode, SampleRate},
};
use telemetry::VoiceTelemetry;

/// Serenity gateway handler for identity and voice-channel membership evidence.
struct Handler {
    voice_manager: Arc<Songbird>,
    telemetry: Arc<VoiceTelemetry>,
    guild_id: GuildId,
    channel_id: ChannelId,
    voice_users: Mutex<HashSet<u64>>,
}

#[derive(Debug)]
/// Parsed top-level operation. Only normal and recording-only modes construct
/// a Discord client.
enum Command {
    Normal {
        config_path: PathBuf,
    },
    RecordOnly {
        config_path: PathBuf,
    },
    Inspect {
        session_directory: PathBuf,
    },
    Recover {
        session_directory: PathBuf,
        user_ids: Vec<u64>,
    },
    RecoverWav {
        session_directory: PathBuf,
    },
    Continue {
        session_directory: PathBuf,
        config_path: Option<PathBuf>,
    },
    BuildWorkItems {
        session_directory: PathBuf,
        config_path: PathBuf,
    },
    Transcribe {
        session_directory: PathBuf,
        config_path: PathBuf,
    },
    RebuildTranscript {
        session_directory: PathBuf,
    },
    Export {
        session_directory: PathBuf,
    },
    Verify {
        session_directory: PathBuf,
    },
}

#[async_trait]
impl EventHandler for Handler {
    async fn ready(&self, _context: Context, ready: Ready) {
        println!(
            "Connected to Discord as {} (user {}).",
            ready.user.name, ready.user.id
        );

        // Register receive telemetry before joining so initial voice evidence is
        // not lost during connection establishment.
        let call = self.voice_manager.get_or_insert(self.guild_id);
        {
            let mut call = call.lock().await;
            self.telemetry.register(&mut call);
        }

        match self
            .voice_manager
            .join(self.guild_id, self.channel_id)
            .await
        {
            Ok(_) => println!(
                "Voice connection established for guild {} and channel {}.",
                self.guild_id, self.channel_id
            ),
            Err(error) => eprintln!(
                "Failed to join guild {} channel {}: {error}",
                self.guild_id, self.channel_id
            ),
        }
    }

    async fn voice_state_update(
        &self,
        _context: Context,
        old: Option<VoiceState>,
        new: VoiceState,
    ) {
        let member = new
            .member
            .as_ref()
            .or_else(|| old.as_ref().and_then(|state| state.member.as_ref()));
        if member.is_some_and(|member| member.user.bot) {
            return;
        }

        let belongs_to_configured_guild = new.guild_id == Some(self.guild_id)
            || old
                .as_ref()
                .is_some_and(|state| state.guild_id == Some(self.guild_id));
        if !belongs_to_configured_guild {
            return;
        }

        if new.channel_id != Some(self.channel_id) {
            // Serenity does not reliably provide `old` voice state, so the
            // locally observed membership set supplies departure evidence.
            let was_present = self
                .voice_users
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .remove(&new.user_id.get());
            if was_present {
                self.telemetry.observe_user_disconnected(new.user_id.get());
            }
            return;
        }

        self.voice_users
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(new.user_id.get());
        let Some(member) = new.member.as_ref() else {
            return;
        };
        self.observe_member_identity(member);
    }

    async fn guild_create(&self, _context: Context, guild: Guild, _is_new: Option<bool>) {
        if guild.id != self.guild_id {
            return;
        }
        // Seed identities for users already seated before the bot connected;
        // gateway state is sufficient and avoids an HTTP member lookup.
        let mut current_users = HashSet::new();
        for voice_state in guild
            .voice_states
            .values()
            .filter(|state| state.channel_id == Some(self.channel_id))
        {
            let Some(member) = guild.members.get(&voice_state.user_id) else {
                continue;
            };
            if member.user.bot {
                continue;
            }
            current_users.insert(voice_state.user_id.get());
            self.observe_member_identity(member);
        }

        let departed_users = {
            let mut observed_users = self
                .voice_users
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let departed = observed_users
                .difference(&current_users)
                .copied()
                .collect::<Vec<_>>();
            *observed_users = current_users;
            departed
        };
        for user_id in departed_users {
            self.telemetry.observe_user_disconnected(user_id);
        }
    }
}

impl Handler {
    fn observe_member_identity(&self, member: &Member) {
        if member.user.bot {
            return;
        }
        self.telemetry.observe_user_identity(
            member.user.id.get(),
            member.nick.clone(),
            member.user.global_name.clone(),
            member.user.name.clone(),
        );
    }
}

async fn build_client(
    config: &Config,
    telemetry: Arc<VoiceTelemetry>,
) -> Result<(Client, Arc<Songbird>), serenity::Error> {
    // Voice-state intent supplies the required member identity evidence without
    // privileged message or content intents.
    let intents = GatewayIntents::GUILDS | GatewayIntents::GUILD_VOICE_STATES;
    let voice_config = SongbirdConfig::default().decode_mode(DecodeMode::Decode(
        DecodeConfig::new(Channels::Mono, SampleRate::Hz48000),
    ));
    let voice_manager = Songbird::serenity_from_config(voice_config);

    let client = Client::builder(config.token(), intents)
        .event_handler(Handler {
            voice_manager: Arc::clone(&voice_manager),
            telemetry,
            guild_id: config.guild_id,
            channel_id: config.channel_id,
            voice_users: Mutex::new(HashSet::new()),
        })
        .register_songbird_with(Arc::clone(&voice_manager))
        .await?;

    Ok((client, voice_manager))
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let (config_path, one_stop) = match parse_command()? {
        Command::Normal { config_path } => (config_path, true),
        Command::RecordOnly { config_path } => (config_path, false),
        Command::Inspect { session_directory } => {
            return inspect::run(&session_directory);
        }
        Command::Recover {
            session_directory,
            user_ids,
        } => {
            return routine_recovery::run(&session_directory, &user_ids);
        }
        Command::RecoverWav { session_directory } => {
            return recover::recover_wav(&session_directory);
        }
        Command::Continue {
            session_directory,
            config_path,
        } => {
            return match config_path {
                Some(config_path) => {
                    orchestration::continue_stage_aware(&session_directory, &config_path)
                }
                None => continuation::run(&session_directory),
            };
        }
        Command::BuildWorkItems {
            session_directory,
            config_path,
        } => {
            return work_items::run(&session_directory, &config_path);
        }
        Command::Transcribe {
            session_directory,
            config_path,
        } => {
            return transcription::run(&session_directory, &config_path);
        }
        Command::RebuildTranscript { session_directory } => {
            return transcription::rebuild_transcript(&session_directory);
        }
        Command::Export { session_directory } => {
            return recover::export(&session_directory);
        }
        Command::Verify { session_directory } => {
            return verify_tracks::run(&session_directory);
        }
    };

    let config = Config::load(&config_path)?;

    println!(
        "EchoScribe configured for guild {} and voice channel {}.",
        config.guild_id, config.channel_id
    );
    println!(
        "Recordings will be written to {}.",
        config.recording.output_directory.display()
    );

    let (capture_sender, capture_drain) = capture::start(
        &config.recording.output_directory,
        &config.guild_id.to_string(),
        &config.channel_id.to_string(),
        config.configuration_version,
        &config.participants,
        config.recording.diagnostic_wav,
    )
    .context("failed to create the capture session")?;
    println!(
        "Capture session: {}.",
        capture_drain.session_directory().display()
    );
    let session_directory = capture_drain.session_directory().to_path_buf();
    let telemetry = Arc::new(VoiceTelemetry::new(capture_sender));
    let (mut client, voice_manager) = match build_client(&config, Arc::clone(&telemetry)).await {
        Ok(client) => client,
        Err(error) => {
            let message = format!("failed to build the Discord client: {error}");
            if let Err(stop_error) = capture_drain
                .stop_after_failure("discord_client", message)
                .await
            {
                eprintln!("recording finalisation also failed: {stop_error}");
            }
            telemetry.report();
            return Err(anyhow::Error::new(error).context("failed to build the Discord client"));
        }
    };
    let shard_manager = client.shard_manager.clone();
    let guild_id = config.guild_id;

    println!("Connecting to Discord; press Ctrl-C to stop.");

    // Keep the drain in this orchestration scope so gateway termination and
    // Ctrl-C converge on the same leave -> drain -> gateway-shutdown route.
    let (gateway_result, recording_failure) = tokio::select! {
        signal = tokio::signal::ctrl_c() => {
            match signal {
                Ok(()) => {
                    println!("Shutdown requested.");
                    (Ok(()), None)
                }
                Err(error) => {
                    let message = format!("failed to listen for Ctrl-C: {error}");
                    (
                        Err(anyhow::Error::new(error).context(
                            "failed to listen for Ctrl-C",
                        )),
                        Some(("shutdown_signal", message)),
                    )
                }
            }
        }
        result = client.start() => {
            let result = result.context("Discord gateway client stopped with an error");
            let message = match &result {
                Ok(()) => "Discord gateway terminated before an explicit stop".to_owned(),
                Err(error) => error.to_string(),
            };
            (result, Some(("gateway_terminated", message)))
        }
    };

    if let Err(error) = voice_manager.remove(guild_id).await {
        eprintln!("failed to leave the voice channel cleanly: {error}");
    }

    let recording_result = stop_capture(capture_drain, recording_failure).await;
    telemetry.report();
    shard_manager.shutdown_all().await;

    let recording_outcome = recording_result.context("recording finalisation failed")?;
    require_clean_recording(recording_outcome)?;
    gateway_result?;

    if one_stop {
        orchestration::run_after_recording(&session_directory, &config_path)?;
    }

    println!("EchoScribe stopped.");

    Ok(())
}

fn require_clean_recording(recording_outcome: RecordingOutcome) -> Result<()> {
    match recording_outcome {
        RecordingOutcome::ReadyForTranscription => {
            println!("Recording finalised cleanly and is ready for transcription.");
            Ok(())
        }
        RecordingOutcome::AwaitingOperator { incomplete_users } => {
            if incomplete_users.is_empty() {
                bail!(
                    "recording stopped with an integrity fault; session is awaiting operator action"
                );
            }
            bail!(
                "recording stopped with incomplete tracks for Discord users {}; \
                 session is awaiting operator action",
                incomplete_users
                    .iter()
                    .map(u64::to_string)
                    .collect::<Vec<_>>()
                    .join(", ")
            );
        }
    }
}

fn parse_command() -> Result<Command> {
    parse_command_args(env::args_os().skip(1))
}

fn parse_command_args(mut args: impl Iterator<Item = std::ffi::OsString>) -> Result<Command> {
    let Some(first) = args.next() else {
        return Ok(Command::Normal {
            config_path: PathBuf::from("echoscribe.toml"),
        });
    };

    if first == "record" {
        let config_path = args
            .next()
            .map_or_else(|| PathBuf::from("echoscribe.toml"), PathBuf::from);
        require_no_extra_args(&mut args)?;
        return Ok(Command::RecordOnly { config_path });
    }

    if matches!(
        first.to_str(),
        Some(
            "inspect"
                | "recover"
                | "recover-wav"
                | "continue"
                | "build-work-items"
                | "transcribe"
                | "rebuild-transcript"
                | "export"
                | "verify"
        )
    ) {
        let operation = first.to_string_lossy();
        let session_directory = args
            .next()
            .map(PathBuf::from)
            .ok_or_else(|| anyhow::anyhow!("{operation} requires a session directory"))?;
        match first.to_str() {
            Some("recover") => {
                let mut user_ids = args
                    .map(|value| {
                        let display = value.to_string_lossy();
                        display
                            .parse::<u64>()
                            .ok()
                            .filter(|user_id| *user_id != 0)
                            .ok_or_else(|| {
                                anyhow::anyhow!(
                                    "recover user ID {display:?} must be a non-zero unsigned Discord ID"
                                )
                            })
                    })
                    .collect::<Result<Vec<_>>>()?;
                user_ids.sort_unstable();
                user_ids.dedup();
                Ok(Command::Recover {
                    session_directory,
                    user_ids,
                })
            }
            Some("inspect") => {
                require_no_extra_args(&mut args)?;
                Ok(Command::Inspect { session_directory })
            }
            Some("recover-wav") => {
                require_no_extra_args(&mut args)?;
                Ok(Command::RecoverWav { session_directory })
            }
            Some("continue") => {
                let config_path = args.next().map(PathBuf::from);
                require_no_extra_args(&mut args)?;
                Ok(Command::Continue {
                    session_directory,
                    config_path,
                })
            }
            Some("build-work-items") => {
                let config_path = args
                    .next()
                    .map(PathBuf::from)
                    .ok_or_else(|| anyhow::anyhow!("build-work-items requires a config path"))?;
                require_no_extra_args(&mut args)?;
                Ok(Command::BuildWorkItems {
                    session_directory,
                    config_path,
                })
            }
            Some("transcribe") => {
                let config_path = args
                    .next()
                    .map(PathBuf::from)
                    .ok_or_else(|| anyhow::anyhow!("transcribe requires a config path"))?;
                require_no_extra_args(&mut args)?;
                Ok(Command::Transcribe {
                    session_directory,
                    config_path,
                })
            }
            Some("rebuild-transcript") => {
                require_no_extra_args(&mut args)?;
                Ok(Command::RebuildTranscript { session_directory })
            }
            Some("export") => {
                require_no_extra_args(&mut args)?;
                Ok(Command::Export { session_directory })
            }
            Some("verify") => {
                require_no_extra_args(&mut args)?;
                Ok(Command::Verify { session_directory })
            }
            _ => unreachable!("command was matched above"),
        }
    } else {
        if let Some(extra) = args.next() {
            bail!("unexpected argument {:?}", extra);
        }
        Ok(Command::Normal {
            config_path: PathBuf::from(first),
        })
    }
}

fn require_no_extra_args(args: &mut impl Iterator<Item = std::ffi::OsString>) -> Result<()> {
    if let Some(extra) = args.next() {
        bail!("unexpected argument {:?}", extra);
    }
    Ok(())
}

async fn stop_capture(
    capture: CaptureDrain,
    failure: Option<(&'static str, String)>,
) -> std::io::Result<RecordingOutcome> {
    match failure {
        Some((kind, message)) => capture.stop_after_failure(kind, message).await,
        None => capture.stop().await,
    }
}

#[cfg(test)]
mod tests {
    use std::{ffi::OsString, path::Path};

    use super::*;

    #[test]
    fn no_arguments_uses_default_config() {
        let command = parse_command_args(Vec::<OsString>::new().into_iter()).unwrap();
        let Command::Normal { config_path } = command else {
            panic!("expected normal command");
        };
        assert_eq!(config_path, Path::new("echoscribe.toml"));
    }

    #[test]
    fn recording_fault_refuses_post_recording_pipeline() {
        let error = require_clean_recording(RecordingOutcome::AwaitingOperator {
            incomplete_users: vec![11, 22],
        })
        .unwrap_err();

        assert!(error.to_string().contains("awaiting operator action"));
        assert!(error.to_string().contains("11, 22"));
    }

    #[test]
    fn one_path_argument_selects_normal_config() {
        let command = parse_command_args([OsString::from("other.toml")].into_iter()).unwrap();
        let Command::Normal { config_path } = command else {
            panic!("expected normal command");
        };
        assert_eq!(config_path, Path::new("other.toml"));
    }

    #[test]
    fn record_command_is_explicitly_recording_only() {
        let default = parse_command_args([OsString::from("record")].into_iter()).unwrap();
        let Command::RecordOnly { config_path } = default else {
            panic!("expected recording-only command");
        };
        assert_eq!(config_path, Path::new("echoscribe.toml"));

        let selected = parse_command_args(
            [OsString::from("record"), OsString::from("other.toml")].into_iter(),
        )
        .unwrap();
        let Command::RecordOnly { config_path } = selected else {
            panic!("expected recording-only command");
        };
        assert_eq!(config_path, Path::new("other.toml"));
    }

    #[test]
    fn inspect_argument_selects_session_directory() {
        let command = parse_command_args(
            [
                OsString::from("inspect"),
                OsString::from("recordings/session-123"),
            ]
            .into_iter(),
        )
        .unwrap();
        let Command::Inspect { session_directory } = command else {
            panic!("expected inspect command");
        };
        assert_eq!(session_directory, Path::new("recordings/session-123"));
    }

    #[test]
    fn inspect_requires_exactly_one_session_directory() {
        let missing = parse_command_args([OsString::from("inspect")].into_iter()).unwrap_err();
        assert!(missing.to_string().contains("requires a session directory"));

        let extra = parse_command_args(
            [
                OsString::from("inspect"),
                OsString::from("session"),
                OsString::from("extra"),
            ]
            .into_iter(),
        )
        .unwrap_err();
        assert!(extra.to_string().contains("unexpected argument"));
    }

    #[test]
    fn recover_argument_selects_session_directory() {
        let command = parse_command_args(
            [
                OsString::from("recover"),
                OsString::from("recordings/session-123"),
            ]
            .into_iter(),
        )
        .unwrap();
        let Command::Recover {
            session_directory,
            user_ids,
        } = command
        else {
            panic!("expected recover command");
        };
        assert_eq!(session_directory, Path::new("recordings/session-123"));
        assert!(user_ids.is_empty());
    }

    #[test]
    fn recover_accepts_and_normalises_explicit_user_ids() {
        let command = parse_command_args(
            [
                OsString::from("recover"),
                OsString::from("recordings/session-123"),
                OsString::from("22"),
                OsString::from("11"),
                OsString::from("22"),
            ]
            .into_iter(),
        )
        .unwrap();
        let Command::Recover { user_ids, .. } = command else {
            panic!("expected recover command");
        };
        assert_eq!(user_ids, [11, 22]);
    }

    #[test]
    fn diagnostic_recovery_has_an_explicit_wav_command() {
        let command = parse_command_args(
            [
                OsString::from("recover-wav"),
                OsString::from("recordings/session-123"),
            ]
            .into_iter(),
        )
        .unwrap();
        let Command::RecoverWav { session_directory } = command else {
            panic!("expected recover-wav command");
        };
        assert_eq!(session_directory, Path::new("recordings/session-123"));
    }

    #[test]
    fn continue_selects_one_session_directory() {
        let command = parse_command_args(
            [
                OsString::from("continue"),
                OsString::from("recordings/session-123"),
            ]
            .into_iter(),
        )
        .unwrap();
        let Command::Continue {
            session_directory,
            config_path,
        } = command
        else {
            panic!("expected continue command");
        };
        assert_eq!(session_directory, Path::new("recordings/session-123"));
        assert_eq!(config_path, None);
    }

    #[test]
    fn transcription_continue_selects_session_and_config_paths() {
        let command = parse_command_args(
            [
                OsString::from("continue"),
                OsString::from("recordings/session-123"),
                OsString::from("echoscribe.toml"),
            ]
            .into_iter(),
        )
        .unwrap();
        let Command::Continue {
            session_directory,
            config_path,
        } = command
        else {
            panic!("expected continue command");
        };
        assert_eq!(session_directory, Path::new("recordings/session-123"));
        assert_eq!(config_path.as_deref(), Some(Path::new("echoscribe.toml")));
    }

    #[test]
    fn continue_rejects_more_than_session_and_optional_config() {
        let error = parse_command_args(
            [
                OsString::from("continue"),
                OsString::from("recordings/session-123"),
                OsString::from("echoscribe.toml"),
                OsString::from("unexpected"),
            ]
            .into_iter(),
        )
        .unwrap_err();

        assert!(error.to_string().contains("unexpected argument"));
    }

    #[test]
    fn build_work_items_selects_session_and_config_paths() {
        let command = parse_command_args(
            [
                OsString::from("build-work-items"),
                OsString::from("recordings/session-123"),
                OsString::from("echoscribe.toml"),
            ]
            .into_iter(),
        )
        .unwrap();
        let Command::BuildWorkItems {
            session_directory,
            config_path,
        } = command
        else {
            panic!("expected build-work-items command");
        };
        assert_eq!(session_directory, Path::new("recordings/session-123"));
        assert_eq!(config_path, Path::new("echoscribe.toml"));
    }

    #[test]
    fn build_work_items_requires_exactly_two_paths() {
        let missing_session =
            parse_command_args([OsString::from("build-work-items")].into_iter()).unwrap_err();
        assert!(
            missing_session
                .to_string()
                .contains("requires a session directory")
        );

        let missing_config = parse_command_args(
            [
                OsString::from("build-work-items"),
                OsString::from("recordings/session-123"),
            ]
            .into_iter(),
        )
        .unwrap_err();
        assert!(
            missing_config
                .to_string()
                .contains("requires a config path")
        );
    }

    #[test]
    fn transcribe_selects_session_and_config_paths() {
        let command = parse_command_args(
            [
                OsString::from("transcribe"),
                OsString::from("recordings/session-123"),
                OsString::from("echoscribe.toml"),
            ]
            .into_iter(),
        )
        .unwrap();
        let Command::Transcribe {
            session_directory,
            config_path,
        } = command
        else {
            panic!("expected transcribe command");
        };
        assert_eq!(session_directory, Path::new("recordings/session-123"));
        assert_eq!(config_path, Path::new("echoscribe.toml"));
    }

    #[test]
    fn transcribe_requires_exactly_two_paths() {
        let missing_session =
            parse_command_args([OsString::from("transcribe")].into_iter()).unwrap_err();
        assert!(
            missing_session
                .to_string()
                .contains("requires a session directory")
        );

        let missing_config = parse_command_args(
            [
                OsString::from("transcribe"),
                OsString::from("recordings/session-123"),
            ]
            .into_iter(),
        )
        .unwrap_err();
        assert!(
            missing_config
                .to_string()
                .contains("requires a config path")
        );

        let extra = parse_command_args(
            [
                OsString::from("transcribe"),
                OsString::from("recordings/session-123"),
                OsString::from("echoscribe.toml"),
                OsString::from("extra"),
            ]
            .into_iter(),
        )
        .unwrap_err();
        assert!(extra.to_string().contains("unexpected argument"));
    }

    #[test]
    fn rebuild_transcript_selects_exactly_one_session() {
        let command = parse_command_args(
            [
                OsString::from("rebuild-transcript"),
                OsString::from("recordings/session-123"),
            ]
            .into_iter(),
        )
        .unwrap();
        let Command::RebuildTranscript { session_directory } = command else {
            panic!("expected rebuild-transcript command");
        };
        assert_eq!(session_directory, Path::new("recordings/session-123"));

        let extra = parse_command_args(
            [
                OsString::from("rebuild-transcript"),
                OsString::from("recordings/session-123"),
                OsString::from("extra"),
            ]
            .into_iter(),
        )
        .unwrap_err();
        assert!(extra.to_string().contains("unexpected argument"));
    }

    #[test]
    fn export_argument_selects_session_directory() {
        let command = parse_command_args(
            [
                OsString::from("export"),
                OsString::from("recordings/session-123"),
            ]
            .into_iter(),
        )
        .unwrap();
        let Command::Export { session_directory } = command else {
            panic!("expected export command");
        };
        assert_eq!(session_directory, Path::new("recordings/session-123"));
    }

    #[test]
    fn verify_argument_selects_session_directory() {
        let command = parse_command_args(
            [
                OsString::from("verify"),
                OsString::from("recordings/session-123"),
            ]
            .into_iter(),
        )
        .unwrap();
        let Command::Verify { session_directory } = command else {
            panic!("expected verify command");
        };
        assert_eq!(session_directory, Path::new("recordings/session-123"));
    }
}
