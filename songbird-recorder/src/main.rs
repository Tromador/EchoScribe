mod artifacts;
mod capture;
mod config;
mod diagnostics;
mod flac_tracks;
mod identity;
mod inspect;
mod journal;
mod participants;
mod playout;
mod recover;
mod session;
mod telemetry;

use std::{
    collections::HashSet,
    env,
    path::PathBuf,
    sync::{Arc, Mutex},
};

use anyhow::{Context as AnyhowContext, Result, bail};
use capture::CaptureDrain;
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

struct Handler {
    voice_manager: Arc<Songbird>,
    telemetry: Arc<VoiceTelemetry>,
    guild_id: GuildId,
    channel_id: ChannelId,
    voice_users: Mutex<HashSet<u64>>,
}

#[derive(Debug)]
enum Command {
    Record { config_path: PathBuf },
    Inspect { session_directory: PathBuf },
    Recover { session_directory: PathBuf },
    Export { session_directory: PathBuf },
}

#[async_trait]
impl EventHandler for Handler {
    async fn ready(&self, _context: Context, ready: Ready) {
        println!(
            "Connected to Discord as {} (user {}).",
            ready.user.name, ready.user.id
        );

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
    let config_path = match parse_command()? {
        Command::Record { config_path } => config_path,
        Command::Inspect { session_directory } => {
            return inspect::run(&session_directory);
        }
        Command::Recover { session_directory } => {
            return recover::run(&session_directory);
        }
        Command::Export { session_directory } => {
            return recover::export(&session_directory);
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
    )
    .context("failed to create the capture session")?;
    println!(
        "Capture session: {}.",
        capture_drain.session_directory().display()
    );
    let telemetry = Arc::new(VoiceTelemetry::new(capture_sender));
    let (mut client, voice_manager) = build_client(&config, Arc::clone(&telemetry))
        .await
        .context("failed to build the Discord client")?;
    let shard_manager = client.shard_manager.clone();
    let guild_id = config.guild_id;

    tokio::spawn(async move {
        match tokio::signal::ctrl_c().await {
            Ok(()) => {
                println!("Shutdown requested.");

                if let Err(error) = voice_manager.remove(guild_id).await {
                    eprintln!("failed to leave the voice channel cleanly: {error}");
                }

                stop_capture(capture_drain).await;
                telemetry.report();
                shard_manager.shutdown_all().await;
            }
            Err(error) => eprintln!("failed to listen for Ctrl-C: {error}"),
        }
    });

    println!("Connecting to Discord; press Ctrl-C to stop.");

    client
        .start()
        .await
        .context("Discord gateway client stopped with an error")?;

    println!("EchoScribe stopped.");

    Ok(())
}

fn parse_command() -> Result<Command> {
    parse_command_args(env::args_os().skip(1))
}

fn parse_command_args(mut args: impl Iterator<Item = std::ffi::OsString>) -> Result<Command> {
    let Some(first) = args.next() else {
        return Ok(Command::Record {
            config_path: PathBuf::from("echoscribe.toml"),
        });
    };

    if matches!(first.to_str(), Some("inspect" | "recover" | "export")) {
        let operation = first.to_string_lossy();
        let session_directory = args
            .next()
            .map(PathBuf::from)
            .ok_or_else(|| anyhow::anyhow!("{operation} requires a session directory"))?;
        if let Some(extra) = args.next() {
            bail!("unexpected argument {:?}", extra);
        }
        match first.to_str() {
            Some("inspect") => Ok(Command::Inspect { session_directory }),
            Some("recover") => Ok(Command::Recover { session_directory }),
            Some("export") => Ok(Command::Export { session_directory }),
            _ => unreachable!("command was matched above"),
        }
    } else {
        if let Some(extra) = args.next() {
            bail!("unexpected argument {:?}", extra);
        }
        Ok(Command::Record {
            config_path: PathBuf::from(first),
        })
    }
}

async fn stop_capture(capture: CaptureDrain) {
    capture.stop().await;
}

#[cfg(test)]
mod tests {
    use std::{ffi::OsString, path::Path};

    use super::*;

    #[test]
    fn no_arguments_uses_default_config() {
        let command = parse_command_args(Vec::<OsString>::new().into_iter()).unwrap();
        let Command::Record { config_path } = command else {
            panic!("expected record command");
        };
        assert_eq!(config_path, Path::new("echoscribe.toml"));
    }

    #[test]
    fn one_path_argument_selects_recording_config() {
        let command = parse_command_args([OsString::from("other.toml")].into_iter()).unwrap();
        let Command::Record { config_path } = command else {
            panic!("expected record command");
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
        let Command::Recover { session_directory } = command else {
            panic!("expected recover command");
        };
        assert_eq!(session_directory, Path::new("recordings/session-123"));
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
}
