mod capture;
mod config;
mod diagnostics;
mod journal;
mod session;
mod telemetry;

use std::{env, path::PathBuf, sync::Arc};

use anyhow::Context as AnyhowContext;
use capture::CaptureDrain;
use config::Config;
use serenity::{
    Client, async_trait,
    client::{Context, EventHandler},
    model::gateway::{GatewayIntents, Ready},
    model::id::{ChannelId, GuildId},
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
        })
        .register_songbird_with(Arc::clone(&voice_manager))
        .await?;

    Ok((client, voice_manager))
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config_path = env::args_os()
        .nth(1)
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("echoscribe.toml"));
    let config = Config::load(&config_path)?;

    println!(
        "EchoScribe configured for guild {} and voice channel {}.",
        config.guild_id, config.channel_id
    );
    println!(
        "Recordings will be written to {}.",
        config.output_directory.display()
    );

    let (capture_sender, capture_drain) = capture::start(
        &config.output_directory,
        &config.guild_id.to_string(),
        &config.channel_id.to_string(),
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

async fn stop_capture(capture: CaptureDrain) {
    capture.stop().await;
}
