import { Client, GatewayIntentBits, Events } from 'discord.js';
import {
  joinVoiceChannel,
  EndBehaviorType,
  getVoiceConnection,
  VoiceReceiver,
} from '@discordjs/voice';
import { createWriteStream, mkdirSync, appendFileSync, existsSync } from 'fs';
import { pipeline } from 'stream';
import * as prism from 'prism-media';
import * as dotenv from 'dotenv';
import * as wav from 'wav';
import path from 'path';
import { EventEmitter } from 'events';

dotenv.config();
EventEmitter.defaultMaxListeners = 0; // Suppress Node warning spam

// 🧾 Static user name mapping
const userNames: Record<string, string> = {
  '123456789012345678': 'Example User',
};

// 🔧 Config
const { DISCORD_TOKEN, GUILD_ID, CHANNEL_ID } = process.env;
if (!DISCORD_TOKEN || !GUILD_ID || !CHANNEL_ID) {
  console.error('Missing environment variables in .env');
  process.exit(1);
}

const AUDIO_BASE = './audio';
const client = new Client({
  intents: [GatewayIntentBits.Guilds, GatewayIntentBits.GuildVoiceStates],
});

let streamCount = 0;

client.once(Events.ClientReady, async () => {
  console.log(`🟢 Logged in as ${client.user?.tag}`);

  const guild = await client.guilds.fetch(GUILD_ID);
  const channel = await guild.channels.fetch(CHANNEL_ID);

  if (!channel || !channel.isVoiceBased()) {
    console.error('❌ Provided channel ID is not a voice channel');
    process.exit(1);
  }

  const connection = joinVoiceChannel({
    channelId: channel.id,
    guildId: guild.id,
    adapterCreator: channel.guild.voiceAdapterCreator,
    selfDeaf: false,
    selfMute: true,
  });

  console.log(`🔊 Joined voice channel: ${channel.name}`);

  const receiver = connection.receiver;
  setupReceiver(receiver);
});

function setupReceiver(receiver: VoiceReceiver) {
  receiver.speaking.on('start', (userId) => {
    console.log(`🗣️  User ${userNames[userId] ?? userId} started speaking`);
    captureUserAudio(receiver, userId);
  });

  receiver.speaking.on('end', (userId) => {
    console.log(`🔇 User ${userNames[userId] ?? userId} stopped speaking`);
  });
}

function captureUserAudio(receiver: VoiceReceiver, userId: string) {
  streamCount++;
  let finalized = false;

  const finalizeStream = () => {
    if (finalized) {
      return false;
    }

    finalized = true;
    streamCount = Math.max(0, streamCount - 1);
    return true;
  };

  if (streamCount > 100) {
    console.log(`******** Stream count: ${streamCount} ********`);
  }

  const opusStream = receiver.subscribe(userId, {
    end: {
      behavior: EndBehaviorType.AfterSilence,
      duration: 1000,
    },
  });

  opusStream.setMaxListeners(0);

  const pcmStream = new prism.opus.Decoder({
    rate: 48000,
    channels: 1,
    frameSize: 960,
  });

  const now = new Date();
  const yyyyMMdd = now.toISOString().split('T')[0];
  const timeStr = now.toTimeString().split(' ')[0].replace(/:/g, '-');
  const filename = `user_${userId}_${timeStr}.wav`;
  const dir = path.join(AUDIO_BASE, yyyyMMdd);
  const filepath = path.join(dir, filename);
  const logPath = path.join(dir, 'session_log.jsonl');

  if (!existsSync(dir)) {
    mkdirSync(dir, { recursive: true });
  }

  const wavWriter = new wav.Writer({
    sampleRate: 48000,
    channels: 1,
    bitDepth: 16,
  });

  const output = createWriteStream(filepath);
  wavWriter.pipe(output);

  const startTime = new Date();

  pipeline(opusStream, pcmStream, wavWriter, (err: Error | null) => {
    finalizeStream();

    const endTime = new Date();

    if (err) {
      console.error(`❌ Error writing stream for ${userId}:`, err);
      return;
    }

    const entry = {
      user: userNames[userId] ?? null,
      user_id: userId,
      start: startTime.toISOString(),
      end: endTime.toISOString(),
      filename,
    };

    appendFileSync(logPath, JSON.stringify(entry) + '\n');
    console.log(`✅ Recorded ${filename}`);
  });

  // Failsafe: clean up in case pipeline never resolves
  setTimeout(() => {
    finalizeStream();
  }, 15000);
}

// 🧼 Graceful shutdown on Ctrl+C
process.on('SIGINT', async () => {
  console.log('\n🛑 Caught SIGINT, shutting down...');

  const connection = getVoiceConnection(GUILD_ID!);
  if (connection) {
    connection.receiver.speaking.removeAllListeners();
    connection.destroy();
    console.log('🔌 Voice connection closed.');
  }

  client.removeAllListeners();
  client.destroy();
  console.log('👋 Discord client destroyed.');

  // Final hammer
  setTimeout(() => {
    console.log('💀 Forcing process exit');
    process.exit(0);
  }, 1000);
});

client.login(DISCORD_TOKEN);
