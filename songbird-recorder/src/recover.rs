//! Explicit offline reconstruction from authoritative session journals.
//!
//! Recovery correlates playout decisions with indexed packet records, decodes
//! selected Opus payloads, and writes diagnostic WAV or verified FLAC. It is
//! never invoked automatically by the normal recording path.

use std::{
    collections::HashMap,
    fs::{self, File, OpenOptions},
    io::{BufReader, BufWriter, Seek, SeekFrom, Write},
    path::Path,
};

use anyhow::{Context, Result, anyhow, bail};
use opus2::{Channels, Decoder, ErrorCode};
use serde::Serialize;
use songbird::packet::rtp::RtpPacket;

use crate::{
    artifacts::{
        EVENT_JOURNAL_FILE_NAME, PACKET_JOURNAL_FILE_NAME, PLAYOUT_JOURNAL_FILE_NAME,
        TRACK_MANIFEST_FILE_NAME, TRACK_MANIFEST_FORMAT_VERSION,
    },
    diagnostics::{
        CHANNELS, DecodedFrame, DiagnosticWriter, SAMPLE_RATE as OUTPUT_SAMPLE_RATE,
        SAMPLES_PER_TICK, TrackSummary,
    },
    flac_tracks::FlacTrackWriter,
    journal::{self, PacketRecord, ReadRecord as ReadPacketRecord},
    playout::{self, OpusPayloadBounds, PlayoutDecision, ReadRecord as ReadPlayoutRecord},
    session::{EVENT_FORMAT_VERSION, LEGACY_EVENT_FORMAT_VERSION, SessionEvent},
};

type PacketKey = (u32, u16, u32);

const SAMPLE_RATE: u32 = 48_000;
const INITIAL_DECODE_CAPACITY: usize = 1_920;
const MAX_DECODE_CAPACITY: usize = 11_520;
const FORMAT_ONE_TRANSPORT_SUFFIX_LENGTH: usize = 20;

#[derive(Clone, Copy)]
/// Selects explicit diagnostic WAV recovery or aligned FLAC export.
enum OutputKind {
    Recovery,
    Tracks,
}

/// Common façade keeps journal replay independent of the derived output format.
enum AudioWriter {
    Recovery(DiagnosticWriter),
    Tracks(FlacTrackWriter),
}

/// Journal offsets keyed by the tuple referenced from playout evidence.
struct PacketIndex {
    offsets: HashMap<PacketKey, u64>,
    records: u64,
    truncated_tail: bool,
}

/// Per-SSRC Opus decoder retained across the complete replay.
struct RecoveryDecoder {
    decoder: Decoder,
    decode_capacity: usize,
}

#[derive(Default)]
struct RecoverySummary {
    decisions: u64,
    packet_decisions: u64,
    loss_decisions: u64,
    decoded_frames: u64,
    decoded_samples: u64,
    skipped_undecoded: u64,
    truncated_playout_tail: bool,
}

pub(crate) fn run(session_directory: &Path) -> Result<()> {
    decode_session(session_directory, OutputKind::Recovery)
}

pub(crate) fn export(session_directory: &Path) -> Result<()> {
    decode_session(session_directory, OutputKind::Tracks)
}

fn decode_session(session_directory: &Path, output_kind: OutputKind) -> Result<()> {
    let packets_path = session_directory.join(PACKET_JOURNAL_FILE_NAME);
    let playout_path = session_directory.join(PLAYOUT_JOURNAL_FILE_NAME);

    let packet_index = build_packet_index(&packets_path)?;
    let packet_file = File::open(&packets_path)
        .with_context(|| format!("failed to reopen packet journal {}", packets_path.display()))?;
    let mut packet_reader = BufReader::new(packet_file);
    let playout_file = File::open(&playout_path)
        .with_context(|| format!("failed to open playout journal {}", playout_path.display()))?;
    let mut playout_reader = BufReader::new(playout_file);
    let playout_format = playout::read_file_header(&mut playout_reader).with_context(|| {
        format!(
            "invalid playout journal header in {}",
            playout_path.display()
        )
    })?;

    let mut output = AudioWriter::new(session_directory, output_kind).with_context(|| {
        format!(
            "failed to create {} output in {}",
            output_kind.operation(),
            session_directory.display()
        )
    })?;
    let mut decoders = HashMap::<u32, RecoveryDecoder>::new();
    let mut summary = RecoverySummary::default();

    // Playout order, not packet arrival order, defines recovered PCM timing and
    // loss positions.
    loop {
        let record =
            match playout::read_record(&mut playout_reader, playout_format).with_context(|| {
                format!(
                    "invalid playout journal record in {}",
                    playout_path.display()
                )
            })? {
                ReadPlayoutRecord::Record(record) => record,
                ReadPlayoutRecord::EndOfFile => break,
                ReadPlayoutRecord::TruncatedTail => {
                    summary.truncated_playout_tail = true;
                    break;
                }
            };
        summary.decisions += 1;

        if record.decoded_samples == 0 {
            summary.skipped_undecoded += 1;
            continue;
        }

        let decoder = decoders
            .entry(record.ssrc)
            .or_insert(RecoveryDecoder::new()?);
        let samples = match record.decision {
            PlayoutDecision::Loss => {
                // Decode loss through the same stateful Opus decoder so packet
                // loss concealment matches the original live playout.
                summary.loss_decisions += 1;
                decoder.decode_loss(record.decoded_samples)?
            }
            PlayoutDecision::Packet {
                sequence,
                timestamp,
                opus_payload: opus_bounds,
            } => {
                // The selected tuple and payload bounds must agree with packet
                // evidence before any audio is regenerated.
                summary.packet_decisions += 1;
                let key = (record.ssrc, sequence, timestamp);
                let offset = packet_index.offsets.get(&key).ok_or_else(|| {
                    anyhow!(
                        "playout tick {} selects missing RTP packet SSRC {}, sequence {}, timestamp {}",
                        record.tick,
                        record.ssrc,
                        sequence,
                        timestamp
                    )
                })?;
                let packet = read_packet_at(&mut packet_reader, *offset, key)?;
                let payload = opus_payload(&packet, opus_bounds)?;
                decoder.decode_packet(payload, record.decoded_samples)?
            }
        };

        summary.decoded_frames += 1;
        summary.decoded_samples += samples.len() as u64;
        output.write_frame(DecodedFrame {
            elapsed_nanos: record.tick.saturating_mul(20_000_000),
            tick: record.tick,
            ssrc: record.ssrc,
            samples,
        })?;
    }

    let tracks = output.finalize()?;

    println!(
        "{} {} decoded frames ({} samples) from {} playout decisions: \
         {} packets, {} losses, {} undecoded decisions skipped.",
        output_kind.past_tense(),
        summary.decoded_frames,
        summary.decoded_samples,
        summary.decisions,
        summary.packet_decisions,
        summary.loss_decisions,
        summary.skipped_undecoded
    );
    println!(
        "Packet index: {} records, {}.",
        packet_index.records,
        tail_description(packet_index.truncated_tail)
    );
    println!(
        "Playout journal: {}.",
        tail_description(summary.truncated_playout_tail)
    );
    if matches!(output_kind, OutputKind::Tracks) {
        write_track_manifest(session_directory, &tracks)?;
    }

    for track in tracks {
        println!(
            "{} SSRC {}: {}, first tick {}, {} frames, {} source samples, \
             {} inserted silence samples, {} nonstandard frames.",
            output_kind.track_description(),
            track.ssrc,
            track.path.display(),
            track.first_tick,
            track.frames,
            track.source_samples,
            track.inserted_silence_samples,
            track.nonstandard_frames
        );
    }

    Ok(())
}

impl OutputKind {
    fn operation(self) -> &'static str {
        match self {
            Self::Recovery => "recovery",
            Self::Tracks => "track export",
        }
    }

    fn past_tense(self) -> &'static str {
        match self {
            Self::Recovery => "Recovered",
            Self::Tracks => "Exported",
        }
    }

    fn track_description(self) -> &'static str {
        match self {
            Self::Recovery => "Recovered WAV",
            Self::Tracks => "Aligned FLAC track",
        }
    }
}

impl AudioWriter {
    fn new(session_directory: &Path, output_kind: OutputKind) -> std::io::Result<Self> {
        match output_kind {
            OutputKind::Recovery => {
                DiagnosticWriter::new_recovery(session_directory).map(Self::Recovery)
            }
            OutputKind::Tracks => FlacTrackWriter::new(session_directory).map(Self::Tracks),
        }
    }

    fn write_frame(&mut self, frame: DecodedFrame) -> std::io::Result<()> {
        match self {
            Self::Recovery(writer) => writer.write_frame(&frame),
            Self::Tracks(writer) => writer.write_frame(frame),
        }
    }

    fn finalize(self) -> std::io::Result<Vec<TrackSummary>> {
        match self {
            Self::Recovery(writer) => writer.finalize(),
            Self::Tracks(writer) => writer.finalize(),
        }
    }
}

#[derive(Serialize)]
struct TrackManifest {
    format: u16,
    codec: &'static str,
    verification: &'static str,
    sample_rate: u32,
    bits_per_sample: u16,
    channels: u16,
    samples_per_tick: u64,
    timeline_origin: &'static str,
    event_journal_truncated_tail: bool,
    tracks: Vec<TrackDescription>,
}

#[derive(Serialize)]
struct TrackDescription {
    ssrc: u32,
    user_id: Option<String>,
    path: String,
    first_tick: u64,
    frames: u64,
    source_samples: u64,
    inserted_silence_samples: u64,
    nonstandard_frames: u64,
}

fn write_track_manifest(session_directory: &Path, tracks: &[TrackSummary]) -> Result<()> {
    let (speaker_mappings, event_journal_truncated_tail) =
        read_speaker_mappings(&session_directory.join(EVENT_JOURNAL_FILE_NAME))?;
    let descriptions = tracks
        .iter()
        .map(|track| {
            let relative_path = track
                .path
                .strip_prefix(session_directory)
                .with_context(|| {
                    format!(
                        "track path {} is not inside session directory {}",
                        track.path.display(),
                        session_directory.display()
                    )
                })?;
            let path = relative_path
                .to_str()
                .ok_or_else(|| {
                    anyhow!("track path {} is not valid UTF-8", relative_path.display())
                })?
                .to_owned();

            Ok(TrackDescription {
                ssrc: track.ssrc,
                user_id: speaker_mappings.get(&track.ssrc).cloned(),
                path,
                first_tick: track.first_tick,
                frames: track.frames,
                source_samples: track.source_samples,
                inserted_silence_samples: track.inserted_silence_samples,
                nonstandard_frames: track.nonstandard_frames,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let manifest = TrackManifest {
        format: TRACK_MANIFEST_FORMAT_VERSION,
        codec: "flac",
        verification: "flac_pcm_md5",
        sample_rate: OUTPUT_SAMPLE_RATE,
        bits_per_sample: 16,
        channels: CHANNELS,
        samples_per_tick: SAMPLES_PER_TICK,
        timeline_origin: "voice_tick_zero",
        event_journal_truncated_tail,
        tracks: descriptions,
    };
    let path = session_directory.join(TRACK_MANIFEST_FILE_NAME);
    let file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&path)
        .with_context(|| format!("failed to create track manifest {}", path.display()))?;
    let mut writer = BufWriter::new(file);

    serde_json::to_writer_pretty(&mut writer, &manifest)
        .with_context(|| format!("failed to write track manifest {}", path.display()))?;
    writer.write_all(b"\n")?;
    writer.flush()?;
    writer.get_ref().sync_data()?;
    println!("Track manifest written to {}.", path.display());

    Ok(())
}

fn read_speaker_mappings(path: &Path) -> Result<(HashMap<u32, String>, bool)> {
    // Later mappings replace earlier ones for manifest attribution; the event
    // journal itself retains the complete timestamped history.
    let bytes = fs::read(path)
        .with_context(|| format!("failed to read event journal {}", path.display()))?;
    let mut mappings = HashMap::new();
    let mut truncated_tail = false;

    for (line_index, line) in bytes.split_inclusive(|byte| *byte == b'\n').enumerate() {
        if !line.ends_with(b"\n") {
            truncated_tail = true;
            break;
        }

        let json = line.strip_suffix(b"\n").expect("line ending checked above");
        let event: SessionEvent = serde_json::from_slice(json).with_context(|| {
            format!(
                "invalid event journal record on line {} in {}",
                line_index + 1,
                path.display()
            )
        })?;

        match event {
            SessionEvent::SpeakerMapping {
                format,
                ssrc,
                user_id,
                ..
            } => {
                if !matches!(format, LEGACY_EVENT_FORMAT_VERSION | EVENT_FORMAT_VERSION) {
                    bail!(
                        "unsupported event format {format} on line {} in {}",
                        line_index + 1,
                        path.display()
                    );
                }
                if let Some(user_id) = user_id {
                    mappings.insert(ssrc, user_id);
                }
            }
            SessionEvent::UserIdentity { format, .. }
            | SessionEvent::UserDisconnected { format, .. }
            | SessionEvent::UnresolvedSsrcAbandoned { format, .. } => {
                if format != EVENT_FORMAT_VERSION {
                    bail!(
                        "unsupported event format {format} on line {} in {}",
                        line_index + 1,
                        path.display()
                    );
                }
            }
        }
    }

    Ok((mappings, truncated_tail))
}

fn build_packet_index(path: &Path) -> Result<PacketIndex> {
    // Store offsets rather than packet bodies so long sessions do not require a
    // second in-memory copy of the authoritative packet journal.
    let file = File::open(path)
        .with_context(|| format!("failed to open packet journal {}", path.display()))?;
    let mut reader = BufReader::new(file);
    journal::read_file_header(&mut reader)
        .with_context(|| format!("invalid packet journal header in {}", path.display()))?;

    let mut index = PacketIndex {
        offsets: HashMap::new(),
        records: 0,
        truncated_tail: false,
    };

    loop {
        let offset = reader.stream_position()?;
        match journal::read_record(&mut reader)
            .with_context(|| format!("invalid packet journal record in {}", path.display()))?
        {
            ReadPacketRecord::Record(record) => {
                index.records += 1;
                index
                    .offsets
                    .insert((record.ssrc, record.sequence, record.timestamp), offset);
            }
            ReadPacketRecord::EndOfFile => break,
            ReadPacketRecord::TruncatedTail => {
                index.truncated_tail = true;
                break;
            }
        }
    }

    Ok(index)
}

fn read_packet_at(
    reader: &mut BufReader<File>,
    offset: u64,
    expected_key: PacketKey,
) -> Result<PacketRecord> {
    reader.seek(SeekFrom::Start(offset))?;
    let ReadPacketRecord::Record(record) = journal::read_record(reader)? else {
        bail!("packet index offset {offset} does not point to a complete packet record");
    };
    let actual_key = (record.ssrc, record.sequence, record.timestamp);
    if actual_key != expected_key {
        bail!(
            "packet index mismatch at offset {offset}: expected {:?}, found {:?}",
            expected_key,
            actual_key
        );
    }

    Ok(record)
}

fn opus_payload(
    record: &PacketRecord,
    recorded_bounds: Option<OpusPayloadBounds>,
) -> Result<&[u8]> {
    let rtp = RtpPacket::new(&record.packet)
        .ok_or_else(|| anyhow!("stored packet is not valid RTP for SSRC {}", record.ssrc))?;

    let embedded_key = (
        rtp.get_ssrc(),
        u16::from(rtp.get_sequence()),
        u32::from(rtp.get_timestamp()),
    );
    let recorded_key = (record.ssrc, record.sequence, record.timestamp);
    if embedded_key != recorded_key {
        bail!(
            "stored RTP header {:?} does not match journal metadata {:?}",
            embedded_key,
            recorded_key
        );
    }

    let (payload_start, payload_end) = match recorded_bounds {
        Some(bounds) => (bounds.start as usize, bounds.end as usize),
        None => (
            record.payload_start as usize,
            record
                .packet
                .len()
                .checked_sub(FORMAT_ONE_TRANSPORT_SUFFIX_LENGTH)
                .ok_or_else(|| anyhow!("stored RTP packet is shorter than its transport suffix"))?,
        ),
    };

    record
        .packet
        .get(payload_start..payload_end)
        .ok_or_else(|| {
            anyhow!(
                "invalid Opus payload bounds {}..{} for SSRC {}, sequence {}",
                payload_start,
                payload_end,
                record.ssrc,
                record.sequence
            )
        })
}

impl RecoveryDecoder {
    fn new() -> Result<Self> {
        Ok(Self {
            decoder: Decoder::new(SAMPLE_RATE, Channels::Mono)?,
            decode_capacity: INITIAL_DECODE_CAPACITY,
        })
    }

    fn decode_packet(&mut self, payload: &[u8], expected_samples: u32) -> Result<Vec<i16>> {
        // Opus frames can exceed the common 20 ms size. Grow conservatively up
        // to the codec maximum instead of assuming 960 decoded samples.
        loop {
            let mut samples = vec![0_i16; self.decode_capacity];
            match self.decoder.decode(payload, &mut samples, false) {
                Ok(decoded) => {
                    samples.truncate(decoded);
                    return verify_sample_count(samples, expected_samples);
                }
                Err(error)
                    if error.code() == ErrorCode::BufferTooSmall
                        && self.decode_capacity < MAX_DECODE_CAPACITY =>
                {
                    self.decode_capacity = next_decode_capacity(self.decode_capacity);
                }
                Err(error) => return Err(error.into()),
            }
        }
    }

    fn decode_loss(&mut self, expected_samples: u32) -> Result<Vec<i16>> {
        let mut samples = vec![0_i16; self.decode_capacity];
        let decoded = self.decoder.decode(&[], &mut samples, false)?;
        samples.truncate(2 * decoded);
        verify_sample_count(samples, expected_samples)
    }
}

fn verify_sample_count(samples: Vec<i16>, expected_samples: u32) -> Result<Vec<i16>> {
    let expected_samples = expected_samples as usize;
    if samples.len() != expected_samples {
        bail!(
            "recovered decoder produced {} samples; playout journal records {}",
            samples.len(),
            expected_samples
        );
    }

    Ok(samples)
}

fn next_decode_capacity(current: usize) -> usize {
    match current {
        1_920 => 2_880,
        2_880 => 3_840,
        3_840 => 5_760,
        5_760.. => MAX_DECODE_CAPACITY,
        _ => MAX_DECODE_CAPACITY,
    }
}

fn tail_description(truncated: bool) -> &'static str {
    if truncated {
        "recoverable truncated tail"
    } else {
        "clean tail"
    }
}

#[cfg(test)]
mod tests {
    use std::{
        env, process,
        time::{SystemTime, UNIX_EPOCH},
    };

    use super::*;

    #[test]
    fn decode_capacity_follows_songbird_sizes() {
        assert_eq!(next_decode_capacity(1_920), 2_880);
        assert_eq!(next_decode_capacity(2_880), 3_840);
        assert_eq!(next_decode_capacity(3_840), 5_760);
        assert_eq!(next_decode_capacity(5_760), 11_520);
        assert_eq!(next_decode_capacity(11_520), 11_520);
    }

    #[test]
    fn speaker_mapping_reader_ignores_a_truncated_final_event() {
        let directory = env::temp_dir().join(format!(
            "echoscribe-mapping-test-{}-{}",
            process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir(&directory).unwrap();
        let path = directory.join("events.ndjson");
        fs::write(
            &path,
            concat!(
                "{\"event\":\"speaker_mapping\",\"format\":1,\"elapsed_nanos\":123,",
                "\"ssrc\":4326,\"user_id\":\"881203221593464864\",\"speaking_bits\":1}\n",
                "{\"event\":\"speaker_mapping\""
            ),
        )
        .unwrap();

        let (mappings, truncated_tail) = read_speaker_mappings(&path).unwrap();
        assert_eq!(
            mappings.get(&4326).map(String::as_str),
            Some("881203221593464864")
        );
        assert!(truncated_tail);

        fs::remove_dir_all(directory).unwrap();
    }
}
