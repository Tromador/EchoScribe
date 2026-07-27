use std::{
    collections::HashMap,
    fs::File,
    io::{BufReader, Seek, SeekFrom},
    path::Path,
};

use anyhow::{Context, Result, anyhow, bail};
use opus2::{Channels, Decoder, ErrorCode};
use songbird::packet::rtp::RtpPacket;

use crate::{
    diagnostics::{DecodedFrame, DiagnosticWriter},
    journal::{self, PacketRecord, ReadRecord as ReadPacketRecord},
    playout::{self, OpusPayloadBounds, PlayoutDecision, ReadRecord as ReadPlayoutRecord},
};

type PacketKey = (u32, u16, u32);

const SAMPLE_RATE: u32 = 48_000;
const INITIAL_DECODE_CAPACITY: usize = 1_920;
const MAX_DECODE_CAPACITY: usize = 11_520;
const FORMAT_ONE_TRANSPORT_SUFFIX_LENGTH: usize = 20;

struct PacketIndex {
    offsets: HashMap<PacketKey, u64>,
    records: u64,
    truncated_tail: bool,
}

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
    let packets_path = session_directory.join("packets.dat");
    let playout_path = session_directory.join("playout.dat");

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

    let mut output = DiagnosticWriter::new_recovery(session_directory).with_context(|| {
        format!(
            "failed to create recovery output in {}",
            session_directory.display()
        )
    })?;
    let mut decoders = HashMap::<u32, RecoveryDecoder>::new();
    let mut summary = RecoverySummary::default();

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
                summary.loss_decisions += 1;
                decoder.decode_loss(record.decoded_samples)?
            }
            PlayoutDecision::Packet {
                sequence,
                timestamp,
                opus_payload: opus_bounds,
            } => {
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
            tick: record.tick,
            ssrc: record.ssrc,
            samples,
        })?;
    }

    let tracks = output.finalize()?;

    println!(
        "Recovered {} decoded frames ({} samples) from {} playout decisions: \
         {} packets, {} losses, {} undecoded decisions skipped.",
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
    for track in tracks {
        println!(
            "Recovered WAV SSRC {}: {}, first tick {}, {} frames, {} source samples, \
             {} inserted silence samples, {} nonstandard frames.",
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

fn build_packet_index(path: &Path) -> Result<PacketIndex> {
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
    use super::*;

    #[test]
    fn decode_capacity_follows_songbird_sizes() {
        assert_eq!(next_decode_capacity(1_920), 2_880);
        assert_eq!(next_decode_capacity(2_880), 3_840);
        assert_eq!(next_decode_capacity(3_840), 5_760);
        assert_eq!(next_decode_capacity(5_760), 11_520);
        assert_eq!(next_decode_capacity(11_520), 11_520);
    }
}
