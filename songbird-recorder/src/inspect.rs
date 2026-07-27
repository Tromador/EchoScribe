use std::{
    collections::{BTreeMap, HashSet, VecDeque},
    fs::{self, File},
    io::BufReader,
    path::Path,
};

use anyhow::{Context, Result, bail};
use serde::Deserialize;

use crate::{
    diagnostics::SAMPLES_PER_TICK,
    journal::{self, ReadRecord as ReadPacketRecord},
    playout::{self, PlayoutDecision, ReadRecord as ReadPlayoutRecord},
    session::SessionEvent,
};

const RECENT_SEQUENCES: usize = 4096;

#[derive(Deserialize)]
struct SessionManifest {
    format: u16,
    session_id: String,
    discord: DiscordSession,
    files: SessionFiles,
}

#[derive(Deserialize)]
struct DiscordSession {
    guild_id: String,
    channel_id: String,
}

#[derive(Deserialize)]
struct SessionFiles {
    packets: FileDescription,
    playout: FileDescription,
    events: FileDescription,
}

#[derive(Deserialize)]
struct FileDescription {
    path: String,
    format: u16,
}

#[derive(Default)]
struct PacketInspection {
    records: u64,
    truncated_tail: bool,
    streams: BTreeMap<u32, PacketStream>,
    packet_keys: HashSet<(u32, u16, u32)>,
}

struct PacketStream {
    packets: u64,
    first_sequence: u16,
    latest_sequence: u16,
    forward_gap_events: u64,
    missing_slots_observed: u64,
    duplicates: u64,
    out_of_order: u64,
    recent_sequence_order: VecDeque<u16>,
    recent_sequences: HashSet<u16>,
}

#[derive(Default)]
struct PlayoutInspection {
    records: u64,
    packet_decisions: u64,
    loss_decisions: u64,
    unmatched_packets: u64,
    truncated_tail: bool,
    streams: BTreeMap<u32, PlayoutStream>,
}

#[derive(Default)]
struct PlayoutStream {
    decisions: u64,
    packet_decisions: u64,
    loss_decisions: u64,
    decoded_decisions: u64,
    nonstandard_decoded_frames: u64,
    first_tick: Option<u64>,
    last_tick: Option<u64>,
}

#[derive(Default)]
struct EventInspection {
    records: u64,
    truncated_tail: bool,
    mappings: Vec<SpeakerMapping>,
}

struct SpeakerMapping {
    elapsed_nanos: u64,
    ssrc: u32,
    user_id: Option<String>,
    speaking_bits: u8,
}

pub(crate) fn run(session_directory: &Path) -> Result<()> {
    let manifest = read_manifest(session_directory)?;
    validate_manifest(&manifest)?;

    let packets = inspect_packets(&session_directory.join("packets.dat"))?;
    let playout = inspect_playout(&session_directory.join("playout.dat"), &packets.packet_keys)?;
    let events = inspect_events(&session_directory.join("events.ndjson"))?;

    println!("Session inspection: {}.", session_directory.display());
    println!(
        "Manifest: format {}, session {}, guild {}, channel {}.",
        manifest.format,
        manifest.session_id,
        manifest.discord.guild_id,
        manifest.discord.channel_id
    );
    println!(
        "packets.dat: {} records, {}.",
        packets.records,
        tail_description(packets.truncated_tail)
    );
    for (ssrc, stream) in &packets.streams {
        println!(
            "  SSRC {ssrc}: {} packets, sequences {}..{}, {} forward gap events \
             ({} missing slots), {} duplicates, {} late/out-of-order.",
            stream.packets,
            stream.first_sequence,
            stream.latest_sequence,
            stream.forward_gap_events,
            stream.missing_slots_observed,
            stream.duplicates,
            stream.out_of_order
        );
    }

    println!(
        "playout.dat: {} decisions ({} packets, {} losses), {}, {} unmatched packet decisions.",
        playout.records,
        playout.packet_decisions,
        playout.loss_decisions,
        tail_description(playout.truncated_tail),
        playout.unmatched_packets
    );
    for (ssrc, stream) in &playout.streams {
        let ticks = match (stream.first_tick, stream.last_tick) {
            (Some(first), Some(last)) => format!("{first}..{last}"),
            _ => "none".to_owned(),
        };
        println!(
            "  SSRC {ssrc}: {} decisions across ticks {ticks}, {} packets, {} losses, \
             {} decoded, {} non-{}-sample decoded frames.",
            stream.decisions,
            stream.packet_decisions,
            stream.loss_decisions,
            stream.decoded_decisions,
            stream.nonstandard_decoded_frames,
            SAMPLES_PER_TICK
        );
    }

    println!(
        "events.ndjson: {} records, {}, {} speaker mappings.",
        events.records,
        tail_description(events.truncated_tail),
        events.mappings.len()
    );
    for mapping in &events.mappings {
        let user = mapping.user_id.as_deref().unwrap_or("unknown");
        println!(
            "  {:.3}s: SSRC {} -> user {} (speaking bits {}).",
            mapping.elapsed_nanos as f64 / 1_000_000_000.0,
            mapping.ssrc,
            user,
            mapping.speaking_bits
        );
    }

    if packets.truncated_tail || playout.truncated_tail || events.truncated_tail {
        println!("Inspection completed with one or more recoverable truncated tails.");
    } else if playout.unmatched_packets > 0 {
        println!("Inspection completed with unmatched playout packet decisions.");
    } else {
        println!("Inspection completed cleanly.");
    }

    Ok(())
}

fn read_manifest(session_directory: &Path) -> Result<SessionManifest> {
    let path = session_directory.join("session.json");
    let bytes = fs::read(&path)
        .with_context(|| format!("failed to read session manifest {}", path.display()))?;
    serde_json::from_slice(&bytes)
        .with_context(|| format!("failed to parse session manifest {}", path.display()))
}

fn validate_manifest(manifest: &SessionManifest) -> Result<()> {
    if manifest.format != 2 {
        bail!(
            "unsupported session manifest format {}; expected 2",
            manifest.format
        );
    }

    validate_file_description(
        "packets",
        &manifest.files.packets,
        "packets.dat",
        journal::FORMAT_VERSION,
    )?;
    validate_file_description(
        "playout",
        &manifest.files.playout,
        "playout.dat",
        playout::FORMAT_VERSION,
    )?;
    validate_file_description("events", &manifest.files.events, "events.ndjson", 1)
}

fn validate_file_description(
    name: &str,
    description: &FileDescription,
    expected_path: &str,
    expected_format: u16,
) -> Result<()> {
    if description.path != expected_path {
        bail!(
            "session manifest {name} path is {:?}; expected {:?}",
            description.path,
            expected_path
        );
    }
    if description.format != expected_format {
        bail!(
            "session manifest {name} format is {}; expected {}",
            description.format,
            expected_format
        );
    }

    Ok(())
}

fn inspect_packets(path: &Path) -> Result<PacketInspection> {
    let file = File::open(path)
        .with_context(|| format!("failed to open packet journal {}", path.display()))?;
    let mut reader = BufReader::new(file);
    journal::read_file_header(&mut reader)
        .with_context(|| format!("invalid packet journal header in {}", path.display()))?;

    let mut inspection = PacketInspection::default();
    loop {
        match journal::read_record(&mut reader)
            .with_context(|| format!("invalid packet journal record in {}", path.display()))?
        {
            ReadPacketRecord::Record(record) => {
                inspection.records += 1;
                inspection
                    .packet_keys
                    .insert((record.ssrc, record.sequence, record.timestamp));
                inspection
                    .streams
                    .entry(record.ssrc)
                    .and_modify(|stream| stream.observe(record.sequence))
                    .or_insert_with(|| PacketStream::new(record.sequence));
            }
            ReadPacketRecord::EndOfFile => break,
            ReadPacketRecord::TruncatedTail => {
                inspection.truncated_tail = true;
                break;
            }
        }
    }

    Ok(inspection)
}

fn inspect_playout(
    path: &Path,
    packet_keys: &HashSet<(u32, u16, u32)>,
) -> Result<PlayoutInspection> {
    let file = File::open(path)
        .with_context(|| format!("failed to open playout journal {}", path.display()))?;
    let mut reader = BufReader::new(file);
    playout::read_file_header(&mut reader)
        .with_context(|| format!("invalid playout journal header in {}", path.display()))?;

    let mut inspection = PlayoutInspection::default();
    loop {
        match playout::read_record(&mut reader)
            .with_context(|| format!("invalid playout journal record in {}", path.display()))?
        {
            ReadPlayoutRecord::Record(record) => {
                inspection.records += 1;
                let stream = inspection.streams.entry(record.ssrc).or_default();
                stream.observe_tick(record.tick);
                if record.decoded_samples > 0 {
                    stream.decoded_decisions += 1;
                    if u64::from(record.decoded_samples) != SAMPLES_PER_TICK {
                        stream.nonstandard_decoded_frames += 1;
                    }
                }

                match record.decision {
                    PlayoutDecision::Loss => {
                        inspection.loss_decisions += 1;
                        stream.loss_decisions += 1;
                    }
                    PlayoutDecision::Packet {
                        sequence,
                        timestamp,
                    } => {
                        inspection.packet_decisions += 1;
                        stream.packet_decisions += 1;
                        if !packet_keys.contains(&(record.ssrc, sequence, timestamp)) {
                            inspection.unmatched_packets += 1;
                        }
                    }
                }
            }
            ReadPlayoutRecord::EndOfFile => break,
            ReadPlayoutRecord::TruncatedTail => {
                inspection.truncated_tail = true;
                break;
            }
        }
    }

    Ok(inspection)
}

fn inspect_events(path: &Path) -> Result<EventInspection> {
    let bytes = fs::read(path)
        .with_context(|| format!("failed to read event journal {}", path.display()))?;
    let mut inspection = EventInspection::default();

    for (line_index, line) in bytes.split_inclusive(|byte| *byte == b'\n').enumerate() {
        let complete_line = line.ends_with(b"\n");
        let json = line.strip_suffix(b"\n").unwrap_or(line);
        if json.is_empty() {
            continue;
        }

        match serde_json::from_slice::<SessionEvent>(json) {
            Ok(SessionEvent::SpeakerMapping {
                elapsed_nanos,
                ssrc,
                user_id,
                speaking_bits,
                ..
            }) => {
                inspection.records += 1;
                inspection.mappings.push(SpeakerMapping {
                    elapsed_nanos,
                    ssrc,
                    user_id,
                    speaking_bits,
                });
            }
            Err(error) if !complete_line && error.is_eof() => {
                inspection.truncated_tail = true;
                break;
            }
            Err(error) => {
                return Err(error).with_context(|| {
                    format!(
                        "invalid event journal record {} in {}",
                        line_index + 1,
                        path.display()
                    )
                });
            }
        }
    }

    Ok(inspection)
}

impl PacketStream {
    fn new(sequence: u16) -> Self {
        let mut recent_sequence_order = VecDeque::with_capacity(RECENT_SEQUENCES);
        let mut recent_sequences = HashSet::with_capacity(RECENT_SEQUENCES);
        recent_sequence_order.push_back(sequence);
        recent_sequences.insert(sequence);

        Self {
            packets: 1,
            first_sequence: sequence,
            latest_sequence: sequence,
            forward_gap_events: 0,
            missing_slots_observed: 0,
            duplicates: 0,
            out_of_order: 0,
            recent_sequence_order,
            recent_sequences,
        }
    }

    fn observe(&mut self, sequence: u16) {
        self.packets += 1;

        if self.recent_sequences.contains(&sequence) {
            self.duplicates += 1;
            return;
        }

        let distance = sequence.wrapping_sub(self.latest_sequence);
        if distance == 0 {
            self.duplicates += 1;
        } else if distance < (1 << 15) {
            if distance > 1 {
                self.forward_gap_events += 1;
                self.missing_slots_observed += u64::from(distance - 1);
            }
            self.latest_sequence = sequence;
        } else {
            self.out_of_order += 1;
        }

        if self.recent_sequence_order.len() == RECENT_SEQUENCES {
            if let Some(expired) = self.recent_sequence_order.pop_front() {
                self.recent_sequences.remove(&expired);
            }
        }
        self.recent_sequence_order.push_back(sequence);
        self.recent_sequences.insert(sequence);
    }
}

impl PlayoutStream {
    fn observe_tick(&mut self, tick: u64) {
        self.decisions += 1;
        self.first_tick.get_or_insert(tick);
        self.last_tick = Some(tick);
    }
}

fn tail_description(truncated: bool) -> &'static str {
    if truncated {
        "recoverable truncated tail"
    } else {
        "clean tail"
    }
}
