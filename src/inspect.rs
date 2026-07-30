//! Read-only inspection of session manifests and authoritative journals.
//!
//! Inspection consumes the paths and format versions declared by `session.json`
//! and reports clean or recoverably truncated tails without modifying a session.

use std::{
    collections::{BTreeMap, HashSet, VecDeque},
    fs::{self, File},
    io::BufReader,
    path::Path,
};

use anyhow::{Context, Result, bail};
use serde::Deserialize;

use crate::{
    artifacts::{EVENT_JOURNAL_FILE_NAME, PACKET_JOURNAL_FILE_NAME, PLAYOUT_JOURNAL_FILE_NAME},
    diagnostics::SAMPLES_PER_TICK,
    journal::{self, ReadRecord as ReadPacketRecord},
    playout::{self, PlayoutDecision, ReadRecord as ReadPlayoutRecord},
    session::{
        EVENT_FORMAT_VERSION, LEGACY_EVENT_FORMAT_VERSION, LEGACY_SESSION_FORMAT_VERSION,
        PREVIOUS_SESSION_FORMAT_VERSION, RECORDING_SESSION_FORMAT_VERSION, SESSION_FORMAT_VERSION,
        SessionEvent, WorkflowState,
    },
};

const RECENT_SEQUENCES: usize = 4096;

#[derive(Deserialize)]
/// Minimal manifest projection shared by current and supported legacy sessions.
struct SessionManifest {
    format: u16,
    session_id: String,
    state: Option<WorkflowState>,
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
/// Aggregate packet-journal health plus per-transport continuity.
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
/// Aggregate playout evidence, including links back to packet decisions.
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
/// Identity/event counts and the mapping timeline useful to an operator.
struct EventInspection {
    records: u64,
    truncated_tail: bool,
    mappings: Vec<SpeakerMapping>,
    identity_updates: u64,
    user_disconnections: u64,
    unresolved_abandonments: u64,
}

struct SpeakerMapping {
    elapsed_nanos: u64,
    ssrc: u32,
    user_id: Option<String>,
    speaking_bits: u8,
}

pub(crate) fn run(session_directory: &Path) -> Result<()> {
    // Validate the manifest before joining recorded paths to the session
    // directory. Absolute and escaping artefact paths are never accepted.
    let manifest = read_manifest(session_directory)?;
    validate_manifest(&manifest)?;

    let packets = inspect_packets(&session_directory.join(&manifest.files.packets.path))?;
    let playout = inspect_playout(
        &session_directory.join(&manifest.files.playout.path),
        &packets.packet_keys,
        manifest.files.playout.format,
    )?;
    let events = inspect_events(
        &session_directory.join(&manifest.files.events.path),
        manifest.files.events.format,
    )?;

    println!("Session inspection: {}.", session_directory.display());
    println!(
        "Manifest: format {}, session {}, guild {}, channel {}, state {}.",
        manifest.format,
        manifest.session_id,
        manifest.discord.guild_id,
        manifest.discord.channel_id,
        manifest
            .state
            .map(WorkflowState::as_str)
            .map(str::to_owned)
            .unwrap_or_else(|| "legacy_untracked".to_owned())
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
        "events.ndjson: {} records, {}, {} speaker mappings, {} identity updates, \
         {} user disconnections, {} unresolved-SSRC abandonments.",
        events.records,
        tail_description(events.truncated_tail),
        events.mappings.len(),
        events.identity_updates,
        events.user_disconnections,
        events.unresolved_abandonments,
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
    let value: serde_json::Value = serde_json::from_slice(&bytes)
        .with_context(|| format!("failed to parse session manifest {}", path.display()))?;
    let format = value
        .get("format")
        .and_then(serde_json::Value::as_u64)
        .ok_or_else(|| anyhow::anyhow!("session manifest has no valid format number"))?;

    match u16::try_from(format).ok() {
        Some(LEGACY_SESSION_FORMAT_VERSION) => {
            #[derive(Deserialize)]
            struct LegacySessionManifest {
                format: u16,
                session_id: String,
                discord: DiscordSession,
                files: SessionFiles,
            }

            let legacy: LegacySessionManifest =
                serde_json::from_value(value).with_context(|| {
                    format!("failed to parse legacy session manifest {}", path.display())
                })?;
            Ok(SessionManifest {
                format: legacy.format,
                session_id: legacy.session_id,
                state: None,
                discord: legacy.discord,
                files: legacy.files,
            })
        }
        Some(
            PREVIOUS_SESSION_FORMAT_VERSION
            | RECORDING_SESSION_FORMAT_VERSION
            | SESSION_FORMAT_VERSION,
        ) => {
            let current = crate::session::read_record(&path)
                .with_context(|| format!("failed to parse session manifest {}", path.display()))?;
            Ok(SessionManifest {
                format: current.format,
                session_id: current.session_id,
                state: Some(current.state),
                discord: DiscordSession {
                    guild_id: current.discord.guild_id,
                    channel_id: current.discord.channel_id,
                },
                files: SessionFiles {
                    packets: FileDescription {
                        path: current.files.packets.path,
                        format: current.files.packets.format,
                    },
                    playout: FileDescription {
                        path: current.files.playout.path,
                        format: current.files.playout.format,
                    },
                    events: FileDescription {
                        path: current.files.events.path,
                        format: current.files.events.format,
                    },
                },
            })
        }
        _ => bail!(
            "unsupported session manifest format {format}; expected {}, {}, {}, or {}",
            LEGACY_SESSION_FORMAT_VERSION,
            PREVIOUS_SESSION_FORMAT_VERSION,
            RECORDING_SESSION_FORMAT_VERSION,
            SESSION_FORMAT_VERSION
        ),
    }
}

fn validate_manifest(manifest: &SessionManifest) -> Result<()> {
    if !matches!(
        manifest.format,
        LEGACY_SESSION_FORMAT_VERSION
            | PREVIOUS_SESSION_FORMAT_VERSION
            | RECORDING_SESSION_FORMAT_VERSION
            | SESSION_FORMAT_VERSION
    ) {
        bail!(
            "unsupported session manifest format {}; expected {}, {}, {}, or {}",
            manifest.format,
            LEGACY_SESSION_FORMAT_VERSION,
            PREVIOUS_SESSION_FORMAT_VERSION,
            RECORDING_SESSION_FORMAT_VERSION,
            SESSION_FORMAT_VERSION
        );
    }

    validate_file_description(
        "packets",
        &manifest.files.packets,
        PACKET_JOURNAL_FILE_NAME,
        journal::FORMAT_VERSION,
    )?;
    validate_playout_description(&manifest.files.playout)?;
    validate_event_description(&manifest.files.events)
}

fn validate_event_description(description: &FileDescription) -> Result<()> {
    if description.path != EVENT_JOURNAL_FILE_NAME {
        bail!(
            "session manifest events path is {:?}; expected {:?}",
            description.path,
            EVENT_JOURNAL_FILE_NAME
        );
    }
    if !matches!(
        description.format,
        LEGACY_EVENT_FORMAT_VERSION | EVENT_FORMAT_VERSION
    ) {
        bail!(
            "session manifest events format is {}; expected {} or {}",
            description.format,
            LEGACY_EVENT_FORMAT_VERSION,
            EVENT_FORMAT_VERSION
        );
    }

    Ok(())
}

fn validate_playout_description(description: &FileDescription) -> Result<()> {
    if description.path != PLAYOUT_JOURNAL_FILE_NAME {
        bail!(
            "session manifest playout path is {:?}; expected {:?}",
            description.path,
            PLAYOUT_JOURNAL_FILE_NAME
        );
    }
    if !matches!(description.format, 1 | playout::FORMAT_VERSION) {
        bail!(
            "session manifest playout format is {}; expected 1 or {}",
            description.format,
            playout::FORMAT_VERSION
        );
    }

    Ok(())
}

fn validate_file_description(
    name: &str,
    description: &FileDescription,
    expected_path: &str,
    expected_format: u16,
) -> Result<()> {
    // Current filenames are fixed contracts even though readers obtain them
    // through the manifest rather than repeating literals at open sites.
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
    // A truncated final record is a reportable crash tail; corruption in the
    // complete prefix remains a hard error.
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
    expected_format_version: u16,
) -> Result<PlayoutInspection> {
    let file = File::open(path)
        .with_context(|| format!("failed to open playout journal {}", path.display()))?;
    let mut reader = BufReader::new(file);
    let format_version = playout::read_file_header(&mut reader)
        .with_context(|| format!("invalid playout journal header in {}", path.display()))?;
    if format_version != expected_format_version {
        bail!(
            "playout journal format {format_version} does not match manifest format \
             {expected_format_version}"
        );
    }

    let mut inspection = PlayoutInspection::default();
    loop {
        match playout::read_record(&mut reader, format_version)
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
                        ..
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

fn inspect_events(path: &Path, expected_format: u16) -> Result<EventInspection> {
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
                format,
                elapsed_nanos,
                ssrc,
                user_id,
                speaking_bits,
            }) => {
                validate_event_record_format(format, expected_format, line_index, path)?;
                inspection.records += 1;
                inspection.mappings.push(SpeakerMapping {
                    elapsed_nanos,
                    ssrc,
                    user_id,
                    speaking_bits,
                });
            }
            Ok(SessionEvent::UserIdentity { format, .. }) => {
                validate_event_record_format(format, expected_format, line_index, path)?;
                validate_format_two_event(format, line_index, path)?;
                inspection.records += 1;
                inspection.identity_updates += 1;
            }
            Ok(SessionEvent::UserDisconnected { format, .. }) => {
                validate_event_record_format(format, expected_format, line_index, path)?;
                validate_format_two_event(format, line_index, path)?;
                inspection.records += 1;
                inspection.user_disconnections += 1;
            }
            Ok(SessionEvent::UnresolvedSsrcAbandoned { format, .. }) => {
                validate_event_record_format(format, expected_format, line_index, path)?;
                validate_format_two_event(format, line_index, path)?;
                inspection.records += 1;
                inspection.unresolved_abandonments += 1;
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

fn validate_format_two_event(actual: u16, line_index: usize, path: &Path) -> Result<()> {
    if actual != EVENT_FORMAT_VERSION {
        bail!(
            "event type on line {} in {} requires format {}",
            line_index + 1,
            path.display(),
            EVENT_FORMAT_VERSION
        );
    }
    Ok(())
}

fn validate_event_record_format(
    actual: u16,
    expected: u16,
    line_index: usize,
    path: &Path,
) -> Result<()> {
    if actual != expected {
        bail!(
            "event format {actual} on line {} in {} does not match manifest format {expected}",
            line_index + 1,
            path.display()
        );
    }
    Ok(())
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

        // Half-range RTP arithmetic distinguishes forward wraparound from
        // late/out-of-order arrival.
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

        if self.recent_sequence_order.len() == RECENT_SEQUENCES
            && let Some(expired) = self.recent_sequence_order.pop_front()
        {
            self.recent_sequences.remove(&expired);
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

#[cfg(test)]
mod tests {
    use std::{
        env, process,
        time::{SystemTime, UNIX_EPOCH},
    };

    use super::*;

    #[test]
    fn format_two_session_manifest_remains_readable() {
        let directory = env::temp_dir().join(format!(
            "echoscribe-inspect-legacy-{}-{}",
            process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir(&directory).unwrap();
        fs::write(
            directory.join("session.json"),
            concat!(
                "{\n",
                "  \"format\": 2,\n",
                "  \"session_id\": \"session-1000\",\n",
                "  \"discord\": {\"guild_id\": \"123\", \"channel_id\": \"456\"},\n",
                "  \"files\": {\n",
                "    \"packets\": {\"path\": \"packets.dat\", \"format\": 1},\n",
                "    \"playout\": {\"path\": \"playout.dat\", \"format\": 2},\n",
                "    \"events\": {\"path\": \"events.ndjson\", \"format\": 1}\n",
                "  }\n",
                "}\n",
            ),
        )
        .unwrap();

        let manifest = read_manifest(&directory).unwrap();
        validate_manifest(&manifest).unwrap();
        assert_eq!(manifest.format, LEGACY_SESSION_FORMAT_VERSION);
        assert_eq!(manifest.state, None);

        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn unsupported_session_format_has_clear_error() {
        let directory = env::temp_dir().join(format!(
            "echoscribe-inspect-version-{}-{}",
            process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir(&directory).unwrap();
        fs::write(directory.join("session.json"), "{\"format\":99}\n").unwrap();

        let error = read_manifest(&directory)
            .err()
            .expect("unsupported session format should fail");
        assert!(
            error
                .to_string()
                .contains("unsupported session manifest format 99")
        );

        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn format_one_events_remain_inspectable() {
        let directory = env::temp_dir().join(format!(
            "echoscribe-inspect-events-{}-{}",
            process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir(&directory).unwrap();
        let path = directory.join(EVENT_JOURNAL_FILE_NAME);
        fs::write(
            &path,
            concat!(
                "{\"event\":\"speaker_mapping\",\"format\":1,\"elapsed_nanos\":123,",
                "\"ssrc\":4326,\"user_id\":\"881203221593464864\",\"speaking_bits\":1}\n"
            ),
        )
        .unwrap();

        let inspection = inspect_events(&path, LEGACY_EVENT_FORMAT_VERSION).unwrap();

        assert_eq!(inspection.records, 1);
        assert_eq!(inspection.mappings.len(), 1);
        assert_eq!(inspection.identity_updates, 0);
        assert_eq!(inspection.user_disconnections, 0);
        assert_eq!(inspection.unresolved_abandonments, 0);
        fs::remove_dir_all(directory).unwrap();
    }
}
