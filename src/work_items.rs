//! Post-recording playout ranges and retained transcription work items.
//!
//! Authoritative replay supplies activity and mapping evidence, while
//! `tracks.json` supplies the observed speaker name and aligned source file.
//! Participant naming, role, and transcription policy always come from the
//! immutable session-local TOML snapshot.

use std::{
    collections::{HashMap, HashSet},
    fs::{self, File, OpenOptions},
    io::{BufWriter, Write},
    path::Path,
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, anyhow, bail};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::{
    artifacts::{
        LEGACY_WORK_ITEM_MANIFEST_FORMAT_VERSION, TRANSCRIPTION_DIRECTORY_NAME,
        WORK_ITEM_MANIFEST_FILE_NAME, WORK_ITEM_MANIFEST_FORMAT_VERSION,
    },
    config::SegmentationConfig,
    diagnostics::{SAMPLE_RATE, SAMPLES_PER_TICK},
    operation_lease::SessionOperationLease,
    participants::{ParticipantContext, TranscriptNameSource},
    recover,
    routine_recovery::MappingTimeline,
    session::{SessionRecord, SessionStore, WorkflowState},
    stage::{StageError, StageResult},
    track_manifest::{TrackDescription, TrackManifest, TrackState},
};

const WORK_ITEM_TEMP_FILE_NAME: &str = ".work-items.jsonl.tmp";
const SAMPLES_PER_MILLISECOND: u64 = SAMPLE_RATE as u64 / 1_000;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct WorkItem {
    pub(crate) format: u16,
    pub(crate) id: String,
    pub(crate) session_id: String,
    pub(crate) sequence: u64,
    pub(crate) discord_user_id: String,
    pub(crate) discord_name: String,
    pub(crate) name: Option<String>,
    pub(crate) speaker: String,
    pub(crate) role: String,
    pub(crate) character: Option<String>,
    pub(crate) start_ms: u64,
    pub(crate) end_ms: u64,
    pub(crate) source: String,
    pub(crate) source_start_ms: u64,
    pub(crate) source_end_ms: u64,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct LegacyWorkItem {
    format: u16,
    id: String,
    session_id: String,
    sequence: u64,
    discord_user_id: String,
    speaker: String,
    role: String,
    character: Option<String>,
    start_ms: u64,
    end_ms: u64,
    source: String,
    source_start_ms: u64,
    source_end_ms: u64,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct CurrentWorkItem {
    format: u16,
    id: String,
    session_id: String,
    sequence: u64,
    discord_user_id: String,
    discord_name: String,
    name: Option<String>,
    speaker: String,
    role: String,
    start_ms: u64,
    end_ms: u64,
    source: String,
    source_start_ms: u64,
    source_end_ms: u64,
}

impl Serialize for WorkItem {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        if self.format == LEGACY_WORK_ITEM_MANIFEST_FORMAT_VERSION {
            LegacyWorkItem {
                format: self.format,
                id: self.id.clone(),
                session_id: self.session_id.clone(),
                sequence: self.sequence,
                discord_user_id: self.discord_user_id.clone(),
                speaker: self.speaker.clone(),
                role: self.role.clone(),
                character: self.character.clone(),
                start_ms: self.start_ms,
                end_ms: self.end_ms,
                source: self.source.clone(),
                source_start_ms: self.source_start_ms,
                source_end_ms: self.source_end_ms,
            }
            .serialize(serializer)
        } else {
            CurrentWorkItem {
                format: self.format,
                id: self.id.clone(),
                session_id: self.session_id.clone(),
                sequence: self.sequence,
                discord_user_id: self.discord_user_id.clone(),
                discord_name: self.discord_name.clone(),
                name: self.name.clone(),
                speaker: self.speaker.clone(),
                role: self.role.clone(),
                start_ms: self.start_ms,
                end_ms: self.end_ms,
                source: self.source.clone(),
                source_start_ms: self.source_start_ms,
                source_end_ms: self.source_end_ms,
            }
            .serialize(serializer)
        }
    }
}

impl<'de> Deserialize<'de> for WorkItem {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = serde_json::Value::deserialize(deserializer)?;
        let format = value
            .get("format")
            .and_then(serde_json::Value::as_u64)
            .ok_or_else(|| serde::de::Error::custom("work item format must be an integer"))?;
        if format == u64::from(LEGACY_WORK_ITEM_MANIFEST_FORMAT_VERSION) {
            let item: LegacyWorkItem =
                serde_json::from_value(value).map_err(serde::de::Error::custom)?;
            Ok(Self {
                format: item.format,
                id: item.id,
                session_id: item.session_id,
                sequence: item.sequence,
                discord_user_id: item.discord_user_id,
                discord_name: item.speaker.clone(),
                name: None,
                speaker: item.speaker,
                role: item.role,
                character: item.character,
                start_ms: item.start_ms,
                end_ms: item.end_ms,
                source: item.source,
                source_start_ms: item.source_start_ms,
                source_end_ms: item.source_end_ms,
            })
        } else {
            let item: CurrentWorkItem =
                serde_json::from_value(value).map_err(serde::de::Error::custom)?;
            Ok(Self {
                format: item.format,
                id: item.id,
                session_id: item.session_id,
                sequence: item.sequence,
                discord_user_id: item.discord_user_id,
                discord_name: item.discord_name,
                name: item.name,
                speaker: item.speaker,
                role: item.role,
                character: None,
                start_ms: item.start_ms,
                end_ms: item.end_ms,
                source: item.source,
                source_start_ms: item.source_start_ms,
                source_end_ms: item.source_end_ms,
            })
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
/// Candidate boundary shared with future VAD refinement implementations.
pub(crate) struct CandidateRange {
    pub(crate) discord_user_id: u64,
    pub(crate) start_sample: u64,
    pub(crate) end_sample: u64,
    pub(crate) source_start_sample: u64,
    pub(crate) source_end_sample: u64,
}

/// Composable boundary for later VAD adjustment, splitting, or rejection.
pub(crate) trait RangeRefiner {
    fn refine(&self, candidates: Vec<CandidateRange>) -> Result<Vec<CandidateRange>>;
}

struct NoopRefiner;

impl RangeRefiner for NoopRefiner {
    fn refine(&self, candidates: Vec<CandidateRange>) -> Result<Vec<CandidateRange>> {
        Ok(candidates)
    }
}

#[derive(Clone, Debug)]
struct AttributedFrame {
    discord_user_id: u64,
    tick: u64,
    samples: u64,
}

#[derive(Default)]
struct UserAlignment {
    next_tick: u64,
    output_samples: u64,
    ranges: Vec<CandidateRange>,
}

pub(crate) fn run(session_directory: &Path, config_path: &Path) -> Result<()> {
    let merge_gap_ms = SegmentationConfig::load_merge_gap_ms(config_path)?;
    let session_directory = canonical_session_directory(session_directory)?;
    let lease = SessionOperationLease::acquire(&session_directory)?;
    run_with_lease(&session_directory, merge_gap_ms, unix_millis_now()?, &lease)
        .map_err(StageError::into_anyhow)
}

#[cfg(test)]
fn run_with_refiner(
    session_directory: &Path,
    merge_gap_ms: u64,
    refiner: &dyn RangeRefiner,
    completed_at_unix_millis: u64,
) -> Result<()> {
    let session_directory = canonical_session_directory(session_directory)?;
    let lease = SessionOperationLease::acquire(&session_directory)?;
    run_with_refiner_under_lease(
        &session_directory,
        merge_gap_ms,
        refiner,
        completed_at_unix_millis,
        &lease,
    )
    .map_err(StageError::into_anyhow)
}

pub(crate) fn run_with_lease(
    session_directory: &Path,
    merge_gap_ms: u64,
    completed_at_unix_millis: u64,
    lease: &SessionOperationLease,
) -> StageResult<()> {
    run_with_refiner_under_lease(
        session_directory,
        merge_gap_ms,
        &NoopRefiner,
        completed_at_unix_millis,
        lease,
    )
}

fn canonical_session_directory(session_directory: &Path) -> Result<std::path::PathBuf> {
    fs::canonicalize(session_directory).with_context(|| {
        format!(
            "failed to resolve session directory {}",
            session_directory.display()
        )
    })
}

fn run_with_refiner_under_lease(
    session_directory: &Path,
    merge_gap_ms: u64,
    refiner: &dyn RangeRefiner,
    completed_at_unix_millis: u64,
    _lease: &SessionOperationLease,
) -> StageResult<()> {
    // Everything before publication is validation or deterministic planning.
    // Refusal here must not turn an operator mistake or damaged input into a
    // new workflow failure record.
    let (mut session, items) = (|| -> Result<_> {
        let session = SessionStore::load(session_directory).with_context(|| {
            format!(
                "failed to load workflow state from {}",
                session_directory.display()
            )
        })?;
        if session.record().state != WorkflowState::ReadyForTranscription {
            bail!(
                "build-work-items requires session state ready_for_transcription; found {}",
                session.record().state.as_str()
            );
        }

        let items =
            build_items_from_authority(session_directory, session.record(), merge_gap_ms, refiner)?;
        Ok((session, items))
    })()
    .map_err(StageError::refused)?;

    // At this point the coordinator has accepted the stage. File or authority
    // publication failures are durable workflow faults when invoked one-stop.
    (|| -> Result<()> {
        write_manifest_atomically(session_directory, &items)?;
        session
            .publish_work_manifest(completed_at_unix_millis)
            .context("work manifest was published but session.json could not record it")?;

        println!(
            "Published {} chronological work item(s) for session {}.",
            items.len(),
            session.record().session_id
        );
        Ok(())
    })()
    .map_err(StageError::accepted)
}

pub(crate) fn build_retranscription_items(
    session_directory: &Path,
    record: &SessionRecord,
    merge_gap_ms: u64,
) -> Result<Vec<WorkItem>> {
    build_items_from_authority(session_directory, record, merge_gap_ms, &NoopRefiner)
}

fn build_items_from_authority(
    session_directory: &Path,
    record: &SessionRecord,
    merge_gap_ms: u64,
    refiner: &dyn RangeRefiner,
) -> Result<Vec<WorkItem>> {
    let participants_path = session_directory.join(&record.files.participants.path);
    let participants = ParticipantContext::load(&participants_path).with_context(|| {
        format!(
            "failed to validate participant snapshot {}",
            participants_path.display()
        )
    })?;
    if participants.format_version() != record.files.participants.format {
        bail!("participant snapshot format does not match session.json");
    }

    let track_manifest_path = session_directory.join(&record.files.tracks.path);
    let tracks = TrackManifest::read(&track_manifest_path).with_context(|| {
        format!(
            "failed to read track manifest {}",
            track_manifest_path.display()
        )
    })?;
    if tracks.format != record.files.tracks.format {
        bail!("track manifest format does not match session.json");
    }
    validate_tracks(session_directory, record.session_id.as_str(), &tracks)?;

    let timeline = MappingTimeline::read(&session_directory.join(&record.files.events.path))?;
    if !timeline.unresolved_ssrcs().is_empty() {
        let mut unresolved = timeline
            .unresolved_ssrcs()
            .iter()
            .copied()
            .collect::<Vec<_>>();
        unresolved.sort_unstable();
        bail!(
            "cannot build work items while SSRC mapping evidence remains unresolved for {}",
            comma_join(unresolved)
        );
    }

    let mut attributed_frames = Vec::new();
    let mut unattributed_ssrcs = HashSet::new();
    let replay =
        recover::replay_activity_session_files(session_directory, &record.files, |activity| {
            match timeline.user_at(activity.ssrc, activity.elapsed_nanos) {
                Some(discord_user_id) => attributed_frames.push(AttributedFrame {
                    discord_user_id,
                    tick: activity.tick,
                    samples: u64::from(activity.decoded_samples),
                }),
                None => {
                    unattributed_ssrcs.insert(activity.ssrc);
                }
            }
            Ok(())
        })
        .context("authoritative packet/playout journal validation failed")?;
    if replay.truncated_packet_tail || replay.truncated_playout_tail {
        bail!("cannot build work items with a truncated authoritative journal tail");
    }
    if replay.skipped_undecoded > 0 {
        bail!(
            "cannot build work items: {} playout decisions lack decoded-sample evidence",
            replay.skipped_undecoded
        );
    }
    if !unattributed_ssrcs.is_empty() {
        let mut ssrcs = unattributed_ssrcs.into_iter().collect::<Vec<_>>();
        ssrcs.sort_unstable();
        bail!(
            "cannot build work items: decoded PCM cannot be attributed safely for SSRCs {}",
            comma_join(ssrcs)
        );
    }

    let candidates = build_candidate_ranges(attributed_frames, merge_gap_ms)?;
    validate_source_alignment(&candidates, &tracks)?;
    let refined = refiner.refine(candidates)?;
    materialise_work_items(record.session_id.as_str(), refined, &tracks, &participants)
}

#[cfg(test)]
fn run_with_refiner_before_lease(
    session_directory: &Path,
    merge_gap_ms: u64,
    refiner: &dyn RangeRefiner,
    completed_at_unix_millis: u64,
    before_lease: impl FnOnce(),
) -> Result<()> {
    let session_directory = canonical_session_directory(session_directory)?;
    before_lease();
    let lease = SessionOperationLease::acquire(&session_directory)?;
    run_with_refiner_under_lease(
        &session_directory,
        merge_gap_ms,
        refiner,
        completed_at_unix_millis,
        &lease,
    )
    .map_err(StageError::into_anyhow)
}

fn validate_tracks(
    session_directory: &Path,
    session_id: &str,
    manifest: &TrackManifest,
) -> Result<()> {
    if manifest.session_id != session_id {
        bail!(
            "track manifest session ID {:?} does not match session.json ID {:?}",
            manifest.session_id,
            session_id
        );
    }

    let mut incomplete_users = Vec::new();
    for track in &manifest.tracks {
        if track.state != TrackState::Complete {
            incomplete_users.push(track.discord_user_id.clone());
            continue;
        }
        let path = session_directory.join(&track.path);
        if !path
            .metadata()
            .with_context(|| format!("failed to inspect routine track {}", path.display()))?
            .is_file()
        {
            bail!("routine track {} is not a regular file", path.display());
        }
    }
    if !incomplete_users.is_empty() {
        bail!(
            "cannot build work items with incomplete tracks for Discord users {}",
            incomplete_users.join(", ")
        );
    }
    Ok(())
}

fn build_candidate_ranges(
    frames: Vec<AttributedFrame>,
    merge_gap_ms: u64,
) -> Result<Vec<CandidateRange>> {
    let merge_gap_samples = merge_gap_ms
        .checked_mul(SAMPLES_PER_MILLISECOND)
        .ok_or_else(|| anyhow!("segmentation.merge_gap_ms is too large"))?;
    let mut users = HashMap::<u64, UserAlignment>::new();

    // Replay order matches the live writer input order. Simulating its silence
    // insertion keeps source offsets exact even after nonstandard frame sizes.
    for frame in frames {
        let alignment = users.entry(frame.discord_user_id).or_default();
        if frame.tick < alignment.next_tick {
            bail!(
                "playout tick {} for Discord user {} follows tick {}",
                frame.tick,
                frame.discord_user_id,
                alignment.next_tick - 1
            );
        }

        let missing_ticks = frame.tick - alignment.next_tick;
        let inserted_silence = missing_ticks
            .checked_mul(SAMPLES_PER_TICK)
            .ok_or_else(|| anyhow!("aligned source offset overflow"))?;
        let source_start_sample = alignment
            .output_samples
            .checked_add(inserted_silence)
            .ok_or_else(|| anyhow!("aligned source offset overflow"))?;
        let source_end_sample = source_start_sample
            .checked_add(frame.samples)
            .ok_or_else(|| anyhow!("aligned source range overflow"))?;
        let start_sample = frame
            .tick
            .checked_mul(SAMPLES_PER_TICK)
            .ok_or_else(|| anyhow!("session range offset overflow"))?;
        let end_sample = start_sample
            .checked_add(frame.samples)
            .ok_or_else(|| anyhow!("session range overflow"))?;

        alignment.ranges.push(CandidateRange {
            discord_user_id: frame.discord_user_id,
            start_sample,
            end_sample,
            source_start_sample,
            source_end_sample,
        });
        alignment.next_tick = frame
            .tick
            .checked_add(1)
            .ok_or_else(|| anyhow!("playout tick overflow"))?;
        alignment.output_samples = source_end_sample;
    }

    let mut merged = Vec::new();
    for (_, mut alignment) in users {
        let mut current: Option<CandidateRange> = None;
        for range in alignment.ranges.drain(..) {
            match current.as_mut() {
                Some(existing)
                    if range.start_sample
                        <= existing
                            .end_sample
                            .checked_add(merge_gap_samples)
                            .unwrap_or(u64::MAX) =>
                {
                    existing.end_sample = existing.end_sample.max(range.end_sample);
                    existing.source_end_sample =
                        existing.source_end_sample.max(range.source_end_sample);
                }
                Some(_) => {
                    merged.push(current.replace(range).expect("current range exists"));
                }
                None => current = Some(range),
            }
        }
        if let Some(range) = current {
            merged.push(range);
        }
    }
    Ok(merged)
}

fn validate_source_alignment(ranges: &[CandidateRange], manifest: &TrackManifest) -> Result<()> {
    let source_lengths = ranges.iter().fold(HashMap::new(), |mut lengths, range| {
        lengths
            .entry(range.discord_user_id)
            .and_modify(|length: &mut u64| *length = (*length).max(range.source_end_sample))
            .or_insert(range.source_end_sample);
        lengths
    });
    let tracks = manifest
        .tracks
        .iter()
        .map(|track| {
            (
                track
                    .discord_user_id
                    .parse::<u64>()
                    .expect("validated tracks contain numeric Discord IDs"),
                track,
            )
        })
        .collect::<HashMap<_, _>>();

    for (user_id, source_length) in &source_lengths {
        let track = tracks.get(user_id).ok_or_else(|| {
            anyhow!("playout activity for Discord user {user_id} has no routine track")
        })?;
        if *source_length != track.length_samples {
            bail!(
                "playout activity reconstructs {} aligned samples for Discord user {}, \
                 but tracks.json records {}",
                source_length,
                user_id,
                track.length_samples
            );
        }
    }
    let mut tracks_without_activity = tracks
        .keys()
        .filter(|user_id| !source_lengths.contains_key(user_id))
        .copied()
        .collect::<Vec<_>>();
    tracks_without_activity.sort_unstable();
    if !tracks_without_activity.is_empty() {
        bail!(
            "complete routine tracks have no attributable playout activity for Discord users {}",
            comma_join(tracks_without_activity)
        );
    }
    Ok(())
}

fn materialise_work_items(
    session_id: &str,
    mut ranges: Vec<CandidateRange>,
    manifest: &TrackManifest,
    participants: &ParticipantContext,
) -> Result<Vec<WorkItem>> {
    ranges.sort_unstable_by_key(|range| {
        (
            range.start_sample,
            range.discord_user_id,
            range.end_sample,
            range.source_start_sample,
        )
    });
    let tracks = manifest
        .tracks
        .iter()
        .map(|track| {
            (
                track
                    .discord_user_id
                    .parse::<u64>()
                    .expect("validated tracks contain numeric Discord IDs"),
                track,
            )
        })
        .collect::<HashMap<_, _>>();

    ranges
        .into_iter()
        .filter(|range| {
            participants
                .get(range.discord_user_id)
                .is_none_or(|participant| participant.transcribe)
        })
        .enumerate()
        .map(|(index, range)| {
            let track = tracks.get(&range.discord_user_id).ok_or_else(|| {
                anyhow!(
                    "playout activity for Discord user {} has no routine track",
                    range.discord_user_id
                )
            })?;
            validate_range(&range, track)?;

            let sequence = u64::try_from(index)
                .ok()
                .and_then(|value| value.checked_add(1))
                .ok_or_else(|| anyhow!("work item sequence overflow"))?;
            let participant = participants.get(range.discord_user_id);
            let role = participant
                .map(|value| value.role.clone())
                .unwrap_or_else(|| participants.default_role().to_owned());
            let name = participant.and_then(|value| value.name.clone());
            let character = participant.and_then(|value| value.character.clone());
            let discord_name = track.display_name.clone();
            let speaker = match participants.transcript_name_source() {
                TranscriptNameSource::Name => name.clone().unwrap_or_else(|| discord_name.clone()),
                TranscriptNameSource::Discord => discord_name.clone(),
            };
            let item = WorkItem {
                format: participants.format_version(),
                id: format!("{session_id}:{sequence:06}"),
                session_id: session_id.to_owned(),
                sequence,
                discord_user_id: range.discord_user_id.to_string(),
                discord_name,
                name,
                speaker,
                role,
                character,
                start_ms: samples_to_millis_floor(range.start_sample),
                end_ms: samples_to_millis_ceil(range.end_sample)?,
                source: track.path.clone(),
                source_start_ms: samples_to_millis_floor(range.source_start_sample),
                source_end_ms: samples_to_millis_ceil(range.source_end_sample)?,
            };
            validate_work_item(&item)?;
            Ok(item)
        })
        .collect()
}

fn validate_range(range: &CandidateRange, track: &TrackDescription) -> Result<()> {
    if range.start_sample >= range.end_sample
        || range.source_start_sample >= range.source_end_sample
    {
        bail!(
            "range refiner produced an empty or reversed range for Discord user {}",
            range.discord_user_id
        );
    }
    if range.source_end_sample > track.length_samples {
        bail!(
            "source range for Discord user {} ends at sample {}, beyond track length {}",
            range.discord_user_id,
            range.source_end_sample,
            track.length_samples
        );
    }
    Ok(())
}

fn validate_work_item(item: &WorkItem) -> Result<()> {
    let metadata_valid = match item.format {
        LEGACY_WORK_ITEM_MANIFEST_FORMAT_VERSION => {
            matches!(item.role.as_str(), "player" | "gm")
                && item.discord_name == item.speaker
                && item.name.is_none()
        }
        WORK_ITEM_MANIFEST_FORMAT_VERSION => {
            !item.discord_name.trim().is_empty()
                && !item.discord_name.contains(['\n', '\r'])
                && item
                    .name
                    .as_ref()
                    .is_none_or(|name| !name.trim().is_empty() && !name.contains(['\n', '\r']))
                && !item.role.trim().is_empty()
                && !item.role.contains(['\n', '\r'])
                && item.character.is_none()
                && (item.speaker == item.discord_name
                    || item.name.as_deref() == Some(item.speaker.as_str()))
        }
        _ => false,
    };
    if !metadata_valid
        || item.sequence == 0
        || item.id.trim().is_empty()
        || item.session_id.trim().is_empty()
        || item
            .discord_user_id
            .parse::<u64>()
            .ok()
            .filter(|id| *id != 0)
            .is_none()
        || item.speaker.trim().is_empty()
        || item.speaker.contains(['\n', '\r'])
        || item.start_ms >= item.end_ms
        || item.source_start_ms >= item.source_end_ms
    {
        bail!("invalid transcription work item {}", item.id);
    }
    Ok(())
}

fn write_manifest_atomically(session_directory: &Path, items: &[WorkItem]) -> Result<()> {
    let directory = session_directory.join(TRANSCRIPTION_DIRECTORY_NAME);
    fs::create_dir_all(&directory).with_context(|| {
        format!(
            "failed to create transcription directory {}",
            directory.display()
        )
    })?;
    File::open(session_directory)?
        .sync_all()
        .context("failed to synchronise session directory")?;

    write_manifest_file_atomically(
        &directory.join(WORK_ITEM_TEMP_FILE_NAME),
        &directory.join(WORK_ITEM_MANIFEST_FILE_NAME),
        items,
    )
}

pub(crate) fn write_retranscription_manifest(path: &Path, items: &[WorkItem]) -> Result<()> {
    let directory = path
        .parent()
        .ok_or_else(|| anyhow!("retranscription work manifest path has no parent"))?;
    fs::create_dir_all(directory).with_context(|| {
        format!(
            "failed to create retranscription staging directory {}",
            directory.display()
        )
    })?;
    let temporary_path = directory.join(WORK_ITEM_TEMP_FILE_NAME);
    write_manifest_file_atomically(&temporary_path, path, items)
}

fn write_manifest_file_atomically(
    temporary_path: &Path,
    final_path: &Path,
    items: &[WorkItem],
) -> Result<()> {
    let directory = final_path
        .parent()
        .ok_or_else(|| anyhow!("work manifest path has no parent"))?;
    let file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(temporary_path)
        .with_context(|| format!("failed to create {}", temporary_path.display()))?;
    let mut writer = BufWriter::new(file);
    for item in items {
        serde_json::to_writer(&mut writer, item)?;
        writer.write_all(b"\n")?;
    }
    writer.flush()?;
    writer.get_ref().sync_all()?;
    drop(writer);

    fs::rename(temporary_path, final_path)
        .with_context(|| format!("failed to publish work manifest {}", final_path.display()))?;
    File::open(&directory)?
        .sync_all()
        .context("failed to synchronise transcription directory")?;
    Ok(())
}

fn samples_to_millis_floor(samples: u64) -> u64 {
    samples / SAMPLES_PER_MILLISECOND
}

fn samples_to_millis_ceil(samples: u64) -> Result<u64> {
    samples
        .checked_add(SAMPLES_PER_MILLISECOND - 1)
        .map(|value| value / SAMPLES_PER_MILLISECOND)
        .ok_or_else(|| anyhow!("millisecond range conversion overflow"))
}

fn comma_join(values: impl IntoIterator<Item = impl ToString>) -> String {
    values
        .into_iter()
        .map(|value| value.to_string())
        .collect::<Vec<_>>()
        .join(", ")
}

fn unix_millis_now() -> Result<u64> {
    Ok(u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .context("system clock precedes Unix epoch")?
            .as_millis(),
    )
    .map_err(|_| anyhow!("current Unix timestamp does not fit in u64"))?)
}

#[cfg(test)]
mod tests {
    use std::{
        env,
        fs::File,
        process,
        sync::mpsc,
        thread,
        time::Duration,
        time::{SystemTime, UNIX_EPOCH},
    };

    use super::*;
    use crate::{
        artifacts::{
            EVENT_JOURNAL_FILE_NAME, FINAL_TRANSCRIPT_PATH, PACKET_JOURNAL_FILE_NAME,
            PLAYOUT_JOURNAL_FILE_NAME, TRACK_DIRECTORY_NAME, TRANSCRIPTION_RESULTS_PATH,
            WORK_ITEM_MANIFEST_PATH,
        },
        journal,
        playout::{self, PlayoutDecision, PlayoutRecord},
        session::{
            NewSession, PREVIOUS_SESSION_FORMAT_VERSION, SessionEvent, SessionStore,
            fail_record_write_after, write_event,
        },
        track_manifest::{TrackDescription, TrackManifest},
    };

    fn frame(user: u64, tick: u64) -> AttributedFrame {
        AttributedFrame {
            discord_user_id: user,
            tick,
            samples: SAMPLES_PER_TICK,
        }
    }

    #[test]
    fn nearby_same_user_runs_merge_and_long_gaps_do_not() {
        let ranges =
            build_candidate_ranges(vec![frame(11, 10), frame(11, 20), frame(11, 80)], 750).unwrap();

        assert_eq!(ranges.len(), 2);
        assert_eq!(ranges[0].discord_user_id, 11);
        assert_eq!(ranges[0].start_sample, 10 * SAMPLES_PER_TICK);
        assert_eq!(ranges[0].end_sample, 21 * SAMPLES_PER_TICK);
        assert_eq!(ranges[1].start_sample, 80 * SAMPLES_PER_TICK);
    }

    #[test]
    fn one_frame_of_short_speech_is_retained() {
        let ranges = build_candidate_ranges(vec![frame(11, 10)], 750).unwrap();

        assert_eq!(ranges.len(), 1);
        assert_eq!(
            ranges[0].end_sample - ranges[0].start_sample,
            SAMPLES_PER_TICK
        );
    }

    #[test]
    fn ssrc_independent_frames_for_one_user_share_one_range() {
        // SSRC attribution has already resolved before this boundary; the
        // stable user ID is the only grouping key.
        let ranges = build_candidate_ranges(vec![frame(11, 10), frame(11, 11)], 750).unwrap();

        assert_eq!(ranges.len(), 1);
        assert_eq!(ranges[0].discord_user_id, 11);
        assert_eq!(ranges[0].end_sample, 12 * SAMPLES_PER_TICK);
    }

    #[test]
    fn nonstandard_frames_preserve_exact_source_offsets() {
        let ranges = build_candidate_ranges(
            vec![
                AttributedFrame {
                    discord_user_id: 11,
                    tick: 10,
                    samples: 480,
                },
                frame(11, 11),
            ],
            750,
        )
        .unwrap();

        assert_eq!(ranges.len(), 1);
        assert_eq!(ranges[0].source_start_sample, 10 * SAMPLES_PER_TICK);
        assert_eq!(
            ranges[0].source_end_sample,
            10 * SAMPLES_PER_TICK + 480 + SAMPLES_PER_TICK
        );
        assert_eq!(ranges[0].end_sample, 12 * SAMPLES_PER_TICK);
    }

    #[test]
    fn reconstructed_source_length_must_match_aligned_track() {
        let ranges = build_candidate_ranges(vec![frame(11, 10)], 750).unwrap();
        let manifest = TrackManifest::new(
            "session-source-length".to_owned(),
            vec![TrackDescription::new(
                11,
                "Alice".to_owned(),
                "player".to_owned(),
                None,
                "tracks/user-11.flac".to_owned(),
                TrackState::Complete,
                12 * SAMPLES_PER_TICK,
                vec![100],
                None,
            )],
        );

        let error = validate_source_alignment(&ranges, &manifest).unwrap_err();

        assert!(error.to_string().contains("tracks.json records"));
    }

    struct SplitRefiner;

    impl RangeRefiner for SplitRefiner {
        fn refine(&self, candidates: Vec<CandidateRange>) -> Result<Vec<CandidateRange>> {
            let range = &candidates[0];
            let session_midpoint = range.start_sample + SAMPLES_PER_TICK;
            let source_midpoint = range.source_start_sample + SAMPLES_PER_TICK;
            Ok(vec![
                CandidateRange {
                    discord_user_id: range.discord_user_id,
                    start_sample: range.start_sample,
                    end_sample: session_midpoint,
                    source_start_sample: range.source_start_sample,
                    source_end_sample: source_midpoint,
                },
                CandidateRange {
                    discord_user_id: range.discord_user_id,
                    start_sample: session_midpoint,
                    end_sample: range.end_sample,
                    source_start_sample: source_midpoint,
                    source_end_sample: range.source_end_sample,
                },
            ])
        }
    }

    #[test]
    fn refinement_interface_can_be_substituted_without_changing_builder() {
        let candidates = build_candidate_ranges(vec![frame(11, 10), frame(11, 11)], 750).unwrap();
        let refined = SplitRefiner.refine(candidates).unwrap();

        assert_eq!(refined.len(), 2);
        assert_eq!(refined[0].end_sample, refined[1].start_sample);
    }

    #[test]
    fn excluded_participants_are_removed_before_global_sequencing() {
        let participants = ParticipantContext::from_toml(
            concat!(
                "version = 1\n",
                "[participants.\"11\"]\n",
                "character = \"Included\"\n",
                "role = \"gm\"\n",
                "[participants.\"22\"]\n",
                "transcribe = false\n",
            ),
            Path::new("session/participants.toml"),
        )
        .unwrap();
        let tracks = TrackManifest::new(
            "session-exclusion".to_owned(),
            vec![
                TrackDescription::new(
                    11,
                    "Alice".to_owned(),
                    "player".to_owned(),
                    None,
                    "tracks/user-11.flac".to_owned(),
                    TrackState::Complete,
                    5 * SAMPLES_PER_TICK,
                    vec![100],
                    None,
                ),
                TrackDescription::new(
                    22,
                    "Astra".to_owned(),
                    "player".to_owned(),
                    None,
                    "tracks/user-22.flac".to_owned(),
                    TrackState::Complete,
                    5 * SAMPLES_PER_TICK,
                    vec![200],
                    None,
                ),
            ],
        );
        let ranges = vec![
            CandidateRange {
                discord_user_id: 11,
                start_sample: SAMPLES_PER_TICK,
                end_sample: 2 * SAMPLES_PER_TICK,
                source_start_sample: SAMPLES_PER_TICK,
                source_end_sample: 2 * SAMPLES_PER_TICK,
            },
            CandidateRange {
                discord_user_id: 22,
                start_sample: 2 * SAMPLES_PER_TICK,
                end_sample: 3 * SAMPLES_PER_TICK,
                source_start_sample: 2 * SAMPLES_PER_TICK,
                source_end_sample: 3 * SAMPLES_PER_TICK,
            },
            CandidateRange {
                discord_user_id: 11,
                start_sample: 4 * SAMPLES_PER_TICK,
                end_sample: 5 * SAMPLES_PER_TICK,
                source_start_sample: 4 * SAMPLES_PER_TICK,
                source_end_sample: 5 * SAMPLES_PER_TICK,
            },
        ];

        let items =
            materialise_work_items("session-exclusion", ranges, &tracks, &participants).unwrap();

        assert_eq!(items.len(), 2);
        assert_eq!(items[0].sequence, 1);
        assert_eq!(items[1].sequence, 2);
        assert_eq!(items[0].id, "session-exclusion:000001");
        assert_eq!(items[1].id, "session-exclusion:000002");
        assert!(items.iter().all(|item| item.discord_user_id == "11"));
        assert_eq!(items[0].speaker, "Alice");
        assert_eq!(items[0].role, "gm");
        assert_eq!(items[0].character.as_deref(), Some("Included"));
        assert_eq!(items[0].source, "tracks/user-11.flac");
    }

    #[test]
    fn current_participant_metadata_controls_attribution_and_manifest_schema() {
        let participants = ParticipantContext::from_toml(
            concat!(
                "version = 2\n",
                "transcript_name_source = \"name\"\n",
                "[participants.\"11\"]\n",
                "name = \"Stefan\"\n",
                "role = \"speaker\"\n",
            ),
            Path::new("session/participants.toml"),
        )
        .unwrap();
        let tracks = TrackManifest::new_with_format(
            WORK_ITEM_MANIFEST_FORMAT_VERSION,
            "session-generic".to_owned(),
            vec![TrackDescription::new(
                11,
                "Tromador".to_owned(),
                "speaker".to_owned(),
                None,
                "tracks/user-11.flac".to_owned(),
                TrackState::Complete,
                2 * SAMPLES_PER_TICK,
                vec![100],
                None,
            )],
        );
        let ranges = vec![CandidateRange {
            discord_user_id: 11,
            start_sample: SAMPLES_PER_TICK,
            end_sample: 2 * SAMPLES_PER_TICK,
            source_start_sample: SAMPLES_PER_TICK,
            source_end_sample: 2 * SAMPLES_PER_TICK,
        }];

        let items =
            materialise_work_items("session-generic", ranges, &tracks, &participants).unwrap();
        let item = &items[0];
        assert_eq!(item.format, WORK_ITEM_MANIFEST_FORMAT_VERSION);
        assert_eq!(item.discord_name, "Tromador");
        assert_eq!(item.name.as_deref(), Some("Stefan"));
        assert_eq!(item.speaker, "Stefan");
        assert_eq!(item.role, "speaker");
        assert_eq!(item.character, None);

        let value = serde_json::to_value(item).unwrap();
        assert_eq!(value["discord_name"], "Tromador");
        assert_eq!(value["name"], "Stefan");
        assert!(value.get("character").is_none());

        let mut inconsistent = item.clone();
        inconsistent.speaker = "Entirely Different Person".to_owned();
        assert!(validate_work_item(&inconsistent).is_err());
    }

    #[test]
    fn complete_session_produces_stable_globally_ordered_manifest() {
        let directory = ready_session_fixture();

        run_with_refiner(&directory, 750, &NoopRefiner, 4_000).unwrap();
        let first_bytes = fs::read(directory.join(WORK_ITEM_MANIFEST_PATH)).unwrap();
        let first_items = read_items(&first_bytes);

        assert_eq!(first_items.len(), 3);
        assert_eq!(
            first_items
                .iter()
                .map(|item| item.discord_user_id.as_str())
                .collect::<Vec<_>>(),
            ["11", "22", "11"]
        );
        assert_eq!(
            first_items
                .iter()
                .map(|item| item.id.as_str())
                .collect::<Vec<_>>(),
            [
                "session-work-items:000001",
                "session-work-items:000002",
                "session-work-items:000003",
            ]
        );
        assert_eq!((first_items[0].start_ms, first_items[0].end_ms), (200, 420));
        assert_eq!(
            (first_items[0].source_start_ms, first_items[0].source_end_ms),
            (200, 420)
        );
        assert_eq!((first_items[1].start_ms, first_items[1].end_ms), (200, 220));
        assert_eq!(
            (first_items[2].start_ms, first_items[2].end_ms),
            (2_000, 2_020)
        );
        assert_eq!(first_items[0].speaker, "Alice");
        assert_eq!(first_items[0].role, "gm");
        assert_eq!(
            first_items[0].character.as_deref(),
            Some("Emperor Coaltongue")
        );
        assert_eq!(first_items[1].role, "player");
        assert_eq!(first_items[1].character, None);

        let after_first = SessionStore::load(&directory).unwrap();
        assert_eq!(
            after_first.record().format,
            crate::session::RECORDING_SESSION_FORMAT_VERSION
        );
        assert_eq!(
            after_first.record().files.work_items.as_ref().unwrap().path,
            WORK_ITEM_MANIFEST_PATH
        );
        assert_eq!(
            after_first.record().state,
            WorkflowState::ReadyForTranscription
        );

        run_with_refiner(&directory, 750, &NoopRefiner, 5_000).unwrap();
        let second_bytes = fs::read(directory.join(WORK_ITEM_MANIFEST_PATH)).unwrap();
        let after_second = SessionStore::load(&directory).unwrap();

        assert_eq!(second_bytes, first_bytes);
        assert_eq!(
            after_second
                .record()
                .checkpoints
                .iter()
                .filter(|checkpoint| checkpoint.stage == "work_manifest_built")
                .count(),
            2
        );
        assert!(
            !directory
                .join(TRANSCRIPTION_DIRECTORY_NAME)
                .join(WORK_ITEM_TEMP_FILE_NAME)
                .exists()
        );

        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn command_boundary_refuses_a_session_not_ready_for_transcription() {
        let directory = test_directory("wrong-state");
        let participants = ParticipantContext::empty_for_test();
        SessionStore::create(
            &directory,
            NewSession {
                session_id: "session-wrong-state",
                started_at_unix_millis: 1_000,
                configuration_version: 1,
                guild_id: "123",
                channel_id: "456",
                participants: &participants,
            },
        )
        .unwrap();

        let error = run_with_refiner(&directory, 750, &NoopRefiner, 2_000).unwrap_err();

        assert!(error.to_string().contains("ready_for_transcription"));
        assert!(!directory.join(WORK_ITEM_MANIFEST_PATH).exists());
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn session_publish_failure_does_not_claim_a_work_manifest_checkpoint() {
        let directory = ready_session_fixture();
        fail_record_write_after(&directory, 0);

        let error = run_with_refiner(&directory, 750, &NoopRefiner, 4_000).unwrap_err();

        assert!(
            error
                .to_string()
                .contains("session.json could not record it")
        );
        assert!(directory.join(WORK_ITEM_MANIFEST_PATH).is_file());
        let session = SessionStore::load(&directory).unwrap();
        assert_eq!(session.record().format, PREVIOUS_SESSION_FORMAT_VERSION);
        assert_eq!(session.record().files.work_items, None);
        assert!(
            session
                .record()
                .checkpoints
                .iter()
                .all(|checkpoint| checkpoint.stage != "work_manifest_built")
        );
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn delayed_builder_reloads_completed_authority_under_operation_lease() {
        let directory = ready_session_fixture();
        run_with_refiner(&directory, 750, &NoopRefiner, 4_000).unwrap();

        let (observed_sender, observed_receiver) = mpsc::channel();
        let (resume_sender, resume_receiver) = mpsc::channel();
        let delayed_directory = directory.clone();
        let delayed = thread::spawn(move || {
            run_with_refiner_before_lease(&delayed_directory, 750, &NoopRefiner, 7_000, || {
                let stale = SessionStore::load(&delayed_directory).unwrap();
                assert_eq!(stale.record().state, WorkflowState::ReadyForTranscription);
                observed_sender.send(()).unwrap();
                resume_receiver.recv().unwrap();
            })
        });
        observed_receiver
            .recv_timeout(Duration::from_secs(5))
            .expect("delayed builder did not observe old authority");

        let lease = SessionOperationLease::acquire(&directory).unwrap();
        let results_path = directory.join(TRANSCRIPTION_RESULTS_PATH);
        let results = File::create(&results_path).unwrap();
        results.sync_all().unwrap();
        let mut session = SessionStore::load(&directory).unwrap();
        session.publish_transcription_start(5_000).unwrap();
        fs::write(directory.join(FINAL_TRANSCRIPT_PATH), b"finished\n").unwrap();
        session.publish_transcription_complete(6_000).unwrap();
        let session_before = fs::read(directory.join("session.json")).unwrap();
        let work_items_before = fs::read(directory.join(WORK_ITEM_MANIFEST_PATH)).unwrap();
        let results_before = fs::read(&results_path).unwrap();
        let final_before = fs::read(directory.join(FINAL_TRANSCRIPT_PATH)).unwrap();
        drop(lease);

        resume_sender.send(()).unwrap();
        let error = delayed.join().unwrap().unwrap_err();

        assert!(error.to_string().contains("found complete"));
        assert_eq!(
            fs::read(directory.join("session.json")).unwrap(),
            session_before
        );
        assert_eq!(
            fs::read(directory.join(WORK_ITEM_MANIFEST_PATH)).unwrap(),
            work_items_before
        );
        assert_eq!(fs::read(results_path).unwrap(), results_before);
        assert_eq!(
            fs::read(directory.join(FINAL_TRANSCRIPT_PATH)).unwrap(),
            final_before
        );
        fs::remove_dir_all(directory).unwrap();
    }

    fn ready_session_fixture() -> std::path::PathBuf {
        let directory = test_directory("ready");
        let source_participants = directory.join("configured-participants.toml");
        fs::write(
            &source_participants,
            concat!(
                "version = 1\n",
                "[participants.\"11\"]\n",
                "character = \"Emperor Coaltongue\"\n",
                "role = \"GM\"\n",
            ),
        )
        .unwrap();
        let participants = ParticipantContext::load(&source_participants).unwrap();
        let mut session = SessionStore::create(
            &directory,
            NewSession {
                session_id: "session-work-items",
                started_at_unix_millis: 1_000,
                configuration_version: 1,
                guild_id: "123",
                channel_id: "456",
                participants: &participants,
            },
        )
        .unwrap();
        session
            .transition(WorkflowState::RecordedClean, 2_000)
            .unwrap();
        session
            .transition(WorkflowState::ReadyForTranscription, 2_100)
            .unwrap();

        let mut packets = File::create(directory.join(PACKET_JOURNAL_FILE_NAME)).unwrap();
        journal::write_file_header(&mut packets).unwrap();
        packets.sync_all().unwrap();

        let mut playout_file = File::create(directory.join(PLAYOUT_JOURNAL_FILE_NAME)).unwrap();
        playout::write_file_header(&mut playout_file).unwrap();
        for (tick, ssrc) in [(10, 100), (10, 200), (11, 100), (20, 101), (100, 101)] {
            playout::write_record(
                &mut playout_file,
                &PlayoutRecord {
                    tick,
                    ssrc,
                    decision: PlayoutDecision::Loss,
                    decoded_samples: u32::try_from(SAMPLES_PER_TICK).unwrap(),
                },
            )
            .unwrap();
        }
        playout_file.sync_all().unwrap();

        let mut events = File::create(directory.join(EVENT_JOURNAL_FILE_NAME)).unwrap();
        for (ssrc, user_id) in [(100, "11"), (101, "11"), (200, "22")] {
            write_event(
                &mut events,
                &SessionEvent::speaker_mapping(0, ssrc, Some(user_id.to_owned()), 1),
            )
            .unwrap();
        }
        events.sync_all().unwrap();

        let track_directory = directory.join(TRACK_DIRECTORY_NAME);
        fs::create_dir(&track_directory).unwrap();
        fs::write(track_directory.join("user-11.flac"), b"complete").unwrap();
        fs::write(track_directory.join("user-22.flac"), b"complete").unwrap();
        TrackManifest::new(
            "session-work-items".to_owned(),
            vec![
                TrackDescription::new(
                    11,
                    "Alice".to_owned(),
                    "player".to_owned(),
                    None,
                    "tracks/user-11.flac".to_owned(),
                    TrackState::Complete,
                    101 * SAMPLES_PER_TICK,
                    vec![100, 101],
                    None,
                ),
                TrackDescription::new(
                    22,
                    "Bob".to_owned(),
                    "gm".to_owned(),
                    Some("Mutable manifest context".to_owned()),
                    "tracks/user-22.flac".to_owned(),
                    TrackState::Complete,
                    11 * SAMPLES_PER_TICK,
                    vec![200],
                    None,
                ),
            ],
        )
        .write(&directory)
        .unwrap();

        // Exercise the approved compatibility path rather than only format 4.
        let mut record = session.record().clone();
        record.format = PREVIOUS_SESSION_FORMAT_VERSION;
        fs::write(
            directory.join("session.json"),
            serde_json::to_vec_pretty(&record).unwrap(),
        )
        .unwrap();
        directory
    }

    fn read_items(bytes: &[u8]) -> Vec<WorkItem> {
        bytes
            .split(|byte| *byte == b'\n')
            .filter(|line| !line.is_empty())
            .map(|line| serde_json::from_slice(line).unwrap())
            .collect()
    }

    fn test_directory(label: &str) -> std::path::PathBuf {
        let directory = env::temp_dir().join(format!(
            "echoscribe-work-items-{label}-{}-{}",
            process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir(&directory).unwrap();
        directory
    }
}
