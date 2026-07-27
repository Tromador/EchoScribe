use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
};

use serenity::async_trait;
use songbird::{Call, CoreEvent, Event, EventContext, EventHandler, packet::Packet as _};

use crate::capture::{CaptureSender, CapturedPacket};

const RECENT_SEQUENCES: usize = 4096;

pub(crate) struct VoiceTelemetry {
    capture: CaptureSender,
    handlers_registered: AtomicBool,
    speaking_updates: AtomicU64,
    rtp_packets: AtomicU64,
    voice_ticks: AtomicU64,
    playout_packets: AtomicU64,
    playout_losses: AtomicU64,
    decoded_frames: AtomicU64,
    streams: Mutex<HashMap<u32, StreamContinuity>>,
}

impl VoiceTelemetry {
    pub(crate) fn new(capture: CaptureSender) -> Self {
        Self {
            capture,
            handlers_registered: AtomicBool::new(false),
            speaking_updates: AtomicU64::new(0),
            rtp_packets: AtomicU64::new(0),
            voice_ticks: AtomicU64::new(0),
            playout_packets: AtomicU64::new(0),
            playout_losses: AtomicU64::new(0),
            decoded_frames: AtomicU64::new(0),
            streams: Mutex::new(HashMap::new()),
        }
    }

    pub(crate) fn register(self: &Arc<Self>, call: &mut Call) {
        if self.handlers_registered.swap(true, Ordering::AcqRel) {
            return;
        }

        for event in [
            CoreEvent::SpeakingStateUpdate,
            CoreEvent::RtpPacket,
            CoreEvent::VoiceTick,
        ] {
            call.add_global_event(
                Event::Core(event),
                TelemetryHandler {
                    telemetry: Arc::clone(self),
                },
            );
        }
    }

    pub(crate) fn report(&self) {
        println!(
            "Voice telemetry: {} speaking updates, {} RTP packets, {} voice ticks, \
             {} playout packets, {} playout losses, {} decoded frames.",
            self.speaking_updates.load(Ordering::Relaxed),
            self.rtp_packets.load(Ordering::Relaxed),
            self.voice_ticks.load(Ordering::Relaxed),
            self.playout_packets.load(Ordering::Relaxed),
            self.playout_losses.load(Ordering::Relaxed),
            self.decoded_frames.load(Ordering::Relaxed),
        );

        let streams = self
            .streams
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let mut streams = streams.iter().collect::<Vec<_>>();
        streams.sort_unstable_by_key(|(ssrc, _)| **ssrc);

        for (ssrc, stream) in streams {
            println!(
                "RTP SSRC {ssrc}: {} packets, sequences {}..{}, {} forward gap events \
                 ({} missing slots observed), {} duplicates, {} late/out-of-order arrivals.",
                stream.packets,
                stream.first_sequence,
                stream.latest_sequence,
                stream.forward_gap_events,
                stream.missing_slots_observed,
                stream.duplicates,
                stream.out_of_order,
            );
        }
    }

    fn observe_rtp(&self, ssrc: u32, sequence: u16) {
        self.rtp_packets.fetch_add(1, Ordering::Relaxed);

        let mut streams = self
            .streams
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        streams
            .entry(ssrc)
            .and_modify(|stream| stream.observe(sequence))
            .or_insert_with(|| StreamContinuity::new(sequence));
    }
}

struct StreamContinuity {
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

impl StreamContinuity {
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

struct TelemetryHandler {
    telemetry: Arc<VoiceTelemetry>,
}

#[async_trait]
impl EventHandler for TelemetryHandler {
    async fn act(&self, context: &EventContext<'_>) -> Option<Event> {
        match context {
            EventContext::SpeakingStateUpdate(update) => {
                self.telemetry
                    .speaking_updates
                    .fetch_add(1, Ordering::Relaxed);
                self.telemetry.capture.try_send_speaker_mapping(
                    update.ssrc,
                    update.user_id.map(|user_id| user_id.to_string()),
                    update.speaking.bits(),
                );

                match update.user_id {
                    Some(user_id) => println!(
                        "Voice mapping: SSRC {} -> user {} ({:?}).",
                        update.ssrc, user_id, update.speaking
                    ),
                    None => println!(
                        "Voice mapping: SSRC {} has no Discord user ID ({:?}).",
                        update.ssrc, update.speaking
                    ),
                }
            }
            EventContext::RtpPacket(packet) => {
                let rtp = packet.rtp();
                let ssrc = rtp.get_ssrc();
                let sequence = rtp.get_sequence().into();
                let packet_length = packet.packet.len();
                let rtp_header_length = packet_length - rtp.payload().len();
                let payload_start = rtp_header_length + packet.payload_offset;
                let payload_end = packet_length - packet.payload_end_pad;

                self.telemetry.capture.try_send(CapturedPacket {
                    ssrc,
                    sequence,
                    timestamp: rtp.get_timestamp().into(),
                    payload_start: payload_start as u32,
                    payload_end: payload_end as u32,
                    packet: packet.packet.to_vec(),
                });
                self.telemetry.observe_rtp(ssrc, sequence);
            }
            EventContext::VoiceTick(tick) => {
                let tick_index = self.telemetry.voice_ticks.fetch_add(1, Ordering::Relaxed);

                let mut playout_packets = 0;
                let mut playout_losses = 0;
                let mut decoded_frames = 0;

                for (ssrc, voice) in &tick.speaking {
                    let packet = voice.packet.as_ref().map(|packet| {
                        let rtp = packet.rtp();
                        (rtp.get_sequence().into(), rtp.get_timestamp().into())
                    });
                    let decoded_samples = voice
                        .decoded_voice
                        .as_ref()
                        .map_or(0, |samples| samples.len() as u32);
                    self.telemetry.capture.try_send_playout(
                        tick_index,
                        *ssrc,
                        packet,
                        decoded_samples,
                    );

                    if packet.is_some() {
                        playout_packets += 1;
                    } else {
                        playout_losses += 1;
                    }

                    if let Some(samples) = &voice.decoded_voice {
                        decoded_frames += 1;
                        self.telemetry
                            .capture
                            .try_send_audio(tick_index, *ssrc, samples.clone());
                    }
                }

                self.telemetry
                    .playout_packets
                    .fetch_add(playout_packets, Ordering::Relaxed);
                self.telemetry
                    .playout_losses
                    .fetch_add(playout_losses, Ordering::Relaxed);
                self.telemetry
                    .decoded_frames
                    .fetch_add(decoded_frames, Ordering::Relaxed);
            }
            _ => {}
        }

        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn contiguous_sequences_cross_rollover() {
        let mut stream = StreamContinuity::new(u16::MAX - 1);

        stream.observe(u16::MAX);
        stream.observe(0);
        stream.observe(1);

        assert_eq!(stream.latest_sequence, 1);
        assert_eq!(stream.forward_gap_events, 0);
        assert_eq!(stream.missing_slots_observed, 0);
        assert_eq!(stream.out_of_order, 0);
    }

    #[test]
    fn forward_gap_counts_missing_sequence_slots() {
        let mut stream = StreamContinuity::new(10);

        stream.observe(13);

        assert_eq!(stream.forward_gap_events, 1);
        assert_eq!(stream.missing_slots_observed, 2);
        assert_eq!(stream.latest_sequence, 13);
    }

    #[test]
    fn repeated_sequence_is_duplicate() {
        let mut stream = StreamContinuity::new(10);

        stream.observe(11);
        stream.observe(11);

        assert_eq!(stream.duplicates, 1);
        assert_eq!(stream.out_of_order, 0);
    }

    #[test]
    fn earlier_unseen_sequence_is_out_of_order() {
        let mut stream = StreamContinuity::new(10);

        stream.observe(12);
        stream.observe(11);

        assert_eq!(stream.latest_sequence, 12);
        assert_eq!(stream.out_of_order, 1);
        assert_eq!(stream.duplicates, 0);
    }
}
