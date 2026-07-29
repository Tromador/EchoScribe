use std::collections::{HashMap, HashSet, VecDeque};

use crate::diagnostics::DecodedFrame;

pub(crate) const MAX_PENDING_TICKS: u64 = 250;
pub(crate) const MAX_PENDING_SAMPLES_PER_SSRC: usize = 96_000;
pub(crate) const MAX_CONCURRENT_PENDING_SSRCS: usize = 32;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct UserIdentity {
    pub(crate) discord_user_id: u64,
    pub(crate) server_display_name: Option<String>,
    pub(crate) global_display_name: Option<String>,
    pub(crate) username: String,
}

impl UserIdentity {
    pub(crate) fn display_name(&self) -> Option<&str> {
        self.server_display_name
            .as_deref()
            .filter(|name| !name.trim().is_empty())
            .or_else(|| {
                self.global_display_name
                    .as_deref()
                    .filter(|name| !name.trim().is_empty())
            })
            .or_else(|| (!self.username.trim().is_empty()).then_some(self.username.as_str()))
    }
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) struct ResolvedFrame {
    pub(crate) discord_user_id: u64,
    pub(crate) display_name: String,
    pub(crate) source_ssrc: u32,
    pub(crate) elapsed_nanos: u64,
    pub(crate) tick: u64,
    pub(crate) samples: Vec<i16>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AbandonmentReason {
    AgeLimit,
    SampleLimit,
    ConcurrentPendingLimit,
    ShutdownUnresolved,
}

impl AbandonmentReason {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::AgeLimit => "age_limit",
            Self::SampleLimit => "sample_limit",
            Self::ConcurrentPendingLimit => "concurrent_pending_limit",
            Self::ShutdownUnresolved => "shutdown_unresolved",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct UnresolvedSsrcAbandonment {
    pub(crate) ssrc: u32,
    pub(crate) first_tick: u64,
    pub(crate) last_tick: u64,
    pub(crate) elapsed_nanos: u64,
    pub(crate) discarded_frames: u64,
    pub(crate) discarded_samples: u64,
    pub(crate) reason: AbandonmentReason,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct UserTrackAbandonment {
    pub(crate) discord_user_id: u64,
    pub(crate) source: UnresolvedSsrcAbandonment,
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) enum RoutingAction {
    Frame(ResolvedFrame),
    IdentityUpdated(UserIdentity),
    UnresolvedSsrcAbandoned(UnresolvedSsrcAbandonment),
    UserTrackAbandoned(UserTrackAbandonment),
    MissingParticipantContext { discord_user_id: u64 },
}

struct PendingContinuity {
    first_tick: u64,
    last_tick: u64,
    last_elapsed_nanos: u64,
    samples: usize,
    frames: VecDeque<DecodedFrame>,
}

pub(crate) struct IdentityRouter {
    known_participants: HashSet<u64>,
    observed_users: HashSet<u64>,
    mappings: HashMap<u32, u64>,
    identities: HashMap<u64, UserIdentity>,
    pending: HashMap<u32, PendingContinuity>,
    abandoned_ssrcs: HashMap<u32, UnresolvedSsrcAbandonment>,
    abandoned_users: HashSet<u64>,
}

impl IdentityRouter {
    pub(crate) fn new(known_participants: impl IntoIterator<Item = u64>) -> Self {
        Self {
            known_participants: known_participants.into_iter().collect(),
            observed_users: HashSet::new(),
            mappings: HashMap::new(),
            identities: HashMap::new(),
            pending: HashMap::new(),
            abandoned_ssrcs: HashMap::new(),
            abandoned_users: HashSet::new(),
        }
    }

    pub(crate) fn observe_identity(&mut self, identity: UserIdentity) -> Vec<RoutingAction> {
        self.identities
            .insert(identity.discord_user_id, identity.clone());
        vec![RoutingAction::IdentityUpdated(identity)]
    }

    pub(crate) fn observe_mapping(
        &mut self,
        ssrc: u32,
        discord_user_id: Option<u64>,
    ) -> Vec<RoutingAction> {
        let Some(discord_user_id) = discord_user_id else {
            return Vec::new();
        };

        let mut actions = self.note_user(discord_user_id);
        self.mappings.insert(ssrc, discord_user_id);

        if let Some(abandonment) = self.abandoned_ssrcs.get(&ssrc).cloned() {
            if self.abandoned_users.insert(discord_user_id) {
                actions.push(RoutingAction::UserTrackAbandoned(UserTrackAbandonment {
                    discord_user_id,
                    source: abandonment,
                }));
            }
            self.pending.remove(&ssrc);
            return actions;
        }

        if self.abandoned_users.contains(&discord_user_id) {
            self.pending.remove(&ssrc);
            return actions;
        }

        if let Some(pending) = self.pending.remove(&ssrc) {
            actions.extend(
                pending
                    .frames
                    .into_iter()
                    .map(|frame| RoutingAction::Frame(self.resolve(discord_user_id, frame))),
            );
        }
        actions
    }

    pub(crate) fn observe_disconnect(&mut self, discord_user_id: u64) {
        self.mappings
            .retain(|_, mapped_user_id| *mapped_user_id != discord_user_id);
    }

    pub(crate) fn advance_tick(&mut self, tick: u64, elapsed_nanos: u64) -> Vec<RoutingAction> {
        let mut expired = self
            .pending
            .iter()
            .filter_map(|(ssrc, pending)| {
                (tick.saturating_sub(pending.first_tick) >= MAX_PENDING_TICKS).then_some(*ssrc)
            })
            .collect::<Vec<_>>();
        expired.sort_unstable();

        expired
            .into_iter()
            .map(|ssrc| {
                let pending = self
                    .pending
                    .remove(&ssrc)
                    .expect("expired pending SSRC was collected above");
                let abandonment = UnresolvedSsrcAbandonment {
                    ssrc,
                    first_tick: pending.first_tick,
                    last_tick: tick,
                    elapsed_nanos,
                    discarded_frames: pending.frames.len() as u64,
                    discarded_samples: pending.samples as u64,
                    reason: AbandonmentReason::AgeLimit,
                };
                self.abandoned_ssrcs.insert(ssrc, abandonment.clone());
                RoutingAction::UnresolvedSsrcAbandoned(abandonment)
            })
            .collect()
    }

    pub(crate) fn route_frame(&mut self, frame: DecodedFrame) -> Vec<RoutingAction> {
        if let Some(discord_user_id) = self.mappings.get(&frame.ssrc).copied() {
            let mut actions = self.note_user(discord_user_id);
            if !self.abandoned_users.contains(&discord_user_id)
                && !self.abandoned_ssrcs.contains_key(&frame.ssrc)
            {
                actions.push(RoutingAction::Frame(self.resolve(discord_user_id, frame)));
            }
            return actions;
        }

        if self.abandoned_ssrcs.contains_key(&frame.ssrc) {
            return Vec::new();
        }

        if let Some(pending) = self.pending.get(&frame.ssrc) {
            let age_exceeded = frame.tick.saturating_sub(pending.first_tick) >= MAX_PENDING_TICKS;
            let samples_exceeded = pending
                .samples
                .checked_add(frame.samples.len())
                .is_none_or(|samples| samples > MAX_PENDING_SAMPLES_PER_SSRC);
            if age_exceeded || samples_exceeded {
                let reason = if age_exceeded {
                    AbandonmentReason::AgeLimit
                } else {
                    AbandonmentReason::SampleLimit
                };
                return vec![self.abandon_with_trigger(frame, reason)];
            }
        } else {
            if self.pending.len() >= MAX_CONCURRENT_PENDING_SSRCS {
                return vec![self.abandon_new(frame, AbandonmentReason::ConcurrentPendingLimit)];
            }
            if frame.samples.len() > MAX_PENDING_SAMPLES_PER_SSRC {
                return vec![self.abandon_new(frame, AbandonmentReason::SampleLimit)];
            }
        }

        self.retain(frame);
        Vec::new()
    }

    pub(crate) fn finish(&mut self) -> Vec<RoutingAction> {
        let mut ssrcs = self.pending.keys().copied().collect::<Vec<_>>();
        ssrcs.sort_unstable();
        ssrcs
            .into_iter()
            .map(|ssrc| {
                let pending = self
                    .pending
                    .remove(&ssrc)
                    .expect("pending SSRC was collected above");
                let abandonment = UnresolvedSsrcAbandonment {
                    ssrc,
                    first_tick: pending.first_tick,
                    last_tick: pending.last_tick,
                    elapsed_nanos: pending.last_elapsed_nanos,
                    discarded_frames: pending.frames.len() as u64,
                    discarded_samples: pending.samples as u64,
                    reason: AbandonmentReason::ShutdownUnresolved,
                };
                self.abandoned_ssrcs.insert(ssrc, abandonment.clone());
                RoutingAction::UnresolvedSsrcAbandoned(abandonment)
            })
            .collect()
    }

    fn note_user(&mut self, discord_user_id: u64) -> Vec<RoutingAction> {
        if self.observed_users.insert(discord_user_id)
            && !self.known_participants.contains(&discord_user_id)
        {
            vec![RoutingAction::MissingParticipantContext { discord_user_id }]
        } else {
            Vec::new()
        }
    }

    fn resolve(&self, discord_user_id: u64, frame: DecodedFrame) -> ResolvedFrame {
        let display_name = self
            .identities
            .get(&discord_user_id)
            .and_then(UserIdentity::display_name)
            .map(str::to_owned)
            .unwrap_or_else(|| discord_user_id.to_string());
        ResolvedFrame {
            discord_user_id,
            display_name,
            source_ssrc: frame.ssrc,
            elapsed_nanos: frame.elapsed_nanos,
            tick: frame.tick,
            samples: frame.samples,
        }
    }

    fn retain(&mut self, frame: DecodedFrame) {
        let sample_count = frame.samples.len();
        match self.pending.entry(frame.ssrc) {
            std::collections::hash_map::Entry::Occupied(mut entry) => {
                let pending = entry.get_mut();
                pending.last_tick = frame.tick;
                pending.last_elapsed_nanos = frame.elapsed_nanos;
                pending.samples += sample_count;
                pending.frames.push_back(frame);
            }
            std::collections::hash_map::Entry::Vacant(entry) => {
                entry.insert(PendingContinuity {
                    first_tick: frame.tick,
                    last_tick: frame.tick,
                    last_elapsed_nanos: frame.elapsed_nanos,
                    samples: sample_count,
                    frames: VecDeque::from([frame]),
                });
            }
        }
    }

    fn abandon_with_trigger(
        &mut self,
        frame: DecodedFrame,
        reason: AbandonmentReason,
    ) -> RoutingAction {
        let pending = self
            .pending
            .remove(&frame.ssrc)
            .expect("triggered abandonment requires pending continuity");
        let abandonment = UnresolvedSsrcAbandonment {
            ssrc: frame.ssrc,
            first_tick: pending.first_tick,
            last_tick: frame.tick,
            elapsed_nanos: frame.elapsed_nanos,
            discarded_frames: pending.frames.len() as u64 + 1,
            discarded_samples: pending.samples as u64 + frame.samples.len() as u64,
            reason,
        };
        self.abandoned_ssrcs.insert(frame.ssrc, abandonment.clone());
        RoutingAction::UnresolvedSsrcAbandoned(abandonment)
    }

    fn abandon_new(&mut self, frame: DecodedFrame, reason: AbandonmentReason) -> RoutingAction {
        let abandonment = UnresolvedSsrcAbandonment {
            ssrc: frame.ssrc,
            first_tick: frame.tick,
            last_tick: frame.tick,
            elapsed_nanos: frame.elapsed_nanos,
            discarded_frames: 1,
            discarded_samples: frame.samples.len() as u64,
            reason,
        };
        self.abandoned_ssrcs.insert(frame.ssrc, abandonment.clone());
        RoutingAction::UnresolvedSsrcAbandoned(abandonment)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mapped_frame_routes_to_one_user() {
        let mut router = IdentityRouter::new([11]);
        assert!(router.observe_mapping(100, Some(11)).is_empty());

        let actions = router.route_frame(frame(10, 100, 960));

        let RoutingAction::Frame(resolved) = &actions[0] else {
            panic!("expected resolved frame");
        };
        assert_eq!(resolved.discord_user_id, 11);
        assert_eq!(resolved.display_name, "11");
        assert_eq!(resolved.source_ssrc, 100);
    }

    #[test]
    fn sequential_ssrcs_route_to_the_same_user() {
        let mut router = IdentityRouter::new([11]);
        router.observe_mapping(100, Some(11));
        router.observe_mapping(200, Some(11));

        for (ssrc, tick) in [(100, 10), (200, 11)] {
            let actions = router.route_frame(frame(tick, ssrc, 960));
            let RoutingAction::Frame(resolved) = &actions[0] else {
                panic!("expected resolved frame");
            };
            assert_eq!(resolved.discord_user_id, 11);
        }
    }

    #[test]
    fn display_name_uses_approved_fallback_order() {
        let cases = [
            (Some("Server"), Some("Global"), "username", "Server"),
            (None, Some("Global"), "username", "Global"),
            (None, None, "username", "username"),
        ];
        for (server, global, username, expected) in cases {
            let identity = UserIdentity {
                discord_user_id: 11,
                server_display_name: server.map(str::to_owned),
                global_display_name: global.map(str::to_owned),
                username: username.to_owned(),
            };
            assert_eq!(identity.display_name(), Some(expected));
        }

        let mut router = IdentityRouter::new([11]);
        router.observe_mapping(100, Some(11));
        let actions = router.route_frame(frame(10, 100, 960));
        let RoutingAction::Frame(resolved) = &actions[0] else {
            panic!("expected numeric fallback");
        };
        assert_eq!(resolved.display_name, "11");
    }

    #[test]
    fn pending_frames_resolve_in_tick_order() {
        let mut router = IdentityRouter::new([11]);
        router.route_frame(frame(10, 100, 960));
        router.route_frame(frame(11, 100, 960));

        let actions = router.observe_mapping(100, Some(11));
        let ticks = actions
            .iter()
            .filter_map(|action| match action {
                RoutingAction::Frame(frame) => Some(frame.tick),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(ticks, [10, 11]);
    }

    #[test]
    fn standard_frame_one_hundred_and_one_hits_sample_limit_before_retention() {
        let mut router = IdentityRouter::new([11]);
        for tick in 10..110 {
            assert!(router.route_frame(frame(tick, 100, 960)).is_empty());
        }

        let actions = router.route_frame(frame(110, 100, 960));
        let RoutingAction::UnresolvedSsrcAbandoned(abandonment) = &actions[0] else {
            panic!("expected unresolved abandonment");
        };
        assert_eq!(abandonment.first_tick, 10);
        assert_eq!(abandonment.last_tick, 110);
        assert_eq!(abandonment.discarded_frames, 101);
        assert_eq!(abandonment.discarded_samples, 96_960);
        assert_eq!(abandonment.reason, AbandonmentReason::SampleLimit);
    }

    #[test]
    fn global_tick_expires_a_silent_pending_ssrc_before_late_mapping() {
        let mut router = IdentityRouter::new([11]);
        router.route_frame(frame(10, 100, 960));

        assert!(router.advance_tick(259, 5_180_000_000).is_empty());
        let actions = router.advance_tick(260, 5_200_000_000);
        let RoutingAction::UnresolvedSsrcAbandoned(abandonment) = &actions[0] else {
            panic!("expected age-limit abandonment");
        };
        assert_eq!(abandonment.first_tick, 10);
        assert_eq!(abandonment.last_tick, 260);
        assert_eq!(abandonment.discarded_frames, 1);
        assert_eq!(abandonment.discarded_samples, 960);
        assert_eq!(abandonment.reason, AbandonmentReason::AgeLimit);

        let actions = router.observe_mapping(100, Some(11));
        assert!(
            actions
                .iter()
                .all(|action| !matches!(action, RoutingAction::Frame(_)))
        );
        assert!(actions.iter().any(|action| matches!(
            action,
            RoutingAction::UserTrackAbandoned(abandonment)
                if abandonment.discord_user_id == 11
        )));
    }

    #[test]
    fn disconnect_revokes_all_user_mappings_without_discarding_identity() {
        let mut router = IdentityRouter::new([11, 22]);
        router.observe_identity(UserIdentity {
            discord_user_id: 11,
            server_display_name: Some("Eleven".into()),
            global_display_name: None,
            username: "eleven".into(),
        });
        router.observe_mapping(100, Some(11));
        router.observe_disconnect(11);

        assert!(router.route_frame(frame(10, 100, 960)).is_empty());
        let actions = router.observe_mapping(100, Some(22));
        let frames = actions
            .iter()
            .filter_map(|action| match action {
                RoutingAction::Frame(frame) => Some(frame),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].discord_user_id, 22);

        router.observe_mapping(200, Some(11));
        let actions = router.route_frame(frame(11, 200, 960));
        let RoutingAction::Frame(frame) = &actions[0] else {
            panic!("expected reconnected user frame");
        };
        assert_eq!(frame.display_name, "Eleven");
    }

    #[test]
    fn sample_limit_is_checked_before_retaining_trigger() {
        let mut router = IdentityRouter::new([11]);
        router.route_frame(frame(10, 100, 95_500));

        let actions = router.route_frame(frame(11, 100, 501));
        let RoutingAction::UnresolvedSsrcAbandoned(abandonment) = &actions[0] else {
            panic!("expected sample-limit abandonment");
        };
        assert_eq!(abandonment.discarded_frames, 2);
        assert_eq!(abandonment.discarded_samples, 96_001);
        assert_eq!(abandonment.reason, AbandonmentReason::SampleLimit);
    }

    #[test]
    fn pending_slot_is_released_after_resolution() {
        let mut router = IdentityRouter::new(1..=33);
        for ssrc in 1..=32 {
            router.route_frame(frame(10, ssrc, 960));
        }
        router.observe_mapping(1, Some(1));

        assert!(router.route_frame(frame(10, 33, 960)).is_empty());
    }

    #[test]
    fn thirty_third_concurrent_pending_ssrc_is_abandoned() {
        let mut router = IdentityRouter::new(1..=33);
        for ssrc in 1..=32 {
            router.route_frame(frame(10, ssrc, 960));
        }

        let actions = router.route_frame(frame(10, 33, 960));
        let RoutingAction::UnresolvedSsrcAbandoned(abandonment) = &actions[0] else {
            panic!("expected concurrency abandonment");
        };
        assert_eq!(
            abandonment.reason,
            AbandonmentReason::ConcurrentPendingLimit
        );
    }

    #[test]
    fn late_mapping_poisons_user_across_replacement_ssrc() {
        let mut router = IdentityRouter::new([11]);
        for tick in 10..110 {
            router.route_frame(frame(tick, 100, 960));
        }
        router.route_frame(frame(110, 100, 960));

        let actions = router.observe_mapping(100, Some(11));
        assert!(actions.iter().any(|action| matches!(
            action,
            RoutingAction::UserTrackAbandoned(abandonment)
                if abandonment.discord_user_id == 11
        )));

        router.observe_mapping(200, Some(11));
        assert!(router.route_frame(frame(111, 200, 960)).is_empty());
    }

    #[test]
    fn missing_participant_context_warns_once_without_blocking() {
        let mut router = IdentityRouter::new([]);
        let actions = router.observe_mapping(100, Some(11));
        assert_eq!(
            actions,
            [RoutingAction::MissingParticipantContext {
                discord_user_id: 11
            }]
        );

        let actions = router.route_frame(frame(10, 100, 960));
        assert!(matches!(actions[0], RoutingAction::Frame(_)));
        assert!(router.observe_mapping(200, Some(11)).is_empty());
    }

    fn frame(tick: u64, ssrc: u32, samples: usize) -> DecodedFrame {
        DecodedFrame {
            elapsed_nanos: tick.saturating_mul(20_000_000),
            tick,
            ssrc,
            samples: vec![1; samples],
        }
    }
}
