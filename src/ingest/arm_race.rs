//! Cross-arm trade matcher: the sampler behind [`crate::ingest::authority`]'s speed re-election.
//!
//! Two redundant arms of one venue publish the same trades, and we need to know which delivers them
//! first. There is no shared wire coordinate to pair on — that is the entire premise of `Sticky`
//! arbitration — so the pairing is by **content**, and the measurement is the difference between our
//! own two receive times. One clock, nanosecond resolution, no dependence on anything the publisher
//! stamps.
//!
//! # Why trades, and only trades
//!
//! A level update's cross-arm-common fields reduce to `(side, price, quantity)` on a coarse bounded
//! price grid, so content matching them would mis-pair constantly. A trade's
//! `(instrument, price, size, aggressor)` is near-unique, and trades are rare enough that a bounded
//! pending set is cheap.
//!
//! # Why the venue timestamp is not used
//!
//! `LevelUpdate.Timestamp` is the venue's own time when the venue supplies one, and the publisher's
//! own clock when it does not — silently, with no flag on the wire. An arm with no venue timestamp
//! would therefore measure only the network leg and look fastest by construction, and filtering those
//! samples out by their millisecond granularity would disenfranchise that arm instead. Since the
//! FIX-sourced arm is the one both expected to be faster and expected to lack venue timestamps, that
//! is precisely backwards. Our own receive clock has no such asymmetry.
//!
//! # Identical trades
//!
//! Two trades with the same signature inside one window are not a collision to be avoided but an
//! ordering to be respected: each signature holds a **FIFO** of unmatched arrivals, so an arm's first
//! copy pairs with the peer's first copy and the second with the second. Both arms see the venue's
//! stream in order, so that pairing is the correct one. The reference implementation this borrows
//! from keys on content with no such queue, which silently mis-pairs repeats.

use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
};

use crate::ingest::arbiter::Publisher;

/// A trade's cross-arm identity. Raw fixed-point integers, never floats: both arms scale by the same
/// instrument definition, so the raw values are bit-identical and compare exactly.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct Signature {
    venue: Arc<str>,
    instrument_id: u32,
    price_raw: i64,
    qty_raw: u64,
    aggressor: u8,
}

/// One arm's unmatched arrival.
#[derive(Debug, Clone, Copy)]
struct Arrival {
    arm: Publisher,
    recv_ns: u64,
}

/// A matched pair: both arms delivered the same trade.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Match {
    pub venue: Arc<str>,
    /// The arm that delivered it first.
    pub first: Publisher,
    /// The arm that delivered the same trade second.
    pub second: Publisher,
    /// How far ahead `first` was, on our receive clock. Always non-negative.
    pub delta_ns: u64,
}

impl Match {
    /// The signed lead to hand [`crate::ingest::authority::StickyAuthority::observe_matched_lead`],
    /// given who currently holds authority: the challenger arm, and **negative** when the challenger
    /// beat the leader.
    ///
    /// The sign lives here, in one tested place, deliberately: getting it backwards is exactly what
    /// made the first version of this election silently inert. `None` when the leader was not one of
    /// the two arms in the pair, which is not evidence about it either way.
    pub fn lead_for(&self, leader: Publisher) -> Option<(Publisher, i64)> {
        let delta = i64::try_from(self.delta_ns).unwrap_or(i64::MAX);
        if leader == self.second {
            Some((self.first, -delta)) // the challenger arrived first: it led
        } else if leader == self.first {
            Some((self.second, delta)) // the challenger arrived second: it lagged
        } else {
            None
        }
    }
}

/// How long an unmatched trade waits for its peer copy. Must exceed the worst plausible inter-arm
/// lead by a wide margin, and stay far below the interval between repeats of one signature.
const DEFAULT_WINDOW_NS: u64 = 5_000_000_000; // 5s

/// Cap on unmatched arrivals held across all signatures. The multicast source is unauthenticated, so
/// a forged stream of distinct trades must not grow this without limit; past the cap the oldest
/// arrival is dropped, which costs a sample and nothing else.
const MAX_PENDING: usize = 1 << 16;

pub struct ArmRace {
    window_ns: u64,
    pending: HashMap<Signature, VecDeque<Arrival>>,
    /// Arrival order across signatures, so both eviction paths are O(1) amortized rather than a scan.
    order: VecDeque<(Signature, u64)>,
}

impl Default for ArmRace {
    fn default() -> Self {
        Self::new(DEFAULT_WINDOW_NS)
    }
}

impl ArmRace {
    pub fn new(window_ns: u64) -> Self {
        Self {
            window_ns,
            pending: HashMap::new(),
            order: VecDeque::new(),
        }
    }

    /// Offer one arm's trade. Returns a [`Match`] when the peer arm already delivered the same trade
    /// and is still inside the window.
    ///
    /// `recv_ns` must be our own receive clock (`recv_ts_ns`), the same clock for both arms — the
    /// whole measurement is the difference between two readings of it. Never a publisher-stamped time.
    #[allow(clippy::too_many_arguments)]
    pub fn on_trade(
        &mut self,
        venue: &Arc<str>,
        instrument_id: u32,
        price_raw: i64,
        qty_raw: u64,
        aggressor: u8,
        arm: Publisher,
        recv_ns: u64,
    ) -> Option<Match> {
        self.evict_stale(recv_ns);
        let sig = Signature {
            venue: venue.clone(),
            instrument_id,
            price_raw,
            qty_raw,
            aggressor,
        };
        if let Some(q) = self.pending.get_mut(&sig) {
            // Match against the OLDEST unmatched arrival from a different arm, so repeats pair in
            // order. A same-arm repeat queues behind rather than matching itself.
            if let Some(i) = q.iter().position(|a| a.arm != arm) {
                let peer = q.remove(i).expect("index just found");
                if q.is_empty() {
                    self.pending.remove(&sig);
                }
                self.forget_one(&sig, peer.recv_ns);
                return Some(Match {
                    venue: venue.clone(),
                    first: peer.arm,
                    second: arm,
                    delta_ns: recv_ns.saturating_sub(peer.recv_ns),
                });
            }
        }
        self.push(sig, Arrival { arm, recv_ns });
        None
    }

    /// Unmatched arrivals still waiting.
    pub fn pending_len(&self) -> usize {
        self.order.len()
    }

    /// Drop arrivals older than the window, returning how many were dropped per arm — the
    /// "seen only on this arm" signal, which is a drop or a genuine one-sided print, and worth a
    /// counter once a caller wires it.
    pub fn evict_stale(&mut self, now_ns: u64) -> Vec<(Publisher, usize)> {
        let mut dropped: HashMap<Publisher, usize> = HashMap::new();
        while let Some((sig, at)) = self.order.front() {
            if now_ns.saturating_sub(*at) <= self.window_ns {
                break;
            }
            let (sig, at) = (sig.clone(), *at);
            self.order.pop_front();
            if let Some(arm) = self.drop_arrival(&sig, at) {
                *dropped.entry(arm).or_default() += 1;
            }
        }
        dropped.into_iter().collect()
    }

    fn push(&mut self, sig: Signature, arrival: Arrival) {
        // Bound first: a forged flood of distinct signatures must not grow either structure.
        while self.order.len() >= MAX_PENDING {
            let Some((old_sig, old_at)) = self.order.pop_front() else {
                break;
            };
            self.drop_arrival(&old_sig, old_at);
        }
        self.order.push_back((sig.clone(), arrival.recv_ns));
        self.pending.entry(sig).or_default().push_back(arrival);
    }

    /// Remove the `order` entry for one arrival that was matched out of `pending`.
    fn forget_one(&mut self, sig: &Signature, recv_ns: u64) {
        if let Some(i) = self
            .order
            .iter()
            .position(|(s, at)| s == sig && *at == recv_ns)
        {
            self.order.remove(i);
        }
    }

    /// Remove the queued arrival matching `(sig, at)`, returning its arm if it was still there.
    fn drop_arrival(&mut self, sig: &Signature, at: u64) -> Option<Publisher> {
        let q = self.pending.get_mut(sig)?;
        let i = q.iter().position(|a| a.recv_ns == at)?;
        let arm = q.remove(i)?.arm;
        if q.is_empty() {
            self.pending.remove(sig);
        }
        Some(arm)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr};

    fn arm(n: u8) -> Publisher {
        Publisher::Edge(IpAddr::V4(Ipv4Addr::new(10, 0, 0, n)))
    }

    fn venue() -> Arc<str> {
        Arc::from("Lashay")
    }

    /// One trade, both arms: arm(1) 10us ahead.
    fn race() -> (ArmRace, Arc<str>) {
        (ArmRace::new(1_000_000_000), venue())
    }

    fn trade(r: &mut ArmRace, v: &Arc<str>, a: Publisher, recv_ns: u64) -> Option<Match> {
        r.on_trade(v, 41, 6_200, 150, 1, a, recv_ns)
    }

    #[test]
    fn the_second_arm_to_deliver_completes_the_match() {
        let (mut r, v) = race();
        assert_eq!(
            trade(&mut r, &v, arm(1), 1_000),
            None,
            "nothing to pair yet"
        );
        assert_eq!(
            trade(&mut r, &v, arm(2), 11_000),
            Some(Match {
                venue: v.clone(),
                first: arm(1),
                second: arm(2),
                delta_ns: 10_000,
            })
        );
        assert_eq!(r.pending_len(), 0, "a matched pair leaves nothing behind");
    }

    /// **The sign, which is what made the first election inert.** Negative means the challenger beat
    /// the leader, so a faster challenger accumulates negative samples and can clear the margin.
    #[test]
    fn lead_for_is_negative_when_the_challenger_led() {
        let m = Match {
            venue: venue(),
            first: arm(2),
            second: arm(1),
            delta_ns: 10_000,
        };
        // arm(1) holds authority and arrived second: the challenger arm(2) led by 10us.
        assert_eq!(m.lead_for(arm(1)), Some((arm(2), -10_000)));
        // arm(2) holds authority and arrived first: the challenger arm(1) lagged by 10us.
        assert_eq!(m.lead_for(arm(2)), Some((arm(1), 10_000)));
        // A leader that was in neither copy is told nothing.
        assert_eq!(m.lead_for(arm(3)), None);
    }

    /// A slower arm feeding a whole window of these must not be able to clear the margin, so the
    /// two halves have to agree on sign. This pins the round trip through `authority`.
    #[test]
    fn a_faster_challenger_actually_wins_the_election() {
        use crate::ingest::authority::{AuthorityConfig, StickyAuthority};
        let (mut r, v) = race();
        let cfg = AuthorityConfig {
            leader_timeout_ns: u64::MAX, // silence is not what is under test
            sample_interval_ns: 1_000,
            transfer_margin_ns: 1_000,
            transfer_win_rate: 0.8,
            min_window_samples: 5,
        };
        let mut auth = StickyAuthority::new(cfg);
        let key = (v.clone(), 2, 41);
        auth.admit(key.clone(), arm(1), 1);

        // arm(2) is genuinely 10us faster on every one of ten trades, in true arrival order.
        for i in 0..10u64 {
            let t = 1_000_000 * i;
            assert_eq!(
                r.on_trade(&v, 41, 6_200 + i as i64, 150, 1, arm(2), t),
                None
            );
            let m = r
                .on_trade(&v, 41, 6_200 + i as i64, 150, 1, arm(1), t + 10_000)
                .expect("the pair matches");
            let leader = auth.venue_leader(&v).unwrap();
            let (challenger, lead) = m.lead_for(leader).expect("leader is in the pair");
            auth.observe_matched_lead(&v, challenger, lead);
        }
        assert_eq!(
            auth.close_window(20_000_000),
            vec![(v.clone(), arm(2))],
            "the faster arm must take authority"
        );
    }

    /// Identical trades pair in order rather than colliding — the gap in the implementation this
    /// borrows from, which keys on content with no queue.
    #[test]
    fn identical_repeated_trades_pair_in_order() {
        let (mut r, v) = race();
        trade(&mut r, &v, arm(1), 1_000);
        trade(&mut r, &v, arm(1), 2_000);
        assert_eq!(
            r.pending_len(),
            2,
            "a same-arm repeat queues, never self-matches"
        );

        let first = trade(&mut r, &v, arm(2), 3_000).expect("pairs the oldest");
        assert_eq!(first.delta_ns, 2_000, "3_000 - 1_000");
        let second = trade(&mut r, &v, arm(2), 4_000).expect("pairs the next");
        assert_eq!(second.delta_ns, 2_000, "4_000 - 2_000");
        assert_eq!(r.pending_len(), 0);
    }

    /// A trade only one arm ever sent is dropped after the window and attributed, so "seen only on
    /// this arm" is observable rather than silent.
    #[test]
    fn an_unmatched_trade_is_evicted_and_attributed() {
        let (mut r, v) = race();
        trade(&mut r, &v, arm(1), 1_000);
        assert_eq!(
            r.evict_stale(500_000_000),
            vec![],
            "still inside the window"
        );
        assert_eq!(r.pending_len(), 1);
        assert_eq!(r.evict_stale(1_000_000_001 + 1_000), vec![(arm(1), 1)]);
        assert_eq!(r.pending_len(), 0);
    }

    /// A peer copy arriving after the window is not a match: pairing it would report a lead of
    /// seconds and drag the median with it.
    #[test]
    fn a_copy_past_the_window_does_not_match() {
        let (mut r, v) = race();
        trade(&mut r, &v, arm(1), 1_000);
        assert_eq!(trade(&mut r, &v, arm(2), 2_000_000_000), None);
        assert_eq!(r.pending_len(), 1, "only the late copy is now waiting");
    }

    /// Different trades never match each other, on any one distinguishing field.
    #[test]
    fn distinct_trades_do_not_match() {
        let v = venue();
        for (id, px, qty, aggr) in [
            (42u32, 6_200i64, 150u64, 1u8),
            (41, 6_201, 150, 1),
            (41, 6_200, 151, 1),
            (41, 6_200, 150, 2),
        ] {
            let mut r = ArmRace::new(1_000_000_000);
            r.on_trade(&v, 41, 6_200, 150, 1, arm(1), 1_000);
            assert_eq!(
                r.on_trade(&v, id, px, qty, aggr, arm(2), 2_000),
                None,
                "id={id} px={px} qty={qty} aggr={aggr} must not pair"
            );
        }
    }

    /// Two venues sharing an instrument id and a price never cross-match.
    #[test]
    fn venues_do_not_cross_match() {
        let mut r = ArmRace::new(1_000_000_000);
        let (a, b): (Arc<str>, Arc<str>) = (Arc::from("Lashay"), Arc::from("Other"));
        r.on_trade(&a, 41, 6_200, 150, 1, arm(1), 1_000);
        assert_eq!(r.on_trade(&b, 41, 6_200, 150, 1, arm(2), 2_000), None);
    }

    /// The pending set is bounded: the source is unauthenticated, so a flood of distinct trades must
    /// cost samples rather than memory.
    #[test]
    fn pending_arrivals_are_bounded() {
        let mut r = ArmRace::new(u64::MAX); // no time-based eviction, so only the cap can bound it
        let v = venue();
        for i in 0..(MAX_PENDING as i64 + 500) {
            r.on_trade(&v, 41, i, 150, 1, arm(1), 1_000);
        }
        assert_eq!(r.pending_len(), MAX_PENDING);
        assert!(
            r.pending.len() <= MAX_PENDING,
            "the signature map must be bounded too, got {}",
            r.pending.len()
        );
    }

    /// Eviction and matching must keep the two structures in step, or `pending_len` drifts from what
    /// is really held and the cap stops bounding anything.
    #[test]
    fn the_order_index_stays_consistent_with_the_queues() {
        let mut r = ArmRace::new(1_000_000_000);
        let v = venue();
        for i in 0..50i64 {
            r.on_trade(&v, 41, i, 150, 1, arm(1), 1_000 + i as u64);
        }
        // Match half of them out of the middle of the index.
        for i in 0..25i64 {
            assert!(r.on_trade(&v, 41, i, 150, 1, arm(2), 5_000).is_some());
        }
        assert_eq!(r.pending_len(), 25);
        let queued: usize = r.pending.values().map(|q| q.len()).sum();
        assert_eq!(queued, r.pending_len(), "order index and queues agree");
        // And the survivors still evict cleanly.
        r.evict_stale(u64::MAX / 2);
        assert_eq!(r.pending_len(), 0);
        assert!(r.pending.is_empty(), "no empty queues left behind");
    }
}
