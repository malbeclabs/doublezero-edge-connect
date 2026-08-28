//! Cross-path trade matcher: the sampler behind [`crate::ingest::authority`]'s speed re-election.
//!
//! Two redundant paths of one venue publish the same trades, and we need to know which delivers them
//! first. There is no shared wire coordinate to pair on — that is the entire premise of `Sticky`
//! arbitration — so the pairing is by **content**, and the measurement is the difference between our
//! own two receive times. One clock, nanosecond resolution, no dependence on anything the publisher
//! stamps.
//!
//! # Why trades, and only trades
//!
//! A level update's cross-path-common fields reduce to `(side, price, quantity)` on a coarse bounded
//! price grid, so content matching them would mis-pair constantly. A trade's
//! `(symbol, price, size, aggressor)` is near-unique, and trades are rare enough that a bounded
//! pending set is cheap.
//!
//! # Matching on normalized fields
//!
//! The pair key is the [`crate::model::NormalizedTrade`]'s own fields, so `symbol` — a truncated
//! 16-byte wire field — stands in for the instrument id and can collide across instruments on a
//! sharded feed. A mis-pair then needs two colliding-symbol instruments to trade at an identical
//! price *and* size *and* aggressor side inside the window; the scope key and the FIFO per signature
//! still hold. That is the recorded cost of matching on normalized rather than wire fields.
//!
//! # Why pairs form within one scope
//!
//! The scope is `(venue, category)` — the emitting row's instrument universe — and not the venue,
//! because this matcher is the sole producer of the evidence
//! [`crate::ingest::authority::StickyAuthority::close_window`] elects on, and that election is per
//! universe. A cross-universe pair would hand `observe_matched_lead` a publisher that is a stranger
//! to the scope it is filed under: `path_ordinal` mints it into that scope's eight admission slots,
//! its samples accumulate there, and a margin transfer can elect a path that serves no `book` in
//! that universe at all. Trades carry no category — the emitting row supplies it, exactly as it does
//! for the tape gate.
//!
//! # Why the venue timestamp is not used
//!
//! `LevelUpdate.Timestamp` is the venue's own time when the venue supplies one, and the publisher's
//! own clock when it does not — silently, with no flag on the wire. A path with no venue timestamp
//! would therefore measure only the network leg and look fastest by construction, and filtering those
//! samples out by their millisecond granularity would disenfranchise that path instead. Since the
//! FIX-sourced path is the one both expected to be faster and expected to lack venue timestamps, that
//! is precisely backwards. Our own receive clock has no such asymmetry.
//!
//! # Identical trades
//!
//! Two trades with the same signature inside one window are not a collision to be avoided but an
//! ordering to be respected: each signature holds a **FIFO** of unmatched arrivals, so a path's first
//! copy pairs with the peer's first copy and the second with the second. Both paths see the venue's
//! feed in order, so that pairing is the correct one. The reference implementation this borrows
//! from keys on content with no such queue, which silently mis-pairs repeats.

use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
};

use crate::{
    ingest::{arbiter::Transport, authority::ScopeKey},
    model::Side,
};

/// A trade's cross-path identity. Price and size key on `f64::to_bits` — **exact**, no rounding step,
/// which holds only for paths sharing one decode path ([`PathRace::on_trade`]'s precondition); a
/// public-JSON decimal would need `arbiter::QuoteId`'s `10^-8` canonicalization instead. A non-finite
/// or signed-zero price is unreachable from a fixed-point decode, and either way costs at most a
/// lost sample.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct Signature {
    /// `(venue, category)`, never the venue alone. The authority elects per instrument universe, and
    /// this matcher is the sole producer of that election's evidence: a pair formed across two
    /// universes would file a lead against paths that publish nothing for each other, mint a foreign
    /// publisher into the scope's eight admission slots, and can elect a path that serves no `book`
    /// there at all. Trades carry no category, so it is supplied by the emitting row.
    scope: ScopeKey,
    symbol: Arc<str>,
    price_bits: u64,
    size_bits: u64,
    aggressor: Side,
}

/// One path's unmatched arrival.
#[derive(Debug, Clone, Copy)]
struct Arrival {
    path: Transport,
    recv_ns: u64,
}

/// A matched pair: both paths delivered the same trade.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Match {
    pub scope: ScopeKey,
    /// The path that delivered it first.
    pub first: Transport,
    /// The path that delivered the same trade second.
    pub second: Transport,
    /// How far ahead `first` was, on our receive clock. Always non-negative.
    pub delta_ns: u64,
}

impl Match {
    /// The signed lead to hand [`crate::ingest::authority::StickyAuthority::observe_matched_lead`],
    /// given who currently holds authority: the challenger path, and **negative** when the challenger
    /// beat the leader.
    ///
    /// The sign lives here, in one tested place, deliberately: getting it backwards is exactly what
    /// made the first version of this election silently inert. `None` when the leader was not one of
    /// the two paths in the pair, which is not evidence about it either way.
    pub fn lead_for(&self, leader: Transport) -> Option<(Transport, i64)> {
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

/// How long an unmatched trade waits for its peer copy. Must exceed the worst plausible inter-path
/// lead by a wide margin, and stay far below the interval between repeats of one signature.
const DEFAULT_WINDOW_NS: u64 = 5_000_000_000; // 5s

/// Cap on `order` entries, arrivals and tombstones alike. The multicast wire is unauthenticated, so
/// a forged feed of distinct trades must not grow this without limit; past the cap the oldest
/// arrival is dropped, which costs a sample and nothing else.
const MAX_PENDING: usize = 1 << 16;

/// Cap on one signature's FIFO. The queue exists only to pair identical repeats inside one window in
/// arrival order, so a legitimate one is a handful deep — and it is scanned per call under the shared
/// arbiter lock, so one path repeating one signature must not be able to lengthen it.
const MAX_PER_SIGNATURE: usize = 8;

/// Cap on distinct `(scope, path)` counter keys. A [`Transport`] is a spoofable source IP address, so past the
/// cap a new key is refused rather than grown into.
const MAX_UNMATCHED_KEYS: usize = 1024;

pub struct PathRace {
    window_ns: u64,
    pending: HashMap<Signature, VecDeque<Arrival>>,
    /// Arrival order across signatures, so eviction is O(1) amortized rather than a scan. An entry
    /// whose arrival left `pending` early (matched, or dropped by a cap) stays as a **tombstone**: a
    /// later no-op eviction, which is why `order.len()` counts held memory and not live arrivals.
    order: VecDeque<(Signature, u64)>,
    /// Live arrivals across all queues, tracked rather than derived so no path has to scan `order`.
    live: usize,
    /// Window evictions since the last drain. Only those: the cap paths are overload, not a one-sided
    /// print, and merging them would blur what the counter means.
    unmatched: HashMap<(ScopeKey, Transport), u64>,
}

impl Default for PathRace {
    fn default() -> Self {
        Self::new(DEFAULT_WINDOW_NS)
    }
}

impl PathRace {
    pub fn new(window_ns: u64) -> Self {
        Self {
            window_ns,
            pending: HashMap::new(),
            order: VecDeque::new(),
            live: 0,
            unmatched: HashMap::new(),
        }
    }

    /// Offer one path's trade. Returns a [`Match`] when the peer path already delivered the same trade
    /// and is still inside the window.
    ///
    /// `recv_ns` must be our own receive clock (`recv_ts_ns`), the same clock for both paths — the
    /// whole measurement is the difference between two readings of it. Never a publisher-stamped time.
    ///
    /// Offer only paths that decode the same wire integers under the same instrument definition: the
    /// pair key compares `price`/`size` bit-for-bit (see [`Signature`]).
    #[allow(clippy::too_many_arguments)]
    pub fn on_trade(
        &mut self,
        scope: &ScopeKey,
        symbol: &Arc<str>,
        price: f64,
        size: f64,
        aggressor: Side,
        path: Transport,
        recv_ns: u64,
    ) -> Option<Match> {
        self.evict_stale(recv_ns);
        let sig = Signature {
            scope: scope.clone(),
            symbol: symbol.clone(),
            price_bits: price.to_bits(),
            size_bits: size.to_bits(),
            aggressor,
        };
        if let Some(q) = self.pending.get_mut(&sig) {
            // Match against the OLDEST unmatched arrival from a different path, so repeats pair in
            // order. A same-path repeat queues behind rather than matching itself.
            if let Some(i) = q.iter().position(|a| a.path != path) {
                let peer = q.remove(i).expect("index just found");
                if q.is_empty() {
                    self.pending.remove(&sig);
                }
                self.live = self.live.saturating_sub(1); // its `order` entry is now a tombstone
                return Some(Match {
                    scope: scope.clone(),
                    first: peer.path,
                    second: path,
                    delta_ns: recv_ns.saturating_sub(peer.recv_ns),
                });
            }
        }
        self.push(sig, Arrival { path, recv_ns });
        None
    }

    /// Unmatched arrivals still waiting: the live count, not `order.len()` (tombstones included).
    pub fn pending_len(&self) -> usize {
        self.live
    }

    /// Take the per-`(scope, path)` window-eviction counts accumulated since the last drain — the
    /// "seen only on this path" signal, which is a drop or a genuine one-sided print. Accumulated
    /// rather than returned by [`Self::evict_stale`], which [`Self::on_trade`] calls mid-burst.
    pub fn drain_unmatched(&mut self) -> Vec<((ScopeKey, Transport), u64)> {
        std::mem::take(&mut self.unmatched).into_iter().collect()
    }

    /// Drop arrivals older than the window, returning how many were dropped per path and adding them
    /// to the [`Self::drain_unmatched`] accumulator.
    pub fn evict_stale(&mut self, now_ns: u64) -> Vec<(Transport, usize)> {
        let mut dropped: HashMap<Transport, usize> = HashMap::new();
        while let Some((sig, at)) = self.order.front() {
            if now_ns.saturating_sub(*at) <= self.window_ns {
                break;
            }
            let (sig, at) = (sig.clone(), *at);
            self.order.pop_front();
            if let Some(path) = self.drop_arrival(&sig, at) {
                *dropped.entry(path).or_default() += 1;
                self.record_unmatched(&sig.scope, path);
            }
        }
        dropped.into_iter().collect()
    }

    fn record_unmatched(&mut self, scope: &ScopeKey, path: Transport) {
        let key = (scope.clone(), path);
        if let Some(n) = self.unmatched.get_mut(&key) {
            *n += 1;
        } else if self.unmatched.len() < MAX_UNMATCHED_KEYS {
            self.unmatched.insert(key, 1);
        }
    }

    fn push(&mut self, sig: Signature, arrival: Arrival) {
        // Bound first: a forged flood of distinct signatures must not grow either structure. The cap
        // is on `order`, so tombstones are counted and memory stays bounded regardless of matching.
        while self.order.len() >= MAX_PENDING {
            let Some((old_sig, old_at)) = self.order.pop_front() else {
                break;
            };
            self.drop_arrival(&old_sig, old_at);
        }
        self.order.push_back((sig.clone(), arrival.recv_ns));
        let q = self.pending.entry(sig).or_default();
        if q.len() >= MAX_PER_SIGNATURE {
            q.pop_front();
            self.live = self.live.saturating_sub(1);
        }
        q.push_back(arrival);
        self.live += 1;
    }

    /// Remove the queued arrival matching `(sig, at)`, returning its path if it was still there.
    fn drop_arrival(&mut self, sig: &Signature, at: u64) -> Option<Transport> {
        let q = self.pending.get_mut(sig)?;
        let i = q.iter().position(|a| a.recv_ns == at)?;
        let path = q.remove(i)?.path;
        if q.is_empty() {
            self.pending.remove(sig);
        }
        self.live = self.live.saturating_sub(1);
        Some(path)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr};

    fn path(n: u8) -> Transport {
        Transport::Edge(IpAddr::V4(Ipv4Addr::new(10, 0, 0, n)))
    }

    /// Distinct paths past a single octet, for the counter-map cap.
    fn path_n(n: usize) -> Transport {
        Transport::Edge(IpAddr::V4(Ipv4Addr::new(10, 0, (n >> 8) as u8, n as u8)))
    }

    fn sym() -> Arc<str> {
        Arc::from("KXBTCPERP")
    }

    /// The instrument universe every test that is not *about* universes runs in.
    const CATEGORY: &str = "perps";

    /// One venue, one universe: the scope a pair has to share to form.
    fn scope() -> ScopeKey {
        (Arc::from("KALSHI"), Arc::from(CATEGORY))
    }

    /// A second universe under the **same** Source ID, publishing an unrelated instrument set.
    fn other_scope() -> ScopeKey {
        (Arc::from("KALSHI"), Arc::from("sports"))
    }

    /// One trade, both paths: path(1) 10us ahead.
    fn race() -> (PathRace, ScopeKey) {
        (PathRace::new(1_000_000_000), scope())
    }

    fn trade(r: &mut PathRace, v: &ScopeKey, a: Transport, recv_ns: u64) -> Option<Match> {
        r.on_trade(v, &sym(), 6_200.0, 150.0, Side::Buy, a, recv_ns)
    }

    #[test]
    fn the_second_path_to_deliver_completes_the_match() {
        let (mut r, v) = race();
        assert_eq!(
            trade(&mut r, &v, path(1), 1_000),
            None,
            "nothing to pair yet"
        );
        assert_eq!(
            trade(&mut r, &v, path(2), 11_000),
            Some(Match {
                scope: v.clone(),
                first: path(1),
                second: path(2),
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
            scope: scope(),
            first: path(2),
            second: path(1),
            delta_ns: 10_000,
        };
        // path(1) holds authority and arrived second: the challenger path(2) led by 10us.
        assert_eq!(m.lead_for(path(1)), Some((path(2), -10_000)));
        // path(2) holds authority and arrived first: the challenger path(1) lagged by 10us.
        assert_eq!(m.lead_for(path(2)), Some((path(1), 10_000)));
        // A leader that was in neither copy is told nothing.
        assert_eq!(m.lead_for(path(3)), None);
    }

    /// A slower path feeding a whole window of these must not be able to clear the margin, so the
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
        let key = (v.0.clone(), v.1.clone(), 2, 41);
        auth.admit(key.clone(), path(1), 1);

        // path(2) is genuinely 10us faster on every one of ten trades, in true arrival order.
        for i in 0..10u64 {
            let (t, px) = (1_000_000 * i, 6_200.0 + i as f64);
            assert_eq!(
                r.on_trade(&v, &sym(), px, 150.0, Side::Buy, path(2), t),
                None
            );
            let m = r
                .on_trade(&v, &sym(), px, 150.0, Side::Buy, path(1), t + 10_000)
                .expect("the pair matches");
            let leader = auth.scope_leader(&v).unwrap();
            let (challenger, lead) = m.lead_for(leader).expect("leader is in the pair");
            auth.observe_matched_lead(&v, challenger, lead);
        }
        assert_eq!(
            auth.close_window(20_000_000),
            vec![(v.clone(), path(2))],
            "the faster path must take authority"
        );
    }

    /// Identical trades pair in order rather than colliding — the gap in the implementation this
    /// borrows from, which keys on content with no queue.
    #[test]
    fn identical_repeated_trades_pair_in_order() {
        let (mut r, v) = race();
        trade(&mut r, &v, path(1), 1_000);
        trade(&mut r, &v, path(1), 2_000);
        assert_eq!(
            r.pending_len(),
            2,
            "a same-path repeat queues, never self-matches"
        );

        let first = trade(&mut r, &v, path(2), 3_000).expect("pairs the oldest");
        assert_eq!(first.delta_ns, 2_000, "3_000 - 1_000");
        let second = trade(&mut r, &v, path(2), 4_000).expect("pairs the next");
        assert_eq!(second.delta_ns, 2_000, "4_000 - 2_000");
        assert_eq!(r.pending_len(), 0);
    }

    /// A trade only one path ever sent is dropped after the window and attributed, so "seen only on
    /// this path" is observable rather than silent.
    #[test]
    fn an_unmatched_trade_is_evicted_and_attributed() {
        let (mut r, v) = race();
        trade(&mut r, &v, path(1), 1_000);
        assert_eq!(
            r.evict_stale(500_000_000),
            vec![],
            "still inside the window"
        );
        assert_eq!(r.pending_len(), 1);
        assert_eq!(r.evict_stale(1_000_000_001 + 1_000), vec![(path(1), 1)]);
        assert_eq!(r.pending_len(), 0);
    }

    /// A peer copy arriving after the window is not a match: pairing it would report a lead of
    /// seconds and drag the median with it.
    #[test]
    fn a_copy_past_the_window_does_not_match() {
        let (mut r, v) = race();
        trade(&mut r, &v, path(1), 1_000);
        assert_eq!(trade(&mut r, &v, path(2), 2_000_000_000), None);
        assert_eq!(r.pending_len(), 1, "only the late copy is now waiting");
    }

    /// Different trades never match each other, on any one distinguishing field.
    #[test]
    fn distinct_trades_do_not_match() {
        let v = scope();
        let other: Arc<str> = Arc::from("KXETHPERP");
        for (s, px, size, side) in [
            (other, 6_200.0, 150.0, Side::Buy),
            (sym(), 6_201.0, 150.0, Side::Buy),
            (sym(), 6_200.0, 151.0, Side::Buy),
            (sym(), 6_200.0, 150.0, Side::Sell),
        ] {
            let mut r = PathRace::new(1_000_000_000);
            r.on_trade(&v, &sym(), 6_200.0, 150.0, Side::Buy, path(1), 1_000);
            assert_eq!(
                r.on_trade(&v, &s, px, size, side, path(2), 2_000),
                None,
                "sym={s} px={px} size={size} side={side:?} must not pair"
            );
        }
    }

    /// Two **universes of one venue** sharing a symbol and a price never cross-match either. This
    /// matcher is the sole producer of the election's evidence, and that election is per universe: a
    /// pair formed across them hands `observe_matched_lead` a publisher that is a stranger to the
    /// scope it is filed under, minting it into that scope's eight admission slots — and a margin
    /// transfer can then elect a path that serves no `book` in that universe at all.
    #[test]
    fn universes_of_one_venue_do_not_cross_match() {
        let mut r = PathRace::new(1_000_000_000);
        r.on_trade(&scope(), &sym(), 6_200.0, 150.0, Side::Buy, path(1), 1_000);
        assert_eq!(
            r.on_trade(
                &other_scope(),
                &sym(),
                6_200.0,
                150.0,
                Side::Buy,
                path(2),
                2_000
            ),
            None,
            "a trade in another universe must not pair with this one"
        );
        assert_eq!(r.pending_len(), 2, "both are still waiting for a real peer");
    }

    /// The control for the test above: two venues sharing a symbol and a price never cross-match.
    #[test]
    fn venues_do_not_cross_match() {
        let mut r = PathRace::new(1_000_000_000);
        let (a, b): (ScopeKey, ScopeKey) = (scope(), (Arc::from("Other"), Arc::from(CATEGORY)));
        r.on_trade(&a, &sym(), 6_200.0, 150.0, Side::Buy, path(1), 1_000);
        assert_eq!(
            r.on_trade(&b, &sym(), 6_200.0, 150.0, Side::Buy, path(2), 2_000),
            None
        );
    }

    /// The pending set is bounded: the wire is unauthenticated, so a flood of distinct trades must
    /// cost samples rather than memory.
    #[test]
    fn pending_arrivals_are_bounded() {
        let mut r = PathRace::new(u64::MAX); // no time-based eviction, so only the cap can bound it
        let v = scope();
        for i in 0..(MAX_PENDING + 500) {
            r.on_trade(&v, &sym(), i as f64, 150.0, Side::Buy, path(1), 1_000);
        }
        assert_eq!(r.pending_len(), MAX_PENDING);
        assert!(
            r.pending.len() <= MAX_PENDING,
            "the signature map must be bounded too, got {}",
            r.pending.len()
        );
        assert!(
            r.drain_unmatched().is_empty(),
            "the cap path is overload, not a one-sided print"
        );
    }

    /// Eviction and matching must keep the two structures in step, or `pending_len` drifts from what
    /// is really held and the cap stops bounding anything.
    #[test]
    fn the_order_index_stays_consistent_with_the_queues() {
        let mut r = PathRace::new(1_000_000_000);
        let v = scope();
        for i in 0..50 {
            r.on_trade(
                &v,
                &sym(),
                i as f64,
                150.0,
                Side::Buy,
                path(1),
                1_000 + i as u64,
            );
        }
        // Match half of them out of the middle of the index.
        for i in 0..25 {
            assert!(r
                .on_trade(&v, &sym(), i as f64, 150.0, Side::Buy, path(2), 5_000)
                .is_some());
        }
        assert_eq!(r.pending_len(), 25);
        let queued: usize = r.pending.values().map(|q| q.len()).sum();
        assert_eq!(queued, r.pending_len(), "order index and queues agree");
        // And the survivors still evict cleanly, tombstones included.
        r.evict_stale(u64::MAX / 2);
        assert_eq!(r.pending_len(), 0);
        assert!(r.pending.is_empty(), "no empty queues left behind");
        assert!(r.order.is_empty(), "no tombstones left behind");
    }

    /// One path repeating one signature must not lengthen its queue: it is scanned per call under the
    /// shared arbiter lock, so an unbounded queue is a stall for every venue, not just a lost sample.
    #[test]
    fn a_repeated_signature_cannot_grow_its_queue() {
        let mut r = PathRace::new(u64::MAX); // no time-based eviction
        let v = scope();
        for i in 0..(MAX_PER_SIGNATURE * 100) {
            trade(&mut r, &v, path(1), 1_000 + i as u64);
        }
        assert_eq!(r.pending.len(), 1, "one signature");
        assert_eq!(r.pending_len(), MAX_PER_SIGNATURE);
        assert_eq!(
            r.pending.values().next().map(|q| q.len()),
            Some(MAX_PER_SIGNATURE)
        );
        assert!(
            r.drain_unmatched().is_empty(),
            "the cap path is overload, not a one-sided print"
        );
        // The retained arrivals are the newest, and still pair oldest-first.
        let m = trade(&mut r, &v, path(2), 9_000).expect("pairs the oldest retained");
        assert_eq!(
            m.delta_ns,
            9_000 - (1_000 + (MAX_PER_SIGNATURE * 99) as u64)
        );
    }

    /// The counter map is keyed on a spoofable source IP address, so it must refuse new keys past its cap
    /// while still counting the keys it already holds.
    #[test]
    fn the_unmatched_counter_map_is_bounded() {
        let mut r = PathRace::new(0); // every arrival is stale by the next call
        let v = scope();
        for n in 0..(MAX_UNMATCHED_KEYS + 8) {
            r.on_trade(
                &v,
                &sym(),
                n as f64,
                150.0,
                Side::Buy,
                path_n(n),
                1_000 + n as u64,
            );
        }
        // A second eviction for a path already in the map, which must still be counted.
        r.on_trade(&v, &sym(), 0.0, 150.0, Side::Buy, path_n(0), 9_000_000);
        r.evict_stale(9_000_001);

        let got: HashMap<_, _> = r.drain_unmatched().into_iter().collect();
        assert_eq!(
            got.len(),
            MAX_UNMATCHED_KEYS,
            "new keys refused past the cap"
        );
        assert_eq!(
            got.get(&(v, path_n(0))),
            Some(&2),
            "a held key keeps counting"
        );
    }

    /// Evictions are attributed per `(scope, path)`, and a drain leaves the accumulator empty.
    #[test]
    fn unmatched_evictions_are_attributed_per_scope_and_path() {
        let mut r = PathRace::new(1_000_000_000);
        let (a, b): (ScopeKey, ScopeKey) = (scope(), (Arc::from("Other"), Arc::from(CATEGORY)));
        r.on_trade(&a, &sym(), 6_200.0, 150.0, Side::Buy, path(1), 1_000);
        r.on_trade(&a, &sym(), 6_201.0, 150.0, Side::Buy, path(1), 1_000);
        r.on_trade(&b, &sym(), 6_200.0, 150.0, Side::Buy, path(2), 1_000);
        r.evict_stale(2_000_000_000);

        let got: HashMap<_, _> = r.drain_unmatched().into_iter().collect();
        assert_eq!(got.len(), 2);
        assert_eq!(got.get(&(a, path(1))), Some(&2));
        assert_eq!(got.get(&(b, path(2))), Some(&1));
        assert!(r.drain_unmatched().is_empty(), "the drain is destructive");
    }

    /// A matched pair must not read as a one-sided print, or the counter measures trade volume.
    #[test]
    fn a_matched_pair_counts_as_nothing_unmatched() {
        let (mut r, v) = race();
        trade(&mut r, &v, path(1), 1_000);
        trade(&mut r, &v, path(2), 11_000).expect("the pair matches");
        r.evict_stale(u64::MAX / 2);
        assert!(r.drain_unmatched().is_empty());
    }

    /// `on_trade` evicts internally, so a burst of trades performs most of the evictions. Those must
    /// survive to the next drain rather than being discarded as they were.
    #[test]
    fn evictions_during_on_trade_still_reach_the_drain() {
        let (mut r, v) = race();
        trade(&mut r, &v, path(1), 1_000);
        // A later, distinct trade: this call evicts the stale arrival before queueing its own.
        r.on_trade(
            &v,
            &sym(),
            6_300.0,
            150.0,
            Side::Buy,
            path(2),
            2_000_000_000,
        );
        assert_eq!(r.pending_len(), 1, "only the new arrival is waiting");

        let got: HashMap<_, _> = r.drain_unmatched().into_iter().collect();
        assert_eq!(got.get(&(v, path(1))), Some(&1));
    }
}
