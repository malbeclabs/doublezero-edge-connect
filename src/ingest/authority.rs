//! Single-arm authority for venues whose redundant publishers have no comparable clock.
//!
//! `Coordinated` arbitration (`arbiter::StalenessFloor`) buckets two copies on a venue-assigned
//! coordinate and re-latches the leader every tick. That needs a coordinate the arms share. A
//! FIX-sourced arm and a WS-sourced arm of one venue have none, and a content hash cannot
//! substitute: the cross-arm-common fields of a level update reduce to `(side, price, quantity)`,
//! which recurs constantly on a coarse bounded price grid.
//!
//! So: per market, exactly one arm is authoritative and its stream is published verbatim; the other
//! is ingested and discarded. Authority transfers on a health verdict, on silence, or on a
//! sustained speed margin — never on a single faster message, because flapping authority
//! re-baselines every consumer's book.
//!
//! **Authority is per instrument, never per level.** Interleaving two arms' deltas corrupts the
//! book while every per-arm sequence check still passes.

use std::{collections::HashMap, sync::Arc};

use crate::ingest::arbiter::{Admit, Publisher};

/// The published market key: venue plus the wire identity pair. Deliberately excludes the arm, so
/// both arms resolve to one entry and arbitrate against each other.
pub type MarketKey = (Arc<str>, u32, u32);

/// Cap on distinct arms given a stable metric ordinal per venue. The source IP is unauthenticated
/// and spoofable, so ordinals are handed out first-come and anything past the cap collapses to
/// `"other"` rather than growing the label set. Real deployments run two arms.
const MAX_LABELLED_ARMS: usize = 8;

const ARM_LABELS: [&str; MAX_LABELLED_ARMS] = [
    "arm0", "arm1", "arm2", "arm3", "arm4", "arm5", "arm6", "arm7",
];

/// Tunables for [`StickyAuthority`], all CLI-settable (`--arb-*`). Read the two transfer conditions
/// off `dz_arm_lead_ns` and `dz_arm_authority_transfers_total`: a sustained transfer rate means they
/// are too loose; a leader whose `dz_arm_lead_ns{winner="challenger"}` sits persistently past the
/// margin with no transfer means they are too tight.
#[derive(Debug, Clone, Copy)]
pub struct AuthorityConfig {
    pub leader_timeout_ns: u64,
    pub sample_interval_ns: u64,
    /// The challenger must beat the leader by at least this much on median to transfer.
    pub transfer_margin_ns: u64,
    /// ...and lead in at least this fraction of the window's contested samples.
    pub transfer_win_rate: f64,
}

/// Cap on samples retained per market per window. Already two orders more than the margin test
/// needs, so overflow costs precision on a pathological market, never memory.
const MAX_WINDOW_SAMPLES: usize = 4096;

/// Per-market authority state.
struct Held {
    leader: Publisher,
    /// Arrival of the leader's most recent admitted message — the baseline a challenger's arrival
    /// is measured against, and the silence clock.
    leader_arrival_ns: u64,
    /// Set once a challenger has been reported since the leader's last message, so a challenger
    /// burst yields one contest sample rather than inflating the histogram.
    contest_recorded: bool,
    /// Signed leads for the open window: positive when the leader was ahead of the challenger's
    /// copy, negative when the challenger was.
    samples: Vec<(Publisher, i64)>,
    window_opened_ns: u64,
}

pub struct StickyAuthority {
    held: HashMap<MarketKey, Held>,
    /// Per `(market, arm)` health. Absent means healthy: an arm that has never reported is presumed
    /// usable, so a market whose processor does not track health still elects a leader.
    health: HashMap<(MarketKey, Publisher), bool>,
    ordinals: HashMap<(String, Publisher), &'static str>,
    ordinal_counts: HashMap<String, usize>,
    cfg: AuthorityConfig,
}

impl StickyAuthority {
    pub fn new(cfg: AuthorityConfig) -> Self {
        Self {
            held: HashMap::new(),
            health: HashMap::new(),
            ordinals: HashMap::new(),
            ordinal_counts: HashMap::new(),
            cfg,
        }
    }

    fn healthy(&self, key: &MarketKey, publisher: Publisher) -> bool {
        self.health
            .get(&(key.clone(), publisher))
            .copied()
            .unwrap_or(true)
    }

    /// Record an arm's book health for one market. `false` means `gap`/`awaiting-snapshot`; the
    /// processor calls this on every state transition.
    pub fn set_health(&mut self, key: &MarketKey, publisher: Publisher, healthy: bool) {
        self.health.insert((key.clone(), publisher), healthy);
    }

    /// The admission decision for one message from `publisher` on `key`.
    pub fn admit(
        &mut self,
        key: MarketKey,
        publisher: Publisher,
        arrival_ns: u64,
    ) -> Admit<Publisher> {
        let challenger_healthy = self.healthy(&key, publisher);
        // Computed before the match: `healthy()` borrows `self` immutably, the match arm below
        // holds a mutable borrow.
        let leader_unhealthy = self
            .held
            .get(&key)
            .is_some_and(|h| !self.healthy(&key, h.leader));
        let leader_timeout_ns = self.cfg.leader_timeout_ns;
        match self.held.get_mut(&key) {
            None => {
                // No dark start: the first arm to deliver is provisionally authoritative even
                // before it has reported health.
                self.held.insert(
                    key,
                    Held {
                        leader: publisher,
                        leader_arrival_ns: arrival_ns,
                        contest_recorded: false,
                        samples: Vec::new(),
                        window_opened_ns: arrival_ns,
                    },
                );
                Admit::Emitted { opened_tick: true }
            }
            Some(h) if h.leader == publisher => {
                h.leader_arrival_ns = arrival_ns;
                h.contest_recorded = false;
                Admit::Emitted { opened_tick: false }
            }
            Some(h) => {
                let leader = h.leader;
                let silent = arrival_ns.saturating_sub(h.leader_arrival_ns) > leader_timeout_ns;
                if challenger_healthy && (leader_unhealthy || silent) {
                    h.leader = publisher;
                    h.leader_arrival_ns = arrival_ns;
                    h.contest_recorded = false;
                    return Admit::Emitted { opened_tick: true };
                }
                if h.contest_recorded {
                    Admit::Dropped
                } else {
                    h.contest_recorded = true;
                    Admit::Contest {
                        winner: leader,
                        lead_ns: arrival_ns.saturating_sub(h.leader_arrival_ns),
                    }
                }
            }
        }
    }

    /// Force authority for one market, returning whether it moved. Task 5's margin path; health and
    /// silence transfers go through [`Self::admit`].
    pub fn transfer_to(&mut self, key: &MarketKey, publisher: Publisher, arrival_ns: u64) -> bool {
        match self.held.get_mut(key) {
            Some(h) if h.leader != publisher => {
                h.leader = publisher;
                h.leader_arrival_ns = arrival_ns;
                h.contest_recorded = false;
                true
            }
            _ => false,
        }
    }

    /// A stable, bounded metric label for an arm within a venue, so a spoofable source IP never
    /// becomes a label value. The ordinal-to-IP mapping is logged once, on first sight.
    pub fn arm_ordinal(&mut self, venue: &str, publisher: Publisher) -> &'static str {
        if let Some(&label) = self.ordinals.get(&(venue.to_string(), publisher)) {
            return label;
        }
        let n = self.ordinal_counts.entry(venue.to_string()).or_insert(0);
        let label = ARM_LABELS.get(*n).copied().unwrap_or("other");
        if *n < MAX_LABELLED_ARMS {
            *n += 1;
        }
        self.ordinals.insert((venue.to_string(), publisher), label);
        tracing::info!(venue, arm = label, ?publisher, "arbitration arm registered");
        label
    }

    /// How many of `venue`'s markets `publisher` holds — the gauge an operator reads to see which
    /// arm is live and whether the venue is split.
    pub fn markets_held(&self, venue: &str, publisher: Publisher) -> usize {
        self.held
            .iter()
            .filter(|((v, _, _), h)| v.as_ref() == venue && h.leader == publisher)
            .count()
    }

    /// Record a challenger's arrival against the leader's most recent message. The arbiter calls
    /// this on every `Contest`, so the sampler sees exactly the head-to-heads the histogram reports.
    pub fn observe_challenger(&mut self, key: &MarketKey, challenger: Publisher, arrival_ns: u64) {
        if let Some(h) = self.held.get_mut(key) {
            if h.samples.len() < MAX_WINDOW_SAMPLES {
                let lead = arrival_ns as i64 - h.leader_arrival_ns as i64;
                h.samples.push((challenger, lead));
            }
        }
    }

    /// Close every elapsed sampling window, transferring authority where a challenger cleared BOTH
    /// conditions. Returns the markets that moved, so the caller counts
    /// `dz_arm_authority_transfers_total{reason="margin"}`.
    pub fn close_window(&mut self, now_ns: u64) -> Vec<(MarketKey, Publisher)> {
        let (margin, rate, interval) = (
            self.cfg.transfer_margin_ns as i64,
            self.cfg.transfer_win_rate,
            self.cfg.sample_interval_ns,
        );
        let mut moved = Vec::new();
        for (key, h) in self.held.iter_mut() {
            if now_ns.saturating_sub(h.window_opened_ns) < interval {
                continue;
            }
            let winner = best_challenger(&h.samples, margin, rate);
            h.samples.clear();
            h.window_opened_ns = now_ns;
            if let Some(c) = winner {
                if c != h.leader {
                    h.leader = c;
                    h.leader_arrival_ns = now_ns;
                    h.contest_recorded = false;
                    moved.push((key.clone(), c));
                }
            }
        }
        moved
    }

    /// The current leader for a market, or `None` if none has been elected.
    pub fn leader_of(&self, key: &MarketKey) -> Option<Publisher> {
        self.held.get(key).map(|h| h.leader)
    }
}

/// The challenger that beat the leader by at least `margin` on median AND led at least `rate` of
/// its own samples. `None` when none cleared both — the ordinary case, and why authority is sticky
/// rather than raced.
fn best_challenger(samples: &[(Publisher, i64)], margin: i64, rate: f64) -> Option<Publisher> {
    let mut by_arm: HashMap<Publisher, Vec<i64>> = HashMap::new();
    for &(p, lead) in samples {
        by_arm.entry(p).or_default().push(lead);
    }
    let mut best: Option<(Publisher, i64)> = None;
    for (p, mut leads) in by_arm {
        let wins = leads.iter().filter(|&&l| l < -margin).count();
        if leads.is_empty() || (wins as f64) / (leads.len() as f64) < rate {
            continue;
        }
        leads.sort_unstable();
        let median = leads[leads.len() / 2];
        if median > -margin {
            continue;
        }
        if best.is_none_or(|(_, m)| median < m) {
            best = Some((p, median));
        }
    }
    best.map(|(p, _)| p)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr};

    fn arm(n: u8) -> Publisher {
        Publisher::Edge(IpAddr::V4(Ipv4Addr::new(10, 0, 0, n)))
    }

    fn key() -> MarketKey {
        ("Lashay".into(), 2, 41)
    }

    const TIMEOUT: u64 = 2_000_000_000; // 2s

    /// Health and silence rules only: `u64::MAX` keeps every sampling window open, so no margin
    /// transfer can fire mid-test.
    fn no_window_cfg() -> AuthorityConfig {
        AuthorityConfig {
            leader_timeout_ns: TIMEOUT,
            sample_interval_ns: u64::MAX,
            transfer_margin_ns: 1_000,
            transfer_win_rate: 0.8,
        }
    }

    fn cfg() -> AuthorityConfig {
        AuthorityConfig {
            leader_timeout_ns: TIMEOUT,
            sample_interval_ns: 1_000_000, // 1ms window keeps the tests fast
            transfer_margin_ns: 1_000,     // 1us
            transfer_win_rate: 0.8,
        }
    }

    /// The first arm to deliver a usable book is provisionally authoritative, so there is no dark
    /// start while the election window is open.
    #[test]
    fn first_arm_takes_authority() {
        let mut a = StickyAuthority::new(no_window_cfg());
        assert_eq!(
            a.admit(key(), arm(1), 1_000),
            Admit::Emitted { opened_tick: true }
        );
    }

    /// `opened_tick` marks an authority TRANSFER, not every leader message — so the
    /// `*_ticks_won_total` family keeps meaning "took the key" in both modes.
    #[test]
    fn leader_keeps_emitting_without_reopening() {
        let mut a = StickyAuthority::new(no_window_cfg());
        a.admit(key(), arm(1), 1_000);
        assert_eq!(
            a.admit(key(), arm(1), 2_000),
            Admit::Emitted { opened_tick: false }
        );
    }

    /// The non-authoritative arm is dropped, and the first drop after each leader message reports
    /// the head-to-head lead, so Task 5's sampler has one sample per leader message rather than one
    /// per challenger burst.
    #[test]
    fn challenger_is_dropped_and_reports_the_lead_once() {
        let mut a = StickyAuthority::new(no_window_cfg());
        a.admit(key(), arm(1), 1_000);
        assert_eq!(
            a.admit(key(), arm(2), 1_400),
            Admit::Contest {
                winner: arm(1),
                lead_ns: 400
            }
        );
        assert_eq!(a.admit(key(), arm(2), 1_500), Admit::Dropped);
        a.admit(key(), arm(1), 2_000);
        assert_eq!(
            a.admit(key(), arm(2), 2_300),
            Admit::Contest {
                winner: arm(1),
                lead_ns: 300
            }
        );
    }

    /// A leader in `gap`/`awaiting-snapshot` yields to a healthy challenger: under incremental
    /// output a lost level does not self-heal until the next snapshot, so holding authority through
    /// a gap serves a knowingly-wrong book.
    #[test]
    fn unhealthy_leader_yields_to_a_healthy_challenger() {
        let mut a = StickyAuthority::new(no_window_cfg());
        a.admit(key(), arm(1), 1_000);
        a.set_health(&key(), arm(1), false);
        assert_eq!(
            a.admit(key(), arm(2), 1_100),
            Admit::Emitted { opened_tick: true }
        );
        assert!(
            !a.admit(key(), arm(1), 1_200).emitted(),
            "authority actually moved"
        );
    }

    /// An unhealthy challenger must not take over from an unhealthy leader — that flaps between two
    /// broken arms, re-baselining every consumer on each flip and fixing nothing.
    #[test]
    fn unhealthy_challenger_does_not_take_over() {
        let mut a = StickyAuthority::new(no_window_cfg());
        a.admit(key(), arm(1), 1_000);
        a.set_health(&key(), arm(1), false);
        a.set_health(&key(), arm(2), false);
        assert!(!a.admit(key(), arm(2), 1_100).emitted());
    }

    #[test]
    fn silent_leader_times_out() {
        let mut a = StickyAuthority::new(no_window_cfg());
        a.admit(key(), arm(1), 1_000);
        assert!(
            !a.admit(key(), arm(2), 1_000 + TIMEOUT).emitted(),
            "not yet past"
        );
        assert_eq!(
            a.admit(key(), arm(2), 1_001 + TIMEOUT),
            Admit::Emitted { opened_tick: true }
        );
    }

    #[test]
    fn authority_is_per_market() {
        let mut a = StickyAuthority::new(no_window_cfg());
        let other: MarketKey = ("Lashay".into(), 2, 42);
        a.admit(key(), arm(1), 1_000);
        a.admit(other.clone(), arm(2), 1_000);
        assert!(!a.admit(key(), arm(2), 1_100).emitted());
        assert!(a.admit(other, arm(2), 1_100).emitted());
    }

    /// Arm ordinals are stable per venue, bounded, and never expose a spoofable source IP as a
    /// metric label.
    #[test]
    fn arm_ordinals_are_stable_and_bounded() {
        let mut a = StickyAuthority::new(no_window_cfg());
        assert_eq!(a.arm_ordinal("Lashay", arm(1)), "arm0");
        assert_eq!(a.arm_ordinal("Lashay", arm(2)), "arm1");
        assert_eq!(a.arm_ordinal("Lashay", arm(1)), "arm0", "stable");
        assert_eq!(a.arm_ordinal("Other", arm(9)), "arm0", "per venue");
        for n in 3..=8 {
            a.arm_ordinal("Lashay", arm(n));
        }
        assert_eq!(a.arm_ordinal("Lashay", arm(200)), "other", "cap holds");
    }

    #[test]
    fn markets_held_counts_per_arm() {
        let mut a = StickyAuthority::new(no_window_cfg());
        a.admit(key(), arm(1), 1_000);
        a.admit(("Lashay".into(), 2, 42), arm(1), 1_000);
        a.admit(("Lashay".into(), 2, 43), arm(2), 1_000);
        assert_eq!(a.markets_held("Lashay", arm(1)), 2);
        assert_eq!(a.markets_held("Lashay", arm(2)), 1);
    }

    /// A challenger consistently faster than the margin takes authority when the window closes —
    /// not on its first fast sample.
    #[test]
    fn sustained_margin_transfers_at_window_close() {
        let mut a = StickyAuthority::new(cfg());
        a.admit(key(), arm(1), 0);
        for i in 1..=10u64 {
            let t = i * 100_000;
            a.admit(key(), arm(1), t);
            a.observe_challenger(&key(), arm(2), t.saturating_sub(10_000)); // 10us ahead
        }
        assert_eq!(a.leader_of(&key()), Some(arm(1)), "window still open");
        let moved = a.close_window(1_000_001);
        assert_eq!(moved.len(), 1);
        assert_eq!(a.leader_of(&key()), Some(arm(2)));
    }

    /// One fast sample among slow ones must not transfer — that is the flap the sustained margin
    /// exists to prevent.
    #[test]
    fn one_fast_sample_does_not_transfer() {
        let mut a = StickyAuthority::new(cfg());
        a.admit(key(), arm(1), 0);
        for i in 1..=10u64 {
            let t = i * 100_000;
            a.admit(key(), arm(1), t);
            let challenger = if i == 5 { t - 10_000 } else { t + 10_000 };
            a.observe_challenger(&key(), arm(2), challenger);
        }
        assert!(a.close_window(1_000_001).is_empty());
        assert_eq!(a.leader_of(&key()), Some(arm(1)));
    }

    /// Winning often but only by noise must not transfer either: margin and win rate are
    /// independent conditions and both must hold.
    #[test]
    fn winning_within_the_margin_does_not_transfer() {
        let mut a = StickyAuthority::new(cfg());
        a.admit(key(), arm(1), 0);
        for i in 1..=10u64 {
            let t = i * 100_000;
            a.admit(key(), arm(1), t);
            a.observe_challenger(&key(), arm(2), t - 100); // 100ns < 1us margin
        }
        assert!(a.close_window(1_000_001).is_empty());
        assert_eq!(a.leader_of(&key()), Some(arm(1)));
    }

    /// Closing a window clears its samples, so the next window judges only its own evidence.
    #[test]
    fn window_close_resets_the_sample_set() {
        let mut a = StickyAuthority::new(cfg());
        a.admit(key(), arm(1), 0);
        for i in 1..=10u64 {
            let t = i * 100_000;
            a.admit(key(), arm(1), t);
            a.observe_challenger(&key(), arm(2), t - 10_000);
        }
        a.close_window(1_000_001);
        assert_eq!(a.leader_of(&key()), Some(arm(2)));
        for i in 11..=20u64 {
            let t = i * 100_000;
            a.admit(key(), arm(2), t);
            a.observe_challenger(&key(), arm(1), t - 10_000);
        }
        a.close_window(2_000_002);
        assert_eq!(a.leader_of(&key()), Some(arm(1)));
    }

    /// A window that has not elapsed is left open, samples intact.
    #[test]
    fn window_does_not_close_early() {
        let mut a = StickyAuthority::new(cfg());
        a.admit(key(), arm(1), 0);
        for i in 1..=10u64 {
            let t = i * 10_000;
            a.admit(key(), arm(1), t);
            a.observe_challenger(&key(), arm(2), t - 10_000);
        }
        assert!(
            a.close_window(500_000).is_empty(),
            "half the interval elapsed"
        );
        assert_eq!(a.leader_of(&key()), Some(arm(1)));
        assert_eq!(
            a.close_window(1_000_001).len(),
            1,
            "samples survived the early call"
        );
    }
}
