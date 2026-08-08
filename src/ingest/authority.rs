//! Single-arm authority for venues whose redundant publishers have no comparable clock.
//!
//! `Coordinated` arbitration (`arbiter::StalenessFloor`) buckets two copies on a venue-assigned
//! coordinate and re-latches the leader every tick. That needs a coordinate the arms share. A
//! FIX-sourced arm and a WS-sourced arm of one venue have none, and a content hash cannot
//! substitute: the cross-arm-common fields of a *level update* reduce to `(side, price, quantity)`,
//! which recurs constantly on a coarse bounded price grid.
//!
//! So: exactly one arm is authoritative and its stream is published verbatim; the other is ingested
//! and discarded. **Authority is per instrument on the wire, never per level** — interleaving two
//! arms' deltas corrupts the book while every per-arm sequence check still passes.
//!
//! # What is scoped where
//!
//! Latency is a property of an *arm*, not of a market: every message from a source IP is evidence
//! about that arm, whatever market carried it. The three transfer triggers therefore sit at two
//! grains, and confusing them produces the two failures this module is shaped to avoid.
//!
//! * **Speed** — per arm, venue-wide. Pooled evidence, one verdict per venue per window
//!   ([`StickyAuthority::close_window`]). Splitting it per market splits the evidence as finely as it
//!   can be split, and across 1,200 sleepy markets that is no evidence at all.
//! * **Silence** — per arm, venue-wide. An arm is live or it is not. Scoping silence per market is a
//!   *bug*: a market quieter than `leader_timeout` makes every challenger message read as leader
//!   silence, so authority ping-pongs on every update and re-baselines the consumer's book each time.
//! * **Health** — per market, and it has to stay there. An arm can be `Synced` on 1,200 markets and
//!   `Gap` on one. A venue-wide health rule would either hand the whole venue over for one bad book
//!   or keep serving that book, and under incremental output a lost level does not self-heal until
//!   the next snapshot.
//!
//! Health is an **override**, computed from health and the venue leader rather than stored: a market
//! whose venue leader is unhealthy is served by a healthy arm, and reverts on its own when the
//! leader's book recovers. Nothing to unwind, and no second authority to keep in sync.
//!
//! # Bounding
//!
//! The multicast source is unauthenticated and a [`Publisher`] is a spoofable source IP, so **only
//! arms holding a metric ordinal are eligible**: past [`MAX_LABELLED_ARMS`] a publisher is neither
//! recorded nor ever authoritative. The cap that existed to bound the metric label set bounds
//! admission too, which keeps the per-arm state from growing under a forged flood. Real deployments
//! run two arms.
//!
//! What stays keyed on wire-supplied `(channel_id, instrument_id)` is per-market health and the
//! last-admitted arm. A caller only reports markets that resolve to a definition and a book, but that
//! bounds only what it holds *live* — a churning id space would still grow this map for the life of
//! the process — so [`MAX_TRACKED_MARKETS`] bounds it here.
//!
//! A caller holding its own per-market state must drop it through [`StickyAuthority::forget_market`],
//! and in the same step. Dropping one half is worse than dropping neither: `last_admitted` is what
//! tells the caller a market changed hands, so an entry gone from here while the caller still holds
//! that market's book makes the next arm's deltas read as a continuation of the previous arm's.

use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::Arc,
};

use crate::ingest::arbiter::{Admit, Publisher};

/// The published market key: venue plus the wire identity pair.
pub type MarketKey = (Arc<str>, u32, u32);

/// Cap on distinct arms per venue. Doubles as the admission bound — see the module doc.
const MAX_LABELLED_ARMS: usize = 8;

const ARM_LABELS: [&str; MAX_LABELLED_ARMS] = [
    "arm0", "arm1", "arm2", "arm3", "arm4", "arm5", "arm6", "arm7",
];

/// Returned for an arm past [`MAX_LABELLED_ARMS`]: unlabelled, unrecorded, never authoritative.
pub const OTHER_ARM: &str = "other";

/// Cap on `(venue, channel, instrument)` markets whose per-market state is retained. The processor's
/// book cap bounds only the books it holds *live*; an id space that churns (a venue relisting markets
/// daily, or a forged stream minting ids) would otherwise grow this map for the life of the process.
/// Least-recently-inserted eviction: losing an entry costs at most one stale health opinion, which
/// that arm's next report re-establishes.
const MAX_TRACKED_MARKETS: usize = 1 << 16;

/// Cap on matched-lead samples retained per arm per window. Overflow is head-biased — an arm busy
/// enough to hit it decides on the window's first 4096 matches — so the cap sits well above what the
/// margin test needs rather than being a tight bound.
const MAX_WINDOW_SAMPLES: usize = 4096;

/// Tunables for [`StickyAuthority`], all CLI-settable (`--arb-*`).
#[derive(Debug, Clone, Copy)]
pub struct AuthorityConfig {
    pub leader_timeout_ns: u64,
    pub sample_interval_ns: u64,
    /// The challenger must beat the leader by at least this much on median to transfer.
    pub transfer_margin_ns: u64,
    /// ...and lead in at least this fraction of its own matched samples.
    pub transfer_win_rate: f64,
    /// Matched samples an arm needs before its window is judged at all. Without a floor a single
    /// match transfers a venue, which is the opposite of sticky.
    pub min_window_samples: usize,
}

impl AuthorityConfig {
    /// The defaults, so an [`Arbiter`](crate::ingest::arbiter::Arbiter) built without a config still
    /// gates books rather than silently interleaving arms.
    ///
    /// **`main.rs`'s `--arb-*` clap defaults are derived from this**, not written out beside it — two
    /// hand-maintained copies of five numbers is exactly how a documented default drifts from the one
    /// a test-built arbiter actually uses.
    pub const DEFAULT: Self = Self {
        leader_timeout_ns: 2_000_000_000,
        sample_interval_ns: 300_000_000_000,
        transfer_margin_ns: 1_000_000,
        transfer_win_rate: 0.8,
        min_window_samples: 32,
    };
}

impl Default for AuthorityConfig {
    fn default() -> Self {
        Self::DEFAULT
    }
}

/// Per-arm state within a venue.
#[derive(Default)]
struct Arm {
    /// Arrival of this arm's most recent message anywhere in the venue — the silence clock.
    last_seen_ns: u64,
    /// Signed matched leads for the open window: negative when this arm beat the leader.
    samples: Vec<i64>,
}

/// Per-venue authority.
struct VenueState {
    leader: Publisher,
    arms: HashMap<Publisher, Arm>,
    window_opened_ns: u64,
}

impl VenueState {
    /// Hand the venue to `leader`. Every sample in the open window was measured against the outgoing
    /// leader, so a handover always starts a new window — judging a new leader on its predecessor's
    /// evidence is how a transfer gets undone at the next close.
    fn take_authority(&mut self, leader: Publisher, arrival_ns: u64) {
        self.leader = leader;
        self.window_opened_ns = arrival_ns;
        for arm in self.arms.values_mut() {
            arm.samples.clear();
        }
    }
}

/// Per-market state. Health and the last-admitted arm only; authority itself is venue-wide.
#[derive(Default)]
struct MarketState {
    /// Arms known unhealthy here (`gap`/`awaiting-snapshot`). Absent means healthy, so a market whose
    /// processor does not report health still gets served.
    unhealthy: HashSet<Publisher>,
    /// Who was last admitted here, so `opened_tick` marks a real change of served arm rather than
    /// every leader message.
    last_admitted: Option<Publisher>,
}

pub struct StickyAuthority {
    venues: HashMap<Arc<str>, VenueState>,
    markets: HashMap<MarketKey, MarketState>,
    /// Insertion order of `markets` keys, oldest at the front, for the [`MAX_TRACKED_MARKETS`]
    /// eviction. Bounding *this* rather than `markets` is what lets [`Self::forget_market`] leave its
    /// entry behind: a stale one costs a no-op pop, and the queue can still never outgrow the cap.
    market_order: VecDeque<MarketKey>,
    /// Arm ordinals per venue. Nested rather than keyed on `(String, Publisher)` so the per-message
    /// lookup borrows the venue instead of allocating one.
    ordinals: HashMap<Arc<str>, HashMap<Publisher, &'static str>>,
    cfg: AuthorityConfig,
}

impl StickyAuthority {
    pub fn new(cfg: AuthorityConfig) -> Self {
        Self {
            venues: HashMap::new(),
            markets: HashMap::new(),
            market_order: VecDeque::new(),
            ordinals: HashMap::new(),
            cfg,
        }
    }

    /// Re-apply the tunables, keeping every elected leader and health flag. The bridge calls this once
    /// at startup; replacing the whole authority instead would blank arbitration state mid-run.
    pub fn set_config(&mut self, cfg: AuthorityConfig) {
        self.cfg = cfg;
    }

    /// Per-market state for `key`, bounded by [`MAX_TRACKED_MARKETS`] with least-recently-inserted
    /// eviction — the market key is wire-supplied, so every path that creates one comes through here.
    fn market_mut(&mut self, key: &MarketKey) -> &mut MarketState {
        if !self.markets.contains_key(key) {
            while self.market_order.len() >= MAX_TRACKED_MARKETS {
                match self.market_order.pop_front() {
                    Some(old) => {
                        self.markets.remove(&old);
                    }
                    None => break,
                }
            }
            self.market_order.push_back(key.clone());
        }
        self.markets.entry(key.clone()).or_default()
    }

    /// Record an arm's book health for one market. `false` means `gap`/`awaiting-snapshot`; the
    /// processor calls this on every state transition.
    pub fn set_health(&mut self, key: &MarketKey, publisher: Publisher, healthy: bool) {
        // Same eligibility bound as `admit`: an ineligible arm (past the labelled cap — the source IP
        // is spoofable) is never authoritative, so it must not enter per-market state either.
        if self.arm_ordinal(&key.0, publisher) == OTHER_ARM {
            return;
        }
        let m = self.market_mut(key);
        if healthy {
            m.unhealthy.remove(&publisher);
        } else {
            m.unhealthy.insert(publisher);
        }
    }

    pub(crate) fn healthy(&self, key: &MarketKey, publisher: Publisher) -> bool {
        self.markets
            .get(key)
            .is_none_or(|m| !m.unhealthy.contains(&publisher))
    }

    /// The admission decision for one message from `publisher` on `key`.
    ///
    /// `arrival_ns` is the host receive clock (`recv_ts_ns`) and must be the same clock
    /// [`Self::close_window`] is driven on — it is the silence baseline. Never pass the
    /// `kernel_rx_ts_ns` sentinel: a `0` baseline reads as unbounded silence.
    pub fn admit(
        &mut self,
        key: MarketKey,
        publisher: Publisher,
        arrival_ns: u64,
    ) -> Admit<Publisher> {
        // Ineligible arms enter no map and are never authoritative (see the module doc).
        if self.arm_ordinal(&key.0, publisher) == OTHER_ARM {
            return Admit::Dropped;
        }
        let timeout = self.cfg.leader_timeout_ns;

        let Some(venue) = self.venues.get_mut(&key.0) else {
            // No dark start: the first eligible arm to deliver is provisionally authoritative.
            let arms = HashMap::from([(
                publisher,
                Arm {
                    last_seen_ns: arrival_ns,
                    samples: Vec::new(),
                },
            )]);
            self.venues.insert(
                key.0.clone(),
                VenueState {
                    leader: publisher,
                    arms,
                    window_opened_ns: arrival_ns,
                },
            );
            self.market_mut(&key).last_admitted = Some(publisher);
            return Admit::Emitted { opened_tick: true };
        };
        venue.arms.entry(publisher).or_default().last_seen_ns = arrival_ns;
        // Silence is measured against the leader's last message ANYWHERE in the venue, not against
        // this market's: a quiet market is not a dead arm.
        let leader_last = venue.arms.get(&venue.leader).map_or(0, |a| a.last_seen_ns);
        if venue.leader != publisher && arrival_ns.saturating_sub(leader_last) > timeout {
            venue.take_authority(publisher, arrival_ns);
        }
        // Then the per-market health override. One definition of "who serves this market", shared
        // with `leader_of` and the gauge — computing it a second time here is how the unhealthy
        // leader ends up re-authorising itself when it is the arm sending.
        if self.serving(&key) != Some(publisher) {
            return Admit::Dropped;
        }
        let m = self.market_mut(&key);
        let opened_tick = m.last_admitted != Some(publisher);
        m.last_admitted = Some(publisher);
        Admit::Emitted { opened_tick }
    }

    /// Force venue authority, returning whether it moved. The margin path; silence goes through
    /// [`Self::admit`] and health is an override rather than a transfer.
    pub fn transfer_venue_to(&mut self, venue: &str, to: Publisher, at_ns: u64) -> bool {
        match self.venues.get_mut(venue) {
            Some(v) if v.leader != to => {
                v.take_authority(to, at_ns);
                true
            }
            _ => false,
        }
    }

    /// A stable, bounded metric label for an arm within a venue, so a spoofable source IP never
    /// becomes a label value. Past the cap returns [`OTHER_ARM`] and records nothing — which is also
    /// what makes that arm ineligible. The mapping is logged once, on first sight.
    pub fn arm_ordinal(&mut self, venue: &str, publisher: Publisher) -> &'static str {
        if let Some(&label) = self.ordinals.get(venue).and_then(|m| m.get(&publisher)) {
            return label;
        }
        let arms = match self.ordinals.get_mut(venue) {
            Some(arms) => arms,
            None => self.ordinals.entry(Arc::from(venue)).or_default(),
        };
        let Some(&label) = ARM_LABELS.get(arms.len()) else {
            return OTHER_ARM;
        };
        arms.insert(publisher, label);
        tracing::info!(venue, arm = label, ?publisher, "arbitration arm registered");
        label
    }

    /// An arm's metric label, minting nothing — [`Self::arm_ordinal`] both labels *and* admits, so it
    /// must only be reached from a path that has applied the eligibility rule. A metric label is not
    /// one: a publisher can reach the counters without ever having been admitted, and registering it
    /// there would spend the venue's admission budget on a source that never serves a book.
    pub fn arm_label(&self, venue: &str, publisher: Publisher) -> &'static str {
        self.ordinals
            .get(venue)
            .and_then(|m| m.get(&publisher))
            .copied()
            .unwrap_or(OTHER_ARM)
    }

    /// The venue-wide leader, before any per-market health override.
    pub fn venue_leader(&self, venue: &str) -> Option<Publisher> {
        self.venues.get(venue).map(|v| v.leader)
    }

    /// The arm actually serving one market: the venue leader, or a healthy arm overriding it when the
    /// leader's book here is not. `None` only when the venue has no leader yet.
    ///
    /// The alternative is chosen by most-recently-live rather than by map order, so two arms cannot
    /// be picked differently on two calls with the same state.
    fn serving(&self, key: &MarketKey) -> Option<Publisher> {
        let leader = self.venue_leader(&key.0)?;
        if self.healthy(key, leader) {
            return Some(leader);
        }
        let arms = &self.venues.get(&key.0)?.arms;
        Some(
            arms.iter()
                .filter(|(a, _)| **a != leader && self.healthy(key, **a))
                // The ordinal breaks a `last_seen_ns` tie: without it two tying arms are ordered by
                // `HashMap` iteration and could alternate as the override, interleaving their deltas.
                .max_by_key(|&(&a, arm)| (arm.last_seen_ns, self.arm_label(&key.0, a)))
                .map_or(leader, |(a, _)| *a),
        )
    }

    /// The arm actually serving one market — the venue leader, or a healthy arm overriding it.
    pub fn leader_of(&self, key: &MarketKey) -> Option<Publisher> {
        self.serving(key)
    }

    /// Whether `venue` has recorded `arm` at all — true exactly for the arms [`Self::admit`] found
    /// eligible, so a caller can bound its own per-arm state on the same rule without paying for (or
    /// re-deriving) the ordinal.
    pub fn tracks_arm(&self, venue: &str, arm: Publisher) -> bool {
        self.venues
            .get(venue)
            .is_some_and(|v| v.arms.contains_key(&arm))
    }

    /// The arm last admitted for one market — who the consumer's book state came from. A change of
    /// this across an [`Self::admit`] is what obliges the caller to re-baseline that market.
    pub fn last_admitted(&self, key: &MarketKey) -> Option<Publisher> {
        self.markets.get(key)?.last_admitted
    }

    /// Every known `(venue, arm)` with the market count it currently serves — the `dz_arm_markets_held`
    /// input. Arms serving nothing are reported as `0`, so a displaced arm's gauge falls instead of
    /// going stale.
    ///
    /// **O(markets + arms)**, resolving each market's serving arm exactly once: the metrics tick runs
    /// this under the shared arbiter lock, so a per-arm rescan of every market would stall ingest.
    pub fn markets_held_all(&self) -> Vec<(Arc<str>, Publisher, usize)> {
        let mut held: HashMap<(Arc<str>, Publisher), usize> = self
            .venues
            .iter()
            .flat_map(|(venue, v)| v.arms.keys().map(|&a| ((venue.clone(), a), 0)))
            .collect();
        for key in self.markets.keys() {
            if let Some(p) = self.leader_of(key) {
                *held.entry((key.0.clone(), p)).or_default() += 1;
            }
        }
        held.into_iter().map(|((v, p), n)| (v, p, n)).collect()
    }

    /// Drop every trace of one market. The caller's own per-market state and this map must be dropped
    /// together: `last_admitted` is what tells the caller a market changed hands, so keeping one
    /// without the other either re-baselines a consumer needlessly or — worse — not at all.
    pub fn forget_market(&mut self, key: &MarketKey) {
        self.markets.remove(key);
    }

    /// Record one matched cross-arm lead for `arm` on `venue`: **negative** when `arm` beat the
    /// authoritative arm's copy of the same event. Pooled per arm across every market, because
    /// latency is an arm property.
    ///
    /// The caller pairs the two arms' copies of the same **trade** and diffs its own receive clock.
    /// Never derive this from `Admit::Contest`'s `lead_ns`: that is inter-arm *phase* — the interval
    /// to the leader's previous, unrelated message — and is structurally non-negative, so a
    /// challenger could never win.
    pub fn observe_matched_lead(&mut self, venue: &str, arm: Publisher, lead_ns: i64) {
        // Eligibility first, so an entry created here is bounded exactly as `admit`'s is.
        if self.arm_ordinal(venue, arm) == OTHER_ARM {
            return;
        }
        let Some(v) = self.venues.get_mut(venue) else {
            return;
        };
        if arm == v.leader {
            return; // the leader does not compete with itself
        }
        // `or_default`, not `get_mut`: a matched lead is evidence about an arm whether or not
        // `admit` has seen it yet, and silently discarding it would leave the sampler dependent on
        // the order the two paths happen to fire in.
        let a = v.arms.entry(arm).or_default();
        if a.samples.len() < MAX_WINDOW_SAMPLES {
            a.samples.push(lead_ns);
        }
    }

    /// Close every elapsed sampling window, transferring venue authority where a challenger cleared
    /// every condition. Returns the venues that moved, so the caller counts
    /// `dz_arm_authority_transfers_total{reason="margin"}`. `now_ns` is on [`Self::admit`]'s clock.
    pub fn close_window(&mut self, now_ns: u64) -> Vec<(Arc<str>, Publisher)> {
        // Saturate rather than cast: a `transfer_margin_ns` past `i64::MAX` would wrap negative and
        // invert both conditions, making every window transfer.
        let margin = i64::try_from(self.cfg.transfer_margin_ns).unwrap_or(i64::MAX);
        let cfg = self.cfg;
        let mut moved = Vec::new();
        for (venue, v) in self.venues.iter_mut() {
            if now_ns.saturating_sub(v.window_opened_ns) < cfg.sample_interval_ns {
                continue;
            }
            let winner = best_challenger(&v.arms, v.leader, margin, &cfg);
            for arm in v.arms.values_mut() {
                arm.samples.clear();
            }
            v.window_opened_ns = now_ns;
            if let Some(c) = winner {
                moved.push((venue.clone(), c));
            }
        }
        for (venue, c) in &moved {
            self.transfer_venue_to(venue, *c, now_ns);
        }
        moved
    }
}

/// The challenger that beat the leader by at least `margin` on median AND led at least
/// `transfer_win_rate` of its own samples AND supplied at least `min_window_samples` of them.
/// `None` when none cleared all three — the ordinary case, and why authority is sticky.
fn best_challenger(
    arms: &HashMap<Publisher, Arm>,
    leader: Publisher,
    margin: i64,
    cfg: &AuthorityConfig,
) -> Option<Publisher> {
    let mut best: Option<(Publisher, i64)> = None;
    for (&p, arm) in arms {
        if p == leader || arm.samples.len() < cfg.min_window_samples.max(1) {
            continue;
        }
        let wins = arm.samples.iter().filter(|&&l| l < -margin).count();
        if (wins as f64) / (arm.samples.len() as f64) < cfg.transfer_win_rate {
            continue;
        }
        let mut leads = arm.samples.clone();
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

    const VENUE: &str = "Lashay";
    const TIMEOUT: u64 = 2_000_000_000; // 2s

    fn key() -> MarketKey {
        (VENUE.into(), 2, 41)
    }

    fn other_market() -> MarketKey {
        (VENUE.into(), 2, 42)
    }

    /// Silence and health rules only: `u64::MAX` holds every window open so no margin transfer can
    /// fire mid-test.
    fn no_window_cfg() -> AuthorityConfig {
        AuthorityConfig {
            leader_timeout_ns: TIMEOUT,
            sample_interval_ns: u64::MAX,
            transfer_margin_ns: 1_000, // 1us
            transfer_win_rate: 0.8,
            min_window_samples: 5,
        }
    }

    fn cfg() -> AuthorityConfig {
        AuthorityConfig {
            sample_interval_ns: 1_000_000, // 1ms window keeps the tests fast
            ..no_window_cfg()
        }
    }

    // ---- per-market state bounds ----

    /// `markets` is keyed on wire-supplied ids, so a churning or forged id space must not grow it for
    /// the life of the process. Eviction is least-recently-inserted.
    #[test]
    fn tracked_markets_are_bounded() {
        let mut a = StickyAuthority::new(no_window_cfg());
        let flood = MAX_TRACKED_MARKETS + 50;
        for id in 0..flood {
            a.set_health(&(VENUE.into(), 2, id as u32), arm(1), false);
        }
        assert!(
            a.markets.len() <= MAX_TRACKED_MARKETS,
            "market map must stay bounded, got {}",
            a.markets.len()
        );
        assert!(
            a.markets
                .contains_key(&(VENUE.into(), 2, (flood - 1) as u32)),
            "newest retained"
        );
        assert!(
            !a.markets.contains_key(&(VENUE.into(), 2, 0)),
            "oldest evicted"
        );
    }

    /// An arm past the labelled cap is never authoritative, so a health report from one — the source IP
    /// is spoofable — must not enter per-market state either.
    #[test]
    fn an_ineligible_arm_reports_no_health() {
        let mut a = StickyAuthority::new(no_window_cfg());
        for n in 0..MAX_LABELLED_ARMS as u8 {
            assert_ne!(a.arm_ordinal(VENUE, arm(n)), OTHER_ARM);
        }
        let ineligible = arm(MAX_LABELLED_ARMS as u8);
        a.set_health(&key(), ineligible, false);
        assert!(
            a.markets.is_empty(),
            "an ineligible arm mints no market state"
        );
        assert!(
            a.healthy(&key(), ineligible),
            "and is reported healthy by absence, since it can never serve anyway"
        );
    }

    // ---- venue-wide election ----

    /// The first eligible arm to deliver is provisionally authoritative, so there is no dark start.
    #[test]
    fn first_arm_takes_authority() {
        let mut a = StickyAuthority::new(no_window_cfg());
        assert_eq!(
            a.admit(key(), arm(1), 1_000),
            Admit::Emitted { opened_tick: true }
        );
        assert_eq!(a.venue_leader(VENUE), Some(arm(1)));
    }

    /// `opened_tick` marks a change of served arm, not every leader message, so the
    /// `*_ticks_won_total` family keeps meaning "took the key".
    #[test]
    fn leader_keeps_emitting_without_reopening() {
        let mut a = StickyAuthority::new(no_window_cfg());
        a.admit(key(), arm(1), 1_000);
        assert_eq!(
            a.admit(key(), arm(1), 2_000),
            Admit::Emitted { opened_tick: false }
        );
    }

    #[test]
    fn the_non_authoritative_arm_is_dropped() {
        let mut a = StickyAuthority::new(no_window_cfg());
        a.admit(key(), arm(1), 1_000);
        assert_eq!(a.admit(key(), arm(2), 1_400), Admit::Dropped);
        assert_eq!(a.admit(key(), arm(2), 1_500), Admit::Dropped);
    }

    /// Authority is venue-wide: winning it serves every market, including one the leader has never
    /// sent for.
    #[test]
    fn authority_is_venue_wide_not_per_market() {
        let mut a = StickyAuthority::new(no_window_cfg());
        a.admit(key(), arm(1), 1_000);
        assert_eq!(a.admit(other_market(), arm(2), 1_100), Admit::Dropped);
        assert!(a.admit(other_market(), arm(1), 1_200).emitted());
    }

    // ---- silence: venue-wide, and the idle-market bug it fixes ----

    #[test]
    fn silent_leader_times_out() {
        let mut a = StickyAuthority::new(no_window_cfg());
        a.admit(key(), arm(1), 1_000);
        assert!(
            !a.admit(key(), arm(2), 1_000 + TIMEOUT).emitted(),
            "not yet past"
        );
        assert!(a.admit(key(), arm(2), 1_001 + TIMEOUT).emitted());
        assert_eq!(a.venue_leader(VENUE), Some(arm(2)));
    }

    /// **The bug venue-wide silence exists to fix.** With the clock scoped per market it only
    /// advanced when the leader sent *for that market*, so any market quieter than `leader_timeout`
    /// handed authority back and forth on every update. On the live sports feed 93 of 1,239
    /// instruments saw any update at all in 39s, so nearly every update would have been a transfer,
    /// each one re-baselining the consumer's book. The leader staying busy elsewhere in the venue is
    /// what makes it not silent.
    #[test]
    fn an_idle_market_does_not_flap_while_the_leader_is_busy_elsewhere() {
        let mut a = StickyAuthority::new(no_window_cfg());
        let idle = other_market();
        a.admit(key(), arm(1), 1_000);
        let mut transfers = 0;
        // Ten updates on the idle market an hour apart, while arm(1) streams the busy one.
        for i in 1..=10u64 {
            let t = 1_000 + i * 3_600_000_000_000;
            a.admit(key(), arm(1), t - 1); // the leader is alive, on another market
            if a.admit(idle.clone(), arm(2), t).emitted() {
                transfers += 1;
            }
        }
        assert_eq!(transfers, 0, "a quiet market must not transfer authority");
        assert_eq!(a.venue_leader(VENUE), Some(arm(1)));
    }

    /// ...but a leader that goes quiet across the whole venue still yields.
    #[test]
    fn a_leader_silent_venue_wide_still_yields() {
        let mut a = StickyAuthority::new(no_window_cfg());
        a.admit(key(), arm(1), 1_000);
        assert!(a
            .admit(other_market(), arm(2), 1_000 + TIMEOUT * 2)
            .emitted());
        assert_eq!(a.venue_leader(VENUE), Some(arm(2)));
    }

    // ---- health: per market, an override rather than a transfer ----

    /// A market whose venue leader is gapped is served by a healthy arm — that market only. Under
    /// incremental output a lost level does not self-heal until the next snapshot, so holding it
    /// would serve a knowingly-wrong book.
    #[test]
    fn an_unhealthy_leader_yields_one_market_not_the_venue() {
        let mut a = StickyAuthority::new(no_window_cfg());
        a.admit(key(), arm(1), 1_000);
        a.admit(other_market(), arm(1), 1_000);
        a.set_health(&key(), arm(1), false);

        assert!(
            a.admit(key(), arm(2), 1_100).emitted(),
            "the bad market moves"
        );
        assert_eq!(a.admit(key(), arm(1), 1_200), Admit::Dropped);
        // The venue, and every other market, is untouched.
        assert_eq!(a.venue_leader(VENUE), Some(arm(1)));
        assert!(a.admit(other_market(), arm(1), 1_300).emitted());
        assert_eq!(a.admit(other_market(), arm(2), 1_350), Admit::Dropped);
    }

    /// The override reverts on its own when the leader's book recovers — it is computed, not stored.
    #[test]
    fn the_override_reverts_when_the_leaders_book_recovers() {
        let mut a = StickyAuthority::new(no_window_cfg());
        a.admit(key(), arm(1), 1_000);
        a.set_health(&key(), arm(1), false);
        assert!(a.admit(key(), arm(2), 1_100).emitted());
        a.set_health(&key(), arm(1), true);
        assert!(
            a.admit(key(), arm(1), 1_200).emitted(),
            "back to the leader"
        );
        assert_eq!(a.admit(key(), arm(2), 1_250), Admit::Dropped);
    }

    /// An unhealthy challenger must not take over from an unhealthy leader — that flaps between two
    /// broken books, re-baselining the consumer on each flip and fixing nothing.
    #[test]
    fn an_unhealthy_challenger_does_not_get_the_override() {
        let mut a = StickyAuthority::new(no_window_cfg());
        a.admit(key(), arm(1), 1_000);
        a.set_health(&key(), arm(1), false);
        a.set_health(&key(), arm(2), false);
        assert_eq!(a.admit(key(), arm(2), 1_100), Admit::Dropped);
    }

    // ---- arm eligibility and labelling ----

    /// Ordinals are stable per venue, bounded, and never expose a spoofable source IP as a label.
    #[test]
    fn arm_ordinals_are_stable_and_bounded() {
        let mut a = StickyAuthority::new(no_window_cfg());
        assert_eq!(a.arm_ordinal(VENUE, arm(1)), "arm0");
        assert_eq!(a.arm_ordinal(VENUE, arm(2)), "arm1");
        assert_eq!(a.arm_ordinal(VENUE, arm(1)), "arm0", "stable");
        assert_eq!(a.arm_ordinal("Other", arm(9)), "arm0", "per venue");
        for n in 3..=8 {
            a.arm_ordinal(VENUE, arm(n));
        }
        assert_eq!(a.arm_ordinal(VENUE, arm(200)), OTHER_ARM, "cap holds");
    }

    /// Past the cap an arm is not merely unlabelled — it is never authoritative and enters no map, so
    /// a forged-source flood can neither displace a real arm nor grow the per-arm state.
    #[test]
    fn an_arm_past_the_cap_is_never_authoritative() {
        let mut a = StickyAuthority::new(no_window_cfg());
        for n in 1..=8 {
            a.arm_ordinal(VENUE, arm(n));
        }
        assert_eq!(a.admit(key(), arm(200), 1_000), Admit::Dropped);
        assert_eq!(a.venue_leader(VENUE), None, "no venue was created");
    }

    /// The gauge counts the arm actually *serving* each market, so a health override shows as a split
    /// venue rather than as the elected arm still holding everything.
    #[test]
    fn markets_held_counts_the_arm_actually_serving() {
        let mut a = StickyAuthority::new(no_window_cfg());
        a.admit(key(), arm(1), 1_000);
        a.admit(other_market(), arm(1), 1_000);
        assert_eq!(held(&a, arm(1)), 2);
        assert_eq!(held(&a, arm(2)), 0);
        a.set_health(&key(), arm(1), false);
        a.admit(key(), arm(2), 1_100);
        assert_eq!(held(&a, arm(1)), 1);
        assert_eq!(held(&a, arm(2)), 1);
    }

    fn held(a: &StickyAuthority, publisher: Publisher) -> usize {
        a.markets_held_all()
            .into_iter()
            .find(|(v, p, _)| v.as_ref() == VENUE && *p == publisher)
            .map_or(0, |(_, _, n)| n)
    }

    /// A market forgotten by the caller must leave nothing behind: a stale `last_admitted` would make
    /// the next arm's deltas read as a continuation of the arm that used to serve it.
    #[test]
    fn a_forgotten_market_leaves_no_trace() {
        let mut a = StickyAuthority::new(no_window_cfg());
        a.admit(key(), arm(1), 1_000);
        a.admit(other_market(), arm(1), 1_000);
        a.forget_market(&key());
        assert_eq!(a.last_admitted(&key()), None);
        assert_eq!(a.last_admitted(&other_market()), Some(arm(1)));
    }

    /// A label lookup must not admit: the metrics path sees every publisher that sent a trade, so
    /// minting there would spend the venue's eight admission slots on sources that never serve a book.
    #[test]
    fn arm_label_never_registers_an_arm() {
        let mut a = StickyAuthority::new(no_window_cfg());
        assert_eq!(a.arm_label(VENUE, arm(9)), OTHER_ARM);
        assert_eq!(
            a.arm_ordinal(VENUE, arm(1)),
            "arm0",
            "the slot was not taken"
        );
        assert_eq!(a.arm_label(VENUE, arm(1)), "arm0");
    }

    /// The re-baseline trigger: it tracks the arm actually admitted, so a health override registers
    /// as a change even though the venue leader never moved.
    #[test]
    fn last_admitted_tracks_the_served_arm() {
        let mut a = StickyAuthority::new(no_window_cfg());
        assert_eq!(a.last_admitted(&key()), None);
        a.admit(key(), arm(1), 1_000);
        assert_eq!(a.last_admitted(&key()), Some(arm(1)));
        a.set_health(&key(), arm(1), false);
        a.admit(key(), arm(2), 1_100);
        assert_eq!(a.last_admitted(&key()), Some(arm(2)));
        // A dropped copy never becomes the served arm.
        a.admit(key(), arm(1), 1_200);
        assert_eq!(a.last_admitted(&key()), Some(arm(2)));
    }

    /// The gauge sweep reports every known arm, including one serving nothing — a displaced arm whose
    /// series simply stopped being written would read as still holding its pre-transfer markets.
    #[test]
    fn markets_held_all_reports_idle_arms_as_zero() {
        let mut a = StickyAuthority::new(no_window_cfg());
        a.admit(key(), arm(1), 1_000);
        a.admit(other_market(), arm(1), 1_000);
        a.admit(key(), arm(2), 1_100); // dropped, but arm(2) is now known to the venue
        let mut all = a.markets_held_all();
        all.sort_by_key(|(_, p, _)| format!("{p:?}"));
        assert_eq!(
            all,
            vec![(Arc::from(VENUE), arm(1), 2), (Arc::from(VENUE), arm(2), 0)]
        );
    }

    // ---- pooled matched-lead re-election ----

    /// Samples pool across markets, because latency is an arm property. Matches spread over three
    /// markets elect at the venue level.
    #[test]
    fn pooled_samples_across_markets_transfer_the_venue() {
        let mut a = StickyAuthority::new(cfg());
        a.admit(key(), arm(1), 1_000);
        for i in 0..10 {
            let m: MarketKey = (VENUE.into(), 2, 40 + i % 3);
            a.admit(m, arm(1), 1_000);
            a.observe_matched_lead(VENUE, arm(2), -50_000);
        }
        assert_eq!(
            a.close_window(2_000_000),
            vec![(Arc::from(VENUE), arm(2))],
            "a sustained pooled margin transfers"
        );
        assert_eq!(a.venue_leader(VENUE), Some(arm(2)));
    }

    /// The floor is the difference between sticky and raced: a lone fast match is noise.
    #[test]
    fn one_fast_sample_does_not_transfer() {
        let mut a = StickyAuthority::new(cfg());
        a.admit(key(), arm(1), 1_000);
        a.observe_matched_lead(VENUE, arm(2), -5_000_000);
        assert!(a.close_window(2_000_000).is_empty());
        assert_eq!(a.venue_leader(VENUE), Some(arm(1)));
    }

    /// Both conditions are independent and both must hold: a heavy tail cannot carry a transfer.
    #[test]
    fn a_minority_of_wins_does_not_transfer_however_large() {
        let mut a = StickyAuthority::new(cfg());
        a.admit(key(), arm(1), 1_000);
        for i in 0..10 {
            a.observe_matched_lead(VENUE, arm(2), if i < 5 { -9_000_000 } else { 9_000_000 });
        }
        assert!(a.close_window(2_000_000).is_empty(), "50% < 0.8 win rate");
    }

    /// ...nor can a high win count built on sub-margin noise.
    #[test]
    fn winning_within_the_margin_does_not_transfer() {
        let mut a = StickyAuthority::new(cfg());
        a.admit(key(), arm(1), 1_000);
        for _ in 0..10 {
            a.observe_matched_lead(VENUE, arm(2), -500); // inside the 1_000ns margin
        }
        assert!(a.close_window(2_000_000).is_empty());
    }

    #[test]
    fn window_does_not_close_early() {
        let mut a = StickyAuthority::new(cfg());
        a.admit(key(), arm(1), 1_000);
        for _ in 0..10 {
            a.observe_matched_lead(VENUE, arm(2), -50_000);
        }
        assert!(a.close_window(1_500).is_empty(), "interval has not elapsed");
        assert!(!a.close_window(2_000_000).is_empty());
    }

    /// A close clears the evidence, so the next verdict is judged on its own window.
    #[test]
    fn window_close_resets_the_sample_set() {
        let mut a = StickyAuthority::new(cfg());
        a.admit(key(), arm(1), 1_000);
        for _ in 0..10 {
            a.observe_matched_lead(VENUE, arm(2), -50_000);
        }
        assert!(!a.close_window(2_000_000).is_empty());
        // arm(2) leads now; arm(1) needs its own fresh evidence to win it back.
        assert!(a.close_window(4_000_000).is_empty());
    }

    /// A handover restarts the window, so a silence transfer is not undone at the next close on the
    /// displaced arm's evidence.
    #[test]
    fn a_silence_transfer_restarts_the_sampling_window() {
        let mut a = StickyAuthority::new(cfg());
        a.admit(key(), arm(1), 1_000);
        for _ in 0..10 {
            a.observe_matched_lead(VENUE, arm(2), 9_000_000); // arm(1) is faster
        }
        a.admit(key(), arm(2), 1_001 + TIMEOUT); // arm(1) went silent
        assert_eq!(a.venue_leader(VENUE), Some(arm(2)));
        assert!(
            a.close_window(TIMEOUT * 3).is_empty(),
            "pre-handover samples must not hand it straight back"
        );
    }

    /// A margin transfer must not vouch for the new leader's liveness. The silence clock is each
    /// arm's own `last_seen_ns`, advanced only by real messages, so winning on samples does not make
    /// a quiet arm look live — it is still displaced by the next message from an arm that is.
    #[test]
    fn a_margin_transfer_does_not_fake_the_new_leaders_liveness() {
        let mut a = StickyAuthority::new(cfg());
        a.admit(key(), arm(1), 1_000);
        for _ in 0..10 {
            a.observe_matched_lead(VENUE, arm(2), -50_000);
        }
        // arm(2) wins the window on a ticker far past its own last message (it never sent one).
        assert!(!a.close_window(TIMEOUT * 5).is_empty());
        assert_eq!(a.venue_leader(VENUE), Some(arm(2)));
        // arm(1) is live, arm(2) has said nothing: authority comes straight back on silence.
        assert!(a.admit(key(), arm(1), TIMEOUT * 5 + 1).emitted());
        assert_eq!(a.venue_leader(VENUE), Some(arm(1)));
    }

    /// The leader's own samples are never collected: it does not compete with itself, and counting
    /// them would let evidence measured against a displaced arm outvote the challenger.
    #[test]
    fn the_leader_does_not_sample_against_itself() {
        let mut a = StickyAuthority::new(cfg());
        a.admit(key(), arm(1), 1_000);
        for _ in 0..10 {
            a.observe_matched_lead(VENUE, arm(1), -9_000_000);
        }
        assert!(a.close_window(2_000_000).is_empty());
        assert_eq!(a.venue_leader(VENUE), Some(arm(1)));
    }
}
