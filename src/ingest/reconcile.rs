//! Subscription-driven feed reconciler — the single activation authority for the bridge.
//!
//! `connect multicast` runs *after* the bridge starts (and subscriptions can change at runtime), so
//! activation can't be a one-shot startup decision. This reconciler polls the host's subscriptions
//! (`crate::ingest::subscriptions`) every `refresh` interval and diffs the *desired* set of running
//! tasks against what's currently running, spawning newly-subscribed feeds and aborting ones that
//! went away. It owns all three subscription-gated task kinds:
//!
//! - **market-data receivers** — one per enabled `Feed` whose group `code` the host subscribes to;
//! - **the WebSocket sink** — active iff configured (`--ws-bind` non-empty) *and* ≥1 market-data
//!   feed is subscribed (no point serving normalized quotes when none flow);
//! - **the shred forwarder** — sources come from the subscribed `edge-solana-*` groups (or an
//!   explicit `--shred-source` override), restarted when that set changes.
//!
//! Behaviour is **default-on with fail-open**: if the `doublezero` CLI isn't present (running from
//! source), gating falls open to the static always-on set. A transient CLI failure keeps the
//! current activations rather than flapping everything off. `--subscription-gating-disable` forces
//! the static model. Teardown is `JoinHandle::abort()`, which is clean for all three (sockets close
//! on drop → the kernel leaves the multicast group; no locks are held across `.await`).

use std::{
    collections::{HashMap, HashSet},
    hash::Hash,
    net::{SocketAddr, SocketAddrV4},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

use anyhow::Result;
use tokio::{sync::broadcast, task::JoinHandle};
use tracing::{info, warn};

use crate::{
    ingest::{
        arbiter::SharedArbiter,
        feeds::{Feed, FeedKind},
        health::{FeedHealth, SharedFeedHealth, TapeLiveness},
        receiver,
        subscriptions::{self, Detected, HostSubs},
    },
    metrics::metrics,
    model::{BookSnapshot, DepthSnapshot, FeedMessage, InstrumentSnapshot},
    shred::{self, DedupMode, ShredConfig},
};

/// Identity of a market-data **receiver** in the active-task map: one per publisher of a feed.
/// `(venue, kind)` identifies the feed row (unique across `FEEDS`, asserted by
/// `feeds::tests::venue_kind_pairs_are_unique`) and the base port the block within it (unique per
/// feed, asserted by `feeds::tests::publisher_base_ports_unique_within_a_feed`).
pub type FeedKey = (&'static str, FeedKind, u16);

/// Whether a receiver currently owns its venue's `trade` tape. Read once per print by the
/// processors; the reconciler flips it in place so ownership can move **without respawning** the
/// receiver — a respawn would drop a healthy publisher's books and reference data every time a
/// *peer* feed's subscription changed.
pub type TapeOwner = Arc<AtomicBool>;

/// Which feed kind should carry a venue's tape, lowest first; `None` for a kind that never prints.
///
/// A venue's two groups are separately subscription-gated, so a host may hold the market-by-price
/// group and not the top-of-book one. Both rows claim the tape ([`Feed::emit_trades`]); this ranking
/// is what makes exactly one of them serve it at any moment — the invariant the arbiter's
/// `trade_id == 0` bypass rests on. Top-of-book wins when both are up: it is the venue's primary
/// tape, and market-by-price carries prints only as a by-product of the book stream.
fn tape_rank(kind: FeedKind) -> Option<u8> {
    match kind {
        FeedKind::TopOfBook => Some(0),
        FeedKind::MarketByPrice => Some(1),
        FeedKind::MarketByOrder | FeedKind::Midpoint => None,
    }
}

/// Whether a feed kind can ever own a tape — what `feeds::tests` asserts `emit_trades` against, so
/// the rank values stay internal to this module.
pub fn tape_rank_is_some(kind: FeedKind) -> bool {
    tape_rank(kind).is_some()
}

/// The tape-owning feed row per venue over a set of running receivers.
///
/// Ordered `(liveness, rank, base port)`, lowest wins: **liveness before rank**, base port breaking
/// a tie so the result never depends on iteration order. Rank alone would let a subscribed-but-dead
/// row hold the tape indefinitely while its peer decodes prints and drops them — the group being
/// subscribed and a publisher actually sending to it are independent facts, which is the normal
/// state during a rollout.
///
/// Liveness is [`TapeLiveness`], three states rather than a `down` flag, because this ranks over
/// `desired` — which includes rows not yet spawned — while registration only follows a successful
/// socket bind. A row that can never bind (the tunnel IP disappearing between resolve and join, say)
/// returns `Err`, is reaped and respawned every tick without ever registering; treated as live it
/// would hold rank 0 forever and mute the venue's tape while `status` still read healthy off the
/// peer that is actually streaming. `Unregistered` therefore ranks below `Up` — an incumbent keeps
/// the tape until the newcomer really registers — but above `Down`, so a cold start where nothing
/// has bound yet still falls back to rank instead of leaving the venue with no owner.
pub fn tape_owners(
    active: impl IntoIterator<Item = FeedKey>,
    liveness: impl Fn(&FeedKey) -> TapeLiveness,
) -> HashMap<&'static str, FeedKey> {
    let mut best: HashMap<&'static str, ((TapeLiveness, u8, u16), FeedKey)> = HashMap::new();
    for key in active {
        let Some(rank) = tape_rank(key.1) else {
            continue;
        };
        let order = (liveness(&key), rank, key.2);
        match best.get(key.0) {
            Some(&(cur, _)) if cur <= order => {}
            _ => {
                best.insert(key.0, (order, key));
            }
        }
    }
    best.into_iter()
        .map(|(venue, (_, key))| (venue, key))
        .collect()
}

/// Whether this receiver serves its venue's tape. Keyed on `(venue, kind)` and not the base port, so
/// **every** publisher of the owning row emits — collapsing mirrored copies is the arbiter's job.
pub fn owns(owners: &HashMap<&'static str, FeedKey>, key: &FeedKey) -> bool {
    owners.get(key.0).is_some_and(|o| o.1 == key.1)
}

/// Every receiver key a feed contributes - one per publisher.
fn feed_keys(f: &Feed) -> impl Iterator<Item = FeedKey> + '_ {
    f.publishers
        .iter()
        .map(|p| (f.venue, f.kind, p.base_port()))
}

/// Static shred-forwarder parameters (everything except the source set, which the reconciler
/// derives from subscriptions each tick). Validation (sigverify needs an RPC, window > 0) happens
/// once in `main` before this is built.
pub struct ShredParams {
    /// Master opt-out (`--shred-forward-disable`): shred stays off regardless of subscriptions.
    pub disabled: bool,
    /// Explicit `--shred-source` override. When non-empty, bypasses subscription discovery.
    pub explicit_sources: Vec<SocketAddrV4>,
    /// Group code prefix that identifies shred groups (default `edge-solana-`).
    pub code_prefix: String,
    /// Port every discovered shred group is bound on.
    pub port: u16,
    pub forward: Vec<SocketAddr>,
    pub mode: DedupMode,
    pub rpc_url: Option<String>,
    pub dedup_window_slots: u64,
}

/// Everything the reconciler needs: the shared pipeline handles (cloned into each spawned task) and
/// the static config. Built in `main`.
pub struct ReconcilerConfig {
    pub tx: broadcast::Sender<std::sync::Arc<FeedMessage>>,
    pub arbiter: SharedArbiter,
    pub instruments: InstrumentSnapshot,
    pub depth: DepthSnapshot,
    pub books: BookSnapshot,
    /// The `--feed`/`--publisher-port`-selected market-data feeds this process may run (subject to
    /// subscription). Owned rather than `&'static` because `--publisher-port` narrows each row's
    /// publisher list.
    pub enabled: Vec<Feed>,
    pub iface: String,
    pub recv_buf: usize,
    pub refresh: Duration,
    /// Force the static always-on model (skip subscription detection entirely).
    pub gating_disabled: bool,
    /// WS bind address; empty disables the sink outright (never activated).
    pub ws_bind: String,
    pub ws_cfg: crate::sinks::ws::WsConfig,
    pub shred: ShredParams,
}

/// The activation target computed from the current subscriptions.
#[derive(Debug, Default)]
struct Desired {
    feeds: HashSet<FeedKey>,
    ws_on: bool,
    /// Sorted; empty means the shred forwarder should be off.
    shred_sources: Vec<SocketAddrV4>,
}

pub struct Reconciler {
    cfg: ReconcilerConfig,
    /// Each running receiver with the tape flag it reads. Deliberately one map and not two: a
    /// separate `tapes` map would need cleaning up on both the abort path and the reap path, and
    /// missing either leaks a flag for a receiver that is gone.
    active: HashMap<FeedKey, (JoinHandle<Result<()>>, TapeOwner)>,
    ws_task: Option<JoinHandle<Result<()>>>,
    /// The running shred forwarder plus the (sorted) source set it was started with, so a changed
    /// set triggers a restart.
    shred_task: Option<(Vec<SocketAddrV4>, JoinHandle<Result<()>>)>,
    /// Shared receiver liveness, cloned into each receiver task. Venue-level `status` /
    /// `dz_feed_up` are derived from this aggregate, so one wedged publisher never declares its
    /// whole venue down. Each receiver registers and deregisters *itself*
    /// (`receiver::ReceiverRegistration`) — an `abort()` here can be overtaken by the still-running
    /// task's own liveness write, so the reconciler must not deregister on its behalf.
    health: SharedFeedHealth,
    cli_missing_logged: bool,
}

impl Reconciler {
    pub fn new(cfg: ReconcilerConfig) -> Self {
        Self {
            cfg,
            active: HashMap::new(),
            ws_task: None,
            shred_task: None,
            health: std::sync::Arc::new(FeedHealth::new()),
            cli_missing_logged: false,
        }
    }

    /// The poll loop. Never returns; if it ever did (it can't), the process would exit via `main`'s
    /// `select!`. Mirrors `shred::leader`'s refresher shape.
    pub async fn run(mut self) -> Result<()> {
        info!(
            refresh_secs = self.cfg.refresh.as_secs(),
            gating_disabled = self.cfg.gating_disabled,
            feeds = ?self.cfg.enabled.iter().map(|f| (f.venue, f.kind.label(), f.publishers.len())).collect::<Vec<_>>(),
            "subscription reconciler started"
        );
        loop {
            self.tick().await;
            tokio::time::sleep(self.cfg.refresh).await;
        }
    }

    async fn tick(&mut self) {
        // `None` == inconclusive this tick (transient CLI error / task join failure): keep the
        // current activations unchanged rather than tearing everything down on a hiccup.
        let Some(desired) = self.compute_desired().await else {
            return;
        };
        self.reap_finished();
        self.apply_feeds(&desired.feeds);
        self.apply_ws(desired.ws_on).await;
        self.apply_shred(desired.shred_sources);
    }

    async fn compute_desired(&mut self) -> Option<Desired> {
        if self.cfg.gating_disabled {
            return Some(self.static_desired());
        }
        // The group list is only needed to resolve shred-group IPs; skip it when shreds are
        // disabled or explicitly sourced.
        let need_group_ips = !self.cfg.shred.disabled && self.cfg.shred.explicit_sources.is_empty();
        match tokio::task::spawn_blocking(move || subscriptions::detect(need_group_ips)).await {
            Ok(Detected::Ok(subs)) => Some(self.desired_from_subs(&subs)),
            Ok(Detected::CliMissing) => {
                if !self.cli_missing_logged {
                    warn!(
                        "`doublezero` CLI not found; subscription gating falls open \
                         (all selected feeds + WS active; shreds via explicit --shred-source only)"
                    );
                    self.cli_missing_logged = true;
                }
                Some(self.static_desired())
            }
            Ok(Detected::Unavailable) => None,
            Err(e) => {
                warn!(%e, "subscription detect task failed; keeping current activations");
                None
            }
        }
    }

    /// Desired state from a successful subscription read.
    fn desired_from_subs(&self, subs: &HostSubs) -> Desired {
        let feeds: HashSet<FeedKey> = subs
            .market_data_feeds(&self.cfg.enabled)
            .into_iter()
            .flat_map(feed_keys)
            .collect();
        Desired {
            ws_on: !self.cfg.ws_bind.is_empty() && !feeds.is_empty(),
            shred_sources: self.desired_shred_sources(Some(subs)),
            feeds,
        }
    }

    /// Fail-open / gating-disabled desired state: every enabled feed on, WS on if configured, shreds
    /// only via explicit sources (no CLI → no discovery).
    fn static_desired(&self) -> Desired {
        let feeds: HashSet<FeedKey> = self.cfg.enabled.iter().flat_map(feed_keys).collect();
        Desired {
            ws_on: !self.cfg.ws_bind.is_empty() && !feeds.is_empty(),
            shred_sources: self.desired_shred_sources(None),
            feeds,
        }
    }

    fn desired_shred_sources(&self, subs: Option<&HostSubs>) -> Vec<SocketAddrV4> {
        if self.cfg.shred.disabled {
            return Vec::new();
        }
        if !self.cfg.shred.explicit_sources.is_empty() {
            let mut v = self.cfg.shred.explicit_sources.clone();
            v.sort();
            return v;
        }
        match subs {
            Some(s) => s.shred_sources(&self.cfg.shred.code_prefix, self.cfg.shred.port),
            None => Vec::new(),
        }
    }

    /// Drop handles for tasks that exited on their own so a later tick can respawn them if still
    /// desired (self-healing — replaces the old "process exits if any receiver returns").
    fn reap_finished(&mut self) {
        self.active.retain(|k, (h, _)| {
            let done = h.is_finished();
            if done {
                warn!(
                    venue = k.0,
                    kind = k.1.label(),
                    publisher = k.2,
                    "market-data receiver exited; will respawn if still subscribed"
                );
            }
            !done
        });
        if self.ws_task.as_ref().is_some_and(|h| h.is_finished()) {
            warn!("WebSocket sink task exited; will re-activate if still desired");
            self.ws_task = None;
        }
        if self
            .shred_task
            .as_ref()
            .is_some_and(|(_, h)| h.is_finished())
        {
            warn!("shred forwarder task exited; will re-activate if still desired");
            self.shred_task = None;
        }
    }

    fn apply_feeds(&mut self, desired: &HashSet<FeedKey>) {
        let current: HashSet<FeedKey> = self.active.keys().copied().collect();
        let (to_spawn, to_abort) = plan(&current, desired);
        for key in to_abort {
            if let Some((h, tape)) = self.active.remove(&key) {
                // Before the abort, which lands at the task's next await: an outgoing owner that
                // keeps printing while the incoming one is already on doubles the tape, and a zero-id
                // print in that window has nothing downstream to collapse it.
                tape.store(false, Ordering::Relaxed);
                h.abort();
                info!(
                    venue = key.0,
                    kind = key.1.label(),
                    publisher = key.2,
                    "deactivating market-data receiver (no longer subscribed)"
                );
            }
        }
        // Ownership over the post-apply running set, which `desired` already is: the aborts above
        // removed everything outside it and the spawns below add the rest. Published to the
        // survivors *before* the spawn loop — an incumbent that has lost the tape must be switched
        // off before its replacement is switched on, for the same reason as the abort above — and
        // each spawn then starts with the flag it will hold, so activating a feed is not also
        // counted as a tape *change*.
        let owners = tape_owners(desired.iter().copied(), |k| self.health.liveness(k));
        self.publish_tape_owners(&owners);
        for key in to_spawn {
            let (feed, publisher) = self
                .cfg
                .enabled
                .iter()
                .find_map(|f| {
                    f.publishers
                        .iter()
                        .find(|p| (f.venue, f.kind, p.base_port()) == key)
                        .map(|p| (*f, *p))
                })
                .expect("desired feed key came from enabled");
            info!(
                venue = key.0,
                kind = key.1.label(),
                publisher = key.2,
                group = %feed.group,
                mktdata = publisher.ports.mktdata(),
                "activating market-data receiver (subscribed)"
            );
            let tape: TapeOwner = Arc::new(AtomicBool::new(owns(&owners, &key)));
            let h = tokio::spawn(receiver::run_feed(
                feed,
                publisher,
                self.cfg.iface.clone(),
                self.cfg.recv_buf,
                self.cfg.arbiter.clone(),
                self.cfg.instruments.clone(),
                self.cfg.depth.clone(),
                self.health.clone(),
                tape.clone(),
            ));
            self.active.insert(key, (h, tape));
        }
    }

    /// Push the current tape ownership onto every running receiver's flag.
    ///
    /// `Relaxed` throughout: the flag is advisory per-message policy, not a synchronization point,
    /// and the worst case at a subscription boundary is one duplicated or one dropped print.
    fn publish_tape_owners(&self, owners: &HashMap<&'static str, FeedKey>) {
        for (key, (_, tape)) in &self.active {
            let want = owns(owners, key);
            if tape.swap(want, Ordering::Relaxed) != want {
                metrics()
                    .tape_owner_changes
                    .with_label_values(&[key.0])
                    .inc();
                info!(
                    venue = key.0,
                    kind = key.1.label(),
                    publisher = key.2,
                    owns_tape = want,
                    "trade-tape ownership changed"
                );
            }
        }
    }

    async fn apply_ws(&mut self, on: bool) {
        match (on, self.ws_task.is_some()) {
            (true, false) => match crate::sinks::ws::bind(&self.cfg.ws_bind).await {
                Ok(listener) => {
                    info!(bind = %self.cfg.ws_bind, "activating WebSocket sink (market-data feed subscribed)");
                    self.ws_task = Some(tokio::spawn(crate::sinks::ws::serve(
                        listener,
                        self.cfg.tx.clone(),
                        self.cfg.instruments.clone(),
                        self.cfg.depth.clone(),
                        self.cfg.books.clone(),
                        self.cfg.ws_cfg.clone(),
                    )));
                }
                Err(e) => warn!(bind = %self.cfg.ws_bind, %e,
                    "WebSocket sink failed to bind (port in use?); staying off, will retry next reconcile"),
            },
            (false, true) => {
                if let Some(h) = self.ws_task.take() {
                    h.abort();
                    info!("deactivating WebSocket sink (no market-data feed subscribed)");
                }
            }
            _ => {}
        }
    }

    fn apply_shred(&mut self, sources: Vec<SocketAddrV4>) {
        let current = self
            .shred_task
            .as_ref()
            .map(|(s, _)| s.clone())
            .unwrap_or_default();
        if current == sources {
            return; // no change (both sorted)
        }
        if let Some((_, h)) = self.shred_task.take() {
            h.abort();
        }
        if sources.is_empty() {
            info!("no subscribed shred groups; shred forwarder inactive");
            return;
        }
        let cfg = ShredConfig {
            iface: self.cfg.iface.clone(),
            recv_buf: self.cfg.recv_buf,
            sources: sources.clone(),
            forward: self.cfg.shred.forward.clone(),
            mode: self.cfg.shred.mode,
            rpc_url: self.cfg.shred.rpc_url.clone(),
            dedup_window_slots: self.cfg.shred.dedup_window_slots,
        };
        info!(?sources, "activating shred forwarder (subscribed groups)");
        self.shred_task = Some((sources, tokio::spawn(shred::run(cfg))));
    }
}

/// Pure set diff: which keys to spawn (desired − current) and which to abort (current − desired).
/// Extracted so the reconcile decision is unit-testable without spawning tasks.
fn plan<K: Eq + Hash + Clone>(current: &HashSet<K>, desired: &HashSet<K>) -> (Vec<K>, Vec<K>) {
    let to_spawn = desired.difference(current).cloned().collect();
    let to_abort = current.difference(desired).cloned().collect();
    (to_spawn, to_abort)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn set(items: &[&str]) -> HashSet<&'static str> {
        // Leak to get 'static &str for the test set; fine in a test.
        items
            .iter()
            .map(|s| Box::leak(s.to_string().into_boxed_str()) as &'static str)
            .collect()
    }

    fn sorted(mut v: Vec<&str>) -> Vec<String> {
        v.sort();
        v.into_iter().map(String::from).collect()
    }

    #[test]
    fn plan_spawns_new_and_keeps_existing() {
        let current = set(&["a", "b"]);
        let desired = set(&["b", "c"]);
        let (mut to_spawn, mut to_abort) = plan(&current, &desired);
        to_spawn.sort();
        to_abort.sort();
        assert_eq!(sorted(to_spawn), vec!["c"]); // b kept (in both), c is new
        assert_eq!(sorted(to_abort), vec!["a"]); // a removed
    }

    #[test]
    fn plan_no_change_is_empty() {
        let s = set(&["a", "b"]);
        let (to_spawn, to_abort) = plan(&s, &s);
        assert!(to_spawn.is_empty() && to_abort.is_empty());
    }

    #[test]
    fn plan_from_empty_spawns_all() {
        let (to_spawn, to_abort) = plan(&HashSet::new(), &set(&["a", "b"]));
        assert_eq!(to_spawn.len(), 2);
        assert!(to_abort.is_empty());
    }

    #[test]
    fn plan_to_empty_aborts_all() {
        let (to_spawn, to_abort) = plan(&set(&["a", "b"]), &HashSet::new());
        assert!(to_spawn.is_empty());
        assert_eq!(to_abort.len(), 2);
    }

    use crate::ingest::feeds::{ArbitrationMode, FeedPorts, FeedPublisher};

    fn test_feed(publishers: &'static [FeedPublisher]) -> Feed {
        Feed {
            venue: "TestVenue",
            code: "testcode",
            kind: FeedKind::TopOfBook,
            group: std::net::Ipv4Addr::new(233, 84, 178, 15),
            publishers,
            emit_trades: true,
            arbitration: ArbitrationMode::Coordinated,
        }
    }

    /// One task key per publisher, so N mirrored publishers produce N receivers rather than
    /// collapsing into one (the pre-multi-publisher behaviour, which silently ingested only the
    /// first port block).
    #[test]
    fn feed_keys_are_per_publisher() {
        static PUBS: &[FeedPublisher] = &[
            FeedPublisher {
                ports: FeedPorts::TwoPort {
                    mktdata: 9101,
                    refdata: 9102,
                },
            },
            FeedPublisher {
                ports: FeedPorts::TwoPort {
                    mktdata: 9201,
                    refdata: 9202,
                },
            },
        ];
        let feed = test_feed(PUBS);
        let keys: Vec<FeedKey> = feed_keys(&feed).collect();
        assert_eq!(
            keys,
            vec![
                ("TestVenue", FeedKind::TopOfBook, 9101),
                ("TestVenue", FeedKind::TopOfBook, 9201),
            ]
        );
    }

    const TOB: FeedKind = FeedKind::TopOfBook;
    const MBP: FeedKind = FeedKind::MarketByPrice;
    const MBO: FeedKind = FeedKind::MarketByOrder;

    /// The steady state: every row registered and up.
    fn all_live(_: &FeedKey) -> TapeLiveness {
        TapeLiveness::Up
    }

    /// Both of a venue's rows claim the tape, so with both up the ranking must pick one — top of
    /// book, the venue's primary tape.
    #[test]
    fn top_of_book_owns_the_tape_when_both_feeds_run() {
        let owners = tape_owners([("V", TOB, 7576), ("V", MBP, 31000)], all_live);
        assert_eq!(owners.get("V"), Some(&("V", TOB, 7576)));
        assert!(owns(&owners, &("V", TOB, 7576)));
        assert!(!owns(&owners, &("V", MBP, 31000)));
    }

    /// The case the static `emit_trades` rule got wrong: a host subscribed to the market-by-price
    /// group alone still has to serve a tape.
    #[test]
    fn market_by_price_owns_the_tape_alone() {
        let owners = tape_owners([("V", MBP, 31000)], all_live);
        assert!(owns(&owners, &("V", MBP, 31000)));
    }

    #[test]
    fn tape_ownership_is_per_venue() {
        let owners = tape_owners([("A", MBP, 31000), ("B", TOB, 7576)], all_live);
        assert!(owns(&owners, &("A", MBP, 31000)));
        assert!(owns(&owners, &("B", TOB, 7576)));
    }

    /// Market-by-order is depth-only, so a venue carried only by it has no tape owner at all —
    /// rather than one silently minted from a row that never prints.
    #[test]
    fn a_depth_only_venue_has_no_tape_owner() {
        let owners = tape_owners([("V", MBO, 10001)], all_live);
        assert!(owners.is_empty());
        assert!(!owns(&owners, &("V", MBO, 10001)));
    }

    /// Ownership is keyed on `(venue, kind)`, so every publisher of the owning row emits and no
    /// publisher of a peer row does — collapsing the mirrored copies is the arbiter's job.
    #[test]
    fn every_publisher_of_the_owning_feed_emits() {
        let owners = tape_owners(
            [
                ("V", TOB, 7576),
                ("V", TOB, 7676),
                ("V", MBP, 31000),
                ("V", MBP, 31100),
            ],
            all_live,
        );
        assert!(owns(&owners, &("V", TOB, 7576)));
        assert!(owns(&owners, &("V", TOB, 7676)));
        assert!(!owns(&owners, &("V", MBP, 31000)));
        assert!(!owns(&owners, &("V", MBP, 31100)));
    }

    /// The group being subscribed and a publisher actually sending to it are independent facts, so
    /// rank alone would let a dead top-of-book row hold the tape while the market-by-price receiver
    /// decodes prints and drops them. Liveness outranks rank; with everything down it falls back.
    #[test]
    fn a_dead_row_yields_the_tape_to_a_live_peer() {
        let keys = [("V", TOB, 7576), ("V", MBP, 31000)];
        let owners = tape_owners(keys, |k| {
            if k.1 == TOB {
                TapeLiveness::Down
            } else {
                TapeLiveness::Up
            }
        });
        assert!(owns(&owners, &("V", MBP, 31000)));
        assert!(!owns(&owners, &("V", TOB, 7576)));

        let both_down = tape_owners(keys, |_| TapeLiveness::Down);
        assert!(owns(&both_down, &("V", TOB, 7576)), "falls back to rank");
    }

    /// One live publisher makes its row live: liveness is per receiver but ownership is per row.
    #[test]
    fn one_live_publisher_keeps_the_row_owning() {
        let owners = tape_owners(
            [("V", TOB, 7576), ("V", TOB, 7676), ("V", MBP, 31000)],
            |k| {
                if *k == ("V", TOB, 7576) {
                    TapeLiveness::Down
                } else {
                    TapeLiveness::Up
                }
            },
        );
        assert!(owns(&owners, &("V", TOB, 7676)));
        assert!(!owns(&owners, &("V", MBP, 31000)));
    }

    /// **The mute.** `tape_owners` ranks over `desired`, which includes rows not yet spawned, while
    /// registration only happens after every socket binds. A row whose `bind_multicast` /
    /// `join_multicast_v4` fails returns `Err`, is reaped, and respawns every tick **without ever
    /// registering** — so its key never becomes registered-and-down and, ranked as if live, it held
    /// rank 0 forever. Meanwhile `publish_tape_owners` cleared the streaming peer's flag every tick:
    /// no `trade` reached the wire for the venue at all, indefinitely, while `status`/`dz_feed_up`
    /// still read healthy off that peer.
    #[test]
    fn a_never_registered_row_cannot_take_the_tape_from_a_live_peer() {
        let owners = tape_owners([("V", TOB, 7576), ("V", MBP, 31000)], |k| {
            if k.1 == TOB {
                TapeLiveness::Unregistered
            } else {
                TapeLiveness::Up
            }
        });
        assert!(
            owns(&owners, &("V", MBP, 31000)),
            "the streaming row keeps the tape until the newcomer actually registers"
        );
        assert!(!owns(&owners, &("V", TOB, 7576)));
    }

    /// The property the three-state ordering must not break, and why "not registered yet" is not
    /// simply folded into `Down`: at cold start no row has bound its sockets, so every row is
    /// unregistered and the ordering has to fall back to rank. Demoting an unregistered row below a
    /// registered one unconditionally would leave a fresh process with no tape owner at all.
    #[test]
    fn a_cold_start_falls_back_to_rank() {
        let owners = tape_owners([("V", TOB, 7576), ("V", MBP, 31000)], |_| {
            TapeLiveness::Unregistered
        });
        assert!(owns(&owners, &("V", TOB, 7576)));
    }

    /// An incumbent that registered and then went down is worse than a newcomer that has not
    /// reported yet: the incumbent is known not to be delivering, while the newcomer may be about
    /// to. Neither is serving prints, so this only decides which row is holding the flag when data
    /// resumes — but it keeps the ordering a total one.
    #[test]
    fn an_unregistered_row_outranks_a_registered_dead_one() {
        let owners = tape_owners([("V", TOB, 7576), ("V", MBP, 31000)], |k| {
            if k.1 == TOB {
                TapeLiveness::Unregistered
            } else {
                TapeLiveness::Down
            }
        });
        assert!(owns(&owners, &("V", TOB, 7576)));
    }

    /// Distinct publishers of the same feed must not collide in the active-task map.
    #[test]
    fn plan_treats_publishers_as_independent() {
        let current: HashSet<FeedKey> = [("V", FeedKind::TopOfBook, 9101)].into_iter().collect();
        let desired: HashSet<FeedKey> = [
            ("V", FeedKind::TopOfBook, 9101),
            ("V", FeedKind::TopOfBook, 9201),
        ]
        .into_iter()
        .collect();
        let (to_spawn, to_abort) = plan(&current, &desired);
        assert_eq!(to_spawn, vec![("V", FeedKind::TopOfBook, 9201)]);
        assert!(to_abort.is_empty());
    }

    /// Losing the top-of-book subscription must hand the tape to market-by-price **in place**: the
    /// surviving receiver keeps its books and reference data, which a respawn would drop. `ptr_eq`
    /// is the assertion — a respawn mints a new flag — and it is the unit-level form of the live
    /// check that `dz_receiver_up` for that block never blips to 0.
    #[tokio::test]
    async fn losing_top_of_book_moves_the_tape_without_respawning() {
        static TOB_PUB: &[FeedPublisher] = &[FeedPublisher {
            ports: FeedPorts::TwoPort {
                mktdata: 7576,
                refdata: 7577,
            },
        }];
        static MBP_PUB: &[FeedPublisher] = &[FeedPublisher {
            ports: FeedPorts::ThreePort {
                mktdata: 31000,
                refdata: 41000,
                snapshot: 51000,
            },
        }];
        // A venue label used by no other test, so the counter delta below is this test's alone.
        let venue = "TapeFlipVenue";
        let mut r = test_reconciler(vec![
            Feed {
                venue,
                kind: FeedKind::TopOfBook,
                publishers: TOB_PUB,
                ..test_feed(TOB_PUB)
            },
            Feed {
                venue,
                kind: FeedKind::MarketByPrice,
                publishers: MBP_PUB,
                ..test_feed(MBP_PUB)
            },
        ]);
        let mbp_key = (venue, FeedKind::MarketByPrice, 31000u16);
        let changes = metrics().tape_owner_changes.with_label_values(&[venue]);
        let before = changes.get();

        r.apply_feeds(
            &[(venue, FeedKind::TopOfBook, 7576), mbp_key]
                .into_iter()
                .collect(),
        );
        let mbp_tape = r.active[&mbp_key].1.clone();
        assert!(
            !mbp_tape.load(Ordering::Relaxed),
            "top of book owns the tape"
        );

        r.apply_feeds(&[mbp_key].into_iter().collect());
        assert!(
            mbp_tape.load(Ordering::Relaxed),
            "the tape moved to market-by-price"
        );
        assert!(
            Arc::ptr_eq(&mbp_tape, &r.active[&mbp_key].1),
            "the market-by-price receiver was respawned"
        );
        assert_eq!(changes.get() - before, 1);
    }

    /// A `Reconciler` whose spawned receivers are never polled: `apply_feeds` is sync, so the tasks
    /// it creates bind no sockets before the test drops them.
    fn test_reconciler(enabled: Vec<Feed>) -> Reconciler {
        let (tx, _rx) = broadcast::channel(16);
        Reconciler::new(ReconcilerConfig {
            arbiter: Arc::new(std::sync::Mutex::new(crate::ingest::arbiter::Arbiter::new(
                tx.clone(),
                16,
            ))),
            tx,
            instruments: Default::default(),
            depth: Default::default(),
            books: Default::default(),
            enabled,
            iface: "127.0.0.1".into(),
            recv_buf: 1 << 20,
            refresh: Duration::from_secs(30),
            gating_disabled: true,
            ws_bind: String::new(),
            ws_cfg: crate::sinks::ws::WsConfig {
                heartbeat: Duration::from_secs(30),
                idle_timeout: Duration::from_secs(90),
                max_clients: 1,
                max_subs: 1,
                max_inbound_per_min: 1,
                broadcast_capacity: 1,
            },
            shred: ShredParams {
                disabled: true,
                explicit_sources: Vec::new(),
                code_prefix: String::new(),
                port: 0,
                forward: Vec::new(),
                mode: DedupMode::None,
                rpc_url: None,
                dedup_window_slots: 1,
            },
        })
    }
}
