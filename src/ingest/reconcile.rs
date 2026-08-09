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
        Arc, Mutex,
    },
    time::Duration,
};

use anyhow::Result;
use tokio::{sync::broadcast, task::JoinHandle};
use tracing::{info, warn};

use crate::{
    history::{self, Store},
    ingest::{
        arbiter::SharedArbiter,
        feeds::{Feed, FeedKind},
        floor::ChannelFloor,
        health::{FeedHealth, SharedFeedHealth, TapeLiveness},
        receiver,
        subscriptions::{self, Detected, HostSubs},
    },
    metrics::metrics,
    model::{BookSnapshot, DepthSnapshot, FeedMessage, InstrumentSnapshot},
    shred::{self, DedupMode, ShredConfig},
    sinks::api,
};

/// Identity of a market-data **receiver** in the active-task map: one per publisher of a feed.
/// `(venue, category, kind)` identifies the feed row (unique across `FEEDS`, asserted by
/// `feeds::tests::venue_category_kind_triples_are_unique`) and the base port the block within it
/// (unique per feed, asserted by `feeds::tests::publisher_base_ports_unique_within_a_feed`).
///
/// The category is what separates two disjoint universes riding one Source ID — without it,
/// ownership below is contested across rows that mirror nothing and one universe's tape goes dark.
/// It is also why `(venue, kind)` is no longer an identity: a venue may carry two rows of the same
/// kind, one per universe.
pub type FeedKey = (&'static str, &'static str, FeedKind, u16);

/// The set of rows that mirror one another: a [`FeedKey`]'s `(venue, category)` prefix. One tape
/// owner is elected per universe, never per venue — see [`tape_owners`].
pub type Universe = (&'static str, &'static str);

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

/// The tape-owning feed row per **universe** — one owner per `(venue, category)`, not per venue —
/// over a set of running receivers.
///
/// Scoped by category because rows sharing a venue can carry instrument universes that mirror
/// nothing of each other (one Source ID, two markets). Ranked venue-wide, a top-of-book row on one
/// universe outranks a book row on the other and mutes it entirely: the losing row's receiver
/// decodes every print and drops it, and the venue's other market shows empty candles that look
/// exactly like a market that did not trade.
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
) -> HashMap<Universe, FeedKey> {
    /// What the rows of one universe are ranked on, lowest wins. Named so the ordering is stated
    /// once, in the order the tuple compares in: liveness, then kind rank, then base port.
    type TapeOrder = (TapeLiveness, u8, u16);
    let mut best: HashMap<Universe, (TapeOrder, FeedKey)> = HashMap::new();
    for key in active {
        let Some(rank) = tape_rank(key.2) else {
            continue;
        };
        let order = (liveness(&key), rank, key.3);
        let universe = (key.0, key.1);
        match best.get(&universe) {
            Some(&(cur, _)) if cur <= order => {}
            _ => {
                best.insert(universe, (order, key));
            }
        }
    }
    best.into_iter()
        .map(|(universe, (_, key))| (universe, key))
        .collect()
}

/// Whether this receiver serves its universe's tape. Keyed on `(venue, category, kind)` and not the
/// base port, so **every** publisher of the owning row emits — collapsing mirrored copies is the
/// arbiter's job. The kind comparison alone is not enough once a venue can hold two rows of one
/// kind: it would hand a sports book row the tape because the perps book row owns one.
pub fn owns(owners: &HashMap<Universe, FeedKey>, key: &FeedKey) -> bool {
    owners.get(&(key.0, key.1)).is_some_and(|o| o.2 == key.2)
}

/// Every receiver key a feed contributes — one per publisher the [`ChannelFloor`] admits.
///
/// The floor is an **input** to the desired set, not a second activation authority: it narrows what
/// this function yields and nothing else, so the spawn/abort diff below is unchanged and this module
/// stays the only place that decides what runs. A publisher the floor drops is simply never a
/// desired key, which means its socket is never bound and the kernel discards that channel's traffic
/// before it reaches userspace.
fn feed_keys<'a>(floor: &'a ChannelFloor, f: &'a Feed) -> impl Iterator<Item = FeedKey> + 'a {
    floor
        .publishers_for(f)
        .into_iter()
        .map(|p| (f.venue, f.category, f.kind, p.base_port()))
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
    /// Which channels of each row this process ingests (`--channels`), parsed and validated once in
    /// `main`. Empty by default, which admits every channel of every row.
    pub floor: ChannelFloor,
    pub iface: String,
    pub recv_buf: usize,
    pub refresh: Duration,
    /// Force the static always-on model (skip subscription detection entirely).
    pub gating_disabled: bool,
    /// WS bind address; empty disables the sink outright (never activated).
    pub ws_bind: String,
    pub ws_cfg: crate::sinks::ws::WsConfig,
    /// Query API bind address; empty disables the sink outright (never activated) - mirrors
    /// `ws_bind`.
    pub api_bind: String,
    /// The shared rolling trade history the reconciler's history feeder writes into and the query
    /// API reads from. Built once in `main` (like `instruments`/`depth`/`books`) so the window
    /// survives the sink's own activate/deactivate cycles.
    pub history: Arc<Mutex<Store>>,
    pub shred: ShredParams,
}

/// The activation target computed from the current subscriptions.
#[derive(Debug, Default)]
struct Desired {
    feeds: HashSet<FeedKey>,
    ws_on: bool,
    /// Same condition as `ws_on`, parameterized on `api_bind` instead of `ws_bind`: the query API
    /// comes up only when it's configured *and* at least one market-data feed is subscribed. There's
    /// no point accumulating history for a query path nobody can reach.
    api_on: bool,
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
    /// The query API sink task and the history feeder that keeps its store fed, treated as one
    /// coupled unit by `apply_api`/`reap_finished`: both come up and go down together, since running
    /// one without the other either buffers history nobody can query or serves a query API with a
    /// stalled window.
    api_task: Option<JoinHandle<Result<()>>>,
    history_feeder: Option<JoinHandle<()>>,
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
            api_task: None,
            history_feeder: None,
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
            feeds = ?self.cfg.enabled.iter().map(|f| (f.venue, f.kind.label(), self.cfg.floor.publishers_for(f).len())).collect::<Vec<_>>(),
            channel_floor = ?self.cfg.floor.summary(),
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
        self.apply_api(desired.api_on).await;
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
            .flat_map(|f| feed_keys(&self.cfg.floor, f))
            .collect();
        Desired {
            ws_on: !self.cfg.ws_bind.is_empty() && !feeds.is_empty(),
            api_on: !self.cfg.api_bind.is_empty() && !feeds.is_empty(),
            shred_sources: self.desired_shred_sources(Some(subs)),
            feeds,
        }
    }

    /// Fail-open / gating-disabled desired state: every enabled feed on, WS on if configured, shreds
    /// only via explicit sources (no CLI → no discovery).
    fn static_desired(&self) -> Desired {
        let feeds: HashSet<FeedKey> = self
            .cfg
            .enabled
            .iter()
            .flat_map(|f| feed_keys(&self.cfg.floor, f))
            .collect();
        Desired {
            ws_on: !self.cfg.ws_bind.is_empty() && !feeds.is_empty(),
            api_on: !self.cfg.api_bind.is_empty() && !feeds.is_empty(),
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
                    category = k.1,
                    kind = k.2.label(),
                    publisher = k.3,
                    "market-data receiver exited; will respawn if still subscribed"
                );
            }
            !done
        });
        if self.ws_task.as_ref().is_some_and(|h| h.is_finished()) {
            warn!("WebSocket sink task exited; will re-activate if still desired");
            self.ws_task = None;
        }
        // Reaped as one pair: if either the API sink or its history feeder exited on its own, tear
        // both down so the next `apply_api` respawns a fresh matched pair instead of layering a new
        // feeder alongside one that's still running (which would double-count every trade).
        if self.api_task.as_ref().is_some_and(|h| h.is_finished())
            || self
                .history_feeder
                .as_ref()
                .is_some_and(|h| h.is_finished())
        {
            warn!("query API (or its history feeder) exited; will re-activate if still desired");
            if let Some(h) = self.api_task.take() {
                h.abort();
            }
            if let Some(h) = self.history_feeder.take() {
                h.abort();
            }
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
                    category = key.1,
                    kind = key.2.label(),
                    publisher = key.3,
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
                        .find(|p| (f.venue, f.category, f.kind, p.base_port()) == key)
                        .map(|p| (*f, *p))
                })
                .expect("desired feed key came from enabled");
            info!(
                venue = key.0,
                category = key.1,
                kind = key.2.label(),
                publisher = key.3,
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
    fn publish_tape_owners(&self, owners: &HashMap<Universe, FeedKey>) {
        for (key, (_, tape)) in &self.active {
            let want = owns(owners, key);
            if tape.swap(want, Ordering::Relaxed) != want {
                metrics()
                    .tape_owner_changes
                    .with_label_values(&[key.0])
                    .inc();
                info!(
                    venue = key.0,
                    category = key.1,
                    kind = key.2.label(),
                    publisher = key.3,
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

    /// Mirrors `apply_ws`: bind first so a taken port is non-fatal (staying off rather than taking
    /// the tunnel down), then spawn the serve loop *and* the history feeder together - the feeder
    /// exists only to keep this sink's store fed, so it has no reason to run without it, and running
    /// it without the sink would silently buffer history nobody can reach.
    async fn apply_api(&mut self, on: bool) {
        match (on, self.api_task.is_some()) {
            (true, false) => match api::bind(&self.cfg.api_bind).await {
                Ok(listener) => {
                    info!(bind = %self.cfg.api_bind, "activating query API (market-data feed subscribed)");
                    self.api_task = Some(tokio::spawn(api::serve(
                        listener,
                        self.cfg.instruments.clone(),
                        self.cfg.depth.clone(),
                        self.cfg.books.clone(),
                        self.cfg.history.clone(),
                        self.health.clone(),
                    )));
                    self.history_feeder = Some(tokio::spawn(feed_history(
                        self.cfg.tx.subscribe(),
                        self.cfg.history.clone(),
                        self.cfg.instruments.clone(),
                    )));
                }
                Err(e) => warn!(bind = %self.cfg.api_bind, %e,
                    "query API failed to bind (port in use?); staying off, will retry next reconcile"),
            },
            (false, true) => {
                if let Some(h) = self.api_task.take() {
                    h.abort();
                }
                if let Some(h) = self.history_feeder.take() {
                    h.abort();
                }
                info!("deactivating query API (no market-data feed subscribed)");
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

/// A `source_ts_ns` more than this far ahead of the same print's own `recv_ts_ns` is implausible - a
/// venue clock error, or (the wire is unauthenticated) a forged one - and is treated the same as the
/// `0` sentinel: fall back to `recv_ts_ns`. Generous against any real venue/host clock skew (at most
/// low milliseconds) while still catching a stamp that is seconds, minutes, or years ahead.
const MAX_PLAUSIBLE_FUTURE_SKEW_NS: u64 = 5_000_000_000; // 5s

/// Resolve one trade's bucket timestamp, clamping an implausible venue time at this feeder seam
/// rather than trusting it into `history::Store` - the same seam that already resolves the
/// `source_ts_ns == 0` sentinel, and for the same reason `history.rs` refuses to know about clocks
/// (see its module doc): that decision belongs to the caller, which knows what a plausible time looks
/// like and the store does not.
///
/// This exists because `Store::ingest`'s late-drop compares a print's bucket only against the
/// *product's own* high-water mark (`newest_seen`), which the store never resets on its own. One
/// print stamped far in the future latches that mark there permanently: every later, correctly-timed
/// print is then late-dropped forever - and since the drop happens before the ring push, `/ticker`
/// empties right along with `/candles`, with no reset path on an unauthenticated wire. Falling back
/// to `recv_ts_ns` (this bridge's own receive clock, which cannot run away from itself) keeps a single
/// bad print from wedging its product's history rather than merely widening one bucket.
///
/// Two conditions, either sufficient: more than `MAX_PLAUSIBLE_FUTURE_SKEW_NS` ahead of `recv_ts_ns`,
/// or older than the store's own rolling window relative to it (a print that far in the past cannot
/// usefully extend the window either way, and treating it as "now" via `recv_ts_ns` is more honest
/// than trusting an implausible venue clock).
fn resolve_ts_ns(source_ts_ns: u64, recv_ts_ns: u64) -> u64 {
    if source_ts_ns == 0 {
        return recv_ts_ns; // the "not available" sentinel - never a real epoch time
    }
    let too_far_future = source_ts_ns > recv_ts_ns.saturating_add(MAX_PLAUSIBLE_FUTURE_SKEW_NS);
    let window_ns = history::WINDOW_SECS.saturating_mul(1_000_000_000);
    let too_old = source_ts_ns.saturating_add(window_ns) < recv_ts_ns;
    if too_far_future || too_old {
        recv_ts_ns
    } else {
        source_ts_ns
    }
}

/// Forward broadcast trades into the shared history store, for as long as the query API sink is
/// active (see [`Reconciler::apply_api`]) - there is no point accumulating history for a query path
/// nobody can reach. Reads the **post-arbiter** broadcast (the same bus the WS sink subscribes to),
/// so every print arriving here is already deduplicated on `trade_id` and gated by the venue's tape
/// owner: one copy of each real print, never a cross-publisher double.
///
/// Keys straight off the message: `NormalizedTrade` now carries `channel`/`instrument_id` itself (the
/// identity `history::Key` groups on), populated at every emission site alongside the `symbol` a
/// price-aggregated venue's mirrored arms can share. There is deliberately no symbol lookup here
/// anymore - matching by `(venue, symbol)` against the instrument catalog is exactly what dropped
/// every trade on a venue whose two arms carry an identical instrument set under distinct `channel`s
/// (see `history::Key`'s docs), because every symbol on such a venue matched more than once.
async fn feed_history(
    mut rx: broadcast::Receiver<Arc<FeedMessage>>,
    history: Arc<Mutex<Store>>,
    instruments: InstrumentSnapshot,
) {
    loop {
        match rx.recv().await {
            Ok(msg) => {
                if let FeedMessage::Trade(t) = msg.as_ref() {
                    // Belt-and-braces for a definition race: the catalog carries no `instrument` for
                    // this exact (venue, channel, instrument_id) yet (or no longer does). Keying
                    // straight off the message means this should be rare-to-never for an edge trade -
                    // its own processor gates emission on already holding that definition - but the
                    // prior failure this whole fix replaces was a venue going silently unattributable,
                    // invisible in both the API and Prometheus, so it is counted and dropped rather
                    // than trusted blind.
                    let known = crate::model::lock(&instruments).contains_key(&(
                        t.venue.clone(),
                        t.channel,
                        t.instrument_id,
                    ));
                    if !known {
                        metrics()
                            .history_unattributable_trades
                            .with_label_values(&[t.venue.as_ref()])
                            .inc();
                        continue;
                    }
                    let ts_ns = resolve_ts_ns(t.source_ts_ns, t.recv_ts_ns);
                    let key = history::Key {
                        source_id: t.source_id,
                        channel: t.channel,
                        instrument_id: t.instrument_id,
                    };
                    let print = history::Print {
                        ts_ns,
                        price: t.price,
                        size: t.size,
                    };
                    crate::model::lock(&history).ingest(key, print);
                }
            }
            // A slow feeder can fall behind the broadcast; the window is a best-effort rolling one,
            // not a promise of every print, so skip the gap rather than exit over it - but count it,
            // like every other broadcast consumer in this bridge (see `sinks::ws`), so a feeder that
            // is punching holes in the window is visible rather than silently thinner.
            Err(broadcast::error::RecvError::Lagged(n)) => {
                metrics().history_feed_lagged.inc();
                warn!(
                    skipped = n,
                    "history feeder lagged the broadcast; window has a gap"
                );
                continue;
            }
            Err(broadcast::error::RecvError::Closed) => return,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::NormalizedTrade;

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
            category: "testcategory",
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
                channel: None,
            },
            FeedPublisher {
                ports: FeedPorts::TwoPort {
                    mktdata: 9201,
                    refdata: 9202,
                },
                channel: None,
            },
        ];
        let feed = test_feed(PUBS);
        let keys: Vec<FeedKey> = feed_keys(&ChannelFloor::default(), &feed).collect();
        assert_eq!(
            keys,
            vec![
                ("TestVenue", "testcategory", FeedKind::TopOfBook, 9101),
                ("TestVenue", "testcategory", FeedKind::TopOfBook, 9201),
            ]
        );
    }

    /// One category for every legacy ownership test: they predate the category dimension and must
    /// keep behaving exactly as they did, which is the safety property of scoping by it.
    const C: &str = "testcategory";
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
        let owners = tape_owners([("V", C, TOB, 7576), ("V", C, MBP, 31000)], all_live);
        assert_eq!(owners.get(&("V", C)), Some(&("V", C, TOB, 7576)));
        assert!(owns(&owners, &("V", C, TOB, 7576)));
        assert!(!owns(&owners, &("V", C, MBP, 31000)));
    }

    /// The case the static `emit_trades` rule got wrong: a host subscribed to the market-by-price
    /// group alone still has to serve a tape.
    #[test]
    fn market_by_price_owns_the_tape_alone() {
        let owners = tape_owners([("V", C, MBP, 31000)], all_live);
        assert!(owns(&owners, &("V", C, MBP, 31000)));
    }

    #[test]
    fn tape_ownership_is_per_venue() {
        let owners = tape_owners([("A", C, MBP, 31000), ("B", C, TOB, 7576)], all_live);
        assert!(owns(&owners, &("A", C, MBP, 31000)));
        assert!(owns(&owners, &("B", C, TOB, 7576)));
    }

    /// Market-by-order is depth-only, so a venue carried only by it has no tape owner at all —
    /// rather than one silently minted from a row that never prints.
    #[test]
    fn a_depth_only_venue_has_no_tape_owner() {
        let owners = tape_owners([("V", C, MBO, 10001)], all_live);
        assert!(owners.is_empty());
        assert!(!owns(&owners, &("V", C, MBO, 10001)));
    }

    /// Ownership is keyed on `(venue, kind)`, so every publisher of the owning row emits and no
    /// publisher of a peer row does — collapsing the mirrored copies is the arbiter's job.
    #[test]
    fn every_publisher_of_the_owning_feed_emits() {
        let owners = tape_owners(
            [
                ("V", C, TOB, 7576),
                ("V", C, TOB, 7676),
                ("V", C, MBP, 31000),
                ("V", C, MBP, 31100),
            ],
            all_live,
        );
        assert!(owns(&owners, &("V", C, TOB, 7576)));
        assert!(owns(&owners, &("V", C, TOB, 7676)));
        assert!(!owns(&owners, &("V", C, MBP, 31000)));
        assert!(!owns(&owners, &("V", C, MBP, 31100)));
    }

    /// The group being subscribed and a publisher actually sending to it are independent facts, so
    /// rank alone would let a dead top-of-book row hold the tape while the market-by-price receiver
    /// decodes prints and drops them. Liveness outranks rank; with everything down it falls back.
    #[test]
    fn a_dead_row_yields_the_tape_to_a_live_peer() {
        let keys = [("V", C, TOB, 7576), ("V", C, MBP, 31000)];
        let owners = tape_owners(keys, |k| {
            if k.2 == TOB {
                TapeLiveness::Down
            } else {
                TapeLiveness::Up
            }
        });
        assert!(owns(&owners, &("V", C, MBP, 31000)));
        assert!(!owns(&owners, &("V", C, TOB, 7576)));

        let both_down = tape_owners(keys, |_| TapeLiveness::Down);
        assert!(owns(&both_down, &("V", C, TOB, 7576)), "falls back to rank");
    }

    /// One live publisher makes its row live: liveness is per receiver but ownership is per row.
    #[test]
    fn one_live_publisher_keeps_the_row_owning() {
        let owners = tape_owners(
            [
                ("V", C, TOB, 7576),
                ("V", C, TOB, 7676),
                ("V", C, MBP, 31000),
            ],
            |k| {
                if *k == ("V", C, TOB, 7576) {
                    TapeLiveness::Down
                } else {
                    TapeLiveness::Up
                }
            },
        );
        assert!(owns(&owners, &("V", C, TOB, 7676)));
        assert!(!owns(&owners, &("V", C, MBP, 31000)));
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
        let owners = tape_owners([("V", C, TOB, 7576), ("V", C, MBP, 31000)], |k| {
            if k.2 == TOB {
                TapeLiveness::Unregistered
            } else {
                TapeLiveness::Up
            }
        });
        assert!(
            owns(&owners, &("V", C, MBP, 31000)),
            "the streaming row keeps the tape until the newcomer actually registers"
        );
        assert!(!owns(&owners, &("V", C, TOB, 7576)));
    }

    /// The property the three-state ordering must not break, and why "not registered yet" is not
    /// simply folded into `Down`: at cold start no row has bound its sockets, so every row is
    /// unregistered and the ordering has to fall back to rank. Demoting an unregistered row below a
    /// registered one unconditionally would leave a fresh process with no tape owner at all.
    #[test]
    fn a_cold_start_falls_back_to_rank() {
        let owners = tape_owners([("V", C, TOB, 7576), ("V", C, MBP, 31000)], |_| {
            TapeLiveness::Unregistered
        });
        assert!(owns(&owners, &("V", C, TOB, 7576)));
    }

    /// An incumbent that registered and then went down is worse than a newcomer that has not
    /// reported yet: the incumbent is known not to be delivering, while the newcomer may be about
    /// to. Neither is serving prints, so this only decides which row is holding the flag when data
    /// resumes — but it keeps the ordering a total one.
    #[test]
    fn an_unregistered_row_outranks_a_registered_dead_one() {
        let owners = tape_owners([("V", C, TOB, 7576), ("V", C, MBP, 31000)], |k| {
            if k.2 == TOB {
                TapeLiveness::Unregistered
            } else {
                TapeLiveness::Down
            }
        });
        assert!(owns(&owners, &("V", C, TOB, 7576)));
    }

    /// Two disjoint universes share one venue when they share a Source ID. Ownership is per
    /// `(venue, category)`: the sports book row must hold its own tape even though a perps
    /// top-of-book row outranks it on kind, because they mirror nothing.
    #[test]
    fn disjoint_categories_each_hold_their_own_tape() {
        let perps_tob = ("KALSHI", "perps", TOB, 7576);
        let sports_mbp = ("KALSHI", "sports", MBP, 33010);
        let owners = tape_owners([perps_tob, sports_mbp], all_live);
        assert!(owns(&owners, &perps_tob));
        assert!(
            owns(&owners, &sports_mbp),
            "the sports row lost its tape to a perps row it mirrors nothing with"
        );
    }

    /// Within one category the existing ranking is unchanged: top-of-book outranks
    /// market-by-price and the losing row is muted. Category scoping must not weaken this.
    #[test]
    fn ranking_within_a_category_is_unchanged() {
        let tob = ("KALSHI", "perps", TOB, 7576);
        let mbp = ("KALSHI", "perps", MBP, 31000);
        let owners = tape_owners([tob, mbp], all_live);
        assert!(owns(&owners, &tob));
        assert!(!owns(&owners, &mbp));
    }

    /// Distinct publishers of the same feed must not collide in the active-task map.
    #[test]
    fn plan_treats_publishers_as_independent() {
        let current: HashSet<FeedKey> = [("V", C, FeedKind::TopOfBook, 9101)].into_iter().collect();
        let desired: HashSet<FeedKey> = [
            ("V", C, FeedKind::TopOfBook, 9101),
            ("V", C, FeedKind::TopOfBook, 9201),
        ]
        .into_iter()
        .collect();
        let (to_spawn, to_abort) = plan(&current, &desired);
        assert_eq!(to_spawn, vec![("V", C, FeedKind::TopOfBook, 9201)]);
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
            channel: None,
        }];
        static MBP_PUB: &[FeedPublisher] = &[FeedPublisher {
            ports: FeedPorts::ThreePort {
                mktdata: 31000,
                refdata: 41000,
                snapshot: 51000,
            },
            channel: None,
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
        let mbp_key = (venue, "testcategory", FeedKind::MarketByPrice, 31000u16);
        let changes = metrics().tape_owner_changes.with_label_values(&[venue]);
        let before = changes.get();

        r.apply_feeds(
            &[(venue, "testcategory", FeedKind::TopOfBook, 7576), mbp_key]
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

    /// The floor reaching the activation path: the desired receiver set for a narrowed row holds
    /// exactly the admitted channels' publishers, so the excluded ones are never spawned and their
    /// sockets are never bound.
    ///
    /// Asserted on the desired **keys** — which base ports this process will bind — rather than on
    /// decoded output, which is empty for an excluded channel whether the floor works or not and so
    /// could not fail. The unfiltered control run is what proves the narrowing is the floor's doing
    /// and not a property of the fixture.
    #[test]
    fn the_floor_narrows_the_desired_receiver_set() {
        let sports = *crate::ingest::feeds::feeds()
            .iter()
            .find(|f| f.category == "sports")
            .expect("the built-in registry has a sports row");
        let floor = ChannelFloor::parse("lashay-4=10,11").unwrap();

        let narrowed = test_reconciler_with_floor(vec![sports], floor);
        let mut ports: Vec<u16> = narrowed.static_desired().feeds.iter().map(|k| k.3).collect();
        ports.sort_unstable();
        assert_eq!(ports, vec![33010, 33011]);

        let wide = test_reconciler(vec![sports]);
        assert_eq!(
            wide.static_desired().feeds.len(),
            sports.publishers.len(),
            "an unnarrowed row must still desire every publisher"
        );
    }

    /// A `Reconciler` whose spawned receivers are never polled: `apply_feeds` is sync, so the tasks
    /// it creates bind no sockets before the test drops them.
    fn test_reconciler(enabled: Vec<Feed>) -> Reconciler {
        test_reconciler_with_floor(enabled, ChannelFloor::default())
    }

    fn test_reconciler_with_floor(enabled: Vec<Feed>, floor: ChannelFloor) -> Reconciler {
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
            floor,
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
            api_bind: String::new(),
            history: Arc::new(Mutex::new(Store::new())),
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

    // ---------------------------------------------------------------------------------------------
    // History feeder + query API activation
    // ---------------------------------------------------------------------------------------------

    fn test_instrument(
        venue: &'static str,
        source_id: u16,
        symbol: &str,
        channel: u32,
        instrument_id: u32,
    ) -> crate::model::NormalizedInstrument {
        crate::model::NormalizedInstrument {
            venue: venue.into(),
            source: venue.into(),
            source_id,
            symbol: symbol.into(),
            channel,
            instrument_id,
            price_exponent: -2,
            qty_exponent: -5,
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn test_trade(
        venue: &'static str,
        source_id: u16,
        symbol: &str,
        channel: u32,
        instrument_id: u32,
        price: f64,
        source_ts_ns: u64,
        recv_ts_ns: u64,
    ) -> NormalizedTrade {
        NormalizedTrade {
            venue: venue.into(),
            source: venue.into(),
            source_id,
            symbol: symbol.into(),
            channel,
            instrument_id,
            price,
            size: 1.0,
            aggressor_side: crate::model::Side::Buy,
            trade_id: 1,
            cumulative_volume: 0.0,
            source_ts_ns,
            recv_ts_ns,
            kernel_rx_ts_ns: 0,
            ws_send_ts_ns: 0,
        }
    }

    /// End-to-end: a trade published on the broadcast is picked up by `feed_history`, lands in the
    /// shared store, and is readable back out through the query API's HTTP surface (`/ticker`, which
    /// reports the raw print ring with no time-window filtering, so this test is purely about the
    /// trade reaching the store, not about bucket/window timing).
    ///
    /// Revert-verify: commenting out this feeder's `history.ingest(...)` call (a no-op stand-in for
    /// "forgot to wire the feeder into the store") makes this test hang the polling loop and fail —
    /// confirmed by hand before landing this test.
    #[tokio::test]
    async fn a_trade_on_the_broadcast_reaches_the_store_and_is_queryable_through_the_api() {
        let (tx, _rx) = broadcast::channel::<Arc<FeedMessage>>(16);
        let instruments: InstrumentSnapshot = Default::default();
        instruments.lock().unwrap().insert(
            ("HYPERLIQUID".into(), 0u32, 41u32),
            test_instrument("HYPERLIQUID", 1, "BTC", 0, 41),
        );
        let history = Arc::new(Mutex::new(Store::new()));
        let health: SharedFeedHealth = Arc::new(FeedHealth::new());

        let listener = api::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(api::serve(
            listener,
            instruments.clone(),
            Default::default(),
            Default::default(),
            history.clone(),
            health,
        ));
        tokio::spawn(feed_history(
            tx.subscribe(),
            history.clone(),
            instruments.clone(),
        ));

        let now = crate::model::now_ns();
        tx.send(Arc::new(FeedMessage::Trade(test_trade(
            "HYPERLIQUID",
            1,
            "BTC",
            0,
            41,
            12345.0,
            now,
            now,
        ))))
        .unwrap();

        let base = format!("http://{addr}");
        for _ in 0..100 {
            let resp = reqwest::get(format!("{base}/v1/products/HYPERLIQUID:BTC/ticker"))
                .await
                .unwrap();
            let body: serde_json::Value = resp.json().await.unwrap();
            if let Some(trades) = body["trades"].as_array() {
                if !trades.is_empty() {
                    assert_eq!(trades[0]["price"], "12345.00");
                    return;
                }
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        panic!("trade never reached the store via the API");
    }

    /// The sentinel case Task 4/5's docs call out by name: `source_ts_ns == 0` must bucket by
    /// `recv_ts_ns`, never by the sentinel itself — a print bucketed at the epoch would silently
    /// vanish from every window query. Checked directly against the stored `Print`, which is why
    /// this doesn't need an API round-trip: `recent_trades` returns the raw ring, ts_ns included.
    ///
    /// Revert-verify: replacing the feeder's `if t.source_ts_ns != 0 { .. } else { .. }` with a bare
    /// `t.source_ts_ns` (always trusting the wire value, sentinel included) makes this test fail —
    /// confirmed by hand before landing this test.
    #[tokio::test]
    async fn a_zero_source_ts_is_bucketed_by_recv_ts_not_by_the_sentinel() {
        let (tx, _rx) = broadcast::channel::<Arc<FeedMessage>>(16);
        let instruments: InstrumentSnapshot = Default::default();
        instruments.lock().unwrap().insert(
            ("HYPERLIQUID".into(), 0u32, 41u32),
            test_instrument("HYPERLIQUID", 1, "BTC", 0, 41),
        );
        let history = Arc::new(Mutex::new(Store::new()));
        tokio::spawn(feed_history(
            tx.subscribe(),
            history.clone(),
            instruments.clone(),
        ));

        let recv_ts_ns = crate::model::now_ns();
        tx.send(Arc::new(FeedMessage::Trade(test_trade(
            "HYPERLIQUID",
            1,
            "BTC",
            0,
            41,
            12345.0,
            0, // source_ts_ns: "not available"
            recv_ts_ns,
        ))))
        .unwrap();

        let key = history::Key {
            source_id: 1,
            channel: 0,
            instrument_id: 41,
        };
        for _ in 0..100 {
            let trades = crate::model::lock(&history).recent_trades(&key, 1);
            if let Some(p) = trades.first() {
                assert_eq!(
                    p.ts_ns, recv_ts_ns,
                    "must fall back to recv_ts_ns, not the 0 sentinel"
                );
                assert_ne!(p.ts_ns, 0);
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        panic!("trade never reached the store");
    }

    /// The headline test, and the inverse of the defect: a price-aggregated venue's two mirrored
    /// arms carry an identical instrument set (same symbol, same `instrument_id`) under distinct
    /// `channel`s. Before this fix, `feed_history` resolved a trade's identity by matching
    /// `(venue, symbol)` against the catalog, and a mirrored-arm symbol always matched twice - so
    /// every trade on such a venue was silently dropped as ambiguous. Keying straight off the
    /// message's own `channel`/`instrument_id` instead means a trade for one arm lands only in that
    /// arm's product, never merged with (or blocked by) its mirror.
    ///
    /// Revert-verify: reintroducing a `(venue, symbol)` catalog match in place of reading
    /// `t.channel`/`t.instrument_id` directly (i.e. restoring the deleted `trade_identity`) makes
    /// this test fail — both catalog entries match the trade's symbol, so it is dropped instead of
    /// reaching either arm's product. Confirmed by hand before landing this test.
    #[tokio::test]
    async fn a_mirrored_arm_trade_is_attributed_to_its_own_product() {
        let (tx, _rx) = broadcast::channel::<Arc<FeedMessage>>(16);
        let instruments: InstrumentSnapshot = Default::default();
        {
            let mut map = instruments.lock().unwrap();
            // Two arms of one price-aggregated venue: identical symbol and instrument_id, distinct
            // channel - exactly the shape `tests/fixtures/PROVENANCE.md` records for the live feed.
            map.insert(
                ("KALSHI".into(), 1u32, 99u32),
                test_instrument("KALSHI", 3, "KXBTCPERP", 1, 99),
            );
            map.insert(
                ("KALSHI".into(), 2u32, 99u32),
                test_instrument("KALSHI", 3, "KXBTCPERP", 2, 99),
            );
        }
        let history = Arc::new(Mutex::new(Store::new()));
        let health: SharedFeedHealth = Arc::new(FeedHealth::new());

        let listener = api::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(api::serve(
            listener,
            instruments.clone(),
            Default::default(),
            Default::default(),
            history.clone(),
            health,
        ));
        tokio::spawn(feed_history(
            tx.subscribe(),
            history.clone(),
            instruments.clone(),
        ));

        // A trade for arm 2 only.
        let now = crate::model::now_ns();
        tx.send(Arc::new(FeedMessage::Trade(test_trade(
            "KALSHI",
            3,
            "KXBTCPERP",
            2,
            99,
            0.62,
            now,
            now,
        ))))
        .unwrap();

        let base = format!("http://{addr}");
        for _ in 0..100 {
            let resp = reqwest::get(format!("{base}/v1/products/KALSHI:KXBTCPERP%232.99/ticker"))
                .await
                .unwrap();
            let body: serde_json::Value = resp.json().await.unwrap();
            if let Some(trades) = body["trades"].as_array() {
                if !trades.is_empty() {
                    assert_eq!(trades[0]["price"], "0.62");
                    // The mirror arm's own product must stay empty - the trade landed in exactly
                    // one product, not both and not neither.
                    let other =
                        reqwest::get(format!("{base}/v1/products/KALSHI:KXBTCPERP%231.99/ticker"))
                            .await
                            .unwrap();
                    let other_body: serde_json::Value = other.json().await.unwrap();
                    assert!(
                        other_body["trades"]
                            .as_array()
                            .is_some_and(|t| t.is_empty()),
                        "the peer arm's product must not also see this trade"
                    );
                    return;
                }
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        panic!("the mirrored-arm trade never reached its product via the API");
    }

    /// An implausible venue clock must not permanently wedge a product's history. Without the clamp,
    /// a print stamped years in the future latches `history::Store`'s per-product high-water mark
    /// there forever, and every later, correctly-timed print is late-dropped before it ever reaches
    /// the ring - so `/ticker` (and `/candles`) empty out and stay empty, with no reset path.
    ///
    /// Revert-verify: replacing `resolve_ts_ns`'s implausibility checks with a bare passthrough of
    /// `source_ts_ns` (trusting the wire value unconditionally, same as before this fix - the `== 0`
    /// sentinel case aside) makes this test time out and fail: the second, normal print never becomes
    /// queryable, wedged behind the first print's runaway high-water mark. Confirmed by hand before
    /// landing this test.
    #[tokio::test]
    async fn an_implausible_future_timestamp_does_not_wedge_the_product() {
        let (tx, _rx) = broadcast::channel::<Arc<FeedMessage>>(16);
        let instruments: InstrumentSnapshot = Default::default();
        instruments.lock().unwrap().insert(
            ("HYPERLIQUID".into(), 0u32, 41u32),
            test_instrument("HYPERLIQUID", 1, "BTC", 0, 41),
        );
        let history = Arc::new(Mutex::new(Store::new()));
        tokio::spawn(feed_history(
            tx.subscribe(),
            history.clone(),
            instruments.clone(),
        ));

        let now = crate::model::now_ns();
        // Ten years ahead of `now` - nowhere near a real venue/host clock skew, and far past
        // `MAX_PLAUSIBLE_FUTURE_SKEW_NS`.
        let implausible_future = now + 10 * 365 * 24 * 3_600 * 1_000_000_000u64;
        tx.send(Arc::new(FeedMessage::Trade(test_trade(
            "HYPERLIQUID",
            1,
            "BTC",
            0,
            41,
            11_111.0,
            implausible_future,
            now,
        ))))
        .unwrap();

        // A normal, correctly-timed print right after.
        let now2 = crate::model::now_ns();
        tx.send(Arc::new(FeedMessage::Trade(test_trade(
            "HYPERLIQUID",
            1,
            "BTC",
            0,
            41,
            22_222.0,
            now2,
            now2,
        ))))
        .unwrap();

        let key = history::Key {
            source_id: 1,
            channel: 0,
            instrument_id: 41,
        };
        for _ in 0..100 {
            let trades = crate::model::lock(&history).recent_trades(&key, 10);
            if trades.iter().any(|p| p.price == 22_222.0) {
                return; // the normal print is queryable - the implausible one did not wedge it
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        panic!("the normal print never became queryable - the future timestamp wedged the product");
    }

    /// The belt-and-braces counter fires on a genuine drop: a trade naming a `(venue, channel,
    /// instrument_id)` the catalog has no definition for at all is dropped rather than stored under
    /// an unconfirmed identity.
    ///
    /// Revert-verify: removing the `known` gate (always falling through to `history.ingest`) makes
    /// this test fail on its second assertion - the trade would reach the store despite naming an
    /// identity nothing defines. Confirmed by hand before landing this test.
    #[tokio::test]
    async fn an_unattributable_trade_is_dropped_and_counted() {
        let (tx, _rx) = broadcast::channel::<Arc<FeedMessage>>(16);
        // Deliberately empty: no `instrument` anywhere names (venue, channel=0, instrument_id=41).
        let instruments: InstrumentSnapshot = Default::default();
        let history = Arc::new(Mutex::new(Store::new()));
        tokio::spawn(feed_history(
            tx.subscribe(),
            history.clone(),
            instruments.clone(),
        ));

        // A venue name unique to this test - the metrics registry is a process-global shared by
        // every test binary, so a shared label would race with other tests touching it.
        let venue = "UnattributableTradeTest";
        let before = metrics()
            .history_unattributable_trades
            .with_label_values(&[venue])
            .get();

        let now = crate::model::now_ns();
        tx.send(Arc::new(FeedMessage::Trade(test_trade(
            venue, 1, "BTC", 0, 41, 1.0, now, now,
        ))))
        .unwrap();

        for _ in 0..100 {
            if metrics()
                .history_unattributable_trades
                .with_label_values(&[venue])
                .get()
                > before
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert_eq!(
            metrics()
                .history_unattributable_trades
                .with_label_values(&[venue])
                .get(),
            before + 1,
            "the unattributable counter must increment exactly once"
        );

        let key = history::Key {
            source_id: 1,
            channel: 0,
            instrument_id: 41,
        };
        assert!(
            crate::model::lock(&history)
                .recent_trades(&key, 1)
                .is_empty(),
            "an unattributable trade must be dropped, not stored"
        );
    }

    /// The bind failure contract `apply_api` shares with `apply_ws`: a port already in use must
    /// leave the API off (and the process alive) rather than propagating an error.
    ///
    /// Revert-verify: changing `apply_api`'s `Err(e) => warn!(..)` arm to `Err(e) => panic!(..)`
    /// makes this test fail — confirmed by hand before landing this test.
    #[tokio::test]
    async fn an_occupied_port_disables_the_api_without_crashing() {
        let occupied = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = occupied.local_addr().unwrap().to_string();

        let mut r = test_reconciler(vec![]);
        r.cfg.api_bind = addr;
        r.apply_api(true).await;

        assert!(
            r.api_task.is_none(),
            "bind failure must not activate the sink"
        );
        assert!(
            r.history_feeder.is_none(),
            "the feeder must not run without its sink"
        );
        drop(occupied);
    }

    /// `apply_api` treats the sink and its feeder as one unit: both come up together, and
    /// `reap_finished` tears both down if either exits on its own (see the doc comment there) so a
    /// later reconcile never layers a second feeder onto a store an old one is still writing to.
    #[tokio::test]
    async fn activating_the_api_starts_both_the_sink_and_its_feeder() {
        let mut r = test_reconciler(vec![]);
        r.cfg.api_bind = "127.0.0.1:0".into();
        r.apply_api(true).await;
        assert!(r.api_task.is_some());
        assert!(r.history_feeder.is_some());

        r.apply_api(false).await;
        assert!(r.api_task.is_none());
        assert!(r.history_feeder.is_none());
    }
}
