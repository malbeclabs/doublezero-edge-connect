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
        channel_filter::ChannelFilter,
        diagnostics::{Activation, Detection, Polled, ReceiverState, SharedDiagnostics},
        feeds::{Feed, FeedKind},
        health::{FeedHealth, SharedFeedHealth, TapeLiveness},
        receiver, sources,
        subscriptions::{self, Detected, HostSubs},
    },
    metrics::metrics,
    model::{category_arc, BookSnapshot, DepthSnapshot, FeedMessage, InstrumentSnapshot},
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

/// Every receiver key a feed contributes — one per publisher the [`ChannelFilter`] admits.
///
/// The channel filter is an **input** to the desired set, not a second activation authority: it
/// narrows what this function yields and nothing else, so the spawn/abort diff below is unchanged
/// and this module stays the only place that decides what runs. A publisher the channel filter
/// drops is simply never a desired key, which means its socket is never bound and the kernel
/// discards that channel's traffic before it reaches userspace.
fn feed_keys<'a>(filter: &'a ChannelFilter, f: &'a Feed) -> impl Iterator<Item = FeedKey> + 'a {
    filter
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
    /// Which channels of each feed this process ingests (`--channels`), parsed and validated once in
    /// `main`. Empty by default, which admits every channel of every feed.
    ///
    /// Shared and mutable: the admin surface (`sinks::admin`, on by default at loopback, off when
    /// `--admin-bind` is set empty) can replace it at runtime, and this reconciler reads it fresh
    /// every tick — a lock acquisition per tick is free at `--subscription-refresh-secs`
    /// granularity, and this is the only change needed for a runtime channel filter swap to reach
    /// the existing spawn/abort diff.
    pub filter: Arc<Mutex<ChannelFilter>>,
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
    /// The shared diagnostics snapshot this reconciler publishes at the end of every tick, and
    /// `sinks::admin` serves. Write-only from here — nothing in this module ever reads it back, so
    /// a diagnostics change can never influence what runs.
    pub diagnostics: SharedDiagnostics,
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

/// One tick's read of the world: the activation target (`None` == inconclusive, keep what is
/// running) plus everything the diagnostics snapshot reports about *why* that target is what it is.
/// The two travel together because they come from one `doublezero status` invocation — diagnostics
/// deliberately adds no second shell-out to the polling path.
#[derive(Debug, Default)]
struct TickOutcome {
    polled: Polled,
    desired: Option<Desired>,
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
    /// The channel filter's own admitted feed-key set for `cfg.enabled`, as of the last completed
    /// tick — **independent of subscription state**, and deliberately not derived from `active`.
    ///
    /// This, not `desired.feeds`, is what a purge diffs against. `desired.feeds` shrinks for two
    /// unrelated reasons — an operator narrowing the channel filter (a decision to drop data) and
    /// a plain subscription loss (a group unsubscribed, or a `doublezero status` blip that parses
    /// fine but momentarily stops listing a code) — and only the first may ever purge a channel's
    /// catalog/book/history: losing a subscription must only stop the receivers, exactly as it did
    /// before this reconciler purged anything at all, or a one-tick re-provision blip destroys an
    /// hour of trade history nothing can refill. `filter_admitted` is computed from `cfg.enabled`
    /// alone (see its doc), so it never moves on a subscription change and only moves on a real
    /// filter change — which is the whole distinction.
    ///
    /// Diffed against consecutive ticks' own value rather than `active`, for the same reason the
    /// pre-purge-split code diffed `desired` sets that way: a receiver that already self-exited
    /// (`reap_finished` runs *before* `apply_feeds` computes `current` from `active`) is gone from
    /// `active` by the time a channel filter change would otherwise abort it, so an `active`-based
    /// check would silently miss the departure.
    last_filter_admitted: HashSet<FeedKey>,
    /// Departed channels whose receiver has been `abort()`-ed but isn't yet confirmed stopped —
    /// see `drain_departed` for why the purge waits here instead of running right away.
    draining: Vec<Draining>,
}

/// How many ticks a departed receiver may sit in `draining` unfinished before `drain_departed`
/// purges its state anyway. `abort()` cancels a task only at its next `.await`; a receiver wedged
/// in a blocking call (or otherwise starved of a poll) might never actually finish, and waiting on
/// it forever would leak both this entry and the stale catalog/book/history state a client keeps
/// reading. Ten ticks (five minutes at the default `--subscription-refresh-secs=30`) is generous —
/// a normally-behaving receiver's only synchronous stretch is one datagram's `on_datagram`, so it
/// finishes on the very next scheduling opportunity — while still bounding the pathological case.
const MAX_DRAIN_TICKS: u32 = 10;

/// One departed receiver waiting in `Reconciler::draining` for `is_finished()` to confirm it has
/// actually stopped, so its catalog/book/history purge can't be overwritten by a write still
/// in flight when it was aborted.
struct Draining {
    key: FeedKey,
    handle: JoinHandle<Result<()>>,
    ticks_waited: u32,
    /// Whether this departure is a channel-filter narrowing — the only thing `drain_departed` may
    /// purge for. Decided by `apply_feeds` at abort time (`filter_departed.contains(&key)`), never
    /// re-derived later: by the time this entry is drained, `last_filter_admitted` has already
    /// moved on to a later tick's value and could no longer answer "was THIS departure a filter
    /// change" correctly.
    purge: bool,
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
            last_filter_admitted: HashSet::new(),
            draining: Vec::new(),
        }
    }

    /// The poll loop. Never returns; if it ever did (it can't), the process would exit via `main`'s
    /// `select!`. Mirrors `shred::leader`'s refresher shape.
    pub async fn run(mut self) -> Result<()> {
        let filter = self.filter();
        info!(
            refresh_secs = self.cfg.refresh.as_secs(),
            gating_disabled = self.cfg.gating_disabled,
            feeds = ?self.cfg.enabled.iter().map(|f| (f.venue, f.kind.label(), filter.publishers_for(f).len())).collect::<Vec<_>>(),
            channel_filter = ?filter.summary(),
            "subscription reconciler started"
        );
        loop {
            self.tick().await;
            tokio::time::sleep(self.cfg.refresh).await;
        }
    }

    async fn tick(&mut self) {
        let mut outcome = self.compute_desired().await;
        // A `None` desired set is inconclusive this tick (transient CLI error / task join failure):
        // keep the current activations unchanged rather than tearing everything down on a hiccup.
        // The diagnostics snapshot is published either way — an inconclusive tick is precisely the
        // state an operator needs reported, and it publishes the *actual* activations below rather
        // than a desired set that was never applied.
        if let Some(desired) = outcome.desired.take() {
            self.apply_desired(desired).await;
        }
        self.publish_diagnostics(&outcome);
    }

    /// Write the polled half of the shared diagnostics snapshot. Activation is read back off this
    /// reconciler's own task maps rather than off the `Desired` that produced them, so an
    /// inconclusive tick reports what is really running instead of what was wanted.
    fn publish_diagnostics(&self, outcome: &TickOutcome) {
        let activation = Activation {
            receivers: self
                .active
                .keys()
                .map(|k| ReceiverState {
                    venue: k.0,
                    category: k.1,
                    kind: k.2,
                    base_port: k.3,
                    liveness: self.health.liveness(k),
                })
                .collect(),
            ws_on: self.ws_task.is_some(),
            api_on: self.api_task.is_some(),
            shred_sources: self
                .shred_task
                .as_ref()
                .map(|(srcs, _)| srcs.iter().map(|s| s.to_string()).collect())
                .unwrap_or_default(),
        };
        let mut diag = crate::model::lock(&self.cfg.diagnostics);
        diag.refresh_secs = self.cfg.refresh.as_secs();
        diag.publish_tick(outcome.polled.clone(), activation);
    }

    /// Apply one computed [`Desired`] state. Split out of `tick` so a test can drive this
    /// directly with a hand-built `Desired` — bypassing `compute_desired`'s real
    /// `doublezero status` shell-out — which is what makes the purge-vs-abort-only distinction
    /// below testable without a `doublezero` CLI on the test host.
    async fn apply_desired(&mut self, desired: Desired) {
        self.reap_finished();

        // The channel filter's own admitted set, independent of subscription state — see
        // `last_filter_admitted`'s doc for why this, and not `desired.feeds`, is the purge diff.
        // Computed and diffed BEFORE `apply_feeds` runs, same as the pre-split code diffed
        // `desired` sets: `reap_finished` above already dropped a self-exited receiver's key from
        // `active`, so an `active`-based check would silently miss a departure that lands the same
        // tick — see `departed_channels`'s doc.
        let filter_admitted = self.filter_admitted();
        let filter_departed: HashSet<FeedKey> =
            Self::departed_channels(&self.last_filter_admitted, &filter_admitted)
                .into_iter()
                .collect();
        self.last_filter_admitted = filter_admitted;

        // Abort the departing receivers *before* purging their catalog/book/history state (N1): if
        // the purge ran first, the still-live receiver could process one more refdata burst in the
        // window between the purge and its own abort taking effect, silently re-inserting the very
        // catalog entry just removed — permanently, since that map has no other removal path and
        // this channel will not be diffed as "departing" again on any later tick.
        //
        // N1 alone isn't enough: `abort()` only cancels a task at its next `.await`, and a receiver
        // already past `recv_any().await` runs the rest of its synchronous body — `on_datagram` ->
        // catalog upsert -> book replay, no `.await` in between (see `receiver::drive`) — before
        // the cancellation lands. Purging in this same uninterrupted stretch can still be
        // overwritten by that in-flight write. A key with no entry in `active` already stopped on
        // its own before this tick (an earlier `reap_finished`, or it never ran at all), so nothing
        // can race an immediate purge; a key still in `active` gets aborted by `apply_feeds` below
        // (which still aborts on ANY shrink of `desired.feeds` — a subscription loss included, just
        // never a purge) and moves to `self.draining`, whose purge waits for confirmed completion
        // and only fires for a filter-departed key — see `drain_departed`.
        for key in &filter_departed {
            if !self.active.contains_key(key) {
                self.forget_departing_channel(key);
            }
        }
        self.apply_feeds(&desired.feeds, &filter_departed);
        self.drain_departed();
        self.apply_ws(desired.ws_on).await;
        self.apply_api(desired.api_on).await;
        self.apply_shred(desired.shred_sources);
    }

    async fn compute_desired(&mut self) -> TickOutcome {
        if self.cfg.gating_disabled {
            return TickOutcome {
                polled: Polled {
                    detection: Detection::GatingDisabled,
                    ..Polled::default()
                },
                desired: Some(self.static_desired()),
            };
        }
        // The group list is only needed to resolve shred-group IPs; skip it when shreds are
        // disabled or explicitly sourced.
        let need_group_ips = !self.cfg.shred.disabled && self.cfg.shred.explicit_sources.is_empty();
        match tokio::task::spawn_blocking(move || subscriptions::detect(need_group_ips)).await {
            Ok(Detected::Ok(subs)) => {
                let (market_data_codes, shred_codes, other_codes) = self.classify_codes(&subs);
                TickOutcome {
                    polled: Polled {
                        detection: Detection::Ok,
                        detail: None,
                        sessions: subs.sessions.clone(),
                        market_data_codes,
                        shred_codes,
                        other_codes,
                        latency: subs.latency.clone(),
                    },
                    desired: Some(self.desired_from_subs(&subs)),
                }
            }
            Ok(Detected::CliMissing) => {
                if !self.cli_missing_logged {
                    warn!(
                        "`doublezero` CLI not found; subscription gating falls open \
                         (all selected feeds + WS active; shreds via explicit --shred-source only)"
                    );
                    self.cli_missing_logged = true;
                }
                TickOutcome {
                    polled: Polled {
                        detection: Detection::CliMissing,
                        ..Polled::default()
                    },
                    desired: Some(self.static_desired()),
                }
            }
            Ok(Detected::Unavailable { detail }) => TickOutcome {
                polled: Polled {
                    detection: Detection::Unavailable,
                    detail,
                    ..Polled::default()
                },
                desired: None,
            },
            Err(e) => {
                warn!(%e, "subscription detect task failed; keeping current activations");
                TickOutcome {
                    polled: Polled {
                        detection: Detection::Unavailable,
                        detail: Some(format!("subscription detect task failed: {e}")),
                        ..Polled::default()
                    },
                    desired: None,
                }
            }
        }
    }

    /// Split the subscribed codes three ways for reporting: the ones matching a feed row this
    /// process may run, the shred groups, and everything else (a group this host holds that this
    /// build has no row for — the shape a stale registry produces). Sorted so a diff of two
    /// diagnostics responses is stable. Reporting only: activation still keys on
    /// [`HostSubs::market_data_feeds`], which this deliberately mirrors rather than replaces.
    fn classify_codes(&self, subs: &HostSubs) -> (Vec<String>, Vec<String>, Vec<String>) {
        let (mut market, mut shred, mut other) = (Vec::new(), Vec::new(), Vec::new());
        for code in &subs.subscribed_codes {
            if self.cfg.enabled.iter().any(|f| f.code == *code) {
                market.push(code.clone());
            } else if code.starts_with(&self.cfg.shred.code_prefix) {
                shred.push(code.clone());
            } else {
                other.push(code.clone());
            }
        }
        for v in [&mut market, &mut shred, &mut other] {
            v.sort();
        }
        (market, shred, other)
    }

    /// A per-tick snapshot of the runtime-mutable channel filter. Cloned rather than held locked
    /// across the tick's spawn/abort work — `ChannelFilter` is one small `HashMap` entry per
    /// narrowed feed, so the lock is held only for the `clone()` itself, and every caller this tick
    /// sees one consistent channel filter even if the admin surface swaps it in between.
    fn filter(&self) -> ChannelFilter {
        crate::model::lock(&self.cfg.filter).clone()
    }

    /// Desired state from a successful subscription read.
    fn desired_from_subs(&self, subs: &HostSubs) -> Desired {
        let filter = self.filter();
        let feeds: HashSet<FeedKey> = subs
            .market_data_feeds(&self.cfg.enabled)
            .into_iter()
            .flat_map(|f| feed_keys(&filter, f))
            .collect();
        Desired {
            ws_on: !self.cfg.ws_bind.is_empty() && !feeds.is_empty(),
            api_on: !self.cfg.api_bind.is_empty() && !feeds.is_empty(),
            shred_sources: self.desired_shred_sources(Some(subs)),
            feeds,
        }
    }

    /// Fail-open / gating-disabled desired state: every enabled feed on, WS on if configured, shreds
    /// only via explicit sources (no CLI → no discovery). Exactly [`Self::filter_admitted`] — there
    /// is no subscription dimension in this mode, so every enabled+admitted key is desired.
    fn static_desired(&self) -> Desired {
        let feeds = self.filter_admitted();
        Desired {
            ws_on: !self.cfg.ws_bind.is_empty() && !feeds.is_empty(),
            api_on: !self.cfg.api_bind.is_empty() && !feeds.is_empty(),
            shred_sources: self.desired_shred_sources(None),
            feeds,
        }
    }

    /// The feed keys the channel filter currently admits across `cfg.enabled` — **independent of
    /// subscription state**: every enabled row contributes, whether or not its group is actually
    /// subscribed to right now. This is what an operator's channel filter (`--channels` at
    /// startup, or `POST /admin/channels` at runtime) controls directly, and *only* it: narrowing
    /// it is a decision to drop data, which is the one thing allowed to purge a channel's
    /// catalog/book/history (`Self::last_filter_admitted`/`apply_desired`). A subscription change
    /// never moves this set, which is exactly the property that keeps a subscription blip from
    /// ever being mistaken for a filter narrowing.
    fn filter_admitted(&self) -> HashSet<FeedKey> {
        let filter = self.filter();
        self.cfg
            .enabled
            .iter()
            .flat_map(|f| feed_keys(&filter, f))
            .collect()
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

    /// Spawn/abort receivers to match `desired`. **Aborts on any shrink** of `desired` — a
    /// subscription loss included — since a stopped receiver is always correct; whether that
    /// departure also *purges* the channel's catalog/book/history is `filter_departed`'s call
    /// alone, threaded into each `Draining` entry so `drain_departed` can honour it later without
    /// having to re-derive it from a `last_filter_admitted` that has since moved on.
    fn apply_feeds(&mut self, desired: &HashSet<FeedKey>, filter_departed: &HashSet<FeedKey>) {
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
                // Its catalog/book/history purge — if this departure is a filter narrowing at all
                // — waits for confirmed completion, not just this `abort()` call. A departure that
                // is merely a subscription loss is never purged: `purge` stays false and
                // `drain_departed` only ever reaps this entry's handle.
                let purge = filter_departed.contains(&key);
                self.draining.push(Draining {
                    key,
                    handle: h,
                    ticks_waited: 0,
                    purge,
                });
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

    /// Reap every draining receiver whose task is actually finished — never merely aborted — and,
    /// for the ones that departed via a **channel-filter narrowing** (`entry.purge`), purge its
    /// catalog/book/history state. A departure that is only a subscription loss reaps its handle
    /// here and nothing else: its data is left exactly as it was, so the receiver resyncs onto it
    /// the moment the subscription (or a wider channel filter) returns.
    ///
    /// See `apply_desired`'s comment for why `abort()` alone isn't a safe purge trigger: a receiver
    /// already past `recv_any().await` runs the rest of its synchronous body before the
    /// cancellation lands, and purging any earlier can be overwritten by that write, permanently.
    /// Waiting for `is_finished()` guarantees the purge runs strictly after the last write that
    /// receiver could ever make.
    ///
    /// Bounded by `MAX_DRAIN_TICKS`: a receiver `abort()` can't actually interrupt would otherwise
    /// sit here forever, permanently leaking this entry and (for a purge-worthy one) the stale
    /// state its purge would have cleared. Past the bound the entry is reaped anyway and logged
    /// loudly — the same residual risk the old immediate-purge path always carried on every
    /// departure, now confined to a rare, bounded, and observable case.
    fn drain_departed(&mut self) {
        let mut i = 0;
        while i < self.draining.len() {
            let finished = self.draining[i].handle.is_finished();
            self.draining[i].ticks_waited += 1;
            if finished || self.draining[i].ticks_waited > MAX_DRAIN_TICKS {
                let entry = self.draining.remove(i);
                if !finished {
                    warn!(
                        venue = entry.key.0,
                        category = entry.key.1,
                        kind = entry.key.2.label(),
                        publisher = entry.key.3,
                        ticks_waited = entry.ticks_waited,
                        purge = entry.purge,
                        "departed receiver still hasn't stopped after the drain bound; reaping it \
                         anyway"
                    );
                }
                if entry.purge {
                    self.forget_departing_channel(&entry.key);
                }
            } else {
                i += 1;
            }
        }
    }

    /// Every key present in `previous` and absent from `current` — a pure set diff. Used by
    /// `apply_desired` to compute `filter_departed` (`last_filter_admitted` vs. this tick's
    /// `filter_admitted()`), **not** on `desired.feeds`/`self.active`: `reap_finished` runs first
    /// each tick and already drops a self-exited receiver's key from `active`, so by the time a
    /// channel filter change removes that same key from `filter_admitted()` it was never in
    /// `active` to diff against, and an `active`-based check would silently miss the departure.
    /// Basing removal on *the key leaving the filter's admitted set* rather than *an abort actually
    /// happening* is what closes that window.
    fn departed_channels(previous: &HashSet<FeedKey>, current: &HashSet<FeedKey>) -> Vec<FeedKey> {
        previous.difference(current).copied().collect()
    }

    /// `key` just left the channel filter's admitted set and its receiver (if any) has already
    /// been aborted by `apply_feeds` (see `apply_desired`): drop everything the query surface and
    /// a reconnecting WS client
    /// would otherwise keep serving from it — the catalog entry, the accumulated book, and the
    /// rolling trade history — so a departed channel goes fully dark rather than reading as "alive
    /// but quiet" (a frozen `best_bid`/`best_ask` beside an empty trade list is worse than no data
    /// at all: every live-looking field stays live while the one field that would reveal the
    /// channel is dead goes empty).
    ///
    /// A flat publisher (`channel: None`) has no channel identity to forget — narrowing a flat row is
    /// refused at channel-filter-parse time, so this only ever fires for a row a channel actually
    /// identifies. The venue/category are resolved from *this* departing key's own row, never
    /// assumed for the whole group `code`: a code can span rows on different venues
    /// (`ingest::channel_filter`'s docs), so each departure carries its own row's identity rather
    /// than guessing one venue for the code.
    ///
    /// **Category-precise for the catalog, the history, and the book.** `history::Store::
    /// forget_channel` and `InstrumentSnapshot`'s key both now carry `category` (mirroring
    /// `BookSnapshot`'s `authority::MarketKey`, which already did), so this purge names the
    /// departing row's own universe on all three and cannot over-drop a live peer universe's
    /// product sharing the departed channel id — never assume `channel_id` ranges stay disjoint
    /// across universes; that separation is a numbering convention owned upstream, mid-migration,
    /// and enforced nowhere in this code.
    ///
    /// `DepthSnapshot` (Market-by-Order's replay map) is deliberately left untouched: its key is
    /// `(venue, symbol)` with no channel dimension at all, so a channel-scoped purge cannot be
    /// expressed against it correctly, and it is unreachable in practice regardless — only a
    /// channel-derived publisher (`channel: Some(_)`) ever reaches this method, and the loaded
    /// registry's only `derived` rows are `MarketByPrice`; no `MarketByOrder` row is ever
    /// channel-derived today, so `MboProcessor`'s writer of `DepthSnapshot` never runs behind a
    /// narrowable publisher.
    fn forget_departing_channel(&self, key: &FeedKey) {
        let Some((feed, publisher)) = self.cfg.enabled.iter().find_map(|f| {
            f.publishers
                .iter()
                .find(|p| (f.venue, f.category, f.kind, p.base_port()) == *key)
                .map(|p| (*f, *p))
        }) else {
            return;
        };
        let Some(channel) = publisher.channel else {
            return;
        };
        let channel = u32::from(channel);

        // N1's invariant, made loud rather than merely documented: by the time this runs, `tick`
        // must have already aborted this key's receiver (if it was running at all) — otherwise a
        // still-live task's next refdata burst can re-populate the catalog entry the purge below is
        // about to remove, and nothing will purge it again (this channel is not diffed as
        // "departing" a second time). Compiles out in release builds; active in every test and
        // debug run, which is where a reordering regression would otherwise pass silently.
        debug_assert!(
            !self.active.contains_key(key),
            "forget_departing_channel ran before its receiver was aborted (N1 regression): \
             venue={} category={} channel={channel}",
            feed.venue,
            feed.category,
        );

        // The catalog: a bare HashMap<(venue, category, channel, instrument_id), _>, keyed on this
        // departing row's own universe — see the doc above.
        let category = category_arc(feed.category);
        crate::model::lock(&self.cfg.instruments)
            .retain(|k, _| !(k.0.as_ref() == feed.venue && k.1 == category && k.2 == channel));

        // The book: routed through the arbiter, never hand-deleted from the replay map directly, so
        // the accumulator, the replay entry and `StickyAuthority::last_admitted` drop together —
        // see `Arbiter::forget_channel_books`'s doc for why a direct replay-map delete is unsafe.
        let books_dropped = crate::ingest::arbiter::lock(&self.cfg.arbiter).forget_channel_books(
            feed.venue,
            feed.category,
            channel,
        );

        let history_dropped = match sources::source_id_of(feed.venue) {
            Some(source_id) => {
                crate::model::lock(&self.cfg.history).forget_channel(source_id, &category, channel)
            }
            None => 0,
        };

        if history_dropped > 0 || books_dropped > 0 {
            info!(
                venue = feed.venue,
                category = feed.category,
                channel,
                history_dropped,
                books_dropped,
                "channel left the channel filter's admitted set; dropped its catalog/book/history \
                 state"
            );
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
                        self.cfg.filter.clone(),
                        self.cfg.enabled.clone(),
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
/// Keys straight off the message: `NormalizedTrade` now carries `channel`/`instrument_id` *and*
/// `category` itself (the identity `history::Key` groups on), populated at every emission site
/// alongside the `symbol` a price-aggregated venue's mirrored arms can share. There is
/// deliberately no symbol lookup here anymore - matching by `(venue, symbol)` against the
/// instrument catalog is exactly what dropped every trade on a venue whose two arms carry an
/// identical instrument set under distinct `channel`s (see `history::Key`'s docs), because every
/// symbol on such a venue matched more than once. `category` closes the companion gap: two
/// disjoint universes under one Source ID can share `(channel, instrument_id)`, and without it
/// this seam could not tell which universe's product a trade belongs to.
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
                    // this exact (venue, category, channel, instrument_id) yet (or no longer does).
                    // Keying straight off the message means this should be rare-to-never for an
                    // edge trade - its own processor gates emission on already holding that
                    // definition - but the prior failure this whole fix replaces was a venue going
                    // silently unattributable, invisible in both the API and Prometheus, so it is
                    // counted and dropped rather than trusted blind.
                    let known = crate::model::lock(&instruments).contains_key(&(
                        t.venue.clone(),
                        t.category.clone(),
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
                        category: t.category.clone(),
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
            mirror_offset: None,
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
                label: None,
            },
            FeedPublisher {
                ports: FeedPorts::TwoPort {
                    mktdata: 9201,
                    refdata: 9202,
                },
                channel: None,
                label: None,
            },
        ];
        let feed = test_feed(PUBS);
        let keys: Vec<FeedKey> = feed_keys(&ChannelFilter::default(), &feed).collect();
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
        let sports_mbp = ("KALSHI", "sports", MBP, 34010);
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
            label: None,
        }];
        static MBP_PUB: &[FeedPublisher] = &[FeedPublisher {
            ports: FeedPorts::ThreePort {
                mktdata: 31000,
                refdata: 41000,
                snapshot: 51000,
            },
            channel: None,
            label: None,
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
            &HashSet::new(),
        );
        let mbp_tape = r.active[&mbp_key].1.clone();
        assert!(
            !mbp_tape.load(Ordering::Relaxed),
            "top of book owns the tape"
        );

        r.apply_feeds(&[mbp_key].into_iter().collect(), &HashSet::new());
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

    /// The channel filter reaching the activation path: the desired receiver set for a narrowed
    /// row holds exactly the admitted channels' publishers, so the excluded ones are never spawned
    /// and their sockets are never bound.
    ///
    /// Asserted on the desired **keys** — which base ports this process will bind — rather than on
    /// decoded output, which is empty for an excluded channel whether the channel filter works or
    /// not and so could not fail. The unfiltered control run is what proves the narrowing is the
    /// channel filter's doing and not a property of the fixture.
    #[test]
    fn the_filter_narrows_the_desired_receiver_set() {
        let sports = *crate::ingest::feeds::feeds()
            .iter()
            .find(|f| f.category == "sports")
            .expect("the built-in registry has a sports row");
        let filter = ChannelFilter::parse("lashay-4=10,11").unwrap();

        let narrowed = test_reconciler_with_filter(vec![sports], filter);
        let mut ports: Vec<u16> = narrowed
            .static_desired()
            .feeds
            .iter()
            .map(|k| k.3)
            .collect();
        ports.sort_unstable();
        assert_eq!(ports, vec![34010, 34011]);

        let wide = test_reconciler(vec![sports]);
        assert_eq!(
            wide.static_desired().feeds.len(),
            sports.publishers.len(),
            "an unnarrowed row must still desire every publisher"
        );
    }

    /// The real "sports" row (group code `lashay-4`), for tests that need genuine channel-filter
    /// narrowing — `ChannelFilter::parse` validates against the loaded registry, so a custom `Feed`
    /// with a made-up code cannot be narrowed at all.
    fn sports_row() -> Feed {
        *crate::ingest::feeds::feeds()
            .iter()
            .find(|f| f.category == "sports")
            .expect("the built-in registry has a sports row")
    }

    /// One `book` batch for a given identity, reused by the tests below that seed a real arbiter.
    #[allow(clippy::too_many_arguments)]
    fn book_message(
        venue: &str,
        source_id: u16,
        symbol: &str,
        channel: u32,
        instrument_id: u32,
        changes: Vec<crate::model::BookChange>,
    ) -> FeedMessage {
        FeedMessage::Book(crate::model::NormalizedBook {
            venue: venue.into(),
            source: venue.into(),
            source_id,
            symbol: symbol.into(),
            channel,
            instrument_id,
            changes,
            snapshot: false,
            last: true,
            source_ts_ns: 1,
            recv_ts_ns: 0,
            kernel_rx_ts_ns: 0,
            ws_send_ts_ns: 0,
        })
    }

    /// Emit an opening re-baseline (`Clear` + one level) for `key` through the reconciler's real
    /// arbiter, so the accumulator, replay entry and authority record all populate the way real
    /// ingest would — never a hand-built `BookReplay` insert.
    fn seed_book(
        r: &Reconciler,
        venue: &str,
        source_id: u16,
        symbol: &str,
        category: &'static str,
        channel: u32,
        instrument_id: u32,
    ) {
        let mut a = crate::ingest::arbiter::lock(&r.cfg.arbiter);
        let publisher = crate::ingest::arbiter::Publisher::Edge(std::net::IpAddr::V4(
            std::net::Ipv4Addr::new(10, 0, 0, 1),
        ));
        a.emit(
            book_message(
                venue,
                source_id,
                symbol,
                channel,
                instrument_id,
                vec![crate::model::BookChange {
                    action: crate::model::BookAction::Clear,
                    side: crate::model::BookSide::Both,
                    price: 0.0,
                    size: 0.0,
                }],
            ),
            publisher,
            category,
        );
        a.emit(
            book_message(
                venue,
                source_id,
                symbol,
                channel,
                instrument_id,
                vec![crate::model::BookChange {
                    action: crate::model::BookAction::Update,
                    side: crate::model::BookSide::Bid,
                    price: 0.5,
                    size: 10.0,
                }],
            ),
            publisher,
            category,
        );
    }

    /// I1's exact regression: `reap_finished` runs (and, in a real tick, would have already dropped
    /// a self-exited receiver from `active`) *before* the channel leaves the channel filter's
    /// admitted set, so a departure check keyed on `active` (as `apply_feeds`'s own `to_abort` is)
    /// never sees it — `active` no longer holds the key by the time `filter_admitted()` stops
    /// naming it either. Diffing `last_filter_admitted` against `filter_admitted()`, independent of
    /// `active` entirely, is what still catches it.
    ///
    /// Drives the real `tick()` across two ticks with a genuinely **narrowing channel filter** on
    /// the real `lashay-4` sports row (`ChannelFilter::parse` validates against the loaded
    /// registry, so a custom `Feed`'s made-up code cannot be narrowed — see `sports_row`). Channel
    /// 10 departs; channel 11 stays admitted, so `cfg.enabled` is never emptied and the row stays
    /// resolvable.
    #[tokio::test]
    async fn a_self_exited_receivers_channel_is_still_forgotten_when_it_leaves_the_desired_set() {
        let mut r = test_reconciler_with_filter(
            vec![sports_row()],
            ChannelFilter::parse("lashay-4=10,11").unwrap(),
        );
        let key10 = ("KALSHI", "sports", FeedKind::MarketByPrice, 34010u16);

        let hist_key = history::Key {
            source_id: 3,
            category: "sports".into(),
            channel: 10,
            instrument_id: 1,
        };
        r.cfg.history.lock().unwrap().ingest(
            hist_key.clone(),
            history::Print {
                ts_ns: 1_000 * 1_000_000_000,
                price: 1.0,
                size: 1.0,
            },
        );
        assert!(
            !r.cfg
                .history
                .lock()
                .unwrap()
                .candles(&hist_key, 60, 10, 1_100)
                .is_empty(),
            "fixture sanity: the seeded print must be queryable before the departure"
        );

        // Tick 1: channels 10 and 11 both admitted.
        r.tick().await;
        assert!(r.active.contains_key(&key10));

        // Simulate the receiver having already self-exited and been reaped BEFORE the channel
        // filter narrows the channel away — removed from `active` directly, bypassing
        // `apply_feeds`'s own abort path, exactly as `reap_finished` would for a task that died on
        // its own.
        let (h, tape) = r.active.remove(&key10).expect("the receiver was running");
        tape.store(false, std::sync::atomic::Ordering::Relaxed);
        h.abort();
        assert!(
            !r.active.contains_key(&key10),
            "fixture sanity: already reaped before the departure"
        );

        // Tick 2: narrow the channel filter to channel 11 only. `active` no longer holds channel
        // 10's key at all, so a diff against `active` (the pre-fix behaviour) would find nothing
        // to forget.
        *r.cfg.filter.lock().unwrap() = ChannelFilter::parse("lashay-4=11").unwrap();
        r.tick().await;

        assert!(
            r.cfg
                .history
                .lock()
                .unwrap()
                .candles(&hist_key, 60, 10, 1_100)
                .is_empty(),
            "the departed channel's history must be forgotten even though its receiver had \
             already exited on its own"
        );
    }

    /// M2: two rows sharing one group `code` on different venues. Resolution from a departing
    /// `FeedKey` to a `source_id` must be per-row — never "the code's first row's venue" — or one
    /// venue's departure would misattribute to (or be silently skipped in favour of) the other. Both
    /// rows share the exact same channel id, so a code-once resolution bug can't hide behind the
    /// channel ids happening to differ.
    ///
    /// Calls `forget_departing_channel` directly (not through `tick()`): this scenario — one group
    /// `code` spanning rows on two *different* venues — has no real registry row to validate a
    /// channel-filter narrowing against (`ChannelFilter::parse` only accepts codes the loaded registry
    /// carries), and shrinking `cfg.enabled` between ticks would remove the very row entry this
    /// method needs to resolve the departing key's venue/channel from, defeating the scenario
    /// entirely. `tick()`'s own ordering and the book-purge routing are what the dedicated
    /// `a_narrowed_channel_is_purged_from_all_three_maps_via_a_real_tick` test below covers; this one
    /// is scoped to the per-row resolution step alone.
    #[test]
    fn departing_channels_on_a_shared_code_resolve_their_own_rows_source_id() {
        static PUB_A: &[FeedPublisher] = &[FeedPublisher {
            ports: FeedPorts::ThreePort {
                mktdata: 33020,
                refdata: 43020,
                snapshot: 53020,
            },
            channel: Some(20),
            label: None,
        }];
        static PUB_B: &[FeedPublisher] = &[FeedPublisher {
            ports: FeedPorts::ThreePort {
                mktdata: 33021,
                refdata: 43021,
                snapshot: 53021,
            },
            channel: Some(20), // same channel id as PUB_A — only the venue/row differs
            label: None,
        }];
        let feed_a = Feed {
            venue: "HYPERLIQUID", // source_id 1
            category: "testcategory",
            code: "shared-code",
            kind: FeedKind::MarketByPrice,
            group: std::net::Ipv4Addr::new(233, 84, 178, 96),
            publishers: PUB_A,
            emit_trades: true,
            arbitration: ArbitrationMode::Sticky,
            mirror_offset: None,
        };
        let feed_b = Feed {
            venue: "KALSHI", // source_id 3
            category: "testcategory",
            code: "shared-code",
            kind: FeedKind::MarketByPrice,
            group: std::net::Ipv4Addr::new(233, 84, 178, 97),
            publishers: PUB_B,
            emit_trades: true,
            arbitration: ArbitrationMode::Sticky,
            mirror_offset: None,
        };
        let r = test_reconciler(vec![feed_a, feed_b]);
        let key_a = (
            "HYPERLIQUID",
            "testcategory",
            FeedKind::MarketByPrice,
            33020u16,
        );
        let key_b = ("KALSHI", "testcategory", FeedKind::MarketByPrice, 33021u16);

        let hist_a = history::Key {
            source_id: 1,
            category: "testcategory".into(),
            channel: 20,
            instrument_id: 1,
        };
        let hist_b = history::Key {
            source_id: 3,
            category: "testcategory".into(),
            channel: 20,
            instrument_id: 1,
        };
        {
            let mut h = r.cfg.history.lock().unwrap();
            h.ingest(
                hist_a.clone(),
                history::Print {
                    ts_ns: 1_000 * 1_000_000_000,
                    price: 1.0,
                    size: 1.0,
                },
            );
            h.ingest(
                hist_b.clone(),
                history::Print {
                    ts_ns: 1_000 * 1_000_000_000,
                    price: 2.0,
                    size: 1.0,
                },
            );
        }

        r.forget_departing_channel(&key_a);
        r.forget_departing_channel(&key_b);

        let store = r.cfg.history.lock().unwrap();
        assert!(
            store.candles(&hist_a, 60, 10, 1_100).is_empty(),
            "HYPERLIQUID's own history must be forgotten"
        );
        assert!(
            store.candles(&hist_b, 60, 10, 1_100).is_empty(),
            "KALSHI's history, under a different source_id sharing the code and channel id, must \
             be forgotten independently — a code-once resolution would misattribute or skip it"
        );
    }

    /// The catalog-purge counterpart of the over-drop this fix closes: two rows on the SAME venue
    /// (so the same Source ID) but different `category` — the shape two disjoint instrument
    /// universes actually take — both use channel `10`. Never assume `channel_id` ranges stay
    /// disjoint across universes; that separation is a numbering convention owned upstream and
    /// enforced nowhere in this code. Departing the "perps" row must purge exactly its own
    /// `InstrumentSnapshot` entry and leave the "sports" row's entry on the identical channel id
    /// fully intact — asserted by symbol survival, not merely a count, so a category-blind filter
    /// (which would drop both) can't hide behind an empty diff.
    #[test]
    fn forget_departing_channel_spares_a_peer_categorys_catalog_entry_sharing_the_channel_id() {
        static PUB_PERPS: &[FeedPublisher] = &[FeedPublisher {
            ports: FeedPorts::ThreePort {
                mktdata: 33030,
                refdata: 43030,
                snapshot: 53030,
            },
            channel: Some(10),
            label: None,
        }];
        static PUB_SPORTS: &[FeedPublisher] = &[FeedPublisher {
            ports: FeedPorts::ThreePort {
                mktdata: 33031,
                refdata: 43031,
                snapshot: 53031,
            },
            channel: Some(10), // same channel id as PUB_PERPS — only the category differs
            label: None,
        }];
        let feed_perps = Feed {
            venue: "KALSHI",
            category: "perps",
            code: "shared-category-code",
            kind: FeedKind::MarketByPrice,
            group: std::net::Ipv4Addr::new(233, 84, 178, 98),
            publishers: PUB_PERPS,
            emit_trades: true,
            arbitration: ArbitrationMode::Sticky,
            mirror_offset: None,
        };
        let feed_sports = Feed {
            venue: "KALSHI",
            category: "sports",
            code: "shared-category-code",
            kind: FeedKind::MarketByPrice,
            group: std::net::Ipv4Addr::new(233, 84, 178, 99),
            publishers: PUB_SPORTS,
            emit_trades: true,
            arbitration: ArbitrationMode::Sticky,
            mirror_offset: None,
        };
        let r = test_reconciler(vec![feed_perps, feed_sports]);
        let key_perps = ("KALSHI", "perps", FeedKind::MarketByPrice, 33030u16);

        r.cfg.instruments.lock().unwrap().insert(
            ("KALSHI".into(), "perps".into(), 10u32, 1u32),
            test_instrument_in("perps", "KALSHI", 3, "KXBTCPERP", 10, 1),
        );
        r.cfg.instruments.lock().unwrap().insert(
            ("KALSHI".into(), "sports".into(), 10u32, 1u32),
            test_instrument_in("sports", "KALSHI", 3, "LAKERSWIN", 10, 1),
        );

        r.forget_departing_channel(&key_perps);

        let map = r.cfg.instruments.lock().unwrap();
        assert!(
            !map.contains_key(&("KALSHI".into(), "perps".into(), 10u32, 1u32)),
            "the departing perps row's own catalog entry must be purged"
        );
        let peer = map
            .get(&("KALSHI".into(), "sports".into(), 10u32, 1u32))
            .expect(
                "the sports row's catalog entry, sharing channel 10 under the same venue, must \
                 survive — a category-blind filter would have dropped it too",
            );
        assert_eq!(peer.symbol.as_ref(), "LAKERSWIN");
    }

    /// C1: after a channel departs, the **query surface** — not just an internal map — must reflect
    /// it. A product still listed with a frozen `best_bid`/`best_ask` beside an empty trade list
    /// reads as "alive but quiet," which is worse than nothing being served at all. Seeds **all
    /// three** maps (catalog, book via the real arbiter, history) so this pins the whole purge, not
    /// the catalog alone, and drives the real `tick()` across two ticks with a narrowing channel
    /// filter on the real sports row (see `sports_row`/`a_self_exited_receivers_...` for why a
    /// custom-code row can't be narrowed).
    #[tokio::test]
    async fn a_departed_channels_product_is_invisible_to_the_query_surface() {
        let mut r = test_reconciler_with_filter(
            vec![sports_row()],
            ChannelFilter::parse("lashay-4=10,11").unwrap(),
        );

        r.cfg.instruments.lock().unwrap().insert(
            ("KALSHI".into(), "sports".into(), 10u32, 1u32),
            crate::model::NormalizedInstrument {
                venue: "KALSHI".into(),
                source: "KALSHI".into(),
                source_id: 3,
                symbol: "DEPARTED".into(),
                channel: 10,
                instrument_id: 1,
                category: "sports".into(),
                price_exponent: -4,
                qty_exponent: -2,
            },
        );
        seed_book(&r, "KALSHI", 3, "DEPARTED", "sports", 10, 1);
        let hist_key = history::Key {
            source_id: 3,
            category: "sports".into(),
            channel: 10,
            instrument_id: 1,
        };
        r.cfg.history.lock().unwrap().ingest(
            hist_key,
            history::Print {
                ts_ns: 1_000 * 1_000_000_000,
                price: 1.0,
                size: 1.0,
            },
        );

        let listener = api::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(api::serve(
            listener,
            r.cfg.instruments.clone(),
            r.cfg.depth.clone(),
            r.cfg.books.clone(),
            r.cfg.history.clone(),
            Arc::new(FeedHealth::new()),
            r.cfg.filter.clone(),
            r.cfg.enabled.clone(),
        ));
        let base = format!("http://{addr}");

        // Fixture sanity: the product resolves, with a real book and trade history, before the
        // departure.
        let resp = reqwest::get(format!("{base}/v1/products/KALSHI:DEPARTED"))
            .await
            .unwrap();
        assert_eq!(
            resp.status(),
            200,
            "fixture sanity: the product must resolve before departure"
        );
        let ticker = reqwest::get(format!("{base}/v1/products/KALSHI:DEPARTED/ticker"))
            .await
            .unwrap()
            .json::<serde_json::Value>()
            .await
            .unwrap();
        assert!(
            !ticker["trades"].as_array().unwrap().is_empty(),
            "fixture sanity: history must be seeded before departure: {ticker}"
        );
        assert!(
            ticker["best_bid"].is_string(),
            "fixture sanity: the book must be seeded before departure: {ticker}"
        );

        // Tick 1: channels 10 and 11 both admitted.
        r.tick().await;
        // Tick 2: narrow the channel filter to channel 11 only — channel 10 departs. The purge
        // itself is now deferred until the aborted receiver is confirmed stopped (`drain_departed`);
        // drive `MAX_DRAIN_TICKS` further ticks so the bound forces it regardless of whether this
        // test's spawned receiver ever gets polled to completion on its own.
        *r.cfg.filter.lock().unwrap() = ChannelFilter::parse("lashay-4=11").unwrap();
        r.tick().await;
        for _ in 0..MAX_DRAIN_TICKS {
            r.tick().await;
        }

        let resp = reqwest::get(format!("{base}/v1/products/KALSHI:DEPARTED"))
            .await
            .unwrap();
        assert_eq!(
            resp.status(),
            404,
            "a departed channel's product must vanish from the query surface, not read as \
             alive-but-quiet"
        );
    }

    /// N1 + N2 + N3, the deliverable: drives the real `tick()` (not a hand-called helper) across two
    /// ticks with a genuinely **narrowing channel filter** (the real `lashay-4` sports row, via
    /// `ChannelFilter::parse`), seeding all three maps a departure purges — the catalog, the book
    /// (through the real arbiter, not a hand-built `BookReplay`), and history.
    ///
    /// Fails on an N1 regression (the purge moved back above `apply_feeds`) because
    /// `forget_departing_channel`'s own `debug_assert!` trips the moment the departing key is still
    /// in `active` — a plain panic, not a silent pass. Fails on an N2 regression (the book purge
    /// deleted) because the book assertions below check `cfg.books` directly: nothing else in this
    /// crate would otherwise clear it for a departed channel.
    ///
    /// The purge itself no longer lands on the departure tick: it waits in `draining` for the
    /// receiver's `JoinHandle` to report `is_finished()` (see `drain_departed`), and this test's
    /// spawned receiver is never actually polled (`test_reconciler`'s doc), so it never satisfies
    /// that on its own. The extra ticks below drive past `MAX_DRAIN_TICKS` so the bound forces the
    /// purge instead — the deterministic seam for *this* test. The race the bound guards against
    /// (a write landing between `abort()` and confirmed completion) is covered on its own by
    /// `a_write_that_lands_after_abort_is_still_purged_once_the_receiver_finishes` below.
    #[tokio::test]
    async fn a_narrowed_channel_is_purged_from_all_three_maps_via_a_real_tick() {
        let feed = sports_row();
        let mut r = test_reconciler_with_filter(
            vec![feed],
            ChannelFilter::parse("lashay-4=10,11").unwrap(),
        );
        let key10 = ("KALSHI", "sports", FeedKind::MarketByPrice, 34010u16);
        let catalog_key: (Arc<str>, Arc<str>, u32, u32) =
            ("KALSHI".into(), "sports".into(), 10u32, 1u32);
        let book_key: crate::ingest::authority::MarketKey =
            (Arc::from("KALSHI"), Arc::from("sports"), 10, 1);
        let hist_key = history::Key {
            source_id: 3,
            category: "sports".into(),
            channel: 10,
            instrument_id: 1,
        };

        // Tick 1: both channel 10 and 11 admitted.
        r.tick().await;
        assert!(
            r.active.contains_key(&key10),
            "fixture sanity: channel 10 is running after tick 1"
        );

        // Seed all three maps for channel 10's identity (KALSHI, source_id 3, channel 10).
        r.cfg.instruments.lock().unwrap().insert(
            catalog_key.clone(),
            crate::model::NormalizedInstrument {
                venue: "KALSHI".into(),
                source: "KALSHI".into(),
                source_id: 3,
                symbol: "NARROWED".into(),
                channel: 10,
                instrument_id: 1,
                category: "sports".into(),
                price_exponent: -4,
                qty_exponent: -2,
            },
        );
        seed_book(&r, "KALSHI", 3, "NARROWED", "sports", 10, 1);
        r.cfg.history.lock().unwrap().ingest(
            hist_key.clone(),
            history::Print {
                ts_ns: 1_000 * 1_000_000_000,
                price: 1.0,
                size: 1.0,
            },
        );

        // Fixture sanity: all three populated before the narrowing.
        assert!(r.cfg.instruments.lock().unwrap().contains_key(&catalog_key));
        assert!(crate::model::lock(&r.cfg.books).contains_key(&book_key));
        assert!(!r
            .cfg
            .history
            .lock()
            .unwrap()
            .candles(&hist_key, 60, 10, 1_100)
            .is_empty());

        // Narrow the channel filter: channel 10 departs, channel 11 stays admitted.
        *r.cfg.filter.lock().unwrap() = ChannelFilter::parse("lashay-4=11").unwrap();

        // Tick 2: the real `tick()` aborts the receiver and queues it in `draining`. Its spawned
        // task is never polled in this harness, so it never reports finished on its own; drive
        // `MAX_DRAIN_TICKS` further ticks so the drain bound forces the purge.
        r.tick().await;
        for _ in 0..MAX_DRAIN_TICKS {
            r.tick().await;
        }

        assert!(
            !r.active.contains_key(&key10),
            "channel 10's receiver must be aborted"
        );
        assert!(
            !r.cfg.instruments.lock().unwrap().contains_key(&catalog_key),
            "the catalog entry must be purged"
        );
        assert!(
            !crate::model::lock(&r.cfg.books).contains_key(&book_key),
            "the book replay entry must be purged through the arbiter"
        );
        assert!(
            r.cfg
                .history
                .lock()
                .unwrap()
                .candles(&hist_key, 60, 10, 1_100)
                .is_empty(),
            "history must be purged"
        );
    }

    /// **The purge split.** A subscription loss — the group unsubscribed, or a `doublezero status`
    /// blip that shrinks `desired.feeds` for a tick without the channel filter moving at all — must
    /// only stop the receiver; the catalog/book/history stay exactly as they were, since before
    /// this reconciler purged anything an unsubscribe only ever stopped receiver tasks. Drives
    /// `apply_desired` directly with hand-built `Desired`s (the seam it exists to expose): `tick()`
    /// itself can't be driven this way without a `doublezero` CLI on the test host, and this is
    /// precisely the distinction a test that only asserts "the receiver stopped" cannot tell apart
    /// from the narrowing case above.
    ///
    /// Revert-verify: reverting `apply_desired` to diff `desired.feeds` instead of
    /// `filter_admitted()` (the pre-fix behaviour, where ANY shrink purges) makes the history/book/
    /// catalog assertions below fail — confirmed by hand before landing this test (see the PR
    /// report for the exact failure output).
    #[tokio::test]
    async fn a_subscription_loss_stops_the_receiver_without_purging_its_state() {
        let mut r = test_reconciler_with_filter(
            vec![sports_row()],
            ChannelFilter::parse("lashay-4=10,11").unwrap(),
        );
        let key10 = ("KALSHI", "sports", FeedKind::MarketByPrice, 34010u16);
        let catalog_key: (Arc<str>, Arc<str>, u32, u32) =
            ("KALSHI".into(), "sports".into(), 10u32, 1u32);
        let book_key: crate::ingest::authority::MarketKey =
            (Arc::from("KALSHI"), Arc::from("sports"), 10, 1);
        let hist_key = history::Key {
            source_id: 3,
            category: "sports".into(),
            channel: 10,
            instrument_id: 1,
        };
        let subscribed = |feeds: HashSet<FeedKey>| Desired {
            feeds,
            ws_on: false,
            api_on: false,
            shred_sources: Vec::new(),
        };

        // Both admitted channels subscribed and running.
        let both = r.filter_admitted();
        r.apply_desired(subscribed(both.clone())).await;
        assert!(
            r.active.contains_key(&key10),
            "fixture sanity: channel 10 is running before the blip"
        );

        // Seed all three maps for channel 10's identity.
        r.cfg.instruments.lock().unwrap().insert(
            catalog_key.clone(),
            crate::model::NormalizedInstrument {
                venue: "KALSHI".into(),
                source: "KALSHI".into(),
                source_id: 3,
                symbol: "STILLHERE".into(),
                channel: 10,
                instrument_id: 1,
                category: "sports".into(),
                price_exponent: -4,
                qty_exponent: -2,
            },
        );
        seed_book(&r, "KALSHI", 3, "STILLHERE", "sports", 10, 1);
        r.cfg.history.lock().unwrap().ingest(
            hist_key.clone(),
            history::Print {
                ts_ns: 1_000 * 1_000_000_000,
                price: 1.0,
                size: 1.0,
            },
        );

        // The subscription blip: channel 10 drops out of `desired.feeds` — the channel filter
        // itself (`cfg.filter`) is never touched. Driven past `MAX_DRAIN_TICKS` so the drain
        // bound's reap-anyway path is exercised too, and it must still not purge.
        let mut lost_sub = both.clone();
        lost_sub.remove(&key10);
        for _ in 0..=MAX_DRAIN_TICKS {
            r.apply_desired(subscribed(lost_sub.clone())).await;
        }

        assert!(
            !r.active.contains_key(&key10),
            "the receiver must stop once its subscription is gone"
        );
        assert!(
            r.cfg.instruments.lock().unwrap().contains_key(&catalog_key),
            "a subscription loss must never purge the catalog"
        );
        assert!(
            crate::model::lock(&r.cfg.books).contains_key(&book_key),
            "a subscription loss must never purge the book"
        );
        assert!(
            !r.cfg
                .history
                .lock()
                .unwrap()
                .candles(&hist_key, 60, 10, 1_100)
                .is_empty(),
            "a subscription loss must never purge history"
        );

        // The subscription returning respawns the receiver onto the very same state — nothing was
        // ever discarded for it to resync from scratch.
        r.apply_desired(subscribed(both)).await;
        assert!(
            r.active.contains_key(&key10),
            "the receiver must resume once the subscription returns"
        );
    }

    /// The race the finding described: `abort()` only cancels a task at its next `.await`, so a
    /// receiver caught mid-synchronous-body (past `recv_any().await`, before its next loop
    /// iteration — see `receiver::drive`) can still write state *after* `tick()`'s abort call.
    /// Genuine OS-thread concurrency can't be driven deterministically in a unit test (this crate's
    /// tests run on the current-thread runtime — see `test_reconciler`'s doc — where two tasks
    /// never truly run "at the same time"), so this test makes the ordering explicit instead: a
    /// stand-in occupies `active` in place of a real receiver, and the "late write" a still-running
    /// receiver would perform is done directly by the test, sequenced strictly after the abort call
    /// and before the stand-in is confirmed finished. What's under test is exactly what
    /// `drain_departed` is supposed to guarantee: a write landing in that window must not survive
    /// as a permanent catalog entry once the purge actually runs.
    ///
    /// Revert-verify: reverting `tick`/`drain_departed` to purge immediately when a key departs
    /// (the pre-fix behaviour) makes the final assertion fail — the late write survives forever,
    /// since this channel is never diffed as departing a second time. Confirmed by hand before
    /// landing this test (see the PR report for the exact failure output).
    #[tokio::test]
    async fn a_write_that_lands_after_abort_is_still_purged_once_the_receiver_finishes() {
        let mut r = test_reconciler_with_filter(
            vec![sports_row()],
            ChannelFilter::parse("lashay-4=10,11").unwrap(),
        );
        let key10 = ("KALSHI", "sports", FeedKind::MarketByPrice, 34010u16);
        let catalog_key: (Arc<str>, Arc<str>, u32, u32) =
            ("KALSHI".into(), "sports".into(), 10u32, 1u32);

        // Tick 1: both channels admitted (spawns real, never-polled receivers).
        r.tick().await;
        assert!(r.active.contains_key(&key10));

        // Replace channel 10's real receiver with a stand-in we fully control: it never completes
        // on its own, so `is_finished()` stays false until `abort()`'s cancellation actually lands.
        let (_, tape) = r
            .active
            .remove(&key10)
            .expect("channel 10 is running after tick 1");
        let standin: JoinHandle<Result<()>> = tokio::spawn(async {
            std::future::pending::<()>().await;
            Ok(())
        });
        r.active.insert(key10, (standin, tape));

        // Seed the catalog entry the departure is supposed to purge.
        r.cfg.instruments.lock().unwrap().insert(
            catalog_key.clone(),
            test_instrument_in("sports", "KALSHI", 3, "NARROWED", 10, 1),
        );

        // Narrow the filter: channel 10 departs. Tick 2 aborts the stand-in and queues it in
        // `draining` rather than purging right away.
        *r.cfg.filter.lock().unwrap() = ChannelFilter::parse("lashay-4=11").unwrap();
        r.tick().await;
        assert!(
            !r.active.contains_key(&key10),
            "fixture sanity: the stand-in is aborted"
        );

        // The in-flight "datagram" lands now — strictly after the abort call above, exactly the
        // ordering the finding describes.
        r.cfg.instruments.lock().unwrap().insert(
            catalog_key.clone(),
            test_instrument_in("sports", "KALSHI", 3, "LATE-WRITE", 10, 1),
        );

        // Give the runtime a chance to actually process the stand-in's cancellation: it's parked
        // on a pending future with no synchronous work of its own, so once `abort()`'s
        // cancellation is scheduled, a handful of scheduling passes is generous.
        for _ in 0..8 {
            tokio::task::yield_now().await;
        }

        // Tick 3: `drain_departed` now sees the stand-in finished and purges — strictly after the
        // late write, not before it.
        r.tick().await;

        assert!(
            !r.cfg.instruments.lock().unwrap().contains_key(&catalog_key),
            "a write that lands after the abort call must not leave a permanent catalog entry"
        );
    }

    /// A `Reconciler` whose spawned receivers are never polled: `apply_feeds` is sync, so the tasks
    /// it creates bind no sockets before the test drops them.
    fn test_reconciler(enabled: Vec<Feed>) -> Reconciler {
        test_reconciler_with_filter(enabled, ChannelFilter::default())
    }

    fn test_reconciler_with_filter(enabled: Vec<Feed>, filter: ChannelFilter) -> Reconciler {
        let (tx, _rx) = broadcast::channel(16);
        // The arbiter's own `book_replay` must point at the *same* `BookSnapshot` handed to
        // `ReconcilerConfig::books` (the object `sinks::api`/`sinks::ws` read and this test module's
        // assertions check) — otherwise `Arbiter::forget_channel_books` purges a replay map nothing
        // else ever sees, and a test seeding books only through `cfg.books` directly would never
        // exercise the real path at all.
        let books: crate::model::BookSnapshot = Default::default();
        let mut arbiter = crate::ingest::arbiter::Arbiter::new(tx.clone(), 16);
        arbiter.set_book_replay(books.clone());
        Reconciler::new(ReconcilerConfig {
            arbiter: Arc::new(std::sync::Mutex::new(arbiter)),
            tx,
            instruments: Default::default(),
            depth: Default::default(),
            books,
            enabled,
            filter: Arc::new(Mutex::new(filter)),
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
            diagnostics: Default::default(),
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
        test_instrument_in("default", venue, source_id, symbol, channel, instrument_id)
    }

    #[allow(clippy::too_many_arguments)]
    fn test_instrument_in(
        category: &'static str,
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
            category: category.into(),
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
        test_trade_in(
            "default",
            venue,
            source_id,
            symbol,
            channel,
            instrument_id,
            price,
            source_ts_ns,
            recv_ts_ns,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn test_trade_in(
        category: &'static str,
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
            category: category.into(),
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
            ("HYPERLIQUID".into(), "default".into(), 0u32, 41u32),
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
            Arc::new(Mutex::new(ChannelFilter::default())),
            Vec::new(),
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
            ("HYPERLIQUID".into(), "default".into(), 0u32, 41u32),
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
            category: "default".into(),
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
                ("KALSHI".into(), "default".into(), 1u32, 99u32),
                test_instrument("KALSHI", 3, "KXBTCPERP", 1, 99),
            );
            map.insert(
                ("KALSHI".into(), "default".into(), 2u32, 99u32),
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
            Arc::new(Mutex::new(ChannelFilter::default())),
            Vec::new(),
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
            ("HYPERLIQUID".into(), "default".into(), 0u32, 41u32),
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
            category: "default".into(),
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
            category: "default".into(),
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
