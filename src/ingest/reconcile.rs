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
    time::Duration,
};

use anyhow::Result;
use tokio::{sync::broadcast, task::JoinHandle};
use tracing::{info, warn};

use crate::{
    ingest::{
        arbiter::SharedArbiter,
        feeds::{Feed, FeedKind},
        receiver,
        subscriptions::{self, Detected, HostSubs},
    },
    model::{DepthSnapshot, FeedMessage, InstrumentSnapshot},
    shred::{self, DedupMode, ShredConfig},
};

/// Identity of a market-data feed in the active-task map. `(venue, kind)` is unique across `FEEDS`
/// (asserted by `feeds::tests::venue_kind_pairs_are_unique`).
type FeedKey = (&'static str, FeedKind);

fn feed_key(f: &Feed) -> FeedKey {
    (f.venue, f.kind)
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
    /// The `--feed`-selected market-data feeds this process may run (subject to subscription).
    pub enabled: Vec<&'static Feed>,
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
    active: HashMap<FeedKey, JoinHandle<Result<()>>>,
    ws_task: Option<JoinHandle<Result<()>>>,
    /// The running shred forwarder plus the (sorted) source set it was started with, so a changed
    /// set triggers a restart.
    shred_task: Option<(Vec<SocketAddrV4>, JoinHandle<Result<()>>)>,
    cli_missing_logged: bool,
}

impl Reconciler {
    pub fn new(cfg: ReconcilerConfig) -> Self {
        Self {
            cfg,
            active: HashMap::new(),
            ws_task: None,
            shred_task: None,
            cli_missing_logged: false,
        }
    }

    /// The poll loop. Never returns; if it ever did (it can't), the process would exit via `main`'s
    /// `select!`. Mirrors `shred::leader`'s refresher shape.
    pub async fn run(mut self) -> Result<()> {
        info!(
            refresh_secs = self.cfg.refresh.as_secs(),
            gating_disabled = self.cfg.gating_disabled,
            feeds = ?self.cfg.enabled.iter().map(|f| f.venue).collect::<Vec<_>>(),
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
            .iter()
            .map(|f| feed_key(f))
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
        let feeds: HashSet<FeedKey> = self.cfg.enabled.iter().map(|f| feed_key(f)).collect();
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
        self.active.retain(|k, h| {
            let done = h.is_finished();
            if done {
                warn!(venue = k.0, kind = ?k.1, "market-data receiver exited; will respawn if still subscribed");
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
            if let Some(h) = self.active.remove(&key) {
                h.abort();
                info!(venue = key.0, kind = ?key.1, "deactivating market-data receiver (no longer subscribed)");
            }
        }
        for key in to_spawn {
            let feed = *self
                .cfg
                .enabled
                .iter()
                .copied()
                .find(|f| feed_key(f) == key)
                .expect("desired feed key came from enabled");
            info!(venue = key.0, kind = ?key.1, group = %feed.group, "activating market-data receiver (subscribed)");
            let h = tokio::spawn(receiver::run_feed(
                feed,
                self.cfg.iface.clone(),
                self.cfg.recv_buf,
                self.cfg.arbiter.clone(),
                self.cfg.instruments.clone(),
                self.cfg.depth.clone(),
            ));
            self.active.insert(key, h);
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
}
