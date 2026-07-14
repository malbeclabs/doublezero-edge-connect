//! Reconciler: discovers which multicast groups are active (via the routing table) and keeps the
//! shred forwarder running over exactly that set, restarting it when the set changes.
//!
//! The forwarder itself is the bridge crate's `doublezero_edge_connect::shred::run` — this module
//! only owns the routing-table detection and the activation loop. Compared with the bridge's own
//! reconciler (which polls `doublezero status` and also activates market-data receivers and the
//! WebSocket sink), here only the shred forwarder remains and detection is the kernel routing table
//! rather than the `doublezero` CLI.

use std::{
    collections::HashSet,
    net::{Ipv4Addr, SocketAddr, SocketAddrV4},
    time::Duration,
};

use anyhow::Result;
use doublezero_edge_connect::shred::{self, DedupMode, ShredConfig};
use tokio::task::JoinHandle;
use tracing::{info, warn};

use crate::net::active_groups;

/// Reconciler parameters. Everything except the source set is static; the sources are derived by the
/// reconciler from the routing table (or from the explicit override).
#[derive(Debug, Clone)]
pub struct ReconcileConfig {
    /// Candidate multicast IPs to probe in the routing table.
    pub candidates: Vec<Ipv4Addr>,
    /// Port shared by all shred groups.
    pub port: u16,
    /// Explicit source override (`--source`). When non-empty, skips detection and runs the forwarder
    /// over this fixed set (tests / manual pin).
    pub explicit_sources: Vec<SocketAddrV4>,
    /// Local destinations every shred is fanned out to.
    pub forward: Vec<SocketAddr>,
    /// Interface to join the groups on, whose name is also matched in the routing-table detection.
    pub iface: String,
    /// SO_RCVBUF per receiver socket.
    pub recv_buf: usize,
    /// Dedup mode.
    pub mode: DedupMode,
    /// Solana JSON-RPC endpoint for the leader schedule. Consumed only when `mode` is
    /// [`DedupMode::Sigverify`]; ignored otherwise.
    pub rpc_url: Option<String>,
    /// Dedup window depth in slots.
    pub dedup_window_slots: u64,
    /// How often the routing table is re-probed.
    pub refresh: Duration,
}

impl ReconcileConfig {
    /// Build the forwarder's `ShredConfig` for a given source set.
    fn shred_config(&self, sources: Vec<SocketAddrV4>) -> ShredConfig {
        ShredConfig {
            iface: self.iface.clone(),
            recv_buf: self.recv_buf,
            sources,
            forward: self.forward.clone(),
            mode: self.mode,
            rpc_url: self.rpc_url.clone(),
            dedup_window_slots: self.dedup_window_slots,
        }
    }
}

/// Run the reconciler. With `explicit_sources` it runs the forwarder once over that fixed set (no
/// polling). Otherwise it probes the routing table every `refresh` and restarts the forwarder when
/// the set of active groups changes. Returns only if the forwarder (in explicit mode) exits.
pub async fn run(cfg: ReconcileConfig) -> Result<()> {
    // Created once, up front, and reused across every `select!` below. A fresh `Signal` per
    // iteration would drop a SIGTERM that arrived while we were between `select!` points (e.g. mid
    // routing probe), leaving `systemctl stop` to fall back to SIGKILL; one long-lived listener
    // catches it on the next poll.
    let mut shutdown = Shutdown::new()?;

    if !cfg.explicit_sources.is_empty() {
        let mut sources = cfg.explicit_sources.clone();
        sources.sort();
        info!(
            ?sources,
            "explicit sources (--source); routing detection disabled"
        );
        // Race the forwarder against a shutdown signal so `systemctl stop` (SIGTERM) exits cleanly
        // with a log of the groups being left, rather than the process being killed silently.
        tokio::select! {
            res = shred::run(cfg.shred_config(sources.clone())) => return res,
            sig = shutdown.recv() => {
                info!(signal = %sig, groups = ?sources,
                      "received shutdown signal; leaving multicast groups and exiting");
                return Ok(());
            }
        }
    }

    info!(
        candidates = ?cfg.candidates,
        iface = %cfg.iface,
        refresh_s = cfg.refresh.as_secs(),
        "routing-table detection enabled; polling for active groups"
    );

    // The live forwarder, together with the (sorted) source set it was started with, so a changed
    // set triggers a restart.
    let mut current: Option<(Vec<SocketAddrV4>, JoinHandle<Result<()>>)> = None;
    // Log the "no active groups" state only once so we don't spam every tick.
    let mut logged_empty = false;

    loop {
        // If the live forwarder exited on its own (channel closed / fatal bind error), surface why
        // and clear it so it can be re-activated if still desired. Awaiting the finished handle
        // yields immediately and lets us log the actual error/panic instead of swallowing it (a
        // persistently-failing forwarder would otherwise silently respawn every tick).
        if current.as_ref().is_some_and(|(_, h)| h.is_finished()) {
            let (_, handle) = current.take().expect("checked is_some above");
            match handle.await {
                Ok(Ok(())) => {
                    info!("shred forwarder exited cleanly; will re-activate if still desired")
                }
                Ok(Err(e)) => {
                    warn!(%e, "shred forwarder exited with error; will re-activate if still desired")
                }
                Err(e) => {
                    warn!(%e, "shred forwarder task panicked; will re-activate if still desired")
                }
            }
        }

        // Probe the routing table. `None` means detection itself failed this tick (transient `ip`
        // error) — keep the current activation rather than tearing the forwarder down on a blank
        // probe (fail-open, matching the bridge reconciler). Only reconcile against a real result.
        if let Some(desired) = detect_active_sources(&cfg).await {
            let running: Option<&Vec<SocketAddrV4>> = current.as_ref().map(|(s, _)| s);
            if running != Some(&desired) {
                // Report the transition as a diff, not just the new set, so an operator sees which
                // groups became active/inactive this tick. `old` is cloned out before the borrow of
                // `current` is needed again below.
                let old: Vec<SocketAddrV4> = running.cloned().unwrap_or_default();
                let added: Vec<SocketAddrV4> = desired
                    .iter()
                    .copied()
                    .filter(|s| !old.contains(s))
                    .collect();
                let removed: Vec<SocketAddrV4> = old
                    .iter()
                    .copied()
                    .filter(|s| !desired.contains(s))
                    .collect();

                // Abort the old forwarder (if any); its sockets close when the handle is dropped. The
                // abort is abrupt, so the receivers can't log on their way out — log the groups we are
                // leaving here, before aborting. Note the whole forwarder is restarted on any change,
                // so this lists every currently-joined group, not only the ones in `removed`.
                //
                // Restarting the whole forwarder resets its `DedupWindow`, so for a brief moment after
                // a membership change already-forwarded shreds can be re-forwarded (a small duplicate
                // burst). This is acceptable for v0.1: membership changes are rare in steady state,
                // and with fail-open detection above they no longer fire on transient probe blips.
                // A follow-up could keep the forwarder task persistent and reconcile only its
                // receiver set to avoid the reset entirely.
                if let Some((old_sources, handle)) = current.take() {
                    info!(groups = ?old_sources, "leaving multicast groups; stopping shred forwarder");
                    handle.abort();
                }
                if desired.is_empty() {
                    if !logged_empty {
                        logged_empty = true;
                        info!("no candidate group active in the routing table; forwarder idle");
                    }
                } else {
                    logged_empty = false;
                    info!(added = ?added, removed = ?removed, active = ?desired,
                          "active groups changed; activating shred forwarder");
                    let handle = tokio::spawn(shred::run(cfg.shred_config(desired.clone())));
                    current = Some((desired, handle));
                }
            }
        }

        // Sleep until the next re-probe, but wake early on a shutdown signal so the service stops
        // promptly (and logs the groups it is leaving) instead of hanging up to `refresh` seconds.
        // On signal we abort the live forwarder and return Ok so the process exits 0.
        tokio::select! {
            _ = tokio::time::sleep(cfg.refresh) => {}
            sig = shutdown.recv() => {
                let groups: Vec<SocketAddrV4> =
                    current.as_ref().map(|(s, _)| s.clone()).unwrap_or_default();
                if let Some((_, handle)) = current.take() {
                    handle.abort();
                }
                info!(signal = %sig, groups = ?groups,
                      "received shutdown signal; leaving multicast groups and exiting");
                return Ok(());
            }
        }
    }
}

/// Long-lived shutdown listener: SIGTERM (what `systemctl stop` sends) or SIGINT (Ctrl-C in a
/// foreground run). Built once and reused so a signal arriving between `select!` points isn't
/// dropped — tokio only queues a signal against a live `Signal` stream. On non-unix it falls back to
/// Ctrl-C only.
struct Shutdown {
    #[cfg(unix)]
    term: tokio::signal::unix::Signal,
    #[cfg(unix)]
    int: tokio::signal::unix::Signal,
}

impl Shutdown {
    fn new() -> Result<Self> {
        #[cfg(unix)]
        {
            use tokio::signal::unix::{signal, SignalKind};
            Ok(Self {
                term: signal(SignalKind::terminate())?,
                int: signal(SignalKind::interrupt())?,
            })
        }
        #[cfg(not(unix))]
        {
            Ok(Self {})
        }
    }

    /// Resolve when a shutdown signal arrives, returning its name for the log.
    async fn recv(&mut self) -> &'static str {
        #[cfg(unix)]
        {
            tokio::select! {
                _ = self.term.recv() => "SIGTERM",
                _ = self.int.recv() => "SIGINT",
            }
        }
        #[cfg(not(unix))]
        {
            let _ = tokio::signal::ctrl_c().await;
            "ctrl-c"
        }
    }
}

/// Probe the routing table (on a blocking thread, since it shells out to `ip`) and return the active
/// sources as sorted `SocketAddrV4`. Returns `None` when detection itself failed (a transient `ip`
/// error or the blocking task dying) — the caller keeps the current activation rather than
/// interpreting a failed probe as "no groups active".
async fn detect_active_sources(cfg: &ReconcileConfig) -> Option<Vec<SocketAddrV4>> {
    let candidates = cfg.candidates.clone();
    let iface = cfg.iface.clone();
    let port = cfg.port;
    let active = match tokio::task::spawn_blocking(move || active_groups(&candidates, &iface)).await
    {
        Ok(Ok(active)) => active,
        Ok(Err(e)) => {
            warn!(%e, "routing detection failed this tick; keeping current activation");
            return None;
        }
        Err(e) => {
            warn!(%e, "routing detection task failed; keeping current activation");
            return None;
        }
    };
    // Defensive dedupe in case the candidate list has repeats, and a stable order for the diff.
    let mut set: Vec<SocketAddrV4> = active
        .into_iter()
        .collect::<HashSet<_>>()
        .into_iter()
        .map(|ip| SocketAddrV4::new(ip, port))
        .collect();
    set.sort();
    Some(set)
}
