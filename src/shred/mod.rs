//! Shred forwarder (peer of `ingest/` and `sinks/`).
//!
//! Joins the DoubleZero `edge-solana-*` shred multicast feeds, combines them, and fans each
//! datagram out to one or more local UDP destinations. It shares nothing with the market-data
//! pipeline — no `FeedMessage`, no WebSocket.
//!
//! ```text
//! N source multicast groups → N receiver tasks → bounded mpsc → 1 forwarder task → fan-out UDP → M destinations
//! ```
//!
//! Routing through a single forwarder is deliberate: it's the one place the dedup + sigverify state
//! lives, with no cross-task sharing. Receivers stay dumb (recv → push bytes); the forwarder owns
//! the send socket, the dedup window, and the leader-schedule lookup.
//!
//! Three modes, selected by the single [`DedupMode`] setting (`--shred-dedup-mode` /
//! `DZ_SHRED_DEDUP_MODE`):
//! - **Dedup-only** ([`DedupMode::Dedup`], the **default**) → forward exactly **one copy** of each
//!   shred keyed on `(slot, index, type, content-fingerprint)` ([`dedup`]), with **no** signature
//!   verification, leader lookup, or RPC. The first copy of a key is forwarded and recorded; later
//!   copies that match over the fingerprinted region are dropped. The fingerprint covers the whole
//!   datagram except the trailing **retransmitter signature** of resigned merkle shreds (rewritten
//!   per turbine path), so cross-group copies of the same shred collapse even though that tail
//!   differs. A shred sharing `(slot, index, type)` but carrying different signed content still
//!   forwards (loss-averse — without sigverify we can't tell which copy is valid). This is the cheap
//!   suppressor for the multicast-overlap duplicates DoubleZero delivers across its several shred
//!   groups — forgery protection is moot on the trusted network, so sigverify isn't required.
//! - **Bare forward** ([`DedupMode::None`]) → every datagram is fanned out (the original behaviour).
//! - **Dedup + sigverify** ([`DedupMode::Sigverify`]; also requires `--shred-rpc-url`) → forward
//!   exactly **one valid copy** of each shred. The forwarder keys the same bounded, prefer-valid
//!   dedup window on `(slot, index, type)`; the first copy of a key is ed25519-verified ([`verify`])
//!   against its slot leader ([`leader`]) using fields pulled from the raw datagram ([`parse`]). A
//!   verified copy is forwarded and recorded; later duplicates are dropped *without* a signature
//!   check; an invalid copy is dropped but leaves the key open so a later valid copy can still win. A
//!   slot whose leader isn't known fails **closed** (drop — an unverifiable shred is never
//!   forwarded). The leader schedule prefetches the next epoch ([`leader`]), so a known leader is
//!   normally always available across a rollover; an unknown leader means cold start, an RPC outage
//!   past the prefetch lead (~an epoch), or a garbled schedule, and is counted as `no_leader`. (Want
//!   forward-when-unverified? That is exactly dedup-only mode — sigverify deliberately does not
//!   degrade into it.)

pub mod dedup;
pub mod discovery;
pub mod leader;
pub mod parse;
pub mod verify;

#[cfg(test)]
mod fixture_tests;

use std::{
    net::{Ipv4Addr, SocketAddr, SocketAddrV4},
    str::FromStr,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};

use anyhow::{Context, Result};
use tokio::{
    net::UdpSocket,
    sync::mpsc::{self, error::TrySendError},
    task::JoinSet,
    time::timeout,
};
use tracing::{info, warn};

use crate::{
    ingest::receiver::{bind_multicast, wait_for_interface_ip},
    metrics::metrics,
};
use dedup::{Action, DedupWindow};
use leader::LeaderSchedule;

/// Re-resolve the interface and rejoin if no shred arrives for this long — the same watchdog idea
/// as the market-data receiver's `IDLE_REJOIN`, guarding against a join that landed on the wrong
/// interface (e.g. the bridge started before `doublezero1` had an IP) or a wedged socket.
const IDLE_REJOIN: Duration = Duration::from_secs(30);

/// Bounded mpsc depth between the receivers and the single forwarder. Shreds are loss-tolerant
/// (the validator recovers via Turbine/repair), so a full queue sheds load rather than blocking.
const CHANNEL_CAPACITY: usize = 8192;

/// Log the running drop total every N drops (rate-limited so sustained backpressure doesn't spam).
const DROP_LOG_EVERY: u64 = 1000;

/// Emit a forwarder verify/dedup tally every N parsed shreds. The valid/invalid ratio is the
/// canary for a systematic shred-parse misread: ≈100% "invalid" means the (unvalidated) offsets are
/// wrong, not that the network is full of forged shreds.
const VERIFY_LOG_EVERY: u64 = 100_000;

/// Back off this long before retrying a failed multicast bind/join, so a transient bind error
/// (e.g. `EADDRNOTAVAIL` while the interface is still settling, or a fast flap) is retried rather
/// than killing the receiver — and with it the whole process.
const BIND_RETRY: Duration = Duration::from_secs(1);

/// Per-receiver recv buffer length. Solana shreds are ~1232B plus headers, comfortably under this,
/// so a datagram that exactly fills the buffer is treated as truncated/unexpected and dropped
/// rather than forwarded corrupt (see [`receiver_task`]).
const RECV_BUF_LEN: usize = 2048;

/// Length of the trailing **retransmitter signature** on resigned merkle shreds. It is rewritten per
/// turbine path, so the dedup-only fingerprint excludes these last bytes to collapse cross-group
/// copies that are otherwise identical (see the dedup-only branch in [`forwarder_task`]).
const RETRANSMITTER_SIG_LEN: usize = parse::SIZE_OF_SIGNATURE;

/// One forwarded datagram: just the shred-bearing UDP payload bytes. (A small reuse pool is a
/// possible later optimization; not needed now.)
pub type ShredPacket = Vec<u8>;

/// How the forwarder deduplicates, selected by `--shred-dedup-mode` / `DZ_SHRED_DEDUP_MODE`. This
/// single setting is the only method selector — there is no implicit promotion from `--shred-rpc-url`
/// (which is just the endpoint parameter consumed by [`DedupMode::Sigverify`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, clap::ValueEnum)]
pub enum DedupMode {
    /// **Default.** Forward one copy per `(slot, index, type, content-fingerprint)`; no signature
    /// verification, leader lookup, or RPC. Collapses DoubleZero's multicast-overlap duplicates.
    #[default]
    Dedup,
    /// Dedup + ed25519 sigverify of the one forwarded copy against its slot leader. Requires a
    /// reachable RPC via `--shred-rpc-url`.
    Sigverify,
    /// Forward every datagram, duplicates and all (the original bare behaviour).
    None,
}

/// Resolved configuration for the shred forwarder. Built in `main` from the CLI/env flags.
#[derive(Debug, Clone)]
pub struct ShredConfig {
    /// Interface to join the groups on (name or IPv4) — reuses `--iface`.
    pub iface: String,
    /// Kernel socket receive buffer (SO_RCVBUF) per receiver socket — reuses `--recv-buf`.
    pub recv_buf: usize,
    /// Source multicast groups (group:port) to join and combine.
    pub sources: Vec<SocketAddrV4>,
    /// Local destinations every datagram is fanned out to.
    pub forward: Vec<SocketAddr>,
    /// Deduplication mode. The single source of truth for forwarder behaviour.
    pub mode: DedupMode,
    /// Solana JSON-RPC endpoint for the leader schedule. Consumed only when `mode` is
    /// [`DedupMode::Sigverify`] (and required there); ignored otherwise.
    pub rpc_url: Option<String>,
    /// Dedup window depth in slots: keys older than this many slots behind the tip are evicted, so
    /// memory is bounded by `window × shreds-per-slot`.
    pub dedup_window_slots: u64,
}

/// Parse repeatable `--shred-forward host:port` values into socket addresses, failing fast on the
/// first invalid one so a typo'd destination is caught at startup, not silently dropped.
pub fn parse_forwards(raw: &[String]) -> Result<Vec<SocketAddr>> {
    raw.iter()
        .map(|s| {
            SocketAddr::from_str(s)
                .with_context(|| format!("invalid --shred-forward '{s}' (expected host:port)"))
        })
        .collect()
}

/// Parse repeatable `--shred-source GROUP:PORT` overrides into `SocketAddrV4`, failing fast on the
/// first invalid one.
pub fn parse_sources(raw: &[String]) -> Result<Vec<SocketAddrV4>> {
    raw.iter()
        .map(|s| {
            SocketAddrV4::from_str(s)
                .with_context(|| format!("invalid --shred-source '{s}' (expected GROUP:PORT)"))
        })
        .collect()
}

/// Resolve the source groups: an explicit `--shred-source` list overrides discovery entirely
/// (for tests/edge cases); otherwise discover `<prefix>*` groups via the `doublezero` CLI and bind
/// each on `port`. Returns an empty list when neither yields anything (the caller then leaves the
/// shred pipeline inactive — activate-on-discovery, consistent with the "sink active when its
/// config is non-empty" rule).
pub fn resolve_sources(explicit: &[String], prefix: &str, port: u16) -> Result<Vec<SocketAddrV4>> {
    if !explicit.is_empty() {
        return parse_sources(explicit);
    }
    Ok(discovery::discover_groups(prefix, port))
}

/// Run the shred forwarder: spawn one receiver task per source group plus a single forwarder task,
/// wired by a bounded mpsc. Loops forever; returns only when a task exits — the forwarder failing
/// to bind its send socket, or (the normal terminal case) the channel closing once every receiver
/// is gone. Receiver bind failures are retried, not propagated (see [`receiver_task`]), so a
/// flapping Solana interface never brings the whole process down.
pub async fn run(cfg: ShredConfig) -> Result<()> {
    info!(sources = ?cfg.sources, forward = ?cfg.forward, iface = %cfg.iface,
          "starting shred forwarder");

    let (tx, rx) = mpsc::channel::<ShredPacket>(CHANNEL_CAPACITY);
    let dropped = Arc::new(AtomicU64::new(0));

    let mut tasks: JoinSet<Result<()>> = JoinSet::new();

    // The mode is the single source of truth. Sigverify builds a leader schedule (and requires an
    // RPC URL — main rejects `Sigverify` without one before we get here); dedup-only and bare run
    // with no schedule, distinguished by the `dedup` bool passed to the forwarder below.
    let schedule = match cfg.mode {
        DedupMode::Sigverify => {
            let url = cfg.rpc_url.clone().expect(
                "sigverify mode requires an RPC URL; main validates this before building ShredConfig",
            );
            warn!(
                "shred sigverify enabled: shred/merkle offsets are transcribed from the agave \
                 layout and NOT validated against a live edge-solana hexdump — watch the periodic \
                 verify tally and confirm against a captured frame before trusting it"
            );
            let sched = Arc::new(LeaderSchedule::new(url));
            let refresher = Arc::clone(&sched);
            tasks.spawn(async move {
                refresher.run_refresher().await; // loops forever
                Ok(())
            });
            Some(sched)
        }
        DedupMode::Dedup => {
            info!(
                "shred dedup-only enabled (default; no sigverify, no RPC); forwarding one copy per \
                 (slot, index, type)"
            );
            None
        }
        DedupMode::None => {
            info!("shred dedup disabled (--shred-dedup-mode none); forwarding every datagram");
            None
        }
    };

    tasks.spawn(forwarder_task(
        rx,
        cfg.forward.clone(),
        schedule,
        cfg.mode == DedupMode::Dedup,
        cfg.dedup_window_slots,
    ));
    for src in cfg.sources {
        tasks.spawn(receiver_task(
            src,
            cfg.iface.clone(),
            cfg.recv_buf,
            tx.clone(),
            Arc::clone(&dropped),
        ));
    }
    // Drop the original sender so the forwarder's channel closes (and it exits) once every
    // receiver task has gone, rather than hanging on a sender that never sends.
    drop(tx);

    // The tasks loop forever; the first to return is terminal (fatal bind error or closed channel).
    if let Some(joined) = tasks.join_next().await {
        return joined.context("shred task panicked")?;
    }
    Ok(())
}

/// One receiver: bind the group's multicast socket and push every received datagram onto the mpsc.
/// Plain `recv` (kernel RX timestamps are NOT needed here, unlike the market-data path). Survives
/// interface flap via the [`IDLE_REJOIN`] watchdog: on idle/error it re-resolves the interface and
/// rebinds. A failed bind is retried (with [`BIND_RETRY`] backoff), never fatal — the shred
/// forwarder is an optional add-on and must not take the market-data bridge down with it. Returns
/// only when the forwarder's channel has closed (all consumers gone).
async fn receiver_task(
    src: SocketAddrV4,
    iface: String,
    recv_buf: usize,
    tx: mpsc::Sender<ShredPacket>,
    dropped: Arc<AtomicU64>,
) -> Result<()> {
    let group = *src.ip();
    let port = src.port();
    let mut buf = vec![0u8; RECV_BUF_LEN];

    // Per-group metric handles resolved once (the group address is fixed for this task).
    let group_label = group.to_string();
    let received_ctr = metrics()
        .shred_datagrams_received
        .with_label_values(&[&group_label]);
    let bytes_ctr = metrics()
        .shred_datagram_bytes
        .with_label_values(&[&group_label]);
    let dropped_ctr = metrics()
        .shred_receiver_dropped
        .with_label_values(&[&group_label]);

    'rejoin: loop {
        // Wait for the interface to acquire an IPv4 before joining, so we don't race the tunnel
        // coming up and fall back to the default interface (mirrors the market-data receiver).
        let iface_ip = wait_for_interface_ip(&iface, Duration::from_secs(60)).await;
        let sock = match bind_multicast(group, port, iface_ip, recv_buf) {
            Ok(sock) => sock,
            Err(e) => {
                // Non-fatal: back off and retry rather than propagate (which would terminate the
                // whole process via main's `select!`). A flapping interface or a transient bind
                // error must not take the market-data bridge down with the shred forwarder.
                warn!(%group, port, %iface_ip, %e, "shred multicast bind failed; retrying");
                tokio::time::sleep(BIND_RETRY).await;
                continue 'rejoin;
            }
        };
        info!(%group, port, %iface, %iface_ip, recv_buf, "shred multicast receiver bound");

        loop {
            let mut guard = match timeout(IDLE_REJOIN, sock.readable()).await {
                Ok(Ok(g)) => g,
                Ok(Err(e)) => {
                    warn!(%group, %e, "shred recv readiness error; rejoining");
                    continue 'rejoin;
                }
                Err(_) => {
                    warn!(%group, idle_s = IDLE_REJOIN.as_secs(),
                          "no shreds; re-resolving interface and rejoining");
                    continue 'rejoin;
                }
            };
            match guard.try_io(|s| s.get_ref().recv(&mut buf)) {
                Ok(Ok(0)) => {} // empty datagram: nothing to forward
                Ok(Ok(n)) => {
                    // A datagram that exactly fills the buffer was truncated by `recv` (no
                    // MSG_TRUNC requested, so the tail is silently lost). Solana shreds are well
                    // under 2048B, so this is mis-bound / unexpected traffic rather than a real
                    // shred — drop it rather than forward a corrupt partial datagram downstream.
                    if n == buf.len() {
                        warn!(%group, len = n,
                              "shred datagram filled the recv buffer (likely truncated); dropping");
                        continue;
                    }
                    received_ctr.inc();
                    bytes_ctr.inc_by(n as u64);
                    match tx.try_send(buf[..n].to_vec()) {
                        Ok(()) => {}
                        Err(TrySendError::Full(_)) => {
                            dropped_ctr.inc();
                            // Forwarder backpressure: shed the NEWEST datagram (this one) and count
                            // it. `try_send` rejects the incoming datagram on a full queue; a
                            // producer can't evict the queue head, so this is drop-newest, not
                            // drop-oldest. For loss-tolerant shreds either is fine (the validator
                            // recovers via Turbine/repair).
                            let total = dropped.fetch_add(1, Ordering::Relaxed) + 1;
                            if total.is_multiple_of(DROP_LOG_EVERY) {
                                warn!(%group, dropped = total,
                                  "shred forwarder backpressure; dropping datagrams");
                            }
                        }
                        Err(TrySendError::Closed(_)) => {
                            warn!(%group, "shred forwarder gone; receiver exiting");
                            return Ok(());
                        }
                    }
                }
                Ok(Err(e)) => {
                    warn!(%group, %e, "shred recv error; rejoining");
                    continue 'rejoin;
                }
                // The socket was not actually ready (spurious wakeup): re-arm readability.
                Err(_would_block) => continue,
            }
        }
    }
}

/// The single forwarder: fan accepted datagrams out to all destinations. A failing destination is
/// logged and skipped — it never blocks delivery to the others. Returns `Ok` when the channel
/// closes (all receivers gone).
///
/// **One send socket per destination, each `connect`ed to its peer.** A shared socket would leak
/// async ICMP errors across destinations: on Linux a `send_to` to a local port with no listener
/// makes the kernel queue an `ECONNREFUSED` that is then delivered on the *next* socket operation
/// regardless of target — so a down destination could fail (and drop) the *next* send to a
/// *healthy* one, and mis-attribute the error. A connected socket only ever surfaces ICMP errors
/// for its own peer, so each destination's failures stay isolated to its own socket.
///
/// When `schedule` is `Some`, each datagram passes the prefer-valid dedup + sigverify gate before
/// fan-out (see the module docs); when `None` and `dedup` is set, it passes a sigverify-free dedup
/// gate keyed on `(slot, index, type)`; when neither, every datagram is forwarded (the bare behaviour).
///
/// Sends are sequential per destination, so effective forwarder throughput is ~`1/M` of a single
/// send and a slow destination sheds load globally (the bounded channel fills and receivers drop).
/// That is fine for the intended use — a few **local unicast** destinations whose sends don't block
/// — but don't point `--shred-forward` at a slow/remote sink. Each socket binds `0.0.0.0:0` and
/// pins no egress interface, so destinations should be loopback/local, not off-box (a non-loopback
/// destination is warned about at startup, since it would route out the default interface).
///
/// Send-socket setup (bind + connect) is one-shot: a failure here returns `Err` and disables the
/// shred forwarder for the rest of the process (main logs it and keeps the market-data bridge
/// running — it is never fatal to the bridge). This is deliberate — binding `0.0.0.0:0` and a
/// connectionless UDP `connect` essentially only fail on fd/memory exhaustion, which is better
/// surfaced loudly than retried in a tight loop. Receiver *joins*, by contrast, do retry (a
/// flapping multicast interface is an expected transient).
///
/// When `schedule` is `Some`, each datagram passes the prefer-valid dedup + sigverify gate before
/// fan-out; when `None` and `dedup`, a sigverify-free dedup gate; when neither, every datagram is
/// forwarded (the bare behaviour).
async fn forwarder_task(
    mut rx: mpsc::Receiver<ShredPacket>,
    dests: Vec<SocketAddr>,
    schedule: Option<Arc<LeaderSchedule>>,
    dedup: bool,
    dedup_window_slots: u64,
) -> Result<()> {
    // Build one connected send socket per destination so ICMP errors can't cross destinations.
    // `connect` on a UDP socket only sets the default peer (no handshake), so it succeeds even for
    // a destination with nothing listening — a port-unreachable surfaces later, on this socket's
    // own next `send`.
    let mut socks: Vec<Dest> = Vec::with_capacity(dests.len());
    for dst in &dests {
        if !dst.ip().is_loopback() {
            // The send sockets pin no egress interface, so a non-loopback target routes out the
            // default interface (possibly the DZ tunnel) and forwards raw, unverified shreds
            // off-box. Intended use is a local sink; warn loudly rather than silently relay.
            warn!(%dst, "shred forward destination is not loopback; intended use is a local sink");
        }
        let sock = UdpSocket::bind((Ipv4Addr::UNSPECIFIED, 0))
            .await
            .with_context(|| format!("binding shred forward send socket for {dst}"))?;
        sock.connect(dst)
            .await
            .with_context(|| format!("connecting shred forward socket to {dst}"))?;
        let dst_label = dst.to_string();
        socks.push(Dest {
            addr: *dst,
            sock,
            sends_ok: metrics().shred_sends.with_label_values(&[&dst_label, "ok"]),
            sends_err: metrics()
                .shred_sends
                .with_label_values(&[&dst_label, "error"]),
            bytes_sent: metrics().shred_bytes_sent.with_label_values(&[&dst_label]),
        });
    }
    // Dedup runs whenever sigverify is on (it dedups too) or dedup-only is requested.
    let dedup_active = schedule.is_some() || dedup;
    info!(
        ?dests,
        sigverify = schedule.is_some(),
        dedup = dedup_active,
        "shred forwarder ready"
    );

    let m = metrics();
    let mut window = DedupWindow::new(dedup_window_slots);
    // Tallies for the periodic misparse canary (see VERIFY_LOG_EVERY). `unparsed` is tracked too so
    // a *total* misparse (every datagram rejected by `parse`) still trips the log — otherwise it
    // would be silent, since `parsed` would never advance.
    let (
        mut processed,
        mut parsed,
        mut unparsed,
        mut forwarded,
        mut dropped,
        mut verify_ok,
        mut no_leader,
    ) = (0u64, 0u64, 0u64, 0u64, 0u64, 0u64, 0u64);
    // Fire one loud warning the first time a shred is dropped for want of a leader, so an
    // RPC-down-at-boot blackout (sigverify forwards nothing) is obvious without scraping the tally.
    let mut no_leader_warned = false;

    while let Some(pkt) = rx.recv().await {
        // Bare mode (no sigverify, no dedup): forward unconditionally.
        if !dedup_active {
            fan_out(&socks, &pkt).await;
            continue;
        }
        processed += 1;
        m.shred_processed.inc();

        // Dedup (with or without sigverify). An unparseable datagram can't be keyed, so forward it
        // rather than silently drop (loss-averse; it simply isn't deduped).
        let Some(meta) = parse::parse(&pkt) else {
            unparsed += 1;
            forwarded += 1;
            m.shred_unparsed.inc();
            m.shred_forwarded.inc();
            fan_out(&socks, &pkt).await;
            log_tally(
                processed, parsed, unparsed, forwarded, dropped, verify_ok, no_leader, &window,
            );
            continue;
        };
        parsed += 1;
        m.shred_parsed.inc();

        let action = match schedule.as_ref() {
            // Dedup + sigverify: the first copy of a key is ed25519-verified against its leader.
            // **Fail closed**: a slot with no known leader can't be verified, so it is dropped rather
            // than forwarded unverified. The next epoch is prefetched, so a known leader is normally
            // always available; `None` here means cold start, a sustained RPC outage past the
            // prefetch lead, or a garbled schedule. (For forward-when-unverified, use dedup-only.)
            Some(schedule) => match schedule.leader(meta.slot).await {
                Some(pk) => {
                    let mut verify_fn = || {
                        let ok = verify::verify(&meta, &pk);
                        if ok {
                            verify_ok += 1;
                            m.shred_verify_ok.inc();
                        }
                        ok
                    };
                    // Sigverify keys content-agnostically (fingerprint = 0): the signature, not the
                    // bytes, decides the valid winner, so a forged copy with different bytes
                    // collapses onto the same key and is dropped on verify — not earn its own.
                    window.decide(meta.slot, meta.index, meta.shred_type, 0, &mut verify_fn)
                }
                None => {
                    no_leader += 1;
                    m.shred_no_leader.inc();
                    if !no_leader_warned {
                        no_leader_warned = true;
                        warn!(
                            "shred sigverify: no leader for a slot — dropping unverifiable shreds \
                             (fail closed). If the RPC is unreachable this is a full blackout until \
                             the leader schedule loads; watch the no_leader tally."
                        );
                    }
                    Action::Drop
                }
            },
            // Dedup-only: no leader lookup, no signature work. The first copy of a key always "wins"
            // (verify -> true), so it is forwarded + recorded and later copies drop. The key carries
            // a content fingerprint, so only copies matching over the fingerprinted region dedup — a
            // same-(slot, index, type) shred with different content still forwards (loss-averse; we
            // can't tell which is valid without sigverify).
            //
            // Resigned merkle shreds carry a per-turbine-path **retransmitter signature** in the
            // trailing 64 bytes, rewritten on every hop, so cross-group copies of the *same* shred
            // differ only there. Fingerprinting the whole datagram would give each its own key and
            // dedup none of them, so the fingerprint excludes that tail; everything before it (leader
            // sig, headers, payload, merkle proof, chained root) is identical across copies. This
            // needs only the already-decoded `resigned` flag and the datagram length, not the
            // unvalidated merkle offsets. `verify_ok` stays 0 — nothing is signature-checked here.
            None => {
                let region = if meta.resigned && pkt.len() >= RETRANSMITTER_SIG_LEN {
                    &pkt[..pkt.len() - RETRANSMITTER_SIG_LEN]
                } else {
                    &pkt[..]
                };
                window.decide(
                    meta.slot,
                    meta.index,
                    meta.shred_type,
                    dedup::fingerprint(region),
                    &mut || true,
                )
            }
        };
        match action {
            Action::Forward => {
                forwarded += 1;
                m.shred_forwarded.inc();
                fan_out(&socks, &pkt).await;
            }
            Action::Drop => {
                dropped += 1;
                m.shred_dropped.inc();
            }
        }

        log_tally(
            processed, parsed, unparsed, forwarded, dropped, verify_ok, no_leader, &window,
        );
    }
    info!("shred forwarder channel closed; exiting");
    Ok(())
}

/// One forward destination: its address, a connected send socket, and the pre-resolved per-outcome
/// send counters so [`fan_out`] records without a per-send label lookup.
struct Dest {
    addr: SocketAddr,
    sock: UdpSocket,
    sends_ok: prometheus::IntCounter,
    sends_err: prometheus::IntCounter,
    bytes_sent: prometheus::IntCounter,
}

/// Fan one datagram out to every connected destination socket, logging and skipping any that fails.
async fn fan_out(socks: &[Dest], pkt: &[u8]) {
    for dest in socks {
        match dest.sock.send(pkt).await {
            Ok(_) => {
                dest.sends_ok.inc();
                dest.bytes_sent.inc_by(pkt.len() as u64);
            }
            Err(e) => {
                dest.sends_err.inc();
                warn!(dst = %dest.addr, %e, "shred forward send failed; skipping this destination");
            }
        }
    }
}

/// Emit the dedup/verify tally every [`VERIFY_LOG_EVERY`] processed datagrams. The `verify_ok`
/// share of `parsed` (and a high `unparsed`) is the canary for a systematic shred-parse misread; a
/// rising `no_leader` (sigverify mode) flags shreds dropped fail-closed for want of a leader
/// schedule — cold start, a sustained RPC outage past the prefetch lead, or a garbled schedule.
#[allow(clippy::too_many_arguments)]
fn log_tally(
    processed: u64,
    parsed: u64,
    unparsed: u64,
    forwarded: u64,
    dropped: u64,
    verify_ok: u64,
    no_leader: u64,
    window: &DedupWindow,
) {
    if processed.is_multiple_of(VERIFY_LOG_EVERY) {
        let tracked_slots = window.tracked_slots();
        metrics()
            .shred_dedup_tracked_slots
            .set(tracked_slots as i64);
        info!(
            processed,
            parsed,
            unparsed,
            forwarded,
            dropped,
            verify_ok,
            no_leader,
            tracked_slots,
            "shred forwarder dedup/verify tally"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_dedup_mode_is_dedup_only() {
        // The out-of-the-box behaviour is dedup-only (collapse multicast-overlap duplicates), not
        // the old bare forward-everything. Pin it so a default change can't slip in silently.
        assert_eq!(DedupMode::default(), DedupMode::Dedup);
    }

    #[test]
    fn parse_forwards_accepts_valid_and_rejects_invalid() {
        let ok = parse_forwards(&["127.0.0.1:20000".into(), "10.0.0.1:7000".into()]).unwrap();
        assert_eq!(ok.len(), 2);
        assert_eq!(ok[0], "127.0.0.1:20000".parse::<SocketAddr>().unwrap());

        assert!(parse_forwards(&["not-an-addr".into()]).is_err());
        assert!(parse_forwards(&["127.0.0.1".into()]).is_err()); // missing port
    }

    #[test]
    fn parse_sources_accepts_group_port_and_rejects_invalid() {
        let ok = parse_sources(&["233.84.178.1:7733".into()]).unwrap();
        assert_eq!(
            ok,
            vec!["233.84.178.1:7733".parse::<SocketAddrV4>().unwrap()]
        );

        assert!(parse_sources(&["233.84.178.1".into()]).is_err()); // missing port
        assert!(parse_sources(&["garbage".into()]).is_err());
    }

    #[test]
    fn resolve_sources_prefers_explicit_override() {
        // Explicit override is used verbatim and never touches discovery (the CLI isn't required).
        let got = resolve_sources(&["233.84.178.1:7733".into()], "edge-solana-", 7733).unwrap();
        assert_eq!(
            got,
            vec!["233.84.178.1:7733".parse::<SocketAddrV4>().unwrap()]
        );
    }

    // --- Loopback forwarding integration tests ---
    //
    // No `lib` target exists (the crate is a single `[[bin]]`; integration tests spawn the binary
    // as a subprocess), so the multicast→fan-out path is exercised here as in-process async tests.
    // Each uses a distinct admin-scoped group so they stay isolated under parallel `cargo test`
    // (`bind_multicast` binds to the group address, not INADDR_ANY). Sender mirrors the E2E
    // harness: multicast-loopback on, TTL 1, no pinned interface — locally-sent datagrams reach a
    // `--iface 0.0.0.0` receiver without `lo` needing the MULTICAST flag.
    // (`UdpSocket`/`timeout` come from the module-scope `use` via `use super::*`.)

    async fn loopback_sender() -> UdpSocket {
        let sock = UdpSocket::bind((Ipv4Addr::UNSPECIFIED, 0)).await.unwrap();
        sock.set_multicast_loop_v4(true).unwrap();
        sock.set_multicast_ttl_v4(1).unwrap();
        sock
    }

    /// Spawn `run` with the given config, then send probe datagrams to `dst` until `probe_listener`
    /// receives one — proving the receiver has joined the group — and drain the probes. Returns the
    /// sender so the caller can send the real batch.
    async fn warmup(sender: &UdpSocket, dst: SocketAddrV4, probe_listener: &UdpSocket) {
        let mut buf = [0u8; 2048];
        for _ in 0..100 {
            sender.send_to(b"probe", dst).await.unwrap();
            if timeout(Duration::from_millis(50), probe_listener.recv(&mut buf))
                .await
                .is_ok()
            {
                break;
            }
        }
        // Drain any remaining buffered probe datagrams so the real batch counts cleanly.
        while timeout(Duration::from_millis(50), probe_listener.recv(&mut buf))
            .await
            .is_ok()
        {}
    }

    /// Count how many datagrams arrive on `listener`, stopping after an idle gap with no datagram.
    /// The idle window is generous (800ms) so a scheduled-out CI runner doesn't truncate the count
    /// mid-batch; the call sites still assert the EXACT total (no dedup), so this can't relax to a
    /// lower bound.
    async fn drain_count(listener: &UdpSocket) -> usize {
        let mut buf = [0u8; 2048];
        let mut n = 0;
        while timeout(Duration::from_millis(800), listener.recv(&mut buf))
            .await
            .is_ok()
        {
            n += 1;
        }
        n
    }

    #[tokio::test]
    async fn fan_out_delivers_to_all_destinations() {
        let l1 = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let l2 = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let (d1, d2) = (l1.local_addr().unwrap(), l2.local_addr().unwrap());

        let group = Ipv4Addr::new(239, 255, 99, 1);
        let src = SocketAddrV4::new(group, 17733);
        let cfg = ShredConfig {
            iface: "0.0.0.0".into(),
            recv_buf: 1 << 20,
            sources: vec![src],
            forward: vec![d1, d2],
            mode: DedupMode::None,
            rpc_url: None,
            dedup_window_slots: 512,
        };
        let handle = tokio::spawn(run(cfg));

        let sender = loopback_sender().await;
        warmup(&sender, src, &l1).await;
        // Drain BOTH destinations with the generous (800ms) window right before the real batch, so
        // a warmup probe that arrives late (after warmup's tight 50ms drain) can't inflate the
        // count past N on a loaded runner. l2 never saw warmup's drain at all.
        drain_count(&l1).await;
        drain_count(&l2).await;

        const N: usize = 20;
        for i in 0..N {
            sender.send_to(&[i as u8; 64], src).await.unwrap();
        }

        assert_eq!(
            drain_count(&l1).await,
            N,
            "destination 1 should receive every datagram"
        );
        assert_eq!(
            drain_count(&l2).await,
            N,
            "destination 2 should receive every datagram"
        );
        handle.abort();
    }

    #[tokio::test]
    async fn dead_destination_does_not_block_live_one() {
        let live = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let live_addr = live.local_addr().unwrap();
        // A real closed port (nothing listening on 127.0.0.1:1) so the kernel generates a genuine
        // ICMP port-unreachable, queued on that destination's socket and delivered on a *later*
        // send — the actual async ECONNREFUSED failure mode. (`127.0.0.1:0` would instead fail
        // synchronously with EINVAL and never exercise this path.) With one socket per destination
        // that error stays isolated to the dead socket and must not disturb the live one.
        let dead: SocketAddr = "127.0.0.1:1".parse().unwrap();

        let group = Ipv4Addr::new(239, 255, 99, 2);
        let src = SocketAddrV4::new(group, 17733);
        let cfg = ShredConfig {
            iface: "0.0.0.0".into(),
            recv_buf: 1 << 20,
            sources: vec![src],
            forward: vec![dead, live_addr], // dead first: it must not stop the live one after it
            mode: DedupMode::None,
            rpc_url: None,
            dedup_window_slots: 512,
        };
        let handle = tokio::spawn(run(cfg));

        let sender = loopback_sender().await;
        warmup(&sender, src, &live).await;
        // Generous drain before the real batch so no late warmup probe inflates the count past N.
        drain_count(&live).await;

        const N: usize = 20;
        for i in 0..N {
            sender.send_to(&[i as u8; 64], src).await.unwrap();
        }

        // This asserts the guarantee that matters: the live destination receives all N despite the
        // dead one ahead of it in the fan-out. It can't directly observe the dead socket's async
        // ECONNREFUSED (which arrives out-of-band, possibly after the batch) — but with one
        // connected socket per destination, that error is confined to the dead socket and cannot
        // perturb this count regardless of when it lands. A regression to a shared socket would
        // intermittently drop a live datagram here.
        assert_eq!(
            drain_count(&live).await,
            N,
            "the live destination must receive every datagram despite the dead one"
        );
        handle.abort();
    }

    // --- Sigverify + dedup forwarder tests ---
    //
    // These drive `forwarder_task` directly over the mpsc with a pre-seeded `LeaderSchedule` (no
    // RPC), so the real parse → leader → verify → dedup path is exercised end-to-end. Shreds are
    // built as legacy data shreds (the signature covers the post-signature payload), signed with a
    // deterministic key whose pubkey is the seeded slot leader.

    const TEST_SLOT: u64 = 100;
    const TEST_INDEX: u32 = 5;

    /// Build a legacy data shred for `(TEST_SLOT, TEST_INDEX)` signed by `signing`.
    fn signed_legacy_shred(signing: &ed25519_dalek::SigningKey) -> Vec<u8> {
        use ed25519_dalek::Signer;
        let mut shred = vec![0x42u8; 1228];
        shred[64] = 0xa5; // legacy data variant
        shred[65..73].copy_from_slice(&TEST_SLOT.to_le_bytes());
        shred[73..77].copy_from_slice(&TEST_INDEX.to_le_bytes());
        let sig = signing.sign(&shred[64..]).to_bytes();
        shred[..64].copy_from_slice(&sig);
        shred
    }

    /// A schedule whose leader for `TEST_SLOT` is `pubkey`.
    fn seeded_schedule(pubkey: [u8; 32]) -> Arc<LeaderSchedule> {
        let mut leaders = vec![None; TEST_SLOT as usize + 1];
        leaders[TEST_SLOT as usize] = Some(pubkey);
        Arc::new(LeaderSchedule::with_seeded_cache(0, 0, leaders))
    }

    async fn run_forwarder(schedule: Arc<LeaderSchedule>, pkts: Vec<Vec<u8>>) -> usize {
        run_forwarder_mode(Some(schedule), false, pkts).await
    }

    /// Drive `forwarder_task` in any mode (sigverify via `schedule`, dedup-only via `dedup`, or
    /// bare) and return how many datagrams reach the single listener.
    async fn run_forwarder_mode(
        schedule: Option<Arc<LeaderSchedule>>,
        dedup: bool,
        pkts: Vec<Vec<u8>>,
    ) -> usize {
        let listener = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let dst = listener.local_addr().unwrap();
        let (tx, rx) = mpsc::channel::<ShredPacket>(64);
        let handle = tokio::spawn(forwarder_task(rx, vec![dst], schedule, dedup, 512));
        for pkt in pkts {
            tx.send(pkt).await.unwrap();
        }
        drop(tx); // close the channel so the forwarder drains and exits
        handle.await.unwrap().unwrap();
        drain_count(&listener).await
    }

    #[tokio::test]
    async fn same_shred_from_two_sources_forwards_once() {
        let signing = ed25519_dalek::SigningKey::from_bytes(&[3u8; 32]);
        let schedule = seeded_schedule(signing.verifying_key().to_bytes());
        let shred = signed_legacy_shred(&signing);
        // The identical valid shred arrives twice (leader copy + retransmit copy).
        let got = run_forwarder(schedule, vec![shred.clone(), shred]).await;
        assert_eq!(got, 1, "exactly one valid copy is forwarded");
    }

    #[tokio::test]
    async fn bad_signature_dropped_good_forwarded() {
        let signing = ed25519_dalek::SigningKey::from_bytes(&[4u8; 32]);
        let schedule = seeded_schedule(signing.verifying_key().to_bytes());
        // A shred signed by the wrong key: leader is known, signature fails -> dropped.
        let wrong = ed25519_dalek::SigningKey::from_bytes(&[99u8; 32]);
        let got = run_forwarder(schedule, vec![signed_legacy_shred(&wrong)]).await;
        assert_eq!(got, 0, "an invalid shred must not be forwarded");
    }

    #[tokio::test]
    async fn prefer_valid_bad_copy_first_then_good_copy_forwards() {
        let signing = ed25519_dalek::SigningKey::from_bytes(&[5u8; 32]);
        let schedule = seeded_schedule(signing.verifying_key().to_bytes());
        let good = signed_legacy_shred(&signing);
        // Forged copy (payload tampered after signing) of the SAME key arrives first.
        let mut bad = good.clone();
        bad[200] ^= 0xff;
        let got = run_forwarder(schedule, vec![bad, good]).await;
        assert_eq!(
            got, 1,
            "the later valid copy still wins and forwards exactly once"
        );
    }

    #[tokio::test]
    async fn unknown_leader_fails_closed() {
        // Schedule has no leader for TEST_SLOT -> leader() is None -> drop (fail closed). Sigverify
        // mode forwards only shreds it can verify; an unverifiable shred is never forwarded. (For the
        // forward-everything-when-unverified behaviour, use dedup-only mode, which is exactly that.)
        let schedule = Arc::new(LeaderSchedule::with_seeded_cache(0, 0, vec![None; 10]));
        let signing = ed25519_dalek::SigningKey::from_bytes(&[6u8; 32]);
        let shred = signed_legacy_shred(&signing);
        let got = run_forwarder(schedule, vec![shred.clone(), shred]).await;
        assert_eq!(
            got, 0,
            "with no known leader the shred cannot be verified, so it is dropped"
        );
    }

    // --- Dedup-only forwarder tests (no RPC, no sigverify) ---
    //
    // `schedule = None` + `dedup = true`: the forwarder dedups on `(slot, index, type)` without any
    // leader lookup or signature work. Signatures on the test shreds are irrelevant in this mode.

    /// A legacy data shred for `(TEST_SLOT, index)` with arbitrary (unsigned) bytes — parse only
    /// needs the variant/slot/index fields, and dedup-only never verifies the signature.
    fn legacy_shred_at_index(index: u32) -> Vec<u8> {
        let mut shred = vec![0x42u8; 1228];
        shred[64] = 0xa5; // legacy data variant
        shred[65..73].copy_from_slice(&TEST_SLOT.to_le_bytes());
        shred[73..77].copy_from_slice(&index.to_le_bytes());
        shred
    }

    #[tokio::test]
    async fn dedup_only_without_rpc_suppresses_duplicates() {
        // The same shred arrives twice (e.g. from two overlapping multicast groups): one is forwarded.
        // The shred carries a garbage (unsigned) signature, so this also proves dedup-only never
        // signature-checks — an unverifiable shred is still forwarded, unlike sigverify mode where a
        // bad signature is dropped (see `bad_signature_dropped_good_forwarded`).
        let shred = legacy_shred_at_index(TEST_INDEX);
        let got = run_forwarder_mode(None, true, vec![shred.clone(), shred]).await;
        assert_eq!(got, 1, "dedup-only forwards exactly one copy per key");
    }

    #[tokio::test]
    async fn dedup_only_collapses_many_copies_to_one() {
        // The production case: DoubleZero delivers the same shred on several overlapping multicast
        // groups (the measured mode was 3 copies). All copies of one key collapse to a single forward.
        let shred = legacy_shred_at_index(TEST_INDEX);
        let got = run_forwarder_mode(
            None,
            true,
            vec![shred.clone(), shred.clone(), shred.clone(), shred],
        )
        .await;
        assert_eq!(got, 1, "all copies of a key collapse to one forward");
    }

    #[tokio::test]
    async fn dedup_only_forwards_unparseable_undeduped() {
        // A datagram too short to parse can't be keyed, so dedup-only forwards every copy rather than
        // silently dropping it (loss-averse) — it simply isn't deduplicated.
        let junk = vec![0u8; 16]; // < SIZE_OF_COMMON_HEADER -> parse() returns None
        let got = run_forwarder_mode(None, true, vec![junk.clone(), junk]).await;
        assert_eq!(
            got, 2,
            "unparseable datagrams are forwarded undeduped, not dropped"
        );
    }

    #[tokio::test]
    async fn dedup_only_distinct_shreds_both_forward() {
        // Different index -> distinct key -> both forwarded (the key must discriminate, not over-drop).
        let got = run_forwarder_mode(
            None,
            true,
            vec![legacy_shred_at_index(1), legacy_shred_at_index(2)],
        )
        .await;
        assert_eq!(got, 2, "distinct shreds are not deduped against each other");
    }

    #[tokio::test]
    async fn dedup_disabled_forwards_every_copy() {
        // Neither sigverify nor dedup: the bare default still forwards every copy.
        let shred = legacy_shred_at_index(TEST_INDEX);
        let got = run_forwarder_mode(None, false, vec![shred.clone(), shred]).await;
        assert_eq!(got, 2, "bare mode forwards every datagram");
    }
}
