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
//! Two modes, selected by whether a Solana RPC URL is configured:
//! - **No RPC URL** → bare forward: every datagram is fanned out (the original behaviour).
//! - **RPC URL set** → forward exactly **one valid copy** of each shred. The forwarder keys a
//!   bounded, prefer-valid dedup window on `(slot, index, type)` ([`dedup`]); the first copy of a
//!   key is ed25519-verified ([`verify`]) against its slot leader ([`leader`]) using fields pulled
//!   from the raw datagram ([`parse`]). A verified copy is forwarded and recorded; later duplicates
//!   are dropped *without* a signature check; an invalid copy is dropped but leaves the key open so
//!   a later valid copy can still win. A slot whose leader isn't known yet fails **open** (forward,
//!   no dedup), so we never silently drop traffic we can't judge.

pub mod dedup;
pub mod discovery;
pub mod leader;
pub mod parse;
pub mod verify;

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

use crate::ingest::receiver::{bind_multicast, wait_for_interface_ip};
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

/// One forwarded datagram: just the shred-bearing UDP payload bytes. (A small reuse pool is a
/// possible later optimization; not needed now.)
pub type ShredPacket = Vec<u8>;

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
    /// Solana JSON-RPC endpoint for the leader schedule. `Some` enables dedup + sigverify; `None`
    /// keeps the bare forward-everything behaviour.
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

    // Sigverify + dedup are enabled iff an RPC URL is configured. Without it we fall back to the
    // bare forward-everything behaviour.
    let schedule = match &cfg.rpc_url {
        Some(url) => {
            warn!(
                "shred sigverify enabled: shred/merkle offsets are transcribed from the agave \
                 layout and NOT validated against a live edge-solana hexdump — watch the periodic \
                 verify tally and confirm against a captured frame before trusting it"
            );
            let sched = Arc::new(LeaderSchedule::new(url.clone()));
            let refresher = Arc::clone(&sched);
            tasks.spawn(async move {
                refresher.run_refresher().await; // loops forever
                Ok(())
            });
            Some(sched)
        }
        None => {
            info!("shred sigverify/dedup disabled (no --shred-rpc-url); forwarding every datagram");
            None
        }
    };

    tasks.spawn(forwarder_task(
        rx,
        cfg.forward.clone(),
        schedule,
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
                    match tx.try_send(buf[..n].to_vec()) {
                        Ok(()) => {}
                        Err(TrySendError::Full(_)) => {
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
/// fan-out (see the module docs); when `None`, every datagram is forwarded (the bare behaviour).
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
/// fan-out; when `None`, every datagram is forwarded (the bare behaviour).
async fn forwarder_task(
    mut rx: mpsc::Receiver<ShredPacket>,
    dests: Vec<SocketAddr>,
    schedule: Option<Arc<LeaderSchedule>>,
    dedup_window_slots: u64,
) -> Result<()> {
    // Build one connected send socket per destination so ICMP errors can't cross destinations.
    // `connect` on a UDP socket only sets the default peer (no handshake), so it succeeds even for
    // a destination with nothing listening — a port-unreachable surfaces later, on this socket's
    // own next `send`.
    let mut socks: Vec<(SocketAddr, UdpSocket)> = Vec::with_capacity(dests.len());
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
        socks.push((*dst, sock));
    }
    info!(
        ?dests,
        sigverify = schedule.is_some(),
        "shred forwarder ready"
    );

    let mut window = DedupWindow::new(dedup_window_slots);
    // Tallies for the periodic misparse canary (see VERIFY_LOG_EVERY).
    let (mut parsed, mut forwarded, mut dropped, mut verify_calls, mut verify_ok) =
        (0u64, 0, 0, 0, 0);

    while let Some(pkt) = rx.recv().await {
        // Bare mode (no RPC): forward unconditionally.
        let Some(schedule) = schedule.as_ref() else {
            fan_out(&socks, &pkt).await;
            continue;
        };

        // Sigverify + dedup mode. An unparseable datagram can't be keyed, so forward it rather than
        // silently drop (loss-averse; it simply isn't deduped).
        let Some(meta) = parse::parse(&pkt) else {
            fan_out(&socks, &pkt).await;
            continue;
        };
        parsed += 1;

        let leader = schedule.leader(meta.slot).await;
        let action = {
            let mut verify_fn = || {
                verify_calls += 1;
                let ok = leader.as_ref().is_some_and(|pk| verify::verify(&meta, pk));
                if ok {
                    verify_ok += 1;
                }
                ok
            };
            window.decide(
                meta.slot,
                meta.index,
                meta.shred_type,
                leader.is_some(),
                &mut verify_fn,
            )
        };
        match action {
            Action::Forward => {
                forwarded += 1;
                fan_out(&socks, &pkt).await;
            }
            Action::Drop => dropped += 1,
        }

        if parsed.is_multiple_of(VERIFY_LOG_EVERY) {
            info!(
                parsed,
                forwarded,
                dropped,
                verify_calls,
                verify_ok,
                tracked_slots = window.tracked_slots(),
                "shred forwarder dedup/verify tally"
            );
        }
    }
    info!("shred forwarder channel closed; exiting");
    Ok(())
}

/// Fan one datagram out to every connected destination socket, logging and skipping any that fails.
async fn fan_out(socks: &[(SocketAddr, UdpSocket)], pkt: &[u8]) {
    for (dst, sock) in socks {
        if let Err(e) = sock.send(pkt).await {
            warn!(%dst, %e, "shred forward send failed; skipping this destination");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
