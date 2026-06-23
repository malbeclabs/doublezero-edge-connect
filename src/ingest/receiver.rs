//! DoubleZero Edge multicast receiver: bind the group's port(s), decode frames, and broadcast
//! normalized `FeedMessage`s.
//!
//! The socket plumbing here is **protocol-agnostic** and shared by every edge-feed-spec feed:
//! interface resolution, the multicast join, kernel RX timestamps, the idle-rejoin watchdog and
//! the venue feed-health status. The per-protocol work (which frame magic, which messages, what
//! to emit) lives behind the [`FrameProcessor`] trait, so [`drive`] runs the same receive loop
//! over 1, 2 or 3 ports for Top-of-Book, Midpoint or Market-by-Order alike.
//!
//! Socket setup follows the DoubleZero edge-multicast-ref `kernel-receiver` reference:
//! resolve the interface name (e.g. `doublezero1`) to its IPv4 for `join_multicast_v4`,
//! set `SO_REUSEADDR`/`SO_REUSEPORT`, and a large `SO_RCVBUF`.

use std::{
    collections::HashMap,
    net::{IpAddr, Ipv4Addr, SocketAddrV4},
    os::fd::AsRawFd,
    time::Duration,
};

use anyhow::{bail, Context, Result};
use futures_util::future::select_all;
use nix::sys::socket::{
    recvmsg, setsockopt, sockopt::ReceiveTimestampns, ControlMessageOwned, MsgFlags,
    SockaddrStorage,
};
use socket2::{Domain, Protocol, Socket, Type};
use tokio::{io::unix::AsyncFd, time::timeout};
use tracing::{info, warn};

/// Re-join the multicast group(s) if no datagram arrives for this long. Guards against a join
/// that landed on the wrong interface (e.g. the bridge started before `doublezero1` had an IP and
/// fell back to the default interface) or a silently wedged socket: when it fires the receiver
/// re-resolves the interface and rebinds, so the feed self-heals without an operator restart.
const IDLE_REJOIN: Duration = Duration::from_secs(30);

/// While waiting for the configured interface to acquire an IPv4, retry this often.
const IFACE_POLL: Duration = Duration::from_millis(500);

use crate::{
    ingest::{
        arbiter::{lock, Publisher, SharedArbiter},
        feeds::{Feed, FeedKind, FeedPorts},
        processor::{MboProcessor, MidpointProcessor, TobProcessor},
    },
    metrics::metrics,
    model::{now_ns, DepthSnapshot, FeedMessage, FeedStatus, InstrumentSnapshot},
};

/// A multicast socket with kernel RX software timestamps enabled. `pub` so the shred forwarder
/// (`crate::shred`) can reuse [`bind_multicast`] without re-deriving the socket plumbing.
pub type TsSocket = AsyncFd<std::net::UdpSocket>;

/// The role a feed's port plays. The market-data stream is what the liveness watchdog tracks
/// (reference/snapshot ports keep ticking even when market data is wedged); a processor uses the
/// role to decide which message families to act on for a given datagram.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PortRole {
    /// Market data: quotes / midpoints / order deltas + trades. Resets the watchdog.
    Mktdata,
    /// Reference data: instrument definitions + manifest.
    Refdata,
    /// Market-by-Order snapshot recovery stream. (Constructed once the MBO receiver lands.)
    #[allow(dead_code)]
    Snapshot,
    /// A single port carrying everything (loopback demo): both market and reference data.
    Combined,
}

impl PortRole {
    /// Whether a processor should handle reference-data messages received on this role.
    pub fn handles_refdata(self) -> bool {
        matches!(self, PortRole::Refdata | PortRole::Combined)
    }
    /// Whether a processor should handle market-data (quote/trade/etc.) messages on this role.
    pub fn handles_mktdata(self) -> bool {
        matches!(self, PortRole::Mktdata | PortRole::Combined)
    }
    /// A stable, low-cardinality label for the metrics `role` dimension.
    fn label(self) -> &'static str {
        match self {
            PortRole::Mktdata => "mktdata",
            PortRole::Refdata => "refdata",
            PortRole::Snapshot => "snapshot",
            PortRole::Combined => "combined",
        }
    }
}

/// Per-datagram context handed to a [`FrameProcessor`]: the shared sinks plus the receive
/// timestamps and which port role the datagram arrived on. Borrowed for the duration of one
/// `on_datagram` call so the processor only needs to hold its own protocol state.
pub struct FrameCtx<'a> {
    /// `&'static` so the dedup key `(venue, instrument_id)` is allocation-free on the hot path; the
    /// venue ultimately comes from the `&'static` `FEEDS` registry.
    pub venue: &'static str,
    /// The shared pre-broadcast arbiter every ingest source emits through (dedup + fan-out).
    pub arbiter: &'a SharedArbiter,
    pub instruments: &'a InstrumentSnapshot,
    /// Kernel `SCM_TIMESTAMPNS` RX timestamp (CLOCK_REALTIME), or 0 if unavailable.
    pub kernel_rx_ts_ns: u64,
    /// User-space wall clock sampled right after the recv syscall returned.
    pub recv_ts_ns: u64,
    /// Which port this datagram arrived on.
    pub role: PortRole,
    /// Source IP of the datagram — the publisher identity. Independent publishers mirror one feed
    /// onto the same group (sharing `channel_id`), so per-publisher state (sequence tracking, MBO
    /// books) keys on this rather than the port.
    pub publisher: IpAddr,
}

impl FrameCtx<'_> {
    /// Emit a normalized message through the shared arbiter, tagged with this datagram's edge
    /// publisher so the quote floor can race it against the other sources for the tick's leadership.
    /// The brief critical section is the arbiter's admit-decision-plus-send.
    pub fn emit(&self, msg: FeedMessage) {
        lock(self.arbiter).emit(msg, Publisher::Edge(self.publisher));
    }
}

/// Protocol-specific frame handling. Implementors own their decode (they know their frame magic
/// and message set) and their persistent state (reference-data state machine, sequence trackers,
/// book state, warn-once flags), and emit normalized `FeedMessage`s via `ctx.emit`.
pub trait FrameProcessor {
    /// Decode and handle one received datagram. Errors are the processor's own concern (it logs
    /// and drops); the driver only deals with socket/transport errors.
    fn on_datagram(&mut self, buf: &[u8], ctx: &FrameCtx);
}

/// Broadcast a venue-level feed-health transition (PROTOCOL.md `status`): `"down"` when the
/// market-data multicast has gone silent past [`IDLE_REJOIN`], `"ok"` when it recovers. Consumers
/// gray out / restore the source on these. Best-effort (ignored if no subscriber is connected).
fn emit_status(arbiter: &SharedArbiter, venue: &str, state: &str, stale_ms: u64) {
    // Mirror the transition into the feed-health gauges (cheap; only fires on a down/ok edge).
    metrics()
        .feed_up
        .with_label_values(&[venue])
        .set(i64::from(state == "ok"));
    metrics()
        .feed_stale_ms
        .with_label_values(&[venue])
        .set(stale_ms as i64);
    // Status carries no business identity to dedup, so it goes straight to the broadcast sender.
    let _ = lock(arbiter).sender().send(FeedMessage::Status(FeedStatus {
        venue: venue.to_string(),
        state: state.to_string(),
        stale_ms,
        ts_ns: now_ns(),
    }));
}

/// Receive one datagram, returning `(len, kernel_rx_ns, user_recv_ns)`.
///
/// `kernel_rx_ns` is the `SCM_TIMESTAMPNS` kernel software RX timestamp (CLOCK_REALTIME,
/// taken in the driver softirq before user-space), or 0 if the kernel did not attach one.
/// `user_recv_ns` is the wall clock sampled right after the syscall returns - the
/// user-space arrival, kept so the kernel-vs-userspace jitter can be quantified.
async fn recv_with_ts(sock: &TsSocket, buf: &mut [u8]) -> Result<(usize, u64, u64, IpAddr)> {
    loop {
        let mut guard = sock.readable().await?;
        let res = guard.try_io(|inner| {
            let fd = inner.get_ref().as_raw_fd();
            let mut iov = [std::io::IoSliceMut::new(buf)];
            let mut cmsg = nix::cmsg_space!(nix::sys::time::TimeSpec);
            let r = recvmsg::<SockaddrStorage>(fd, &mut iov, Some(&mut cmsg), MsgFlags::empty())
                .map_err(std::io::Error::from)?;
            let mut kernel_ns = 0u64;
            if let Ok(cmsgs) = r.cmsgs() {
                for c in cmsgs {
                    if let ControlMessageOwned::ScmTimestampns(ts) = c {
                        kernel_ns = (ts.tv_sec() as u64) * 1_000_000_000 + ts.tv_nsec() as u64;
                    }
                }
            }
            let src = datagram_src_ip(r.address);
            Ok((r.bytes, kernel_ns, src))
        });
        match res {
            Ok(Ok((n, kernel_ns, src))) => return Ok((n, kernel_ns, now_ns(), src)),
            Ok(Err(e)) => return Err(e.into()),
            Err(_would_block) => continue,
        }
    }
}

/// The source IP of a received datagram (its `recvmsg` source address), used to demultiplex
/// independent publishers that mirror one feed onto the same multicast group. Falls back to
/// `0.0.0.0` if the kernel attached no source address.
fn datagram_src_ip(addr: Option<SockaddrStorage>) -> IpAddr {
    addr.and_then(|a| a.as_sockaddr_in().map(|s| IpAddr::V4(s.ip())))
        .unwrap_or(IpAddr::V4(Ipv4Addr::UNSPECIFIED))
}

/// Resolve an interface name (e.g. "doublezero1") to its IPv4 address via `ip`, without logging.
/// If `iface` already parses as an IPv4 address it is used directly. Returns `None` when the
/// interface does not exist yet or has no IPv4 (so the caller can wait/retry).
pub fn try_resolve_interface_ip(iface: &str) -> Option<Ipv4Addr> {
    if let Ok(ip) = iface.parse::<Ipv4Addr>() {
        return Some(ip);
    }
    let output = std::process::Command::new("ip")
        .args(["-4", "-o", "addr", "show", iface])
        .output()
        .ok()?;
    let stdout = std::str::from_utf8(&output.stdout).ok()?;
    for part in stdout.split_whitespace() {
        if let Some(ip_str) = part.split('/').next() {
            if let Ok(ip) = ip_str.parse::<Ipv4Addr>() {
                return Some(ip);
            }
        }
    }
    None
}

/// Wait until the configured interface has an IPv4, then return it. Polls every [`IFACE_POLL`],
/// logging once a second while it waits, so a multicast join always happens on the right
/// interface (e.g. `doublezero1`) rather than racing the tunnel coming up and falling back to the
/// default interface. After `max_wait` it gives up and returns `0.0.0.0` (join on the default
/// interface) so a genuinely-misconfigured interface degrades rather than hanging forever.
// `pub` so the shred forwarder (`crate::shred`) joins on the same interface with identical
// tunnel-up race handling instead of re-deriving it.
pub async fn wait_for_interface_ip(iface: &str, max_wait: Duration) -> Ipv4Addr {
    if let Some(ip) = try_resolve_interface_ip(iface) {
        return ip;
    }
    info!(%iface, "interface has no IPv4 yet; waiting before joining multicast");
    let started = std::time::Instant::now();
    let mut last_log = started;
    loop {
        tokio::time::sleep(IFACE_POLL).await;
        if let Some(ip) = try_resolve_interface_ip(iface) {
            info!(%iface, %ip, waited_ms = started.elapsed().as_millis() as u64,
                  "interface is up; joining multicast");
            return ip;
        }
        if started.elapsed() >= max_wait {
            warn!(%iface, waited_s = max_wait.as_secs(),
                  "interface still has no IPv4 after waiting; joining on 0.0.0.0 (default interface)");
            return Ipv4Addr::UNSPECIFIED;
        }
        if last_log.elapsed() >= Duration::from_secs(1) {
            last_log = std::time::Instant::now();
            info!(%iface, waited_s = started.elapsed().as_secs(), "still waiting for interface");
        }
    }
}

/// Join a UDP multicast group and return an async socket bound to `port`.
///
/// `pub` so the shred forwarder (`crate::shred`) reuses the exact bind semantics — crucially the
/// bind-to-GROUP (not INADDR_ANY) behavior documented below, which matters identically there:
/// all `edge-solana-*` groups share port 7733 and differ only by group.
pub fn bind_multicast(
    group: Ipv4Addr,
    port: u16,
    iface_ip: Ipv4Addr,
    recv_buf: usize,
) -> Result<TsSocket> {
    let socket = Socket::new(Domain::IPV4, Type::DGRAM, Some(Protocol::UDP))
        .context("creating UDP socket")?;
    socket.set_reuse_address(true)?;
    #[cfg(unix)]
    socket.set_reuse_port(true)?;

    // Bind to the multicast GROUP address (not INADDR_ANY): feeds share the mktdata/refdata
    // ports and differ only by group, so an INADDR_ANY bind puts every feed's sockets in one
    // SO_REUSEPORT set on the same port. The kernel then reuseport-hashes a group's datagrams
    // to ANY socket in that set regardless of which group each joined, so one venue's quotes
    // leak into another's receiver and get mislabeled. Binding to the group address makes the
    // kernel deliver only that group's datagrams to this socket, keeping feeds isolated.
    let bind_addr = SocketAddrV4::new(group, port);
    socket
        .bind(&bind_addr.into())
        .with_context(|| format!("binding to {group}:{port}"))?;
    socket
        .join_multicast_v4(&group, &iface_ip)
        .with_context(|| format!("join_multicast_v4 {group} on {iface_ip}"))?;
    socket
        .set_recv_buffer_size(recv_buf)
        .context("setting SO_RCVBUF")?;
    socket.set_nonblocking(true)?;

    let std_sock: std::net::UdpSocket = socket.into();
    // Kernel software RX timestamps (SCM_TIMESTAMPNS). Best-effort: if the option is not
    // supported the bridge still works, falling back to the user-space recv timestamp.
    if let Err(e) = setsockopt(&std_sock, ReceiveTimestampns, &true) {
        warn!("SO_TIMESTAMPNS unavailable on port {port}: {e}; using user-space recv ts only");
    }
    Ok(AsyncFd::new(std_sock)?)
}

/// Outcome of checking a frame's header against the per-channel sequence tracker.
///
/// Mirrors the edge-feed-spec frame-header semantics: the Sequence Number is "monotonically
/// increasing per channel ... Resets to 0 when `Reset Count` changes", and "Subscribers detect a
/// reset by comparing [`Reset Count`] against their last-seen value". Everything but
/// [`Stale`](SeqCheck::Stale) is processed.
///
/// We deliberately do *not* flag forward gaps: on the live feed the publisher's channel-0
/// sequence is effectively global/multiplexed across venues and groups, so any single multicast
/// group sees only a sparse slice of it - a "gap" per group is expected, not packet loss. Only a
/// *lower* sequence on the same group (a reorder/replay) is actionable.
#[derive(Debug, PartialEq, Eq)]
pub enum SeqCheck {
    /// First frame seen on this channel.
    First,
    /// Sequence at or above the last seen within the epoch (forward progress or a duplicate of
    /// the last). Accepted.
    Ok,
    /// `Reset Count` changed: the publisher reset the channel and the sequence restarts. Accepted.
    Reset,
    /// Sequence below the last seen within the same reset epoch - a reordered or duplicated
    /// datagram carrying a now-superseded update. Dropped, so an old message never overwrites
    /// a fresher one.
    Stale,
}

/// Per-`channel_id` frame-sequence state for gap detection and stale-frame rejection on the
/// market-data feed, implementing the edge-feed-spec sequence/reset contract (see [`SeqCheck`]).
#[derive(Default)]
pub struct SeqTracker {
    /// channel_id -> (last reset_count, last accepted sequence)
    last: HashMap<u8, (u8, u64)>,
}

impl SeqTracker {
    /// Classify a frame and advance the tracker. A reset (`reset_count` differs from the
    /// last-seen value, per spec) re-anchors to this frame's sequence; otherwise the sequence
    /// is compared within the epoch. The tracker is only advanced for accepted frames, so a
    /// dropped stale frame leaves the anchor on the freshest sequence.
    pub fn check(&mut self, channel_id: u8, reset_count: u8, sequence: u64) -> SeqCheck {
        match self.last.get_mut(&channel_id) {
            None => {
                self.last.insert(channel_id, (reset_count, sequence));
                SeqCheck::First
            }
            Some(entry) => {
                let (last_reset, last_seq) = *entry;
                if reset_count != last_reset {
                    *entry = (reset_count, sequence);
                    SeqCheck::Reset
                } else if sequence < last_seq {
                    SeqCheck::Stale // do not advance: keep the anchor on the freshest sequence
                } else {
                    *entry = (reset_count, sequence);
                    SeqCheck::Ok // forward progress or a duplicate of the last sequence
                }
            }
        }
    }
}

/// A bound multicast socket together with its receive buffer and the role it plays.
struct Channel {
    role: PortRole,
    sock: TsSocket,
    buf: Vec<u8>,
    /// Pre-resolved `dz_datagrams_received_total{venue, role}` child, so the hot path increments
    /// without a per-datagram label lookup.
    dgrams: prometheus::IntCounter,
}

/// Await the next datagram across all of a feed's sockets concurrently. Returns the role it
/// arrived on, the channel index (so the caller can read that channel's buffer), the length, and
/// the kernel/user-space RX timestamps. All in-flight borrows on `channels` are released by the
/// time this returns, so the caller can index `channels[idx].buf`.
async fn recv_any(channels: &mut [Channel]) -> Result<(PortRole, usize, usize, u64, u64, IpAddr)> {
    let futs: Vec<_> = channels
        .iter_mut()
        .enumerate()
        .map(|(i, ch)| {
            let role = ch.role;
            let sock = &ch.sock;
            let buf = &mut ch.buf;
            Box::pin(async move {
                let (n, kernel_ns, recv_ns, src) = recv_with_ts(sock, buf).await?;
                Ok::<_, anyhow::Error>((role, i, n, kernel_ns, recv_ns, src))
            })
        })
        .collect();
    // `select_all` resolves on the first ready socket; dropping the rest here cancels their
    // (cancellation-safe) readability waits without consuming any datagram, releasing the borrows.
    let (res, _idx, _rest) = select_all(futs).await;
    res
}

/// The shared receive loop for one feed, generic over its [`FrameProcessor`]. Binds every port in
/// `ports` on `group`, then loops receiving datagrams and handing each to `processor`. The
/// [`IDLE_REJOIN`] watchdog tracks the **market-data** port only (reference/snapshot ports keep
/// ticking even when market data is wedged), and breaks back out to re-resolve the interface and
/// rebind - self-healing a join that landed on the wrong interface or a wedged socket.
#[allow(clippy::too_many_arguments)]
async fn drive<P: FrameProcessor>(
    group: Ipv4Addr,
    ports: Vec<(PortRole, u16)>,
    iface: String,
    recv_buf: usize,
    venue: &'static str,
    arbiter: SharedArbiter,
    instruments: InstrumentSnapshot,
    mut processor: P,
) -> Result<()> {
    // Feed-health transition tracking: true while the market-data multicast is considered down
    // (silent past IDLE_REJOIN). Persists across rejoins so we emit `down`/`ok` only on the edge.
    let mut down = false;

    // Per-feed metric handles resolved once (venue is `&'static`); the per-channel datagram counter
    // is resolved per role at bind time below.
    let m = metrics();
    let bytes_ctr = m.datagram_bytes.with_label_values(&[venue]);
    let socket_errors = m.socket_errors.with_label_values(&[venue]);
    let idle_rejoin = m.idle_rejoin.with_label_values(&[venue]);

    'rejoin: loop {
        // Wait for the interface to acquire an IPv4 before joining, so we don't race the tunnel
        // coming up and fall back to the default interface.
        let iface_ip = wait_for_interface_ip(&iface, Duration::from_secs(60)).await;

        let mut channels: Vec<Channel> = Vec::with_capacity(ports.len());
        for &(role, port) in &ports {
            let sock = bind_multicast(group, port, iface_ip, recv_buf)?;
            channels.push(Channel {
                role,
                sock,
                buf: vec![0u8; 2048],
                dgrams: m.datagrams_received.with_label_values(&[venue, role.label()]),
            });
        }
        info!(%group, ?ports, %iface, %iface_ip, recv_buf, "DZ Edge multicast receiver bound");

        // Watchdog on the market-data stream specifically: rejoin when no market-data datagram has
        // arrived for IDLE_REJOIN, regardless of refdata/snapshot (which keep ticking even when
        // market data is wedged - the exact symptom of a join on the wrong interface).
        let mut last_mkt = std::time::Instant::now();
        loop {
            let remaining = IDLE_REJOIN.saturating_sub(last_mkt.elapsed());
            if remaining.is_zero() {
                warn!(%group, idle_s = IDLE_REJOIN.as_secs(),
                      "no market data; re-resolving interface and rejoining");
                idle_rejoin.inc();
                if !down {
                    emit_status(
                        &arbiter,
                        venue,
                        "down",
                        last_mkt.elapsed().as_millis() as u64,
                    );
                    down = true;
                }
                continue 'rejoin;
            }

            let (role, idx, n, kernel_ns, recv_ns, publisher) =
                match timeout(remaining, recv_any(&mut channels)).await {
                    Ok(Ok(v)) => v,
                    Ok(Err(e)) => {
                        warn!(%group, "recv error: {e}; rejoining");
                        socket_errors.inc();
                        continue 'rejoin;
                    }
                    Err(_) => {
                        warn!(%group, idle_s = IDLE_REJOIN.as_secs(),
                              "no market data; re-resolving interface and rejoining");
                        idle_rejoin.inc();
                        if !down {
                            emit_status(
                                &arbiter,
                                venue,
                                "down",
                                last_mkt.elapsed().as_millis() as u64,
                            );
                            down = true;
                        }
                        continue 'rejoin;
                    }
                };

            channels[idx].dgrams.inc();
            bytes_ctr.inc_by(n as u64);

            // Reset the liveness watchdog only on the market-data stream; recovery clears `down`.
            if matches!(role, PortRole::Mktdata | PortRole::Combined) {
                last_mkt = std::time::Instant::now();
                if down {
                    emit_status(&arbiter, venue, "ok", 0);
                    down = false;
                }
            }

            let ctx = FrameCtx {
                venue,
                arbiter: &arbiter,
                instruments: &instruments,
                kernel_rx_ts_ns: kernel_ns,
                recv_ts_ns: recv_ns,
                role,
                publisher,
            };
            processor.on_datagram(&channels[idx].buf[..n], &ctx);
        }
    }
}

/// Map a feed's two-port (or combined single-port) layout to driver port roles. When the publisher
/// sends everything on one port (loopback demo), `mktdata == refdata`, so a single `Combined`
/// socket carries both halves.
fn two_port_roles(ports: FeedPorts) -> Vec<(PortRole, u16)> {
    let (mkt, refd) = (ports.mktdata(), ports.refdata());
    if mkt == refd {
        vec![(PortRole::Combined, mkt)]
    } else {
        vec![(PortRole::Mktdata, mkt), (PortRole::Refdata, refd)]
    }
}

/// Run the receiver for one feed: pick the protocol's [`FrameProcessor`] and port roles from the
/// feed's [`FeedKind`], then drive the shared receive loop. Returns only on a fatal bind error
/// (it otherwise runs forever).
pub async fn run_feed(
    feed: Feed,
    iface: String,
    recv_buf: usize,
    arbiter: SharedArbiter,
    instruments: InstrumentSnapshot,
    depth: DepthSnapshot,
) -> Result<()> {
    let venue: &'static str = feed.venue;
    match feed.kind {
        FeedKind::TopOfBook => {
            let ports = two_port_roles(feed.ports);
            drive(
                feed.group,
                ports,
                iface,
                recv_buf,
                venue,
                arbiter,
                instruments,
                TobProcessor::new(feed.emit_trades),
            )
            .await
        }
        FeedKind::Midpoint => {
            let ports = two_port_roles(feed.ports);
            drive(
                feed.group,
                ports,
                iface,
                recv_buf,
                venue,
                arbiter,
                instruments,
                MidpointProcessor::new(),
            )
            .await
        }
        FeedKind::MarketByOrder => {
            let FeedPorts::ThreePort {
                mktdata,
                refdata,
                snapshot,
            } = feed.ports
            else {
                bail!("Market-by-Order feed '{venue}' must use FeedPorts::ThreePort (mktdata/refdata/snapshot)");
            };
            let ports = vec![
                (PortRole::Mktdata, mktdata),
                (PortRole::Refdata, refdata),
                (PortRole::Snapshot, snapshot),
            ];
            drive(
                feed.group,
                ports,
                iface,
                recv_buf,
                venue,
                arbiter,
                instruments,
                MboProcessor::new(depth, feed.emit_trades),
            )
            .await
        }
    }
}

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr, SocketAddr, SocketAddrV4};

    use super::{datagram_src_ip, SeqCheck, SeqTracker, SockaddrStorage};

    #[test]
    fn datagram_src_ip_extracts_v4() {
        let sa = SockaddrStorage::from(SocketAddr::V4(SocketAddrV4::new(
            Ipv4Addr::new(10, 0, 0, 5),
            1234,
        )));
        assert_eq!(
            datagram_src_ip(Some(sa)),
            IpAddr::V4(Ipv4Addr::new(10, 0, 0, 5))
        );
    }

    #[test]
    fn datagram_src_ip_defaults_when_absent() {
        assert_eq!(datagram_src_ip(None), IpAddr::V4(Ipv4Addr::UNSPECIFIED));
    }

    #[test]
    fn first_frame_on_a_channel() {
        let mut s = SeqTracker::default();
        assert_eq!(s.check(0, 0, 0), SeqCheck::First); // sequence starts at 0 per spec
    }

    #[test]
    fn contiguous_sequence_is_ok() {
        let mut s = SeqTracker::default();
        assert_eq!(s.check(0, 0, 0), SeqCheck::First);
        assert_eq!(s.check(0, 0, 1), SeqCheck::Ok);
        assert_eq!(s.check(0, 0, 2), SeqCheck::Ok);
    }

    #[test]
    fn forward_jump_is_accepted() {
        // The channel-0 sequence is global across groups, so a per-group jump is expected, not
        // loss: a forward jump is plain Ok (no gap accounting).
        let mut s = SeqTracker::default();
        assert_eq!(s.check(0, 0, 10), SeqCheck::First);
        assert_eq!(s.check(0, 0, 13), SeqCheck::Ok);
        assert_eq!(s.check(0, 0, 14), SeqCheck::Ok);
    }

    #[test]
    fn lower_sequence_is_stale_and_anchor_holds() {
        let mut s = SeqTracker::default();
        assert_eq!(s.check(0, 0, 10), SeqCheck::First);
        assert_eq!(s.check(0, 0, 9), SeqCheck::Stale); // reordered/duplicated old datagram
        assert_eq!(s.check(0, 0, 3), SeqCheck::Stale);
        // The anchor stayed at 10 (stale frames don't advance it), so 11 is the next contiguous one.
        assert_eq!(s.check(0, 0, 11), SeqCheck::Ok);
    }

    #[test]
    fn duplicate_of_last_is_not_stale() {
        // Equal sequence is a duplicate full-state update (idempotent); only strictly-lower is stale.
        let mut s = SeqTracker::default();
        assert_eq!(s.check(0, 0, 7), SeqCheck::First);
        assert_eq!(s.check(0, 0, 7), SeqCheck::Ok);
    }

    #[test]
    fn reset_count_change_is_a_reset() {
        let mut s = SeqTracker::default();
        assert_eq!(s.check(0, 0, 100), SeqCheck::First);
        // Publisher reset the channel: reset_count bumped, sequence legitimately restarts at 0.
        // Without the reset_count check this 0 would be misread as a stale frame.
        assert_eq!(s.check(0, 1, 0), SeqCheck::Reset);
        assert_eq!(s.check(0, 1, 1), SeqCheck::Ok);
        // Within the new epoch, lower sequences are stale again.
        assert_eq!(s.check(0, 1, 0), SeqCheck::Stale);
    }

    #[test]
    fn channels_are_tracked_independently() {
        let mut s = SeqTracker::default();
        assert_eq!(s.check(0, 0, 10), SeqCheck::First);
        assert_eq!(s.check(1, 0, 2), SeqCheck::First); // a different channel has its own counter
        assert_eq!(s.check(0, 0, 9), SeqCheck::Stale); // channel 0 still drops its own stale frame
        assert_eq!(s.check(1, 0, 3), SeqCheck::Ok);
    }
}
