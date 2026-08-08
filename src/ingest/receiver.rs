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

/// Ceiling for the escalated idle-rejoin interval (see [`escalate_idle`]).
const IDLE_REJOIN_MAX: Duration = Duration::from_secs(300);

/// The next idle interval after a rejoin that produced no market data: double, capped at
/// [`IDLE_REJOIN_MAX`].
///
/// A permanently-silent port block - a publisher that retired, or a registry row whose endpoint
/// never went live - otherwise rebinds its sockets and logs a warn+info pair every 30s for the
/// life of the process. Escalating cuts that to ~12 rejoins/hour. The socket stays bound while we
/// wait, so a publisher that comes back is picked up on its first datagram regardless of the
/// current interval; only the pointless rebind is deferred. The cap keeps a genuinely wedged
/// socket (a join that landed on the wrong interface) self-healing within a few minutes.
fn escalate_idle(idle: Duration) -> Duration {
    idle.saturating_mul(2).min(IDLE_REJOIN_MAX)
}

/// While waiting for the configured interface to acquire an IPv4, retry this often.
const IFACE_POLL: Duration = Duration::from_millis(500);

use crate::{
    ingest::{
        arbiter::{lock, Publisher, SharedArbiter},
        feeds::{Feed, FeedKind, FeedPorts, FeedPublisher},
        health::{FeedHealth, ReceiverKey, SharedFeedHealth},
        processor::{MboProcessor, MbpProcessor, MidpointProcessor, TobProcessor},
        reconcile::TapeOwner,
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
    /// The in-band snapshot recovery stream of a book protocol (Market-by-Order/-Price).
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

/// Materialize the feed-health gauges for `venue` in their at-rest state at receiver setup, so a
/// venue healthy from boot still exposes a `dz_feed_up{venue}` series (without this the children
/// are created only on the first down/ok edge, making the headline `dz_feed_up == 0` alert
/// un-fireable). Reads the shared aggregate rather than asserting 1, so a depth-only receiver
/// starting while the venue's quote publishers are all down does not paper over that. `stale_ms` is
/// left alone in that case — zeroing it would publish the contradictory pair `feed_up=0,
/// stale_ms=0`.
fn init_feed_health(health: &FeedHealth, venue: &str) {
    let up = health.venue_up(venue);
    let m = metrics();
    m.feed_up.with_label_values(&[venue]).set(i64::from(up));
    if up {
        m.feed_stale_ms.with_label_values(&[venue]).set(0);
    }
}

/// Ties one receiver's [`FeedHealth`] entry and its `dz_receiver_up` gauge to the task's lifetime.
///
/// Dropped on **every** exit path — a reconciler `abort()` (which drops the future only after the
/// in-flight poll returns, so this cannot race a late [`Self::set`]), a panic, or a bind error — so
/// a stopped receiver never pins its venue up with nothing serving it, and never leaves a dead
/// publisher reading `dz_receiver_up == 1`. Doing it here rather than in the reconciler is what
/// makes the ordering safe: an external `abort()` + `deregister()` pair can be overtaken by the
/// still-running task's own liveness write.
struct ReceiverRegistration {
    health: SharedFeedHealth,
    arbiter: SharedArbiter,
    key: ReceiverKey,
    up_gauge: prometheus::IntGauge,
}

impl ReceiverRegistration {
    fn new(
        health: SharedFeedHealth,
        arbiter: SharedArbiter,
        key: ReceiverKey,
        up_gauge: prometheus::IntGauge,
    ) -> Self {
        up_gauge.set(1);
        let venue = key.0;
        health.register(key, |venue_up| emit_status(&arbiter, venue, venue_up, 0));
        Self {
            health,
            arbiter,
            key,
            up_gauge,
        }
    }

    /// Record this receiver's liveness. The venue-level `status` fires only when the **venue**
    /// aggregate flips, and is published inside `FeedHealth`'s lock so two receivers crossing
    /// opposite edges concurrently can't publish out of order.
    fn set(&self, up: bool, stale_ms: u64) {
        self.up_gauge.set(i64::from(up));
        let (venue, arbiter) = (self.key.0, &self.arbiter);
        self.health.set(self.key, up, |venue_up| {
            emit_status(arbiter, venue, venue_up, stale_ms)
        });
    }
}

impl Drop for ReceiverRegistration {
    fn drop(&mut self) {
        self.up_gauge.set(0);
        let (venue, arbiter) = (self.key.0, &self.arbiter);
        self.health.deregister(self.key, |venue_up| {
            emit_status(arbiter, venue, venue_up, 0)
        });
    }
}

/// Broadcast a venue-level feed-health transition (PROTOCOL.md `status`): `"down"` when every one of
/// the venue's quote publishers has gone silent past [`IDLE_REJOIN`], `"ok"` when one recovers.
/// Consumers gray out / restore the source on these. Best-effort (ignored if no subscriber is
/// connected). Called only from `FeedHealth`'s `on_edge`, i.e. only on a venue-level edge, and with
/// that lock held — so two receivers can't publish contradictory states out of order. `stale_ms` is
/// only meaningful on a `down` edge.
fn emit_status(arbiter: &SharedArbiter, venue: &str, up: bool, stale_ms: u64) {
    let state = if up { "ok" } else { "down" };
    let stale_ms = if up { 0 } else { stale_ms };
    // Mirror the transition into the feed-health gauges (cheap; only fires on a down/ok edge).
    metrics()
        .feed_up
        .with_label_values(&[venue])
        .set(i64::from(up));
    metrics()
        .feed_stale_ms
        .with_label_values(&[venue])
        .set(stale_ms as i64);
    // Status carries no business identity to dedup, so it goes straight to the broadcast sender
    // (the backbone carries `Arc<FeedMessage>`). Only fires on a down/ok edge, so the `Arc`/`Arc<str>`
    // allocation here is off the per-message hot path.
    let _ = lock(arbiter)
        .sender()
        .send(std::sync::Arc::new(FeedMessage::Status(FeedStatus {
            venue: std::sync::Arc::from(venue),
            state: state.to_string(),
            stale_ms,
            ts_ns: now_ns(),
        })));
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
    // A feed binds 1, 2 or 3 ports (see `FeedPorts`). Match on that fixed shape and race the sockets
    // with a biased `select!` over disjoint slice bindings — no per-datagram `Box`/`Vec` allocation on
    // the hot receive loop (the old `select_all` collected a fresh `Vec<Box<dyn Future>>` every call).
    // Each `recv_with_ts` future is cancellation-safe, so the losers are dropped without consuming a
    // datagram. Fields of one `&mut Channel` are borrowed disjointly (`&sock` + `&mut buf`).
    match channels {
        [c0] => {
            let (n, k, r, s) = recv_with_ts(&c0.sock, &mut c0.buf).await?;
            Ok((c0.role, 0, n, k, r, s))
        }
        [c0, c1] => {
            tokio::select! {
                res = recv_with_ts(&c0.sock, &mut c0.buf) => { let (n, k, r, s) = res?; Ok((c0.role, 0, n, k, r, s)) }
                res = recv_with_ts(&c1.sock, &mut c1.buf) => { let (n, k, r, s) = res?; Ok((c1.role, 1, n, k, r, s)) }
            }
        }
        [c0, c1, c2] => {
            tokio::select! {
                res = recv_with_ts(&c0.sock, &mut c0.buf) => { let (n, k, r, s) = res?; Ok((c0.role, 0, n, k, r, s)) }
                res = recv_with_ts(&c1.sock, &mut c1.buf) => { let (n, k, r, s) = res?; Ok((c1.role, 1, n, k, r, s)) }
                res = recv_with_ts(&c2.sock, &mut c2.buf) => { let (n, k, r, s) = res?; Ok((c2.role, 2, n, k, r, s)) }
            }
        }
        _ => unreachable!("a feed binds 1..=3 ports"),
    }
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
    kind: FeedKind,
    publisher_port: u16,
    arbiter: SharedArbiter,
    instruments: InstrumentSnapshot,
    health: SharedFeedHealth,
    mut processor: P,
) -> Result<()> {
    // This receiver's own liveness: true while its market-data multicast is considered down (silent
    // past IDLE_REJOIN). Persists across rejoins so the gauge/aggregate is touched only on the edge;
    // the venue-level `status` fires only when `health` reports the VENUE aggregate flipped.
    let mut down = false;
    // Escalating idle-rejoin interval. Lives outside the rejoin loop so it survives a rebind, and is
    // reset ONLY by market data actually arriving - resetting it on a successful bind would be a
    // no-op guard, since binding a dead port succeeds every time.
    let mut idle = IDLE_REJOIN;

    // Per-feed metric handles resolved once (venue is `&'static`); the per-channel datagram counter
    // is resolved per role at bind time below.
    let m = metrics();
    let kind_label = kind.label();
    // The `publisher` label/log value is the base port, rendered once here - never per datagram.
    let publisher_port_str = publisher_port.to_string();
    let publisher_name: &str = &publisher_port_str;
    let bytes_ctr = m
        .datagram_bytes
        .with_label_values(&[venue, kind_label, publisher_name]);
    let socket_errors = m
        .socket_errors
        .with_label_values(&[venue, kind_label, publisher_name]);
    let idle_rejoin = m
        .idle_rejoin
        .with_label_values(&[venue, kind_label, publisher_name]);
    // Registers this receiver in the shared health map and owns its `dz_receiver_up`; deregisters
    // both when this task ends for any reason (see `ReceiverRegistration`). Deferred until the
    // sockets actually bind: a receiver that can never bind (taken port, bad interface) would
    // otherwise register-then-drop on every reconciler respawn, publishing a `status` down/ok pair
    // per tick. A bind error is a known failure, not silence, so it stays out of the aggregate.
    let mut registration: Option<ReceiverRegistration> = None;
    // Create the feed-health gauge series up front, so a feed that never goes down still exposes
    // `dz_feed_up{venue}` (the venue-level down/ok edges flip it).
    init_feed_health(&health, venue);

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
                dgrams: m.datagrams_received.with_label_values(&[
                    venue,
                    kind_label,
                    publisher_name,
                    role.label(),
                ]),
            });
        }
        let reg = registration.get_or_insert_with(|| {
            ReceiverRegistration::new(
                health.clone(),
                arbiter.clone(),
                (venue, kind, publisher_port),
                m.receiver_up
                    .with_label_values(&[venue, kind_label, publisher_name]),
            )
        });
        info!(%group, ?ports, %iface, %iface_ip, recv_buf, venue, kind = kind_label,
              publisher = publisher_name, "DZ Edge multicast receiver bound");

        // Watchdog on the market-data stream specifically: rejoin when no market-data datagram has
        // arrived for IDLE_REJOIN, regardless of refdata/snapshot (which keep ticking even when
        // market data is wedged - the exact symptom of a join on the wrong interface).
        let mut last_mkt = std::time::Instant::now();
        loop {
            let remaining = idle.saturating_sub(last_mkt.elapsed());
            if remaining.is_zero() {
                warn!(%group, venue, kind = kind_label, publisher = publisher_name,
                      idle_s = idle.as_secs(),
                      "no market data; re-resolving interface and rejoining");
                idle_rejoin.inc();
                if !down {
                    down = true;
                    // Only when the venue's LAST up quote receiver goes down is the venue down.
                    reg.set(false, last_mkt.elapsed().as_millis() as u64);
                }
                idle = escalate_idle(idle);
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
                        warn!(%group, venue, kind = kind_label, publisher = publisher_name,
                              idle_s = idle.as_secs(),
                              "no market data; re-resolving interface and rejoining");
                        idle_rejoin.inc();
                        if !down {
                            down = true;
                            reg.set(false, last_mkt.elapsed().as_millis() as u64);
                        }
                        idle = escalate_idle(idle);
                        continue 'rejoin;
                    }
                };

            channels[idx].dgrams.inc();
            bytes_ctr.inc_by(n as u64);

            // Reset the liveness watchdog only on the market-data stream; recovery clears `down`
            // and un-escalates the rejoin interval (this is the only thing that proves the block is
            // live, so it is the only thing that may reset it).
            if matches!(role, PortRole::Mktdata | PortRole::Combined) {
                last_mkt = std::time::Instant::now();
                idle = IDLE_REJOIN;
                if down {
                    down = false;
                    reg.set(true, 0);
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

/// Run the receiver for **one publisher** of one feed: pick the protocol's [`FrameProcessor`] and
/// port roles from the feed's [`FeedKind`], then drive the shared receive loop over that
/// publisher's port block. Returns only on a fatal bind error (it otherwise runs forever).
#[allow(clippy::too_many_arguments)]
pub async fn run_feed(
    feed: Feed,
    publisher: FeedPublisher,
    iface: String,
    recv_buf: usize,
    arbiter: SharedArbiter,
    instruments: InstrumentSnapshot,
    depth: DepthSnapshot,
    health: SharedFeedHealth,
    tape: TapeOwner,
) -> Result<()> {
    let venue: &'static str = feed.venue;
    match feed.kind {
        FeedKind::TopOfBook => {
            let ports = two_port_roles(publisher.ports);
            drive(
                feed.group,
                ports,
                iface,
                recv_buf,
                venue,
                feed.kind,
                publisher.base_port(),
                arbiter,
                instruments,
                health,
                TobProcessor::new(tape),
            )
            .await
        }
        FeedKind::Midpoint => {
            let ports = two_port_roles(publisher.ports);
            drive(
                feed.group,
                ports,
                iface,
                recv_buf,
                venue,
                feed.kind,
                publisher.base_port(),
                arbiter,
                instruments,
                health,
                MidpointProcessor::new(),
            )
            .await
        }
        FeedKind::MarketByOrder => {
            let FeedPorts::ThreePort {
                mktdata,
                refdata,
                snapshot,
            } = publisher.ports
            else {
                bail!(
                    "Market-by-Order feed '{venue}' publisher '{}' must use FeedPorts::ThreePort \
                     (mktdata/refdata/snapshot)",
                    publisher.base_port()
                );
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
                feed.kind,
                publisher.base_port(),
                arbiter,
                instruments,
                health,
                MboProcessor::new(depth, tape),
            )
            .await
        }
        FeedKind::MarketByPrice => {
            let FeedPorts::ThreePort {
                mktdata,
                refdata,
                snapshot,
            } = publisher.ports
            else {
                bail!(
                    "Market-by-Price feed '{venue}' publisher '{}' must use FeedPorts::ThreePort \
                     (mktdata/refdata/snapshot)",
                    publisher.base_port()
                );
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
                feed.kind,
                publisher.base_port(),
                arbiter,
                instruments,
                health,
                MbpProcessor::new(tape),
            )
            .await
        }
    }
}

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr, SocketAddr, SocketAddrV4};

    use super::{datagram_src_ip, init_feed_health, SeqCheck, SeqTracker, SockaddrStorage};
    use crate::metrics::metrics;

    #[test]
    fn feed_health_gauges_materialize_up_at_setup() {
        use crate::ingest::{feeds::FeedKind, health::FeedHealth};
        // A unique venue label keeps this independent of any other test touching the shared,
        // process-global metrics registry (see `metrics()` docs).
        let venue = "FeedHealthInitTest";
        let health = FeedHealth::new();
        health.register((venue, FeedKind::TopOfBook, 9101), |_| {});
        init_feed_health(&health, venue);
        // The gauge reads healthy (1) with no prior down/ok transition — the whole point of the
        // up-front init, so the `dz_feed_up == 0` alert has a series to evaluate.
        assert_eq!(metrics().feed_up.with_label_values(&[venue]).get(), 1);
        assert_eq!(metrics().feed_stale_ms.with_label_values(&[venue]).get(), 0);
    }

    /// The idle interval doubles per fruitless rejoin and stops at the cap, so a permanently-silent
    /// block settles at one rejoin per `IDLE_REJOIN_MAX` instead of one per 30s forever. The first
    /// escalation happens only *after* the first timeout, so a publisher going silent is still
    /// declared down at the usual 30s.
    #[test]
    fn idle_rejoin_escalates_then_caps() {
        use super::{escalate_idle, Duration, IDLE_REJOIN, IDLE_REJOIN_MAX};
        let mut idle = IDLE_REJOIN;
        let mut seen = vec![idle];
        for _ in 0..8 {
            idle = escalate_idle(idle);
            seen.push(idle);
        }
        assert_eq!(seen[0], Duration::from_secs(30));
        assert_eq!(seen[1], Duration::from_secs(60));
        assert_eq!(seen[2], Duration::from_secs(120));
        assert_eq!(seen[3], Duration::from_secs(240));
        // Doubling 240 would overshoot the cap, so it clamps and stays there.
        assert_eq!(seen[4], IDLE_REJOIN_MAX);
        assert_eq!(*seen.last().unwrap(), IDLE_REJOIN_MAX);
        assert!(
            IDLE_REJOIN_MAX > IDLE_REJOIN,
            "cap must escalate, not shrink"
        );
    }

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
