//! WebSocket server: fan out normalized `FeedMessage`s to connected subscribers as JSON
//! text frames. Implements the v1 protocol (see PROTOCOL.md):
//!   - replay instrument snapshot on connect, then stream quotes;
//!   - optional per-client subscribe/unsubscribe filtering (default: receive all);
//!   - app-level ping/pong + server heartbeat with an idle timeout to reap dead clients;
//!   - connection / subscription / inbound-rate limits and broadcast backpressure.

use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use anyhow::Result;
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::{
    net::{TcpListener, TcpStream},
    sync::broadcast,
};
use tokio_tungstenite::tungstenite::{Message as WsMessage, Utf8Bytes};
use tracing::{info, warn};

use crate::{
    metrics::metrics,
    model::{
        now_ns, BookAccumulator, BookKey, BookSnapshot, DepthSnapshot, FeedMessage,
        InstrumentSnapshot, ReplayScope,
    },
};

/// A message serialized **once** for all clients: the JSON text plus the fields the per-client
/// filter needs. Built by the single serializer task (see [`serve`]) and shared by reference-count
/// (`Arc` + `Utf8Bytes`, both cheap to clone) to every connected client, so the same quote is never
/// serialized more than once no matter how many consumers are attached.
struct PreparedFrame {
    /// The complete JSON text frame, ready to write. `ws_send_ts_ns` is already stamped in here
    /// (once, shared by all clients — see PROTOCOL.md).
    payload: Utf8Bytes,
    /// Message kind for the `dz_ws_*{kind}` metrics.
    kind: &'static str,
    /// The message's venue, for subscription filtering.
    venue: Arc<str>,
    /// The message's symbol, or `None` for a venue-level `status` (matched by venue alone).
    symbol: Option<Arc<str>>,
    /// The message's `channel_id`, or `None` for a type that carries none. Populated when the
    /// incremental `book` message lands; every current type is `None`.
    channel: Option<u8>,
    /// For a `book`/`order_book` frame, the market it belongs to — the replay map's own key,
    /// arbitration scope included, so a watermark cannot be looked up under a wire identity two
    /// universes share. `None` for every other kind.
    book_market: Option<BookKey>,
    /// The frame's `recv_ts_ns`, carried so the join watermark can be applied without re-parsing
    /// the JSON. Only read for a `book_market` frame; `0` on the rest.
    recv_ts_ns: u64,
}

/// Markets one client has **not** been bootstrapped for and must therefore not be handed
/// incremental batches of, each against the moment its withhold started.
///
/// A market the replay skipped — accumulated partway through, or mid-event with changes still
/// buffered behind their `last` — has batches the client can never place: the ones before the
/// skip are in no bootstrap and, if they were broadcast before the client's `rx` subscribed, in
/// no queue either. Applying what does arrive leaves the levels it did not touch at values the
/// client invented, which is the frozen-side book with no way back.
///
/// ⚠️ **What ends the withhold is the market's next *completed event*, not its next producer
/// re-baseline.** A mid-event market is complete again at the venue's next slot boundary, under a
/// second; a producer re-baseline is one or two an hour, so releasing on that left a client dark on
/// a live market for tens of minutes — measured on 5 of 8 joins against the Oregon bridge. See
/// [`withheld_bootstrap`].
type WithheldMarkets = std::collections::HashMap<BookKey, Instant>;

/// Per-market join watermarks for one client: the `BookAccumulator::wire_ts_ns` each replayed
/// market's bootstrap was materialized at, and when it was recorded.
///
/// A client's `rx` is subscribed in the accept loop, before [`serve_client`] completes the
/// WebSocket handshake and reads the replay caches, so batches broadcast in between are queued
/// *behind* a snapshot that already contains them. Re-applying them walks the client's book
/// backwards — the defect is invisible on `quote`/`depth`, which are full state, and permanent on
/// the incremental `book`/`order_book`.
///
/// Moving the `subscribe` after the snapshot instead would trade the overlap for a gap, which is
/// the worse failure: a batch broadcast in that window would reach nobody.
///
/// ⚠️ **The only thing a watermark may drop is the queued prefix that predates the bootstrap**, so
/// it is consumed by the first frame that passes it (broadcast order is wire order, so everything
/// after that frame is newer than the bootstrap) and expires at
/// [`BOOTSTRAP_RELEASE_DEADLINE`] regardless. Without either rule a stamp that somehow lands ahead
/// of the live feed — a backwards host-clock step is the one way left, every book frame otherwise
/// carrying `now_ns()` read on the same wall clock — silences that market for the life of the
/// connection.
type BookWatermarks = std::collections::HashMap<BookKey, (u64, Instant)>;

/// One client's per-market bootstrap state: what it was replayed and at what point, and what it
/// was not replayed at all. Both are written by [`replay_scoped`] and read by the forwarding loop.
#[derive(Default)]
struct ClientBooks {
    watermarks: BookWatermarks,
    awaiting: WithheldMarkets,
}

/// Serialize one backbone message once: clone it, stamp the shared `ws_send_ts_ns`, render the JSON,
/// and capture the fields the per-client filter needs. Returns `None` only if serialization fails
/// (never expected for our own types).
fn prepare(m: &FeedMessage) -> Option<Arc<PreparedFrame>> {
    let mut m = m.clone();
    let now = now_ns();
    // Stamp the WS hand-off time on the latency-bearing kinds. One stamp, shared by every client
    // (the accepted trade-off for serializing once — see PROTOCOL.md `ws_send_ts_ns`).
    let kind = match &mut m {
        FeedMessage::Quote(q) => {
            q.ws_send_ts_ns = now;
            "quote"
        }
        FeedMessage::Trade(t) => {
            t.ws_send_ts_ns = now;
            "trade"
        }
        FeedMessage::Midpoint(mp) => {
            mp.ws_send_ts_ns = now;
            "midpoint"
        }
        FeedMessage::Depth(d) => {
            d.ws_send_ts_ns = now;
            "depth"
        }
        FeedMessage::Book(b) => {
            b.ws_send_ts_ns = now;
            if b.order_level {
                "order_book"
            } else {
                "book"
            }
        }
        FeedMessage::OrderBook(b) => {
            b.ws_send_ts_ns = now;
            "order_book"
        }
        FeedMessage::Instrument(_) => "instrument",
        FeedMessage::Status(_) => "status",
    };
    // An order-level batch renders under its own `type`. Only the tag differs — the body is the same
    // `NormalizedBook` — and the wrap is local to serialization, so the filter fields below are still
    // read off the original message and no other `match` on `FeedMessage` grows a path.
    let payload: Utf8Bytes = match &m {
        FeedMessage::Book(b) if b.order_level => {
            serde_json::to_string(&FeedMessage::OrderBook(b.clone())).ok()?
        }
        _ => serde_json::to_string(&m).ok()?,
    }
    .into();
    let (venue, symbol) = match &m {
        FeedMessage::Instrument(i) => (i.venue.clone(), Some(i.symbol.clone())),
        FeedMessage::Quote(q) => (q.venue.clone(), Some(q.symbol.clone())),
        FeedMessage::Trade(t) => (t.venue.clone(), Some(t.symbol.clone())),
        FeedMessage::Midpoint(mp) => (mp.venue.clone(), Some(mp.symbol.clone())),
        FeedMessage::Depth(d) => (d.venue.clone(), Some(d.symbol.clone())),
        FeedMessage::Book(b) | FeedMessage::OrderBook(b) => {
            (b.venue.clone(), Some(b.symbol.clone()))
        }
        FeedMessage::Status(s) => (s.venue.clone(), None),
    };
    let (book_market, recv_ts_ns) = match &m {
        FeedMessage::Book(b) | FeedMessage::OrderBook(b) => (
            Some((
                crate::model::SourceKey::of(b),
                b.category.clone(),
                b.channel,
                b.instrument_id,
            )),
            b.recv_ts_ns,
        ),
        _ => (None, 0),
    };
    Some(Arc::new(PreparedFrame {
        payload,
        kind,
        venue,
        symbol,
        channel: m.channel(),
        book_market,
        recv_ts_ns,
    }))
}

/// Tunable server limits / liveness (from CLI args).
#[derive(Clone, Debug)]
pub struct WsConfig {
    pub heartbeat: Duration,
    pub idle_timeout: Duration,
    pub max_clients: usize,
    pub max_subs: usize,
    pub max_inbound_per_min: u32,
    /// Capacity of the internal "prepared frame" broadcast (the serialize-once fan-out); sized to
    /// match the backbone so a client that keeps up with the backbone keeps up here too.
    pub broadcast_capacity: usize,
    /// Shortest gap between one client's lag-triggered book repairs; production passes
    /// [`LAG_REPAIR_MIN_INTERVAL`]. A field rather than a bare constant so a test can collapse the
    /// window and assert that a *coalesced* lag is still discharged — the property the pace is only
    /// safe because of, and one no unit test of `lag_repair_ready` alone can reach.
    pub lag_repair_min_interval: Duration,
    /// How long a client waits for a withheld market's event to close before it is bootstrapped
    /// from the last complete state anyway, and the ceiling on how long a join watermark may hold
    /// one silent; production passes [`BOOTSTRAP_RELEASE_DEADLINE`]. A field for the same reason
    /// `lag_repair_min_interval` is one — a test collapsing the window is the only way to reach
    /// either bound.
    pub bootstrap_release_deadline: Duration,
}

/// A subscription filter: a `None` field matches any value (so `{}` = everything).
#[derive(Deserialize, Serialize, Clone, Debug, PartialEq)]
struct SubFilter {
    #[serde(default)]
    venue: Option<String>,
    /// Alias for `venue`, the preferred spelling. Both are accepted for the deprecation window; if
    /// a client sends both they are ANDed, so a disagreeing pair matches nothing rather than
    /// silently honouring one.
    ///
    #[serde(default)]
    source_name: Option<String>,
    /// The pre-rename spelling, still accepted. Its own slot rather than a `serde(alias)` on
    /// `source_name`: an alias shares one field slot, so a client sending both spellings — the
    /// natural way to straddle a rename — would get `duplicate field` and have the whole `subscribe`
    /// refused, registering no filter at all and landing on the firehose. That is the failure this
    /// key exists to prevent. ANDed like `venue`, so all three spellings compose.
    #[serde(default, rename = "source")]
    deprecated_source_name: Option<String>,
    #[serde(default)]
    symbol: Option<String>,
    /// The wire `channel_id` — the competition, not the path. Path identity is deliberately not
    /// client-selectable: exactly one arbitrated book per market reaches the wire.
    #[serde(default)]
    channel: Option<u8>,
    /// Message `type` (`quote`/`trade`/`book`/...). Named `msg_type` in Rust because `type` is a
    /// keyword; the wire name is `type`.
    #[serde(rename = "type", default)]
    msg_type: Option<String>,
}

impl SubFilter {
    /// Whether this filter can deliver `kind` **at all**, ignoring scope. `type` is the one absolute
    /// dimension (see [`Self::matches`]): `venue`/`symbol`/`channel` narrow *which* markets a client
    /// sees and can leave that set empty, but they never rule a product out, while a `type` naming
    /// another kind rules it out wholesale. Split out so the `type` dimension has one implementation
    /// — a second copy is how a filter starts admitting on one path and refusing on the other.
    fn admits_kind(&self, kind: &str) -> bool {
        self.msg_type.as_deref().is_none_or(|t| t == kind)
    }

    /// The single match path. `venue`/`source_name` are aliases and are ANDed. `symbol`/`channel` are
    /// `None` for a venue-level message (today only `status`), and a `None` on the *message* side
    /// satisfies a filter on that dimension — a venue-level message is about the whole venue, so a
    /// symbol- or channel-scoped subscriber must still receive it. A filter dimension the message
    /// *does* carry is matched normally.
    fn matches(&self, venue: &str, symbol: Option<&str>, channel: Option<u8>, kind: &str) -> bool {
        // Venue codes are registry identifiers, not free text - match case-insensitively so a
        // subscription for `PHOENIX` / `phoenix` still selects the wire venue `Phoenix`. Symbol and
        // type stay exact (venues name symbols precisely; types are a closed protocol set).
        self.venue
            .as_deref()
            .is_none_or(|v| v.eq_ignore_ascii_case(venue))
            && self
                .source_name
                .as_deref()
                .is_none_or(|s| s.eq_ignore_ascii_case(venue))
            && self
                .deprecated_source_name
                .as_deref()
                .is_none_or(|s| s.eq_ignore_ascii_case(venue))
            // `type` is a *kind* selector and so is absolute, with no carve-out: a client that named
            // one type asked for that type. Filters are a union, so wanting books plus definitions is
            // two subscriptions. `venue`/`symbol`/`channel` below are *scope* selectors — which
            // markets — and those do carve out messages that aren't about one market.
            && self.admits_kind(kind)
            && match symbol {
                None => true,
                Some(s) => self.symbol.as_deref().is_none_or(|f| f == s),
            }
            && match channel {
                // A venue-level message (`status`) is about no single channel, so an explicit
                // channel filter must not exclude it; a channelless *market* message is excluded,
                // or `{"channel":2}` would be a firehose of quotes.
                None => self.channel.is_none() || symbol.is_none(),
                Some(c) => self.channel.is_none_or(|f| f == c),
            }
    }
}

/// Inbound control messages a client may send.
#[derive(Deserialize)]
#[serde(tag = "method", rename_all = "snake_case")]
enum ClientMsg {
    Ping,
    Subscribe { subscription: SubFilter },
    Unsubscribe { subscription: SubFilter },
}

/// Releases a connection's accounting on drop — the live-client atomic and the `dz_ws_clients`
/// gauge — so an unexpected panic inside `serve_client` cannot leak the slot. Without this the
/// `clients` count would drift up on each panic and eventually wedge new connections at
/// `max_clients` (and the gauge would over-report forever).
struct ClientGuard {
    clients: Arc<AtomicUsize>,
}

impl Drop for ClientGuard {
    fn drop(&mut self) {
        self.clients.fetch_sub(1, Ordering::SeqCst);
        metrics().ws_clients.dec();
    }
}

/// Bind the WebSocket listener up front so the caller can decide what a bind failure means.
/// A taken port must not be fatal to the whole process (it would take the DoubleZero tunnel
/// down with it — see `main.rs`), so binding is a separate, awaitable step from serving.
pub async fn bind(addr: &str) -> Result<TcpListener> {
    let listener = TcpListener::bind(addr).await?;
    info!(bind = %addr, "WebSocket server listening");
    Ok(listener)
}

/// The accept loop, split out so tests (and `main`) can drive a pre-bound listener.
pub async fn serve(
    listener: TcpListener,
    tx: broadcast::Sender<Arc<FeedMessage>>,
    instruments: InstrumentSnapshot,
    depth: DepthSnapshot,
    books: BookSnapshot,
    cfg: WsConfig,
) -> Result<()> {
    let clients = Arc::new(AtomicUsize::new(0));

    // Serialize-once fan-out: a single task reads the `Arc<FeedMessage>` backbone, serializes each
    // surviving message to JSON exactly once (stamping one shared `ws_send_ts_ns`), and re-broadcasts
    // the ready-to-write `Arc<PreparedFrame>` to every client. Client tasks then only filter and write
    // a cheap `Utf8Bytes` clone — the same quote is never serialized N times for N clients. With no
    // clients attached the serializer skips the work entirely (see the `receiver_count` guard), so the
    // no-consumer case stays as cheap as the old no-subscriber `send`.
    let (prepared_tx, _prepared_rx) =
        broadcast::channel::<Arc<PreparedFrame>>(cfg.broadcast_capacity);
    {
        let prepared_tx = prepared_tx.clone();
        let mut backbone = tx.subscribe();
        tokio::spawn(async move {
            loop {
                match backbone.recv().await {
                    Ok(m) => {
                        // No connected clients → don't spend CPU serializing. Correctness of this
                        // skip rests on connect-time replay (the instrument snapshot, then the
                        // latest `depth` per symbol, sent directly — not via prepare()) plus quote
                        // full-state semantics: a client that connects while the serializer is
                        // skipping is caught up from the snapshot, then every subsequent quote/depth
                        // is full state, so nothing skipped here is lost. (Trades in the
                        // accept→subscribe gap are point-in-time and not replayed — matches prior
                        // behavior.)
                        if prepared_tx.receiver_count() == 0 {
                            continue;
                        }
                        if let Some(frame) = prepare(&m) {
                            let _ = prepared_tx.send(frame);
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        metrics().ws_serializer_lagged.inc();
                        warn!("ws serializer lagged, dropped {n}");
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
        });
    }

    loop {
        let (stream, peer) = listener.accept().await?;
        // Connection limit: reject (drop the TCP stream) once at capacity.
        if clients.fetch_add(1, Ordering::SeqCst) >= cfg.max_clients {
            clients.fetch_sub(1, Ordering::SeqCst);
            warn!(%peer, max = cfg.max_clients, "max clients reached; rejecting connection");
            metrics()
                .ws_connections
                .with_label_values(&["rejected"])
                .inc();
            drop(stream);
            continue;
        }
        metrics()
            .ws_connections
            .with_label_values(&["accepted"])
            .inc();
        metrics().ws_clients.inc();
        let rx = prepared_tx.subscribe();
        let instruments = instruments.clone();
        let depth = depth.clone();
        let books = books.clone();
        let cfg = cfg.clone();
        // The guard releases the slot + gauge on drop, so the accounting is correct even if
        // `serve_client` panics rather than returning.
        let guard = ClientGuard {
            clients: clients.clone(),
        };
        tokio::spawn(async move {
            let _guard = guard;
            if let Err(e) = serve_client(stream, rx, instruments, depth, books, cfg).await {
                warn!(%peer, "client ended: {e}");
            }
        });
    }
}

fn text(value: serde_json::Value) -> WsMessage {
    WsMessage::Text(value.to_string().into())
}

/// The scope to bootstrap one market at: **the market's own granularity, always**.
///
/// It is not a client choice. A bootstrap and a feed of different granularity cannot be reconciled
/// — an order-level change carries one *order's* absolute size, and a client handed price levels has
/// no order state to apply it to — so the only consumer a fold could serve is one folding the live
/// feed itself, which needs every resting order's size and is exactly what the fold discards. The
/// `book_scope` subscription field offered that choice and is withdrawn; it never shipped in a
/// release.
/// The wire `type` one market's book is served under — the same market property `book_scope` reads,
/// and for the same reason: the bootstrap and the live feed must agree.
fn book_kind(acc: &BookAccumulator) -> &'static str {
    if acc.is_order_level() {
        "order_book"
    } else {
        "book"
    }
}

fn book_scope(acc: &BookAccumulator) -> ReplayScope {
    if acc.is_order_level() {
        ReplayScope::Orders
    } else {
        ReplayScope::Levels
    }
}

/// How long a withheld market may stay mid-event before the client is bootstrapped from the last
/// complete state anyway (see [`WithheldMarkets`]).
///
/// A venue slot is ~400 ms and the producer's own boundary fallback closes a stalled event after 2 s
/// (`ingest::processor`'s `BOUNDARY_TIMEOUT_NS`), so a market still open at this bound is not going
/// to close on its own. Also the ceiling on how long a join watermark may hold a market silent —
/// same argument, from the other end: nothing the bootstrap contains is still queued this long
/// after it.
pub const BOOTSTRAP_RELEASE_DEADLINE: Duration = Duration::from_secs(5);

/// How often a client re-checks the markets its join withheld. A quiet market produces no frame to
/// carry the check on, and it is exactly the one whose bootstrap the client has no other way to get.
const BOOTSTRAP_SWEEP_INTERVAL: Duration = Duration::from_secs(1);

/// The bootstrap owed to a market this client's join withheld, once there is an honest one to send:
/// the same `to_book` the join would have sent, its wire point, and which rule released it.
///
/// Released on the market's next **completed event**, not on its next producer re-baseline: an event
/// closes at the venue's next slot boundary, a re-baseline is one or two an hour, and waiting for the
/// second is what left a client dark on a live market for tens of minutes. `overdue` is the backstop
/// for a channel whose events stop closing at all.
///
/// [`Withheld::Waiting`] while [`BookAccumulator::baselined`] does not hold, `overdue` included: an
/// accumulator seeded partway through holds only what has moved since, and there is no honest
/// bootstrap to send until some path re-baselines it. Dark is the correct answer there, and the
/// client keeps waiting.
fn withheld_bootstrap(
    books: &BookSnapshot,
    key: &BookKey,
    subs: &[SubFilter],
    overdue: bool,
) -> Withheld {
    let guard = crate::model::lock(books);
    // A market with no accumulator at all is `Waiting`, not `OutOfScope`: an evicted one is
    // recreated by the next batch and forced to re-baseline, so it is still owed a bootstrap.
    let Some(acc) = guard.get(key) else {
        return Withheld::Waiting;
    };
    let wanted = subs.is_empty()
        || subs.iter().any(|f| {
            f.matches(
                key.0.name(),
                Some(acc.symbol()),
                Some(key.2),
                book_kind(acc),
            )
        });
    if !wanted {
        return Withheld::OutOfScope;
    }
    if !acc.baselined() {
        return Withheld::Waiting;
    }
    let release = match (acc.pending_empty(), overdue) {
        (true, _) => "complete",
        // ⚠️ The event still open here is *lost* to this client: its batches so far were dropped
        // while the market was withheld, and the ones still to come land on a bootstrap without
        // them. Bounded staleness on levels that event touched, against a client dark for minutes.
        (false, true) => "deadline",
        (false, false) => return Withheld::Waiting,
    };
    let book = acc.to_book(key, book_scope(acc));
    let msg = if acc.is_order_level() {
        FeedMessage::OrderBook(book)
    } else {
        FeedMessage::Book(book)
    };
    Withheld::Ready {
        msg,
        wire_ts: acc.wire_ts_ns(),
        release,
    }
}

/// What [`withheld_bootstrap`] found for one withheld market.
///
/// ⚠️ **`OutOfScope` is not `Waiting`.** A market the unfiltered connect replay withheld and a later
/// `subscribe` put out of scope is owed no bootstrap at all, and conflating the two left it on the
/// withhold list for the life of the connection: every frame for it charged
/// `dz_ws_frames_dropped_total{reason="awaiting"}`, which reads as a client dark on that book, and
/// the sweep arm stayed armed taking the shared `BookSnapshot` mutex once a second for it.
enum Withheld {
    /// No honest bootstrap yet — keep withholding.
    Waiting,
    /// Outside this client's subscription scope, so nothing is owed. Stop tracking it; a widening
    /// re-accounts for it through [`replay_scoped`].
    OutOfScope,
    Ready {
        msg: FeedMessage,
        wire_ts: u64,
        release: &'static str,
    },
}

/// Shortest interval between one client's lag-triggered book repairs (see [`Replay::Books`] and the
/// `Lagged` arm of [`serve_client`]).
///
/// A re-baseline is written into a client that is *already* behind, and writing it is exactly what
/// keeps that client from draining the broadcast — so an unpaced one re-arms the lag that asked for
/// it and the connection converges on replaying instead of streaming. The pace is the same trade
/// `sinks::hyperliquid`'s `REBOOTSTRAP_MIN_INTERVAL` prices for its own `l4Book` bootstrap: long
/// enough that the replay cannot be the dominant cost of the connection, short enough that a book
/// left stale by a dropped batch is corrected in about the time a consumer would notice.
pub const LAG_REPAIR_MIN_INTERVAL: Duration = Duration::from_secs(5);

/// Whether an owed lag repair may go out now: `due` says one is owed, `last` is when this client was
/// last repaired (`None` = never). Split out from the loop so the pace is unit-testable without
/// waiting [`LAG_REPAIR_MIN_INTERVAL`] of wall clock.
///
/// The first repair after a quiet period is immediate — the pace exists to stop a repair from
/// re-arming the lag that asked for it, not to delay a client's only correction.
fn lag_repair_ready(due: bool, last: Option<Instant>, interval: Duration) -> bool {
    due && last.is_none_or(|t| t.elapsed() >= interval)
}

/// Whether a lag repair can reach this client at all. Only `book`/`order_book` are repaired, so a
/// client filtered to another type is owed nothing by a lag.
///
/// Checked where the repair is *marked due* rather than inside [`replay_scoped`], because what this
/// avoids is the market scan itself — taken under the mutex the ingest emit path shares — and not the
/// frames it would have sent, of which there are none. A client that adds a book subscription later
/// is bootstrapped by that `subscribe`'s own [`Replay::Full`], so deciding here loses nothing.
fn lag_repair_reaches(subs: &[SubFilter]) -> bool {
    subs.is_empty()
        || subs
            .iter()
            .any(|f| f.admits_kind("book") || f.admits_kind("order_book"))
}

/// How much current state one [`replay_scoped`] call sends.
///
/// The distinction is which products **cannot heal on their own**, and it is the whole reason a lag
/// is not answered with a bootstrap:
/// - `quote`/`depth` are full state, so a dropped one is corrected by the next message for that
///   symbol with no producer action at all;
/// - `instrument` is re-announced on the first publisher refdata burst after
///   `ingest::arbiter::INSTRUMENT_REANNOUNCE_NS` — that rate limit is a rate limit rather than a
///   latch *precisely* so a definition lost under backpressure comes back, and it is the mechanism
///   an established client heals by (see that constant's doc);
/// - `book`/`order_book` are incremental, and nothing in the stream restates them: a dropped batch
///   leaves the consumer's book permanently wrong until the producer re-baselines it.
///
/// So a lag replays [`Replay::Books`] only. Replaying the catalog instead is the pathology in #149:
/// the definitions outnumber the markets by orders of magnitude on a venue like Kalshi, they are
/// pure duplication of state the client already holds, and the write blocks the very recv loop whose
/// stall caused the lag.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Replay {
    /// Everything: definitions (precision first), then `depth`, then the `book` re-baselines. The
    /// connect and `subscribe` bootstrap, where the client holds no state at all.
    Full,
    /// The `book`/`order_book` re-baselines alone — the lag repair.
    Books,
}

/// Replay current full state matching `subs` (empty = everything): instrument definitions first so
/// precision is known before any book, then the latest `depth` per `(venue, symbol)` and a `book`
/// re-baseline per `(venue, channel, instrument_id)`. `what` selects how much of that is sent — see
/// [`Replay`].
///
/// Called on connect, and again on each `subscribe` so a client that narrows after connecting is
/// bootstrapped for its new scope rather than waiting for the next event. Replay is idempotent full
/// state, so the overlap a connect-then-subscribe client sees is harmless.
async fn replay_scoped<W>(
    write: &mut W,
    instruments: &InstrumentSnapshot,
    depth: &DepthSnapshot,
    books: &BookSnapshot,
    subs: &[SubFilter],
    what: Replay,
    books_seen: &mut ClientBooks,
) -> Result<()>
where
    W: SinkExt<WsMessage> + Unpin,
    <W as futures_util::Sink<WsMessage>>::Error: std::error::Error + Send + Sync + 'static,
{
    // Each kind passes its own channel: a channel-bearing kind that passed `None` would leave a
    // `{"channel":N}` client with no bootstrap at all.
    let pass = |venue: &str, symbol: &str, channel: Option<u8>, kind: &str| {
        subs.is_empty()
            || subs
                .iter()
                .any(|f| f.matches(venue, Some(symbol), channel, kind))
    };
    // Every lock is taken and released before any `await`: a `std::sync::MutexGuard` held across an
    // await point does not compile here, and would be a latency bug regardless.
    // Skipped entirely under `Replay::Books` — not merely filtered out of the send: the clone is
    // taken under the mutex the ingest emit path shares, and on a large catalog that scan is itself
    // the cost (a lagging client repeating it is what made `/v1/status`, which walks the same map,
    // time out in #149).
    let snapshot: Vec<FeedMessage> = if what == Replay::Full {
        let guard = crate::model::lock(instruments);
        guard
            .values()
            .filter(|i| pass(&i.venue, &i.symbol, Some(i.channel), "instrument"))
            .cloned()
            .map(FeedMessage::Instrument)
            .collect()
    } else {
        Vec::new()
    };
    let depths: Vec<FeedMessage> = if what == Replay::Full {
        let guard = crate::model::lock(depth);
        guard
            .values()
            .filter(|d| pass(&d.venue, &d.symbol, None, "depth"))
            .cloned()
            .map(FeedMessage::Depth)
            .collect()
    } else {
        Vec::new()
    };
    let rebaselines: Vec<FeedMessage> = {
        let guard = crate::model::lock(books);
        let mut out = Vec::new();
        for (key, acc) in guard.iter() {
            // The key's second element is the producer-side arbitration scope (`Feed::category`,
            // which keeps two universes' colliding instrument ids apart in the map); it is not a
            // filter dimension and never reaches the wire.
            // Filtered and tagged under the market's *own* type, matching the live feed it
            // precedes: a client subscribed to `order_book` alone must still be bootstrapped, and
            // one subscribed to `book` alone must not be handed a market it cannot apply.
            if !pass(key.0.name(), acc.symbol(), Some(key.2), book_kind(acc)) {
                continue;
            }
            // A market accumulated partway through holds only the levels that have moved since, so
            // replaying it as full state would tell the client to discard the ones it never saw.
            // One **mid-event** is no better: `to_book` materializes folded state, so the batches
            // still buffered behind their `last` are in no bootstrap, and any of them broadcast
            // before this client's `rx` subscribed are in no queue either. Both are withheld, and
            // the market is held back until its accumulator is whole (see `WithheldMarkets`).
            if !acc.baselined() || !acc.pending_empty() {
                books_seen
                    .awaiting
                    .entry(key.clone())
                    .or_insert_with(Instant::now);
                continue;
            }
            // Recorded market by market, only for the ones actually sent: a market the filters
            // skipped gets no bootstrap, so every queued batch for it is still the client's only
            // copy. `0` means nothing has been folded in, which is no watermark at all.
            if acc.wire_ts_ns() != 0 {
                books_seen
                    .watermarks
                    .insert(key.clone(), (acc.wire_ts_ns(), Instant::now()));
            }
            books_seen.awaiting.remove(key);
            let book = acc.to_book(key, book_scope(acc));
            out.push(if acc.is_order_level() {
                FeedMessage::OrderBook(book)
            } else {
                FeedMessage::Book(book)
            });
        }
        out
    };
    for m in snapshot.into_iter().chain(depths).chain(rebaselines) {
        write
            .send(WsMessage::Text(serde_json::to_string(&m)?.into()))
            .await?;
    }
    Ok(())
}

/// Send the bootstrap [`withheld_bootstrap`] says is owed for one withheld market, and take it off
/// the withhold list. Returns whether the market is *still* withheld.
async fn release_withheld<W>(
    write: &mut W,
    books: &BookSnapshot,
    key: &BookKey,
    subs: &[SubFilter],
    books_seen: &mut ClientBooks,
    deadline: Duration,
) -> Result<bool>
where
    W: SinkExt<WsMessage> + Unpin,
    <W as futures_util::Sink<WsMessage>>::Error: std::error::Error + Send + Sync + 'static,
{
    let Some(&since) = books_seen.awaiting.get(key) else {
        return Ok(false);
    };
    let overdue = since.elapsed() >= deadline;
    let (msg, wire_ts, release) = match withheld_bootstrap(books, key, subs, overdue) {
        Withheld::Waiting => return Ok(true),
        // Nothing is owed, so nothing is withheld — and the caller must not charge the frame to
        // the withhold counters. The client's own filter excludes it from here on.
        Withheld::OutOfScope => {
            books_seen.awaiting.remove(key);
            return Ok(false);
        }
        Withheld::Ready {
            msg,
            wire_ts,
            release,
        } => (msg, wire_ts, release),
    };
    books_seen.awaiting.remove(key);
    // The same pairing the join makes: the bootstrap stands at the accumulator's wire point, so the
    // batches already folded into it must not be re-applied on top.
    if wire_ts != 0 {
        books_seen
            .watermarks
            .insert(key.clone(), (wire_ts, Instant::now()));
    }
    metrics()
        .ws_bootstrap_withheld
        .with_label_values(&[key.0.name(), release])
        .inc();
    write
        .send(WsMessage::Text(serde_json::to_string(&msg)?.into()))
        .await?;
    Ok(false)
}

async fn serve_client(
    stream: TcpStream,
    mut rx: broadcast::Receiver<Arc<PreparedFrame>>,
    instruments: InstrumentSnapshot,
    depth: DepthSnapshot,
    books: BookSnapshot,
    cfg: WsConfig,
) -> Result<()> {
    let ws = tokio_tungstenite::accept_async(stream).await?;
    let (mut write, mut read) = ws.split();

    // Per-client state. Empty `subs` = firehose (receive every venue/symbol).
    let mut subs: Vec<SubFilter> = Vec::new();
    let mut books_seen = ClientBooks::default();

    // Replay definitions (precision first) then current book state, so a consumer joining partway is
    // bootstrapped immediately instead of waiting for the next periodic book. (Quotes/trades are not
    // replayed - the next quote is itself full state.) `subs` is empty here, so this connect-time
    // replay is unfiltered.
    replay_scoped(
        &mut write,
        &instruments,
        &depth,
        &books,
        &subs,
        Replay::Full,
        &mut books_seen,
    )
    .await?;

    let mut last_seen = Instant::now();
    let mut win_start = Instant::now();
    let mut win_count: u32 = 0;
    let mut hb = tokio::time::interval(cfg.heartbeat);
    let mut sweep = tokio::time::interval(BOOTSTRAP_SWEEP_INTERVAL);
    // The sweep arm is disabled while nothing is withheld, so its ticks go unpolled for as long as
    // a connection is healthy; `Burst` would then fire every one of them at once the moment a
    // market is withheld again.
    sweep.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    // Lag repair state: when this client was last repaired, and whether a lag since then is still
    // owed one. Both are needed — the pace alone would drop the repair a suppressed lag asked
    // for, leaving that client's book wrong for as long as it stays connected.
    let mut last_lag_repair: Option<Instant> = None;
    let mut lag_repair_due = false;

    loop {
        // Discharge an owed lag repair here, at the top of the loop rather than in the `Lagged` arm
        // that owed it: a client that keeps lagging never reaches a quiet moment of its own, and one
        // that recovers would otherwise hold a stale book until its next lag. This runs on the next
        // pass through the loop, so an owed repair goes out on this client's next frame or, on a
        // silent feed, its next heartbeat tick — never later than that, and a silent feed is not one
        // producing the batches the book is missing. Ordering against the live frames written in
        // between is not a hazard: the re-baseline is materialized here, so it is current as of this
        // moment and the batches that follow apply on top of it.
        if lag_repair_ready(lag_repair_due, last_lag_repair, cfg.lag_repair_min_interval) {
            lag_repair_due = false;
            // Counts the repair *pass*, which is the quantity the pace above governs and the one
            // that costs the market scan — not the re-baselines the pass writes, of which there can
            // legitimately be none (a client whose filters match no baselined market, or any
            // deployment carrying no book-bearing feed). Counting frames instead would leave this
            // silent on exactly the deployments where a lag is worth reading about, and would make
            // its ratio against `dz_ws_client_lagged_total` unreadable: a gap would no longer mean
            // "coalesced by the pace".
            metrics().ws_lag_repairs.inc();
            replay_scoped(
                &mut write,
                &instruments,
                &depth,
                &books,
                &subs,
                Replay::Books,
                &mut books_seen,
            )
            .await?;
            // Stamped *after* the write, not before it, so the pace bounds the duty cycle and not
            // merely the count. A repair is written into a client that is by definition slow, so
            // the write itself can outlast the window — and a start-to-start pace would then make
            // the next repair eligible the instant this one drained, leaving zero streaming in
            // between and reproducing #149's shape at O(markets) instead of O(catalog). End-to-start
            // guarantees the client gets a whole window of live frames between two repairs, however
            // long a repair takes.
            last_lag_repair = Some(Instant::now());
        }

        tokio::select! {
            incoming = read.next() => match incoming {
                Some(Ok(WsMessage::Text(txt))) => {
                    last_seen = Instant::now();
                    // Inbound rate limit (per rolling minute).
                    if win_start.elapsed() >= Duration::from_secs(60) {
                        win_start = Instant::now();
                        win_count = 0;
                    }
                    win_count += 1;
                    if win_count > cfg.max_inbound_per_min {
                        metrics().ws_rate_limited.inc();
                        write.send(text(json!({"channel": "error", "error": "inbound rate limit exceeded"}))).await?;
                        break;
                    }
                    match serde_json::from_str::<ClientMsg>(&txt) {
                        Ok(ClientMsg::Ping) => {
                            metrics().ws_inbound.with_label_values(&["ping"]).inc();
                            write.send(text(json!({"channel": "pong"}))).await?
                        }
                        Ok(ClientMsg::Subscribe { subscription }) => {
                            metrics().ws_inbound.with_label_values(&["subscribe"]).inc();
                            if subs.len() >= cfg.max_subs {
                                write.send(text(json!({"channel": "error", "error": "max subscriptions reached"}))).await?;
                            } else {
                                let added = !subs.contains(&subscription);
                                if added {
                                    subs.push(subscription.clone());
                                }
                                write.send(text(json!({
                                    "channel": "subscription_response", "method": "subscribe",
                                    "subscription": subscription,
                                }))).await?;
                                // Bootstrap the newly-added scope only: not all of `subs` (else a
                                // client subscribing to ten symbols replays the first one ten times),
                                // and nothing at all for a duplicate — a re-subscribe adds no scope,
                                // and replaying anyway would let a client loop O(state) snapshot work
                                // (taken under the mutex the ingest emit path shares) at the inbound
                                // rate limit without ever reaching `max_subs`.
                                if added {
                                    replay_scoped(&mut write, &instruments, &depth, &books, std::slice::from_ref(&subscription), Replay::Full, &mut books_seen).await?;
                                }
                            }
                        }
                        Ok(ClientMsg::Unsubscribe { subscription }) => {
                            metrics().ws_inbound.with_label_values(&["unsubscribe"]).inc();
                            let before = subs.len();
                            subs.retain(|s| s != &subscription);
                            let widened = subs.len() != before;
                            write.send(text(json!({
                                "channel": "subscription_response", "method": "unsubscribe",
                                "subscription": subscription,
                            }))).await?;
                            // ⚠️ Widening needs the same book bootstrap `subscribe` does, for the
                            // markets coming back into scope: one dropped from `awaiting` as
                            // `Withheld::OutOfScope` has neither a bootstrap nor a withhold, so
                            // without this its batches would be applied to a book the client does
                            // not hold. `Books` only — the catalog is #149's cost and a client
                            // widening already holds it. Guarded on an actual removal, so a
                            // no-op unsubscribe cannot drive O(markets) at the inbound rate.
                            if widened {
                                replay_scoped(&mut write, &instruments, &depth, &books, &subs, Replay::Books, &mut books_seen).await?;
                            }
                        }
                        Err(_) => {
                            metrics().ws_inbound.with_label_values(&["error"]).inc();
                            write.send(text(json!({"channel": "error", "error": "unrecognized message"}))).await?
                        }
                    }
                }
                Some(Ok(WsMessage::Ping(p))) => { last_seen = Instant::now(); write.send(WsMessage::Pong(p)).await?; }
                Some(Ok(WsMessage::Pong(_))) => last_seen = Instant::now(),
                Some(Ok(WsMessage::Close(_))) | None => break,
                Some(Ok(_)) => {}
                Some(Err(e)) => return Err(e.into()),
            },

            // The withheld markets a live frame cannot release: a market that goes quiet mid-event
            // produces nothing to carry the check on, and is precisely the one whose bootstrap the
            // client has no other way to get. Also where the deadline fires.
            _ = sweep.tick(), if !books_seen.awaiting.is_empty() => {
                for key in books_seen.awaiting.keys().cloned().collect::<Vec<_>>() {
                    release_withheld(&mut write, &books, &key, &subs, &mut books_seen, cfg.bootstrap_release_deadline).await?;
                }
            },

            // Heartbeat tick: reap silent clients, otherwise ping to keep liveness measurable.
            _ = hb.tick() => {
                if last_seen.elapsed() > cfg.idle_timeout {
                    metrics().ws_idle_timeout.inc();
                    let _ = write.send(WsMessage::Close(None)).await;
                    break;
                }
                write.send(WsMessage::Ping(Vec::new().into())).await?;
            },

            // Forward already-serialized frames this subscriber wants. The frame was serialized once
            // upstream (see `serve`); here we only filter and write a cheap `Utf8Bytes` clone.
            msg = rx.recv() => match msg {
                Ok(frame) => {
                    // A batch strictly older than the bootstrap this client was just sent: queued
                    // on `rx` before the replay read the market, so forwarding it would re-apply
                    // state the snapshot already carries — silently wrong on an incremental
                    // product, and the reason the watermark exists (see `BookWatermarks`).
                    //
                    // ⚠️ **Strictly older. A tie is forwarded, and must be.** One datagram
                    // straddling a venue batch boundary emits two batches for the same market —
                    // the boundary's closing one and the changes after it — both stamped that
                    // datagram's `recv_ts_ns`. The first folds and the second only buffers, so
                    // the watermark sits at their shared stamp while the bootstrap holds the
                    // first alone: dropping at the tie would take the second with it and its
                    // changes would never arrive, the next boundary delivering a `last` over a
                    // buffer missing them. Re-delivering a tie instead costs nothing — a change
                    // carries an absolute size, the tie set replays in broadcast order, and the
                    // bootstrap ends exactly at the last folded batch of that set, so re-applying
                    // lands on the same state.
                    if let Some(key) = &frame.book_market {
                        // A market this client was not bootstrapped for: an incremental batch
                        // applied to a book the client does not hold is a book it can never
                        // correct. A frame for it is also the cheapest evidence that the market has
                        // moved on, so the release is attempted here before the frame is judged —
                        // in the common case the accumulator has already folded this very frame
                        // (the arbiter advances it before it broadcasts), so the bootstrap goes out
                        // and the watermark below ties with the frame and forwards it.
                        if !books_seen.awaiting.is_empty()
                            && release_withheld(&mut write, &books, key, &subs, &mut books_seen, cfg.bootstrap_release_deadline).await?
                        {
                            metrics().ws_frames_dropped.with_label_values(&[&frame.venue, "awaiting"]).inc();
                            continue;
                        }
                        // ⚠️ Consumed by the first frame that passes, and expired at
                        // `BOOTSTRAP_RELEASE_DEADLINE` regardless: broadcast order is wire order,
                        // so a watermark may only ever drop the queued prefix that predates the
                        // bootstrap (see `BookWatermarks`).
                        if let Some(&(w, at)) = books_seen.watermarks.get(key) {
                            if frame.recv_ts_ns < w && at.elapsed() < cfg.bootstrap_release_deadline {
                                metrics().ws_frames_dropped.with_label_values(&[&frame.venue, "watermark"]).inc();
                                continue;
                            }
                            books_seen.watermarks.remove(key);
                        }
                    }
                    // One match path for every kind, venue-level included: a dimension added to
                    // `matches` cannot silently exempt half the feed.
                    let pass = subs.is_empty()
                        || subs.iter().any(|f| {
                            f.matches(
                                &frame.venue,
                                frame.symbol.as_deref(),
                                frame.channel,
                                frame.kind,
                            )
                        });
                    if pass {
                        metrics().ws_messages_sent.with_label_values(&[frame.kind]).inc();
                        metrics().ws_bytes_sent.with_label_values(&[frame.kind]).inc_by(frame.payload.len() as u64);
                        write.send(WsMessage::Text(frame.payload.clone())).await?;
                    }
                }
                Err(broadcast::error::RecvError::Lagged(n)) => {
                    metrics().ws_client_lagged.inc();
                    warn!("subscriber lagged, dropped {n}");
                    // `book` is incremental: a dropped batch leaves this client's book permanently
                    // wrong, so it must be re-baselined. Everything else this client missed restates
                    // itself (see `Replay`), so nothing else is replayed — and the repair is *owed*
                    // here rather than written here, so that a client lagging faster than
                    // `LAG_REPAIR_MIN_INTERVAL` cannot drive its own connection into replaying
                    // full state in a loop instead of streaming (#149).
                    //
                    // Never cleared here: a client that unsubscribes from books between the lag and
                    // the repair leaves a scan that sends nothing (cheap, and self-correcting), while
                    // clearing it would drop a repair that a *later* filter still needs.
                    if lag_repair_reaches(&subs) {
                        lag_repair_due = true;
                    }
                }
                Err(broadcast::error::RecvError::Closed) => break,
            },
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashMap,
        sync::{Arc, Mutex},
        time::Duration,
    };

    use futures_util::StreamExt;
    use serial_test::serial;
    use tokio::{net::TcpListener, sync::broadcast, time::timeout};

    use super::{serve, SubFilter, WsConfig, WsMessage};
    use crate::{
        metrics::metrics,
        model::{
            BookAccumulator, BookAction, BookChange, BookReplay, BookSide, FeedMessage,
            NormalizedBook, NormalizedInstrument, NormalizedQuote,
        },
    };

    fn filter(json: &str) -> SubFilter {
        serde_json::from_str(json).expect("filter parses")
    }

    #[test]
    fn venue_matches_case_insensitively() {
        // The wire venue is `Phoenix`; a filter spelled any case must still select it (the
        // PROTOCOL.md example historically showed `PHOENIX`, which would silently drop the feed
        // under an exact match).
        assert!(filter(r#"{"venue":"PHOENIX"}"#).matches("PHOENIX", Some("BTC"), None, "quote"));
        assert!(filter(r#"{"venue":"phoenix"}"#).matches("PHOENIX", Some("BTC"), None, "quote"));
        assert!(filter(r#"{"venue":"PHOENIX"}"#).matches("PHOENIX", Some("BTC"), None, "quote"));
        assert!(!filter(r#"{"venue":"HYPERLIQUID"}"#).matches(
            "PHOENIX",
            Some("BTC"),
            None,
            "quote"
        ));
    }

    #[test]
    fn omitted_field_matches_any_symbol_exact() {
        assert!(filter("{}").matches("PHOENIX", Some("BTC"), None, "quote")); // {} = everything
        assert!(filter(r#"{"symbol":"BTC"}"#).matches("PHOENIX", Some("BTC"), None, "quote"));
        // symbol stays exact
        assert!(!filter(r#"{"symbol":"btc"}"#).matches("PHOENIX", Some("BTC"), None, "quote"));
    }

    /// The omitted-field-matches-anything rule must survive the two new dimensions.
    #[test]
    fn empty_filter_still_matches_everything() {
        let f = filter("{}");
        assert!(f.matches("KALSHI", Some("KXBTCPERP"), Some(2), "book"));
        assert!(f.matches("HYPERLIQUID", Some("SOL"), None, "quote"));
        assert!(f.matches("KALSHI", None, None, "status"));
    }

    #[test]
    fn type_filter_selects_one_message_kind() {
        let f = filter(r#"{"type":"book"}"#);
        assert!(f.matches("KALSHI", Some("KXBTCPERP"), Some(2), "book"));
        assert!(!f.matches("KALSHI", Some("KXBTCPERP"), Some(2), "quote"));
    }

    /// `type` is matched exactly, like `symbol`: the wire values are a closed set the protocol
    /// defines, so a near-miss is a client bug worth surfacing as "no data" rather than guessing.
    #[test]
    fn type_filter_is_exact() {
        assert!(!filter(r#"{"type":"BOOK"}"#).matches("KALSHI", Some("X"), None, "book"));
    }

    #[test]
    fn channel_filter_selects_one_channel() {
        let f = filter(r#"{"channel":2}"#);
        assert!(f.matches("KALSHI", Some("KXBTCPERP"), Some(2), "book"));
        assert!(!f.matches("KALSHI", Some("KXBTCPERP"), Some(1), "book"));
    }

    /// An explicit channel filter must not pass a message that carries no channel — otherwise
    /// `{"channel":2}` would receive every quote on every venue.
    #[test]
    fn channel_filter_excludes_channelless_messages() {
        assert!(!filter(r#"{"channel":2}"#).matches("HYPERLIQUID", Some("SOL"), None, "quote"));
    }

    /// `instrument` carries its own channel, so it is filtered like `book`: a channel-scoped client
    /// gets the definitions it needs to scale that channel's books, and no other channel's.
    #[test]
    fn channel_filter_selects_one_channels_instrument_definitions() {
        let f = filter(r#"{"channel":2}"#);
        assert!(f.matches("KALSHI", Some("KXBTCPERP"), Some(2), "instrument"));
        assert!(!f.matches("KALSHI", Some("KXETHPERP"), Some(1), "instrument"));
        // `symbol` still narrows instruments independently of the channel.
        assert!(!filter(r#"{"channel":2,"symbol":"SOL"}"#).matches(
            "KALSHI",
            Some("KXBTCPERP"),
            Some(2),
            "instrument"
        ));
    }

    /// `status` is venue-level: no symbol and no channel, so it matches on venue and type alone —
    /// the same carve-out `symbol` already has, extended to `channel`. Without this a
    /// `{"venue":"KALSHI","channel":2}` subscriber would never learn its venue went down.
    #[test]
    fn status_matches_on_venue_despite_symbol_and_channel_filters() {
        let f = filter(r#"{"venue":"KALSHI","symbol":"KXBTCPERP","channel":2}"#);
        assert!(f.matches("KALSHI", None, None, "status"));
        assert!(!f.matches("HYPERLIQUID", None, None, "status"));
    }

    /// ...but an explicit `type` filter still excludes it, so a consumer that asked for `book` only
    /// does not get status frames it never requested.
    #[test]
    fn type_filter_still_excludes_status() {
        assert!(!filter(r#"{"type":"book"}"#).matches("KALSHI", None, None, "status"));
    }

    /// The venue's committed slot is omitted while unknown, never sent as a placeholder: a consumer
    /// keying a same-slot comparison would read `0` as a real slot.
    #[test]
    fn prepare_omits_an_unknown_committed_slot() {
        use super::prepare;
        let mut b = book_batch("SOL", vec![level_update(BookSide::Bid, 0.62, 150.0)], true);
        let payload = |b: &NormalizedBook| {
            prepare(&FeedMessage::Book(b.clone()))
                .expect("serializes")
                .payload
                .to_string()
        };
        assert!(!payload(&b).contains("batch_id"));
        b.batch_id = Some(900);
        assert!(payload(&b).contains(r#""batch_id":900"#));
    }

    /// `book` and `instrument` must carry their channel so an explicit channel filter can select
    /// them; every other kind carries none, which is what the filter's exclusion rule rests on.
    #[test]
    fn prepare_populates_the_channel_for_book_and_instrument() {
        use super::prepare;
        let b = FeedMessage::Book(NormalizedBook {
            batch_id: None,
            venue: "KALSHI".into(),
            source_name: "KALSHI".into(),
            source_id: 0,
            symbol: "KXBTCPERP".into(),
            channel: 2,
            instrument_id: 41,
            order_level: false,
            changes: vec![BookChange {
                action: BookAction::Update,
                side: BookSide::Bid,
                price: 0.62,
                size: 150.0,
                order_id: 0,
            }],
            snapshot: false,
            last: true,
            source_ts_ns: 1,
            recv_ts_ns: 2,
            kernel_rx_ts_ns: 3,
            ws_send_ts_ns: 0,
            category: "default".into(),
        });
        let f = prepare(&b).expect("serializes");
        assert_eq!(f.kind, "book");
        assert_eq!(f.channel, Some(2));
        assert!(
            !f.payload.contains("category"),
            "category is producer-side only and must never reach the wire"
        );
        assert!(f.payload.contains(r#""ws_send_ts_ns":"#));
        assert!(
            !f.payload.contains(r#""ws_send_ts_ns":0"#),
            "stamped, not left at 0"
        );
        let i = prepare(&FeedMessage::Instrument(NormalizedInstrument {
            tick_size: 0,
            venue: "KALSHI".into(),
            source_name: "KALSHI".into(),
            source_id: 0,
            symbol: "KXBTCPERP".into(),
            channel: 2,
            instrument_id: 41,
            category: "default".into(),
            price_exponent: -2,
            qty_exponent: -2,
        }))
        .expect("serializes");
        assert_eq!(i.kind, "instrument");
        assert_eq!(i.channel, Some(2));
        assert!(
            !i.payload.contains("category"),
            "category is producer-side only and must never reach the wire"
        );

        assert_eq!(
            prepare(&FeedMessage::Quote(sample_quote()))
                .expect("serializes")
                .channel,
            None
        );
    }

    #[test]
    fn venue_stays_case_insensitive() {
        assert!(filter(r#"{"venue":"kalshi"}"#).matches("KALSHI", Some("X"), None, "book"));
    }

    /// Poll `cond` until it holds, failing the test if it doesn't within ~2s. The metric updates we
    /// wait on happen on another task, so a short poll is more robust than a fixed sleep.
    async fn wait_until(mut cond: impl FnMut() -> bool) {
        let ok = timeout(Duration::from_secs(2), async {
            while !cond() {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await;
        assert!(ok.is_ok(), "condition not met within timeout");
    }

    fn sample_quote() -> NormalizedQuote {
        NormalizedQuote {
            venue: "HYPERLIQUID".into(),
            source_name: "HYPERLIQUID".into(),
            source_id: 0,
            symbol: "BTC".into(),
            bid: 1.0,
            ask: 2.0,
            bid_size: 1.0,
            ask_size: 1.0,
            bid_n: 1,
            ask_n: 1,
            source_ts_ns: 1,
            recv_ts_ns: 0,
            kernel_rx_ts_ns: 0,
            ws_send_ts_ns: 0,
        }
    }

    /// A client connect→disconnect must leave the live-client gauge where it started and record
    /// exactly one accepted connection; a forwarded quote must advance the per-kind byte counter.
    /// `#[serial]` because `dz_ws_clients` is a process-global gauge shared with any concurrent test
    /// (see the `metrics()` docs); the assertions are baseline-relative for the same reason.
    #[tokio::test]
    #[serial]
    async fn ws_client_accounting_and_byte_counter() {
        let m = metrics();
        let accepted_before = m.ws_connections.with_label_values(&["accepted"]).get();
        let clients_before = m.ws_clients.get();
        let bytes_before = m.ws_bytes_sent.with_label_values(&["quote"]).get();

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (tx, _rx) = broadcast::channel::<std::sync::Arc<FeedMessage>>(16);
        let instruments = Arc::new(Mutex::new(HashMap::new()));
        let depth = Arc::new(Mutex::new(HashMap::new()));
        let cfg = WsConfig {
            heartbeat: Duration::from_secs(30),
            idle_timeout: Duration::from_secs(60),
            max_clients: 8,
            max_subs: 8,
            max_inbound_per_min: 600,
            broadcast_capacity: 16,
            lag_repair_min_interval: super::LAG_REPAIR_MIN_INTERVAL,
            bootstrap_release_deadline: super::BOOTSTRAP_RELEASE_DEADLINE,
        };
        let books = Arc::new(Mutex::new(BookReplay::default()));
        let srv = tokio::spawn(serve(listener, tx.clone(), instruments, depth, books, cfg));

        let (mut ws, _resp) = tokio_tungstenite::connect_async(format!("ws://{addr}"))
            .await
            .unwrap();

        // The server accounts the client on its own task, so wait for the gauge to reflect it.
        wait_until(|| m.ws_clients.get() == clients_before + 1).await;
        assert_eq!(
            m.ws_connections.with_label_values(&["accepted"]).get(),
            accepted_before + 1
        );

        // Push a quote and drain the client until it arrives, then the byte counter must have moved.
        // (Retry the send: the subscriber is created inside the spawned task, so an immediate first
        // send can race ahead of the subscribe.)
        let mut got_quote = false;
        for _ in 0..100 {
            let _ = tx.send(std::sync::Arc::new(FeedMessage::Quote(sample_quote())));
            match timeout(Duration::from_millis(50), ws.next()).await {
                Ok(Some(Ok(WsMessage::Text(txt)))) if txt.contains("\"quote\"") => {
                    got_quote = true;
                    break;
                }
                Ok(Some(Ok(_))) => continue, // replayed snapshot frame / other; keep draining
                _ => continue,
            }
        }
        assert!(got_quote, "client never received the forwarded quote");
        assert!(
            m.ws_bytes_sent.with_label_values(&["quote"]).get() > bytes_before,
            "quote byte counter did not advance"
        );

        // Disconnect and confirm the gauge nets back to the baseline (the RAII guard fires).
        drop(ws);
        wait_until(|| m.ws_clients.get() == clients_before).await;

        srv.abort();
    }

    /// Serialize-once: a single backbone message is rendered to JSON exactly once and the identical
    /// frame is fanned out to every client, so two clients receive **byte-for-byte equal** payloads
    /// (including a single shared `ws_send_ts_ns`). `#[serial]` for the shared `dz_ws_clients` gauge.
    #[tokio::test]
    #[serial]
    async fn ws_serializes_once_identical_payload_across_clients() {
        let m = metrics();
        let clients_before = m.ws_clients.get();

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (tx, _rx) = broadcast::channel::<std::sync::Arc<FeedMessage>>(16);
        let instruments = Arc::new(Mutex::new(HashMap::new()));
        let depth = Arc::new(Mutex::new(HashMap::new()));
        let cfg = WsConfig {
            heartbeat: Duration::from_secs(30),
            idle_timeout: Duration::from_secs(60),
            max_clients: 8,
            max_subs: 8,
            max_inbound_per_min: 600,
            broadcast_capacity: 16,
            lag_repair_min_interval: super::LAG_REPAIR_MIN_INTERVAL,
            bootstrap_release_deadline: super::BOOTSTRAP_RELEASE_DEADLINE,
        };
        let books = Arc::new(Mutex::new(BookReplay::default()));
        let srv = tokio::spawn(serve(listener, tx.clone(), instruments, depth, books, cfg));

        let (mut ws1, _) = tokio_tungstenite::connect_async(format!("ws://{addr}"))
            .await
            .unwrap();
        let (mut ws2, _) = tokio_tungstenite::connect_async(format!("ws://{addr}"))
            .await
            .unwrap();

        // Both clients accounted (so both prepared-frame receivers are subscribed) and the serializer
        // has subscribed to the backbone, before the single send — so exactly one prepared frame is
        // built and delivered to both, with no second send racing in a different `ws_send_ts_ns`.
        wait_until(|| m.ws_clients.get() == clients_before + 2).await;
        wait_until(|| tx.receiver_count() >= 1).await;

        tx.send(std::sync::Arc::new(FeedMessage::Quote(sample_quote())))
            .expect("backbone has the serializer as a receiver");

        // Read the first `quote` frame each client receives (skipping any empty-snapshot replay).
        async fn next_quote<S>(ws: &mut S) -> String
        where
            S: futures_util::StreamExt<
                    Item = Result<WsMessage, tokio_tungstenite::tungstenite::Error>,
                > + Unpin,
        {
            loop {
                match timeout(Duration::from_secs(2), ws.next()).await {
                    Ok(Some(Ok(WsMessage::Text(t)))) if t.contains("\"quote\"") => {
                        return t.to_string()
                    }
                    Ok(Some(Ok(_))) => continue,
                    other => panic!("client did not receive the quote: {other:?}"),
                }
            }
        }

        let t1 = next_quote(&mut ws1).await;
        let t2 = next_quote(&mut ws2).await;
        assert_eq!(
            t1, t2,
            "serialize-once: all clients must receive byte-identical payloads"
        );
        assert!(
            t1.contains("ws_send_ts_ns"),
            "quote must carry ws_send_ts_ns"
        );

        srv.abort();
    }

    /// A `subscribe` replays current state scoped to the filter just added, so a client that narrows
    /// after connecting is bootstrapped for its new scope instead of waiting for the next event —
    /// and only for that scope. `#[serial]` for the shared `dz_ws_clients` gauge.
    #[tokio::test]
    #[serial]
    async fn subscribe_replays_only_the_new_scope() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (tx, _rx) = broadcast::channel::<std::sync::Arc<FeedMessage>>(16);

        let mut defs = HashMap::new();
        for (n, sym) in ["SOL", "BTC"].into_iter().enumerate() {
            let arc: Arc<str> = sym.into();
            defs.insert(
                (
                    crate::model::SourceKey::from("HYPERLIQUID"),
                    Arc::<str>::from("default"),
                    0u8,
                    n as u32,
                ),
                NormalizedInstrument {
                    tick_size: 0,
                    venue: "HYPERLIQUID".into(),
                    source_name: "HYPERLIQUID".into(),
                    source_id: 0,
                    symbol: arc,
                    channel: 0,
                    instrument_id: n as u32,
                    category: "default".into(),
                    price_exponent: -2,
                    qty_exponent: -2,
                },
            );
        }
        let instruments = Arc::new(Mutex::new(defs));
        let depth = Arc::new(Mutex::new(HashMap::new()));
        let cfg = WsConfig {
            heartbeat: Duration::from_secs(30),
            idle_timeout: Duration::from_secs(60),
            max_clients: 8,
            max_subs: 8,
            max_inbound_per_min: 600,
            broadcast_capacity: 16,
            lag_repair_min_interval: super::LAG_REPAIR_MIN_INTERVAL,
            bootstrap_release_deadline: super::BOOTSTRAP_RELEASE_DEADLINE,
        };
        let books = Arc::new(Mutex::new(BookReplay::default()));
        let srv = tokio::spawn(serve(listener, tx.clone(), instruments, depth, books, cfg));

        let (mut ws, _) = tokio_tungstenite::connect_async(format!("ws://{addr}"))
            .await
            .unwrap();

        // The next text frame, skipping the server's heartbeat Pings.
        async fn next_text<S>(ws: &mut S, within: Duration) -> Option<String>
        where
            S: futures_util::StreamExt<
                    Item = Result<WsMessage, tokio_tungstenite::tungstenite::Error>,
                > + Unpin,
        {
            timeout(within, async {
                loop {
                    match ws.next().await {
                        Some(Ok(WsMessage::Text(t))) => return t.to_string(),
                        Some(Ok(_)) => continue,
                        other => panic!("stream ended: {other:?}"),
                    }
                }
            })
            .await
            .ok()
        }

        // Connect-time replay is unfiltered: both definitions arrive.
        let mut connect_replay = Vec::new();
        for _ in 0..2 {
            connect_replay.push(
                next_text(&mut ws, Duration::from_secs(2))
                    .await
                    .expect("replayed instrument"),
            );
        }
        assert!(connect_replay.iter().any(|t| t.contains("\"SOL\"")));
        assert!(connect_replay.iter().any(|t| t.contains("\"BTC\"")));

        use futures_util::SinkExt;
        ws.send(WsMessage::Text(
            r#"{"method":"subscribe","subscription":{"symbol":"SOL"}}"#.into(),
        ))
        .await
        .unwrap();

        // The ack, then a replay scoped to the filter just added: SOL only, nothing else.
        let ack = next_text(&mut ws, Duration::from_secs(2))
            .await
            .expect("subscription ack");
        assert!(ack.contains("subscription_response"), "got {ack}");
        let replayed = next_text(&mut ws, Duration::from_secs(2))
            .await
            .expect("scoped replay frame");
        assert!(replayed.contains("\"instrument\"") && replayed.contains("\"SOL\""));
        assert_eq!(
            next_text(&mut ws, Duration::from_millis(200)).await,
            None,
            "BTC must not be replayed for a SOL subscription"
        );

        // A duplicate subscribe adds no scope, so it is acked and replays nothing — otherwise a
        // client could loop full-state replays at the inbound rate limit without reaching max_subs.
        ws.send(WsMessage::Text(
            r#"{"method":"subscribe","subscription":{"symbol":"SOL"}}"#.into(),
        ))
        .await
        .unwrap();
        let ack = next_text(&mut ws, Duration::from_secs(2))
            .await
            .expect("subscription ack");
        assert!(ack.contains("subscription_response"), "got {ack}");
        assert_eq!(
            next_text(&mut ws, Duration::from_millis(200)).await,
            None,
            "a re-subscribe must not replay again"
        );

        srv.abort();
    }

    /// A `{"channel":N}` subscriber's replay is scoped by the instrument's own channel, so it is
    /// bootstrapped with the definitions it can use and not another channel's. The two markets
    /// differ by channel (the snapshot's actual identity component); distinct symbols are used too,
    /// purely so the assertions below can tell them apart by content. `#[serial]` for the shared
    /// `dz_ws_clients` gauge.
    #[tokio::test]
    #[serial]
    async fn replay_is_scoped_to_the_subscribed_channel() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (tx, _rx) = broadcast::channel::<std::sync::Arc<FeedMessage>>(16);

        let mut defs = HashMap::new();
        for (sym, channel) in [("KXBTCPERP", 2u8), ("KXETHPERP", 3)] {
            let arc: Arc<str> = sym.into();
            defs.insert(
                (
                    crate::model::SourceKey::from("KALSHI"),
                    Arc::<str>::from("default"),
                    channel,
                    41u32,
                ),
                NormalizedInstrument {
                    tick_size: 0,
                    venue: "KALSHI".into(),
                    source_name: "KALSHI".into(),
                    source_id: 0,
                    symbol: arc,
                    channel,
                    instrument_id: 41,
                    category: "default".into(),
                    price_exponent: -2,
                    qty_exponent: -2,
                },
            );
        }
        let instruments = Arc::new(Mutex::new(defs));
        let depth = Arc::new(Mutex::new(HashMap::new()));
        let cfg = WsConfig {
            heartbeat: Duration::from_secs(30),
            idle_timeout: Duration::from_secs(60),
            max_clients: 8,
            max_subs: 8,
            max_inbound_per_min: 600,
            broadcast_capacity: 16,
            lag_repair_min_interval: super::LAG_REPAIR_MIN_INTERVAL,
            bootstrap_release_deadline: super::BOOTSTRAP_RELEASE_DEADLINE,
        };
        let srv = tokio::spawn(serve(
            listener,
            tx.clone(),
            instruments,
            depth,
            Arc::new(Mutex::new(BookReplay::default())),
            cfg,
        ));

        let (mut ws, _) = tokio_tungstenite::connect_async(format!("ws://{addr}"))
            .await
            .unwrap();

        async fn next_text<S>(ws: &mut S, within: Duration) -> Option<String>
        where
            S: futures_util::StreamExt<
                    Item = Result<WsMessage, tokio_tungstenite::tungstenite::Error>,
                > + Unpin,
        {
            timeout(within, async {
                loop {
                    match ws.next().await {
                        Some(Ok(WsMessage::Text(t))) => return t.to_string(),
                        Some(Ok(_)) => continue,
                        other => panic!("stream ended: {other:?}"),
                    }
                }
            })
            .await
            .ok()
        }

        // Connect-time replay has no subscriptions to scope by: both definitions arrive.
        for _ in 0..2 {
            next_text(&mut ws, Duration::from_secs(2))
                .await
                .expect("replayed instrument");
        }

        use futures_util::SinkExt;
        ws.send(WsMessage::Text(
            r#"{"method":"subscribe","subscription":{"channel":2}}"#.into(),
        ))
        .await
        .unwrap();

        let ack = next_text(&mut ws, Duration::from_secs(2))
            .await
            .expect("subscription ack");
        assert!(ack.contains("subscription_response"), "got {ack}");
        let replayed = next_text(&mut ws, Duration::from_secs(2))
            .await
            .expect("scoped replay frame");
        assert!(
            replayed.contains(r#""instrument""#) && replayed.contains(r#""KXBTCPERP""#),
            "got {replayed}"
        );
        assert_eq!(
            next_text(&mut ws, Duration::from_millis(200)).await,
            None,
            "channel 3's definition must not be replayed for a channel 2 subscription"
        );

        srv.abort();
    }

    fn level_update(side: BookSide, price: f64, size: f64) -> BookChange {
        BookChange {
            action: BookAction::Update,
            side,
            price,
            size,
            order_id: 0,
        }
    }

    fn book_batch(symbol: &str, changes: Vec<BookChange>, last: bool) -> NormalizedBook {
        NormalizedBook {
            batch_id: None,
            venue: "KALSHI".into(),
            source_name: "KALSHI".into(),
            source_id: 0,
            symbol: symbol.into(),
            channel: 0,
            instrument_id: 0,
            order_level: changes.iter().any(|c| c.order_id != 0),
            changes,
            snapshot: false,
            last,
            source_ts_ns: 7,
            recv_ts_ns: 0,
            kernel_rx_ts_ns: 0,
            ws_send_ts_ns: 0,
            category: crate::model::empty_category(),
        }
    }

    /// A market whose levels are its whole book: a producer re-baseline (`Clear`-led), folded from a
    /// two-batch logical event (the first batch is not `last`, so only the pair together is replayed).
    fn accumulator(symbol: &str, bid: f64, ask: f64) -> BookAccumulator {
        let mut acc = BookAccumulator::new(symbol.into());
        acc.apply(&book_batch(
            symbol,
            vec![
                BookChange {
                    action: BookAction::Clear,
                    side: BookSide::Both,
                    price: 0.0,
                    size: 0.0,
                    order_id: 0,
                },
                level_update(BookSide::Bid, bid, 10.0),
            ],
            false,
        ));
        acc.apply(&book_batch(
            symbol,
            vec![level_update(BookSide::Ask, ask, 20.0)],
            true,
        ));
        acc
    }

    /// Spawn a server over the given replay maps (`depth` empty). The returned sender must be held by
    /// the caller for the lifetime of the test.
    async fn spawn_server(
        instruments: HashMap<(crate::model::SourceKey, Arc<str>, u8, u32), NormalizedInstrument>,
        books: BookReplay,
    ) -> (
        tokio::task::JoinHandle<anyhow::Result<()>>,
        broadcast::Sender<std::sync::Arc<FeedMessage>>,
        std::net::SocketAddr,
    ) {
        let (srv, tx, addr, _) =
            spawn_server_shared(instruments, Arc::new(Mutex::new(books)), None).await;
        (srv, tx, addr)
    }

    /// [`spawn_server`] keeping the caller's handle on the replay map, so a test can advance the
    /// accumulator the way the arbiter does — under the shared lock, *before* the batch is
    /// broadcast — instead of serving a map frozen at connect time.
    async fn spawn_server_shared(
        instruments: HashMap<(crate::model::SourceKey, Arc<str>, u8, u32), NormalizedInstrument>,
        books: crate::model::BookSnapshot,
        deadline: Option<Duration>,
    ) -> (
        tokio::task::JoinHandle<anyhow::Result<()>>,
        broadcast::Sender<std::sync::Arc<FeedMessage>>,
        std::net::SocketAddr,
        crate::model::BookSnapshot,
    ) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (tx, _rx) = broadcast::channel::<std::sync::Arc<FeedMessage>>(16);
        let cfg = WsConfig {
            heartbeat: Duration::from_secs(30),
            idle_timeout: Duration::from_secs(60),
            max_clients: 8,
            max_subs: 8,
            max_inbound_per_min: 600,
            broadcast_capacity: 16,
            lag_repair_min_interval: super::LAG_REPAIR_MIN_INTERVAL,
            bootstrap_release_deadline: deadline.unwrap_or(super::BOOTSTRAP_RELEASE_DEADLINE),
        };
        let srv = tokio::spawn(serve(
            listener,
            tx.clone(),
            Arc::new(Mutex::new(instruments)),
            Arc::new(Mutex::new(HashMap::new())),
            books.clone(),
            cfg,
        ));
        (srv, tx, addr, books)
    }

    /// Advance the shared replay accumulator with a batch, exactly as `ingest::arbiter` does:
    /// under the lock, **before** the batch is broadcast.
    fn fold(books: &crate::model::BookSnapshot, key: &crate::model::BookKey, b: &NormalizedBook) {
        crate::model::lock(books)
            .entry_or_insert_with(key, || BookAccumulator::new(b.symbol.clone()))
            .apply(b);
    }

    /// The next text frame, skipping the server's heartbeat Pings.
    async fn next_frame<S>(ws: &mut S, within: Duration) -> Option<String>
    where
        S: futures_util::StreamExt<Item = Result<WsMessage, tokio_tungstenite::tungstenite::Error>>
            + Unpin,
    {
        timeout(within, async {
            loop {
                match ws.next().await {
                    Some(Ok(WsMessage::Text(t))) => return t.to_string(),
                    Some(Ok(_)) => continue,
                    other => panic!("stream ended: {other:?}"),
                }
            }
        })
        .await
        .ok()
    }

    /// Accepts either book type — the granularity is the market's, and most callers here assert the
    /// body rather than the tag. `book_type` is what pins the tag.
    fn parse_book(frame: &str) -> NormalizedBook {
        match serde_json::from_str(frame).expect("frame parses") {
            FeedMessage::Book(b) | FeedMessage::OrderBook(b) => b,
            other => panic!("expected a book frame, got {other:?}"),
        }
    }

    /// The wire `type` of a frame, verbatim.
    fn book_type(frame: &str) -> String {
        serde_json::from_str::<serde_json::Value>(frame).expect("frame parses")["type"]
            .as_str()
            .expect("a type tag")
            .to_string()
    }

    /// A connecting client is bootstrapped with the accumulated `book` state as a re-baseline: a
    /// `Clear`/`Both` leading the complete level set, best-first, marked `last`.
    #[tokio::test]
    #[serial]
    async fn connect_replays_the_accumulated_book_rebaseline() {
        let mut books = BookReplay::default();
        books.insert(
            (
                crate::model::SourceKey::unassigned("KALSHI"),
                Arc::<str>::from("perps"),
                2u8,
                41u32,
            ),
            accumulator("KXBTCPERP", 0.61, 0.63),
        );
        let (srv, _tx, addr) = spawn_server(HashMap::new(), books).await;

        let (mut ws, _) = tokio_tungstenite::connect_async(format!("ws://{addr}"))
            .await
            .unwrap();
        let frame = next_frame(&mut ws, Duration::from_secs(2))
            .await
            .expect("replayed book");
        let b = parse_book(&frame);
        assert_eq!(
            (&*b.symbol, b.channel, b.instrument_id),
            ("KXBTCPERP", 2, 41)
        );
        assert!(b.snapshot && b.last, "a re-baseline is a complete event");
        assert_eq!(b.changes[0].action, BookAction::Clear);
        assert_eq!(b.changes[0].side, BookSide::Both);
        assert_eq!(
            b.changes[1..],
            [
                level_update(BookSide::Bid, 0.61, 10.0),
                level_update(BookSide::Ask, 0.63, 20.0),
            ]
        );

        srv.abort();
    }

    /// An order-level accumulator, bootstrappable: a `Clear`-led re-baseline holding two resting
    /// orders at one price — the population a price-keyed consumer would collapse to one.
    fn order_level_accumulator(symbol: &str) -> BookAccumulator {
        let mut acc = BookAccumulator::new(symbol.into());
        acc.apply(&book_batch(
            symbol,
            vec![
                BookChange {
                    action: BookAction::Clear,
                    side: BookSide::Both,
                    price: 0.0,
                    size: 0.0,
                    order_id: 0,
                },
                BookChange {
                    action: BookAction::Update,
                    side: BookSide::Bid,
                    price: 100.0,
                    size: 5.0,
                    order_id: 7,
                },
                BookChange {
                    action: BookAction::Update,
                    side: BookSide::Bid,
                    price: 100.0,
                    size: 3.0,
                    order_id: 8,
                },
            ],
            true,
        ));
        acc
    }

    /// The wire `type` follows the market's granularity, and **only** the tag differs: a consumer
    /// routing on `type` never has to inspect a change to learn which product it holds.
    #[test]
    fn the_wire_type_follows_the_market_granularity() {
        use super::prepare;
        let level = book_batch(
            "KXBTCPERP",
            vec![level_update(BookSide::Bid, 0.62, 150.0)],
            true,
        );
        let mut order = level.clone();
        order.order_level = true;
        order.changes[0].order_id = 7;

        let plain = prepare(&FeedMessage::Book(level)).expect("serializes");
        assert_eq!(
            plain.kind, "book",
            "a price-aggregated market keeps the type it always had"
        );
        let f = prepare(&FeedMessage::Book(order)).expect("serializes");
        assert_eq!(f.kind, "order_book");
        assert!(
            f.payload.as_str().contains(r#""type":"order_book""#),
            "the rendered tag must match the filter kind, got {}",
            f.payload.as_str()
        );
        // Same envelope either way, so a consumer that does know `order_book` parses it with the body
        // it already has.
        assert!(f.payload.as_str().contains(r#""order_id":7"#));
        // The filter fields are read off the original message, so re-tagging cannot disturb them —
        // asserted against the price-aggregated frame rather than literals, since losing `channel`
        // here would silently strip order-level markets from every `{"channel":N}` subscriber.
        assert_eq!((f.channel, &f.symbol), (plain.channel, &plain.symbol));
    }

    /// **The compatibility guarantee.** A consumer that subscribes to `type: book` must receive
    /// neither the bootstrap nor the live feed of an order-level market: keying those changes by
    /// price collapses two orders resting at one price to the last one's size. A distinct type is the
    /// mechanism PROTOCOL.md's forward-compatibility rule already promises, which is what makes this
    /// additive rather than breaking.
    ///
    /// Scoped to a client that *asks*, deliberately. The connect-time replay is unfiltered — an empty
    /// filter list is the documented firehose — so an `order_book` frame does reach a client that
    /// subscribed to nothing, and safety there rests on it ignoring unknown types, exactly as it does
    /// for any type added since it was written.
    #[tokio::test]
    #[serial]
    async fn a_book_subscriber_is_never_served_an_order_level_market() {
        use futures_util::SinkExt;
        let mut books = BookReplay::default();
        books.insert(
            (
                crate::model::SourceKey::unassigned("HYPERLIQUID"),
                Arc::<str>::from("perps"),
                2u8,
                41u32,
            ),
            order_level_accumulator("BTC"),
        );
        // A price-aggregated market beside it, so "received nothing" cannot pass this by accident:
        // the `book` subscriber must still be bootstrapped with this one.
        books.insert(
            (
                crate::model::SourceKey::unassigned("KALSHI"),
                Arc::<str>::from("perps"),
                2u8,
                41u32,
            ),
            accumulator("KXBTCPERP", 0.61, 0.63),
        );
        let (srv, tx, addr) = spawn_server(HashMap::new(), books).await;

        let (mut ws, _) = tokio_tungstenite::connect_async(format!("ws://{addr}"))
            .await
            .unwrap();
        // Drain the unfiltered connect replay (both markets) — see the note above.
        for _ in 0..2 {
            next_frame(&mut ws, Duration::from_secs(2))
                .await
                .expect("connect replay");
        }

        ws.send(WsMessage::Text(
            r#"{"method":"subscribe","subscription":{"type":"book"}}"#.into(),
        ))
        .await
        .unwrap();
        let ack = next_frame(&mut ws, Duration::from_secs(2))
            .await
            .expect("subscription ack");
        assert!(ack.contains("subscription_response"), "got {ack}");

        // The scoped replay: the price-aggregated market, and only it.
        let frame = next_frame(&mut ws, Duration::from_secs(2))
            .await
            .expect("scoped book replay");
        assert_eq!(book_type(&frame), "book", "got {frame}");
        assert_eq!(&*parse_book(&frame).symbol, "KXBTCPERP");
        assert_eq!(
            next_frame(&mut ws, Duration::from_millis(300)).await,
            None,
            "the order-level market must not be bootstrapped for a `book` subscription"
        );

        // And the live feed is filtered by the same one `SubFilter::matches` path.
        let mut order = book_batch("BTC", vec![level_update(BookSide::Bid, 100.0, 5.0)], true);
        order.order_level = true;
        order.changes[0].order_id = 9;
        let _ = tx.send(Arc::new(FeedMessage::Book(order)));
        assert_eq!(
            next_frame(&mut ws, Duration::from_millis(300)).await,
            None,
            "a live order-level batch must not reach a `book` subscriber"
        );

        srv.abort();
    }

    /// An order-level market bootstraps as **orders**, matching the feed the client is about to
    /// receive: a level bootstrap followed by order-level changes cannot be reconciled, and no
    /// subscription can ask for one.
    #[tokio::test]
    #[serial]
    async fn the_book_replay_scope_always_follows_the_market() {
        let mut acc = BookAccumulator::new("BTC".into());
        acc.apply(&book_batch(
            "BTC",
            vec![
                BookChange {
                    action: BookAction::Clear,
                    side: BookSide::Both,
                    price: 0.0,
                    size: 0.0,
                    order_id: 0,
                },
                BookChange {
                    action: BookAction::Update,
                    side: BookSide::Bid,
                    price: 100.0,
                    size: 5.0,
                    order_id: 7,
                },
                BookChange {
                    action: BookAction::Update,
                    side: BookSide::Bid,
                    price: 100.0,
                    size: 3.0,
                    order_id: 8,
                },
            ],
            true,
        ));
        let mut books = BookReplay::default();
        books.insert(
            (
                crate::model::SourceKey::unassigned("HYPERLIQUID"),
                Arc::<str>::from("perps"),
                0u8,
                1u32,
            ),
            acc,
        );
        let (srv, _tx, addr) = spawn_server(HashMap::new(), books).await;

        let (mut ws, _) = tokio_tungstenite::connect_async(format!("ws://{addr}"))
            .await
            .unwrap();
        let frame = next_frame(&mut ws, Duration::from_secs(2))
            .await
            .expect("replayed book");
        assert_eq!(
            book_type(&frame),
            "order_book",
            "the bootstrap must carry the same type as the feed it precedes"
        );
        let b = parse_book(&frame);
        assert_eq!(
            b.changes
                .iter()
                .filter(|c| c.action != BookAction::Clear)
                .map(|c| c.order_id)
                .collect::<Vec<_>>(),
            vec![7, 8],
            "the default bootstrap must carry the order ids the feed carries"
        );

        // A subscription cannot ask for anything else. `book_scope: "levels"` used to fold the
        // bootstrap while the live feed stayed order-level, which is unusable: an order-level
        // change carries one *order's* absolute size, and a client handed price levels holds no
        // order state to apply it to. The field is gone, and an unknown key is ignored, so a client
        // still sending it is bootstrapped at the market's own granularity.
        use futures_util::SinkExt;
        ws.send(WsMessage::Text(
            r#"{"method":"subscribe","subscription":{"symbol":"BTC","book_scope":"levels"}}"#
                .into(),
        ))
        .await
        .unwrap();
        let ack = next_frame(&mut ws, Duration::from_secs(2))
            .await
            .expect("subscription ack");
        assert!(ack.contains("subscription_response"), "got {ack}");
        let b = parse_book(
            &next_frame(&mut ws, Duration::from_secs(2))
                .await
                .expect("book replay"),
        );
        assert_eq!(
            b.changes
                .iter()
                .filter(|c| c.action != BookAction::Clear)
                .map(|c| c.order_id)
                .collect::<Vec<_>>(),
            vec![7, 8],
            "an order-level market is always bootstrapped as orders"
        );

        srv.abort();
    }

    /// A `{"channel":N}` subscribe replays only that channel's markets — the reason `replay_scoped`
    /// passes each message's own channel to the filter instead of `None`.
    #[tokio::test]
    #[serial]
    async fn subscribe_scopes_the_book_replay_by_channel() {
        let mut books = BookReplay::default();
        books.insert(
            (
                crate::model::SourceKey::unassigned("KALSHI"),
                Arc::<str>::from("perps"),
                2u8,
                41u32,
            ),
            accumulator("KXBTCPERP", 0.61, 0.63),
        );
        books.insert(
            (
                crate::model::SourceKey::unassigned("KALSHI"),
                Arc::<str>::from("perps"),
                3u8,
                7u32,
            ),
            accumulator("KXETHPERP", 0.41, 0.43),
        );
        let (srv, _tx, addr) = spawn_server(HashMap::new(), books).await;

        let (mut ws, _) = tokio_tungstenite::connect_async(format!("ws://{addr}"))
            .await
            .unwrap();
        // Connect-time replay is unfiltered: both markets arrive.
        for _ in 0..2 {
            next_frame(&mut ws, Duration::from_secs(2))
                .await
                .expect("replayed book");
        }

        use futures_util::SinkExt;
        ws.send(WsMessage::Text(
            r#"{"method":"subscribe","subscription":{"channel":3}}"#.into(),
        ))
        .await
        .unwrap();

        let ack = next_frame(&mut ws, Duration::from_secs(2))
            .await
            .expect("subscription ack");
        assert!(ack.contains("subscription_response"), "got {ack}");
        let b = parse_book(
            &next_frame(&mut ws, Duration::from_secs(2))
                .await
                .expect("scoped book replay"),
        );
        assert_eq!((b.channel, b.instrument_id), (3, 7));
        assert_eq!(
            next_frame(&mut ws, Duration::from_millis(200)).await,
            None,
            "channel 2 must not be replayed for a channel 3 subscription"
        );

        srv.abort();
    }

    /// A `channel` past the wire's width is refused as a malformed frame, not accepted as a filter
    /// that then matches nothing for the life of the client. The frame is refused, the connection is
    /// not.
    #[tokio::test]
    #[serial]
    async fn a_subscribe_channel_above_the_wire_width_is_answered_with_an_error() {
        let (srv, _tx, addr) = spawn_server(HashMap::new(), BookReplay::default()).await;
        let (mut ws, _) = tokio_tungstenite::connect_async(format!("ws://{addr}"))
            .await
            .unwrap();

        use futures_util::SinkExt;
        ws.send(WsMessage::Text(
            r#"{"method":"subscribe","subscription":{"channel":300}}"#.into(),
        ))
        .await
        .unwrap();
        let frame = next_frame(&mut ws, Duration::from_secs(2))
            .await
            .expect("error frame");
        let v: serde_json::Value = serde_json::from_str(&frame).expect("frame parses");
        assert_eq!(v["channel"], "error", "got {frame}");
        assert_eq!(v["error"], "unrecognized message", "got {frame}");

        ws.send(WsMessage::Text(
            r#"{"method":"subscribe","subscription":{"channel":3}}"#.into(),
        ))
        .await
        .unwrap();
        let ack = next_frame(&mut ws, Duration::from_secs(2))
            .await
            .expect("subscription ack");
        assert!(ack.contains("subscription_response"), "got {ack}");

        srv.abort();
    }

    /// Precision before price: the `instrument` definition is replayed ahead of the market's `book`.
    #[tokio::test]
    #[serial]
    async fn instrument_is_replayed_before_the_book() {
        let mut defs = HashMap::new();
        defs.insert(
            (
                crate::model::SourceKey::from("KALSHI"),
                Arc::<str>::from("perps"),
                2u8,
                41u32,
            ),
            NormalizedInstrument {
                tick_size: 0,
                venue: "KALSHI".into(),
                source_name: "KALSHI".into(),
                source_id: 0,
                symbol: "KXBTCPERP".into(),
                channel: 2,
                instrument_id: 41,
                category: "perps".into(),
                price_exponent: -2,
                qty_exponent: -2,
            },
        );
        let mut books = BookReplay::default();
        books.insert(
            (
                crate::model::SourceKey::unassigned("KALSHI"),
                Arc::<str>::from("perps"),
                2u8,
                41u32,
            ),
            accumulator("KXBTCPERP", 0.61, 0.63),
        );
        let (srv, _tx, addr) = spawn_server(defs, books).await;

        let (mut ws, _) = tokio_tungstenite::connect_async(format!("ws://{addr}"))
            .await
            .unwrap();
        let first = next_frame(&mut ws, Duration::from_secs(2))
            .await
            .expect("replayed instrument");
        assert!(
            first.contains(r#""type":"instrument""#) && first.contains("KXBTCPERP"),
            "definition must arrive first, got {first}"
        );
        let second = next_frame(&mut ws, Duration::from_secs(2))
            .await
            .expect("replayed book");
        assert_eq!(parse_book(&second).channel, 2);

        srv.abort();
    }

    /// ⚠️ **A market mid-event at its join is withheld, and released at its next completed
    /// event** — not at its next producer re-baseline, which is one or two an hour.
    ///
    /// `to_book` materializes folded state, so the batches still buffered behind their `last` are
    /// in no bootstrap, and the ones broadcast before this client's `rx` subscribed are in no queue
    /// either: applying what does arrive leaves the untouched levels at values the client invented.
    /// The event closes at the venue's next slot boundary, under a second, and the accumulator is a
    /// whole book again — which is what this waits for. A peer market on the same socket is served
    /// throughout.
    #[tokio::test]
    #[serial]
    async fn a_market_mid_event_is_withheld_until_its_event_closes() {
        let stamped = |b: NormalizedBook, recv_ts_ns: u64, id: u32| NormalizedBook {
            recv_ts_ns,
            channel: 2,
            instrument_id: id,
            category: "perps".into(),
            ..b
        };
        let key = |id: u32| {
            (
                crate::model::SourceKey::unassigned("KALSHI"),
                Arc::<str>::from("perps"),
                2u8,
                id,
            )
        };

        // 41 is mid-event: complete through `recv_ts_ns` 100, with one batch buffered behind its
        // `last`. 42 is a complete peer market, and must be unaffected throughout.
        let mut mid_event = BookAccumulator::new("KXBTCPERP".into());
        mid_event.apply(&stamped(
            book_batch(
                "KXBTCPERP",
                vec![
                    BookChange {
                        action: BookAction::Clear,
                        side: BookSide::Both,
                        price: 0.0,
                        size: 0.0,
                        order_id: 0,
                    },
                    level_update(BookSide::Bid, 0.62, 7.0),
                ],
                true,
            ),
            100,
            41,
        ));
        mid_event.apply(&stamped(
            book_batch(
                "KXBTCPERP",
                vec![level_update(BookSide::Ask, 0.64, 3.0)],
                false,
            ),
            200,
            41,
        ));
        assert!(mid_event.baselined() && !mid_event.pending_empty());

        let mut books = BookReplay::default();
        books.insert(key(41), mid_event);
        books.insert(key(42), accumulator("KXETHPERP", 0.41, 0.43));
        let (srv, tx, addr, books) =
            spawn_server_shared(HashMap::new(), Arc::new(Mutex::new(books)), None).await;

        let (mut ws, _) = tokio_tungstenite::connect_async(format!("ws://{addr}"))
            .await
            .unwrap();
        assert_eq!(
            parse_book(
                &next_frame(&mut ws, Duration::from_secs(2))
                    .await
                    .expect("the peer market is bootstrapped")
            )
            .instrument_id,
            42,
            "only the mid-event market is withheld"
        );
        assert_eq!(
            next_frame(&mut ws, Duration::from_millis(200)).await,
            None,
            "no bootstrap for a market whose event is still open"
        );

        // A batch of the open event must not reach the client either: it has no baseline to apply
        // it to, and the batch that opened the event is in neither place.
        let more = stamped(
            book_batch(
                "KXBTCPERP",
                vec![level_update(BookSide::Ask, 0.65, 4.0)],
                false,
            ),
            300,
            41,
        );
        fold(&books, &key(41), &more);
        let _ = tx.send(std::sync::Arc::new(FeedMessage::Book(more)));
        assert_eq!(
            next_frame(&mut ws, Duration::from_millis(200)).await,
            None,
            "an incremental batch onto a book the client does not hold"
        );

        // The venue's next boundary closes the event. The arbiter folds it into the shared
        // accumulator before it broadcasts, so the frame that carries the close is also what proves
        // the market is whole again.
        let closing = stamped(
            book_batch(
                "KXBTCPERP",
                vec![level_update(BookSide::Bid, 0.63, 5.0)],
                true,
            ),
            400,
            41,
        );
        fold(&books, &key(41), &closing);
        let _ = tx.send(std::sync::Arc::new(FeedMessage::Book(closing)));
        let boot = parse_book(
            &next_frame(&mut ws, Duration::from_secs(2))
                .await
                .expect("the withheld bootstrap is released"),
        );
        assert_eq!(
            (boot.instrument_id, boot.snapshot, boot.last),
            (41, true, true)
        );
        assert_eq!(
            boot.changes.first().map(|c| c.action),
            Some(BookAction::Clear),
            "a bootstrap is clear-led"
        );
        // The batch that carried the close is in the bootstrap and ties with its wire point, so it
        // is re-delivered rather than dropped — the same rule the join watermark follows, and free:
        // a change carries an absolute size.
        let echo = parse_book(
            &next_frame(&mut ws, Duration::from_secs(2))
                .await
                .expect("the closing batch ties with the bootstrap"),
        );
        assert_eq!((echo.instrument_id, echo.recv_ts_ns), (41, 400));

        // And the live feed flows from there, for both markets.
        for (id, symbol) in [(41u32, "KXBTCPERP"), (42, "KXETHPERP")] {
            let live = stamped(
                book_batch(symbol, vec![level_update(BookSide::Bid, 0.60, 1.0)], true),
                500 + u64::from(id),
                id,
            );
            fold(&books, &key(id), &live);
            let _ = tx.send(std::sync::Arc::new(FeedMessage::Book(live)));
            assert_eq!(
                parse_book(
                    &next_frame(&mut ws, Duration::from_secs(2))
                        .await
                        .expect("the market is live")
                )
                .instrument_id,
                id
            );
        }
        srv.abort();
    }

    /// The deadline backstop: a market whose event never closes is bootstrapped from the last
    /// complete state anyway, rather than leaving the client dark for the life of the connection.
    /// A quiet market produces no frame to carry the check, so this runs off the sweep tick alone.
    #[tokio::test]
    #[serial]
    async fn a_market_whose_event_never_closes_is_released_at_the_deadline() {
        let key = (
            crate::model::SourceKey::unassigned("KALSHI"),
            Arc::<str>::from("perps"),
            2u8,
            41u32,
        );
        let mut mid_event = accumulator("KXBTCPERP", 0.61, 0.63);
        mid_event.apply(&NormalizedBook {
            channel: 2,
            instrument_id: 41,
            category: "perps".into(),
            ..book_batch(
                "KXBTCPERP",
                vec![level_update(BookSide::Bid, 0.62, 7.0)],
                false,
            )
        });
        assert!(mid_event.baselined() && !mid_event.pending_empty());
        let mut books = BookReplay::default();
        books.insert(key, mid_event);

        let released = || {
            metrics()
                .ws_bootstrap_withheld
                .with_label_values(&["KALSHI", "deadline"])
                .get()
        };
        let before = released();
        let (srv, _tx, addr, _books) = spawn_server_shared(
            HashMap::new(),
            Arc::new(Mutex::new(books)),
            Some(Duration::from_millis(1)),
        )
        .await;

        let (mut ws, _) = tokio_tungstenite::connect_async(format!("ws://{addr}"))
            .await
            .unwrap();
        // The sweep runs at 1 Hz, so the first tick is the one that discharges this.
        let boot = parse_book(
            &next_frame(&mut ws, Duration::from_secs(3))
                .await
                .expect("the deadline releases the market"),
        );
        assert_eq!(
            (boot.instrument_id, boot.snapshot),
            (41, true),
            "a clear-led re-baseline from the accumulator"
        );
        assert_eq!(released(), before + 1, "counted as a deadline release");
        srv.abort();
    }

    /// A market the connect replay withheld and a later `subscribe` puts out of scope must leave
    /// the withhold list, not sit on it for the life of the connection. It is not owed a bootstrap
    /// while it is out of scope, so charging its frames to
    /// `dz_ws_frames_dropped_total{reason="awaiting"}` reads as a client dark on a book it never
    /// asked for, and the sweep arm stays armed taking the shared `BookSnapshot` mutex once a
    /// second for it.
    #[tokio::test]
    #[serial]
    async fn a_market_put_out_of_scope_leaves_the_withhold_list() {
        let (books, tx, _addr, srv) = withheld_and_narrowed().await;

        // A frame for the out-of-scope market: excluded by the client's filter either way, so what
        // this pins is the accounting, not the delivery.
        let awaiting = || {
            metrics()
                .ws_frames_dropped
                .with_label_values(&["KALSHI", "awaiting"])
                .get()
        };
        let before = awaiting();
        let mut ws = books.1;
        let more = mid_event_batch(300, false);
        fold(&books.0, &mid_event_key(), &more);
        let _ = tx.send(std::sync::Arc::new(FeedMessage::Book(more)));
        assert_eq!(
            next_frame(&mut ws, Duration::from_millis(300)).await,
            None,
            "the client's own filter excludes it"
        );
        assert_eq!(
            awaiting(),
            before,
            "an out-of-scope market is not a withheld one"
        );
        srv.abort();
    }

    /// ⚠️ And dropping it must not leave the market unaccounted for: `unsubscribe` widens the
    /// client's scope back, and a market with no bootstrap and no withhold would then have its
    /// incremental batches applied to a book the client does not hold — the corruption the withhold
    /// exists to prevent. Widening re-runs the book bootstrap, exactly as `subscribe` does.
    #[tokio::test]
    #[serial]
    async fn widening_the_scope_again_re_accounts_for_the_market() {
        let (books, tx, _addr, srv) = withheld_and_narrowed().await;
        let (books, mut ws) = books;

        // Take the market out of the withhold list the way the test above does.
        let more = mid_event_batch(300, false);
        fold(&books, &mid_event_key(), &more);
        let _ = tx.send(std::sync::Arc::new(FeedMessage::Book(more)));
        assert_eq!(next_frame(&mut ws, Duration::from_millis(300)).await, None);

        use futures_util::SinkExt;
        ws.send(WsMessage::Text(
            r#"{"method":"unsubscribe","subscription":{"symbol":"KXETHPERP"}}"#.into(),
        ))
        .await
        .unwrap();
        let ack = next_frame(&mut ws, Duration::from_secs(2))
            .await
            .expect("unsubscribe ack");
        assert!(ack.contains("subscription_response"), "got {ack}");
        // The widening re-bootstraps the books back in scope: 42 is complete, 41 is mid-event and
        // so is re-withheld rather than sent.
        assert_eq!(
            parse_book(
                &next_frame(&mut ws, Duration::from_secs(2))
                    .await
                    .expect("the complete market is re-bootstrapped")
            )
            .instrument_id,
            42
        );

        // Back on the firehose, and the market is still mid-event: its batches must still be
        // withheld rather than applied to a book this client was never given.
        let open = mid_event_batch(400, false);
        fold(&books, &mid_event_key(), &open);
        let _ = tx.send(std::sync::Arc::new(FeedMessage::Book(open)));
        assert_eq!(
            next_frame(&mut ws, Duration::from_millis(300)).await,
            None,
            "an incremental batch onto a book the client does not hold"
        );

        // And the event closing releases it, as it would for any withheld market.
        let closing = mid_event_batch(500, true);
        fold(&books, &mid_event_key(), &closing);
        let _ = tx.send(std::sync::Arc::new(FeedMessage::Book(closing)));
        let boot = parse_book(
            &next_frame(&mut ws, Duration::from_secs(2))
                .await
                .expect("the market is bootstrapped once its event closes"),
        );
        assert_eq!((boot.instrument_id, boot.snapshot), (41, true));
        srv.abort();
    }

    fn mid_event_key() -> crate::model::BookKey {
        (
            crate::model::SourceKey::unassigned("KALSHI"),
            Arc::<str>::from("perps"),
            2u8,
            41u32,
        )
    }

    fn mid_event_batch(recv_ts_ns: u64, last: bool) -> NormalizedBook {
        NormalizedBook {
            recv_ts_ns,
            channel: 2,
            instrument_id: 41,
            category: "perps".into(),
            ..book_batch(
                "KXBTCPERP",
                vec![level_update(BookSide::Ask, 0.64, 3.0)],
                last,
            )
        }
    }

    /// A connected client that was withheld market 41 (mid-event at connect) and has since
    /// subscribed to a different symbol, so 41 is out of its scope.
    #[allow(clippy::type_complexity)]
    async fn withheld_and_narrowed() -> (
        (
            crate::model::BookSnapshot,
            tokio_tungstenite::WebSocketStream<
                tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
            >,
        ),
        broadcast::Sender<std::sync::Arc<FeedMessage>>,
        std::net::SocketAddr,
        tokio::task::JoinHandle<anyhow::Result<()>>,
    ) {
        let mut mid_event = accumulator("KXBTCPERP", 0.61, 0.63);
        mid_event.apply(&mid_event_batch(100, false));
        assert!(mid_event.baselined() && !mid_event.pending_empty());
        let mut books = BookReplay::default();
        books.insert(mid_event_key(), mid_event);
        books.insert(
            (
                crate::model::SourceKey::unassigned("KALSHI"),
                Arc::<str>::from("perps"),
                2u8,
                42u32,
            ),
            accumulator("KXETHPERP", 0.41, 0.43),
        );
        let (srv, tx, addr, books) = spawn_server_shared(
            HashMap::new(),
            Arc::new(Mutex::new(books)),
            // Long, so what these two tests pin is the scope accounting and never the deadline
            // release — which would otherwise discharge the market the moment its scope widened.
            Some(Duration::from_secs(60)),
        )
        .await;

        let (mut ws, _) = tokio_tungstenite::connect_async(format!("ws://{addr}"))
            .await
            .unwrap();
        // Connect is unfiltered: 42 is bootstrapped, 41 is withheld.
        assert_eq!(
            parse_book(
                &next_frame(&mut ws, Duration::from_secs(2))
                    .await
                    .expect("the complete market")
            )
            .instrument_id,
            42
        );
        use futures_util::SinkExt;
        ws.send(WsMessage::Text(
            r#"{"method":"subscribe","subscription":{"symbol":"KXETHPERP"}}"#.into(),
        ))
        .await
        .unwrap();
        let ack = next_frame(&mut ws, Duration::from_secs(2))
            .await
            .expect("subscribe ack");
        assert!(ack.contains("subscription_response"), "got {ack}");
        assert_eq!(
            parse_book(
                &next_frame(&mut ws, Duration::from_secs(2))
                    .await
                    .expect("the scoped replay")
            )
            .instrument_id,
            42
        );
        // Long enough for a sweep tick to have run over the withheld market.
        tokio::time::sleep(Duration::from_millis(1200)).await;
        ((books, ws), tx, addr, srv)
    }

    /// A market with no complete book anywhere in this process stays withheld at the deadline too:
    /// an accumulator seeded partway through holds only what has moved since, and there is nothing
    /// honest to bootstrap a client with. Dark is the correct answer, and the invariant the
    /// deadline must not trade away.
    #[tokio::test]
    #[serial]
    async fn the_deadline_never_hands_out_a_partial_book() {
        let mut partway = BookAccumulator::new("KXBTCPERP".into());
        partway.apply(&NormalizedBook {
            channel: 2,
            instrument_id: 41,
            category: "perps".into(),
            ..book_batch(
                "KXBTCPERP",
                vec![level_update(BookSide::Bid, 0.62, 7.0)],
                true,
            )
        });
        assert!(!partway.baselined(), "no Clear was folded in");
        let mut books = BookReplay::default();
        books.insert(
            (
                crate::model::SourceKey::unassigned("KALSHI"),
                Arc::<str>::from("perps"),
                2u8,
                41u32,
            ),
            partway,
        );
        let (srv, _tx, addr, _books) = spawn_server_shared(
            HashMap::new(),
            Arc::new(Mutex::new(books)),
            Some(Duration::from_millis(1)),
        )
        .await;

        let (mut ws, _) = tokio_tungstenite::connect_async(format!("ws://{addr}"))
            .await
            .unwrap();
        assert_eq!(
            next_frame(&mut ws, Duration::from_millis(1500)).await,
            None,
            "nothing to send, so nothing is sent"
        );
        srv.abort();
    }

    /// re-baseline, as it did before the book replay existed.
    #[tokio::test]
    #[serial]
    async fn markets_accumulated_partway_are_not_replayed() {
        let mut accumulated_partway = BookAccumulator::new("KXETHPERP".into());
        accumulated_partway.apply(&book_batch(
            "KXETHPERP",
            vec![level_update(BookSide::Bid, 0.41, 5.0)],
            true,
        ));
        assert!(!accumulated_partway.baselined(), "no Clear was folded in");

        let mut books = BookReplay::default();
        books.insert(
            (
                crate::model::SourceKey::unassigned("KALSHI"),
                Arc::<str>::from("perps"),
                3u8,
                7u32,
            ),
            accumulated_partway,
        );
        books.insert(
            (
                crate::model::SourceKey::unassigned("KALSHI"),
                Arc::<str>::from("perps"),
                2u8,
                41u32,
            ),
            accumulator("KXBTCPERP", 0.61, 0.63),
        );
        let (srv, _tx, addr) = spawn_server(HashMap::new(), books).await;

        let (mut ws, _) = tokio_tungstenite::connect_async(format!("ws://{addr}"))
            .await
            .unwrap();
        let b = parse_book(
            &next_frame(&mut ws, Duration::from_secs(2))
                .await
                .expect("replayed book"),
        );
        assert_eq!((b.channel, b.instrument_id), (2, 41));
        assert_eq!(
            next_frame(&mut ws, Duration::from_millis(200)).await,
            None,
            "a market with no re-baseline must not be replayed as full state"
        );

        srv.abort();
    }

    /// A client that lagged is re-baselined rather than left holding a book missing a batch. The lag
    /// is deterministic: the receiver is overflowed before `serve_client` first polls it, so the very
    /// first `recv` returns `Lagged`.
    #[tokio::test]
    #[serial]
    async fn a_lagging_client_is_rebaselined() {
        use super::{prepare, serve_client};

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (prepared_tx, prepared_rx) = broadcast::channel(1);
        for _ in 0..2 {
            let frame = prepare(&FeedMessage::Quote(sample_quote())).expect("serializes");
            assert!(prepared_tx.send(frame).is_ok(), "the receiver is alive");
        }

        let mut books = BookReplay::default();
        books.insert(
            (
                crate::model::SourceKey::unassigned("KALSHI"),
                Arc::<str>::from("perps"),
                2u8,
                41u32,
            ),
            accumulator("KXBTCPERP", 0.61, 0.63),
        );
        let cfg = WsConfig {
            heartbeat: Duration::from_secs(30),
            idle_timeout: Duration::from_secs(60),
            max_clients: 8,
            max_subs: 8,
            max_inbound_per_min: 600,
            broadcast_capacity: 1,
            lag_repair_min_interval: super::LAG_REPAIR_MIN_INTERVAL,
            bootstrap_release_deadline: super::BOOTSTRAP_RELEASE_DEADLINE,
        };
        let srv = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            serve_client(
                stream,
                prepared_rx,
                Arc::new(Mutex::new(HashMap::new())),
                Arc::new(Mutex::new(HashMap::new())),
                Arc::new(Mutex::new(books)),
                cfg,
            )
            .await
        });

        let (mut ws, _) = tokio_tungstenite::connect_async(format!("ws://{addr}"))
            .await
            .unwrap();
        // Connect replay, the re-baseline the lag triggers, then the frame that survived the overflow.
        for expected in ["connect replay", "re-baseline after lag"] {
            let frame = next_frame(&mut ws, Duration::from_secs(2))
                .await
                .expect(expected);
            assert_eq!(parse_book(&frame).instrument_id, 41, "{expected}");
        }
        let quote = next_frame(&mut ws, Duration::from_secs(2))
            .await
            .expect("surviving quote");
        assert!(quote.contains(r#""type":"quote""#), "got {quote}");

        srv.abort();
    }

    /// The lag repair is **books only**: a client that fell behind is not re-sent the instrument
    /// catalog. That is the #149 loop — the definitions outnumber the markets by orders of magnitude,
    /// so replaying them writes far more into an already-behind client than the batches it actually
    /// lost, which re-arms the lag and converges the connection on replaying instead of streaming.
    /// A dropped definition heals on the publisher's next refdata burst instead (see `Replay`).
    #[tokio::test]
    #[serial]
    async fn a_lag_repair_replays_books_but_not_the_instrument_catalog() {
        use super::{prepare, serve_client};

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (prepared_tx, prepared_rx) = broadcast::channel(1);
        // Two sends with no await between them: the runtime is current-thread, so the client task
        // cannot drain the first, and the capacity-1 buffer overflows deterministically.
        for _ in 0..2 {
            let frame = prepare(&FeedMessage::Quote(sample_quote())).expect("serializes");
            assert!(prepared_tx.send(frame).is_ok(), "the receiver is alive");
        }

        let mut defs = HashMap::new();
        defs.insert(
            (
                crate::model::SourceKey::from("KALSHI"),
                Arc::<str>::from("perps"),
                2u8,
                41u32,
            ),
            NormalizedInstrument {
                tick_size: 0,
                venue: "KALSHI".into(),
                source_name: "KALSHI".into(),
                source_id: 0,
                symbol: "KXBTCPERP".into(),
                channel: 2,
                instrument_id: 41,
                category: "perps".into(),
                price_exponent: -2,
                qty_exponent: -2,
            },
        );
        let mut books = BookReplay::default();
        books.insert(
            (
                crate::model::SourceKey::unassigned("KALSHI"),
                Arc::<str>::from("perps"),
                2u8,
                41u32,
            ),
            accumulator("KXBTCPERP", 0.61, 0.63),
        );
        let cfg = WsConfig {
            heartbeat: Duration::from_secs(30),
            idle_timeout: Duration::from_secs(60),
            max_clients: 8,
            max_subs: 8,
            max_inbound_per_min: 600,
            broadcast_capacity: 1,
            lag_repair_min_interval: super::LAG_REPAIR_MIN_INTERVAL,
            bootstrap_release_deadline: super::BOOTSTRAP_RELEASE_DEADLINE,
        };
        let srv = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            serve_client(
                stream,
                prepared_rx,
                Arc::new(Mutex::new(defs)),
                Arc::new(Mutex::new(HashMap::new())),
                Arc::new(Mutex::new(books)),
                cfg,
            )
            .await
        });

        let (mut ws, _) = tokio_tungstenite::connect_async(format!("ws://{addr}"))
            .await
            .unwrap();
        // The connect bootstrap is still the full one: definition first, then the book.
        let connect_def = next_frame(&mut ws, Duration::from_secs(2))
            .await
            .expect("connect replay definition");
        assert_eq!(book_type(&connect_def), "instrument", "got {connect_def}");
        let connect_book = next_frame(&mut ws, Duration::from_secs(2))
            .await
            .expect("connect replay book");
        assert_eq!(parse_book(&connect_book).instrument_id, 41);

        // The lag repair that follows carries the book and nothing else — the next frame after it is
        // the quote that survived the overflow, so no definition was re-sent in between.
        let repair = next_frame(&mut ws, Duration::from_secs(2))
            .await
            .expect("re-baseline after lag");
        assert_eq!(parse_book(&repair).instrument_id, 41, "got {repair}");
        let quote = next_frame(&mut ws, Duration::from_secs(2))
            .await
            .expect("surviving quote");
        assert!(
            quote.contains(r#""type":"quote""#),
            "the lag repair must not replay the catalog, got {quote}"
        );

        srv.abort();
    }

    /// The join race: a batch broadcast between `prepared_tx.subscribe()` and the replay's read of
    /// the accumulator is queued *behind* a bootstrap that already contains it. Re-applying it
    /// walks the client's book backwards — the SOL bid-equals-ask state Ellipsis observed, where a
    /// snapshot at one slot was followed by deltas from an earlier one.
    ///
    /// The market is folded at `N`; `N - 1` (already in the bootstrap) and `N + 1` (not) are both
    /// queued before the client is served. Only `N + 1` may reach it.
    #[tokio::test]
    #[serial]
    async fn a_batch_already_in_the_bootstrap_is_not_replayed_after_it() {
        use super::{prepare, serve_client};

        const N: u64 = 1_000;
        let market = (
            crate::model::SourceKey::unassigned("KALSHI"),
            Arc::<str>::from("perps"),
            2u8,
            41u32,
        );
        // `recv_ts_ns` is the watermark's unit, so the queued frames straddle the fold at `N`.
        let queued = |recv_ts_ns: u64, price: f64| {
            prepare(&FeedMessage::Book(NormalizedBook {
                recv_ts_ns,
                category: "perps".into(),
                channel: 2,
                instrument_id: 41,
                changes: vec![level_update(BookSide::Bid, price, 10.0)],
                ..book_batch("KXBTCPERP", Vec::new(), true)
            }))
            .expect("serializes")
        };

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (prepared_tx, prepared_rx) = broadcast::channel(8);
        for (ts, price) in [(N - 1, 0.55), (N + 1, 0.57)] {
            assert!(
                prepared_tx.send(queued(ts, price)).is_ok(),
                "the receiver is alive"
            );
        }

        let mut acc = BookAccumulator::new("KXBTCPERP".into());
        acc.apply(&NormalizedBook {
            recv_ts_ns: N,
            ..book_batch(
                "KXBTCPERP",
                vec![
                    BookChange {
                        action: BookAction::Clear,
                        side: BookSide::Both,
                        price: 0.0,
                        size: 0.0,
                        order_id: 0,
                    },
                    level_update(BookSide::Bid, 0.56, 10.0),
                ],
                true,
            )
        });
        let mut books = BookReplay::default();
        books.insert(market, acc);

        let cfg = WsConfig {
            heartbeat: Duration::from_secs(30),
            idle_timeout: Duration::from_secs(60),
            max_clients: 8,
            max_subs: 8,
            max_inbound_per_min: 600,
            broadcast_capacity: 8,
            lag_repair_min_interval: super::LAG_REPAIR_MIN_INTERVAL,
            bootstrap_release_deadline: super::BOOTSTRAP_RELEASE_DEADLINE,
        };
        let srv = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            serve_client(
                stream,
                prepared_rx,
                Arc::new(Mutex::new(HashMap::new())),
                Arc::new(Mutex::new(HashMap::new())),
                Arc::new(Mutex::new(books)),
                cfg,
            )
            .await
        });

        let (mut ws, _) = tokio_tungstenite::connect_async(format!("ws://{addr}"))
            .await
            .unwrap();
        let bootstrap = parse_book(
            &next_frame(&mut ws, Duration::from_secs(2))
                .await
                .expect("connect replay"),
        );
        assert!(bootstrap.snapshot, "the bootstrap is the re-baseline");
        let live = parse_book(
            &next_frame(&mut ws, Duration::from_secs(2))
                .await
                .expect("the batch the bootstrap does not contain"),
        );
        assert_eq!(
            live.recv_ts_ns,
            N + 1,
            "the stale queued batch must be dropped, not forwarded"
        );
        assert_eq!(
            next_frame(&mut ws, Duration::from_millis(200)).await,
            None,
            "nothing else is owed"
        );

        srv.abort();
    }

    /// The tie, which is why the gate is strictly `<`. One datagram straddling a venue batch
    /// boundary emits two batches for one market at the same `recv_ts_ns`: the boundary's closing
    /// batch, which folds, and the changes after it, which only buffer. The watermark then sits at
    /// their shared stamp while the bootstrap holds the first alone, so dropping at the tie would
    /// take the second with it — permanently, since the next boundary would deliver its `last` over
    /// a buffer missing those changes.
    #[tokio::test]
    #[serial]
    async fn a_batch_tied_with_the_bootstrap_is_still_delivered() {
        use super::{prepare, serve_client};

        const T: u64 = 1_000;
        let market = (
            crate::model::SourceKey::unassigned("KALSHI"),
            Arc::<str>::from("perps"),
            2u8,
            41u32,
        );
        let tied = |changes: Vec<BookChange>, last: bool| {
            prepare(&FeedMessage::Book(NormalizedBook {
                recv_ts_ns: T,
                category: "perps".into(),
                channel: 2,
                instrument_id: 41,
                last,
                ..book_batch("KXBTCPERP", changes, last)
            }))
            .expect("serializes")
        };

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (prepared_tx, prepared_rx) = broadcast::channel(8);
        // Closing batch (folds, so it IS in the bootstrap), then the post-boundary changes
        // (buffered, so they are NOT) — same datagram, same stamp.
        for frame in [
            tied(vec![level_update(BookSide::Bid, 0.56, 10.0)], true),
            tied(vec![level_update(BookSide::Ask, 0.60, 20.0)], false),
        ] {
            assert!(prepared_tx.send(frame).is_ok(), "the receiver is alive");
        }

        // The replay accumulator has folded only the first: `wire_ts_ns == T`, ask absent.
        let mut acc = BookAccumulator::new("KXBTCPERP".into());
        acc.apply(&NormalizedBook {
            recv_ts_ns: T,
            ..book_batch(
                "KXBTCPERP",
                vec![
                    BookChange {
                        action: BookAction::Clear,
                        side: BookSide::Both,
                        price: 0.0,
                        size: 0.0,
                        order_id: 0,
                    },
                    level_update(BookSide::Bid, 0.56, 10.0),
                ],
                true,
            )
        });
        assert_eq!(acc.wire_ts_ns(), T, "the watermark ties with both frames");
        let mut books = BookReplay::default();
        books.insert(market, acc);

        let cfg = WsConfig {
            heartbeat: Duration::from_secs(30),
            idle_timeout: Duration::from_secs(60),
            max_clients: 8,
            max_subs: 8,
            max_inbound_per_min: 600,
            broadcast_capacity: 8,
            lag_repair_min_interval: super::LAG_REPAIR_MIN_INTERVAL,
            bootstrap_release_deadline: super::BOOTSTRAP_RELEASE_DEADLINE,
        };
        let srv = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            serve_client(
                stream,
                prepared_rx,
                Arc::new(Mutex::new(HashMap::new())),
                Arc::new(Mutex::new(HashMap::new())),
                Arc::new(Mutex::new(books)),
                cfg,
            )
            .await
        });

        let (mut ws, _) = tokio_tungstenite::connect_async(format!("ws://{addr}"))
            .await
            .unwrap();
        // Drive the frames through the same accumulator a consumer would: the bootstrap, then both
        // ties. Re-applying the folded one is a no-op because a change carries an absolute size.
        let mut consumer = BookAccumulator::new("KXBTCPERP".into());
        for expected in ["bootstrap", "the folded tie", "the buffered tie"] {
            let frame = next_frame(&mut ws, Duration::from_secs(2))
                .await
                .unwrap_or_else(|| panic!("{expected} must reach the client"));
            consumer.apply(&parse_book(&frame));
        }
        // The buffered tie carried no `last`, so close its event as the next boundary would.
        consumer.apply(&NormalizedBook {
            recv_ts_ns: T + 1,
            ..book_batch("KXBTCPERP", Vec::new(), true)
        });
        assert_eq!(
            (consumer.best_bid(), consumer.best_ask()),
            (Some((0.56, 10.0)), Some((0.60, 20.0))),
            "the post-boundary changes must survive the join"
        );

        srv.abort();
    }

    /// ⚠️ **A watermark may only drop the queued prefix that predates the bootstrap.** Broadcast
    /// order is wire order, so the first frame that passes proves every later one is newer than the
    /// bootstrap — whatever their stamps say. Without the spend, a stamp that runs backwards after
    /// the join (the arbiter's synthesized re-baselines read `now_ns()` where a batch carries the
    /// datagram's own stamp, and a host clock can step) takes the market off that client for good.
    #[tokio::test]
    #[serial]
    async fn the_watermark_is_spent_on_the_first_frame_that_passes() {
        use super::{prepare, serve_client};

        const N: u64 = 1_000;
        let market = (
            crate::model::SourceKey::unassigned("KALSHI"),
            Arc::<str>::from("perps"),
            2u8,
            41u32,
        );
        let queued = |recv_ts_ns: u64, price: f64| {
            prepare(&FeedMessage::Book(NormalizedBook {
                recv_ts_ns,
                category: "perps".into(),
                channel: 2,
                instrument_id: 41,
                changes: vec![level_update(BookSide::Bid, price, 10.0)],
                ..book_batch("KXBTCPERP", Vec::new(), true)
            }))
            .expect("serializes")
        };

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (prepared_tx, prepared_rx) = broadcast::channel(8);
        // In the bootstrap; past it; then a stamp that runs backwards again, which is *after* the
        // bootstrap in wire order and so must be forwarded.
        for (ts, price) in [(N - 1, 0.55), (N + 1, 0.57), (N - 2, 0.58)] {
            assert!(
                prepared_tx.send(queued(ts, price)).is_ok(),
                "receiver alive"
            );
        }

        let mut acc = BookAccumulator::new("KXBTCPERP".into());
        acc.apply(&NormalizedBook {
            recv_ts_ns: N,
            ..book_batch(
                "KXBTCPERP",
                vec![
                    BookChange {
                        action: BookAction::Clear,
                        side: BookSide::Both,
                        price: 0.0,
                        size: 0.0,
                        order_id: 0,
                    },
                    level_update(BookSide::Bid, 0.56, 10.0),
                ],
                true,
            )
        });
        let mut books = BookReplay::default();
        books.insert(market, acc);

        let dropped = || {
            metrics()
                .ws_frames_dropped
                .with_label_values(&["KALSHI", "watermark"])
                .get()
        };
        let before = dropped();
        let cfg = WsConfig {
            heartbeat: Duration::from_secs(30),
            idle_timeout: Duration::from_secs(60),
            max_clients: 8,
            max_subs: 8,
            max_inbound_per_min: 600,
            broadcast_capacity: 8,
            lag_repair_min_interval: super::LAG_REPAIR_MIN_INTERVAL,
            bootstrap_release_deadline: super::BOOTSTRAP_RELEASE_DEADLINE,
        };
        let srv = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            serve_client(
                stream,
                prepared_rx,
                Arc::new(Mutex::new(HashMap::new())),
                Arc::new(Mutex::new(HashMap::new())),
                Arc::new(Mutex::new(books)),
                cfg,
            )
            .await
        });

        let (mut ws, _) = tokio_tungstenite::connect_async(format!("ws://{addr}"))
            .await
            .unwrap();
        assert!(
            parse_book(
                &next_frame(&mut ws, Duration::from_secs(2))
                    .await
                    .expect("connect replay")
            )
            .snapshot,
            "the bootstrap is the re-baseline"
        );
        for expected in [N + 1, N - 2] {
            assert_eq!(
                parse_book(
                    &next_frame(&mut ws, Duration::from_secs(2))
                        .await
                        .expect("a frame past the bootstrap")
                )
                .recv_ts_ns,
                expected
            );
        }
        assert_eq!(
            dropped(),
            before + 1,
            "exactly the one frame the bootstrap provably contained"
        );

        srv.abort();
    }

    /// The bound on the same rule, for the case no frame ever passes: a bootstrap materialized from
    /// state stamped ahead of everything still to be broadcast — the arbiter advances the shared
    /// accumulator before it sends, so a batch it folded can be queued behind the bootstrap that
    /// contains it — must not silence the market for the life of the connection.
    #[tokio::test]
    #[serial]
    async fn a_watermark_ahead_of_the_live_feed_expires() {
        use super::{prepare, serve_client};

        let market = (
            crate::model::SourceKey::unassigned("KALSHI"),
            Arc::<str>::from("perps"),
            2u8,
            41u32,
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (prepared_tx, prepared_rx) = broadcast::channel(8);

        // Folded at 10_000; every live frame stamped far below it.
        let mut acc = BookAccumulator::new("KXBTCPERP".into());
        acc.apply(&NormalizedBook {
            recv_ts_ns: 10_000,
            ..book_batch(
                "KXBTCPERP",
                vec![
                    BookChange {
                        action: BookAction::Clear,
                        side: BookSide::Both,
                        price: 0.0,
                        size: 0.0,
                        order_id: 0,
                    },
                    level_update(BookSide::Bid, 0.56, 10.0),
                ],
                true,
            )
        });
        let mut books = BookReplay::default();
        books.insert(market, acc);

        let cfg = WsConfig {
            heartbeat: Duration::from_secs(30),
            idle_timeout: Duration::from_secs(60),
            max_clients: 8,
            max_subs: 8,
            max_inbound_per_min: 600,
            broadcast_capacity: 8,
            lag_repair_min_interval: super::LAG_REPAIR_MIN_INTERVAL,
            bootstrap_release_deadline: Duration::from_millis(200),
        };
        let srv = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            serve_client(
                stream,
                prepared_rx,
                Arc::new(Mutex::new(HashMap::new())),
                Arc::new(Mutex::new(HashMap::new())),
                Arc::new(Mutex::new(books)),
                cfg,
            )
            .await
        });

        let (mut ws, _) = tokio_tungstenite::connect_async(format!("ws://{addr}"))
            .await
            .unwrap();
        assert!(
            parse_book(
                &next_frame(&mut ws, Duration::from_secs(2))
                    .await
                    .expect("connect replay")
            )
            .snapshot
        );
        let live = |recv_ts_ns: u64| {
            prepare(&FeedMessage::Book(NormalizedBook {
                recv_ts_ns,
                category: "perps".into(),
                channel: 2,
                instrument_id: 41,
                changes: vec![level_update(BookSide::Bid, 0.55, 10.0)],
                ..book_batch("KXBTCPERP", Vec::new(), true)
            }))
            .expect("serializes")
        };
        assert!(prepared_tx.send(live(1)).is_ok(), "receiver alive");
        assert_eq!(
            next_frame(&mut ws, Duration::from_millis(100)).await,
            None,
            "inside the bound the watermark still holds"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
        assert!(prepared_tx.send(live(2)).is_ok(), "receiver alive");
        assert_eq!(
            parse_book(
                &next_frame(&mut ws, Duration::from_secs(2))
                    .await
                    .expect("the market is not silenced for the connection")
            )
            .recv_ts_ns,
            2
        );

        srv.abort();
    }

    /// A second lag inside the pace window is coalesced rather than answered immediately: the client
    /// keeps receiving live frames instead of another O(markets) replay. Without this a client that
    /// lags faster than it can be repaired spends the whole connection being repaired — the traffic
    /// #149 measured.
    #[tokio::test]
    #[serial]
    async fn a_second_lag_inside_the_pace_window_replays_nothing() {
        use super::{prepare, serve_client};

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (prepared_tx, prepared_rx) = broadcast::channel(1);
        let quote = || prepare(&FeedMessage::Quote(sample_quote())).expect("serializes");
        for _ in 0..2 {
            assert!(prepared_tx.send(quote()).is_ok(), "the receiver is alive");
        }

        let mut books = BookReplay::default();
        books.insert(
            (
                crate::model::SourceKey::unassigned("KALSHI"),
                Arc::<str>::from("perps"),
                2u8,
                41u32,
            ),
            accumulator("KXBTCPERP", 0.61, 0.63),
        );
        let cfg = WsConfig {
            heartbeat: Duration::from_secs(30),
            idle_timeout: Duration::from_secs(60),
            max_clients: 8,
            max_subs: 8,
            max_inbound_per_min: 600,
            broadcast_capacity: 1,
            lag_repair_min_interval: super::LAG_REPAIR_MIN_INTERVAL,
            bootstrap_release_deadline: super::BOOTSTRAP_RELEASE_DEADLINE,
        };
        let srv = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            serve_client(
                stream,
                prepared_rx,
                Arc::new(Mutex::new(HashMap::new())),
                Arc::new(Mutex::new(HashMap::new())),
                Arc::new(Mutex::new(books)),
                cfg,
            )
            .await
        });

        let (mut ws, _) = tokio_tungstenite::connect_async(format!("ws://{addr}"))
            .await
            .unwrap();
        for expected in ["connect replay", "re-baseline after the first lag"] {
            let frame = next_frame(&mut ws, Duration::from_secs(2))
                .await
                .expect(expected);
            assert_eq!(parse_book(&frame).instrument_id, 41, "{expected}");
        }
        let first = next_frame(&mut ws, Duration::from_secs(2))
            .await
            .expect("surviving quote");
        assert!(first.contains(r#""type":"quote""#), "got {first}");

        // The client is parked on `recv` and caught up; overflow it again, still well inside
        // `LAG_REPAIR_MIN_INTERVAL`.
        for _ in 0..2 {
            assert!(prepared_tx.send(quote()).is_ok(), "the receiver is alive");
        }
        let after = next_frame(&mut ws, Duration::from_secs(2))
            .await
            .expect("the frame that survived the second overflow");
        assert!(
            after.contains(r#""type":"quote""#),
            "a second lag inside the window must not replay again, got {after}"
        );

        srv.abort();
    }

    /// A client that cannot receive a book is owed no repair: only `book`/`order_book` are repaired,
    /// so a lag on a `{"type":"quote"}` subscriber must not walk the market map at all. The frames
    /// are identical either way (the filter would have excluded every market), so the counter is the
    /// observable — the cost being avoided is the scan, under the mutex the ingest emit path shares.
    /// `#[serial]` because that counter is a process-global Prometheus child, as it is for every
    /// other lag test here.
    #[tokio::test]
    #[serial]
    async fn a_lag_on_a_client_that_cannot_receive_books_repairs_nothing() {
        use futures_util::SinkExt;

        use super::{prepare, serve_client};

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (prepared_tx, prepared_rx) = broadcast::channel(1);

        let mut books = BookReplay::default();
        books.insert(
            (
                crate::model::SourceKey::unassigned("KALSHI"),
                Arc::<str>::from("perps"),
                2u8,
                41u32,
            ),
            accumulator("KXBTCPERP", 0.61, 0.63),
        );
        let cfg = WsConfig {
            heartbeat: Duration::from_secs(30),
            idle_timeout: Duration::from_secs(60),
            max_clients: 8,
            max_subs: 8,
            max_inbound_per_min: 600,
            broadcast_capacity: 1,
            lag_repair_min_interval: super::LAG_REPAIR_MIN_INTERVAL,
            bootstrap_release_deadline: super::BOOTSTRAP_RELEASE_DEADLINE,
        };
        let srv = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            serve_client(
                stream,
                prepared_rx,
                Arc::new(Mutex::new(HashMap::new())),
                Arc::new(Mutex::new(HashMap::new())),
                Arc::new(Mutex::new(books)),
                cfg,
            )
            .await
        });

        let (mut ws, _) = tokio_tungstenite::connect_async(format!("ws://{addr}"))
            .await
            .unwrap();
        // The connect bootstrap is unfiltered (no subscriptions yet), so the book still arrives here.
        let connect = next_frame(&mut ws, Duration::from_secs(2))
            .await
            .expect("connect replay");
        assert_eq!(parse_book(&connect).instrument_id, 41);

        ws.send(WsMessage::Text(
            r#"{"method":"subscribe","subscription":{"type":"quote"}}"#.into(),
        ))
        .await
        .unwrap();
        let ack = next_frame(&mut ws, Duration::from_secs(2))
            .await
            .expect("subscription ack");
        assert!(ack.contains("subscription_response"), "got {ack}");

        // The client is parked on `recv` and holds a book-excluding filter; overflow it.
        let before = metrics().ws_lag_repairs.get();
        for _ in 0..2 {
            let frame = prepare(&FeedMessage::Quote(sample_quote())).expect("serializes");
            assert!(prepared_tx.send(frame).is_ok(), "the receiver is alive");
        }
        let quote = next_frame(&mut ws, Duration::from_secs(2))
            .await
            .expect("the frame that survived the overflow");
        assert!(quote.contains(r#""type":"quote""#), "got {quote}");
        assert_eq!(
            metrics().ws_lag_repairs.get(),
            before,
            "a lag on a client that cannot receive books must not scan the market map"
        );

        srv.abort();
    }

    /// The counter's own semantics, pinned: `dz_ws_lag_repairs_total` counts the repair **pass**,
    /// not the re-baselines it wrote. This client is subscribed to books — so the repair is owed and
    /// the pass runs — but under a `venue` that matches no market, so the pass writes nothing. It
    /// still counts, because what the series is read against is the pace (`dz_ws_client_lagged_total`)
    /// and counting frames would make a gap mean either "coalesced" or "nothing to repair"; on a
    /// deployment carrying no book-bearing feed it would read zero through every lag.
    /// `#[serial]` because that counter is a process-global Prometheus child.
    #[tokio::test]
    #[serial]
    async fn a_repair_pass_that_writes_no_rebaseline_is_still_counted() {
        use futures_util::SinkExt;

        use super::{prepare, serve_client};

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (prepared_tx, prepared_rx) = broadcast::channel(1);

        let mut books = BookReplay::default();
        books.insert(
            (
                crate::model::SourceKey::unassigned("KALSHI"),
                Arc::<str>::from("perps"),
                2u8,
                41u32,
            ),
            accumulator("KXBTCPERP", 0.61, 0.63),
        );
        let cfg = WsConfig {
            heartbeat: Duration::from_secs(30),
            idle_timeout: Duration::from_secs(60),
            max_clients: 8,
            max_subs: 8,
            max_inbound_per_min: 600,
            broadcast_capacity: 1,
            lag_repair_min_interval: super::LAG_REPAIR_MIN_INTERVAL,
            bootstrap_release_deadline: super::BOOTSTRAP_RELEASE_DEADLINE,
        };
        let srv = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            serve_client(
                stream,
                prepared_rx,
                Arc::new(Mutex::new(HashMap::new())),
                Arc::new(Mutex::new(HashMap::new())),
                Arc::new(Mutex::new(books)),
                cfg,
            )
            .await
        });

        let (mut ws, _) = tokio_tungstenite::connect_async(format!("ws://{addr}"))
            .await
            .unwrap();
        let connect = next_frame(&mut ws, Duration::from_secs(2))
            .await
            .expect("connect replay");
        assert_eq!(parse_book(&connect).instrument_id, 41);

        // Books from a venue this process serves nothing for, plus quotes — the quote filter is what
        // lets the surviving frame through, so the pass can be observed without a sleep.
        for sub in [
            r#"{"method":"subscribe","subscription":{"venue":"NOBODY","type":"book"}}"#,
            r#"{"method":"subscribe","subscription":{"type":"quote"}}"#,
        ] {
            ws.send(WsMessage::Text(sub.into())).await.unwrap();
            let ack = next_frame(&mut ws, Duration::from_secs(2))
                .await
                .expect("subscription ack");
            assert!(ack.contains("subscription_response"), "got {ack}");
        }

        let before = metrics().ws_lag_repairs.get();
        for _ in 0..2 {
            let frame = prepare(&FeedMessage::Quote(sample_quote())).expect("serializes");
            assert!(prepared_tx.send(frame).is_ok(), "the receiver is alive");
        }
        let survivor = next_frame(&mut ws, Duration::from_secs(2))
            .await
            .expect("the frame that survived the overflow");
        assert!(
            survivor.contains(r#""type":"quote""#),
            "the repair wrote nothing, so the next frame is the live quote, got {survivor}"
        );
        assert_eq!(
            metrics().ws_lag_repairs.get(),
            before + 1,
            "the pass ran and is counted even though it re-baselined nothing"
        );

        srv.abort();
    }

    /// The reach test itself. A `type` filter is the only one that can rule the product out: a
    /// `venue`/`symbol`/`channel` filter narrows which markets are repaired, possibly to none, but
    /// the repair is still the right answer for it — the client is receiving books.
    #[test]
    fn only_a_type_filter_puts_a_client_out_of_reach_of_a_lag_repair() {
        use super::lag_repair_reaches;

        let f = |json: &str| serde_json::from_str::<SubFilter>(json).expect("parses");
        assert!(lag_repair_reaches(&[]), "the firehose receives books");
        for json in [
            r#"{"type":"book"}"#,
            r#"{"type":"order_book"}"#,
            r#"{"venue":"KALSHI"}"#,
            r#"{"symbol":"KXBTCPERP"}"#,
            r#"{"channel":2}"#,
            r#"{"venue":"NOBODY","type":"book"}"#,
        ] {
            assert!(lag_repair_reaches(&[f(json)]), "{json} receives books");
        }
        for json in [r#"{"type":"quote"}"#, r#"{"type":"instrument"}"#] {
            assert!(!lag_repair_reaches(&[f(json)]), "{json} receives no book");
        }
        assert!(
            lag_repair_reaches(&[f(r#"{"type":"quote"}"#), f(r#"{"type":"book"}"#)]),
            "filters are a union: one book subscription is enough"
        );
    }

    /// The end-to-end half of the pace: a lag coalesced inside the window is **discharged**, not
    /// dropped. `the_lag_repair_pace_holds_a_repair_without_losing_it` below pins the predicate,
    /// which is the easy half — clearing `lag_repair_due` in the wrong place, or never reaching the
    /// top of the loop again, leaves that unit test green while the client keeps a permanently
    /// wrong book, which is the one thing the pace is not allowed to cost. So this drives a real
    /// connection: lag once (repaired immediately), lag again inside the window (held), then assert
    /// the held repair goes out once the window has passed.
    ///
    /// `lag_repair_min_interval` is collapsed to 500ms so the wait is a test's rather than a
    /// consumer's — the field exists for this. `#[serial]` because `dz_ws_lag_repairs_total` is a
    /// process-global Prometheus child.
    #[tokio::test]
    #[serial]
    async fn a_coalesced_lag_repair_is_discharged_once_the_window_passes() {
        use super::{prepare, serve_client};

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (prepared_tx, prepared_rx) = broadcast::channel(1);

        let mut books = BookReplay::default();
        books.insert(
            (
                crate::model::SourceKey::unassigned("KALSHI"),
                Arc::<str>::from("perps"),
                2u8,
                41u32,
            ),
            accumulator("KXBTCPERP", 0.61, 0.63),
        );
        let window = Duration::from_millis(500);
        let cfg = WsConfig {
            heartbeat: Duration::from_secs(30),
            idle_timeout: Duration::from_secs(60),
            max_clients: 8,
            max_subs: 8,
            max_inbound_per_min: 600,
            broadcast_capacity: 1,
            lag_repair_min_interval: window,
            bootstrap_release_deadline: super::BOOTSTRAP_RELEASE_DEADLINE,
        };
        let srv = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            serve_client(
                stream,
                prepared_rx,
                Arc::new(Mutex::new(HashMap::new())),
                Arc::new(Mutex::new(HashMap::new())),
                Arc::new(Mutex::new(books)),
                cfg,
            )
            .await
        });

        let (mut ws, _) = tokio_tungstenite::connect_async(format!("ws://{addr}"))
            .await
            .unwrap();
        let connect = next_frame(&mut ws, Duration::from_secs(2))
            .await
            .expect("connect replay");
        assert_eq!(parse_book(&connect).instrument_id, 41);

        // First lag: repaired immediately — a client's only correction must not wait on the pace.
        let before = metrics().ws_lag_repairs.get();
        for _ in 0..2 {
            let frame = prepare(&FeedMessage::Quote(sample_quote())).expect("serializes");
            assert!(prepared_tx.send(frame).is_ok(), "the receiver is alive");
        }
        let repair = next_frame(&mut ws, Duration::from_secs(2))
            .await
            .expect("the first repair");
        assert_eq!(parse_book(&repair).instrument_id, 41, "got {repair}");
        let survivor = next_frame(&mut ws, Duration::from_secs(2))
            .await
            .expect("the frame that survived the first overflow");
        assert!(survivor.contains(r#""type":"quote""#), "got {survivor}");
        assert_eq!(metrics().ws_lag_repairs.get(), before + 1);

        // Second lag, inside the window: held. Reading the frame that survived it proves the
        // `Lagged` arm has already run, so an unchanged counter here is suppression, not a race.
        for _ in 0..2 {
            let frame = prepare(&FeedMessage::Quote(sample_quote())).expect("serializes");
            assert!(prepared_tx.send(frame).is_ok(), "the receiver is alive");
        }
        let survivor = next_frame(&mut ws, Duration::from_secs(2))
            .await
            .expect("the frame that survived the second overflow");
        assert!(survivor.contains(r#""type":"quote""#), "got {survivor}");
        assert_eq!(
            metrics().ws_lag_repairs.get(),
            before + 1,
            "a lag inside the window is coalesced, not repaired"
        );

        // Once the window passes the held repair goes out on this client's next frame. The loop
        // discharges at its top, so the quote that wakes it is written first and the re-baseline
        // follows — the ordering is safe either way, the re-baseline is materialized when it is sent.
        tokio::time::sleep(window + Duration::from_millis(300)).await;
        let frame = prepare(&FeedMessage::Quote(sample_quote())).expect("serializes");
        assert!(prepared_tx.send(frame).is_ok(), "the receiver is alive");
        let waker = next_frame(&mut ws, Duration::from_secs(2))
            .await
            .expect("the quote that wakes the loop");
        assert!(waker.contains(r#""type":"quote""#), "got {waker}");
        let held = next_frame(&mut ws, Duration::from_secs(2))
            .await
            .expect("the held repair");
        assert_eq!(
            parse_book(&held).instrument_id,
            41,
            "a coalesced lag must still be repaired, got {held}"
        );
        assert_eq!(
            metrics().ws_lag_repairs.get(),
            before + 2,
            "exactly one further pass: the held one"
        );

        srv.abort();
    }

    /// The pace itself: the first repair goes out immediately (a client's only correction must not
    /// wait), a second inside the window is held, and it is released — not dropped — once the window
    /// has passed.
    #[test]
    fn the_lag_repair_pace_holds_a_repair_without_losing_it() {
        use super::{lag_repair_ready, LAG_REPAIR_MIN_INTERVAL as WINDOW};
        use std::time::Instant;

        assert!(!lag_repair_ready(false, None, WINDOW), "nothing owed");
        assert!(
            lag_repair_ready(true, None, WINDOW),
            "the first repair is immediate"
        );
        let just_now = Instant::now();
        assert!(
            !lag_repair_ready(true, Some(just_now), WINDOW),
            "a repair inside the window is held"
        );
        let stale = just_now
            .checked_sub(WINDOW + Duration::from_secs(1))
            .expect("a representable instant");
        assert!(
            lag_repair_ready(true, Some(stale), WINDOW),
            "the held repair is released once the window has passed"
        );
    }

    #[test]
    fn a_source_name_filter_selects_the_same_messages_as_a_venue_filter() {
        let by_venue: SubFilter = serde_json::from_str(r#"{"venue":"HYPERLIQUID"}"#).unwrap();
        let by_source: SubFilter =
            serde_json::from_str(r#"{"source_name":"HYPERLIQUID"}"#).unwrap();
        for kind in ["quote", "trade", "status"] {
            assert_eq!(
                by_venue.matches("HYPERLIQUID", Some("SOL"), None, kind),
                by_source.matches("HYPERLIQUID", Some("SOL"), None, kind),
            );
            assert!(!by_source.matches("PHOENIX", Some("SOL"), None, kind));
        }
    }

    /// The alias keeps the case-insensitivity the venue key already had.
    #[test]
    fn a_source_name_filter_is_case_insensitive() {
        let f: SubFilter = serde_json::from_str(r#"{"source_name":"HYPERLIQUID"}"#).unwrap();
        assert!(f.matches("HYPERLIQUID", Some("SOL"), None, "quote"));
    }

    /// Both keys present and disagreeing must match nothing — silently honouring one would make a
    /// client's filter mean something it did not ask for.
    #[test]
    fn disagreeing_source_name_and_venue_keys_match_nothing() {
        let f: SubFilter =
            serde_json::from_str(r#"{"venue":"HYPERLIQUID","source_name":"PHOENIX"}"#).unwrap();
        assert!(!f.matches("HYPERLIQUID", Some("SOL"), None, "quote"));
        assert!(!f.matches("PHOENIX", Some("SOL"), None, "quote"));
    }

    /// The pre-rename `source` key still narrows, and — the reason it is its own field rather than a
    /// `serde(alias)` — a client sending **both** spellings is accepted rather than having the whole
    /// `subscribe` refused as a duplicate field, which would have registered no filter at all and
    /// left it on the firehose, then cost it the messages it did ask for to drop-oldest backpressure.
    #[test]
    fn the_retired_source_key_narrows_and_composes_with_the_new_one() {
        let retired: SubFilter = serde_json::from_str(r#"{"source":"HYPERLIQUID"}"#).unwrap();
        assert!(retired.matches("HYPERLIQUID", Some("SOL"), None, "quote"));
        assert!(!retired.matches("PHOENIX", Some("SOL"), None, "quote"));

        let both: SubFilter =
            serde_json::from_str(r#"{"source":"HYPERLIQUID","source_name":"HYPERLIQUID"}"#)
                .expect("both spellings must parse, not collide");
        assert!(both.matches("HYPERLIQUID", Some("SOL"), None, "quote"));

        // ANDed like `venue`, so a disagreeing pair matches nothing rather than honouring one.
        let disagreeing: SubFilter =
            serde_json::from_str(r#"{"source":"HYPERLIQUID","source_name":"PHOENIX"}"#).unwrap();
        assert!(!disagreeing.matches("HYPERLIQUID", Some("SOL"), None, "quote"));
        assert!(!disagreeing.matches("PHOENIX", Some("SOL"), None, "quote"));
    }
}
