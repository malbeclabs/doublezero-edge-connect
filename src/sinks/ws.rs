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
    model::{now_ns, DepthSnapshot, FeedMessage, InstrumentSnapshot},
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
    channel: Option<u32>,
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
            "book"
        }
        FeedMessage::Instrument(_) => "instrument",
        FeedMessage::Status(_) => "status",
    };
    let payload: Utf8Bytes = serde_json::to_string(&m).ok()?.into();
    let (venue, symbol, channel) = match &m {
        FeedMessage::Instrument(i) => (i.venue.clone(), Some(i.symbol.clone()), None),
        FeedMessage::Quote(q) => (q.venue.clone(), Some(q.symbol.clone()), None),
        FeedMessage::Trade(t) => (t.venue.clone(), Some(t.symbol.clone()), None),
        FeedMessage::Midpoint(mp) => (mp.venue.clone(), Some(mp.symbol.clone()), None),
        FeedMessage::Depth(d) => (d.venue.clone(), Some(d.symbol.clone()), None),
        FeedMessage::Book(b) => (b.venue.clone(), Some(b.symbol.clone()), Some(b.channel)),
        FeedMessage::Status(s) => (s.venue.clone(), None, None),
    };
    Some(Arc::new(PreparedFrame {
        payload,
        kind,
        venue,
        symbol,
        channel,
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
}

/// A subscription filter: a `None` field matches any value (so `{}` = everything).
#[derive(Deserialize, Serialize, Clone, PartialEq, Debug)]
struct SubFilter {
    #[serde(default)]
    venue: Option<String>,
    #[serde(default)]
    symbol: Option<String>,
    /// The wire `channel_id` — the competition, not the arm. Arm identity is deliberately not
    /// client-selectable: exactly one arbitrated book per market reaches the wire.
    #[serde(default)]
    channel: Option<u32>,
    /// Message `type` (`quote`/`trade`/`book`/...). Named `msg_type` in Rust because `type` is a
    /// keyword; the wire name is `type`.
    #[serde(rename = "type", default)]
    msg_type: Option<String>,
}

impl SubFilter {
    /// The single match path. `symbol`/`channel` are `None` for a venue-level message (today only
    /// `status`), and a `None` on the *message* side satisfies a filter on that dimension — a
    /// venue-level message is about the whole venue, so a symbol- or channel-scoped subscriber must
    /// still receive it. A filter dimension the message *does* carry is matched normally.
    fn matches(&self, venue: &str, symbol: Option<&str>, channel: Option<u32>, kind: &str) -> bool {
        // Venue codes are registry identifiers, not free text - match case-insensitively so a
        // subscription for `PHOENIX` / `phoenix` still selects the wire venue `Phoenix`. Symbol and
        // type stay exact (venues name symbols precisely; types are a closed protocol set).
        self.venue
            .as_deref()
            .is_none_or(|v| v.eq_ignore_ascii_case(venue))
            && self.msg_type.as_deref().is_none_or(|t| t == kind)
            && match symbol {
                None => true,
                Some(s) => self.symbol.as_deref().is_none_or(|f| f == s),
            }
            && match channel {
                // `instrument` is infrastructure, not market data: precision must arrive before
                // price, so it passes an explicit channel filter the way a venue-level message
                // does. The carve-out is deliberately just this one kind — widening it to every
                // channelless message would make `{"channel":2}` a firehose of quotes.
                None => self.channel.is_none() || symbol.is_none() || kind == "instrument",
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
        let cfg = cfg.clone();
        // The guard releases the slot + gauge on drop, so the accounting is correct even if
        // `serve_client` panics rather than returning.
        let guard = ClientGuard {
            clients: clients.clone(),
        };
        tokio::spawn(async move {
            let _guard = guard;
            if let Err(e) = serve_client(stream, rx, instruments, depth, cfg).await {
                warn!(%peer, "client ended: {e}");
            }
        });
    }
}

fn text(value: serde_json::Value) -> WsMessage {
    WsMessage::Text(value.to_string().into())
}

/// Replay current full state matching `subs` (empty = everything): instrument definitions first so
/// precision is known before any book, then the latest `depth` per `(venue, symbol)`.
///
/// Called on connect, and again on each `subscribe` so a client that narrows after connecting is
/// bootstrapped for its new scope rather than waiting for the next event. Replay is idempotent full
/// state, so the overlap a connect-then-subscribe client sees is harmless.
async fn replay_scoped<W>(
    write: &mut W,
    instruments: &InstrumentSnapshot,
    depth: &DepthSnapshot,
    subs: &[SubFilter],
) -> Result<()>
where
    W: SinkExt<WsMessage> + Unpin,
    <W as futures_util::Sink<WsMessage>>::Error: std::error::Error + Send + Sync + 'static,
{
    let pass = |venue: &str, symbol: &str, kind: &str| {
        subs.is_empty()
            || subs
                .iter()
                .any(|f| f.matches(venue, Some(symbol), None, kind))
    };
    // Both locks are taken and released before any `await`: a `std::sync::MutexGuard` held across an
    // await point does not compile here, and would be a latency bug regardless.
    let snapshot: Vec<FeedMessage> = {
        let guard = crate::model::lock(instruments);
        guard
            .values()
            .filter(|i| pass(&i.venue, &i.symbol, "instrument"))
            .cloned()
            .map(FeedMessage::Instrument)
            .collect()
    };
    let books: Vec<FeedMessage> = {
        let guard = crate::model::lock(depth);
        guard
            .values()
            .filter(|d| pass(&d.venue, &d.symbol, "depth"))
            .cloned()
            .map(FeedMessage::Depth)
            .collect()
    };
    for m in snapshot.into_iter().chain(books) {
        write
            .send(WsMessage::Text(serde_json::to_string(&m)?.into()))
            .await?;
    }
    Ok(())
}

async fn serve_client(
    stream: TcpStream,
    mut rx: broadcast::Receiver<Arc<PreparedFrame>>,
    instruments: InstrumentSnapshot,
    depth: DepthSnapshot,
    cfg: WsConfig,
) -> Result<()> {
    let ws = tokio_tungstenite::accept_async(stream).await?;
    let (mut write, mut read) = ws.split();

    // Per-client state. Empty `subs` = firehose (receive every venue/symbol).
    let mut subs: Vec<SubFilter> = Vec::new();

    // Replay definitions (precision first) then the latest full-state `depth` per (venue, symbol),
    // so a mid-stream consumer is bootstrapped immediately instead of waiting for the next periodic
    // book. (Quotes/trades are not replayed - the next quote is itself full state.) `subs` is empty
    // here, so this connect-time replay is unfiltered.
    replay_scoped(&mut write, &instruments, &depth, &subs).await?;

    let mut last_seen = Instant::now();
    let mut win_start = Instant::now();
    let mut win_count: u32 = 0;
    let mut hb = tokio::time::interval(cfg.heartbeat);

    loop {
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
                                if !subs.contains(&subscription) {
                                    subs.push(subscription.clone());
                                }
                                write.send(text(json!({
                                    "channel": "subscription_response", "method": "subscribe",
                                    "subscription": subscription,
                                }))).await?;
                                // Bootstrap the newly-added scope only, not all of `subs` — else a
                                // client subscribing to ten symbols replays the first one ten times.
                                replay_scoped(&mut write, &instruments, &depth, std::slice::from_ref(&subscription)).await?;
                            }
                        }
                        Ok(ClientMsg::Unsubscribe { subscription }) => {
                            metrics().ws_inbound.with_label_values(&["unsubscribe"]).inc();
                            subs.retain(|s| s != &subscription);
                            write.send(text(json!({
                                "channel": "subscription_response", "method": "unsubscribe",
                                "subscription": subscription,
                            }))).await?;
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
                    // One match path for every kind, venue-level included: a dimension added to
                    // `matches` cannot silently exempt half the stream.
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
                Err(broadcast::error::RecvError::Lagged(n)) => { metrics().ws_client_lagged.inc(); warn!("subscriber lagged, dropped {n}"); }
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
        model::{FeedMessage, NormalizedInstrument, NormalizedQuote},
    };

    fn filter(json: &str) -> SubFilter {
        serde_json::from_str(json).expect("filter parses")
    }

    #[test]
    fn venue_matches_case_insensitively() {
        // The wire venue is `Phoenix`; a filter spelled any case must still select it (the
        // PROTOCOL.md example historically showed `PHOENIX`, which would silently drop the feed
        // under an exact match).
        assert!(filter(r#"{"venue":"PHOENIX"}"#).matches("Phoenix", Some("BTC"), None, "quote"));
        assert!(filter(r#"{"venue":"phoenix"}"#).matches("Phoenix", Some("BTC"), None, "quote"));
        assert!(filter(r#"{"venue":"Phoenix"}"#).matches("Phoenix", Some("BTC"), None, "quote"));
        assert!(!filter(r#"{"venue":"Hyperliquid"}"#).matches(
            "Phoenix",
            Some("BTC"),
            None,
            "quote"
        ));
    }

    #[test]
    fn omitted_field_matches_any_symbol_exact() {
        assert!(filter("{}").matches("Phoenix", Some("BTC"), None, "quote")); // {} = everything
        assert!(filter(r#"{"symbol":"BTC"}"#).matches("Phoenix", Some("BTC"), None, "quote"));
        // symbol stays exact
        assert!(!filter(r#"{"symbol":"btc"}"#).matches("Phoenix", Some("BTC"), None, "quote"));
    }

    /// The omitted-field-matches-anything rule must survive the two new dimensions.
    #[test]
    fn empty_filter_still_matches_everything() {
        let f = filter("{}");
        assert!(f.matches("Lashay", Some("KXBTCPERP"), Some(2), "book"));
        assert!(f.matches("Hyperliquid", Some("SOL"), None, "quote"));
        assert!(f.matches("Lashay", None, None, "status"));
    }

    #[test]
    fn type_filter_selects_one_message_kind() {
        let f = filter(r#"{"type":"book"}"#);
        assert!(f.matches("Lashay", Some("KXBTCPERP"), Some(2), "book"));
        assert!(!f.matches("Lashay", Some("KXBTCPERP"), Some(2), "quote"));
    }

    /// `type` is matched exactly, like `symbol`: the wire values are a closed set the protocol
    /// defines, so a near-miss is a client bug worth surfacing as "no data" rather than guessing.
    #[test]
    fn type_filter_is_exact() {
        assert!(!filter(r#"{"type":"BOOK"}"#).matches("Lashay", Some("X"), None, "book"));
    }

    #[test]
    fn channel_filter_selects_one_channel() {
        let f = filter(r#"{"channel":2}"#);
        assert!(f.matches("Lashay", Some("KXBTCPERP"), Some(2), "book"));
        assert!(!f.matches("Lashay", Some("KXBTCPERP"), Some(1), "book"));
    }

    /// An explicit channel filter must not pass a message that carries no channel — otherwise
    /// `{"channel":2}` would receive every quote on every venue.
    #[test]
    fn channel_filter_excludes_channelless_messages() {
        assert!(!filter(r#"{"channel":2}"#).matches("Hyperliquid", Some("SOL"), None, "quote"));
    }

    /// ...except `instrument`, which carries no channel and never will until the processor supplies
    /// one. Excluding it would leave a channel-scoped client unable to scale the books it just
    /// subscribed to, breaking the protocol's precision-before-price promise.
    #[test]
    fn channel_filter_still_delivers_instrument_definitions() {
        let f = filter(r#"{"channel":2}"#);
        assert!(f.matches("Lashay", Some("KXBTCPERP"), None, "instrument"));
        assert!(!f.matches("Lashay", Some("KXBTCPERP"), None, "quote"));
        // The carve-out is on the channel dimension only — `symbol` still narrows instruments.
        assert!(!filter(r#"{"channel":2,"symbol":"SOL"}"#).matches(
            "Lashay",
            Some("KXBTCPERP"),
            None,
            "instrument"
        ));
    }

    /// `status` is venue-level: no symbol and no channel, so it matches on venue and type alone —
    /// the same carve-out `symbol` already has, extended to `channel`. Without this a
    /// `{"venue":"Lashay","channel":2}` subscriber would never learn its venue went down.
    #[test]
    fn status_matches_on_venue_despite_symbol_and_channel_filters() {
        let f = filter(r#"{"venue":"Lashay","symbol":"KXBTCPERP","channel":2}"#);
        assert!(f.matches("Lashay", None, None, "status"));
        assert!(!f.matches("Hyperliquid", None, None, "status"));
    }

    /// ...but an explicit `type` filter still excludes it, so a consumer that asked for `book` only
    /// does not get status frames it never requested.
    #[test]
    fn type_filter_still_excludes_status() {
        assert!(!filter(r#"{"type":"book"}"#).matches("Lashay", None, None, "status"));
    }

    /// A `book` frame must carry its channel so an explicit channel filter can select it.
    #[test]
    fn prepare_populates_the_channel_for_book_only() {
        use super::prepare;
        use crate::model::{BookAction, BookChange, BookSide, NormalizedBook};
        let b = FeedMessage::Book(NormalizedBook {
            venue: "Lashay".into(),
            symbol: "KXBTCPERP".into(),
            channel: 2,
            instrument_id: 41,
            changes: vec![BookChange {
                action: BookAction::Update,
                side: BookSide::Bid,
                price: 0.62,
                size: 150.0,
            }],
            snapshot: false,
            last: true,
            source_ts_ns: 1,
            recv_ts_ns: 2,
            kernel_rx_ts_ns: 3,
            ws_send_ts_ns: 0,
        });
        let f = prepare(&b).expect("serializes");
        assert_eq!(f.kind, "book");
        assert_eq!(f.channel, Some(2));
        assert!(f.payload.contains(r#""ws_send_ts_ns":"#));
        assert!(
            !f.payload.contains(r#""ws_send_ts_ns":0"#),
            "stamped, not left at 0"
        );
        // Every other kind carries no channel, which is what the filter's exclusion rule rests on.
        assert_eq!(
            prepare(&FeedMessage::Quote(sample_quote()))
                .expect("serializes")
                .channel,
            None
        );
    }

    #[test]
    fn venue_stays_case_insensitive() {
        assert!(filter(r#"{"venue":"lashay"}"#).matches("Lashay", Some("X"), None, "book"));
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
            venue: "Hyperliquid".into(),
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
        };
        let srv = tokio::spawn(serve(listener, tx.clone(), instruments, depth, cfg));

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
        };
        let srv = tokio::spawn(serve(listener, tx.clone(), instruments, depth, cfg));

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
        for sym in ["SOL", "BTC"] {
            let arc: Arc<str> = sym.into();
            defs.insert(
                (Arc::<str>::from("Hyperliquid"), arc.clone()),
                NormalizedInstrument {
                    venue: "Hyperliquid".into(),
                    symbol: arc,
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
        };
        let srv = tokio::spawn(serve(listener, tx.clone(), instruments, depth, cfg));

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

        srv.abort();
    }
}
