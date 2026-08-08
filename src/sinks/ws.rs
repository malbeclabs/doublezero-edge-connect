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
    model::{now_ns, BookSnapshot, DepthSnapshot, FeedMessage, InstrumentSnapshot},
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
    let (venue, symbol) = match &m {
        FeedMessage::Instrument(i) => (i.venue.clone(), Some(i.symbol.clone())),
        FeedMessage::Quote(q) => (q.venue.clone(), Some(q.symbol.clone())),
        FeedMessage::Trade(t) => (t.venue.clone(), Some(t.symbol.clone())),
        FeedMessage::Midpoint(mp) => (mp.venue.clone(), Some(mp.symbol.clone())),
        FeedMessage::Depth(d) => (d.venue.clone(), Some(d.symbol.clone())),
        FeedMessage::Book(b) => (b.venue.clone(), Some(b.symbol.clone())),
        FeedMessage::Status(s) => (s.venue.clone(), None),
    };
    Some(Arc::new(PreparedFrame {
        payload,
        kind,
        venue,
        symbol,
        channel: m.channel(),
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
            // `type` is a *kind* selector and so is absolute, with no carve-out: a client that named
            // one type asked for that type. Filters are a union, so wanting books plus definitions is
            // two subscriptions. `venue`/`symbol`/`channel` below are *scope* selectors — which
            // markets — and those do carve out messages that aren't about one market.
            && self.msg_type.as_deref().is_none_or(|t| t == kind)
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

/// Replay current full state matching `subs` (empty = everything): instrument definitions first so
/// precision is known before any book, then the latest `depth` per `(venue, symbol)` and a `book`
/// re-baseline per `(venue, channel, instrument_id)`.
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
) -> Result<()>
where
    W: SinkExt<WsMessage> + Unpin,
    <W as futures_util::Sink<WsMessage>>::Error: std::error::Error + Send + Sync + 'static,
{
    // Each kind passes its own channel: a channel-bearing kind that passed `None` would leave a
    // `{"channel":N}` client with no bootstrap at all.
    let pass = |venue: &str, symbol: &str, channel: Option<u32>, kind: &str| {
        subs.is_empty()
            || subs
                .iter()
                .any(|f| f.matches(venue, Some(symbol), channel, kind))
    };
    // Every lock is taken and released before any `await`: a `std::sync::MutexGuard` held across an
    // await point does not compile here, and would be a latency bug regardless.
    let snapshot: Vec<FeedMessage> = {
        let guard = crate::model::lock(instruments);
        guard
            .values()
            .filter(|i| pass(&i.venue, &i.symbol, Some(i.channel), "instrument"))
            .cloned()
            .map(FeedMessage::Instrument)
            .collect()
    };
    let depths: Vec<FeedMessage> = {
        let guard = crate::model::lock(depth);
        guard
            .values()
            .filter(|d| pass(&d.venue, &d.symbol, None, "depth"))
            .cloned()
            .map(FeedMessage::Depth)
            .collect()
    };
    let rebaselines: Vec<FeedMessage> = {
        let guard = crate::model::lock(books);
        guard
            .iter()
            // A market accumulated mid-stream holds only the levels that have moved since, so
            // replaying it as full state would tell the client to discard the ones it never saw.
            .filter(|(_, acc)| acc.baselined())
            .filter(|((venue, channel, _), acc)| pass(venue, acc.symbol(), Some(*channel), "book"))
            .map(|((venue, channel, id), acc)| FeedMessage::Book(acc.to_book(venue, *channel, *id)))
            .collect()
    };
    for m in snapshot.into_iter().chain(depths).chain(rebaselines) {
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
    books: BookSnapshot,
    cfg: WsConfig,
) -> Result<()> {
    let ws = tokio_tungstenite::accept_async(stream).await?;
    let (mut write, mut read) = ws.split();

    // Per-client state. Empty `subs` = firehose (receive every venue/symbol).
    let mut subs: Vec<SubFilter> = Vec::new();

    // Replay definitions (precision first) then current book state, so a mid-stream consumer is
    // bootstrapped immediately instead of waiting for the next periodic book. (Quotes/trades are not
    // replayed - the next quote is itself full state.) `subs` is empty here, so this connect-time
    // replay is unfiltered.
    replay_scoped(&mut write, &instruments, &depth, &books, &subs).await?;

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
                                    replay_scoped(&mut write, &instruments, &depth, &books, std::slice::from_ref(&subscription)).await?;
                                }
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
                Err(broadcast::error::RecvError::Lagged(n)) => {
                    metrics().ws_client_lagged.inc();
                    warn!("subscriber lagged, dropped {n}");
                    // `book` is incremental: a dropped batch leaves this client's book permanently
                    // wrong, so re-baseline it. (`quote`/`depth` self-heal on the next message.)
                    replay_scoped(&mut write, &instruments, &depth, &books, &subs).await?;
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
            BookAccumulator, BookAction, BookChange, BookSide, FeedMessage, NormalizedBook,
            NormalizedInstrument, NormalizedQuote,
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

    /// `instrument` carries its own channel, so it is filtered like `book`: a channel-scoped client
    /// gets the definitions it needs to scale that channel's books, and no other channel's.
    #[test]
    fn channel_filter_selects_one_channels_instrument_definitions() {
        let f = filter(r#"{"channel":2}"#);
        assert!(f.matches("Lashay", Some("KXBTCPERP"), Some(2), "instrument"));
        assert!(!f.matches("Lashay", Some("KXETHPERP"), Some(1), "instrument"));
        // `symbol` still narrows instruments independently of the channel.
        assert!(!filter(r#"{"channel":2,"symbol":"SOL"}"#).matches(
            "Lashay",
            Some("KXBTCPERP"),
            Some(2),
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

    /// `book` and `instrument` must carry their channel so an explicit channel filter can select
    /// them; every other kind carries none, which is what the filter's exclusion rule rests on.
    #[test]
    fn prepare_populates_the_channel_for_book_and_instrument() {
        use super::prepare;
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
        let i = prepare(&FeedMessage::Instrument(NormalizedInstrument {
            venue: "Lashay".into(),
            symbol: "KXBTCPERP".into(),
            channel: 2,
            instrument_id: 41,
            price_exponent: -2,
            qty_exponent: -2,
        }))
        .expect("serializes");
        assert_eq!(i.kind, "instrument");
        assert_eq!(i.channel, Some(2));

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
        let books = Arc::new(Mutex::new(HashMap::new()));
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
        };
        let books = Arc::new(Mutex::new(HashMap::new()));
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
        for sym in ["SOL", "BTC"] {
            let arc: Arc<str> = sym.into();
            defs.insert(
                (Arc::<str>::from("Hyperliquid"), arc.clone()),
                NormalizedInstrument {
                    venue: "Hyperliquid".into(),
                    symbol: arc,
                    channel: 0,
                    instrument_id: 1,
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
        let books = Arc::new(Mutex::new(HashMap::new()));
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
    /// bootstrapped with the definitions it can use and not another channel's. The two markets use
    /// different symbols because the snapshot is keyed `(venue, symbol)`. `#[serial]` for the shared
    /// `dz_ws_clients` gauge.
    #[tokio::test]
    #[serial]
    async fn replay_is_scoped_to_the_subscribed_channel() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (tx, _rx) = broadcast::channel::<std::sync::Arc<FeedMessage>>(16);

        let mut defs = HashMap::new();
        for (sym, channel) in [("KXBTCPERP", 2u32), ("KXETHPERP", 3)] {
            let arc: Arc<str> = sym.into();
            defs.insert(
                (Arc::<str>::from("Lashay"), arc.clone()),
                NormalizedInstrument {
                    venue: "Lashay".into(),
                    symbol: arc,
                    channel,
                    instrument_id: 41,
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
        let srv = tokio::spawn(serve(
            listener,
            tx.clone(),
            instruments,
            depth,
            Arc::new(Mutex::new(HashMap::new())),
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
        }
    }

    fn book_batch(symbol: &str, changes: Vec<BookChange>, last: bool) -> NormalizedBook {
        NormalizedBook {
            venue: "Lashay".into(),
            symbol: symbol.into(),
            channel: 0,
            instrument_id: 0,
            changes,
            snapshot: false,
            last,
            source_ts_ns: 7,
            recv_ts_ns: 0,
            kernel_rx_ts_ns: 0,
            ws_send_ts_ns: 0,
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
        instruments: HashMap<(Arc<str>, Arc<str>), NormalizedInstrument>,
        books: HashMap<(Arc<str>, u32, u32), BookAccumulator>,
    ) -> (
        tokio::task::JoinHandle<anyhow::Result<()>>,
        broadcast::Sender<std::sync::Arc<FeedMessage>>,
        std::net::SocketAddr,
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
        };
        let srv = tokio::spawn(serve(
            listener,
            tx.clone(),
            Arc::new(Mutex::new(instruments)),
            Arc::new(Mutex::new(HashMap::new())),
            Arc::new(Mutex::new(books)),
            cfg,
        ));
        (srv, tx, addr)
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

    fn parse_book(frame: &str) -> NormalizedBook {
        match serde_json::from_str(frame).expect("frame parses") {
            FeedMessage::Book(b) => b,
            other => panic!("expected a book frame, got {other:?}"),
        }
    }

    /// A connecting client is bootstrapped with the accumulated `book` state as a re-baseline: a
    /// `Clear`/`Both` leading the complete level set, best-first, marked `last`.
    #[tokio::test]
    #[serial]
    async fn connect_replays_the_accumulated_book_rebaseline() {
        let mut books = HashMap::new();
        books.insert(
            (Arc::<str>::from("Lashay"), 2u32, 41u32),
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

    /// A `{"channel":N}` subscribe replays only that channel's markets — the reason `replay_scoped`
    /// passes each message's own channel to the filter instead of `None`.
    #[tokio::test]
    #[serial]
    async fn subscribe_scopes_the_book_replay_by_channel() {
        let mut books = HashMap::new();
        books.insert(
            (Arc::<str>::from("Lashay"), 2u32, 41u32),
            accumulator("KXBTCPERP", 0.61, 0.63),
        );
        books.insert(
            (Arc::<str>::from("Lashay"), 3u32, 7u32),
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

    /// Precision before price: the `instrument` definition is replayed ahead of the market's `book`.
    #[tokio::test]
    #[serial]
    async fn instrument_is_replayed_before_the_book() {
        let mut defs = HashMap::new();
        defs.insert(
            (Arc::<str>::from("Lashay"), Arc::<str>::from("KXBTCPERP")),
            NormalizedInstrument {
                venue: "Lashay".into(),
                symbol: "KXBTCPERP".into(),
                channel: 2,
                instrument_id: 41,
                price_exponent: -2,
                qty_exponent: -2,
            },
        );
        let mut books = HashMap::new();
        books.insert(
            (Arc::<str>::from("Lashay"), 2u32, 41u32),
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

    /// Only a re-baselined market is bootstrapped. One accumulated mid-stream holds just the levels
    /// that have moved since, and `to_book` stamps `snapshot: true` — replaying it would tell the
    /// client to discard the rest of the book. Such a client waits for the producer's next
    /// re-baseline, as it did before the book replay existed.
    #[tokio::test]
    #[serial]
    async fn mid_stream_markets_are_not_replayed() {
        let mut mid_stream = BookAccumulator::new("KXETHPERP".into());
        mid_stream.apply(&book_batch(
            "KXETHPERP",
            vec![level_update(BookSide::Bid, 0.41, 5.0)],
            true,
        ));
        assert!(!mid_stream.baselined(), "no Clear was folded in");

        let mut books = HashMap::new();
        books.insert((Arc::<str>::from("Lashay"), 3u32, 7u32), mid_stream);
        books.insert(
            (Arc::<str>::from("Lashay"), 2u32, 41u32),
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
    async fn a_lagging_client_is_rebaselined() {
        use super::{prepare, serve_client};

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (prepared_tx, prepared_rx) = broadcast::channel(1);
        for _ in 0..2 {
            let frame = prepare(&FeedMessage::Quote(sample_quote())).expect("serializes");
            assert!(prepared_tx.send(frame).is_ok(), "the receiver is alive");
        }

        let mut books = HashMap::new();
        books.insert(
            (Arc::<str>::from("Lashay"), 2u32, 41u32),
            accumulator("KXBTCPERP", 0.61, 0.63),
        );
        let cfg = WsConfig {
            heartbeat: Duration::from_secs(30),
            idle_timeout: Duration::from_secs(60),
            max_clients: 8,
            max_subs: 8,
            max_inbound_per_min: 600,
            broadcast_capacity: 1,
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
}
