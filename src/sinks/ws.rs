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
use tokio_tungstenite::tungstenite::Message as WsMessage;
use tracing::{info, warn};

use crate::{
    metrics::metrics,
    model::{now_ns, DepthSnapshot, FeedMessage, InstrumentSnapshot},
};

/// Tunable server limits / liveness (from CLI args).
#[derive(Clone, Debug)]
pub struct WsConfig {
    pub heartbeat: Duration,
    pub idle_timeout: Duration,
    pub max_clients: usize,
    pub max_subs: usize,
    pub max_inbound_per_min: u32,
}

/// A subscription filter: a `None` field matches any value (so `{}` = everything).
#[derive(Deserialize, Serialize, Clone, PartialEq, Debug)]
struct SubFilter {
    #[serde(default)]
    venue: Option<String>,
    #[serde(default)]
    symbol: Option<String>,
}

impl SubFilter {
    fn matches(&self, venue: &str, symbol: &str) -> bool {
        // Venue codes are registry identifiers, not free text - match case-insensitively so a
        // subscription for `PHOENIX` / `phoenix` still selects the wire venue `Phoenix`. Symbol
        // stays an exact match (venues name symbols precisely, e.g. `SOL-PERP`).
        self.venue
            .as_deref()
            .is_none_or(|v| v.eq_ignore_ascii_case(venue))
            && self.symbol.as_deref().is_none_or(|s| s == symbol)
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

pub async fn run(
    bind: String,
    tx: broadcast::Sender<FeedMessage>,
    instruments: InstrumentSnapshot,
    depth: DepthSnapshot,
    cfg: WsConfig,
) -> Result<()> {
    let listener = TcpListener::bind(&bind).await?;
    info!(%bind, max_clients = cfg.max_clients, "WebSocket server listening");
    serve(listener, tx, instruments, depth, cfg).await
}

/// The accept loop, split out so tests can drive a pre-bound listener on an ephemeral port.
async fn serve(
    listener: TcpListener,
    tx: broadcast::Sender<FeedMessage>,
    instruments: InstrumentSnapshot,
    depth: DepthSnapshot,
    cfg: WsConfig,
) -> Result<()> {
    let clients = Arc::new(AtomicUsize::new(0));

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
        let rx = tx.subscribe();
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

async fn serve_client(
    stream: TcpStream,
    mut rx: broadcast::Receiver<FeedMessage>,
    instruments: InstrumentSnapshot,
    depth: DepthSnapshot,
    cfg: WsConfig,
) -> Result<()> {
    let ws = tokio_tungstenite::accept_async(stream).await?;
    let (mut write, mut read) = ws.split();

    // Replay current instrument definitions first so precision is known before any quote/depth.
    let snapshot: Vec<FeedMessage> = {
        let guard = instruments.lock().unwrap();
        guard
            .values()
            .cloned()
            .map(FeedMessage::Instrument)
            .collect()
    };
    for inst in snapshot {
        write
            .send(WsMessage::Text(serde_json::to_string(&inst)?.into()))
            .await?;
    }

    // Then replay the latest order-book `depth` per (venue, symbol): depth is full state, so one
    // replayed snapshot bootstraps a mid-stream consumer immediately instead of waiting for the
    // next periodic one. (Quotes/trades are not replayed - the next quote is itself full state.)
    let depth_snapshot: Vec<FeedMessage> = {
        let guard = depth.lock().unwrap();
        guard.values().cloned().map(FeedMessage::Depth).collect()
    };
    for d in depth_snapshot {
        write
            .send(WsMessage::Text(serde_json::to_string(&d)?.into()))
            .await?;
    }

    // Per-client state. Empty `subs` = firehose (receive every venue/symbol).
    let mut subs: Vec<SubFilter> = Vec::new();
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

            // Forward broadcast feed messages this subscriber wants.
            msg = rx.recv() => match msg {
                Ok(mut m) => {
                    let pass = match &m {
                        // A venue-level status has no symbol, so match it by venue alone - else a
                        // symbol-scoped subscription (e.g. {venue,symbol:"SOL"}) would never see it.
                        FeedMessage::Status(st) => {
                            subs.is_empty()
                                || subs.iter().any(|f| {
                                    f.venue
                                        .as_deref()
                                        .is_none_or(|v| v.eq_ignore_ascii_case(&st.venue))
                                })
                        }
                        _ => {
                            let (v, s) = m.venue_symbol();
                            subs.is_empty() || subs.iter().any(|f| f.matches(v, s))
                        }
                    };
                    if pass {
                        // Stamp the WS hand-off time on the latency-bearing data messages.
                        let kind = match m {
                            FeedMessage::Quote(ref mut q) => { q.ws_send_ts_ns = now_ns(); "quote" }
                            FeedMessage::Trade(ref mut t) => { t.ws_send_ts_ns = now_ns(); "trade" }
                            FeedMessage::Midpoint(ref mut mp) => { mp.ws_send_ts_ns = now_ns(); "midpoint" }
                            FeedMessage::Depth(ref mut d) => { d.ws_send_ts_ns = now_ns(); "depth" }
                            FeedMessage::Instrument(_) => "instrument",
                            FeedMessage::Status(_) => "status",
                        };
                        let payload = serde_json::to_string(&m)?;
                        metrics().ws_messages_sent.with_label_values(&[kind]).inc();
                        metrics().ws_bytes_sent.with_label_values(&[kind]).inc_by(payload.len() as u64);
                        write.send(WsMessage::Text(payload.into())).await?;
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
        model::{FeedMessage, NormalizedQuote},
    };

    fn filter(venue: Option<&str>, symbol: Option<&str>) -> SubFilter {
        SubFilter {
            venue: venue.map(str::to_string),
            symbol: symbol.map(str::to_string),
        }
    }

    #[test]
    fn venue_matches_case_insensitively() {
        // The wire venue is `Phoenix`; a filter spelled any case must still select it (the
        // PROTOCOL.md example historically showed `PHOENIX`, which would silently drop the feed
        // under an exact match).
        assert!(filter(Some("PHOENIX"), None).matches("Phoenix", "BTC"));
        assert!(filter(Some("phoenix"), None).matches("Phoenix", "BTC"));
        assert!(filter(Some("Phoenix"), None).matches("Phoenix", "BTC"));
        assert!(!filter(Some("Hyperliquid"), None).matches("Phoenix", "BTC"));
    }

    #[test]
    fn omitted_field_matches_any_symbol_exact() {
        assert!(filter(None, None).matches("Phoenix", "BTC")); // {} = everything
        assert!(filter(None, Some("BTC")).matches("Phoenix", "BTC"));
        assert!(!filter(None, Some("btc")).matches("Phoenix", "BTC")); // symbol stays exact
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
            venue: "Hyperliquid".to_string(),
            symbol: "BTC".to_string(),
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
        let (tx, _rx) = broadcast::channel::<FeedMessage>(16);
        let instruments = Arc::new(Mutex::new(HashMap::new()));
        let depth = Arc::new(Mutex::new(HashMap::new()));
        let cfg = WsConfig {
            heartbeat: Duration::from_secs(30),
            idle_timeout: Duration::from_secs(60),
            max_clients: 8,
            max_subs: 8,
            max_inbound_per_min: 600,
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
            let _ = tx.send(FeedMessage::Quote(sample_quote()));
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
}
