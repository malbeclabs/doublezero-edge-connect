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

use crate::model::{now_ns, DepthSnapshot, FeedMessage, InstrumentSnapshot};

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

pub async fn run(
    bind: String,
    tx: broadcast::Sender<FeedMessage>,
    instruments: InstrumentSnapshot,
    depth: DepthSnapshot,
    cfg: WsConfig,
) -> Result<()> {
    let listener = TcpListener::bind(&bind).await?;
    info!(%bind, max_clients = cfg.max_clients, "WebSocket server listening");
    let clients = Arc::new(AtomicUsize::new(0));

    loop {
        let (stream, peer) = listener.accept().await?;
        // Connection limit: reject (drop the TCP stream) once at capacity.
        if clients.fetch_add(1, Ordering::SeqCst) >= cfg.max_clients {
            clients.fetch_sub(1, Ordering::SeqCst);
            warn!(%peer, max = cfg.max_clients, "max clients reached; rejecting connection");
            drop(stream);
            continue;
        }
        let rx = tx.subscribe();
        let instruments = instruments.clone();
        let depth = depth.clone();
        let cfg = cfg.clone();
        let clients = clients.clone();
        tokio::spawn(async move {
            if let Err(e) = serve_client(stream, rx, instruments, depth, cfg).await {
                warn!(%peer, "client ended: {e}");
            }
            clients.fetch_sub(1, Ordering::SeqCst);
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
                        write.send(text(json!({"channel": "error", "error": "inbound rate limit exceeded"}))).await?;
                        break;
                    }
                    match serde_json::from_str::<ClientMsg>(&txt) {
                        Ok(ClientMsg::Ping) => write.send(text(json!({"channel": "pong"}))).await?,
                        Ok(ClientMsg::Subscribe { subscription }) => {
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
                            subs.retain(|s| s != &subscription);
                            write.send(text(json!({
                                "channel": "subscription_response", "method": "unsubscribe",
                                "subscription": subscription,
                            }))).await?;
                        }
                        Err(_) => write.send(text(json!({"channel": "error", "error": "unrecognized message"}))).await?,
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
                        match m {
                            FeedMessage::Quote(ref mut q) => q.ws_send_ts_ns = now_ns(),
                            FeedMessage::Trade(ref mut t) => t.ws_send_ts_ns = now_ns(),
                            FeedMessage::Midpoint(ref mut mp) => mp.ws_send_ts_ns = now_ns(),
                            FeedMessage::Depth(ref mut d) => d.ws_send_ts_ns = now_ns(),
                            _ => {}
                        }
                        write.send(WsMessage::Text(serde_json::to_string(&m)?.into())).await?;
                    }
                }
                Err(broadcast::error::RecvError::Lagged(n)) => warn!("subscriber lagged, dropped {n}"),
                Err(broadcast::error::RecvError::Closed) => break,
            },
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::SubFilter;

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
}
