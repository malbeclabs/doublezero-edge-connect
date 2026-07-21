//! Venue-generic **public WebSocket input feeder** scaffolding — the transport half shared by every
//! public backstop source (Hyperliquid, Phoenix, …).
//!
//! A public feeder is a second ingest source, off by default, that connects to a venue's own public
//! `wss://`, decodes its JSON into the same [`FeedMessage`]s the multicast pipeline produces, and
//! emits them through the **shared [`crate::ingest::arbiter`]** as [`Publisher::PublicWs`]. It is a
//! different transport from the multicast receiver — it never touches the `FrameProcessor` /
//! `recv_any` machinery — but it converges on the *same* per-`(venue, symbol)` arbiter dedup state,
//! so a public copy of an update the edge already emitted collapses into a no-op, and when the edge
//! gaps the public copy is the first to cross and fills in. That backstop falls out of the arbiter
//! with no health check.
//!
//! Everything venue-specific (URL, subscribe frames, frame decode) lives behind [`PublicVenue`];
//! this module owns only the reconnect/backoff loop, the frame pump, and the small validation
//! helpers every venue decoder reuses.
//!
//! **Failure isolation.** [`run`] is its own task with reconnect + exponential backoff; every
//! decode/socket error is logged and swallowed, so neither a reconnect storm nor a malformed frame
//! can ever wedge the multicast hot path.

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use futures_util::{SinkExt, StreamExt};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{info, warn};

use crate::{ingest::arbiter::SharedArbiter, metrics::metrics, model::InstrumentSnapshot};

/// Reconnect backoff bounds. Backoff resets to the minimum once a connection is established, then
/// doubles up to the maximum across consecutive failures.
const BACKOFF_MIN: Duration = Duration::from_millis(500);
const BACKOFF_MAX: Duration = Duration::from_secs(30);
/// Minimum session duration that counts as "stable": only after staying connected this long do we
/// reset the backoff to `BACKOFF_MIN`. A connect-then-immediate-drop (rejected subscriptions,
/// instant server close) keeps escalating instead of pinning at the floor and hammering the public
/// endpoint ~2 reconnects/sec.
const STABLE_SESSION: Duration = Duration::from_secs(30);
/// Client-initiated keepalive interval. A quiet market sends no data, and some public venues reap a
/// silent connection, so we ping rather than relying only on the server pinging us. Matches the
/// reference public-API client's 20s keepalive.
const KEEPALIVE_INTERVAL: Duration = Duration::from_secs(20);

/// One public venue's transport contract: where to connect, what to subscribe, and how to decode a
/// text frame into emitted `FeedMessage`s. The reconnect loop and frame pump are venue-agnostic and
/// supplied by [`run`].
pub trait PublicVenue {
    /// Venue name stamped on every emitted message and every metric (must match the edge feed's
    /// venue so both land in the same arbiter dedup state).
    fn venue(&self) -> &str;
    /// The public `wss://` (or `ws://` in tests) endpoint to connect to.
    fn url(&self) -> &str;
    /// The subscribe frames to send once connected, one per channel/market. An empty list means the
    /// feeder is off ([`run`] returns immediately).
    fn subscribe_msgs(&self) -> Vec<String>;
    /// Decode one text frame and emit any resulting message through the arbiter. Unknown channels and
    /// malformed payloads must be ignored (best-effort feed); decode errors should increment
    /// `ws_feeder_decode_errors[venue]`.
    fn handle_text(&self, txt: &str, arbiter: &SharedArbiter, instruments: &InstrumentSnapshot);
}

/// Run a public WS input feeder forever (reconnecting on any failure). Returns immediately as a
/// no-op when the venue has no subscriptions (the feeder is off by default). Spawned as its own
/// task; it never propagates an error, so the multicast hot path is unaffected by its churn.
pub async fn run(venue: impl PublicVenue, arbiter: SharedArbiter, instruments: InstrumentSnapshot) {
    let subs = venue.subscribe_msgs();
    if subs.is_empty() {
        return;
    }
    let v = venue.venue().to_string();
    let url = venue.url().to_string();

    let m = metrics();
    let mut backoff = BACKOFF_MIN;
    loop {
        match connect_async(&url).await {
            Ok((ws, _resp)) => {
                info!(venue = %v, %url, "public WS input feeder connected");
                m.ws_feeder_up.with_label_values(&[&v]).set(1);
                let started = Instant::now();
                match stream(ws, &venue, &subs, &arbiter, &instruments).await {
                    Ok(()) => info!(venue = %v, "public WS input feeder closed; reconnecting"),
                    Err(e) => {
                        warn!(venue = %v, error = %e, "public WS input feeder session error; reconnecting")
                    }
                }
                m.ws_feeder_up.with_label_values(&[&v]).set(0);
                // Reset the backoff only after a *stable* session; a connect-then-immediate-drop
                // keeps escalating so a flapping endpoint isn't hammered (see `STABLE_SESSION`).
                backoff = if started.elapsed() >= STABLE_SESSION {
                    BACKOFF_MIN
                } else {
                    (backoff * 2).min(BACKOFF_MAX)
                };
            }
            Err(e) => {
                warn!(venue = %v, error = %e, %url, "public WS input feeder connect failed; retrying");
                m.ws_feeder_up.with_label_values(&[&v]).set(0);
                backoff = (backoff * 2).min(BACKOFF_MAX);
            }
        }
        // Each loop iteration past the initial connect is one reconnect cycle (a drop or a failed
        // attempt, now backing off to retry).
        m.ws_feeder_reconnects.with_label_values(&[&v]).inc();
        tokio::time::sleep(backoff).await;
    }
}

/// Send every subscribe frame on the open connection, then pump decoded text frames into the venue
/// decoder until the socket closes or errors.
async fn stream<S, V: PublicVenue>(
    mut ws: S,
    venue: &V,
    subs: &[String],
    arbiter: &SharedArbiter,
    instruments: &InstrumentSnapshot,
) -> Result<(), tokio_tungstenite::tungstenite::Error>
where
    S: SinkExt<Message, Error = tokio_tungstenite::tungstenite::Error>
        + StreamExt<Item = Result<Message, tokio_tungstenite::tungstenite::Error>>
        + Unpin,
{
    for sub in subs {
        ws.send(Message::Text(sub.clone().into())).await?;
    }

    // Pump inbound frames, and ping on an interval so a silent (quiet-market) session isn't reaped.
    // tokio-tungstenite auto-answers inbound pings but never initiates one, so the client keepalive
    // is ours to send.
    let mut keepalive = tokio::time::interval(KEEPALIVE_INTERVAL);
    keepalive.tick().await; // first tick is immediate — skip it so the first ping is one interval in
    loop {
        tokio::select! {
            msg = ws.next() => {
                let Some(msg) = msg else { break }; // stream ended
                match msg? {
                    Message::Text(txt) => venue.handle_text(&txt, arbiter, instruments),
                    // Reply to server pings so the connection is not reaped while a market is quiet.
                    Message::Ping(payload) => ws.send(Message::Pong(payload)).await?,
                    Message::Close(_) => break,
                    _ => {}
                }
            }
            _ = keepalive.tick() => ws.send(Message::Ping(Default::default())).await?,
        }
    }
    Ok(())
}

/// True if the `(venue, symbol)` instrument is already in the shared snapshot, so a price emitted for
/// it carries known precision (precision before price).
pub fn instrument_known(instruments: &InstrumentSnapshot, venue: &str, symbol: &str) -> bool {
    crate::model::lock(instruments).contains_key(&(Arc::from(venue), Arc::from(symbol)))
}

/// Parse a non-negative, finite `f64` from a decimal string, or `None`. Rejects `NaN`/`±inf`
/// (which `str::parse::<f64>` accepts from `"nan"`/`"inf"` and produces on overflow like `"1e400"`)
/// and negatives, so a malformed public px/sz is dropped rather than emitted — a non-finite would
/// otherwise serialize to JSON `null` on the wire (breaking the numeric contract) and a `NaN` would
/// defeat the floor's content dedup (`NaN != NaN`).
pub fn parse_decimal(s: &str) -> Option<f64> {
    finite_non_negative(s.parse().ok()?)
}

/// Validate an already-parsed JSON float the same way [`parse_decimal`] validates a string: keep it
/// only if it is finite and non-negative, else `None`.
pub fn finite_non_negative(v: f64) -> Option<f64> {
    (v.is_finite() && v >= 0.0).then_some(v)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_decimal_rejects_non_finite_and_negative() {
        assert_eq!(parse_decimal("104783.0"), Some(104783.0));
        assert_eq!(parse_decimal("0"), Some(0.0));
        assert!(parse_decimal("nan").is_none());
        assert!(parse_decimal("inf").is_none());
        assert!(parse_decimal("1e400").is_none());
        assert!(parse_decimal("-0.5").is_none());
        assert!(parse_decimal("notanumber").is_none());
    }

    #[test]
    fn finite_non_negative_matches_parse_decimal() {
        assert_eq!(finite_non_negative(1.5), Some(1.5));
        assert_eq!(finite_non_negative(0.0), Some(0.0));
        assert!(finite_non_negative(-1.0).is_none());
        assert!(finite_non_negative(f64::NAN).is_none());
        assert!(finite_non_negative(f64::INFINITY).is_none());
    }
}
