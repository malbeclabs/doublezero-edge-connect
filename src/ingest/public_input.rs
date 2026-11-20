//! Venue-generic **public WebSocket input** scaffolding — the transport half shared by every
//! public backstop venue (Hyperliquid, Phoenix, …).
//!
//! A public input is off by default and connects to a venue's own public
//! `wss://`, decodes its JSON into the same [`FeedMessage`]s the multicast pipeline produces, and
//! emits them through the **shared [`crate::ingest::arbiter`]** as [`Transport::PublicWs`]. It is a
//! different transport from the multicast receiver — it never touches the `DatagramProcessor` /
//! `recv_any` machinery — but it converges on the *same* per-`(venue, symbol)` arbiter dedup state,
//! so a public copy of an update the edge already emitted collapses into a no-op, and when the edge
//! gaps the public copy is the first to cross and fills in. That backstop falls out of the arbiter
//! with no health check.
//!
//! **The backstop needs the edge to have gone live at least once.** Each venue's `instrument_known`
//! gate reads the shared `InstrumentSnapshot`, which `ingest::processor` populates only once the edge
//! has revealed an instrument's wire Source ID (see that module's per-instrument deferral) — refdata
//! alone never populates it. A gap partway through (the edge was live, then goes quiet) is backstopped
//! normally; a cold start with no edge price at all for that instrument leaves this gate permanently
//! closed for it, since there is no wire-derived precision/venue to key it by.
//!
//! Everything venue-specific (URL, subscribe frames, frame decode) lives behind [`PublicVenue`];
//! this module owns only the reconnect/backoff loop, the frame pump, the subscription sequencer, and
//! the small validation helpers every venue decoder reuses.
//!
//! **Subscriptions go out one at a time**, each after the previous one's answer or [`ACK_TIMEOUT`],
//! so an answer — or, on a venue that drops the connection on an unknown name, a dropped connection
//! — names exactly one subscription. Each one's standing feeds the `GET /v1/race` counting gate
//! ([`crate::ingest::race`]).
//!
//! **Failure isolation.** [`run`] is its own task with reconnect + exponential backoff; every
//! decode/socket error is logged and swallowed, so neither a reconnect storm nor a malformed frame
//! can ever wedge the multicast hot path.

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Duration,
};

use futures_util::{SinkExt, StreamExt};
use tokio::time::Instant;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{info, warn};

use crate::{
    ingest::{
        arbiter::{lock, SharedArbiter},
        race::{Kind, SubState},
    },
    metrics::metrics,
    model::InstrumentSnapshot,
};

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

/// How long a subscription waits for its answer before the next one is sent. It stays pending.
const ACK_TIMEOUT: Duration = Duration::from_secs(5);

/// Connection drops with a name's subscription in flight before it is refused, on a venue that drops
/// the connection on an unknown name. One would refuse a valid name for a network drop inside its
/// round trip.
const STRIKES_TO_REFUSE: u8 = 2;

/// One subscribe frame and what it races.
pub struct Subscription {
    pub symbol: String,
    pub kind: Kind,
    pub frame: String,
}

/// What a text frame meant for the subscriptions. Any market data it carried is already emitted.
#[derive(Debug, PartialEq, Eq)]
pub enum Frame {
    Ack {
        symbol: String,
        kind: Kind,
    },
    Refused {
        symbol: String,
    },
    Data {
        symbol: String,
        kind: Kind,
    },
    /// A batch with nothing in it, so naming no symbol: on a replaying venue, the replay of the
    /// subscription acked last, which the venue sends straight after its ack.
    EmptyBatch {
        kind: Kind,
    },
    Other,
}

/// One public venue's transport contract: where to connect, what to subscribe, and how to decode a
/// text frame into emitted `FeedMessage`s. The reconnect loop and frame pump are venue-agnostic and
/// supplied by [`run`].
pub trait PublicVenue {
    /// The venue answers an unknown name by dropping the connection rather than refusing it, so the
    /// subscription in flight when the connection drops is a strike against its name.
    const REFUSES_BY_DISCONNECT: bool;
    /// The first data frame after each ack replays state from before the subscription.
    const REPLAYS_ON_SUBSCRIBE: bool;
    /// Venue name stamped on every emitted message and every metric (must match the edge feed's
    /// venue so both land in the same arbiter dedup state).
    fn venue(&self) -> &str;
    /// The registry Source ID stamped on every emitted message.
    fn source_id(&self) -> u16;
    /// The public `wss://` (or `ws://` in tests) endpoint to connect to.
    fn url(&self) -> &str;
    /// One per name and kind, in sending order. An empty list means the input is off ([`run`]
    /// returns immediately).
    fn subscriptions(&self) -> Vec<Subscription>;
    /// Decode one text frame and emit any resulting message through the arbiter. Unknown channels and
    /// malformed payloads must be ignored (best-effort feed); decode errors should increment
    /// `ws_input_decode_errors[venue]`.
    fn handle_text(
        &self,
        txt: &str,
        arbiter: &SharedArbiter,
        instruments: &InstrumentSnapshot,
    ) -> Frame;
}

/// Configured names in order, each once, none empty: Hyperliquid never answers a duplicate
/// subscription.
pub fn distinct(names: Vec<String>) -> Vec<String> {
    let mut seen = HashSet::new();
    names
        .into_iter()
        .filter(|n| !n.is_empty() && seen.insert(n.clone()))
        .collect()
}

/// Run a public WS input forever (reconnecting on any failure). Returns immediately as a
/// no-op when the venue has no subscriptions (the input is off by default). Spawned as its own
/// task; it never propagates an error, so the multicast hot path is unaffected by its churn.
pub async fn run<V: PublicVenue>(
    venue: V,
    arbiter: SharedArbiter,
    instruments: InstrumentSnapshot,
) {
    let subs = venue.subscriptions();
    if subs.is_empty() {
        return;
    }
    let v = venue.venue().to_string();
    let url = venue.url().to_string();
    let source_id = venue.source_id();
    lock(&arbiter).race_board().register(
        source_id,
        &v,
        subs.iter().map(|s| (s.symbol.as_str(), s.kind)),
    );
    let mut seq = Sequencer::new(subs, V::REPLAYS_ON_SUBSCRIBE, &v);

    let m = metrics();
    let mut backoff = BACKOFF_MIN;
    loop {
        match connect_async(&url).await {
            Ok((ws, _resp)) => {
                info!(venue = %v, %url, "public WS input connected");
                m.ws_input_up.with_label_values(&[&v]).set(1);
                lock(&arbiter).race_board().set_up(source_id, true);
                seq.start_session();
                let started = Instant::now();
                match stream(ws, &venue, &mut seq, &arbiter, &instruments).await {
                    Ok(()) => info!(venue = %v, "public WS input closed; reconnecting"),
                    Err(e) => {
                        warn!(venue = %v, error = %e, "public WS input session error; reconnecting")
                    }
                }
                m.ws_input_up.with_label_values(&[&v]).set(0);
                seq.end_session(V::REFUSES_BY_DISCONNECT);
                seq.publish(&arbiter, source_id);
                lock(&arbiter).race_board().set_up(source_id, false);
                // Reset the backoff only after a *stable* session; a connect-then-immediate-drop
                // keeps escalating so a flapping endpoint isn't hammered (see `STABLE_SESSION`).
                backoff = if started.elapsed() >= STABLE_SESSION {
                    BACKOFF_MIN
                } else {
                    (backoff * 2).min(BACKOFF_MAX)
                };
            }
            Err(e) => {
                warn!(venue = %v, error = %e, %url, "public WS input connect failed; retrying");
                m.ws_input_up.with_label_values(&[&v]).set(0);
                backoff = (backoff * 2).min(BACKOFF_MAX);
            }
        }
        // Each loop iteration past the initial connect is one reconnect cycle (a drop or a failed
        // attempt, now backing off to retry).
        m.ws_input_reconnects.with_label_values(&[&v]).inc();
        lock(&arbiter).race_board().count_reconnect(source_id);
        tokio::time::sleep(backoff).await;
    }
}

/// Send the subscriptions one at a time on the open connection, pumping decoded text frames into
/// the venue decoder until the socket closes or errors.
async fn stream<S, V: PublicVenue>(
    mut ws: S,
    venue: &V,
    seq: &mut Sequencer,
    arbiter: &SharedArbiter,
    instruments: &InstrumentSnapshot,
) -> Result<(), tokio_tungstenite::tungstenite::Error>
where
    S: SinkExt<Message, Error = tokio_tungstenite::tungstenite::Error>
        + StreamExt<Item = Result<Message, tokio_tungstenite::tungstenite::Error>>
        + Unpin,
{
    // Pump inbound frames, and ping on an interval so a silent (quiet-market) session isn't reaped.
    // tokio-tungstenite auto-answers inbound pings but never initiates one, so the client keepalive
    // is ours to send.
    let mut keepalive = tokio::time::interval(KEEPALIVE_INTERVAL);
    keepalive.tick().await; // first tick is immediate — skip it so the first ping is one interval in
    loop {
        if let Some(frame) = seq.next_frame(Instant::now()) {
            ws.send(Message::Text(frame.into())).await?;
        }
        let deadline = seq.deadline();
        tokio::select! {
            msg = ws.next() => {
                let Some(msg) = msg else { break }; // stream ended
                match msg? {
                    Message::Text(txt) => seq.on_frame(venue.handle_text(&txt, arbiter, instruments)),
                    // Reply to server pings so the connection is not reaped while a market is quiet.
                    Message::Ping(payload) => ws.send(Message::Pong(payload)).await?,
                    Message::Close(_) => break,
                    _ => {}
                }
            }
            _ = keepalive.tick() => ws.send(Message::Ping(Default::default())).await?,
            () = tokio::time::sleep_until(deadline.unwrap_or_else(Instant::now)),
                if deadline.is_some() => seq.on_timeout(),
        }
        seq.publish(arbiter, venue.source_id());
    }
    Ok(())
}

/// Each subscription's standing across sessions, and which one this session is waiting on.
struct Sequencer {
    venue: String,
    subs: Vec<Subscription>,
    states: Vec<SubState>,
    /// Per name, its subscriptions' indices by [`Kind`].
    by_name: HashMap<String, [Option<usize>; 2]>,
    replays: bool,
    /// Whether a subscription went unanswered past `ACK_TIMEOUT` this session.
    timed_out: bool,
    /// The subscription acked last this session, whose replay an empty batch of its kind is.
    last_acked: Option<usize>,
    /// Drops of the connection with the name's subscription in flight.
    strikes: HashMap<String, u8>,
    next: usize,
    /// The subscription awaiting its answer, and when to stop waiting.
    waiting: Option<(usize, Instant)>,
    /// Indices whose state changed since the last [`Self::publish`].
    changed: Vec<usize>,
}

impl Sequencer {
    fn new(subs: Vec<Subscription>, replays: bool, venue: &str) -> Self {
        let mut by_name: HashMap<String, [Option<usize>; 2]> = HashMap::new();
        for (i, s) in subs.iter().enumerate() {
            by_name.entry(s.symbol.clone()).or_default()[s.kind as usize] = Some(i);
        }
        Self {
            venue: venue.to_string(),
            states: vec![SubState::Pending; subs.len()],
            subs,
            by_name,
            replays,
            timed_out: false,
            last_acked: None,
            strikes: HashMap::new(),
            next: 0,
            waiting: None,
            changed: Vec::new(),
        }
    }

    fn start_session(&mut self) {
        self.next = 0;
        self.waiting = None;
        self.timed_out = false;
        self.last_acked = None;
    }

    /// The next subscribe frame, once nothing awaits an answer.
    fn next_frame(&mut self, now: Instant) -> Option<String> {
        if self.waiting.is_some() {
            return None;
        }
        while self.states.get(self.next) == Some(&SubState::Refused) {
            self.next += 1;
        }
        let i = self.next;
        let frame = self.subs.get(i)?.frame.clone();
        self.next += 1;
        self.waiting = Some((i, now + ACK_TIMEOUT));
        Some(frame)
    }

    fn deadline(&self) -> Option<Instant> {
        self.waiting.map(|(_, at)| at)
    }

    fn on_timeout(&mut self) {
        self.waiting = None;
        self.timed_out = true;
    }

    fn on_frame(&mut self, frame: Frame) {
        match frame {
            Frame::Ack { symbol, kind } => {
                let Some(i) = self.find(&symbol, kind) else {
                    return;
                };
                if self.states[i] == SubState::Pending {
                    let state = if self.replays {
                        SubState::Acked
                    } else {
                        SubState::Live
                    };
                    self.set(i, state);
                    self.strikes.remove(&symbol);
                    self.last_acked = Some(i);
                }
                self.answered(i);
            }
            Frame::Refused { symbol } => self.refuse(&symbol),
            Frame::Data { symbol, kind } => {
                if let Some(i) = self.find(&symbol, kind) {
                    self.replayed(i);
                }
            }
            Frame::EmptyBatch { kind } => {
                if let Some(i) = self.last_acked.filter(|&i| self.subs[i].kind == kind) {
                    self.replayed(i);
                }
            }
            Frame::Other => {}
        }
    }

    /// Every subscription back to pending for the next session. Where the venue refuses by
    /// disconnecting, the one in flight takes a strike — unless one timed out this session: a
    /// venue that has stopped answering names nothing by dropping.
    fn end_session(&mut self, refuses_by_disconnect: bool) {
        if let (true, false, Some((i, _))) = (refuses_by_disconnect, self.timed_out, self.waiting) {
            let name = self.subs[i].symbol.clone();
            let strikes = self.strikes.entry(name.clone()).or_default();
            *strikes += 1;
            if *strikes >= STRIKES_TO_REFUSE {
                self.refuse(&name);
            }
        }
        for i in 0..self.states.len() {
            if self.states[i] != SubState::Refused {
                self.set(i, SubState::Pending);
            }
        }
        self.waiting = None;
    }

    /// Push the changed states onto the arbiter's race board.
    fn publish(&mut self, arbiter: &SharedArbiter, source_id: u16) {
        if self.changed.is_empty() {
            return;
        }
        let mut arbiter = lock(arbiter);
        for i in self.changed.drain(..) {
            let sub = &self.subs[i];
            arbiter
                .race_board()
                .set_state(source_id, &sub.symbol, sub.kind, self.states[i]);
        }
    }

    fn find(&self, symbol: &str, kind: Kind) -> Option<usize> {
        self.by_name.get(symbol)?[kind as usize]
    }

    fn set(&mut self, i: usize, state: SubState) {
        if self.states[i] != state {
            self.states[i] = state;
            self.changed.push(i);
        }
    }

    /// The venue's replay for subscription `i` has passed, so it counts from here.
    fn replayed(&mut self, i: usize) {
        if self.states[i] == SubState::Acked {
            self.set(i, SubState::Live);
        }
    }

    fn answered(&mut self, i: usize) {
        if self.waiting.is_some_and(|(w, _)| w == i) {
            self.waiting = None;
        }
    }

    fn refuse(&mut self, symbol: &str) {
        let Some(slots) = self.by_name.get(symbol).copied() else {
            return;
        };
        let slots = slots.into_iter().flatten();
        if slots.clone().all(|i| self.states[i] == SubState::Refused) {
            return;
        }
        warn!(
            venue = %self.venue,
            symbol,
            "public WS input: the venue refused this name; not subscribing it again"
        );
        for i in slots {
            self.set(i, SubState::Refused);
            self.answered(i);
        }
    }
}

/// True if the `(source_id, symbol)` instrument is already in the shared snapshot, so a price emitted for
/// it carries known precision (precision before price). The snapshot is populated by the edge side
/// only once it has revealed that instrument's Source ID (see `ingest::processor`'s deferral), so
/// this stays closed for an instrument the edge has never once gone live for — a cold start, not a
/// gap.
///
/// `InstrumentSnapshot` is keyed on the `(venue, channel, instrument_id)` identity, not `(venue,
/// symbol)` — this caller only ever knows a bare `(venue, symbol)` pair (Hyperliquid/Phoenix name
/// their public WS instruments by symbol, not by the edge's channel/instrument_id), so this is a
/// scan over the shared catalog's *values* rather than a map lookup. Measured (release build,
/// worst case — no match, so every call scans the whole map) against a synthetic 2,000-entry
/// catalog — above any real deployment's instrument count, Kalshi's ~1,300 real markets included:
/// 100,000 calls in ~339ms, ~3.4µs/call. That is negligible next to this caller's own per-frame
/// JSON decode (called once per public WS text frame, for two venues), so no secondary
/// `HashSet<(venue, symbol)>` is added.
///
/// Matched on the input's own Source ID, not its name: several IDs may share a name, and a trade
/// built here is stamped with this one.
pub fn instrument_known(instruments: &InstrumentSnapshot, source_id: u16, symbol: &str) -> bool {
    crate::model::lock(instruments)
        .values()
        .any(|i| i.source_id == source_id && i.symbol.as_ref() == symbol)
}

/// What [`resolve_instrument`] reads off a catalog entry: the identity a message built at this seam
/// must carry, plus the universe it belongs to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedInstrument {
    pub channel: u8,
    pub instrument_id: u32,
    /// The matched entry's `category` — the universe the edge row that defined it carries. One
    /// public input can span several universes under its one Source ID (Hyperliquid's native perps
    /// and the `xyz:` builder DEX both arrive as ID 1 on one public socket), so a constant would
    /// file every symbol under one of them and history would drop the rest as unattributable.
    pub category: Arc<str>,
}

/// Resolve the `(channel, instrument_id)` identity a trade built here must carry, from the same
/// shared catalog [`instrument_known`] gates on. The public JSON these inputs decode has no
/// channel/instrument_id of its own — only a bare symbol — so a `NormalizedTrade` built at this
/// seam needs this lookup to carry the identity `history::Key` groups trades on downstream; without
/// it every symbol from a given public input would collapse onto one history product the moment its
/// trade won a floor tick.
///
/// Same scan, same cost, same gate as `instrument_known` (`None` until the edge has revealed this
/// instrument at least once) — this only additionally reads off the matching entry's identity rather
/// than just its existence. A plain scan against the live `InstrumentSnapshot`, not a cached index, so
/// it can never drift out of step with an `upsert_instrument`/`remove_instrument` the way a
/// separately-maintained symbol→identity map would.
///
/// Scoped to the input's own Source ID for the same reason as [`instrument_known`]. Within it, first
/// match wins. Safe here because neither venue this module backstops presents one symbol under
/// two different `(channel, instrument_id)` pairs the way a price-aggregated venue's mirrored paths
/// do (see `history::Key`'s docs) — if a public backstop ever grew a mirrored-path venue of its own,
/// this would need the same disambiguation `products::resolve` applies instead of a bare first match.
pub fn resolve_instrument(
    instruments: &InstrumentSnapshot,
    source_id: u16,
    symbol: &str,
) -> Option<ResolvedInstrument> {
    crate::model::lock(instruments)
        .values()
        .find(|i| i.source_id == source_id && i.symbol.as_ref() == symbol)
        .map(|i| ResolvedInstrument {
            channel: i.channel,
            instrument_id: i.instrument_id,
            category: i.category.clone(),
        })
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

    fn catalog(entries: &[(u16, &str, u32)]) -> InstrumentSnapshot {
        let mut map = std::collections::HashMap::new();
        for &(source_id, symbol, instrument_id) in entries {
            let i = crate::model::NormalizedInstrument {
                tick_size: 0,
                asset_class: 0,
                venue: "SHARED".into(),
                source_name: "SHARED".into(),
                source_id,
                symbol: symbol.into(),
                channel: 0,
                instrument_id,
                category: "c".into(),
                price_exponent: -2,
                qty_exponent: -2,
            };
            map.insert(
                (
                    crate::model::SourceKey::of(&i),
                    i.category.clone(),
                    0,
                    instrument_id,
                ),
                i,
            );
        }
        std::sync::Arc::new(std::sync::Mutex::new(map))
    }

    /// A backstop stamps one fixed Source ID, so it must only match that ID's instruments: another
    /// ID under the same name is a different engine.
    #[test]
    fn lookups_are_scoped_to_the_inputs_own_source_id() {
        let snap = catalog(&[(4, "A", 1), (10, "dex:A", 2)]);
        assert!(instrument_known(&snap, 4, "A"));
        assert!(!instrument_known(&snap, 4, "dex:A"));
        assert_eq!(
            resolve_instrument(&snap, 10, "dex:A").map(|r| (r.channel, r.instrument_id)),
            Some((0, 2))
        );
        assert_eq!(resolve_instrument(&snap, 4, "dex:A"), None);
    }

    fn subs(names: &[&str], kinds: &[Kind]) -> Vec<Subscription> {
        names
            .iter()
            .flat_map(|n| {
                kinds.iter().map(move |&kind| Subscription {
                    symbol: n.to_string(),
                    kind,
                    frame: format!("{n}/{}", kind.label()),
                })
            })
            .collect()
    }

    fn ack(symbol: &str, kind: Kind) -> Frame {
        Frame::Ack {
            symbol: symbol.into(),
            kind,
        }
    }

    fn data(symbol: &str, kind: Kind) -> Frame {
        Frame::Data {
            symbol: symbol.into(),
            kind,
        }
    }

    #[test]
    fn subscriptions_go_out_one_answer_at_a_time() {
        let now = Instant::now();
        let mut seq = Sequencer::new(subs(&["BTC", "ETH"], &[Kind::Quotes]), false, "V");
        seq.start_session();
        assert_eq!(seq.next_frame(now).as_deref(), Some("BTC/quotes"));
        assert_eq!(seq.next_frame(now), None, "BTC is still unanswered");
        seq.on_frame(ack("BTC", Kind::Quotes));
        assert_eq!(seq.next_frame(now).as_deref(), Some("ETH/quotes"));
        seq.on_frame(ack("ETH", Kind::Quotes));
        assert_eq!(seq.next_frame(now), None);
        assert_eq!(seq.states, [SubState::Live, SubState::Live]);
    }

    #[test]
    fn a_missing_answer_times_out_and_stays_pending() {
        let now = Instant::now();
        let mut seq = Sequencer::new(subs(&["BTC", "ETH"], &[Kind::Trades]), false, "V");
        seq.start_session();
        seq.next_frame(now);
        assert_eq!(seq.deadline(), Some(now + ACK_TIMEOUT));
        seq.on_timeout();
        assert_eq!(seq.next_frame(now).as_deref(), Some("ETH/trades"));
        assert_eq!(seq.states[0], SubState::Pending);
    }

    /// Where the venue replays earlier state on subscribe, the subscription counts only from the
    /// data frame after that replay.
    #[test]
    fn a_replaying_venue_counts_from_the_frame_after_the_ack() {
        let now = Instant::now();
        let mut seq = Sequencer::new(subs(&["BTC"], &[Kind::Quotes]), true, "V");
        seq.start_session();
        seq.next_frame(now);
        seq.on_frame(data("BTC", Kind::Quotes));
        assert_eq!(
            seq.states[0],
            SubState::Pending,
            "no data counts before the ack"
        );
        seq.on_frame(ack("BTC", Kind::Quotes));
        assert_eq!(seq.states[0], SubState::Acked);
        seq.on_frame(data("BTC", Kind::Quotes));
        assert_eq!(seq.states[0], SubState::Live);
    }

    /// A replay with nothing in it names no symbol, so it completes the subscription acked last,
    /// and the next batch counts rather than being taken for the replay.
    #[test]
    fn an_empty_replay_completes_the_subscription_acked_last() {
        let now = Instant::now();
        let mut seq = Sequencer::new(subs(&["BTC", "ETH"], &Kind::ALL), true, "V");
        seq.start_session();
        seq.next_frame(now);
        seq.on_frame(ack("BTC", Kind::Quotes));
        seq.on_frame(data("BTC", Kind::Quotes));
        seq.next_frame(now);
        seq.on_frame(ack("BTC", Kind::Trades));
        seq.on_frame(Frame::EmptyBatch { kind: Kind::Quotes });
        assert_eq!(seq.states[1], SubState::Acked, "not a replay of its kind");
        seq.on_frame(Frame::EmptyBatch { kind: Kind::Trades });
        assert_eq!(seq.states[..2], [SubState::Live, SubState::Live]);
        seq.next_frame(now);
        seq.on_frame(ack("ETH", Kind::Quotes));
        seq.on_frame(Frame::EmptyBatch { kind: Kind::Trades });
        assert_eq!(seq.states[2..], [SubState::Acked, SubState::Pending]);
    }

    #[test]
    fn a_session_end_returns_every_subscription_to_pending() {
        let now = Instant::now();
        let mut seq = Sequencer::new(subs(&["BTC"], &[Kind::Quotes]), false, "V");
        seq.start_session();
        seq.next_frame(now);
        seq.on_frame(ack("BTC", Kind::Quotes));
        seq.end_session(true);
        assert_eq!(seq.states[0], SubState::Pending);
        assert!(seq.strikes.is_empty(), "an answered name takes no strike");
        seq.start_session();
        assert_eq!(seq.next_frame(now).as_deref(), Some("BTC/quotes"));
    }

    /// The name in flight when a dropping venue ends the session takes a strike; a second refuses
    /// it, and it is never sent again. An ack in between clears the strike.
    #[test]
    fn two_drops_with_a_name_in_flight_refuse_it() {
        let now = Instant::now();
        let mut seq = Sequencer::new(subs(&["BTC", "NOPE"], &Kind::ALL), true, "V");
        let session = |seq: &mut Sequencer| {
            seq.start_session();
            let mut sent = Vec::new();
            while let Some(frame) = seq.next_frame(now) {
                if frame.starts_with("NOPE") {
                    sent.push(frame);
                    break; // the venue drops the connection here
                }
                let (name, kind) = frame.split_once('/').unwrap();
                let kind = if kind == "quotes" {
                    Kind::Quotes
                } else {
                    Kind::Trades
                };
                seq.on_frame(ack(name, kind));
                sent.push(frame);
            }
            seq.end_session(true);
            sent
        };
        assert_eq!(
            session(&mut seq),
            ["BTC/quotes", "BTC/trades", "NOPE/quotes"]
        );
        assert_eq!(
            seq.states[2],
            SubState::Pending,
            "one strike refuses nothing"
        );
        session(&mut seq);
        assert_eq!(seq.states[2..], [SubState::Refused, SubState::Refused]);
        assert_eq!(session(&mut seq), ["BTC/quotes", "BTC/trades"]);

        let mut seq = Sequencer::new(subs(&["BTC"], &[Kind::Quotes]), true, "V");
        seq.start_session();
        seq.next_frame(now);
        seq.end_session(true);
        seq.start_session();
        seq.next_frame(now);
        seq.on_frame(ack("BTC", Kind::Quotes));
        seq.end_session(true);
        seq.start_session();
        seq.next_frame(now);
        seq.end_session(true);
        assert_eq!(
            seq.states[0],
            SubState::Pending,
            "the ack cleared the strike"
        );
    }

    /// A venue that stops answering names nothing by dropping: a timeout spares every name the
    /// session's drop would otherwise strike.
    #[test]
    fn a_drop_after_a_timeout_strikes_nothing() {
        let now = Instant::now();
        let mut seq = Sequencer::new(subs(&["BTC", "ETH"], &[Kind::Quotes]), true, "V");
        for _ in 0..3 {
            seq.start_session();
            seq.next_frame(now);
            seq.on_timeout();
            seq.next_frame(now);
            seq.end_session(true);
        }
        assert!(seq.strikes.is_empty());
        assert_eq!(seq.states, [SubState::Pending, SubState::Pending]);
    }

    /// A venue that refuses explicitly takes no strike from a dropped connection, and its refusal
    /// covers every kind of the name.
    #[test]
    fn an_explicit_refusal_refuses_the_name_and_a_drop_does_not() {
        let now = Instant::now();
        let mut seq = Sequencer::new(subs(&["SOL", "NOPE"], &Kind::ALL), false, "V");
        for _ in 0..3 {
            seq.start_session();
            seq.next_frame(now);
            seq.end_session(false);
        }
        assert!(seq.states.iter().all(|s| *s == SubState::Pending));
        seq.start_session();
        seq.on_frame(Frame::Refused {
            symbol: "NOPE".into(),
        });
        assert_eq!(seq.states[2..], [SubState::Refused, SubState::Refused]);
    }

    #[test]
    fn distinct_keeps_the_first_of_each_name_in_order() {
        let names = ["kPEPE", "BTC", "", "kPEPE", "btc"]
            .map(String::from)
            .to_vec();
        assert_eq!(distinct(names), ["kPEPE", "BTC", "btc"]);
    }

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
