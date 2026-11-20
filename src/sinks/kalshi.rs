//! Kalshi-compatible WebSocket sink: Kalshi's event-contract books, quotes and prints in **Kalshi's
//! own trade-API WebSocket protocol** at `/trade-api/ws/v2`, so a Kalshi client reads edge-connect by
//! changing its WebSocket URL. Off by default (`--kalshi-ws-bind`).
//!
//! A rendering, like [`super::hyperliquid`]: nothing here belongs in PROTOCOL.md. Kalshi's documented
//! protocol is the authority and NautilusTrader 2.0.0rc9's Kalshi data client is the acceptance test.
//! Only Prediction Binary markets (`Asset Class` 2) at the publisher's −4/−2 fixed point are served;
//! Kalshi serves its perpetuals on another socket, at another scale.
//!
//! Kalshi's book lists YES bids and NO bids. Our bid at `p` is a YES bid at `p`, and our ask at `a` a
//! NO bid at `1 − a`, reported at `a` when the subscription sets `use_yes_price`. `orderbook_delta`
//! is relative, so the shared stage folds each logical book event and publishes its net per-level
//! changes under one `clock`, removals first so no intermediate book crosses. A replaced book is sent
//! as that difference too: Kalshi sends a snapshot only on subscribe and `get_snapshot`, and a client
//! applies no other.

use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque},
    fmt::Write as _,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::{Duration, Instant},
};

use anyhow::Result;
use futures_util::{SinkExt, StreamExt};
use serde::Serialize;
use serde_json::Value;
use tokio::{
    net::{TcpListener, TcpStream},
    sync::broadcast,
};
use tokio_tungstenite::{
    tungstenite::{
        handshake::server::{ErrorResponse, Request, Response},
        http::StatusCode,
        Message as WsMessage,
    },
    WebSocketStream,
};
use tracing::{info, warn};

use super::hyperliquid::{
    finish_close, json, ms_or_now, ClientGuard, CLOSE_HANDSHAKE_TIMEOUT, HANDSHAKE_TIMEOUT,
    IDLE_TIMEOUT, MAX_CLIENTS, PREPARED_CAPACITY,
};
use crate::{
    ingest::sources::KALSHI_SOURCE_ID,
    metrics::metrics,
    model::{
        lock, now_mono_ns, now_ns, BookAccumulator, BookAction, BookChange, BookKey, BookSide,
        BookSnapshot, FeedMessage, InstrumentSnapshot, NormalizedBook, NormalizedInstrument,
        NormalizedQuote, NormalizedTrade, Side, SourceKey, MAX_PENDING_CHANGES,
    },
};

const PATH: &str = "/trade-api/ws/v2";
/// Kalshi's keep-alive: a Ping with body `heartbeat` every 10s, the first 10s after connecting.
const HEARTBEAT: Duration = Duration::from_secs(10);
const ACCEPT_BACKOFF: Duration = Duration::from_secs(1);

const PREDICTION_BINARY: u8 = 2;
/// The publisher's event-contract fixed point: $1 is `10_000` and one contract `100`.
const PRICE_EXPONENT: i8 = -4;
const QTY_EXPONENT: i8 = -2;
const ONE_DOLLAR: i64 = 10_000;

/// Markets the stage tracks: the arbiter's own `MAX_BOOK_MARKETS`.
const MAX_MARKETS: usize = 16_384;
/// How long the followed host may be silent while the other publishes before the book follows it.
const FAILOVER_NS: u64 = 2_000_000_000;
/// Tickers in one subscription; past it a command is refused with Kalshi's code 26.
const MAX_TICKERS: usize = 4_096;
/// The v3 `Symbol` field's width, so no longer ticker names a market here.
const MAX_TICKER_LEN: usize = 64;
/// An inbound frame: room for a command naming `MAX_TICKERS` of the longest tickers.
const MAX_FRAME: usize = MAX_TICKERS * (MAX_TICKER_LEN + 3) + 4_096;
/// Commands per connection per minute, `get_snapshot` aside: it is charged per market named, up to
/// `MAX_TICKERS`, so a client can recover every book it holds after a gap.
const MAX_COMMANDS_PER_MIN: u32 = 600;

/// What the feed cannot supply: no market UUID crosses the publisher.
const NIL_UUID: &str = "00000000-0000-0000-0000-000000000000";

const NOT_FOUND: &str = "not found: connect to /trade-api/ws/v2\n";
const PERPS_NOT_SERVED: &str =
    "not found: Kalshi perpetuals are not served; event contracts are at /trade-api/ws/v2\n";

// --- the fixed point ---

/// `raw` at −4 as Kalshi's dollar string, `"0.5600"`.
fn dollars(raw: i64) -> String {
    format!("{}.{:04}", raw / ONE_DOLLAR, raw % ONE_DOLLAR)
}

/// `raw` at −2 as Kalshi's signed fixed-point count, `"-54.00"`.
fn count(raw: i64) -> String {
    let sign = if raw < 0 { "-" } else { "" };
    let abs = raw.unsigned_abs();
    format!("{sign}{}.{:02}", abs / 100, abs % 100)
}

/// A scaled value back at its fixed point. Exact here: the decoder's `apply_exponent` is one rounding
/// of the wire integer, far inside half a unit at these magnitudes.
fn raw(v: f64, exponent: i8) -> Option<i64> {
    let x = (v * 10f64.powi(-i32::from(exponent))).round();
    (x.is_finite() && x.abs() < 1e15).then_some(x as i64)
}

/// Strictly inside `(0, 1)`: the only prices a binary contract rests or trades at.
fn quotable(price: i64) -> bool {
    (1..ONE_DOLLAR).contains(&price)
}

/// Our ask at `a` as Kalshi reports the NO bid it is: at `1 − a`, or at `a` under `use_yes_price`.
fn no_price(ask: i64, use_yes_price: bool) -> i64 {
    if use_yes_price {
        ask
    } else {
        ONE_DOLLAR - ask
    }
}

/// Our 64-bit trade id in the UUID form Kalshi's `trade_id` takes.
fn trade_uuid(id: u64) -> String {
    format!(
        "00000000-0000-0000-{:04x}-{:012x}",
        id >> 48,
        id & 0xFFFF_FFFF_FFFF
    )
}

/// Milliseconds since the epoch as RFC 3339 UTC, for the deprecated `time` field.
fn rfc3339(ms: u64) -> String {
    // Howard Hinnant's `civil_from_days`.
    let z = (ms / 86_400_000) as i64 + 719_468;
    let (era, doe) = (z.div_euclid(146_097), z.rem_euclid(146_097));
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = yoe + era * 400 + i64::from(m <= 2);
    let s = ms % 86_400_000 / 1_000;
    format!(
        "{y:04}-{m:02}-{d:02}T{:02}:{:02}:{:02}.{:03}Z",
        s / 3_600,
        s / 60 % 60,
        s % 60,
        ms % 1_000
    )
}

// --- the wire shapes ---

#[derive(Serialize)]
struct SnapshotMsg<'a> {
    market_ticker: &'a str,
    market_id: &'static str,
    clock: u64,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    yes_dollars_fp: Vec<[String; 2]>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    no_dollars_fp: Vec<[String; 2]>,
}

#[derive(Serialize)]
struct DeltaMsg<'a> {
    market_ticker: &'a str,
    market_id: &'static str,
    clock: u64,
    price_dollars: String,
    delta_fp: String,
    side: &'static str,
    ts_ms: u64,
}

/// Volume and open interest are not on the feed, so they are zero.
#[derive(Serialize)]
struct TickerMsg<'a> {
    market_id: &'static str,
    market_ticker: &'a str,
    price_dollars: String,
    yes_bid_dollars: String,
    yes_ask_dollars: String,
    volume_fp: &'static str,
    open_interest_fp: &'static str,
    dollar_volume: i64,
    dollar_open_interest: i64,
    yes_bid_size_fp: String,
    yes_ask_size_fp: String,
    last_trade_size_fp: String,
    ts: u64,
    ts_ms: u64,
    time: String,
}

#[derive(Serialize)]
struct TradeMsg<'a> {
    trade_id: String,
    market_ticker: &'a str,
    yes_price_dollars: String,
    no_price_dollars: String,
    count_fp: String,
    taker_side: &'static str,
    taker_outcome_side: &'static str,
    taker_book_side: &'static str,
    is_block_trade: bool,
    ts: u64,
    ts_ms: u64,
}

#[derive(Serialize)]
struct ErrorMsg<'a> {
    code: u32,
    msg: &'a str,
    #[serde(skip_serializing_if = "<[_]>::is_empty")]
    market_tickers: &'a [Arc<str>],
}

/// One frame: `msg` is already JSON, so only numbers and a static type are spliced around it.
fn frame(
    kind: &'static str,
    id: Option<u64>,
    sid: Option<u64>,
    seq: Option<u64>,
    msg: Option<&str>,
) -> String {
    let mut f = format!(r#"{{"type":"{kind}""#);
    for (name, v) in [("id", id), ("sid", sid), ("seq", seq)] {
        if let Some(v) = v {
            let _ = write!(f, r#","{name}":{v}"#);
        }
    }
    let _ = write!(f, r#","sending_ts_ms":{}"#, now_ns() / 1_000_000);
    if let Some(msg) = msg {
        let _ = write!(f, r#","msg":{msg}"#);
    }
    f.push('}');
    f
}

/// One outbound frame and the `type` it counts under.
type Out = (&'static str, String);

fn error(id: Option<u64>, sid: Option<u64>, refusal: Refusal) -> Out {
    let (code, msg, tickers) = refusal;
    let msg = json(ErrorMsg {
        code,
        msg,
        market_tickers: &tickers,
    });
    ("error", frame("error", id, sid, None, Some(&msg)))
}

/// A Kalshi error code, its message, and the tickers it is about.
type Refusal = (u32, &'static str, Vec<Arc<str>>);

fn refusal(code: u32, msg: &'static str) -> Refusal {
    (code, msg, Vec::new())
}

// --- the shared stage ---

/// One logical event's net change to one level, at −4/−2.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Delta {
    bid: bool,
    price: i64,
    delta: i64,
}

/// One market's book as every client holds it: YES bids, and our asks at YES-leg prices.
struct Book {
    key: BookKey,
    /// `[bids, asks]`, price to size. Empty while `!complete`.
    levels: [BTreeMap<i64, i64>; 2],
    complete: bool,
    /// The open event's changes, folded at its `last`.
    pending: Vec<BookChange>,
    /// The next close re-reads the replay entry instead of folding `pending`, whose event lost
    /// batches or came from another key.
    resync: bool,
    /// When the followed key last published, on the monotonic clock.
    seen_ns: u64,
}

/// A quote at −4/−2. An empty bid is `0` and an empty ask `1.0000`, each at size `0`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Quote {
    bid: i64,
    bid_size: i64,
    ask: i64,
    ask_size: i64,
    ts_ms: u64,
}

/// What `send_initial_snapshot` replays, and the ticker's last print.
#[derive(Default)]
struct Top {
    quote: Option<Quote>,
    print: Option<(i64, i64)>,
}

/// The sink's own view of the feed, written only by [`prepare_loop`] and copied by clients under its
/// own mutex, never the arbiter's.
#[derive(Default)]
struct State {
    /// The last clock issued, across markets: Kalshi's clocks need only rise per market.
    clock: u64,
    books: HashMap<Arc<str>, Book>,
    /// `books` in the order they were created, oldest first.
    order: VecDeque<Arc<str>>,
    /// Messages queued behind the last re-read of a replay entry, which may already hold them:
    /// until they are through, a close re-reads rather than fold them again and walk levels back.
    owed: usize,
    /// Whether the message being folded is one of `owed`.
    catching_up: bool,
    /// Whether folding it re-read a replay entry.
    reread: bool,
    tops: HashMap<Arc<str>, Top>,
    /// Whether each market key passes the definition gate. A cache, cleared when full.
    gates: HashMap<BookKey, bool>,
    /// Tickers whose definition fails the gate, the perpetuals among them.
    refused: HashSet<Arc<str>>,
}

/// One broadcast message's frames for every client, before each client's `sid` and `seq`.
enum Prepared {
    /// One logical book event, removals first.
    Book {
        ticker: Arc<str>,
        clock: u64,
        deltas: Vec<Delta>,
        ts_ms: u64,
    },
    Ticker {
        ticker: Arc<str>,
        msg: String,
    },
    Trade {
        ticker: Arc<str>,
        msg: String,
    },
    /// The stage missed messages: the prints among them are gone, so trade subscriptions skip a `seq`.
    Gap,
}

fn dropped(reason: &'static str) {
    metrics()
        .kalshi_sink_dropped
        .with_label_values(&[reason])
        .inc();
}

fn serves(i: &NormalizedInstrument) -> bool {
    i.asset_class == PREDICTION_BINARY
        && i.price_exponent == PRICE_EXPONENT
        && i.qty_exponent == QTY_EXPONENT
}

fn key_of(source: SourceKey, category: &Arc<str>, channel: u8, instrument_id: u32) -> BookKey {
    (source, category.clone(), channel, instrument_id)
}

/// Whether the replay entry is a whole book of `ticker`. Served by ticker but read by key, so the
/// symbol is checked: a key is wire-supplied, and nothing else ties it to this market.
fn replay_whole(acc: &BookAccumulator, ticker: &str) -> bool {
    acc.baselined() && !acc.is_order_level() && acc.symbol().as_ref() == ticker
}

/// The replay entry's committed levels, or `None` when it is not a whole book of `ticker`.
fn replay_levels(
    replay: &BookSnapshot,
    key: &BookKey,
    ticker: &str,
) -> Option<[BTreeMap<i64, i64>; 2]> {
    let (bids, asks, ..) = lock(replay)
        .get(key)
        .filter(|acc| replay_whole(acc, ticker))?
        .top_levels(usize::MAX);
    let side = |levels: Vec<(f64, f64)>| {
        levels
            .into_iter()
            .filter_map(|(p, s)| {
                let (p, s) = (raw(p, PRICE_EXPONENT)?, raw(s, QTY_EXPONENT)?);
                (quotable(p) && s > 0).then_some((p, s))
            })
            .collect()
    };
    Some([side(bids), side(asks)])
}

impl Book {
    fn new(key: BookKey) -> Self {
        Self {
            key,
            levels: Default::default(),
            complete: false,
            pending: Vec::new(),
            resync: true,
            seen_ns: 0,
        }
    }

    /// Every resting level, as `(ask, price)`.
    fn prices(&self) -> Vec<(bool, i64)> {
        [false, true]
            .into_iter()
            .flat_map(|ask| self.levels[usize::from(ask)].keys().map(move |&p| (ask, p)))
            .collect()
    }

    /// Set one level, remembering its size before this event.
    fn set(&mut self, ask: bool, price: i64, size: i64, touched: &mut BTreeMap<(bool, i64), i64>) {
        let side = &mut self.levels[usize::from(ask)];
        let old = if size > 0 {
            side.insert(price, size)
        } else {
            side.remove(&price)
        };
        touched.entry((ask, price)).or_insert(old.unwrap_or(0));
    }

    fn fold(&mut self, c: &BookChange, touched: &mut BTreeMap<(bool, i64), i64>) {
        let sides: &[bool] = match c.side {
            BookSide::Bid => &[false],
            BookSide::Ask => &[true],
            BookSide::Both => &[false, true],
        };
        if c.action == BookAction::Clear {
            for (ask, price) in self.prices() {
                if sides.contains(&ask) {
                    self.set(ask, price, 0, touched);
                }
            }
            return;
        }
        let (Some(price), Some(size), [ask]) = (
            raw(c.price, PRICE_EXPONENT),
            raw(c.size, QTY_EXPONENT),
            sides,
        ) else {
            return;
        };
        if !quotable(price) {
            dropped("out_of_range");
            return;
        }
        let size = if c.action == BookAction::Delete {
            0
        } else {
            size.max(0)
        };
        self.set(*ask, price, size, touched);
    }

    /// Close one logical event and return its net changes, removals first, then resizes, then new
    /// levels: the book in between is a subset of the one before or the one after, so it never
    /// crosses unless one of them does.
    fn close(&mut self, ticker: &str, replay: &BookSnapshot, catching_up: bool) -> Vec<Delta> {
        let pending = std::mem::take(&mut self.pending);
        let cleared = |side| {
            pending.iter().any(|c| {
                c.action == BookAction::Clear && (c.side == side || c.side == BookSide::Both)
            })
        };
        let mut touched = BTreeMap::new();
        if self.resync || catching_up {
            self.resync = false;
            let target = replay_levels(replay, &self.key, ticker);
            self.complete = target.is_some();
            let target = target.unwrap_or_default();
            for (ask, price) in self.prices() {
                if !target[usize::from(ask)].contains_key(&price) {
                    self.set(ask, price, 0, &mut touched);
                }
            }
            for (ask, side) in [(false, &target[0]), (true, &target[1])] {
                for (&price, &size) in side {
                    self.set(ask, price, size, &mut touched);
                }
            }
        } else if cleared(BookSide::Bid) && cleared(BookSide::Ask) {
            // A re-baseline carries the whole book, unless it is the arbiter's degraded one: a bare
            // clear over a replay entry that is not whole, followed by only what moves after it.
            self.complete = lock(replay)
                .get(&self.key)
                .is_some_and(|acc| replay_whole(acc, ticker));
            if self.complete {
                for c in &pending {
                    self.fold(c, &mut touched);
                }
            } else {
                for (ask, price) in self.prices() {
                    self.set(ask, price, 0, &mut touched);
                }
            }
        } else if self.complete {
            for c in &pending {
                self.fold(c, &mut touched);
            }
        }
        let mut deltas: Vec<(u8, Delta)> = touched
            .into_iter()
            .filter_map(|((ask, price), old)| {
                let new = self.levels[usize::from(ask)]
                    .get(&price)
                    .copied()
                    .unwrap_or(0);
                let rank = match (old, new) {
                    _ if old == new => return None,
                    (_, 0) => 0,
                    (0, _) => 2,
                    _ => 1,
                };
                Some((
                    rank,
                    Delta {
                        bid: !ask,
                        price,
                        delta: new - old,
                    },
                ))
            })
            .collect();
        deltas.sort_by_key(|(rank, _)| *rank);
        deltas.into_iter().map(|(_, d)| d).collect()
    }
}

impl State {
    /// Whether `key`'s definition passes the gate, or `None` while there is none to read.
    fn gate(
        &mut self,
        key: &BookKey,
        symbol: &Arc<str>,
        instruments: &InstrumentSnapshot,
    ) -> Option<bool> {
        if let Some(&served) = self.gates.get(key) {
            return Some(served);
        }
        let served = lock(instruments).get(key).map(serves)?;
        self.learn(key, symbol, served);
        Some(served)
    }

    fn learn(&mut self, key: &BookKey, symbol: &Arc<str>, served: bool) {
        if self.gates.len() >= 4 * MAX_MARKETS {
            self.gates.clear();
        }
        self.gates.insert(key.clone(), served);
        if served {
            self.refused.remove(symbol);
        } else if self.refused.len() < MAX_MARKETS {
            self.refused.insert(symbol.clone());
        }
    }

    fn top(&mut self, ticker: &Arc<str>) -> &mut Top {
        if self.tops.len() >= MAX_MARKETS && !self.tops.contains_key(ticker) {
            if let Some(any) = self.tops.keys().next().cloned() {
                self.tops.remove(&any);
            }
        }
        self.tops.entry(ticker.clone()).or_default()
    }

    fn publish(&mut self, ticker: &Arc<str>, deltas: Vec<Delta>, ts_ms: u64) -> Option<Prepared> {
        if deltas.is_empty() {
            return None;
        }
        self.clock += 1;
        Some(Prepared::Book {
            ticker: ticker.clone(),
            clock: self.clock,
            deltas,
            ts_ms,
        })
    }

    /// Fold one broadcast message, or re-read every book after a gap (`None`). `queued` counts the
    /// messages behind it, and is called after the replay reads: the arbiter writes there before it
    /// broadcasts, so a count taken first misses a commit landing in between. A batch written but
    /// not yet sent is its entry's newest, so folding it again is a no-op.
    fn next(
        &mut self,
        msg: Option<&FeedMessage>,
        replay: &BookSnapshot,
        instruments: &InstrumentSnapshot,
        queued: impl FnOnce() -> usize,
    ) -> Vec<Prepared> {
        let out = match msg {
            Some(m) => {
                self.catching_up = self.owed > 0;
                self.owed = self.owed.saturating_sub(1);
                self.prepare(m, replay, instruments)
            }
            None => self.resync_all(replay),
        };
        if std::mem::take(&mut self.reread) {
            self.owed = self.owed.max(queued());
        }
        out
    }

    /// Fold one broadcast message, returning what clients are owed.
    fn prepare(
        &mut self,
        msg: &FeedMessage,
        replay: &BookSnapshot,
        instruments: &InstrumentSnapshot,
    ) -> Vec<Prepared> {
        if msg.source_id() != KALSHI_SOURCE_ID {
            return Vec::new();
        }
        match msg {
            FeedMessage::Instrument(i) => {
                let key = key_of(SourceKey::of(i), &i.category, i.channel, i.instrument_id);
                self.learn(&key, &i.symbol, serves(i));
                Vec::new()
            }
            FeedMessage::Book(b) => self.book(b, replay, instruments),
            FeedMessage::Quote(q) => self.quote(q, instruments).into_iter().collect(),
            FeedMessage::Trade(t) => self.trade(t, instruments).into_iter().collect(),
            _ => Vec::new(),
        }
    }

    fn book(
        &mut self,
        b: &NormalizedBook,
        replay: &BookSnapshot,
        instruments: &InstrumentSnapshot,
    ) -> Vec<Prepared> {
        let key = key_of(SourceKey::of(b), &b.category, b.channel, b.instrument_id);
        if b.order_level || self.gate(&key, &b.symbol, instruments) != Some(true) {
            return Vec::new();
        }
        let mut out = Vec::new();
        if !self.books.contains_key(&b.symbol) {
            if self.books.len() >= MAX_MARKETS {
                out.extend(self.evict_oldest());
            }
            self.order.push_back(b.symbol.clone());
            self.books.insert(b.symbol.clone(), Book::new(key.clone()));
        }
        let catching_up = self.catching_up;
        let Some(book) = self.books.get_mut(&b.symbol) else {
            return out;
        };
        let now = now_mono_ns();
        if book.key != key {
            // Each host numbers instruments itself, so the other host's copy of this market is
            // another key: followed once ours stops being whole or falls silent.
            if book.complete && now.saturating_sub(book.seen_ns) < FAILOVER_NS {
                return out;
            }
            book.key = key;
            book.pending.clear();
            book.resync = true;
        }
        book.seen_ns = now;
        book.pending.extend_from_slice(&b.changes);
        if !b.last {
            // The replay entry abandons an event this long too, and its republish arrives as a
            // re-baseline; until then this one re-reads rather than fold half an event.
            if book.pending.len() > MAX_PENDING_CHANGES {
                book.pending.clear();
                book.resync = true;
            }
            return out;
        }
        self.reread |= book.resync || catching_up;
        let deltas = book.close(&b.symbol, replay, catching_up);
        out.extend(self.publish(&b.symbol, deltas, ms_or_now(b.source_ts_ns)));
        out
    }

    /// Make room by dropping the oldest market, emptied for its clients, as the arbiter evicts its own.
    /// A live market comes back whole with its next batch.
    fn evict_oldest(&mut self) -> Option<Prepared> {
        let ticker = self.order.pop_front()?;
        let book = self.books.get(&ticker)?;
        dropped("evicted");
        let deltas = book
            .prices()
            .into_iter()
            .map(|(ask, price)| Delta {
                bid: !ask,
                price,
                delta: -book.levels[usize::from(ask)][&price],
            })
            .collect();
        let p = self.publish(&ticker, deltas, now_ns() / 1_000_000);
        self.books.remove(&ticker);
        p
    }

    /// After a gap in the broadcast: re-read every book now, and again at its next close, since the
    /// open event lost batches.
    fn resync_all(&mut self, replay: &BookSnapshot) -> Vec<Prepared> {
        let ts_ms = now_ns() / 1_000_000;
        let tickers: Vec<Arc<str>> = self.books.keys().cloned().collect();
        let mut out = Vec::new();
        for ticker in tickers {
            let Some(book) = self.books.get_mut(&ticker) else {
                continue;
            };
            book.pending.clear();
            let deltas = book.close(&ticker, replay, true);
            book.resync = true;
            out.extend(self.publish(&ticker, deltas, ts_ms));
        }
        self.reread = true;
        out.push(Prepared::Gap);
        out
    }

    fn quote(&mut self, q: &NormalizedQuote, instruments: &InstrumentSnapshot) -> Option<Prepared> {
        let key = key_of(SourceKey::of(q), &q.category, q.channel, q.instrument_id);
        let w = &q.wire;
        if self.gate(&key, &q.symbol, instruments) != Some(true)
            || (w.price_exponent, w.qty_exponent) != (PRICE_EXPONENT, QTY_EXPONENT)
        {
            return None;
        }
        let (bid_size, ask_size) = (
            i64::try_from(w.bid_qty).ok()?,
            i64::try_from(w.ask_qty).ok()?,
        );
        if (bid_size > 0 && !quotable(w.bid_price)) || (ask_size > 0 && !quotable(w.ask_price)) {
            dropped("out_of_range");
            return None;
        }
        let quote = Quote {
            bid: if bid_size > 0 { w.bid_price } else { 0 },
            bid_size,
            ask: if ask_size > 0 {
                w.ask_price
            } else {
                ONE_DOLLAR
            },
            ask_size,
            ts_ms: ms_or_now(q.source_ts_ns),
        };
        let top = self.top(&q.symbol);
        top.quote = Some(quote);
        let msg = ticker_msg(&q.symbol, &quote, top.print);
        Some(Prepared::Ticker {
            ticker: q.symbol.clone(),
            msg,
        })
    }

    fn trade(&mut self, t: &NormalizedTrade, instruments: &InstrumentSnapshot) -> Option<Prepared> {
        let key = key_of(SourceKey::of(t), &t.category, t.channel, t.instrument_id);
        if self.gate(&key, &t.symbol, instruments) != Some(true) {
            return None;
        }
        // Kalshi's schema has no "unknown" taker, and the side is what a consumer reads.
        let (outcome, book_side) = match t.aggressor_side {
            Side::Buy => ("yes", "bid"),
            Side::Sell => ("no", "ask"),
            Side::Unknown => {
                dropped("unknown_aggressor");
                return None;
            }
        };
        let (price, size) = (raw(t.price, PRICE_EXPONENT)?, raw(t.size, QTY_EXPONENT)?);
        if !quotable(price) || size <= 0 {
            dropped("out_of_range");
            return None;
        }
        self.top(&t.symbol).print = Some((price, size));
        let ms = ms_or_now(t.source_ts_ns);
        let msg = json(TradeMsg {
            trade_id: trade_uuid(t.trade_id),
            market_ticker: &t.symbol,
            yes_price_dollars: dollars(price),
            no_price_dollars: dollars(ONE_DOLLAR - price),
            count_fp: count(size),
            taker_side: outcome,
            taker_outcome_side: outcome,
            taker_book_side: book_side,
            is_block_trade: false,
            ts: ms / 1_000,
            ts_ms: ms,
        });
        Some(Prepared::Trade {
            ticker: t.symbol.clone(),
            msg,
        })
    }
}

fn ticker_msg(ticker: &str, q: &Quote, print: Option<(i64, i64)>) -> String {
    let (last, last_size) = print.unwrap_or((0, 0));
    json(TickerMsg {
        market_id: NIL_UUID,
        market_ticker: ticker,
        price_dollars: dollars(last),
        yes_bid_dollars: dollars(q.bid),
        yes_ask_dollars: dollars(q.ask),
        volume_fp: "0.00",
        open_interest_fp: "0.00",
        dollar_volume: 0,
        dollar_open_interest: 0,
        yes_bid_size_fp: count(q.bid_size),
        yes_ask_size_fp: count(q.ask_size),
        last_trade_size_fp: count(last_size),
        ts: q.ts_ms / 1_000,
        ts_ms: q.ts_ms,
        time: rfc3339(q.ts_ms),
    })
}

/// The one task that writes [`State`]. Clients skip a queued book change at or below the `clock`
/// of the snapshot they were sent, so committing before broadcasting can neither lose nor repeat one.
async fn prepare_loop(
    mut backbone: broadcast::Receiver<Arc<FeedMessage>>,
    out: broadcast::Sender<Arc<Prepared>>,
    state: Arc<Mutex<State>>,
    replay: BookSnapshot,
    instruments: InstrumentSnapshot,
) {
    loop {
        let prepared = match backbone.recv().await {
            Ok(m) => lock(&state).next(Some(&m), &replay, &instruments, || backbone.len()),
            Err(broadcast::error::RecvError::Lagged(n)) => {
                warn!("kalshi sink prepare stage lagged, dropped {n}");
                dropped("prepare_lagged");
                lock(&state).next(None, &replay, &instruments, || backbone.len())
            }
            Err(broadcast::error::RecvError::Closed) => break,
        };
        if out.receiver_count() > 0 {
            for p in prepared {
                let _ = out.send(Arc::new(p));
            }
        }
    }
}

// --- one connection ---

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Channel {
    Book,
    Ticker,
    Trade,
    Lifecycle,
}

impl Channel {
    fn name(self) -> &'static str {
        match self {
            Self::Book => "orderbook_delta",
            Self::Ticker => "ticker",
            Self::Trade => "trade",
            Self::Lifecycle => "market_lifecycle_v2",
        }
    }

    /// The channel, or how Kalshi refuses it: code 9 for a channel scoped to an account, which this
    /// connection has none of, and 8 for one this sink does not carry.
    fn parse(name: &str) -> Result<Self, Refusal> {
        Ok(match name {
            "orderbook_delta" => Self::Book,
            "ticker" => Self::Ticker,
            "trade" => Self::Trade,
            "market_lifecycle_v2" => Self::Lifecycle,
            "fill"
            | "market_positions"
            | "user_orders"
            | "order_group_updates"
            | "communications" => {
                return Err(refusal(
                    9,
                    "Authentication required: this feed carries no account data",
                ))
            }
            _ => return Err(refusal(8, "Unknown channel name")),
        })
    }
}

struct Sub {
    channel: Channel,
    sid: u64,
    seq: u64,
    /// `None` is every market, which only `ticker` and `trade` allow.
    tickers: Option<BTreeSet<Arc<str>>>,
    use_yes_price: bool,
}

impl Sub {
    fn covers(&self, ticker: &str) -> bool {
        self.tickers.as_ref().is_none_or(|t| t.contains(ticker))
    }

    fn next_seq(&mut self) -> u64 {
        self.seq += 1;
        self.seq
    }
}

/// One connection's subscriptions, at most one per channel.
#[derive(Default)]
struct Session {
    subs: Vec<Sub>,
    last_sid: u64,
    /// The `clock` of the snapshot each subscribed book was last sent; its queued changes at or below
    /// that clock are already in it.
    watermarks: HashMap<Arc<str>, u64>,
}

/// A command's `market_ticker`/`market_tickers`, deduplicated in order, or `None` if it names none.
fn tickers(params: &Value) -> Result<Option<Vec<Arc<str>>>, Refusal> {
    let mut named: Vec<&Value> = params.get("market_ticker").into_iter().collect();
    match params.get("market_tickers") {
        Some(Value::Array(list)) => named.extend(list),
        Some(_) => return Err(refusal(11, "market_tickers must be an array of strings")),
        None => {}
    }
    if named.is_empty() {
        return Ok(None);
    }
    let mut out: Vec<Arc<str>> = Vec::new();
    for t in named {
        match t.as_str() {
            Some(t) if !t.is_empty() && t.len() <= MAX_TICKER_LEN => {
                if !out.iter().any(|o| o.as_ref() == t) {
                    out.push(t.into());
                }
            }
            _ => return Err(refusal(11, "Invalid market ticker")),
        }
    }
    if out.len() > MAX_TICKERS {
        return Err(refusal(26, "Subscription market limit exceeded"));
    }
    Ok(Some(out))
}

/// A count per fixed minute.
struct Budget {
    start: Instant,
    used: u32,
    cap: u32,
}

impl Budget {
    fn new(cap: u32) -> Self {
        Self {
            start: Instant::now(),
            used: 0,
            cap,
        }
    }

    fn take(&mut self, n: u32) -> bool {
        if self.start.elapsed() >= Duration::from_secs(60) {
            self.start = Instant::now();
            self.used = 0;
        }
        self.used = self.used.saturating_add(n);
        self.used <= self.cap
    }
}

/// How many markets a `get_snapshot` command names, or `None` for any other frame.
fn snapshot_markets(text: &str) -> Option<u32> {
    let cmd: Value = serde_json::from_str(text).ok()?;
    let params = cmd.get("params")?;
    let named = |t: Option<Vec<Arc<str>>>| t.map_or(1, |t| t.len() as u32);
    (cmd.get("cmd")? == "update_subscription" && params.get("action")? == "get_snapshot")
        .then(|| tickers(params).map_or(1, named))
}

impl Session {
    fn sub_mut(&mut self, channel: Channel) -> Option<&mut Sub> {
        self.subs.iter_mut().find(|s| s.channel == channel)
    }

    /// Answer one command frame.
    fn command(&mut self, text: &str, state: &Mutex<State>) -> Vec<Out> {
        let Ok(cmd) = serde_json::from_str::<Value>(text) else {
            return vec![error(None, None, refusal(1, "Unable to process message"))];
        };
        let id = cmd.get("id").and_then(Value::as_u64).filter(|&id| id != 0);
        let params = cmd.get("params");
        let result = match (cmd.get("cmd").and_then(Value::as_str), params) {
            (Some("list_subscriptions"), _) => Ok(self.list(id)),
            (Some("subscribe" | "unsubscribe" | "update_subscription"), None) => {
                Err((None, refusal(2, "Params required")))
            }
            (Some("subscribe"), Some(p)) => self.subscribe(id, p, state),
            (Some("unsubscribe"), Some(p)) => self.unsubscribe(id, p),
            (Some("update_subscription"), Some(p)) => self.update(id, p, state),
            _ => Err((None, refusal(5, "Unknown command"))),
        };
        result.unwrap_or_else(|(sid, r)| vec![error(id, sid, r)])
    }

    fn list(&self, id: Option<u64>) -> Vec<Out> {
        #[derive(Serialize)]
        struct Listed {
            channel: &'static str,
            sid: u64,
        }
        let msg = json(
            self.subs
                .iter()
                .map(|s| Listed {
                    channel: s.channel.name(),
                    sid: s.sid,
                })
                .collect::<Vec<_>>(),
        );
        vec![("ok", frame("ok", id, None, None, Some(&msg)))]
    }

    /// Refuse tickers whose definition this sink will not serve, the perpetuals among them.
    fn refused(tickers: &[Arc<str>], state: &Mutex<State>) -> Result<(), Refusal> {
        let st = lock(state);
        let bad: Vec<Arc<str>> = tickers
            .iter()
            .filter(|t| st.refused.contains(*t))
            .cloned()
            .collect();
        if bad.is_empty() {
            Ok(())
        } else {
            Err((
                11,
                "Market not served: only Kalshi event contracts are",
                bad,
            ))
        }
    }

    fn subscribe(
        &mut self,
        id: Option<u64>,
        params: &Value,
        state: &Mutex<State>,
    ) -> Result<Vec<Out>, (Option<u64>, Refusal)> {
        let names: Vec<&str> = params
            .get("channels")
            .and_then(Value::as_array)
            .map(|c| c.iter().filter_map(Value::as_str).collect())
            .unwrap_or_default();
        if names.is_empty() {
            return Err((None, refusal(3, "Channels required")));
        }
        let tickers = tickers(params).map_err(|r| (None, r))?;
        let flag = |name| params.get(name).and_then(Value::as_bool).unwrap_or(false);
        let mut out = Vec::new();
        for name in names {
            let channel = match Channel::parse(name) {
                Ok(c) => c,
                Err(r) => {
                    out.push(error(id, None, r));
                    continue;
                }
            };
            // A lifecycle subscription takes no market filter and is never sent anything.
            let tickers = tickers.clone().filter(|_| channel != Channel::Lifecycle);
            if let Some(t) = &tickers {
                if let Err(r) = Self::refused(t, state) {
                    out.push(error(id, None, r));
                    continue;
                }
            }
            // One subscription per channel: a repeat adds its markets to the existing one.
            if let Some(sid) = self.sub_mut(channel).map(|s| s.sid) {
                let added = match tickers {
                    Some(t) => self.add(id, sid, &t, flag("send_initial_snapshot"), state),
                    None => Err((Some(sid), refusal(13, "Subscription has no market filter"))),
                };
                match added {
                    Ok(frames) => out.extend(frames),
                    Err((sid, r)) => out.push(error(id, sid, r)),
                }
                continue;
            }
            if channel == Channel::Book && tickers.is_none() {
                out.push(error(id, None, refusal(14, "Market Ticker required")));
                continue;
            }
            self.last_sid += 1;
            let sid = self.last_sid;
            #[derive(Serialize)]
            struct Subscribed {
                channel: &'static str,
                sid: u64,
            }
            let msg = json(Subscribed {
                channel: channel.name(),
                sid,
            });
            out.push((
                "subscribed",
                frame("subscribed", id, None, None, Some(&msg)),
            ));
            self.subs.push(Sub {
                channel,
                sid,
                seq: 0,
                tickers: tickers.as_ref().map(|t| t.iter().cloned().collect()),
                use_yes_price: flag("use_yes_price"),
            });
            out.extend(self.initial(
                channel,
                tickers.as_deref(),
                flag("send_initial_snapshot"),
                state,
            ));
        }
        Ok(out)
    }

    /// What new markets on `channel` are sent first: their books, or their last quotes when asked.
    /// `None` is every market.
    fn initial(
        &mut self,
        channel: Channel,
        tickers: Option<&[Arc<str>]>,
        send_initial_snapshot: bool,
        state: &Mutex<State>,
    ) -> Vec<Out> {
        match channel {
            Channel::Book => tickers
                .unwrap_or_default()
                .iter()
                .map(|t| self.snapshot(t, None, state))
                .collect(),
            Channel::Ticker if send_initial_snapshot => {
                let Some(sid) = self.sub_mut(channel).map(|s| s.sid) else {
                    return Vec::new();
                };
                let quoted = |(t, top): (&Arc<str>, &Top)| Some((t.clone(), top.quote?, top.print));
                // Rendered after the lock drops: without a filter, this is every market.
                let quotes: Vec<_> = {
                    let st = lock(state);
                    match tickers {
                        Some(t) => t
                            .iter()
                            .filter_map(|t| quoted((t, st.tops.get(t)?)))
                            .collect(),
                        None => st.tops.iter().filter_map(quoted).collect(),
                    }
                };
                quotes
                    .iter()
                    .map(|(t, q, print)| {
                        let msg = ticker_msg(t, q, *print);
                        ("ticker", frame("ticker", None, Some(sid), None, Some(&msg)))
                    })
                    .collect()
            }
            _ => Vec::new(),
        }
    }

    /// One market's book as it stands, on the book subscription. Unknown and incomplete markets are
    /// empty, as Kalshi answers a ticker it has no book for. Stamped with the stage's clock rather
    /// than the market's, so it also covers the removals an eviction left queued.
    fn snapshot(&mut self, ticker: &Arc<str>, id: Option<u64>, state: &Mutex<State>) -> Out {
        let (clock, [bids, asks]) = {
            let st = lock(state);
            let levels = st.books.get(ticker).map(|b| b.levels.clone());
            (st.clock, levels.unwrap_or_default())
        };
        let Some(sub) = self.sub_mut(Channel::Book) else {
            return error(id, None, refusal(7, "Unknown subscription ID"));
        };
        let level = |p, s| [dollars(p), count(s)];
        let yes = bids.iter().map(|(&p, &s)| level(p, s)).collect();
        let no = if sub.use_yes_price {
            asks.iter().map(|(&p, &s)| level(p, s)).collect()
        } else {
            asks.iter()
                .rev()
                .map(|(&p, &s)| level(no_price(p, false), s))
                .collect()
        };
        let msg = json(SnapshotMsg {
            market_ticker: ticker,
            market_id: NIL_UUID,
            clock,
            yes_dollars_fp: yes,
            no_dollars_fp: no,
        });
        let (sid, seq) = (sub.sid, sub.next_seq());
        if sub.covers(ticker) {
            self.watermarks.insert(ticker.clone(), clock);
        }
        let f = frame("orderbook_snapshot", id, Some(sid), Some(seq), Some(&msg));
        ("orderbook_snapshot", f)
    }

    fn unsubscribe(
        &mut self,
        id: Option<u64>,
        params: &Value,
    ) -> Result<Vec<Out>, (Option<u64>, Refusal)> {
        let sids: Vec<u64> = params
            .get("sids")
            .and_then(Value::as_array)
            .map(|s| s.iter().filter_map(Value::as_u64).collect())
            .unwrap_or_default();
        if sids.is_empty() {
            return Err((None, refusal(4, "Subscription IDs required")));
        }
        let mut out = Vec::new();
        for sid in sids {
            let Some(i) = self.subs.iter().position(|s| s.sid == sid) else {
                out.push(error(id, Some(sid), refusal(7, "Unknown subscription ID")));
                continue;
            };
            let mut sub = self.subs.remove(i);
            if sub.channel == Channel::Book {
                self.watermarks.clear();
            }
            let f = frame("unsubscribed", id, Some(sid), Some(sub.next_seq()), None);
            out.push(("unsubscribed", f));
        }
        Ok(out)
    }

    fn update(
        &mut self,
        id: Option<u64>,
        params: &Value,
        state: &Mutex<State>,
    ) -> Result<Vec<Out>, (Option<u64>, Refusal)> {
        let sid = match (params.get("sid"), params.get("sids")) {
            (Some(sid), None) => sid.as_u64(),
            (None, Some(Value::Array(sids))) if sids.len() == 1 => sids[0].as_u64(),
            _ => None,
        }
        .ok_or((None, refusal(12, "Exactly one subscription ID is required")))?;
        let Some(action) = params.get("action").and_then(Value::as_str) else {
            return Err((Some(sid), refusal(15, "Action required")));
        };
        let Some(channel) = self.subs.iter().find(|s| s.sid == sid).map(|s| s.channel) else {
            return Err((Some(sid), refusal(7, "Unknown subscription ID")));
        };
        let tickers = tickers(params)
            .map_err(|r| (Some(sid), r))?
            .ok_or((Some(sid), refusal(14, "Market Ticker required")))?;
        match action {
            "add_markets" | "get_snapshot" => {
                Self::refused(&tickers, state).map_err(|r| (Some(sid), r))?;
            }
            "delete_markets" => {}
            _ => return Err((Some(sid), refusal(13, "Unsupported action"))),
        }
        let initial = params
            .get("send_initial_snapshot")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        match action {
            "add_markets" => self.add(id, sid, &tickers, initial, state),
            "delete_markets" => self.delete(id, sid, &tickers),
            _ if channel == Channel::Book => Ok(tickers
                .iter()
                .map(|t| self.snapshot(t, id, state))
                .collect()),
            _ => Err((Some(sid), refusal(13, "Unsupported action"))),
        }
    }

    fn add(
        &mut self,
        id: Option<u64>,
        sid: u64,
        tickers: &[Arc<str>],
        send_initial_snapshot: bool,
        state: &Mutex<State>,
    ) -> Result<Vec<Out>, (Option<u64>, Refusal)> {
        let sub = self
            .subs
            .iter_mut()
            .find(|s| s.sid == sid)
            .expect("sid resolved");
        let channel = sub.channel;
        let Some(held) = &mut sub.tickers else {
            return Err((Some(sid), refusal(13, "Subscription has no market filter")));
        };
        let added: Vec<Arc<str>> = tickers
            .iter()
            .filter(|t| !held.contains(*t))
            .cloned()
            .collect();
        if held.len() + added.len() > MAX_TICKERS {
            return Err((Some(sid), refusal(26, "Subscription market limit exceeded")));
        }
        held.extend(added.iter().cloned());
        let mut out = vec![self.ok(id, sid)];
        out.extend(self.initial(channel, Some(&added), send_initial_snapshot, state));
        Ok(out)
    }

    fn delete(
        &mut self,
        id: Option<u64>,
        sid: u64,
        tickers: &[Arc<str>],
    ) -> Result<Vec<Out>, (Option<u64>, Refusal)> {
        let sub = self
            .subs
            .iter_mut()
            .find(|s| s.sid == sid)
            .expect("sid resolved");
        let book = sub.channel == Channel::Book;
        let Some(held) = &mut sub.tickers else {
            return Err((Some(sid), refusal(13, "Subscription has no market filter")));
        };
        for t in tickers {
            held.remove(t);
            if book {
                self.watermarks.remove(t);
            }
        }
        Ok(vec![self.ok(id, sid)])
    }

    /// Kalshi's acknowledgement of a changed market filter, carrying the whole filter.
    fn ok(&mut self, id: Option<u64>, sid: u64) -> Out {
        #[derive(Serialize)]
        struct Updated<'a> {
            market_tickers: Vec<&'a str>,
        }
        let sub = self
            .subs
            .iter_mut()
            .find(|s| s.sid == sid)
            .expect("sid resolved");
        let msg = json(Updated {
            market_tickers: sub.tickers.iter().flatten().map(|t| t.as_ref()).collect(),
        });
        (
            "ok",
            frame("ok", id, Some(sid), Some(sub.next_seq()), Some(&msg)),
        )
    }

    /// The frames one prepared message owes this connection.
    fn frames(&mut self, p: &Prepared) -> Vec<Out> {
        match p {
            Prepared::Book {
                ticker,
                clock,
                deltas,
                ts_ms,
            } => {
                let Some(sub) = self.subs.iter_mut().find(|s| s.channel == Channel::Book) else {
                    return Vec::new();
                };
                if !sub.covers(ticker)
                    || *clock <= self.watermarks.get(ticker.as_ref()).copied().unwrap_or(0)
                {
                    return Vec::new();
                }
                deltas
                    .iter()
                    .map(|d| {
                        let (side, price) = if d.bid {
                            ("yes", d.price)
                        } else {
                            ("no", no_price(d.price, sub.use_yes_price))
                        };
                        let msg = json(DeltaMsg {
                            market_ticker: ticker,
                            market_id: NIL_UUID,
                            clock: *clock,
                            price_dollars: dollars(price),
                            delta_fp: count(d.delta),
                            side,
                            ts_ms: *ts_ms,
                        });
                        let (sid, seq) = (sub.sid, sub.next_seq());
                        let f = frame("orderbook_delta", None, Some(sid), Some(seq), Some(&msg));
                        ("orderbook_delta", f)
                    })
                    .collect()
            }
            Prepared::Ticker { ticker, msg } => match self.sub_mut(Channel::Ticker) {
                Some(sub) if sub.covers(ticker) => {
                    vec![(
                        "ticker",
                        frame("ticker", None, Some(sub.sid), None, Some(msg)),
                    )]
                }
                _ => Vec::new(),
            },
            Prepared::Trade { ticker, msg } => match self.sub_mut(Channel::Trade) {
                Some(sub) if sub.covers(ticker) => {
                    let (sid, seq) = (sub.sid, sub.next_seq());
                    vec![(
                        "trade",
                        frame("trade", None, Some(sid), Some(seq), Some(msg)),
                    )]
                }
                _ => Vec::new(),
            },
            Prepared::Gap => {
                self.skip_seq(&[Channel::Trade]);
                Vec::new()
            }
        }
    }

    /// Leave a gap in `seq`, which a Kalshi client answers with `get_snapshot`, rather than close a
    /// connection its client may never reopen.
    fn skip_seq(&mut self, channels: &[Channel]) {
        for sub in self
            .subs
            .iter_mut()
            .filter(|s| channels.contains(&s.channel))
        {
            sub.seq += 1;
        }
    }
}

// --- the server ---

/// Bound up front so a taken port disables this sink, never the process and its tunnel.
pub async fn bind(addr: &str) -> Result<TcpListener> {
    let listener = TcpListener::bind(addr).await?;
    info!(bind = %addr, "Kalshi-compatible WebSocket sink listening");
    Ok(listener)
}

/// Never returns an accept error: `main`'s `select!` would exit the process over it.
pub async fn serve(
    listener: TcpListener,
    tx: broadcast::Sender<Arc<FeedMessage>>,
    books: BookSnapshot,
    instruments: InstrumentSnapshot,
) -> Result<()> {
    let clients = Arc::new(AtomicUsize::new(0));
    let state = Arc::new(Mutex::new(State::default()));
    let (prepared, _) = broadcast::channel::<Arc<Prepared>>(PREPARED_CAPACITY);
    tokio::spawn(prepare_loop(
        tx.subscribe(),
        prepared.clone(),
        state.clone(),
        books,
        instruments,
    ));
    loop {
        let (stream, peer) = match listener.accept().await {
            Ok(pair) => pair,
            Err(e) => {
                warn!("kalshi sink accept failed: {e}");
                tokio::time::sleep(ACCEPT_BACKOFF).await;
                continue;
            }
        };
        if clients.fetch_add(1, Ordering::SeqCst) >= MAX_CLIENTS {
            clients.fetch_sub(1, Ordering::SeqCst);
            warn!(%peer, max = MAX_CLIENTS, "kalshi sink at max clients; rejecting connection");
            continue;
        }
        metrics().kalshi_sink_clients.inc();
        let guard = ClientGuard {
            clients: clients.clone(),
            gauge: metrics().kalshi_sink_clients.clone(),
        };
        let (prepared, state) = (prepared.clone(), state.clone());
        tokio::spawn(async move {
            let _guard = guard;
            if let Err(e) = serve_client(stream, prepared, state).await {
                warn!(%peer, "kalshi sink client ended: {e}");
            }
        });
    }
}

/// Accept only Kalshi's path. The request's signed headers are never read, and so never logged.
async fn accept(stream: TcpStream) -> Result<WebSocketStream<TcpStream>> {
    #[allow(
        clippy::result_large_err,
        reason = "tungstenite's handshake callback fixes the error type"
    )]
    let route = |request: &Request, response: Response| {
        let refuse = |body: &str| {
            let mut r = ErrorResponse::new(Some(body.to_string()));
            *r.status_mut() = StatusCode::NOT_FOUND;
            Err(r)
        };
        match request.uri().path().trim_end_matches('/') {
            PATH => Ok(response),
            "/trade-api/ws/v2/margin" => refuse(PERPS_NOT_SERVED),
            _ => refuse(NOT_FOUND),
        }
    };
    let limits = tokio_tungstenite::tungstenite::protocol::WebSocketConfig::default()
        .max_message_size(Some(MAX_FRAME))
        .max_frame_size(Some(MAX_FRAME));
    let ws = tokio::time::timeout(
        HANDSHAKE_TIMEOUT,
        tokio_tungstenite::accept_hdr_async_with_config(stream, route, Some(limits)),
    )
    .await
    .map_err(|_| anyhow::anyhow!("handshake timed out"))??;
    Ok(ws)
}

async fn serve_client(
    stream: TcpStream,
    prepared: broadcast::Sender<Arc<Prepared>>,
    state: Arc<Mutex<State>>,
) -> Result<()> {
    let (mut write, mut read) = accept(stream).await?.split();
    // Before any snapshot is taken, so every change after one is queued here.
    let mut rx = prepared.subscribe();
    let mut session = Session::default();
    let mut last_seen = Instant::now();
    let mut hb = tokio::time::interval_at(tokio::time::Instant::now() + HEARTBEAT, HEARTBEAT);
    let mut commands = Budget::new(MAX_COMMANDS_PER_MIN);
    let mut snapshots = Budget::new(MAX_TICKERS as u32);

    loop {
        let out = tokio::select! {
            incoming = read.next() => match incoming {
                Some(Ok(WsMessage::Close(_))) | None => break,
                Some(Ok(frame)) => {
                    last_seen = Instant::now();
                    let allowed = match &frame {
                        WsMessage::Text(text) => match snapshot_markets(text) {
                            Some(n) => snapshots.take(n),
                            None => commands.take(1),
                        },
                        _ => commands.take(1),
                    };
                    match frame {
                        WsMessage::Text(text) if allowed => session.command(&text, &state),
                        // Refused in-band rather than by closing: Nautilus does not reconnect.
                        WsMessage::Text(text) => {
                            let id = serde_json::from_str::<Value>(&text)
                                .ok()
                                .and_then(|c| c.get("id").and_then(Value::as_u64))
                                .filter(|&id| id != 0);
                            vec![error(id, None, refusal(27, "Too many requests"))]
                        }
                        WsMessage::Ping(p) if allowed => {
                            send(&mut write, WsMessage::Pong(p)).await?;
                            continue;
                        }
                        _ => continue,
                    }
                }
                Some(Err(e)) => return Err(e.into()),
            },

            _ = hb.tick() => {
                if last_seen.elapsed() > IDLE_TIMEOUT {
                    let _ = send(&mut write, WsMessage::Close(None)).await;
                    finish_close(&mut read, CLOSE_HANDSHAKE_TIMEOUT).await;
                    break;
                }
                send(&mut write, WsMessage::Ping("heartbeat".into())).await?;
                continue;
            },

            msg = rx.recv() => match msg {
                Ok(p) => session.frames(&p),
                Err(broadcast::error::RecvError::Lagged(n)) => {
                    warn!("kalshi sink client lagged, dropped {n}");
                    dropped("client_lagged");
                    session.skip_seq(&[Channel::Book, Channel::Trade]);
                    continue;
                }
                Err(broadcast::error::RecvError::Closed) => break,
            },
        };
        for (kind, f) in out {
            metrics()
                .kalshi_sink_messages
                .with_label_values(&[kind])
                .inc();
            send(&mut write, WsMessage::Text(f.into())).await?;
        }
    }
    Ok(())
}

/// Bounded, because a peer that stops reading leaves a send pending for good, and with it this
/// connection's heartbeat and idle reap.
async fn send<S>(write: &mut S, msg: WsMessage) -> Result<()>
where
    S: SinkExt<WsMessage> + Unpin,
    S::Error: std::error::Error + Send + Sync + 'static,
{
    tokio::time::timeout(IDLE_TIMEOUT, write.send(msg))
        .await
        .map_err(|_| anyhow::anyhow!("client stopped reading"))??;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use serde_json::json as j;
    use tokio::time::timeout;

    use super::*;
    use crate::{
        ingest::codec_common::apply_exponent,
        model::{BookAccumulator, BookReplay, QuoteWire},
    };

    const TICKER: &str = "KXNFLGAME-26NOV22DALPHI-DAL";
    const OTHER: &str = "KXNFLGAME-26NOV22DALPHI-PHI";
    const PERP: &str = "KXBTCPERP";
    /// 2023-11-14T22:13:20.123Z.
    const TS_NS: u64 = 1_700_000_000_123_000_000;

    fn key(instrument_id: u32) -> BookKey {
        (
            SourceKey::from_id(KALSHI_SOURCE_ID),
            "events".into(),
            10,
            instrument_id,
        )
    }

    fn instrument(id: u32, symbol: &str, asset_class: u8, exps: (i8, i8)) -> NormalizedInstrument {
        let (k, source) = (key(id), SourceKey::from_id(KALSHI_SOURCE_ID));
        NormalizedInstrument {
            venue: source.name_arc().clone(),
            source_name: source.name_arc().clone(),
            source_id: KALSHI_SOURCE_ID,
            symbol: symbol.into(),
            channel: k.2,
            instrument_id: id,
            category: k.1,
            price_exponent: exps.0,
            qty_exponent: exps.1,
            tick_size: 1,
            asset_class,
        }
    }

    /// A stage, the replay map the arbiter writes before every broadcast, and the catalog.
    struct Stage {
        state: Mutex<State>,
        replay: BookSnapshot,
        catalog: InstrumentSnapshot,
    }

    impl Stage {
        /// Event markets `1` (`TICKER`) and `2` (`OTHER`), and the perp `9`.
        fn new() -> Self {
            let catalog: InstrumentSnapshot = Default::default();
            for i in [
                instrument(1, TICKER, PREDICTION_BINARY, (-4, -2)),
                instrument(2, OTHER, PREDICTION_BINARY, (-4, -2)),
                instrument(9, PERP, 5, (-8, -8)),
            ] {
                lock(&catalog).insert(key(i.instrument_id), i);
            }
            Self {
                state: Default::default(),
                replay: Arc::new(Mutex::new(BookReplay::default())),
                catalog,
            }
        }

        /// Publish as the arbiter does, replay entry first, with nothing else queued behind it.
        fn feed(&self, msg: FeedMessage) -> Vec<Prepared> {
            self.publish(&msg);
            self.read(Some(&msg), 0)
        }

        /// The stage reading `msg`, or a gap, with `queued` more behind it.
        fn read(&self, msg: Option<&FeedMessage>, queued: usize) -> Vec<Prepared> {
            lock(&self.state).next(msg, &self.replay, &self.catalog, || queued)
        }

        /// The arbiter's half of a publish alone: what a lagging stage has not read yet.
        fn publish(&self, msg: &FeedMessage) {
            if let FeedMessage::Book(b) = msg {
                lock(&self.replay)
                    .entry_or_insert_with(&key(b.instrument_id), || {
                        BookAccumulator::new(b.symbol.clone())
                    })
                    .apply(b);
            }
        }

        /// A whole book for market `id`: bids then asks, `(price, size)` in dollars and contracts.
        fn rebaseline(&self, id: u32, bids: &[(f64, f64)], asks: &[(f64, f64)]) -> Vec<Prepared> {
            let mut changes = vec![change(BookAction::Clear, BookSide::Both, 0.0, 0.0)];
            changes.extend(
                bids.iter()
                    .map(|&(p, s)| change(BookAction::Update, BookSide::Bid, p, s)),
            );
            changes.extend(
                asks.iter()
                    .map(|&(p, s)| change(BookAction::Update, BookSide::Ask, p, s)),
            );
            self.feed(FeedMessage::Book(batch(id, changes, true)))
        }

        fn levels(&self, ticker: &str) -> [BTreeMap<i64, i64>; 2] {
            lock(&self.state).books[ticker].levels.clone()
        }
    }

    fn change(action: BookAction, side: BookSide, price: f64, size: f64) -> BookChange {
        BookChange {
            action,
            side,
            price,
            size,
            order_id: 0,
        }
    }

    fn symbol(id: u32) -> &'static str {
        match id {
            2 => OTHER,
            9 => PERP,
            _ => TICKER,
        }
    }

    fn batch(id: u32, changes: Vec<BookChange>, last: bool) -> NormalizedBook {
        let k = key(id);
        NormalizedBook {
            venue: k.0.name_arc().clone(),
            source_name: k.0.name_arc().clone(),
            source_id: KALSHI_SOURCE_ID,
            symbol: symbol(id).into(),
            channel: k.2,
            instrument_id: id,
            category: k.1,
            order_level: false,
            changes,
            batch_id: None,
            snapshot: false,
            last,
            source_ts_ns: TS_NS,
            recv_ts_ns: TS_NS,
            kernel_rx_ts_ns: 0,
            ws_send_ts_ns: 0,
        }
    }

    /// A top-of-book quote as the decoder emits it from wire integers at −4/−2.
    fn quote(id: u32, bid: (i64, u64), ask: (i64, u64)) -> NormalizedQuote {
        let k = key(id);
        NormalizedQuote {
            venue: k.0.name_arc().clone(),
            source_name: k.0.name_arc().clone(),
            source_id: KALSHI_SOURCE_ID,
            symbol: symbol(id).into(),
            bid: apply_exponent(bid.0, -4),
            ask: apply_exponent(ask.0, -4),
            bid_size: apply_exponent(bid.1 as i64, -2),
            ask_size: apply_exponent(ask.1 as i64, -2),
            bid_n: 0,
            ask_n: 0,
            source_ts_ns: TS_NS,
            recv_ts_ns: TS_NS,
            kernel_rx_ts_ns: 0,
            ws_send_ts_ns: 0,
            venue_update_id: 0,
            wire: QuoteWire {
                channel: k.2,
                bid_price: bid.0,
                bid_qty: bid.1,
                ask_price: ask.0,
                ask_qty: ask.1,
                price_exponent: -4,
                qty_exponent: -2,
            },
            channel: k.2,
            instrument_id: id,
            category: k.1,
        }
    }

    fn trade(id: u32, side: Side, price: i64, size: i64) -> NormalizedTrade {
        let k = key(id);
        NormalizedTrade {
            venue: k.0.name_arc().clone(),
            source_name: k.0.name_arc().clone(),
            source_id: KALSHI_SOURCE_ID,
            symbol: if id == 9 { PERP } else { TICKER }.into(),
            channel: k.2,
            instrument_id: id,
            category: k.1,
            price: apply_exponent(price, -4),
            size: apply_exponent(size, -2),
            aggressor_side: side,
            trade_id: 0x0123_4567_89ab_cdef,
            cumulative_volume: 0.0,
            source_ts_ns: TS_NS,
            recv_ts_ns: TS_NS,
            kernel_rx_ts_ns: 0,
            ws_send_ts_ns: 0,
            wire: Default::default(),
        }
    }

    fn parse(frames: &[Out]) -> Vec<Value> {
        frames
            .iter()
            .map(|(_, f)| serde_json::from_str(f).unwrap())
            .collect()
    }

    fn command(session: &mut Session, stage: &Stage, cmd: Value) -> Vec<Value> {
        parse(&session.command(&cmd.to_string(), &stage.state))
    }

    fn deliver(session: &mut Session, prepared: &[Prepared]) -> Vec<Value> {
        parse(
            &prepared
                .iter()
                .flat_map(|p| session.frames(p))
                .collect::<Vec<_>>(),
        )
    }

    fn subscribe_book(
        session: &mut Session,
        stage: &Stage,
        tickers: &[&str],
        use_yes_price: bool,
    ) -> Vec<Value> {
        command(
            session,
            stage,
            j!({"id": 1, "cmd": "subscribe", "params": {"channels": ["orderbook_delta"], "market_tickers": tickers, "use_yes_price": use_yes_price}}),
        )
    }

    /// What a Kalshi client holds, checked as Nautilus checks it after every delta.
    #[derive(Default)]
    struct Client {
        yes: BTreeMap<String, f64>,
        /// Asks, at YES-leg prices.
        asks: BTreeMap<String, f64>,
    }

    impl Client {
        fn apply(&mut self, frame: &Value, use_yes_price: bool) {
            let ask_of = |p: &str| {
                if use_yes_price {
                    p.to_string()
                } else {
                    dollars(ONE_DOLLAR - raw(p.parse().unwrap(), -4).unwrap())
                }
            };
            let msg = &frame["msg"];
            match frame["type"].as_str().unwrap() {
                "orderbook_snapshot" => {
                    self.yes.clear();
                    self.asks.clear();
                    for l in msg["yes_dollars_fp"].as_array().into_iter().flatten() {
                        self.yes.insert(
                            l[0].as_str().unwrap().into(),
                            l[1].as_str().unwrap().parse().unwrap(),
                        );
                    }
                    for l in msg["no_dollars_fp"].as_array().into_iter().flatten() {
                        self.asks.insert(
                            ask_of(l[0].as_str().unwrap()),
                            l[1].as_str().unwrap().parse().unwrap(),
                        );
                    }
                }
                "orderbook_delta" => {
                    let p = msg["price_dollars"].as_str().unwrap();
                    let (side, p) = if msg["side"] == "yes" {
                        (&mut self.yes, p.to_string())
                    } else {
                        (&mut self.asks, ask_of(p))
                    };
                    let delta: f64 = msg["delta_fp"].as_str().unwrap().parse().unwrap();
                    assert_ne!(delta, 0.0, "a delta must change a level");
                    let size = side.get(&p).copied().unwrap_or(0.0) + delta;
                    assert!(size > -1e-9, "a delta took {p} negative");
                    if size.abs() < 1e-9 {
                        side.remove(&p);
                    } else {
                        side.insert(p, size);
                    }
                }
                _ => return,
            }
            if let (Some(bid), Some(ask)) = (self.yes.keys().next_back(), self.asks.keys().next()) {
                assert!(bid < ask, "crossed or locked book: bid {bid} ask {ask}");
            }
        }

        fn levels(&self) -> Levels {
            (
                self.yes.iter().map(|(p, s)| (p.clone(), *s)).collect(),
                self.asks.iter().map(|(p, s)| (p.clone(), *s)).collect(),
            )
        }
    }

    /// Bids and asks, as `(price, contracts)` at YES-leg prices.
    type Levels = (Vec<(String, f64)>, Vec<(String, f64)>);

    fn stage_levels(stage: &Stage, ticker: &str) -> Levels {
        let [bids, asks] = stage.levels(ticker);
        let side = |m: BTreeMap<i64, i64>| {
            m.into_iter()
                .map(|(p, s)| (dollars(p), s as f64 / 100.0))
                .collect()
        };
        (side(bids), side(asks))
    }

    #[test]
    fn the_fixed_point_renders_as_kalshi_writes_it() {
        assert_eq!(
            [
                dollars(0),
                dollars(1),
                dollars(100),
                dollars(9_900),
                dollars(9_990),
                dollars(ONE_DOLLAR)
            ],
            ["0.0000", "0.0001", "0.0100", "0.9900", "0.9990", "1.0000"]
        );
        assert_eq!(
            [count(-5_400), count(1_250), count(1), count(0)],
            ["-54.00", "12.50", "0.01", "0.00"]
        );
        assert_eq!(raw(apply_exponent(9_990, -4), -4), Some(9_990));
        assert_eq!(raw(apply_exponent(1, -4), -4), Some(1));
        assert_eq!(raw(f64::NAN, -4), None);
        assert_eq!(
            trade_uuid(0x0123_4567_89ab_cdef),
            "00000000-0000-0000-0123-456789abcdef"
        );
        assert_eq!(rfc3339(TS_NS / 1_000_000), "2023-11-14T22:13:20.123Z");
        assert_eq!(rfc3339(951_782_400_000), "2000-02-29T00:00:00.000Z");
    }

    /// At a cent and a tenth of a cent from either bound, under both price scales.
    #[test]
    fn yes_and_no_map_exactly_at_the_extremes() {
        for use_yes_price in [false, true] {
            let stage = Stage::new();
            stage.rebaseline(
                1,
                &[(0.001, 3.0), (0.01, 5.0)],
                &[(0.99, 7.0), (0.999, 11.0)],
            );
            let mut s = Session::default();
            let frames = subscribe_book(&mut s, &stage, &[TICKER], use_yes_price);
            let snap = &frames[1]["msg"];
            assert_eq!(
                snap["yes_dollars_fp"],
                j!([["0.0010", "3.00"], ["0.0100", "5.00"]])
            );
            let no = if use_yes_price {
                j!([["0.9900", "7.00"], ["0.9990", "11.00"]])
            } else {
                j!([["0.0010", "11.00"], ["0.0100", "7.00"]])
            };
            assert_eq!(snap["no_dollars_fp"], no, "use_yes_price={use_yes_price}");

            let p = stage.feed(FeedMessage::Book(batch(
                1,
                vec![
                    change(BookAction::Update, BookSide::Ask, 0.99, 2.0),
                    change(BookAction::Update, BookSide::Bid, 0.0001, 1.0),
                ],
                true,
            )));
            let deltas = deliver(&mut s, &p);
            let ask = deltas.iter().find(|d| d["msg"]["side"] == "no").unwrap();
            let price = if use_yes_price { "0.9900" } else { "0.0100" };
            assert_eq!(
                (
                    ask["msg"]["price_dollars"].as_str(),
                    ask["msg"]["delta_fp"].as_str()
                ),
                (Some(price), Some("-5.00"))
            );
            let bid = deltas.iter().find(|d| d["msg"]["side"] == "yes").unwrap();
            assert_eq!(bid["msg"]["price_dollars"], "0.0001");
        }
    }

    #[test]
    fn subscribe_sends_a_snapshot_then_deltas_on_one_continuous_seq() {
        let stage = Stage::new();
        stage.rebaseline(1, &[(0.40, 10.0)], &[(0.45, 4.0)]);
        let mut s = Session::default();
        let frames = subscribe_book(&mut s, &stage, &[TICKER, OTHER], true);
        assert_eq!(
            frames[0],
            j!({"type": "subscribed", "id": 1, "sending_ts_ms": frames[0]["sending_ts_ms"], "msg": {"channel": "orderbook_delta", "sid": 1}})
        );
        assert_eq!(
            (frames[1]["type"].as_str(), frames[1]["seq"].as_u64()),
            (Some("orderbook_snapshot"), Some(1))
        );
        assert!(
            frames[1].get("id").is_none(),
            "an initial snapshot answers no command"
        );
        assert_eq!(
            (
                frames[2]["msg"]["market_ticker"].as_str(),
                frames[2]["seq"].as_u64()
            ),
            (Some(OTHER), Some(2))
        );
        assert_eq!(
            frames[2]["msg"]["clock"], frames[1]["msg"]["clock"],
            "every snapshot carries the stage's clock"
        );
        assert!(
            frames[2]["msg"].get("yes_dollars_fp").is_none(),
            "a market with no book is empty"
        );

        let p = stage.feed(FeedMessage::Book(batch(
            1,
            vec![change(BookAction::Update, BookSide::Bid, 0.41, 2.0)],
            true,
        )));
        let deltas = deliver(&mut s, &p);
        assert_eq!(deltas.len(), 1);
        assert_eq!(deltas[0]["seq"], 3);
        assert!(deltas[0]["msg"]["clock"].as_u64() > frames[1]["msg"]["clock"].as_u64());
        assert_eq!(deltas[0]["msg"]["delta_fp"], "2.00");
        assert_eq!(deltas[0]["msg"]["ts_ms"], TS_NS / 1_000_000);
    }

    /// A batch that is not its event's last is buffered: a client sees an event whole or not at all.
    #[test]
    fn an_event_is_published_at_its_last_batch() {
        let stage = Stage::new();
        stage.rebaseline(1, &[(0.40, 10.0)], &[(0.45, 4.0)]);
        let open = stage.feed(FeedMessage::Book(batch(
            1,
            vec![change(BookAction::Delete, BookSide::Ask, 0.45, 0.0)],
            false,
        )));
        assert!(open.is_empty());
        let closed = stage.feed(FeedMessage::Book(batch(
            1,
            vec![change(BookAction::Update, BookSide::Ask, 0.46, 1.0)],
            true,
        )));
        let [Prepared::Book { deltas, .. }] = closed.as_slice() else {
            panic!("one event")
        };
        assert_eq!(deltas.len(), 2);
    }

    /// The client ends on the new book without passing through a crossed or locked one.
    #[test]
    fn a_rebaseline_is_sent_as_deltas_removals_first() {
        let stage = Stage::new();
        stage.rebaseline(1, &[(0.50, 5.0)], &[(0.51, 5.0)]);
        let mut s = Session::default();
        let mut client = Client::default();
        for f in subscribe_book(&mut s, &stage, &[TICKER], true) {
            client.apply(&f, true);
        }
        let p = stage.rebaseline(1, &[(0.51, 5.0)], &[(0.52, 5.0)]);
        let deltas = deliver(&mut s, &p);
        let seen: Vec<(&str, &str, &str)> = deltas
            .iter()
            .map(|d| {
                (
                    d["msg"]["side"].as_str().unwrap(),
                    d["msg"]["price_dollars"].as_str().unwrap(),
                    d["msg"]["delta_fp"].as_str().unwrap(),
                )
            })
            .collect();
        assert_eq!(
            seen,
            [
                ("yes", "0.5000", "-5.00"),
                ("no", "0.5100", "-5.00"),
                ("yes", "0.5100", "5.00"),
                ("no", "0.5200", "5.00")
            ]
        );
        assert!(deltas
            .iter()
            .all(|d| d["type"] == "orderbook_delta"
                && d["msg"]["clock"] == deltas[0]["msg"]["clock"]));
        for d in &deltas {
            client.apply(d, true);
        }
        assert_eq!(client.levels(), stage_levels(&stage, TICKER));
    }

    /// Random walks through re-baselines and incremental events, under both price scales.
    #[test]
    fn no_intermediate_book_crosses() {
        let mut seed: u64 = 0x2545_f491_4f6c_dd1d;
        let mut next = |n: u64| {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed % n
        };
        for use_yes_price in [false, true] {
            let stage = Stage::new();
            stage.rebaseline(1, &[(0.30, 1.0)], &[(0.70, 1.0)]);
            let mut s = Session::default();
            let mut client = Client::default();
            for f in subscribe_book(&mut s, &stage, &[TICKER], use_yes_price) {
                client.apply(&f, use_yes_price);
            }
            for _ in 0..300 {
                let mid = 2 + next(96) as i64;
                let bids: Vec<(f64, f64)> = (0..next(4))
                    .map(|i| ((mid - 1 - i as i64) as f64 / 100.0, (1 + next(9)) as f64))
                    .collect();
                let asks: Vec<(f64, f64)> = (0..next(4))
                    .map(|i| ((mid + i as i64) as f64 / 100.0, (1 + next(9)) as f64))
                    .collect();
                let p = if next(2) == 0 {
                    stage.rebaseline(1, &bids, &asks)
                } else {
                    // The same move as an incremental event, against what the book holds now.
                    let [old_bids, old_asks] = stage.levels(TICKER);
                    let mut changes = Vec::new();
                    for (old, new, side) in [
                        (old_bids, &bids, BookSide::Bid),
                        (old_asks, &asks, BookSide::Ask),
                    ] {
                        for p in old.keys() {
                            changes.push(change(
                                BookAction::Delete,
                                side,
                                apply_exponent(*p, -4),
                                0.0,
                            ));
                        }
                        changes.extend(
                            new.iter()
                                .map(|&(p, sz)| change(BookAction::Update, side, p, sz)),
                        );
                    }
                    stage.feed(FeedMessage::Book(batch(1, changes, true)))
                };
                for f in deliver(&mut s, &p) {
                    client.apply(&f, use_yes_price);
                }
                assert_eq!(client.levels(), stage_levels(&stage, TICKER));
            }
        }
    }

    /// The arbiter's degraded re-baseline: a bare clear over a replay entry that is not whole.
    #[test]
    fn a_bare_clear_empties_the_book_until_a_whole_one() {
        let stage = Stage::new();
        stage.rebaseline(1, &[(0.40, 10.0)], &[(0.45, 4.0)]);
        let mut s = Session::default();
        subscribe_book(&mut s, &stage, &[TICKER], true);

        lock(&stage.replay).insert(key(1), BookAccumulator::new(TICKER.into()));
        let bare = batch(
            1,
            vec![change(BookAction::Clear, BookSide::Both, 0.0, 0.0)],
            true,
        );
        let p = stage.read(Some(&FeedMessage::Book(bare)), 0);
        let removed = deliver(&mut s, &p);
        assert_eq!(removed.len(), 2);
        assert!(removed
            .iter()
            .all(|d| d["msg"]["delta_fp"].as_str().unwrap().starts_with('-')));

        let moved = stage.feed(FeedMessage::Book(batch(
            1,
            vec![change(BookAction::Update, BookSide::Bid, 0.41, 1.0)],
            true,
        )));
        assert!(moved.is_empty(), "levels after a bare clear are not a book");
        let snapshot = command(
            &mut s,
            &stage,
            j!({"id": 2, "cmd": "update_subscription", "params": {"sid": 1, "action": "get_snapshot", "market_tickers": [TICKER]}}),
        );
        assert!(snapshot[0]["msg"].get("yes_dollars_fp").is_none());

        let restored = deliver(&mut s, &stage.rebaseline(1, &[(0.42, 3.0)], &[]));
        assert_eq!(restored.len(), 1);
        assert_eq!(restored[0]["msg"]["price_dollars"], "0.4200");
    }

    /// A change queued before a snapshot that already holds it is skipped by its `clock`.
    #[test]
    fn a_change_at_or_below_the_snapshot_clock_is_skipped() {
        let stage = Stage::new();
        stage.rebaseline(1, &[(0.40, 10.0)], &[]);
        let queued = stage.feed(FeedMessage::Book(batch(
            1,
            vec![change(BookAction::Update, BookSide::Bid, 0.41, 2.0)],
            true,
        )));
        let mut s = Session::default();
        let frames = subscribe_book(&mut s, &stage, &[TICKER], true);
        assert_eq!(
            frames[1]["msg"]["yes_dollars_fp"],
            j!([["0.4000", "10.00"], ["0.4100", "2.00"]])
        );
        assert!(deliver(&mut s, &queued).is_empty());
        let next = stage.feed(FeedMessage::Book(batch(
            1,
            vec![change(BookAction::Update, BookSide::Bid, 0.41, 3.0)],
            true,
        )));
        assert_eq!(deliver(&mut s, &next).len(), 1);
    }

    /// An evicted market's snapshot, gone or back but unpublished, holds its queued removals.
    #[test]
    fn an_evicted_markets_queued_removals_are_in_a_later_snapshot() {
        for recreated in [false, true] {
            let stage = Stage::new();
            stage.rebaseline(1, &[(0.40, 10.0)], &[(0.45, 4.0)]);
            let removals: Vec<Prepared> = lock(&stage.state).evict_oldest().into_iter().collect();
            assert_eq!(removals.len(), 1);
            if recreated {
                let open = batch(
                    1,
                    vec![change(BookAction::Update, BookSide::Bid, 0.41, 1.0)],
                    false,
                );
                assert!(stage.feed(FeedMessage::Book(open)).is_empty());
            }
            let mut s = Session::default();
            let mut client = Client::default();
            for f in subscribe_book(&mut s, &stage, &[TICKER], true) {
                client.apply(&f, true);
            }
            for f in deliver(&mut s, &removals) {
                client.apply(&f, true);
            }
            assert_eq!(client.levels(), (vec![], vec![]));
        }
    }

    /// The answer to the gap carries the command's id and moves the client's watermark.
    #[test]
    fn a_lagging_client_gets_a_seq_gap_and_get_snapshot_repairs_it() {
        let stage = Stage::new();
        stage.rebaseline(1, &[(0.40, 10.0)], &[]);
        let mut s = Session::default();
        subscribe_book(&mut s, &stage, &[TICKER], true);
        command(
            &mut s,
            &stage,
            j!({"id": 2, "cmd": "subscribe", "params": {"channels": ["trade"]}}),
        );
        let missed = stage.feed(FeedMessage::Book(batch(
            1,
            vec![change(BookAction::Update, BookSide::Bid, 0.41, 2.0)],
            true,
        )));
        s.skip_seq(&[Channel::Book, Channel::Trade]);

        let snap = command(
            &mut s,
            &stage,
            j!({"id": 3, "cmd": "update_subscription", "params": {"sid": 1, "action": "get_snapshot", "market_tickers": [TICKER]}}),
        );
        assert_eq!(
            (
                snap[0]["id"].as_u64(),
                snap[0]["sid"].as_u64(),
                snap[0]["seq"].as_u64()
            ),
            (Some(3), Some(1), Some(3)),
            "seq 2 was skipped"
        );
        assert!(
            deliver(&mut s, &missed).is_empty(),
            "the snapshot already holds it"
        );
        let print = stage.feed(FeedMessage::Trade(trade(1, Side::Buy, 4_100, 200)));
        assert_eq!(
            deliver(&mut s, &print)[0]["seq"],
            2,
            "the trade sid skipped seq 1"
        );
    }

    /// Books are re-read from the replay map; only trade subscriptions are left a gap.
    #[test]
    fn a_lagging_stage_rereads_every_book() {
        let stage = Stage::new();
        stage.rebaseline(1, &[(0.40, 10.0)], &[]);
        let mut s = Session::default();
        let mut client = Client::default();
        for f in subscribe_book(&mut s, &stage, &[TICKER], true) {
            client.apply(&f, true);
        }
        command(
            &mut s,
            &stage,
            j!({"id": 2, "cmd": "subscribe", "params": {"channels": ["trade"]}}),
        );
        // Folded by the arbiter, never seen by the stage.
        lock(&stage.replay)
            .entry_or_insert_with(&key(1), || unreachable!())
            .apply(&batch(
                1,
                vec![change(BookAction::Update, BookSide::Bid, 0.41, 2.0)],
                true,
            ));

        let p = stage.read(None, 0);
        let frames = deliver(&mut s, &p);
        assert_eq!(
            (
                frames[0]["seq"].as_u64(),
                frames[0]["msg"]["delta_fp"].as_str()
            ),
            (Some(2), Some("2.00"))
        );
        client.apply(&frames[0], true);
        assert_eq!(client.levels(), stage_levels(&stage, TICKER));
        let print = stage.feed(FeedMessage::Trade(trade(1, Side::Buy, 4_100, 200)));
        assert_eq!(
            deliver(&mut s, &print)[0]["seq"],
            2,
            "the trade sid skipped seq 1"
        );
    }

    /// An event the replay map abandoned for its size is re-read at its close, not folded half-done.
    #[test]
    fn an_event_past_the_pending_cap_is_reread() {
        let stage = Stage::new();
        stage.rebaseline(1, &[(0.40, 10.0)], &[]);
        let flood =
            vec![change(BookAction::Update, BookSide::Bid, 0.30, 1.0); MAX_PENDING_CHANGES + 1];
        assert!(stage
            .feed(FeedMessage::Book(batch(1, flood, false)))
            .is_empty());
        let p = stage.feed(FeedMessage::Book(batch(
            1,
            vec![change(BookAction::Update, BookSide::Bid, 0.31, 1.0)],
            true,
        )));
        let [Prepared::Book { deltas, .. }] = p.as_slice() else {
            panic!("one event")
        };
        assert!(
            deltas.iter().all(|d| d.delta < 0),
            "the replay entry is no longer whole: {deltas:?}"
        );
    }

    /// A perpetual shares the Source ID; a binary at another fixed point is refused too.
    #[test]
    fn perps_and_other_scales_are_not_served() {
        let stage = Stage::new();
        lock(&stage.catalog).insert(key(1), instrument(1, TICKER, PREDICTION_BINARY, (-8, -2)));
        assert!(stage.rebaseline(1, &[(0.4, 1.0)], &[]).is_empty());
        assert!(stage.rebaseline(9, &[(60_000.0, 1.0)], &[]).is_empty());
        assert!(stage
            .feed(FeedMessage::Quote(quote(9, (1, 1), (2, 1))))
            .is_empty());
        assert!(stage
            .feed(FeedMessage::Trade(trade(9, Side::Buy, 1, 1)))
            .is_empty());

        let mut s = Session::default();
        let frames = subscribe_book(&mut s, &stage, &[OTHER, PERP], true);
        assert_eq!(frames.len(), 1);
        assert_eq!(
            (
                frames[0]["type"].as_str(),
                frames[0]["id"].as_u64(),
                frames[0]["msg"]["code"].as_u64()
            ),
            (Some("error"), Some(1), Some(11))
        );
        assert_eq!(frames[0]["msg"]["market_tickers"], j!([PERP]));
        assert!(s.subs.is_empty(), "a refused command subscribes nothing");
    }

    /// Each host numbers instruments itself, so instrument `3` is the other host's copy of `TICKER`.
    fn second_host(stage: &Stage) {
        let twin = instrument(3, TICKER, PREDICTION_BINARY, (-4, -2));
        lock(&stage.catalog).insert(key(3), twin);
    }

    /// The other host's copy is ignored while ours is live, and followed once ours falls silent.
    #[test]
    fn the_other_hosts_copy_takes_over_when_ours_falls_silent() {
        let stage = Stage::new();
        second_host(&stage);
        stage.rebaseline(1, &[(0.40, 10.0)], &[(0.45, 4.0)]);
        let mut s = Session::default();
        let mut client = Client::default();
        for f in subscribe_book(&mut s, &stage, &[TICKER], true) {
            client.apply(&f, true);
        }
        assert!(stage
            .rebaseline(3, &[(0.40, 10.0)], &[(0.45, 4.0)])
            .is_empty());
        assert!(stage
            .feed(FeedMessage::Book(batch(
                3,
                vec![change(BookAction::Update, BookSide::Bid, 0.41, 1.0)],
                true
            )))
            .is_empty());

        lock(&stage.state).books.get_mut(TICKER).unwrap().seen_ns = 0;
        let p = stage.feed(FeedMessage::Book(batch(
            3,
            vec![change(BookAction::Update, BookSide::Ask, 0.44, 2.0)],
            true,
        )));
        for f in deliver(&mut s, &p) {
            client.apply(&f, true);
        }
        assert_eq!(lock(&stage.state).books[TICKER].key, key(3));
        assert_eq!(client.levels(), stage_levels(&stage, TICKER));
        assert_eq!(
            client.levels().0.len(),
            2,
            "the 0.41 bid only the other host carried"
        );
    }

    /// A book that is no longer whole hands over at the other host's next event.
    #[test]
    fn a_book_that_is_not_whole_fails_over_at_once() {
        let stage = Stage::new();
        second_host(&stage);
        stage.rebaseline(1, &[(0.40, 10.0)], &[]);
        stage.rebaseline(3, &[(0.40, 10.0)], &[]);
        lock(&stage.replay).insert(key(1), BookAccumulator::new(TICKER.into()));
        let bare = batch(
            1,
            vec![change(BookAction::Clear, BookSide::Both, 0.0, 0.0)],
            true,
        );
        stage.read(Some(&FeedMessage::Book(bare)), 0);
        assert!(stage.levels(TICKER)[0].is_empty());

        let p = stage.feed(FeedMessage::Book(batch(
            3,
            vec![change(BookAction::Update, BookSide::Bid, 0.41, 1.0)],
            true,
        )));
        let [Prepared::Book { deltas, .. }] = p.as_slice() else {
            panic!("one event")
        };
        assert_eq!(
            deltas.len(),
            2,
            "the whole book, from the other host: {deltas:?}"
        );
    }

    /// A key is wire-supplied: a replay entry under it holding another market is not this one's book.
    #[test]
    fn a_reread_ignores_another_markets_entry_under_the_key() {
        let stage = Stage::new();
        stage.rebaseline(1, &[(0.40, 10.0)], &[]);
        let mut other = BookAccumulator::new(OTHER.into());
        other.apply(&batch(
            2,
            vec![change(BookAction::Clear, BookSide::Both, 0.0, 0.0)],
            true,
        ));
        lock(&stage.replay).insert(key(1), other);
        let p = stage.read(None, 0);
        assert!(matches!(&p[0], Prepared::Book { deltas, .. } if deltas[0].delta == -1_000));
        assert!(!lock(&stage.state).books[TICKER].complete);
    }

    /// After a re-read, the backlog it already holds is not folded again.
    #[test]
    fn a_reread_is_not_walked_back_by_the_backlog() {
        let stage = Stage::new();
        stage.rebaseline(1, &[(0.40, 10.0)], &[]);
        let sizes = |size| {
            FeedMessage::Book(batch(
                1,
                vec![change(BookAction::Update, BookSide::Bid, 0.41, size)],
                true,
            ))
        };
        let backlog = [sizes(2.0), sizes(3.0), sizes(4.0)];
        for m in &backlog {
            stage.publish(m);
        }
        let reread = stage.read(None, 2);
        assert!(matches!(&reread[0], Prepared::Book { deltas, .. } if deltas[0].delta == 400));
        for (m, queued) in [(&backlog[1], 1), (&backlog[2], 0)] {
            assert!(
                stage.read(Some(m), queued).is_empty(),
                "folding it again would walk 0.41 back"
            );
        }
        let p = stage.feed(sizes(5.0));
        assert!(matches!(&p[0], Prepared::Book { deltas, .. } if deltas[0].delta == 100));
    }

    /// A commit landing as the backlog is counted is in the count or not yet in the re-read.
    #[test]
    fn a_commit_racing_the_backlog_count_is_never_folded_twice() {
        let update = |side, price, size| change(BookAction::Update, side, price, size);
        for gap in [false, true] {
            let stage = Stage::new();
            let mut s = Session::default();
            let mut client = Client::default();
            for f in subscribe_book(&mut s, &stage, &[TICKER], true) {
                client.apply(&f, true);
            }
            let opening = FeedMessage::Book(batch(
                1,
                vec![
                    change(BookAction::Clear, BookSide::Both, 0.0, 0.0),
                    update(BookSide::Bid, 0.40, 10.0),
                ],
                true,
            ));
            stage.publish(&opening);
            let mut p = Vec::new();
            if gap {
                p.extend(stage.read(Some(&opening), 0));
            }
            // The third event lifts the 0.42 ask the second one placed.
            let racing = [
                vec![update(BookSide::Bid, 0.41, 2.0)],
                vec![update(BookSide::Ask, 0.42, 5.0)],
                vec![
                    change(BookAction::Delete, BookSide::Ask, 0.42, 0.0),
                    update(BookSide::Bid, 0.43, 1.0),
                ],
            ]
            .map(|changes| FeedMessage::Book(batch(1, changes, true)));
            // A new book re-reads at its first close, and a gap re-reads every book.
            let trigger = (!gap).then_some(&opening);
            p.extend(
                lock(&stage.state).next(trigger, &stage.replay, &stage.catalog, || {
                    racing.iter().for_each(|m| stage.publish(m));
                    0
                }),
            );
            for (i, m) in racing.iter().enumerate() {
                p.extend(stage.read(Some(m), racing.len() - 1 - i));
            }
            for f in deliver(&mut s, &p) {
                client.apply(&f, true);
            }
            assert_eq!(client.levels(), stage_levels(&stage, TICKER));
            assert_eq!(
                Some(stage.levels(TICKER)),
                replay_levels(&stage.replay, &key(1), TICKER)
            );
        }
    }

    #[test]
    fn markets_are_added_deleted_and_unsubscribed_on_one_seq() {
        let stage = Stage::new();
        stage.rebaseline(2, &[(0.20, 1.0)], &[]);
        let mut s = Session::default();
        subscribe_book(&mut s, &stage, &[TICKER], true);
        let added = command(
            &mut s,
            &stage,
            j!({"id": 2, "cmd": "update_subscription", "params": {"sid": 1, "action": "add_markets", "market_tickers": [OTHER]}}),
        );
        assert_eq!(
            (
                added[0]["type"].as_str(),
                added[0]["id"].as_u64(),
                added[0]["seq"].as_u64()
            ),
            (Some("ok"), Some(2), Some(2))
        );
        assert_eq!(
            added[0]["msg"]["market_tickers"],
            j!([TICKER, OTHER]),
            "the whole filter, sorted"
        );
        assert_eq!(
            (added[1]["type"].as_str(), added[1]["seq"].as_u64()),
            (Some("orderbook_snapshot"), Some(3))
        );
        assert_eq!(added[1]["msg"]["yes_dollars_fp"], j!([["0.2000", "1.00"]]));

        let deleted = command(
            &mut s,
            &stage,
            j!({"id": 3, "cmd": "update_subscription", "params": {"sids": [1], "action": "delete_markets", "market_tickers": [TICKER]}}),
        );
        assert_eq!(
            (
                deleted[0]["seq"].as_u64(),
                &deleted[0]["msg"]["market_tickers"]
            ),
            (Some(4), &j!([OTHER]))
        );
        assert!(
            deliver(&mut s, &stage.rebaseline(1, &[(0.5, 1.0)], &[])).is_empty(),
            "a deleted market streams nothing"
        );

        let listed = command(&mut s, &stage, j!({"id": 4, "cmd": "list_subscriptions"}));
        assert_eq!(
            listed[0]["msg"],
            j!([{"channel": "orderbook_delta", "sid": 1}])
        );
        let gone = command(
            &mut s,
            &stage,
            j!({"id": 5, "cmd": "unsubscribe", "params": {"sids": [1]}}),
        );
        assert_eq!(
            gone[0],
            j!({"type": "unsubscribed", "id": 5, "sid": 1, "seq": 5, "sending_ts_ms": gone[0]["sending_ts_ms"]})
        );
    }

    /// Nautilus subscribes it on every connect, and an error there fails its `connect()`.
    #[test]
    fn market_lifecycle_is_acknowledged_and_silent() {
        let stage = Stage::new();
        let mut s = Session::default();
        let frames = command(
            &mut s,
            &stage,
            j!({"id": 1, "cmd": "subscribe", "params": {"channels": ["market_lifecycle_v2"]}}),
        );
        assert_eq!(
            frames[0]["msg"],
            j!({"channel": "market_lifecycle_v2", "sid": 1})
        );
        let mut traffic = stage.rebaseline(1, &[(0.4, 1.0)], &[]);
        traffic.extend(stage.feed(FeedMessage::Trade(trade(1, Side::Buy, 4_000, 100))));
        assert!(deliver(&mut s, &traffic).is_empty());
    }

    #[test]
    fn refusals_carry_kalshis_codes() {
        let stage = Stage::new();
        let mut s = Session::default();
        subscribe_book(&mut s, &stage, &[TICKER], true);
        command(
            &mut s,
            &stage,
            j!({"id": 2, "cmd": "subscribe", "params": {"channels": ["ticker"]}}),
        );
        let code = |s: &mut Session, cmd: Value| {
            let f = command(s, &stage, cmd);
            assert_eq!(f.len(), 1, "{f:?}");
            assert_eq!(f[0]["type"], "error");
            f[0]["msg"]["code"].as_u64().unwrap()
        };
        let many: Vec<String> = (0..=MAX_TICKERS).map(|i| format!("T{i}")).collect();
        for (cmd, want) in [
            (j!({"id": 9, "cmd": "subscribe"}), 2),
            (j!({"id": 9, "cmd": "subscribe", "params": {}}), 3),
            (j!({"id": 9, "cmd": "unsubscribe", "params": {}}), 4),
            (j!({"id": 9, "cmd": "nope"}), 5),
            (
                j!({"id": 9, "cmd": "unsubscribe", "params": {"sids": [7]}}),
                7,
            ),
            (
                j!({"id": 9, "cmd": "subscribe", "params": {"channels": ["cfbenchmarks_value"]}}),
                8,
            ),
            (
                j!({"id": 9, "cmd": "subscribe", "params": {"channels": ["fill"]}}),
                9,
            ),
            (
                j!({"id": 9, "cmd": "subscribe", "params": {"channels": ["user_orders"]}}),
                9,
            ),
            (
                j!({"id": 9, "cmd": "subscribe", "params": {"channels": ["trade"], "market_tickers": [""]}}),
                11,
            ),
            (
                j!({"id": 9, "cmd": "update_subscription", "params": {"sids": [1, 2], "action": "add_markets"}}),
                12,
            ),
            (
                j!({"id": 9, "cmd": "update_subscription", "params": {"sid": 1, "action": "move", "market_tickers": [TICKER]}}),
                13,
            ),
            (
                j!({"id": 9, "cmd": "update_subscription", "params": {"sid": 2, "action": "get_snapshot", "market_tickers": [TICKER]}}),
                13,
            ),
            (
                j!({"id": 9, "cmd": "update_subscription", "params": {"sid": 2, "action": "add_markets", "market_tickers": [TICKER]}}),
                13,
            ),
            (
                j!({"id": 9, "cmd": "subscribe", "params": {"channels": ["ticker"]}}),
                13,
            ),
            (
                j!({"id": 9, "cmd": "subscribe", "params": {"channels": ["trade"], "market_tickers": many}}),
                26,
            ),
            (
                j!({"id": 9, "cmd": "update_subscription", "params": {"sid": 1, "action": "get_snapshot"}}),
                14,
            ),
            (
                j!({"id": 9, "cmd": "update_subscription", "params": {"sid": 1, "market_tickers": [TICKER]}}),
                15,
            ),
        ] {
            assert_eq!(code(&mut s, cmd.clone()), want, "{cmd}");
        }
        let fresh = &mut Session::default();
        assert_eq!(
            code(
                fresh,
                j!({"id": 9, "cmd": "subscribe", "params": {"channels": ["orderbook_delta"]}})
            ),
            14
        );
        assert_eq!(command(fresh, &stage, j!("{")).len(), 1);
        assert_eq!(
            parse(&fresh.command("{", &stage.state))[0]["msg"]["code"],
            1
        );
        assert!(parse(&fresh.command("{", &stage.state))[0]
            .get("id")
            .is_none());
    }

    /// An empty side is a zero size, at `0` for a bid and `1` for an ask.
    #[test]
    fn ticker_renders_the_quote_and_placeholders() {
        let stage = Stage::new();
        stage.feed(FeedMessage::Trade(trade(1, Side::Sell, 4_100, 250)));
        let p = stage.feed(FeedMessage::Quote(quote(1, (4_000, 1_000), (0, 0))));
        let mut s = Session::default();
        command(
            &mut s,
            &stage,
            j!({"id": 1, "cmd": "subscribe", "params": {"channels": ["ticker"], "market_tickers": [TICKER]}}),
        );
        let msg = deliver(&mut s, &p)[0]["msg"].clone();
        assert_eq!(
            msg,
            j!({
                "market_id": NIL_UUID, "market_ticker": TICKER, "price_dollars": "0.4100",
                "yes_bid_dollars": "0.4000", "yes_ask_dollars": "1.0000", "volume_fp": "0.00",
                "open_interest_fp": "0.00", "dollar_volume": 0, "dollar_open_interest": 0,
                "yes_bid_size_fp": "10.00", "yes_ask_size_fp": "0.00", "last_trade_size_fp": "2.50",
                "ts": TS_NS / 1_000_000_000, "ts_ms": TS_NS / 1_000_000, "time": "2023-11-14T22:13:20.123Z"
            })
        );
    }

    #[test]
    fn send_initial_snapshot_replays_the_last_quote() {
        let stage = Stage::new();
        stage.feed(FeedMessage::Quote(quote(1, (4_000, 1_000), (4_200, 500))));
        let mut s = Session::default();
        let frames = command(
            &mut s,
            &stage,
            j!({"id": 1, "cmd": "subscribe", "params": {"channels": ["ticker"], "market_tickers": [TICKER, OTHER], "send_initial_snapshot": true}}),
        );
        assert_eq!(
            frames.len(),
            2,
            "subscribed, then the one market with a quote"
        );
        assert_eq!(
            (frames[1]["type"].as_str(), frames[1]["sid"].as_u64()),
            (Some("ticker"), Some(1))
        );
        assert!(frames[1].get("seq").is_none());
        assert_eq!(frames[1]["msg"]["yes_ask_dollars"], "0.4200");
    }

    /// Without a market filter, every market's last quote is replayed; with one, only its markets'.
    #[test]
    fn send_initial_snapshot_without_a_filter_replays_every_quote() {
        let stage = Stage::new();
        for id in [1, 2] {
            stage.feed(FeedMessage::Quote(quote(id, (4_000, 1_000), (4_200, 500))));
        }
        let replayed = |filter: &[&str]| {
            let mut params = j!({"channels": ["ticker"], "send_initial_snapshot": true});
            if !filter.is_empty() {
                params["market_tickers"] = j!(filter);
            }
            let cmd = j!({"id": 1, "cmd": "subscribe", "params": params});
            let mut tickers: Vec<String> = command(&mut Session::default(), &stage, cmd)
                .iter()
                .filter(|f| f["type"] == "ticker")
                .map(|f| f["msg"]["market_ticker"].as_str().unwrap().into())
                .collect();
            tickers.sort();
            tickers
        };
        assert_eq!(replayed(&[]), [TICKER, OTHER]);
        assert_eq!(replayed(&[OTHER]), [OTHER]);
    }

    /// YES and NO sum to exactly a dollar, and a print with no aggressor is not sent.
    #[test]
    fn trades_carry_both_prices_and_the_taker() {
        let stage = Stage::new();
        let mut s = Session::default();
        command(
            &mut s,
            &stage,
            j!({"id": 1, "cmd": "subscribe", "params": {"channels": ["trade"], "market_tickers": [TICKER]}}),
        );
        let buy = deliver(
            &mut s,
            &stage.feed(FeedMessage::Trade(trade(1, Side::Buy, 9_990, 1))),
        );
        assert_eq!(
            buy[0]["msg"],
            j!({
                "trade_id": "00000000-0000-0000-0123-456789abcdef", "market_ticker": TICKER,
                "yes_price_dollars": "0.9990", "no_price_dollars": "0.0010", "count_fp": "0.01",
                "taker_side": "yes", "taker_outcome_side": "yes", "taker_book_side": "bid",
                "is_block_trade": false, "ts": TS_NS / 1_000_000_000, "ts_ms": TS_NS / 1_000_000
            })
        );
        assert_eq!(buy[0]["seq"], 1);
        let sell = deliver(
            &mut s,
            &stage.feed(FeedMessage::Trade(trade(1, Side::Sell, 1, 100))),
        );
        assert_eq!(
            (
                sell[0]["msg"]["taker_outcome_side"].as_str(),
                sell[0]["msg"]["taker_book_side"].as_str()
            ),
            (Some("no"), Some("ask"))
        );
        assert_eq!(sell[0]["msg"]["no_price_dollars"], "0.9999");
        assert!(stage
            .feed(FeedMessage::Trade(trade(1, Side::Unknown, 5_000, 100)))
            .is_empty());
    }

    // --- golden frames, each parsed into Nautilus's types by `tests/kalshi_sink_shapes.rs` ---

    /// Compare one rendered frame with its fixture, ignoring only the send time.
    fn golden(rendered: &Value, fixture: &str) {
        let mut want: Value = serde_json::from_str(fixture).unwrap();
        want["sending_ts_ms"] = rendered["sending_ts_ms"].clone();
        assert_eq!(rendered, &want);
    }

    #[test]
    fn golden_frames_match_the_committed_fixtures() {
        let stage = Stage::new();
        stage.rebaseline(
            1,
            &[(0.01, 300.0), (0.22, 333.0)],
            &[(0.44, 146.0), (0.46, 20.0)],
        );
        stage.feed(FeedMessage::Quote(quote(
            1,
            (2_200, 33_300),
            (4_400, 14_600),
        )));
        let mut s = Session::default();
        let subscribed = subscribe_book(&mut s, &stage, &[TICKER], true);
        golden(
            &subscribed[1],
            include_str!("../../tests/fixtures/kalshi_orderbook_snapshot_golden.json"),
        );
        let p = stage.feed(FeedMessage::Book(batch(
            1,
            vec![change(BookAction::Update, BookSide::Ask, 0.44, 92.0)],
            true,
        )));
        golden(
            &deliver(&mut s, &p)[0],
            include_str!("../../tests/fixtures/kalshi_orderbook_delta_golden.json"),
        );

        let mut s = Session::default();
        command(
            &mut s,
            &stage,
            j!({"id": 1, "cmd": "subscribe", "params": {"channels": ["ticker", "trade"], "market_tickers": [TICKER]}}),
        );
        let print = stage.feed(FeedMessage::Trade(trade(1, Side::Sell, 2_200, 1_250)));
        golden(
            &deliver(&mut s, &print)[0],
            include_str!("../../tests/fixtures/kalshi_trade_golden.json"),
        );
        let p = stage.feed(FeedMessage::Quote(quote(
            1,
            (2_200, 33_300),
            (4_400, 14_600),
        )));
        golden(
            &deliver(&mut s, &p)[0],
            include_str!("../../tests/fixtures/kalshi_ticker_golden.json"),
        );
    }

    // --- over a socket ---

    async fn spawn(stage: &Stage) -> (broadcast::Sender<Arc<FeedMessage>>, std::net::SocketAddr) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (tx, _rx) = broadcast::channel::<Arc<FeedMessage>>(64);
        tokio::spawn(serve(
            listener,
            tx.clone(),
            stage.replay.clone(),
            stage.catalog.clone(),
        ));
        (tx, addr)
    }

    async fn connect(
        addr: std::net::SocketAddr,
        path: &str,
    ) -> std::result::Result<
        WebSocketStream<tokio_tungstenite::MaybeTlsStream<TcpStream>>,
        tokio_tungstenite::tungstenite::Error,
    > {
        use tokio_tungstenite::tungstenite::client::IntoClientRequest;
        let mut request = format!("ws://{addr}{path}").into_client_request().unwrap();
        for (name, value) in [
            ("KALSHI-ACCESS-KEY", "key-id"),
            ("KALSHI-ACCESS-SIGNATURE", "c2lnbmF0dXJl"),
            ("KALSHI-ACCESS-TIMESTAMP", "1700000000000"),
        ] {
            request.headers_mut().insert(name, value.parse().unwrap());
        }
        tokio_tungstenite::connect_async(request)
            .await
            .map(|(ws, _)| ws)
    }

    async fn next_json<S>(ws: &mut S) -> Value
    where
        S: StreamExt<Item = std::result::Result<WsMessage, tokio_tungstenite::tungstenite::Error>>
            + Unpin,
    {
        timeout(Duration::from_secs(5), async {
            loop {
                match ws.next().await {
                    Some(Ok(WsMessage::Text(t))) => return serde_json::from_str(&t).unwrap(),
                    Some(Ok(_)) => continue,
                    other => panic!("stream ended: {other:?}"),
                }
            }
        })
        .await
        .expect("a frame within the timeout")
    }

    /// A book that arrives after the empty snapshot comes as deltas, all on one `seq`.
    #[tokio::test]
    async fn a_signed_client_streams_a_book_on_kalshis_path() {
        let stage = Stage::new();
        let (tx, addr) = spawn(&stage).await;
        let publish = |b: NormalizedBook| {
            lock(&stage.replay)
                .entry_or_insert_with(&key(1), || BookAccumulator::new(TICKER.into()))
                .apply(&b);
            tx.send(Arc::new(FeedMessage::Book(b))).unwrap();
        };
        let mut ws = connect(addr, PATH).await.unwrap();
        let sub = j!({"id": 1, "cmd": "subscribe", "params": {"channels": ["orderbook_delta"], "market_tickers": [TICKER], "use_yes_price": true}});
        ws.send(WsMessage::Text(sub.to_string().into()))
            .await
            .unwrap();
        assert_eq!(next_json(&mut ws).await["type"], "subscribed");
        let snap = next_json(&mut ws).await;
        assert_eq!(
            (snap["seq"].as_u64(), snap["msg"]["clock"].as_u64()),
            (Some(1), Some(0))
        );

        let whole = vec![
            change(BookAction::Clear, BookSide::Both, 0.0, 0.0),
            change(BookAction::Update, BookSide::Bid, 0.40, 10.0),
            change(BookAction::Update, BookSide::Ask, 0.45, 4.0),
        ];
        publish(batch(1, whole, true));
        publish(batch(
            1,
            vec![change(BookAction::Update, BookSide::Bid, 0.41, 1.0)],
            true,
        ));
        let mut client = Client::default();
        client.apply(&snap, true);
        for seq in 2..=4 {
            let d = next_json(&mut ws).await;
            assert_eq!(
                (d["type"].as_str(), d["seq"].as_u64()),
                (Some("orderbook_delta"), Some(seq))
            );
            client.apply(&d, true);
        }
        let levels = |v: &[(&str, f64)]| {
            v.iter()
                .map(|&(p, s)| (p.to_string(), s))
                .collect::<Vec<_>>()
        };
        assert_eq!(
            client.levels(),
            (
                levels(&[("0.4000", 10.0), ("0.4100", 1.0)]),
                levels(&[("0.4500", 4.0)])
            )
        );

        let get = j!({"id": 2, "cmd": "update_subscription", "params": {"sid": 1, "action": "get_snapshot", "market_tickers": [TICKER]}});
        ws.send(WsMessage::Text(get.to_string().into()))
            .await
            .unwrap();
        let again = next_json(&mut ws).await;
        assert_eq!(
            (again["id"].as_u64(), again["seq"].as_u64()),
            (Some(2), Some(5))
        );
        assert_eq!(
            again["msg"]["yes_dollars_fp"],
            j!([["0.4000", "10.00"], ["0.4100", "1.00"]])
        );
        assert_eq!(again["msg"]["no_dollars_fp"], j!([["0.4500", "4.00"]]));

        let unsub = j!({"id": 3, "cmd": "unsubscribe", "params": {"sids": [1]}});
        ws.send(WsMessage::Text(unsub.to_string().into()))
            .await
            .unwrap();
        let gone = next_json(&mut ws).await;
        assert_eq!(
            (gone["type"].as_str(), gone["seq"].as_u64()),
            (Some("unsubscribed"), Some(6))
        );
    }

    #[tokio::test]
    async fn other_paths_are_refused_at_the_handshake() {
        let (_tx, addr) = spawn(&Stage::new()).await;
        for path in ["/", "/ws", "/trade-api/ws/v2/margin"] {
            match connect(addr, path).await {
                Err(tokio_tungstenite::tungstenite::Error::Http(r)) => {
                    assert_eq!(r.status(), StatusCode::NOT_FOUND, "{path}");
                    let body = String::from_utf8(r.body().clone().unwrap_or_default()).unwrap();
                    assert_eq!(
                        body.contains("perpetuals"),
                        path.ends_with("margin"),
                        "{path}: {body}"
                    );
                }
                other => panic!("{path}: {other:?}"),
            }
        }
        assert!(connect(addr, "/trade-api/ws/v2/").await.is_ok());
    }

    /// A command past the cap gets code 27 and the connection stays up, since Nautilus does not
    /// reconnect. `get_snapshot` is budgeted apart, so a client can recover every book after a gap.
    #[tokio::test]
    async fn an_over_rate_command_is_refused_with_code_27() {
        let stage = Stage::new();
        let (_tx, addr) = spawn(&stage).await;
        let mut ws = connect(addr, PATH).await.unwrap();
        let send = |cmd: Value| WsMessage::Text(cmd.to_string().into());
        let tickers: Vec<String> = (0..MAX_TICKERS).map(|i| format!("{i:0>64}")).collect();
        ws.send(send(j!({"id": 1, "cmd": "subscribe", "params": {"channels": ["trade"], "market_tickers": tickers}}))).await.unwrap();
        assert_eq!(
            next_json(&mut ws).await["type"],
            "subscribed",
            "the largest subscribe fits a frame"
        );
        ws.send(send(j!({"id": 2, "cmd": "subscribe", "params": {"channels": ["orderbook_delta"], "market_tickers": [TICKER]}}))).await.unwrap();
        assert_eq!(next_json(&mut ws).await["type"], "subscribed");
        assert_eq!(next_json(&mut ws).await["type"], "orderbook_snapshot");

        let get = |id: u64| {
            send(
                j!({"id": id, "cmd": "update_subscription", "params": {"sid": 2, "action": "get_snapshot", "market_tickers": [TICKER]}}),
            )
        };
        for id in 1_000..1_700 {
            ws.send(get(id)).await.unwrap();
        }
        for id in 3..=601 {
            ws.send(send(j!({"id": id, "cmd": "list_subscriptions"})))
                .await
                .unwrap();
        }
        let mut snapshots = 0;
        let refused = loop {
            let f = next_json(&mut ws).await;
            match f["type"].as_str() {
                Some("error") => break f,
                Some("orderbook_snapshot") => snapshots += 1,
                _ => {}
            }
        };
        assert_eq!(
            (
                snapshots,
                refused["id"].as_u64(),
                refused["msg"]["code"].as_u64()
            ),
            (700, Some(601), Some(27))
        );
        ws.send(get(1_700)).await.unwrap();
        assert_eq!(next_json(&mut ws).await["id"], 1_700, "still connected");
    }
}
