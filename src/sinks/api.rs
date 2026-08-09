//! The read-only `/v1` query API: a JSON-over-HTTP surface that answers from the shared,
//! already-computed state every other part of the bridge maintains — the instrument catalog
//! (`InstrumentSnapshot`), the MBO depth and MBP book replay maps (`DepthSnapshot`/`BookSnapshot`),
//! `history::Store`'s rolling one-hour candles/prints, and `ingest::health::FeedHealth`'s per-venue
//! liveness. It reads all of it; it owns none of it and mutates nothing here.
//!
//! Connection handling and response writing are the shared [`crate::sinks::http`] scaffolding (no
//! HTTP framework dependency, matching every other hand-rolled sink in this crate); this module only
//! supplies the request handler and the JSON envelopes.
//!
//! Like [`crate::sinks::ws`], [`bind`] is split from [`serve`] so a caller (the subscription
//! reconciler) can treat a bind failure as non-fatal — a taken port disables this sink without
//! taking the tunnel down with it.
//!
//! ## Design rules this module holds itself to
//!
//! - **A product id may be ambiguous.** [`crate::products::resolve`] can return more than one
//!   candidate for a bare `SOURCE:SYMBOL` id (the market-by-price protocol's `symbol` is a
//!   truncated display label that can collide across genuinely different markets — see
//!   `products.rs`). This module never silently picks one; it reports every candidate and lets the
//!   caller disambiguate with the `#<channel>.<instrument_id>` suffix.
//! - **`book` never claims more completeness than [`crate::model::BookAccumulator::baselined`]
//!   backs.** An accumulator that has not folded in a producer re-baseline holds only the levels
//!   that moved since it started accumulating; serving that as `"complete": true` would tell an
//!   agent to treat a partial reconstruction as the whole book.
//! - **No field is fabricated to fill out the emulated envelope.** `price_increment`/
//!   `base_increment` are derived from the instrument's own price/qty exponent (`10^exponent`);
//!   `volume_24h`, `price_percentage_change_24h`, `base_currency_id`/`quote_currency_id` and
//!   `quote_increment` have no honest source in this crate's reference data or its one-hour window,
//!   so they are omitted rather than guessed. An absent field is safe under PROTOCOL.md's
//!   forward-compat rule; a fabricated one silently corrupts an agent's reasoning.
//! - **Errors carry a remedy.** Every error body names what went wrong *and* what to do about it —
//!   see [`product_not_found`] / [`invalid_granularity`] / [`ambiguous_response`].
//!
//! No TLS (consistent with the rest of the service surface); terminate at a reverse proxy if this
//! endpoint is exposed beyond a trusted network.

use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex},
};

use anyhow::Result;
use serde_json::{json, Value};
use tokio::net::TcpListener;
use tracing::info;

use super::http::{self, Request, Response};
use crate::{
    history,
    ingest::{
        feeds::{FeedKind, FEEDS},
        health::SharedFeedHealth,
        processor::DEPTH_LEVELS,
        sources::source_label,
    },
    model::{
        venue_arc, BookSide, BookSnapshot, DepthSnapshot, InstrumentSnapshot, NormalizedInstrument,
    },
    products::{self, Resolution},
};

/// Max connections handled concurrently — same bound as [`crate::sinks::metrics`].
const MAX_CONNS: usize = 32;

/// Default candles per request when `limit` is omitted.
const DEFAULT_CANDLE_LIMIT: usize = 100;

/// `limit` on `candles` caps here regardless of what the caller asks for, matching the emulated
/// API. When it binds, the `retention` block's `truncated` says so.
const MAX_CANDLE_LIMIT: usize = 350;

/// Levels served per side on `book` / `best_bid_ask`. A deliberate serving cap independent of
/// [`crate::model::BookAccumulator::baselined`]: even a fully re-baselined book is truncated here,
/// and that truncation also gates `"complete"` — see [`book`].
const MAX_LEVELS_PER_SIDE: usize = 50;

/// Recent prints returned by `ticker`.
const DEFAULT_TICKER_TRADES: usize = 50;

/// The accepted `granularity` values, in the enum order PROTOCOL.md-adjacent tooling expects, paired
/// with their length in seconds. A value outside this list is the one thing that IS an error here —
/// a coarser-than-the-window value inside it is not (see [`candles`]).
const GRANULARITIES: &[(&str, u64)] = &[
    ("ONE_MINUTE", 60),
    ("FIVE_MINUTE", 300),
    ("FIFTEEN_MINUTE", 900),
    ("THIRTY_MINUTE", 1_800),
    ("ONE_HOUR", 3_600),
    ("TWO_HOUR", 7_200),
    ("SIX_HOUR", 21_600),
    ("ONE_DAY", 86_400),
];

/// Shared read-only state the query API answers from. Every field is a handle already owned and
/// mutated elsewhere (the ingest pipeline, `history::Store`'s ingest call) — this module only reads
/// through them.
struct ApiState {
    instruments: InstrumentSnapshot,
    depth: DepthSnapshot,
    books: BookSnapshot,
    history: Arc<Mutex<history::Store>>,
    health: SharedFeedHealth,
}

/// Bind the listener up front so the caller can decide what a bind failure means — mirrors
/// [`crate::sinks::ws::bind`]. A taken port must not be fatal to the whole process.
pub async fn bind(addr: &str) -> Result<TcpListener> {
    let listener = TcpListener::bind(addr).await?;
    info!(bind = %addr, "query API listening");
    Ok(listener)
}

/// The accept loop, split out so tests (and a future `main`) can drive a pre-bound listener —
/// mirrors [`crate::sinks::ws::serve`].
pub async fn serve(
    listener: TcpListener,
    instruments: InstrumentSnapshot,
    depth: DepthSnapshot,
    books: BookSnapshot,
    history: Arc<Mutex<history::Store>>,
    health: SharedFeedHealth,
) -> Result<()> {
    let state = Arc::new(ApiState {
        instruments,
        depth,
        books,
        history,
        health,
    });
    http::serve_loop(
        listener,
        MAX_CONNS,
        Arc::new(move |req: &Request| handle(&state, req)),
    )
    .await
}

/// Answer one parsed request. Read-only: only `GET` is meaningful.
fn handle(state: &ApiState, req: &Request) -> Response {
    if req.method != "GET" {
        return error_json(
            "405 Method Not Allowed",
            "method_not_allowed",
            "Only GET is supported.".to_string(),
            "Retry with GET.",
        );
    }

    let path = req.path.as_str();
    if path == "/v1/products" {
        return products_list(state);
    }
    if path == "/v1/best_bid_ask" {
        return best_bid_ask(state);
    }
    if path == "/v1/status" {
        return status(state);
    }
    if let Some(rest) = path.strip_prefix("/v1/products/") {
        return product_scoped(state, rest, req);
    }
    unknown_endpoint(path)
}

// ---------------------------------------------------------------------------------------------
// Routing within /v1/products/{id}[/ticker|/candles|/book]
// ---------------------------------------------------------------------------------------------

fn product_scoped(state: &ApiState, rest: &str, req: &Request) -> Response {
    let mut parts = rest.splitn(2, '/');
    let raw_id = parts.next().unwrap_or("");
    let sub = parts.next();
    let id = http::percent_decode(raw_id);

    let Some(parsed) = products::parse(&id) else {
        return invalid_product_id(&id);
    };

    let resolved = match products::resolve(&state.instruments, &parsed) {
        Resolution::None => return product_not_found(&id),
        Resolution::Ambiguous(candidates) => return ambiguous_response(&id, candidates),
        Resolution::One(p) => p,
    };

    let Some(inst) = lookup_instrument(
        state,
        resolved.source_id,
        resolved.channel,
        resolved.instrument_id,
    ) else {
        // resolve() found this identity in the same snapshot a moment ago; only a concurrent
        // eviction/removal between the two locks could get here. Report it the same as any other
        // miss rather than panicking on a benign race.
        return product_not_found(&id);
    };

    match sub {
        None | Some("") => {
            let ambiguous = is_ambiguous(state, inst.source_id, &inst.symbol);
            ok_json(json!({ "product": product_entry(state, &inst, ambiguous) }))
        }
        Some("ticker") => ticker(state, &inst),
        Some("candles") => candles(state, &inst, req),
        Some("book") => book(state, &inst),
        Some(other) => unknown_subresource(other),
    }
}

/// Re-fetch the full instrument record for an identity `resolve()` already matched.
/// `InstrumentSnapshot` is keyed exactly on this identity (`(venue, channel, instrument_id)`), so
/// this is a direct lookup, not a scan — `venue` is rederived from `source_id` the same way
/// `ingest::processor` resolved it when it wrote the entry (`venue_arc(source_label(source_id))`).
fn lookup_instrument(
    state: &ApiState,
    source_id: u16,
    channel: u32,
    instrument_id: u32,
) -> Option<NormalizedInstrument> {
    let venue = venue_arc(source_label(source_id));
    let map = crate::model::lock(&state.instruments);
    map.get(&(venue, channel, instrument_id)).cloned()
}

/// Whether more than one instrument shares `(source_id, symbol)` — what decides whether a
/// `product_id` needs its `#<channel>.<instrument_id>` suffix to stay unique.
fn is_ambiguous(state: &ApiState, source_id: u16, symbol: &Arc<str>) -> bool {
    let map = crate::model::lock(&state.instruments);
    map.values()
        .filter(|i| i.source_id == source_id && i.symbol.as_ref() == symbol.as_ref())
        .count()
        > 1
}

// ---------------------------------------------------------------------------------------------
// GET /v1/products, GET /v1/products/{id}
// ---------------------------------------------------------------------------------------------

fn products_list(state: &ApiState) -> Response {
    // Snapshot the catalog into an owned `Vec` and drop the lock immediately — `product_entry`
    // (via `feed_kind_for`) takes the `books` then `depth` locks per instrument, and this map is
    // also the one the ingest hot path's `upsert_instrument` writes on every refdata burst. Holding
    // this guard across ~3 lock acquisitions per instrument (here, times a catalog that can run into
    // the thousands) would contend against ingest for the whole response build; the same discipline
    // `sinks/ws.rs::replay_scoped` documents (take-then-drop before any further work).
    let instruments: Vec<NormalizedInstrument> = {
        let map = crate::model::lock(&state.instruments);
        map.values().cloned().collect()
    };
    let mut counts: HashMap<(u16, String), usize> = HashMap::new();
    for i in &instruments {
        *counts
            .entry((i.source_id, i.symbol.to_string()))
            .or_insert(0) += 1;
    }
    let products: Vec<Value> = instruments
        .iter()
        .map(|i| {
            let ambiguous = counts
                .get(&(i.source_id, i.symbol.to_string()))
                .copied()
                .unwrap_or(1)
                > 1;
            product_entry(state, i, ambiguous)
        })
        .collect();
    ok_json(json!({ "products": products }))
}

/// One product's identity + registry-derived fields. Carries the discrete identity fields
/// (`source_id`/`source`/`symbol`/`channel`/`instrument_id`) alongside the rendered `product_id`
/// string — an agent joining on identity should never have to re-parse the display id.
fn product_entry(state: &ApiState, i: &NormalizedInstrument, ambiguous: bool) -> Value {
    let pid = products::ProductId {
        source_id: i.source_id,
        symbol: i.symbol.clone(),
        channel: i.channel,
        instrument_id: i.instrument_id,
    };
    json!({
        "product_id": pid.render(ambiguous),
        "source_id": i.source_id,
        "source": i.source.as_ref(),
        "symbol": i.symbol.as_ref(),
        "channel": i.channel,
        "instrument_id": i.instrument_id,
        "price_increment": increment_string(i.price_exponent),
        "base_increment": increment_string(i.qty_exponent),
        "status": if state.health.venue_up(i.venue.as_ref()) { "online" } else { "offline" },
        "feed_kind": feed_kind_for(state, i),
    })
}

/// `10^exponent` rendered as a decimal string — `price_increment`/`base_increment`. Derivable from
/// the instrument's own exponent, unlike `quote_increment` (no honest source; deliberately omitted
/// — see the module docs).
fn increment_string(exponent: i8) -> String {
    decimal_string(10f64.powi(exponent as i32), exponent)
}

/// Render `value` as a fixed-decimal string at `exponent`'s precision (never Rust's scientific/debug
/// float formatting) — what every numeric field in this API's envelopes uses.
fn decimal_string(value: f64, exponent: i8) -> String {
    let decimals = (-exponent).max(0) as usize;
    format!("{value:.decimals$}")
}

/// The `book` replay entry for one wire identity, or `None`.
///
/// `model::BookSnapshot` is keyed on the producer-side arbitration scope as well
/// (`(venue, category, channel, instrument_id)`), because two instrument universes under one Source
/// ID have independent id spaces and would otherwise share one accumulator. This REST surface
/// addresses a market by its wire identity alone — a `NormalizedInstrument` carries no category, and
/// PROTOCOL.md never exposes one — so the scope element is skipped and the first matching entry wins.
/// On a venue whose universes genuinely collide on `(channel, instrument_id)` that choice is
/// arbitrary; the same ambiguity is already in `InstrumentSnapshot`, which is what supplied `i`.
///
/// A scan, not a lookup: the map is bounded by the arbiter's `MAX_BOOK_MARKETS` and this is a
/// per-request REST path, never the ingest hot path.
fn book_entry<'a>(
    books: &'a crate::model::BookMap,
    i: &NormalizedInstrument,
) -> Option<&'a crate::model::BookAccumulator> {
    books
        .iter()
        .find(|((venue, _, channel, id), _)| {
            *venue == i.venue && *channel == i.channel && *id == i.instrument_id
        })
        .map(|(_, acc)| acc)
}

/// Best-effort feed-kind label for one instrument, derived from which snapshot actually holds data
/// for its exact identity — never fabricated. A `BookSnapshot` entry for `(venue, channel,
/// instrument_id)` means the serving row speaks Market-by-Price; failing that, a `DepthSnapshot`
/// entry for `(venue, symbol)` means Market-by-Order; failing that, fall back to the registry only
/// when it is unambiguous — a venue with exactly one `FEEDS` kind reports it. A venue with **several**
/// rows (e.g. one carrying both a Top-of-Book and a Market-by-Price feed) has no book/depth evidence
/// yet for *this* instrument, so which row actually serves it is unknown — reporting the venue's
/// Top-of-Book row in that case would be a guess (e.g. an about-to-baseline market-by-price
/// instrument would misreport as `top_of_book`), which this module's docs promise never to do.
fn feed_kind_for(state: &ApiState, i: &NormalizedInstrument) -> &'static str {
    {
        let books = crate::model::lock(&state.books);
        if book_entry(&books, i).is_some() {
            return "market_by_price";
        }
    }
    {
        let depth = crate::model::lock(&state.depth);
        if depth.contains_key(&(i.venue.clone(), i.symbol.clone())) {
            return "market_by_order";
        }
    }
    let kinds: HashSet<FeedKind> = FEEDS
        .iter()
        .filter(|f| f.venue == i.venue.as_ref())
        .map(|f| f.kind)
        .collect();
    match kinds.len() {
        1 => feed_kind_label(*kinds.iter().next().expect("len() == 1")),
        _ => "unknown",
    }
}

fn feed_kind_label(kind: FeedKind) -> &'static str {
    match kind {
        FeedKind::TopOfBook => "top_of_book",
        FeedKind::Midpoint => "midpoint",
        FeedKind::MarketByOrder => "market_by_order",
        FeedKind::MarketByPrice => "market_by_price",
    }
}

// ---------------------------------------------------------------------------------------------
// GET /v1/products/{id}/candles
// ---------------------------------------------------------------------------------------------

fn candles(state: &ApiState, inst: &NormalizedInstrument, req: &Request) -> Response {
    let granularity_name = req.query("granularity").unwrap_or("ONE_MINUTE");
    let Some(granularity_secs) = granularity_secs(granularity_name) else {
        return invalid_granularity(granularity_name);
    };
    // A malformed or zero `limit` is rejected rather than silently substituting the default — the
    // same strictness `granularity` gets. `0` would otherwise return an empty array with no
    // indication anything was wrong with the request.
    let limit = match req.query("limit") {
        None => DEFAULT_CANDLE_LIMIT,
        Some(s) => match s.parse::<usize>() {
            Ok(n) if n > 0 => n.min(MAX_CANDLE_LIMIT),
            _ => return invalid_limit(s),
        },
    };

    let now_secs = crate::model::now_ns() / 1_000_000_000;
    let key = history::Key {
        source_id: inst.source_id,
        channel: inst.channel,
        instrument_id: inst.instrument_id,
    };
    let (candles, retention) = {
        let store = crate::model::lock(&state.history);
        (
            store.candles(&key, granularity_secs, limit, now_secs),
            store.retention(&key, granularity_secs, limit, now_secs),
        )
    };

    let candles_json: Vec<Value> = candles
        .iter()
        .map(|c| {
            json!({
                "start": c.start.to_string(),
                "low": decimal_string(c.low, inst.price_exponent),
                "high": decimal_string(c.high, inst.price_exponent),
                "open": decimal_string(c.open, inst.price_exponent),
                "close": decimal_string(c.close, inst.price_exponent),
                "volume": decimal_string(c.volume, inst.qty_exponent),
            })
        })
        .collect();

    ok_json(json!({
        "candles": candles_json,
        "retention": {
            "window_seconds": retention.window_seconds,
            "oldest": retention.oldest.to_string(),
            "newest": retention.newest.to_string(),
            "truncated": retention.truncated,
        },
    }))
}

fn granularity_secs(name: &str) -> Option<u64> {
    GRANULARITIES
        .iter()
        .find(|(n, _)| *n == name)
        .map(|(_, secs)| *secs)
}

fn invalid_granularity(got: &str) -> Response {
    let accepted: Vec<&str> = GRANULARITIES.iter().map(|(n, _)| *n).collect();
    json_status(
        "400 Bad Request",
        json!({
            "error": "invalid_granularity",
            "message": format!("\"{got}\" is not a recognised granularity."),
            "remediation": format!("Use one of: {}", accepted.join(", ")),
            "accepted": accepted,
        }),
    )
}

// ---------------------------------------------------------------------------------------------
// GET /v1/products/{id}/ticker
// ---------------------------------------------------------------------------------------------

fn ticker(state: &ApiState, inst: &NormalizedInstrument) -> Response {
    let key = history::Key {
        source_id: inst.source_id,
        channel: inst.channel,
        instrument_id: inst.instrument_id,
    };
    let trades = {
        let store = crate::model::lock(&state.history);
        store.recent_trades(&key, DEFAULT_TICKER_TRADES)
    };
    let trades_json: Vec<Value> = trades
        .iter()
        .map(|p| {
            json!({
                "time_ns": p.ts_ns.to_string(),
                "price": decimal_string(p.price, inst.price_exponent),
                "size": decimal_string(p.size, inst.qty_exponent),
            })
        })
        .collect();

    let (bid, ask) = best_levels(state, inst);
    ok_json(json!({
        "trades": trades_json,
        "best_bid": bid.map(|(p, _)| decimal_string(p, inst.price_exponent)),
        "best_ask": ask.map(|(p, _)| decimal_string(p, inst.price_exponent)),
    }))
}

/// One side's best (price, size), or absent when nothing is derivable for it.
type Level = Option<(f64, f64)>;

/// The best (price, size) per side, derived from whichever full-state book snapshot this identity
/// actually has — the market-by-price accumulator's current top level if one exists (regardless of
/// `baselined()`: reporting only the touched top level as "the best available" does not claim
/// completeness the way replaying the whole book as a re-baseline would), else the market-by-order
/// depth snapshot's top level. `None` for a side/venue with neither — there is no persisted "last
/// quote" cache in this crate to fall back to (top-of-book quotes are not replayed anywhere; see
/// `sinks/ws.rs`), so an honest answer here is "not available", not a guess.
///
/// Reads `BookAccumulator::{best_bid, best_ask}` rather than `to_book()` — this only ever needs the
/// inside market, and materializing the market's entire level set (plus a `to_book` timestamp
/// syscall) to then discard everything past the first entry per side would be wasted work on every
/// `ticker`/`best_bid_ask` call.
fn best_levels(state: &ApiState, inst: &NormalizedInstrument) -> (Level, Level) {
    {
        let books = crate::model::lock(&state.books);
        if let Some(acc) = book_entry(&books, inst) {
            return (acc.best_bid(), acc.best_ask());
        }
    }
    let depth = crate::model::lock(&state.depth);
    if let Some(d) = depth.get(&(inst.venue.clone(), inst.symbol.clone())) {
        let bid = d.bids.first().map(|b| (b[0], b[1]));
        let ask = d.asks.first().map(|a| (a[0], a[1]));
        return (bid, ask);
    }
    (None, None)
}

// ---------------------------------------------------------------------------------------------
// GET /v1/products/{id}/book
// ---------------------------------------------------------------------------------------------

fn book(state: &ApiState, inst: &NormalizedInstrument) -> Response {
    let ambiguous = is_ambiguous(state, inst.source_id, &inst.symbol);
    let rendered_id = products::ProductId {
        source_id: inst.source_id,
        symbol: inst.symbol.clone(),
        channel: inst.channel,
        instrument_id: inst.instrument_id,
    }
    .render(ambiguous);

    // Prefer the incremental market-by-price accumulator when this identity has one.
    let mbp = {
        let books = crate::model::lock(&state.books);
        book_entry(&books, inst).map(|acc| {
            (
                acc.baselined(),
                acc.to_book(&inst.venue, inst.channel, inst.instrument_id),
            )
        })
    };
    if let Some((baselined, materialized)) = mbp {
        let mut bids = Vec::new();
        let mut asks = Vec::new();
        for c in &materialized.changes {
            match c.side {
                BookSide::Bid => bids.push((c.price, c.size)),
                BookSide::Ask => asks.push((c.price, c.size)),
                // The leading `Clear` that opens `to_book`'s output; not a level.
                BookSide::Both => {}
            }
        }
        let (bids_total, asks_total) = (bids.len(), asks.len());
        // Our own serving cap, independent of `baselined()`: even a fully re-baselined book is
        // truncated here, and — unlike the market-by-order path below — we know the true pre-cap
        // count, so cutting real levels off is itself what makes the response incomplete.
        let cut_by_cap = bids_total > MAX_LEVELS_PER_SIDE || asks_total > MAX_LEVELS_PER_SIDE;
        // Honour `baselined()` for the completeness claim: an accumulator that has not folded in a
        // producer re-baseline holds only the levels that moved since it started accumulating (the
        // same fact `sinks/ws.rs::replay_scoped` acts on by skipping such a market on replay
        // entirely — this module reports the partial reconstruction instead of omitting it, since
        // the brief calls for a partial response with an honest `complete: false`, not a 404).
        let complete = baselined && !cut_by_cap;
        return book_response(
            &rendered_id,
            bids,
            asks,
            materialized.source_ts_ns,
            (inst.price_exponent, inst.qty_exponent),
            MAX_LEVELS_PER_SIDE,
            complete,
        );
    }

    // No market-by-price accumulator for this identity; fall back to the market-by-order depth
    // snapshot. PROTOCOL.md's guarantee for `depth` is that each message *self-heals* (a dropped one
    // is superseded by the next full-state message) — it says nothing about containing every level.
    // `NormalizedDepth` is always a top-`DEPTH_LEVELS` slice of a book that can hold vastly more (the
    // crate's own MBO fixture measures 44,598 real orders on one instrument), so "self-healing" must
    // not be conflated with "complete". The one case we CAN call complete: a side reporting fewer
    // than `DEPTH_LEVELS` entries proves the producer had nothing further to send for it — there is
    // nothing to truncate. At exactly `DEPTH_LEVELS`, more may exist beyond what we can see, so the
    // honest answer is "we don't know" — `complete: false`, not a guess.
    let depth_entry = {
        let d = crate::model::lock(&state.depth);
        d.get(&(inst.venue.clone(), inst.symbol.clone())).cloned()
    };
    if let Some(d) = depth_entry {
        let complete = d.bids.len() < DEPTH_LEVELS && d.asks.len() < DEPTH_LEVELS;
        let bids: Vec<(f64, f64)> = d.bids.iter().map(|b| (b[0], b[1])).collect();
        let asks: Vec<(f64, f64)> = d.asks.iter().map(|a| (a[0], a[1])).collect();
        return book_response(
            &rendered_id,
            bids,
            asks,
            d.source_ts_ns,
            (inst.price_exponent, inst.qty_exponent),
            DEPTH_LEVELS,
            complete,
        );
    }

    // Neither store has this identity: no book data has arrived yet. Report that honestly rather
    // than erroring — the product itself is real, it simply has no book to show right now.
    book_response(
        &rendered_id,
        Vec::new(),
        Vec::new(),
        0,
        (inst.price_exponent, inst.qty_exponent),
        MAX_LEVELS_PER_SIDE,
        false,
    )
}

/// Assemble the `pricebook`/`coverage` envelope. `levels_capped_at` and `complete` are the caller's
/// own honest completeness verdict for whichever path served this book (see [`book`] — the
/// market-by-price and market-by-order paths derive them differently, since one is a serving cap we
/// impose ourselves and the other is a producer-imposed slice we can only partially see past). This
/// function's own [`MAX_LEVELS_PER_SIDE`] truncation is a last-resort defensive bound (it never
/// actually binds for the market-by-order path, since `DEPTH_LEVELS` is far smaller) and does not by
/// itself downgrade `complete` — the caller has already accounted for whatever cap applies to it.
fn book_response(
    product_id: &str,
    mut bids: Vec<(f64, f64)>,
    mut asks: Vec<(f64, f64)>,
    time_ns: u64,
    (price_exponent, qty_exponent): (i8, i8),
    levels_capped_at: usize,
    complete: bool,
) -> Response {
    bids.truncate(MAX_LEVELS_PER_SIDE);
    asks.truncate(MAX_LEVELS_PER_SIDE);
    let levels_returned = bids.len() + asks.len();

    let render = |levels: &[(f64, f64)]| -> Vec<Value> {
        levels
            .iter()
            .map(|(p, s)| {
                json!([
                    decimal_string(*p, price_exponent),
                    decimal_string(*s, qty_exponent)
                ])
            })
            .collect()
    };

    ok_json(json!({
        "pricebook": {
            "product_id": product_id,
            "bids": render(&bids),
            "asks": render(&asks),
            "time": time_ns.to_string(),
        },
        "coverage": {
            "levels_returned": levels_returned,
            "levels_capped_at": levels_capped_at,
            "complete": complete,
        },
    }))
}

// ---------------------------------------------------------------------------------------------
// GET /v1/best_bid_ask
// ---------------------------------------------------------------------------------------------

fn best_bid_ask(state: &ApiState) -> Response {
    // Same discipline as `products_list`: snapshot the catalog and drop the `instruments` lock
    // before looping — `best_levels` takes `books`/`depth` per instrument, and materializing a
    // `NormalizedInstrument` clone up front is far cheaper than holding the catalog lock across
    // every instrument's book/depth lookup.
    let instruments: Vec<NormalizedInstrument> = {
        let map = crate::model::lock(&state.instruments);
        map.values().cloned().collect()
    };
    let mut counts: HashMap<(u16, String), usize> = HashMap::new();
    for i in &instruments {
        *counts
            .entry((i.source_id, i.symbol.to_string()))
            .or_insert(0) += 1;
    }
    let mut pricebooks = Vec::new();
    for i in &instruments {
        let (bid, ask) = best_levels(state, i);
        if bid.is_none() && ask.is_none() {
            // Nothing derivable for this identity (no persisted quote cache — see `best_levels`'s
            // docs); omitting it is honest, a zeroed/fabricated level would not be.
            continue;
        }
        let ambiguous = counts
            .get(&(i.source_id, i.symbol.to_string()))
            .copied()
            .unwrap_or(1)
            > 1;
        let product_id = products::ProductId {
            source_id: i.source_id,
            symbol: i.symbol.clone(),
            channel: i.channel,
            instrument_id: i.instrument_id,
        }
        .render(ambiguous);
        pricebooks.push(json!({
            "product_id": product_id,
            "bids": bid.map(|(p, s)| vec![json!([decimal_string(p, i.price_exponent), decimal_string(s, i.qty_exponent)])]).unwrap_or_default(),
            "asks": ask.map(|(p, s)| vec![json!([decimal_string(p, i.price_exponent), decimal_string(s, i.qty_exponent)])]).unwrap_or_default(),
        }));
    }
    ok_json(json!({ "pricebooks": pricebooks }))
}

// ---------------------------------------------------------------------------------------------
// GET /v1/status
// ---------------------------------------------------------------------------------------------

fn status(state: &ApiState) -> Response {
    let mut venues: Vec<&'static str> = FEEDS.iter().map(|f| f.venue).collect();
    venues.sort_unstable();
    venues.dedup();
    let venues_json: Vec<Value> = venues
        .iter()
        .map(|v| {
            json!({
                "venue": v,
                "status": if state.health.venue_up(v) { "online" } else { "offline" },
            })
        })
        .collect();

    let history_json = {
        let store = crate::model::lock(&state.history);
        json!({
            "products_tracked": store.len(),
            "late_drops": store.late_drops(),
            "evicted": store.evicted(),
            "window_seconds": history::WINDOW_SECS,
        })
    };

    ok_json(json!({ "venues": venues_json, "history": history_json }))
}

// ---------------------------------------------------------------------------------------------
// Error envelopes — every one names a remedy, not just the fact of failure.
// ---------------------------------------------------------------------------------------------

fn product_not_found(id: &str) -> Response {
    error_json(
        "404 Not Found",
        "product_not_found",
        format!("No product \"{id}\"."),
        "Run `doublezero-edge products list` to see available products.",
    )
}

fn invalid_product_id(id: &str) -> Response {
    error_json(
        "400 Bad Request",
        "invalid_product_id",
        format!("\"{id}\" is not a valid product id."),
        "Use SOURCE:SYMBOL (e.g. HYPERLIQUID:BTC); add #<channel>.<instrument_id> only to \
         disambiguate a symbol that collides within its source.",
    )
}

/// A well-formed, resolved product id followed by a path segment this API does not serve — e.g. a
/// typo'd `/v1/products/HYPERLIQUID:BTC/tikcer`. Distinct from [`unknown_endpoint`] (an entirely
/// unrecognised top-level path) so the remedy can name the actual valid choices for *this* product.
fn unknown_subresource(got: &str) -> Response {
    error_json(
        "404 Not Found",
        "unknown_subresource",
        format!("\"{got}\" is not a product sub-resource."),
        "Use one of: (none, for the product itself), ticker, candles, book.",
    )
}

/// A top-level path this API does not serve at all.
fn unknown_endpoint(path: &str) -> Response {
    error_json(
        "404 Not Found",
        "unknown_endpoint",
        format!("\"{path}\" is not a route this API serves."),
        "Use one of: /v1/products, /v1/products/{id}, /v1/products/{id}/ticker, \
         /v1/products/{id}/candles, /v1/products/{id}/book, /v1/best_bid_ask, /v1/status.",
    )
}

/// `limit` present but not a positive integer (a parse failure, or `0`). Silently falling back to
/// the default (as an absent `limit` does) would hide a caller's typo the same way an unrecognised
/// `granularity` must not be hidden.
fn invalid_limit(got: &str) -> Response {
    error_json(
        "400 Bad Request",
        "invalid_limit",
        format!("\"{got}\" is not a valid limit."),
        &format!("Use a positive integer, up to {MAX_CANDLE_LIMIT}."),
    )
}

/// An ambiguous bare id is an error that names its alternatives — never a silent pick. `candidates`
/// is surfaced both in `message` (for a human/log line) and as its own array (for a caller that
/// wants to machine-parse the choices without re-deriving them).
fn ambiguous_response(id: &str, candidates: Vec<String>) -> Response {
    json_status(
        "409 Conflict",
        json!({
            "error": "ambiguous_product",
            "message": format!("\"{id}\" matches more than one market: {}.", candidates.join(", ")),
            "remediation": "Disambiguate using one of the listed candidates.",
            "candidates": candidates,
        }),
    )
}

fn error_json(status: &'static str, error: &str, message: String, remediation: &str) -> Response {
    json_status(
        status,
        json!({ "error": error, "message": message, "remediation": remediation }),
    )
}

fn ok_json(v: Value) -> Response {
    json_status("200 OK", v)
}

fn json_status(status: &'static str, v: Value) -> Response {
    (
        status,
        "application/json".to_string(),
        serde_json::to_vec(&v).unwrap_or_default(),
    )
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use tokio::net::TcpListener;

    use super::*;
    use crate::{
        history::{Print, Store},
        ingest::{
            arbiter::{Arbiter, SharedArbiter},
            codec_mbp,
            feeds::FeedKind,
            health::FeedHealth,
            processor::MbpProcessor,
            receiver::{FrameCtx, FrameProcessor, PortRole},
        },
        model::{
            BookAccumulator, BookAction, BookChange, BookSide, NormalizedBook, NormalizedDepth,
        },
    };

    fn inst(
        source_id: u16,
        venue: &str,
        symbol: &str,
        channel: u32,
        instrument_id: u32,
        price_exponent: i8,
        qty_exponent: i8,
    ) -> NormalizedInstrument {
        NormalizedInstrument {
            venue: venue.into(),
            source: venue.into(),
            source_id,
            symbol: symbol.into(),
            channel,
            instrument_id,
            price_exponent,
            qty_exponent,
        }
    }

    fn empty_state() -> (
        InstrumentSnapshot,
        DepthSnapshot,
        BookSnapshot,
        Arc<Mutex<Store>>,
        SharedFeedHealth,
    ) {
        (
            Arc::new(Mutex::new(HashMap::new())),
            Arc::new(Mutex::new(HashMap::new())),
            Arc::new(Mutex::new(HashMap::new())),
            Arc::new(Mutex::new(Store::new())),
            Arc::new(FeedHealth::new()),
        )
    }

    /// Spawn `serve` over an ephemeral listener and return its base URL. The join handle is
    /// dropped intentionally (matches `sinks::metrics`'s test pattern) — the spawned task is
    /// aborted implicitly when the test process exits.
    async fn spawn(
        instruments: InstrumentSnapshot,
        depth: DepthSnapshot,
        books: BookSnapshot,
        history: Arc<Mutex<Store>>,
        health: SharedFeedHealth,
    ) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let _ = serve(listener, instruments, depth, books, history, health).await;
        });
        format!("http://{addr}")
    }

    // -----------------------------------------------------------------------------------------
    // The seven cases the brief names
    // -----------------------------------------------------------------------------------------

    #[tokio::test]
    async fn products_list_carries_discrete_identity_fields() {
        let (instruments, depth, books, history, health) = empty_state();
        instruments.lock().unwrap().insert(
            ("HYPERLIQUID".into(), 0u32, 41u32),
            inst(1, "HYPERLIQUID", "BTC", 0, 41, -2, -5),
        );
        health.register(("HYPERLIQUID", "perps", FeedKind::TopOfBook, 9001), |_| {});

        let base = spawn(instruments, depth, books, history, health).await;
        let resp = reqwest::get(format!("{base}/v1/products")).await.unwrap();
        assert_eq!(resp.status(), 200);
        let body: Value = resp.json().await.unwrap();
        let products = body["products"].as_array().unwrap();
        assert_eq!(products.len(), 1);
        let p = &products[0];
        assert_eq!(p["product_id"], "HYPERLIQUID:BTC");
        assert_eq!(p["source_id"], 1);
        assert_eq!(p["source"], "HYPERLIQUID");
        assert_eq!(p["symbol"], "BTC");
        assert_eq!(p["channel"], 0);
        assert_eq!(p["instrument_id"], 41);
        assert_eq!(p["price_increment"], "0.01");
        assert_eq!(p["base_increment"], "0.00001");
        assert_eq!(p["status"], "online");
        // Hyperliquid carries two `FEEDS` kinds (Top-of-Book + Market-by-Order) and this fixture
        // has no `BookSnapshot`/`DepthSnapshot` entry for it, so which row serves it is genuinely
        // unknown — see `feed_kind_ladder_prefers_book_then_depth_then_registry_then_unknown`.
        assert_eq!(p["feed_kind"], "unknown");
    }

    #[tokio::test]
    async fn an_unknown_product_404s_with_a_remedy() {
        let (instruments, depth, books, history, health) = empty_state();
        instruments.lock().unwrap().insert(
            ("HYPERLIQUID".into(), 0u32, 41u32),
            inst(1, "HYPERLIQUID", "BTC", 0, 41, -2, -5),
        );

        let base = spawn(instruments, depth, books, history, health).await;
        let resp = reqwest::get(format!("{base}/v1/products/HYPERLIQUID:BTCC"))
            .await
            .unwrap();
        assert_eq!(resp.status(), 404);
        let body: Value = resp.json().await.unwrap();
        assert_eq!(body["error"], "product_not_found");
        assert!(body["message"]
            .as_str()
            .unwrap()
            .contains("HYPERLIQUID:BTCC"));
        assert!(
            body["remediation"]
                .as_str()
                .unwrap()
                .contains("products list"),
            "remediation must name the fix: {body}"
        );
    }

    /// Extract the real captured `InstrumentDefinition`s for `ids` from a Market-by-Price refdata
    /// fixture (`[u32 LE frame length][frame bytes]` records). Reimplemented in miniature rather
    /// than reused from `tests/common/replay.rs`, which lives in a separate test crate this unit
    /// test cannot reach. One definition per id (the capture's periodic reannounce repeats each one
    /// across several manifest bursts; first sighting wins, matching how the real subscriber state
    /// machine treats a same-`instrument_id` redefinition).
    fn real_mbp_definitions(path: &str, ids: &[u32]) -> Vec<codec_mbp::InstrumentDefinition> {
        let bytes = std::fs::read(path).unwrap_or_else(|e| panic!("read {path}: {e}"));
        let mut out: HashMap<u32, codec_mbp::InstrumentDefinition> = HashMap::new();
        let mut off = 0usize;
        while off < bytes.len() {
            let len =
                u32::from_le_bytes([bytes[off], bytes[off + 1], bytes[off + 2], bytes[off + 3]])
                    as usize;
            off += 4;
            let frame = &bytes[off..off + len];
            off += len;
            let Ok((_h, msgs)) = codec_mbp::decode_frame(frame) else {
                continue;
            };
            for m in msgs {
                if let codec_mbp::Message::InstrumentDefinition(d) = m {
                    if ids.contains(&d.instrument_id) {
                        out.entry(d.instrument_id).or_insert(d);
                    }
                }
            }
        }
        ids.iter().filter_map(|id| out.remove(id)).collect()
    }

    /// Drive the real collision in `tests/fixtures/mbp.refdata.bin` through the real
    /// `MbpProcessor` — `EAVE-27JAN01-YES` genuinely names two distinct markets, instrument_ids
    /// 1165 and 1403, both on channel 120 (see `tests/fixtures/PROVENANCE.md`) — so the resulting
    /// `InstrumentSnapshot` is exactly what production ingest would produce for this fixture, not
    /// a hand-built map. Both definitions are v1 (no wire Source ID; the whole capture carries zero
    /// v3 definitions), so a bare definition never reaches `InstrumentSnapshot` on its own
    /// (`ingest::processor`'s per-instrument deferral) — and the capture's own market data covers
    /// neither id (it is filtered to a different instrument), so a minimal synthetic `Trade`
    /// reveals each one under Lashay's registered Source ID instead.
    fn ingest_real_mbp_collision(instruments: &InstrumentSnapshot) {
        const CHANNEL: u8 = 120;
        const LASHAY_SOURCE_ID: u16 = 3;
        let defs = real_mbp_definitions("tests/fixtures/mbp.refdata.bin", &[1165, 1403]);
        assert_eq!(
            defs.len(),
            2,
            "fixture must still carry both colliding definitions"
        );
        assert!(
            defs.iter().all(|d| &*d.symbol == "EAVE-27JAN01-YES"),
            "fixture no longer carries the known collision: {:?}",
            defs.iter().map(|d| &*d.symbol).collect::<Vec<_>>()
        );

        let (tx, _rx) = tokio::sync::broadcast::channel(64);
        let arbiter: SharedArbiter = Arc::new(Mutex::new(Arbiter::new(tx, 8)));
        let publisher = std::net::IpAddr::V4(std::net::Ipv4Addr::new(10, 0, 0, 9));
        let ctx = |role: PortRole| FrameCtx {
            venue: "TestLashay",
            category: "testcategory",
            arbiter: &arbiter,
            instruments,
            kernel_rx_ts_ns: 0,
            recv_ts_ns: 0,
            role,
            publisher,
        };
        let mut proc = MbpProcessor::new(Arc::new(std::sync::atomic::AtomicBool::new(false)));

        let manifest = codec_mbp::tests::enc_manifest_summary(&codec_mbp::ManifestSummary {
            channel_id: CHANNEL,
            valid: true,
            manifest_seq: defs[0].manifest_seq,
            instrument_count: defs.len() as u32,
            ts: 0,
        });
        let mut refdata_msgs = vec![manifest];
        refdata_msgs.extend(defs.iter().map(codec_mbp::tests::enc_instrument_definition));
        proc.on_datagram(
            &codec_mbp::tests::frame(CHANNEL, 0, 0, &refdata_msgs),
            &ctx(PortRole::Refdata),
        );

        // A definition alone never reaches `InstrumentSnapshot`; reveal each id under Lashay's
        // real Source ID via a minimal `Trade` (the capture's own mktdata does not cover either).
        for (n, id) in [1165u32, 1403u32].into_iter().enumerate() {
            let reveal = codec_mbp::tests::enc_trade(&codec_mbp::Trade {
                instrument_id: id,
                source_id: LASHAY_SOURCE_ID,
                aggressor_side: 0,
                trade_flags: 0,
                source_ts: 0,
                trade_price_raw: 1,
                trade_qty_raw: 1,
                trade_id: 0,
                cumulative_volume_raw: 0,
            });
            proc.on_datagram(
                &codec_mbp::tests::frame(CHANNEL, 0, 1 + n as u64, &[reveal]),
                &ctx(PortRole::Mktdata),
            );
        }
    }

    /// The root defect this task exists to fix (Task 4b), driven through the real ingest path
    /// rather than a hand-built map: two genuinely different markets that share the
    /// price-aggregated protocol's truncated 16-byte display symbol must both survive
    /// `InstrumentSnapshot`'s upsert — the collision happens at INSERT time, so a map keyed on the
    /// mutable display label had the second insert silently destroy the first's entry — must both
    /// list in `/v1/products`, and must resolve `Ambiguous` at the one endpoint that exists to
    /// disambiguate them. Uses the real captured collision in `tests/fixtures/mbp.refdata.bin`
    /// rather than a synthetic one (see `ingest_real_mbp_collision`).
    #[tokio::test]
    async fn an_ambiguous_product_id_lists_its_candidates() {
        let (instruments, depth, books, history, health) = empty_state();
        ingest_real_mbp_collision(&instruments);

        let base = spawn(instruments, depth, books, history, health).await;

        // Both markets survive real ingest and are listed.
        let list = reqwest::get(format!("{base}/v1/products")).await.unwrap();
        assert_eq!(list.status(), 200);
        let body: Value = list.json().await.unwrap();
        let collided: Vec<u64> = body["products"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|p| p["symbol"] == "EAVE-27JAN01-YES")
            .map(|p| p["instrument_id"].as_u64().unwrap())
            .collect();
        assert_eq!(
            collided.len(),
            2,
            "both colliding markets must survive real ingest, got {body}"
        );

        let resp = reqwest::get(format!("{base}/v1/products/KALSHI:EAVE-27JAN01-YES"))
            .await
            .unwrap();
        assert_eq!(resp.status(), 409);
        let body: Value = resp.json().await.unwrap();
        assert_eq!(body["error"], "ambiguous_product");
        let mut candidates: Vec<String> = body["candidates"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_str().unwrap().to_string())
            .collect();
        candidates.sort();
        assert_eq!(
            candidates,
            vec![
                "KALSHI:EAVE-27JAN01-YES#120.1165".to_string(),
                "KALSHI:EAVE-27JAN01-YES#120.1403".to_string(),
            ],
            "never a silent pick: both candidates must be listed, got {body}"
        );
    }

    /// Pins the *actual* values in `retention`, not just their shape. A prior version of this test
    /// asserted only `is_string()`/`is_boolean()` plus the fixed `window_seconds`, which a
    /// hardcoded `{"oldest":"0","newest":"0","truncated":false}` (i.e. `store.retention()` never
    /// called at all) would also satisfy — caught by a revert that produced no failure (see the
    /// report). This version ingests five known one-minute-apart prints and checks the real
    /// `oldest`/`newest` seconds, plus both directions of `truncated` (a `limit` that binds true,
    /// one that doesn't false) against the SAME pre-limit span, per `history.rs`'s own contract.
    #[tokio::test]
    async fn candles_carry_a_retention_block() {
        let (instruments, depth, books, history, health) = empty_state();
        instruments.lock().unwrap().insert(
            ("HYPERLIQUID".into(), 0u32, 41u32),
            inst(1, "HYPERLIQUID", "BTC", 0, 41, -2, -5),
        );
        let now = crate::model::now_ns() / 1_000_000_000;
        // Five prints, exactly one minute apart, all safely closed (well inside the window, well
        // before the not-yet-closed current second) — five distinct ONE_MINUTE candles with a
        // known, deterministic span.
        let base_ts = now - 300;
        let expected_oldest = (base_ts / 60) * 60;
        let expected_newest = expected_oldest + 4 * 60;
        {
            let mut store = history.lock().unwrap();
            let key = crate::history::Key {
                source_id: 1,
                channel: 0,
                instrument_id: 41,
            };
            for i in 0..5u64 {
                store.ingest(
                    key,
                    Print {
                        ts_ns: (base_ts + i * 60) * 1_000_000_000,
                        price: 100.0 + i as f64,
                        size: 1.0,
                    },
                );
            }
        }

        let base = spawn(instruments, depth, books, history, health).await;

        // A `limit` small enough to bind: `truncated` must be true, and `oldest`/`newest` must
        // still report the pre-limit span (all 5 candles), not the 2 actually returned.
        let resp = reqwest::get(format!(
            "{base}/v1/products/HYPERLIQUID:BTC/candles?granularity=ONE_MINUTE&limit=2"
        ))
        .await
        .unwrap();
        assert_eq!(resp.status(), 200);
        let body: Value = resp.json().await.unwrap();
        assert_eq!(body["candles"].as_array().unwrap().len(), 2);
        assert_eq!(body["retention"]["window_seconds"], 3600);
        assert_eq!(
            body["retention"]["oldest"],
            expected_oldest.to_string(),
            "{body}"
        );
        assert_eq!(
            body["retention"]["newest"],
            expected_newest.to_string(),
            "{body}"
        );
        assert_eq!(
            body["retention"]["truncated"], true,
            "limit=2 against 5 candles must bind: {body}"
        );
        for c in body["candles"].as_array().unwrap() {
            assert!(c["start"].is_string());
            assert!(c["open"].is_string());
            assert!(c["close"].is_string());
        }

        // A `limit` that does not bind: same span, `truncated` now false.
        let resp = reqwest::get(format!(
            "{base}/v1/products/HYPERLIQUID:BTC/candles?granularity=ONE_MINUTE&limit=50"
        ))
        .await
        .unwrap();
        let body: Value = resp.json().await.unwrap();
        assert_eq!(body["candles"].as_array().unwrap().len(), 5);
        assert_eq!(
            body["retention"]["oldest"],
            expected_oldest.to_string(),
            "{body}"
        );
        assert_eq!(
            body["retention"]["newest"],
            expected_newest.to_string(),
            "{body}"
        );
        assert_eq!(
            body["retention"]["truncated"], false,
            "limit=50 against 5 candles must not bind: {body}"
        );
    }

    /// Pins the *actual* bucket width `granularity_secs` maps each accepted name to — not just that
    /// coarser-than-the-window is accepted (see the next test). A prior version of this coverage
    /// only asserted `len() == 1` against a single-print fixture, a count identical whether
    /// `ONE_DAY` maps to 86,400s or to 60s; a transposed table entry (e.g. `SIX_HOUR = 3600`) would
    /// have shipped silently. Two prints two minutes apart: distinct candles at ONE_MINUTE, one
    /// candle at ONE_DAY — a count only the real bucket widths produce.
    #[tokio::test]
    async fn granularity_names_map_to_the_documented_bucket_width() {
        let (instruments, depth, books, history, health) = empty_state();
        instruments.lock().unwrap().insert(
            ("HYPERLIQUID".into(), 0u32, 41u32),
            inst(1, "HYPERLIQUID", "BTC", 0, 41, -2, -5),
        );
        let now = crate::model::now_ns() / 1_000_000_000;
        {
            let mut store = history.lock().unwrap();
            let key = crate::history::Key {
                source_id: 1,
                channel: 0,
                instrument_id: 41,
            };
            store.ingest(
                key,
                Print {
                    ts_ns: (now - 300) * 1_000_000_000,
                    price: 100.0,
                    size: 1.0,
                },
            );
            store.ingest(
                key,
                Print {
                    ts_ns: (now - 180) * 1_000_000_000,
                    price: 101.0,
                    size: 1.0,
                },
            );
        }

        let base = spawn(instruments, depth, books, history, health).await;

        let resp = reqwest::get(format!(
            "{base}/v1/products/HYPERLIQUID:BTC/candles?granularity=ONE_MINUTE&limit=10"
        ))
        .await
        .unwrap();
        let body: Value = resp.json().await.unwrap();
        assert_eq!(
            body["candles"].as_array().unwrap().len(),
            2,
            "two prints 120s apart must land in different 60s buckets: {body}"
        );

        let resp = reqwest::get(format!(
            "{base}/v1/products/HYPERLIQUID:BTC/candles?granularity=ONE_DAY&limit=10"
        ))
        .await
        .unwrap();
        let body: Value = resp.json().await.unwrap();
        assert_eq!(
            body["candles"].as_array().unwrap().len(),
            1,
            "both fall in the same day: {body}"
        );
    }

    #[tokio::test]
    async fn a_granularity_coarser_than_the_window_returns_a_partial_candle() {
        let (instruments, depth, books, history, health) = empty_state();
        instruments.lock().unwrap().insert(
            ("HYPERLIQUID".into(), 0u32, 41u32),
            inst(1, "HYPERLIQUID", "BTC", 0, 41, -2, -5),
        );
        let now = crate::model::now_ns() / 1_000_000_000;
        {
            let mut store = history.lock().unwrap();
            let key = crate::history::Key {
                source_id: 1,
                channel: 0,
                instrument_id: 41,
            };
            store.ingest(
                key,
                Print {
                    ts_ns: (now - 10) * 1_000_000_000,
                    price: 100.0,
                    size: 1.0,
                },
            );
        }

        let base = spawn(instruments, depth, books, history, health).await;
        // ONE_DAY (86,400s) is coarser than the store's whole 3,600s window — not an error.
        let resp = reqwest::get(format!(
            "{base}/v1/products/HYPERLIQUID:BTC/candles?granularity=ONE_DAY"
        ))
        .await
        .unwrap();
        assert_eq!(resp.status(), 200, "a coarse granularity is not an error");
        let body: Value = resp.json().await.unwrap();
        let candles = body["candles"].as_array().unwrap();
        assert_eq!(
            candles.len(),
            1,
            "one partial candle covering whatever the window holds"
        );
        assert_eq!(body["retention"]["window_seconds"], 3600);
    }

    #[tokio::test]
    async fn an_unrecognised_granularity_is_rejected_with_the_accepted_values() {
        let (instruments, depth, books, history, health) = empty_state();
        instruments.lock().unwrap().insert(
            ("HYPERLIQUID".into(), 0u32, 41u32),
            inst(1, "HYPERLIQUID", "BTC", 0, 41, -2, -5),
        );

        let base = spawn(instruments, depth, books, history, health).await;
        let resp = reqwest::get(format!(
            "{base}/v1/products/HYPERLIQUID:BTC/candles?granularity=FORTNIGHT"
        ))
        .await
        .unwrap();
        assert_eq!(resp.status(), 400);
        let body: Value = resp.json().await.unwrap();
        assert_eq!(body["error"], "invalid_granularity");
        let accepted: Vec<String> = body["accepted"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_str().unwrap().to_string())
            .collect();
        for g in [
            "ONE_MINUTE",
            "FIVE_MINUTE",
            "FIFTEEN_MINUTE",
            "THIRTY_MINUTE",
            "ONE_HOUR",
            "TWO_HOUR",
            "SIX_HOUR",
            "ONE_DAY",
        ] {
            assert!(
                accepted.contains(&g.to_string()),
                "accepted list missing {g}: {accepted:?}"
            );
        }
    }

    /// A malformed or zero `limit` must be rejected with a remedy, the same strictness an
    /// unrecognised `granularity` gets — not silently substitute the default (a typo'd `limit`
    /// would otherwise read as "everything is fine", and `limit=0` would read as "this product has
    /// no candles" instead of "you asked for zero of them").
    #[tokio::test]
    async fn malformed_limit_is_rejected_with_a_remedy() {
        let (instruments, depth, books, history, health) = empty_state();
        instruments.lock().unwrap().insert(
            ("HYPERLIQUID".into(), 0u32, 41u32),
            inst(1, "HYPERLIQUID", "BTC", 0, 41, -2, -5),
        );

        let base = spawn(instruments, depth, books, history, health).await;
        for bad in ["abc", "-5", "0"] {
            let resp = reqwest::get(format!(
                "{base}/v1/products/HYPERLIQUID:BTC/candles?limit={bad}"
            ))
            .await
            .unwrap();
            assert_eq!(resp.status(), 400, "limit={bad} must be rejected");
            let body: Value = resp.json().await.unwrap();
            assert_eq!(body["error"], "invalid_limit");
            assert!(
                body["remediation"]
                    .as_str()
                    .unwrap()
                    .contains("positive integer"),
                "remediation must name the fix: {body}"
            );
        }
    }

    fn level_update(side: BookSide, price: f64, size: f64) -> BookChange {
        BookChange {
            action: BookAction::Update,
            side,
            price,
            size,
        }
    }

    fn book_batch(
        venue: &str,
        symbol: &str,
        channel: u32,
        instrument_id: u32,
        changes: Vec<BookChange>,
        last: bool,
    ) -> NormalizedBook {
        NormalizedBook {
            venue: venue.into(),
            source: venue.into(),
            source_id: 3,
            symbol: symbol.into(),
            channel,
            instrument_id,
            changes,
            snapshot: false,
            last,
            source_ts_ns: 7,
            recv_ts_ns: 0,
            kernel_rx_ts_ns: 0,
            ws_send_ts_ns: 0,
        }
    }

    #[tokio::test]
    async fn book_reports_coverage_and_respects_baselined() {
        let (instruments, depth, books, history, health) = empty_state();
        {
            let mut map = instruments.lock().unwrap();
            map.insert(
                ("KALSHI".into(), 2u32, 41u32),
                inst(3, "KALSHI", "BASELINED", 2, 41, -4, -2),
            );
            map.insert(
                ("KALSHI".into(), 3u32, 7u32),
                inst(3, "KALSHI", "MIDSTREAM", 3, 7, -4, -2),
            );
        }
        {
            let mut map = books.lock().unwrap();

            // A fully re-baselined market: a producer `Clear` was folded in, so this holds the
            // market's whole book.
            let mut baselined = BookAccumulator::new("BASELINED".into());
            baselined.apply(&book_batch(
                "KALSHI",
                "BASELINED",
                2,
                41,
                vec![
                    BookChange {
                        action: BookAction::Clear,
                        side: BookSide::Both,
                        price: 0.0,
                        size: 0.0,
                    },
                    level_update(BookSide::Bid, 0.61, 100.0),
                    level_update(BookSide::Ask, 0.63, 50.0),
                ],
                true,
            ));
            assert!(baselined.baselined());
            map.insert(("KALSHI".into(), "perps".into(), 2, 41), baselined);

            // A market accumulated mid-stream: no `Clear` has ever been folded in, so this holds
            // only the levels that moved since accumulation started.
            let mut mid_stream = BookAccumulator::new("MIDSTREAM".into());
            mid_stream.apply(&book_batch(
                "KALSHI",
                "MIDSTREAM",
                3,
                7,
                vec![level_update(BookSide::Bid, 0.41, 5.0)],
                true,
            ));
            assert!(!mid_stream.baselined());
            map.insert(("KALSHI".into(), "perps".into(), 3, 7), mid_stream);
        }

        let base = spawn(instruments, depth, books, history, health).await;

        let resp = reqwest::get(format!("{base}/v1/products/KALSHI:BASELINED%232.41/book"))
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
        let body: Value = resp.json().await.unwrap();
        assert_eq!(
            body["coverage"]["complete"], true,
            "a re-baselined book is honestly complete"
        );
        assert_eq!(body["coverage"]["levels_returned"], 2);
        assert_eq!(body["pricebook"]["bids"][0][0], "0.6100");
        assert_eq!(body["pricebook"]["asks"][0][0], "0.6300");

        let resp = reqwest::get(format!("{base}/v1/products/KALSHI:MIDSTREAM%233.7/book"))
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
        let body: Value = resp.json().await.unwrap();
        assert_eq!(
            body["coverage"]["complete"], false,
            "a mid-stream accumulator must never claim completeness it cannot back: {body}"
        );
    }

    /// A fully re-baselined book — `baselined()` true — can still be dishonest about completeness
    /// if the serving cap silently truncates it: `complete` must go false when the cap actually
    /// cuts levels, not just when the accumulator itself is incomplete. Caught a revert of the
    /// `!cut_by_cap` term in `book_response` that `book_reports_coverage_and_respects_baselined`
    /// alone did not (its books are too small to ever hit the cap).
    #[tokio::test]
    async fn book_caps_levels_per_side_and_reports_the_cap_as_incomplete() {
        let (instruments, depth, books, history, health) = empty_state();
        instruments.lock().unwrap().insert(
            ("KALSHI".into(), 5u32, 1u32),
            inst(3, "KALSHI", "BIGBOOK", 5, 1, -4, -2),
        );
        {
            let mut map = books.lock().unwrap();
            let mut acc = BookAccumulator::new("BIGBOOK".into());
            let mut changes = vec![BookChange {
                action: BookAction::Clear,
                side: BookSide::Both,
                price: 0.0,
                size: 0.0,
            }];
            // One more bid level than the per-side cap allows.
            for i in 0..(MAX_LEVELS_PER_SIDE + 1) {
                changes.push(level_update(BookSide::Bid, 1.0 - (i as f64) * 0.0001, 1.0));
            }
            acc.apply(&book_batch("KALSHI", "BIGBOOK", 5, 1, changes, true));
            assert!(
                acc.baselined(),
                "fixture sanity: this book IS fully re-baselined"
            );
            map.insert(("KALSHI".into(), "perps".into(), 5, 1), acc);
        }

        let base = spawn(instruments, depth, books, history, health).await;
        let resp = reqwest::get(format!("{base}/v1/products/KALSHI:BIGBOOK/book"))
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
        let body: Value = resp.json().await.unwrap();
        assert_eq!(
            body["coverage"]["levels_returned"], MAX_LEVELS_PER_SIDE as u64,
            "truncated to the per-side cap: {body}"
        );
        assert_eq!(
            body["coverage"]["levels_capped_at"], MAX_LEVELS_PER_SIDE as u64,
            "the binding cap for the market-by-price path is our own per-side serving cap: {body}"
        );
        assert_eq!(
            body["coverage"]["complete"], false,
            "baselined but cap-truncated must not read as complete: {body}"
        );
    }

    /// Not one of the seven named cases, but exercises the market-by-order fallback path (no
    /// `BookSnapshot` entry, a `DepthSnapshot` one instead) with the same coverage contract.
    #[tokio::test]
    async fn book_falls_back_to_market_by_order_depth_when_no_accumulator_exists() {
        let (instruments, depth, books, history, health) = empty_state();
        instruments.lock().unwrap().insert(
            ("HYPERLIQUID".into(), 0u32, 41u32),
            inst(1, "HYPERLIQUID", "BTC", 0, 41, -2, -5),
        );
        depth.lock().unwrap().insert(
            ("HYPERLIQUID".into(), "BTC".into()),
            NormalizedDepth {
                venue: "HYPERLIQUID".into(),
                source: "HYPERLIQUID".into(),
                source_id: 1,
                symbol: "BTC".into(),
                bids: vec![[100.0, 1.0]],
                asks: vec![[101.0, 2.0]],
                source_ts_ns: 5,
                recv_ts_ns: 0,
                kernel_rx_ts_ns: 0,
                ws_send_ts_ns: 0,
            },
        );

        let base = spawn(instruments, depth, books, history, health).await;
        let resp = reqwest::get(format!("{base}/v1/products/HYPERLIQUID:BTC/book"))
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
        let body: Value = resp.json().await.unwrap();
        assert_eq!(body["coverage"]["complete"], true);
        assert_eq!(body["coverage"]["levels_returned"], 2);
        assert_eq!(
            body["coverage"]["levels_capped_at"], DEPTH_LEVELS as u64,
            "the binding cap for this path is the wire's own top-N slice, not our 50-per-side serving cap: {body}"
        );
    }

    /// `NormalizedDepth` is always a top-`DEPTH_LEVELS` slice of a book that can (and, per the
    /// crate's own MBO fixture, does — 44,598 real orders on one instrument) hold vastly more.
    /// PROTOCOL.md's guarantee for it is that each message self-heals, not that it contains every
    /// level — conflating the two used to report `complete: true` for a 20-level response here.
    /// Pins the fix: sitting exactly at `DEPTH_LEVELS` per side must NOT read as complete, since we
    /// cannot tell whether the true book holds more beyond what the producer chose to send.
    #[tokio::test]
    async fn book_market_by_order_depth_at_the_wire_cap_is_not_reported_complete() {
        let (instruments, depth, books, history, health) = empty_state();
        instruments.lock().unwrap().insert(
            ("HYPERLIQUID".into(), 0u32, 41u32),
            inst(1, "HYPERLIQUID", "BTC", 0, 41, -2, -5),
        );
        let bids: Vec<[f64; 2]> = (0..DEPTH_LEVELS).map(|i| [100.0 - i as f64, 1.0]).collect();
        let asks: Vec<[f64; 2]> = (0..DEPTH_LEVELS).map(|i| [101.0 + i as f64, 1.0]).collect();
        depth.lock().unwrap().insert(
            ("HYPERLIQUID".into(), "BTC".into()),
            NormalizedDepth {
                venue: "HYPERLIQUID".into(),
                source: "HYPERLIQUID".into(),
                source_id: 1,
                symbol: "BTC".into(),
                bids,
                asks,
                source_ts_ns: 5,
                recv_ts_ns: 0,
                kernel_rx_ts_ns: 0,
                ws_send_ts_ns: 0,
            },
        );

        let base = spawn(instruments, depth, books, history, health).await;
        let resp = reqwest::get(format!("{base}/v1/products/HYPERLIQUID:BTC/book"))
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
        let body: Value = resp.json().await.unwrap();
        assert_eq!(
            body["coverage"]["levels_returned"],
            (DEPTH_LEVELS * 2) as u64
        );
        assert_eq!(body["coverage"]["levels_capped_at"], DEPTH_LEVELS as u64);
        assert_eq!(
            body["coverage"]["complete"], false,
            "sitting exactly at the producer's own top-N cap must not read as complete: {body}"
        );
    }

    // -----------------------------------------------------------------------------------------
    // Coverage for the routes the seven named cases don't touch (I-3).
    // -----------------------------------------------------------------------------------------

    #[tokio::test]
    async fn product_detail_returns_the_full_entry() {
        let (instruments, depth, books, history, health) = empty_state();
        instruments.lock().unwrap().insert(
            ("PHOENIX".into(), 0u32, 7u32),
            inst(2, "PHOENIX", "SOL", 0, 7, -3, -4),
        );
        health.register(("PHOENIX", "spot", FeedKind::TopOfBook, 9201), |_| {});

        let base = spawn(instruments, depth, books, history, health).await;
        let resp = reqwest::get(format!("{base}/v1/products/PHOENIX:SOL"))
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
        let body: Value = resp.json().await.unwrap();
        let p = &body["product"];
        assert_eq!(p["product_id"], "PHOENIX:SOL");
        assert_eq!(p["source_id"], 2);
        assert_eq!(p["channel"], 0);
        assert_eq!(p["instrument_id"], 7);
        assert_eq!(p["price_increment"], "0.001");
        assert_eq!(p["base_increment"], "0.0001");
        assert_eq!(p["status"], "online");
        // Phoenix carries exactly one `FEEDS` kind and has no book/depth entry — an unambiguous
        // registry fallback.
        assert_eq!(p["feed_kind"], "top_of_book");
    }

    #[tokio::test]
    async fn ticker_returns_recent_trades_and_best_levels() {
        let (instruments, depth, books, history, health) = empty_state();
        instruments.lock().unwrap().insert(
            ("HYPERLIQUID".into(), 0u32, 41u32),
            inst(1, "HYPERLIQUID", "BTC", 0, 41, -2, -5),
        );
        let now = crate::model::now_ns() / 1_000_000_000;
        {
            let mut store = history.lock().unwrap();
            let key = crate::history::Key {
                source_id: 1,
                channel: 0,
                instrument_id: 41,
            };
            store.ingest(
                key,
                Print {
                    ts_ns: (now - 10) * 1_000_000_000,
                    price: 100.0,
                    size: 1.5,
                },
            );
            store.ingest(
                key,
                Print {
                    ts_ns: (now - 5) * 1_000_000_000,
                    price: 101.0,
                    size: 2.5,
                },
            );
        }
        depth.lock().unwrap().insert(
            ("HYPERLIQUID".into(), "BTC".into()),
            NormalizedDepth {
                venue: "HYPERLIQUID".into(),
                source: "HYPERLIQUID".into(),
                source_id: 1,
                symbol: "BTC".into(),
                bids: vec![[99.5, 3.0]],
                asks: vec![[100.5, 4.0]],
                source_ts_ns: 1,
                recv_ts_ns: 0,
                kernel_rx_ts_ns: 0,
                ws_send_ts_ns: 0,
            },
        );

        let base = spawn(instruments, depth, books, history, health).await;
        let resp = reqwest::get(format!("{base}/v1/products/HYPERLIQUID:BTC/ticker"))
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
        let body: Value = resp.json().await.unwrap();
        let trades = body["trades"].as_array().unwrap();
        assert_eq!(trades.len(), 2, "both prints returned: {body}");
        assert_eq!(trades[0]["price"], "101.00", "newest first: {body}");
        assert_eq!(trades[1]["price"], "100.00");
        assert_eq!(body["best_bid"], "99.50");
        assert_eq!(body["best_ask"], "100.50");
    }

    #[tokio::test]
    async fn best_bid_ask_reports_only_derivable_products() {
        let (instruments, depth, books, history, health) = empty_state();
        {
            let mut map = instruments.lock().unwrap();
            map.insert(
                ("HYPERLIQUID".into(), 0u32, 41u32),
                inst(1, "HYPERLIQUID", "BTC", 0, 41, -2, -5),
            );
            map.insert(
                ("PHOENIX".into(), 0u32, 7u32),
                inst(2, "PHOENIX", "SOL", 0, 7, -3, -4),
            );
            map.insert(
                ("KALSHI".into(), 9u32, 1u32),
                inst(3, "KALSHI", "KXBTCPERP", 9, 1, -4, -2),
            );
        }
        // Hyperliquid:BTC has a depth entry (a derivable best level); Phoenix:SOL has neither a
        // book nor a depth entry, so nothing is derivable for it — it must be omitted entirely,
        // never reported with a fabricated/zeroed level. Lashay:KXBTCPERP has a `BookAccumulator`
        // entry instead, exercising `BookAccumulator::{best_bid, best_ask}` (not just the depth
        // fallback every other case here goes through).
        depth.lock().unwrap().insert(
            ("HYPERLIQUID".into(), "BTC".into()),
            NormalizedDepth {
                venue: "HYPERLIQUID".into(),
                source: "HYPERLIQUID".into(),
                source_id: 1,
                symbol: "BTC".into(),
                bids: vec![[100.0, 1.0]],
                asks: vec![[101.0, 2.0]],
                source_ts_ns: 1,
                recv_ts_ns: 0,
                kernel_rx_ts_ns: 0,
                ws_send_ts_ns: 0,
            },
        );
        books
            .lock()
            .unwrap()
            .insert(("KALSHI".into(), "perps".into(), 9, 1), {
                let mut acc = BookAccumulator::new("KXBTCPERP".into());
                acc.apply(&book_batch(
                    "KALSHI",
                    "KXBTCPERP",
                    9,
                    1,
                    vec![
                        BookChange {
                            action: BookAction::Clear,
                            side: BookSide::Both,
                            price: 0.0,
                            size: 0.0,
                        },
                        // Two levels per side, deliberately: with only one, "best" and "worst"
                        // would be indistinguishable and a swapped best_bid()/best_ask() (highest
                        // vs. lowest) would go uncaught.
                        level_update(BookSide::Bid, 0.61, 10.0),
                        level_update(BookSide::Bid, 0.59, 30.0),
                        level_update(BookSide::Ask, 0.63, 20.0),
                        level_update(BookSide::Ask, 0.65, 40.0),
                    ],
                    true,
                ));
                acc
            });

        let base = spawn(instruments, depth, books, history, health).await;
        let resp = reqwest::get(format!("{base}/v1/best_bid_ask"))
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
        let body: Value = resp.json().await.unwrap();
        let pricebooks = body["pricebooks"].as_array().unwrap();
        assert_eq!(
            pricebooks.len(),
            2,
            "only the two derivable products appear: {body}"
        );
        let hl = pricebooks
            .iter()
            .find(|p| p["product_id"] == "HYPERLIQUID:BTC")
            .expect("Hyperliquid:BTC present");
        assert_eq!(hl["bids"][0][0], "100.00");
        assert_eq!(hl["asks"][0][0], "101.00");
        let lashay = pricebooks
            .iter()
            .find(|p| p["product_id"] == "KALSHI:KXBTCPERP")
            .expect("Lashay:KXBTCPERP present");
        assert_eq!(
            lashay["bids"][0][0], "0.6100",
            "from BookAccumulator::best_bid, not depth"
        );
        assert_eq!(
            lashay["asks"][0][0], "0.6300",
            "from BookAccumulator::best_ask, not depth"
        );
    }

    #[tokio::test]
    async fn status_reports_venue_health_and_history_counters() {
        let (instruments, depth, books, history, health) = empty_state();
        health.register(("HYPERLIQUID", "perps", FeedKind::TopOfBook, 9001), |_| {});
        {
            let mut store = history.lock().unwrap();
            let now = crate::model::now_ns() / 1_000_000_000;
            store.ingest(
                crate::history::Key {
                    source_id: 1,
                    channel: 0,
                    instrument_id: 41,
                },
                Print {
                    ts_ns: (now - 1) * 1_000_000_000,
                    price: 100.0,
                    size: 1.0,
                },
            );
        }

        let base = spawn(instruments, depth, books, history, health).await;
        let resp = reqwest::get(format!("{base}/v1/status")).await.unwrap();
        assert_eq!(resp.status(), 200);
        let body: Value = resp.json().await.unwrap();
        let venues = body["venues"].as_array().unwrap();
        let hl = venues
            .iter()
            .find(|v| v["venue"] == "HYPERLIQUID")
            .expect("Hyperliquid listed");
        assert_eq!(hl["status"], "online");
        let phoenix = venues
            .iter()
            .find(|v| v["venue"] == "PHOENIX")
            .expect("Phoenix listed");
        assert_eq!(phoenix["status"], "offline", "never registered up: {body}");
        assert_eq!(body["history"]["products_tracked"], 1);
        assert_eq!(body["history"]["window_seconds"], 3600);
    }

    /// Pins all four rungs of `feed_kind_for`'s derivation ladder against one snapshot: a
    /// `BookSnapshot` entry wins outright; failing that a `DepthSnapshot` entry; failing that, the
    /// registry only when it is unambiguous (Phoenix carries exactly one `FEEDS` kind); and
    /// `"unknown"` — never a guess — for a venue with several kinds (Lashay: Top-of-Book +
    /// Market-by-Price) and no evidence yet for this exact identity.
    #[tokio::test]
    async fn feed_kind_ladder_prefers_book_then_depth_then_registry_then_unknown() {
        let (instruments, depth, books, history, health) = empty_state();
        {
            let mut map = instruments.lock().unwrap();
            map.insert(
                ("KALSHI".into(), 9u32, 1u32),
                inst(3, "KALSHI", "BOOKED", 9, 1, -4, -2),
            );
            map.insert(
                ("HYPERLIQUID".into(), 0u32, 2u32),
                inst(1, "HYPERLIQUID", "DEPTHED", 0, 2, -2, -5),
            );
            map.insert(
                ("PHOENIX".into(), 0u32, 3u32),
                inst(2, "PHOENIX", "PLAIN", 0, 3, -3, -4),
            );
            map.insert(
                ("KALSHI".into(), 9u32, 4u32),
                inst(3, "KALSHI", "UNRESOLVED", 9, 4, -4, -2),
            );
        }
        books.lock().unwrap().insert(
            ("KALSHI".into(), "perps".into(), 9, 1),
            BookAccumulator::new("BOOKED".into()),
        );
        depth.lock().unwrap().insert(
            ("HYPERLIQUID".into(), "DEPTHED".into()),
            NormalizedDepth {
                venue: "HYPERLIQUID".into(),
                source: "HYPERLIQUID".into(),
                source_id: 1,
                symbol: "DEPTHED".into(),
                bids: Vec::new(),
                asks: Vec::new(),
                source_ts_ns: 0,
                recv_ts_ns: 0,
                kernel_rx_ts_ns: 0,
                ws_send_ts_ns: 0,
            },
        );

        let base = spawn(instruments, depth, books, history, health).await;
        let resp = reqwest::get(format!("{base}/v1/products")).await.unwrap();
        let body: Value = resp.json().await.unwrap();
        let products = body["products"].as_array().unwrap().clone();
        let kind_of = |symbol: &str| -> String {
            products
                .iter()
                .find(|p| p["symbol"] == symbol)
                .unwrap_or_else(|| panic!("{symbol} missing from {products:?}"))["feed_kind"]
                .as_str()
                .unwrap()
                .to_string()
        };
        assert_eq!(
            kind_of("BOOKED"),
            "market_by_price",
            "a BookSnapshot entry wins outright"
        );
        assert_eq!(
            kind_of("DEPTHED"),
            "market_by_order",
            "a DepthSnapshot entry wins over the registry"
        );
        assert_eq!(
            kind_of("PLAIN"),
            "top_of_book",
            "Phoenix has exactly one FEEDS kind"
        );
        assert_eq!(
            kind_of("UNRESOLVED"),
            "unknown",
            "Lashay has two FEEDS kinds and no evidence yet for this identity — never guess"
        );
    }

    #[tokio::test]
    async fn unknown_endpoint_404s_with_a_remedy() {
        let (instruments, depth, books, history, health) = empty_state();
        let base = spawn(instruments, depth, books, history, health).await;
        let resp = reqwest::get(format!("{base}/v1/nope")).await.unwrap();
        assert_eq!(resp.status(), 404);
        let body: Value = resp.json().await.unwrap();
        assert_eq!(body["error"], "unknown_endpoint");
        assert!(
            body["remediation"]
                .as_str()
                .unwrap()
                .contains("/v1/products"),
            "{body}"
        );
    }

    #[tokio::test]
    async fn unknown_subresource_404s_with_a_remedy() {
        let (instruments, depth, books, history, health) = empty_state();
        instruments.lock().unwrap().insert(
            ("HYPERLIQUID".into(), 0u32, 41u32),
            inst(1, "HYPERLIQUID", "BTC", 0, 41, -2, -5),
        );
        let base = spawn(instruments, depth, books, history, health).await;
        let resp = reqwest::get(format!("{base}/v1/products/HYPERLIQUID:BTC/tikcer"))
            .await
            .unwrap();
        assert_eq!(resp.status(), 404);
        let body: Value = resp.json().await.unwrap();
        assert_eq!(body["error"], "unknown_subresource");
        assert!(
            body["remediation"].as_str().unwrap().contains("ticker"),
            "{body}"
        );
    }

    // -----------------------------------------------------------------------------------------
    // products::resolve pinning (Task 1 shipped it untested).
    // -----------------------------------------------------------------------------------------

    /// Two genuinely different markets sharing a truncated display symbol within one source — a
    /// direct-construction fixture for `resolve`'s matching logic in isolation from the ingest
    /// pipeline (the real-ingest counterpart, driven through the actual `MbpProcessor`, lives in
    /// `an_ambiguous_product_id_lists_its_candidates` below). `resolve` only ever reads the map's
    /// *values*, so this is a faithful fixture regardless of the identity key's structure.
    fn colliding_snapshot() -> InstrumentSnapshot {
        let mut map = HashMap::new();
        map.insert(
            ("KALSHI".into(), 2u32, 41u32),
            inst(3, "KALSHI", "KXBTCPERP", 2, 41, -4, -2),
        );
        map.insert(
            ("KALSHI".into(), 3u32, 99u32),
            inst(3, "KALSHI", "KXBTCPERP", 3, 99, -4, -2),
        );
        Arc::new(Mutex::new(map))
    }

    #[test]
    fn resolve_a_bare_id_returns_every_candidate_rendered_with_its_suffix() {
        let snap = colliding_snapshot();
        let parsed = products::parse("KALSHI:KXBTCPERP").unwrap();
        match products::resolve(&snap, &parsed) {
            Resolution::Ambiguous(mut candidates) => {
                candidates.sort();
                assert_eq!(
                    candidates,
                    vec![
                        "KALSHI:KXBTCPERP#2.41".to_string(),
                        "KALSHI:KXBTCPERP#3.99".to_string()
                    ]
                );
            }
            _ => panic!("a bare id matching two markets must be Ambiguous"),
        }
    }

    #[test]
    fn resolve_a_suffixed_id_narrows_to_exactly_one() {
        let snap = colliding_snapshot();
        let parsed = products::parse("KALSHI:KXBTCPERP#3.99").unwrap();
        match products::resolve(&snap, &parsed) {
            Resolution::One(p) => {
                assert_eq!(p.channel, 3);
                assert_eq!(p.instrument_id, 99);
            }
            _ => panic!("a suffixed id naming an existing market must resolve to exactly One"),
        }
    }

    /// A suffixed id naming a market that does not exist is Not Found — never a fallback to the
    /// bare-symbol match, which would silently serve the wrong market.
    #[test]
    fn resolve_a_suffixed_id_naming_a_nonexistent_market_is_not_found() {
        let snap = colliding_snapshot();
        let parsed = products::parse("KALSHI:KXBTCPERP#9.999").unwrap();
        assert!(
            matches!(products::resolve(&snap, &parsed), Resolution::None),
            "a nonexistent suffixed identity must never fall back to a bare-symbol match"
        );
    }
}
