//! The latest top-of-book quote and last trade per instrument (`--quote-map`, off by default).
//!
//! Every other price this process keeps comes from a book: the market-by-price accumulator in
//! `model::BookSnapshot` and the market-by-order `depth` in `model::DepthSnapshot`. An instrument
//! that only a top-of-book feed carries therefore has no current price anywhere. This map holds one
//! [`LatestValues`] per instrument for that case, beside the books.
//!
//! **Written in one place: the arbiter's send points** (`Arbiter::set_quote_map`), the same way
//! `depth_replay` is, so the map holds only the copy that reached the wire. A mirror's copy the
//! floor drops never overwrites it.
//!
//! **Keyed by `model::BookKey`** — `(SourceKey, category, channel, instrument_id)`, the key the
//! catalog and the book replay use — never by symbol, which is a truncated label two instruments
//! can share. A producer with no instrument identity (empty `category`, the public WebSocket
//! input) is never written: [`quote_key`]/[`trade_key`] return `None` for it.
//!
//! `as_of_ns` on every value is our receive time of the last change (`recv_ts_ns`), because the
//! venue's own `source_ts_ns` may be `0`, which means unavailable.

use crate::model::{BookKey, NormalizedQuote, NormalizedTrade, Side, SourceKey};

/// The latest admitted top-of-book quote for one instrument.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct QuoteEntry {
    pub bid: f64,
    pub ask: f64,
    pub bid_size: f64,
    pub ask_size: f64,
    pub bid_n: u16,
    pub ask_n: u16,
    /// The venue's time, `0` when unavailable.
    pub source_ts_ns: u64,
    /// The venue's per-instrument update counter (low 32 bits), `0` when unavailable.
    pub venue_update_id: u32,
    /// Our receive time of this quote.
    pub as_of_ns: u64,
}

impl QuoteEntry {
    pub fn of(q: &NormalizedQuote) -> Self {
        Self {
            bid: q.bid,
            ask: q.ask,
            bid_size: q.bid_size,
            ask_size: q.ask_size,
            bid_n: q.bid_n,
            ask_n: q.ask_n,
            source_ts_ns: q.source_ts_ns,
            venue_update_id: q.venue_update_id,
            as_of_ns: q.recv_ts_ns,
        }
    }
}

/// The latest admitted trade for one instrument.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct TradeEntry {
    pub price: f64,
    pub size: f64,
    pub side: Side,
    pub trade_id: u64,
    /// The venue's time, `0` when unavailable.
    pub source_ts_ns: u64,
    /// Our receive time of this trade.
    pub as_of_ns: u64,
}

impl TradeEntry {
    pub fn of(t: &NormalizedTrade) -> Self {
        Self {
            price: t.price,
            size: t.size,
            side: t.aggressor_side,
            trade_id: t.trade_id,
            source_ts_ns: t.source_ts_ns,
            as_of_ns: t.recv_ts_ns,
        }
    }
}

/// One instrument's entry. Each half has its own writer and its own purges: a reset of the
/// top-of-book channel drops `quote` and leaves `trade`, which is still the last trade. An entry
/// with neither half is removed rather than kept empty.
#[derive(Debug, Default, Clone, PartialEq)]
pub struct LatestValues {
    pub quote: Option<QuoteEntry>,
    pub trade: Option<TradeEntry>,
}

/// The key a quote is filed under, or `None` for a producer with no instrument identity.
pub fn quote_key(q: &NormalizedQuote) -> Option<BookKey> {
    (!q.category.is_empty()).then(|| {
        (
            SourceKey::of(q),
            q.category.clone(),
            q.channel,
            q.instrument_id,
        )
    })
}

/// The key a trade is filed under, or `None` for a producer with no instrument identity.
pub fn trade_key(t: &NormalizedTrade) -> Option<BookKey> {
    (!t.category.is_empty()).then(|| {
        (
            SourceKey::of(t),
            t.category.clone(),
            t.channel,
            t.instrument_id,
        )
    })
}
