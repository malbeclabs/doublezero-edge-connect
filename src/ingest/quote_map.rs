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

use std::collections::HashMap;

use crate::model::{
    BookAccumulator, BookKey, NormalizedDepth, NormalizedQuote, NormalizedTrade, QuoteMap, Side,
    SourceKey,
};

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

    /// Whether this trade is the newer print than `held`, so it may replace it as the last trade.
    /// The trade dedup admits any id it has not seen, in arrival order, so a mirror's late copy
    /// of an older print reaches here after the newer one. Venue time decides; at equal times the
    /// trade id breaks the tie. Where either has no venue time (`0`), or no id at a tie, there is
    /// no order to keep and the later arrival replaces.
    pub fn supersedes(&self, held: &TradeEntry) -> bool {
        if self.source_ts_ns == 0 || held.source_ts_ns == 0 {
            return true;
        }
        if self.source_ts_ns != held.source_ts_ns {
            return self.source_ts_ns > held.source_ts_ns;
        }
        if self.trade_id == 0 || held.trade_id == 0 {
            return true;
        }
        self.trade_id > held.trade_id
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

// ---------------------------------------------------------------------------------------------
// Candidates: what each source would answer for one instrument, and the order between them.
//
// These are for a periodic snapshot builder only. It reads each store (this map, the book
// replay, the depth replay) in its own pass, taking that store's lock once and never two at a
// time, and hands each instrument's values here. None of these functions takes a lock except
// `copy_quote_map`. A request handler must never call them: the stores' locks are the ones the
// arbiter takes on every message. They mirror `sinks::api`'s `best_levels`, which is left as is.
// ---------------------------------------------------------------------------------------------

/// Which store answered.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QuoteSource {
    /// The top-of-book quote.
    Tob,
    /// The market-by-price book's top level.
    Mbp,
    /// The market-by-order `depth`'s top level.
    Mbo,
}

/// One source's best bid and ask, each `(price, size)`, with our receive time of the value.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Bbo {
    pub bid: Option<(f64, f64)>,
    pub ask: Option<(f64, f64)>,
    pub source: QuoteSource,
    pub as_of_ns: u64,
}

/// The top-of-book candidate: the quote half of an entry, if it holds one. A side with no size is
/// no level — the decoder turns a side the publisher left out into zeros — so it answers `None`,
/// as the book candidates do for an empty side, rather than a `0.0` price that would win over a
/// whole book. A quote with neither side is no candidate.
pub fn tob_candidate(v: &LatestValues) -> Option<Bbo> {
    let q = v.quote?;
    let side = |price: f64, size: f64| (size > 0.0).then_some((price, size));
    let (bid, ask) = (side(q.bid, q.bid_size), side(q.ask, q.ask_size));
    (bid.is_some() || ask.is_some()).then_some(Bbo {
        bid,
        ask,
        source: QuoteSource::Tob,
        as_of_ns: q.as_of_ns,
    })
}

/// The market-by-price candidate. Only a whole price book answers: one not yet re-baselined holds
/// only the levels that moved since it started accumulating, and one mid-event holds a top the
/// venue never published, so either could name a best level that is not the real best. An
/// order-level accumulator is not a price book and answers through `depth` instead.
pub fn mbp_candidate(acc: &BookAccumulator) -> Option<Bbo> {
    if acc.is_order_level() || !acc.baselined() || !acc.pending_empty() {
        return None;
    }
    let (bid, ask) = (acc.best_bid(), acc.best_ask());
    (bid.is_some() || ask.is_some()).then_some(Bbo {
        bid,
        ask,
        source: QuoteSource::Mbp,
        as_of_ns: acc.wire_ts_ns(),
    })
}

/// The market-by-order candidate: `depth`'s first level per side. `depth` is keyed by symbol, so
/// where the symbol is `ambiguous` (two instruments share it) it cannot say whose book it holds and
/// answers nothing.
pub fn mbo_candidate(d: &NormalizedDepth, ambiguous: bool) -> Option<Bbo> {
    if ambiguous {
        return None;
    }
    let bid = d.bids.first().map(|l| (l[0], l[1]));
    let ask = d.asks.first().map(|l| (l[0], l[1]));
    (bid.is_some() || ask.is_some()).then_some(Bbo {
        bid,
        ask,
        source: QuoteSource::Mbo,
        as_of_ns: d.recv_ts_ns,
    })
}

/// Every source's candidate for one instrument, and its last trade.
#[derive(Debug, Default, Clone)]
pub struct Candidates {
    pub tob: Option<Bbo>,
    pub mbp: Option<Bbo>,
    pub mbo: Option<Bbo>,
    pub last_trade: Option<TradeEntry>,
}

impl Candidates {
    /// The source order: the top-of-book quote, then the market-by-price book, then the
    /// market-by-order depth. The quote is skipped when the caller says its channel is not live
    /// (`tob_live`), so a quiet top-of-book channel falls through to a live book instead of
    /// answering with its last quote. Whether a channel is live is the caller's to decide.
    pub fn best(&self, tob_live: bool) -> Option<&Bbo> {
        self.tob
            .as_ref()
            .filter(|_| tob_live)
            .or(self.mbp.as_ref())
            .or(self.mbo.as_ref())
    }
}

/// The whole map, copied under one hold of its lock, for the snapshot builder. The hold is time
/// the arbiter can wait on its next send, so it is a plain clone and nothing else.
pub fn copy_quote_map(map: &QuoteMap) -> HashMap<BookKey, LatestValues> {
    crate::model::lock(map).clone()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{BookAction, BookChange, BookSide, NormalizedBook};
    use std::sync::{Arc, Mutex};

    fn quote_entry(bid: f64, ask: f64) -> QuoteEntry {
        QuoteEntry {
            bid,
            ask,
            bid_size: 1.0,
            ask_size: 2.0,
            bid_n: 0,
            ask_n: 0,
            source_ts_ns: 0,
            venue_update_id: 0,
            as_of_ns: 10,
        }
    }

    fn trade_entry() -> TradeEntry {
        TradeEntry {
            price: 100.5,
            size: 1.0,
            side: Side::Buy,
            trade_id: 1,
            source_ts_ns: 0,
            as_of_ns: 11,
        }
    }

    fn change(
        action: BookAction,
        side: BookSide,
        price: f64,
        size: f64,
        order_id: u64,
    ) -> BookChange {
        BookChange {
            action,
            side,
            price,
            size,
            order_id,
        }
    }

    fn batch(changes: Vec<BookChange>, last: bool, recv_ts_ns: u64) -> NormalizedBook {
        NormalizedBook {
            batch_id: None,
            venue: "KALSHI".into(),
            source_name: "KALSHI".into(),
            source_id: 3,
            symbol: "KXBTCPERP".into(),
            channel: 2,
            instrument_id: 41,
            category: "perps".into(),
            order_level: changes.iter().any(|c| c.order_id != 0),
            changes,
            snapshot: false,
            last,
            source_ts_ns: 1,
            recv_ts_ns,
            kernel_rx_ts_ns: 0,
            ws_send_ts_ns: 0,
        }
    }

    /// A price-level book holding bid 99 x 1 and ask 102 x 2 after a producer re-baseline.
    fn baselined_book() -> BookAccumulator {
        let mut acc = BookAccumulator::new("KXBTCPERP".into());
        acc.apply(&batch(
            vec![
                change(BookAction::Clear, BookSide::Both, 0.0, 0.0, 0),
                change(BookAction::Update, BookSide::Bid, 99.0, 1.0, 0),
                change(BookAction::Update, BookSide::Ask, 102.0, 2.0, 0),
            ],
            true,
            500,
        ));
        acc
    }

    fn depth(recv_ts_ns: u64) -> NormalizedDepth {
        NormalizedDepth {
            venue: "HYPERLIQUID".into(),
            source_name: "HYPERLIQUID".into(),
            source_id: 1,
            symbol: "BTC".into(),
            bids: vec![[98.0, 3.0], [97.0, 1.0]],
            asks: vec![[103.0, 4.0]],
            source_ts_ns: 1,
            recv_ts_ns,
            kernel_rx_ts_ns: 0,
            ws_send_ts_ns: 0,
        }
    }

    fn both_candidates() -> Candidates {
        let values = LatestValues {
            quote: Some(quote_entry(100.0, 101.0)),
            trade: None,
        };
        Candidates {
            tob: tob_candidate(&values),
            mbp: mbp_candidate(&baselined_book()),
            mbo: None,
            last_trade: None,
        }
    }

    #[test]
    fn tob_wins_when_live() {
        let best = both_candidates();
        let best = best.best(true).unwrap();
        assert_eq!(best.source, QuoteSource::Tob);
        assert_eq!(best.bid, Some((100.0, 1.0)));
        assert_eq!(best.ask, Some((101.0, 2.0)));
        assert_eq!(best.as_of_ns, 10);
    }

    #[test]
    fn book_wins_when_tob_is_not_live() {
        let c = both_candidates();
        let best = c.best(false).unwrap();
        assert_eq!(best.source, QuoteSource::Mbp);
        assert_eq!(best.bid, Some((99.0, 1.0)));
    }

    #[test]
    fn tob_only_instrument_gets_a_quote() {
        let c = Candidates {
            tob: tob_candidate(&LatestValues {
                quote: Some(quote_entry(100.0, 101.0)),
                trade: None,
            }),
            ..Candidates::default()
        };
        assert_eq!(c.best(true).unwrap().source, QuoteSource::Tob);
        assert!(c.best(false).is_none());
    }

    #[test]
    fn baselined_book_is_a_candidate() {
        let acc = baselined_book();
        let c = mbp_candidate(&acc).expect("a whole price book answers");
        assert_eq!(c.source, QuoteSource::Mbp);
        assert_eq!((c.bid, c.ask), (Some((99.0, 1.0)), Some((102.0, 2.0))));
        assert_eq!(c.as_of_ns, acc.wire_ts_ns());
    }

    #[test]
    fn unbaselined_book_is_not_a_candidate() {
        let mut acc = BookAccumulator::new("KXBTCPERP".into());
        acc.apply(&batch(
            vec![change(BookAction::Update, BookSide::Bid, 99.0, 1.0, 0)],
            true,
            500,
        ));
        assert!(acc.best_bid().is_some(), "fixture: it holds a level");
        assert!(mbp_candidate(&acc).is_none());
    }

    #[test]
    fn book_mid_event_is_not_a_candidate() {
        let mut acc = baselined_book();
        acc.apply(&batch(
            vec![change(BookAction::Update, BookSide::Bid, 99.5, 1.0, 0)],
            false,
            600,
        ));
        assert!(!acc.pending_empty(), "fixture: an event is open");
        assert!(mbp_candidate(&acc).is_none());
    }

    /// An order-level market is not a price book, even when its price tree holds a level from
    /// before it showed an order id: that level is not the market's inside.
    #[test]
    fn order_level_book_is_not_a_candidate() {
        let mut acc = baselined_book();
        acc.apply(&batch(
            vec![change(BookAction::Update, BookSide::Bid, 98.0, 1.0, 7)],
            true,
            600,
        ));
        assert!(acc.is_order_level(), "fixture");
        assert!(acc.best_bid().is_some(), "fixture: a price level remains");
        assert!(mbp_candidate(&acc).is_none());
    }

    #[test]
    fn ambiguous_symbol_skips_depth() {
        let d = depth(700);
        assert!(mbo_candidate(&d, true).is_none());
        let c = mbo_candidate(&d, false).unwrap();
        assert_eq!(c.source, QuoteSource::Mbo);
        assert_eq!((c.bid, c.ask), (Some((98.0, 3.0)), Some((103.0, 4.0))));
        assert_eq!(c.as_of_ns, d.recv_ts_ns);
    }

    #[test]
    fn best_falls_to_depth_last() {
        let c = Candidates {
            mbo: mbo_candidate(&depth(700), false),
            ..Candidates::default()
        };
        assert_eq!(c.best(true).unwrap().source, QuoteSource::Mbo);
    }

    /// The top-of-book decoder turns a side the publisher left out into price and size `0`. A side
    /// with no size is no level: reported as a `0.0` price it would read as a real quote and win
    /// over a whole book.
    #[test]
    fn a_side_with_no_size_is_not_a_level() {
        let one_sided = LatestValues {
            quote: Some(QuoteEntry {
                bid: 0.0,
                bid_size: 0.0,
                ..quote_entry(100.0, 101.0)
            }),
            trade: None,
        };
        let c = tob_candidate(&one_sided).expect("the ask still answers");
        assert_eq!((c.bid, c.ask), (None, Some((101.0, 2.0))));

        let empty = LatestValues {
            quote: Some(QuoteEntry {
                bid_size: 0.0,
                ask_size: 0.0,
                ..quote_entry(0.0, 0.0)
            }),
            trade: None,
        };
        assert!(tob_candidate(&empty).is_none(), "no level on either side");
    }

    #[test]
    fn quote_half_dropped_gives_no_tob_candidate() {
        let values = LatestValues {
            quote: None,
            trade: Some(trade_entry()),
        };
        assert!(tob_candidate(&values).is_none());
    }

    #[test]
    fn copy_quote_map_copies_every_entry() {
        let map: QuoteMap = Arc::new(Mutex::new(HashMap::new()));
        for id in 0..3u32 {
            crate::model::lock(&map).insert(
                (SourceKey::from_id(1), "perps".into(), 0, id),
                LatestValues {
                    quote: Some(quote_entry(f64::from(id), f64::from(id) + 1.0)),
                    trade: Some(trade_entry()),
                },
            );
        }
        let copy = copy_quote_map(&map);
        assert_eq!(copy, *crate::model::lock(&map));
        assert_eq!(copy.len(), 3);
    }

    /// The cost of the snapshot builder's one hold of the map lock at 100,000 instruments, which
    /// is time the arbiter can be held up on each publish. Prints the figure rather than asserting a
    /// bound: run with `cargo test --release copy_quote_map_100k_timing -- --ignored --nocapture`.
    #[test]
    #[ignore]
    fn copy_quote_map_100k_timing() {
        let map: QuoteMap = Arc::new(Mutex::new(HashMap::new()));
        {
            let mut m = crate::model::lock(&map);
            for id in 0..100_000u32 {
                m.insert(
                    (SourceKey::from_id(1), "perps".into(), (id % 64) as u8, id),
                    LatestValues {
                        quote: Some(quote_entry(1.0, 2.0)),
                        trade: Some(trade_entry()),
                    },
                );
            }
        }
        let runs = 20;
        let mut times = Vec::with_capacity(runs);
        for _ in 0..runs {
            let start = std::time::Instant::now();
            let copy = copy_quote_map(&map);
            times.push(start.elapsed());
            assert_eq!(copy.len(), 100_000);
        }
        times.sort();
        println!(
            "copy_quote_map at 100,000 entries over {runs} runs: min {:?}, median {:?}, max {:?}",
            times[0],
            times[runs / 2],
            times[runs - 1]
        );
    }
}
