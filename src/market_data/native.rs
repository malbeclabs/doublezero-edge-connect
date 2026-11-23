//! The native market-data format: instrument ids, freshness, and the quote, markets and
//! ambiguous-instrument bodies, as pure functions over plain values.
//!
//! Nothing here reads a store or takes a lock: the caller hands in what one instrument (or one
//! page) holds and gets JSON back. A field with no source is `null` or left out, never `0`.
//! Prices and sizes are decimal strings at the instrument's exponent; times are RFC 3339 UTC with
//! nanoseconds.

use std::{fmt::Write as _, sync::Arc};

use serde::Serialize;
use serde_json::{json, Map, Value};

use super::response::{Freshness as Status, Problem, ProblemKind};
use crate::{
    ingest::quote_map::{Bbo, QuoteSource, TradeEntry},
    model::Side,
};

/// One catalog entry named by its full key, `<source_id>:<category>:<channel>:<instrument_id>`.
/// The category is part of it because one Source ID can carry several universes whose
/// `(channel, instrument_id)` pairs and symbols collide.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NativeId {
    pub source_id: u16,
    pub category: Arc<str>,
    pub channel: u8,
    pub instrument_id: u32,
}

impl NativeId {
    pub fn render(&self) -> String {
        format!(
            "{}:{}:{}:{}",
            self.source_id, self.category, self.channel, self.instrument_id
        )
    }

    /// Accepts only the canonical spelling, so `parse(s).map(|i| i.render()) == Some(s)`: every
    /// other spelling of one id would be its own cache key and its own origin build.
    pub fn parse(s: &str) -> Option<NativeId> {
        let mut parts = s.split(':');
        let (source_id, category, channel, instrument_id) =
            (parts.next()?, parts.next()?, parts.next()?, parts.next()?);
        if parts.next().is_some() || !is_category(category) {
            return None;
        }
        Some(NativeId {
            source_id: canonical_number(source_id)?,
            category: category.into(),
            channel: canonical_number(channel)?,
            instrument_id: canonical_number(instrument_id)?,
        })
    }
}

/// Whether `s` is a registry category an id can carry: `[a-z0-9-]+`.
pub fn is_category(s: &str) -> bool {
    !s.is_empty()
        && s.bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
}

/// Plain ASCII decimal with no sign and no leading zero but `0` itself, within `T`. The digit
/// check comes first because `str::parse` accepts a leading `+`.
fn canonical_number<T: std::str::FromStr>(s: &str) -> Option<T> {
    let digits = !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit());
    if !digits || (s.len() > 1 && s.starts_with('0')) {
        return None;
    }
    s.parse().ok()
}

/// What the format needs to know about one instrument. The quote currency and asset class are
/// the operator's, from the allowlist, passed through as written.
#[derive(Debug, Clone)]
pub struct InstrumentView {
    pub id: NativeId,
    pub symbol: Arc<str>,
    pub quote_currency: Arc<str>,
    pub asset_class: Option<Arc<str>>,
    pub price_exponent: i8,
    pub qty_exponent: i8,
}

#[derive(Serialize)]
struct InstrumentJson<'a> {
    id: String,
    symbol: &'a str,
    category: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    asset_class: Option<&'a str>,
    quote_currency: &'a str,
}

impl InstrumentView {
    fn to_json(&self) -> Value {
        json!(InstrumentJson {
            id: self.id.render(),
            symbol: &self.symbol,
            category: &self.id.category,
            asset_class: self.asset_class.as_deref(),
            quote_currency: &self.quote_currency,
        })
    }
}

fn status_str(s: Status) -> &'static str {
    match s {
        Status::Live => "live",
        Status::Stale => "stale",
        Status::FeedDown => "feed_down",
        Status::Halted => "halted",
    }
}

/// One response's freshness verdict and the values it was judged from, so a caller with its own
/// clock can judge it again.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Freshness {
    pub status: Status,
    /// The channel's last confirmation of life, `0` if never.
    pub live_at_ns: u64,
    pub threshold_ms: u32,
    /// The request time the status was judged at.
    pub built_at_ns: u64,
}

impl Freshness {
    /// The status is [`Status::of`]'s; staleness never comes from `as_of` alone.
    pub fn of(
        halted: bool,
        receiver_up: bool,
        confirmed: bool,
        live_at_ns: u64,
        threshold_ms: u32,
        now_ns: u64,
    ) -> Self {
        Freshness {
            status: Status::of(halted, receiver_up, confirmed),
            live_at_ns,
            threshold_ms,
            built_at_ns: now_ns,
        }
    }

    fn to_json(self) -> Value {
        json!({
            "status": status_str(self.status),
            "live_at": opt_time(self.live_at_ns),
            "threshold_ms": self.threshold_ms,
            "built_at": rfc3339_ns(self.built_at_ns),
        })
    }
}

/// `value` as a fixed-point decimal string at `exponent`'s precision, never scientific notation.
pub fn decimal(value: f64, exponent: i8) -> String {
    // `unsigned_abs`, not negation: an exponent of `i8::MIN` comes off the wire unchecked.
    let decimals = exponent.min(0).unsigned_abs() as usize;
    format!("{value:.decimals$}")
}

/// Nanoseconds since the Unix epoch as RFC 3339 UTC, always with nine fractional digits.
pub fn rfc3339_ns(ns: u64) -> String {
    let secs = ns / 1_000_000_000;
    let (days, rem) = (secs / 86_400, secs % 86_400);
    let (y, m, d) = civil_from_days(days as i64);
    format!(
        "{y:04}-{m:02}-{d:02}T{:02}:{:02}:{:02}.{:09}Z",
        rem / 3600,
        rem % 3600 / 60,
        rem % 60,
        ns % 1_000_000_000
    )
}

/// A time whose `0` means the source gave none.
fn opt_time(ns: u64) -> Option<String> {
    (ns != 0).then(|| rfc3339_ns(ns))
}

/// Days since 1970-01-01 to a proleptic Gregorian date (H. Hinnant's `civil_from_days`).
fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    let y = yoe + era * 400 + i64::from(m <= 2);
    (y, m, d)
}

/// The URLs the bodies link to, all from configuration and never from the request, so a response
/// depends on nothing a caller can vary outside the cache key.
#[derive(Debug, Clone)]
pub struct Links {
    /// The public base URL, e.g. `https://host`.
    pub base: Arc<str>,
    /// The docs page; `None` while it is not published.
    pub docs: Option<Arc<str>>,
    /// `attribution.url`.
    pub attribution: Arc<str>,
}

impl Links {
    pub fn quote_by_id(&self, venue: &str, id: &NativeId) -> String {
        self.url("quote", &[("venue", venue), ("id", &id.render())])
    }

    pub fn quote_by_symbol(&self, venue: &str, symbol: &str) -> String {
        self.url("quote", &[("venue", venue), ("symbol", symbol)])
    }

    /// A markets page: the first with no cursor, else the page after `cursor`.
    pub fn markets(&self, venue: &str, cursor: Option<&NativeId>) -> String {
        match cursor {
            None => self.url("markets", &[("venue", venue)]),
            Some(c) => self.url("markets", &[("venue", venue), ("cursor", &c.render())]),
        }
    }

    fn url(&self, route: &str, params: &[(&str, &str)]) -> String {
        let mut url = format!("{}/v1/{route}", self.base.trim_end_matches('/'));
        for (i, (k, v)) in params.iter().enumerate() {
            url.push(if i == 0 { '?' } else { '&' });
            url.push_str(k);
            url.push('=');
            percent_encode(v, &mut url);
        }
        url
    }

    fn attribution_json(&self, venue: &str) -> Value {
        json!({ "venue": venue, "source": "DoubleZero", "url": &*self.attribution })
    }

    fn with_docs(&self, mut links: Map<String, Value>) -> Value {
        if let Some(docs) = &self.docs {
            links.insert("docs".into(), json!(&**docs));
        }
        Value::Object(links)
    }
}

/// Every byte outside RFC 3986's unreserved set as `%XX`, so a symbol's `/`, `:`, `&`, `+` or
/// space survives the round trip whatever the server's query decoder does with `+`.
fn percent_encode(s: &str, out: &mut String) {
    for b in s.bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'.' | b'_' | b'~') {
            out.push(b as char);
        } else {
            let _ = write!(out, "%{b:02X}");
        }
    }
}

fn source_name(s: QuoteSource) -> &'static str {
    match s {
        QuoteSource::Tob => "top_of_book",
        QuoteSource::Mbp => "market_by_price",
        QuoteSource::Mbo => "market_by_order",
    }
}

/// The `GET /v1/quote` body. `quote` is the chosen source's best bid and ask paired with that
/// source's venue time (`0` when the feed gave none), so the time cannot come from another source.
pub fn quote_body(
    venue: &str,
    inst: &InstrumentView,
    quote: Option<(&Bbo, u64)>,
    trade: Option<&TradeEntry>,
    freshness: &Freshness,
    links: &Links,
) -> Value {
    let level = |side: Option<(f64, f64)>| {
        side.map(|(price, size)| {
            json!({
                "price": decimal(price, inst.price_exponent),
                "size": decimal(size, inst.qty_exponent),
            })
        })
    };
    // A value with neither side is no value: nothing to date or attribute.
    let quote = quote.filter(|(q, _)| q.bid.is_some() || q.ask.is_some());
    let quote_json = match quote {
        Some((q, source_ts_ns)) => json!({
            "bid": level(q.bid),
            "ask": level(q.ask),
            "as_of": rfc3339_ns(q.as_of_ns),
            "source_time": opt_time(source_ts_ns),
            "source": source_name(q.source),
            "empty_reason": (q.bid.is_none() || q.ask.is_none()).then_some("one_sided"),
        }),
        None => json!({
            "bid": null,
            "ask": null,
            "as_of": null,
            "source_time": null,
            "source": null,
            "empty_reason": if freshness.status == Status::Halted { "halted" } else { "no_quote_yet" },
        }),
    };
    let last_trade = trade.map(|t| {
        json!({
            "price": decimal(t.price, inst.price_exponent),
            "size": decimal(t.size, inst.qty_exponent),
            "side": match t.side {
                Side::Buy => Some("buy"),
                Side::Sell => Some("sell"),
                Side::Unknown => None,
            },
            "as_of": rfc3339_ns(t.as_of_ns),
            "source_time": opt_time(t.source_ts_ns),
        })
    });
    let mut link_map = Map::new();
    link_map.insert("self".into(), json!(links.quote_by_id(venue, &inst.id)));
    link_map.insert("markets".into(), json!(links.markets(venue, None)));
    json!({
        "venue": venue,
        "instrument": inst.to_json(),
        "quote": quote_json,
        "last_trade": last_trade,
        "freshness": freshness.to_json(),
        "attribution": links.attribution_json(venue),
        "links": links.with_docs(link_map),
    })
}

/// One `GET /v1/markets` entry: price-free by construction, since it holds no value to render.
#[derive(Debug, Clone)]
pub struct MarketView {
    pub instrument: InstrumentView,
    /// The allowlist's retirement date, when the feed is retiring.
    pub retiring_on: Option<Arc<str>>,
    /// Whether no other instrument in the venue shares the symbol, which lets the quote link name
    /// the symbol instead of the id.
    pub symbol_unique: bool,
}

/// The `GET /v1/markets` body for the page after `cursor` (`None` for the first page). `next` is
/// the cursor of the following page, `None` on the last.
pub fn markets_body(
    venue: &str,
    page: &[MarketView],
    cursor: Option<&NativeId>,
    next: Option<&NativeId>,
    total: usize,
    complete: bool,
    links: &Links,
) -> Value {
    let markets: Vec<Value> = page
        .iter()
        .map(|m| {
            let inst = &m.instrument;
            let quote = if m.symbol_unique {
                links.quote_by_symbol(venue, &inst.symbol)
            } else {
                links.quote_by_id(venue, &inst.id)
            };
            let mut entry = json!({ "instrument": inst.to_json(), "links": { "quote": quote } });
            if let Some(date) = &m.retiring_on {
                entry["retiring_on"] = json!(&**date);
            }
            entry
        })
        .collect();
    let mut link_map = Map::new();
    link_map.insert("self".into(), json!(links.markets(venue, cursor)));
    if let Some(next) = next {
        link_map.insert("next".into(), json!(links.markets(venue, Some(next))));
    }
    json!({
        "venue": venue,
        "markets": markets,
        "complete": complete,
        "total": total,
        "attribution": links.attribution_json(venue),
        "links": links.with_docs(link_map),
    })
}

/// The ambiguous-symbol problem: every candidate, each with a quote link by its own id, and the
/// attribution.
pub fn ambiguous_instrument(venue: &str, candidates: &[InstrumentView], links: &Links) -> Problem {
    let candidates: Vec<Value> = candidates
        .iter()
        .map(|c| {
            json!({
                "instrument": c.to_json(),
                "links": { "quote": links.quote_by_id(venue, &c.id) },
            })
        })
        .collect();
    Problem {
        kind: ProblemKind::AmbiguousInstrument,
        detail: "The symbol names more than one instrument; request one by id.".into(),
        extensions: vec![
            ("candidates", Value::Array(candidates)),
            ("attribution", links.attribution_json(venue)),
        ],
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id(source_id: u16, category: &str, channel: u8, instrument_id: u32) -> NativeId {
        NativeId {
            source_id,
            category: category.into(),
            channel,
            instrument_id,
        }
    }

    fn inst(symbol: &str, instrument_id: u32) -> InstrumentView {
        InstrumentView {
            id: id(1, "hl-perps", 0, instrument_id),
            symbol: symbol.into(),
            quote_currency: "USDC".into(),
            asset_class: Some("perpetual_future".into()),
            price_exponent: -1,
            qty_exponent: -2,
        }
    }

    fn links() -> Links {
        Links {
            base: "https://md.example".into(),
            docs: Some("https://docs.example/md/".into()),
            attribution: "https://doublezero.xyz".into(),
        }
    }

    fn live() -> Freshness {
        Freshness::of(false, true, true, 2_000, 5_000, 3_000)
    }

    fn bbo(bid: Option<(f64, f64)>, ask: Option<(f64, f64)>) -> Bbo {
        Bbo {
            bid,
            ask,
            source: QuoteSource::Tob,
            as_of_ns: 1_795_010_587_120_451_022,
        }
    }

    fn trade(side: Side) -> TradeEntry {
        TradeEntry {
            price: 97012.0,
            size: 0.01,
            side,
            trade_id: 7,
            source_ts_ns: 1_795_010_586_900_000_000,
            as_of_ns: 1_795_010_586_902_114_377,
        }
    }

    #[test]
    fn native_id_round_trips() {
        let i = id(1, "hl-perps", 0, 3);
        assert_eq!(i.render(), "1:hl-perps:0:3");
        assert_eq!(NativeId::parse("1:hl-perps:0:3"), Some(i));
        let max = id(u16::MAX, "a-0", u8::MAX, u32::MAX);
        assert_eq!(NativeId::parse(&max.render()), Some(max));
    }

    #[test]
    fn native_id_accepts_only_canonical() {
        for bad in [
            "1:hl-perps:0",
            "1:hl-perps:0:3:4",
            "+1:hl-perps:0:3",
            "1:hl-perps:0:+3",
            "1:hl-perps:0:03",
            "1:hl-perps:00:3",
            "1:hl-perps:-0:3",
            "1:hl-perps:0: 3",
            "1:hl-perps:0:3 ",
            "1:hl-perps:256:3",
            "65536:hl-perps:0:3",
            "1:hl-perps:0:4294967296",
            "1::0:3",
            "1:HL-perps:0:3",
            "1:hl_perps:0:3",
            "1:hl-perps::3",
            "",
            ":::",
            "1:hl-perps:0:٣",
        ] {
            assert_eq!(NativeId::parse(bad), None, "{bad:?}");
        }
        for good in [
            "0:a:0:0",
            "1:hl-perps:0:3",
            "65535:xyz-perps:255:4294967295",
        ] {
            let parsed = NativeId::parse(good).expect(good);
            assert_eq!(parsed.render(), good);
        }
    }

    #[test]
    fn every_registry_category_is_id_safe() {
        let doc: Value = serde_json::from_str(include_str!("../ingest/registry.json")).unwrap();
        let feeds = doc["feeds"].as_array().unwrap();
        assert!(!feeds.is_empty());
        for feed in feeds {
            let category = feed["category"].as_str().unwrap();
            assert!(is_category(category), "{category:?}");
        }
    }

    #[test]
    fn stale_when_not_confirmed() {
        // The verdict follows the channel's confirmation, whatever the value's own age.
        let f = Freshness::of(false, true, true, 9, 5_000, 10);
        assert_eq!(f.status, Status::Live);
        let f = Freshness::of(false, true, false, 9, 5_000, 10);
        assert_eq!(f.status, Status::Stale);
        // Never live: `live_at` is null, and the verdict is stale.
        let f = Freshness::of(false, true, false, 0, 5_000, 10);
        assert_eq!(f.status, Status::Stale);
        assert_eq!(f.to_json()["live_at"], Value::Null);
        assert_eq!(f.to_json()["status"], "stale");
        assert_eq!(f.built_at_ns, 10);
    }

    #[test]
    fn freshness_json_fields() {
        let f = Freshness::of(false, false, true, 1_000_000_001, 5_000, 2_000_000_000);
        assert_eq!(
            f.to_json(),
            json!({
                "status": "feed_down",
                "live_at": "1970-01-01T00:00:01.000000001Z",
                "threshold_ms": 5000,
                "built_at": "1970-01-01T00:00:02.000000000Z",
            })
        );
    }

    #[test]
    fn decimal_is_never_scientific() {
        assert_eq!(decimal(1e-9, -9), "0.000000001");
        assert_eq!(decimal(1e12, -2), "1000000000000.00");
        assert_eq!(decimal(1e21, 0), "1000000000000000000000");
        assert_eq!(decimal(97012.0, -1), "97012.0");
        assert_eq!(decimal(5.0, 2), "5");
        assert_eq!(decimal(1.0, i8::MIN), format!("1.{}", "0".repeat(128)));
    }

    #[test]
    fn rfc3339_known_instants() {
        assert_eq!(rfc3339_ns(0), "1970-01-01T00:00:00.000000000Z");
        assert_eq!(
            rfc3339_ns(1_709_210_096_123_456_789),
            "2024-02-29T12:34:56.123456789Z"
        );
        assert_eq!(
            rfc3339_ns(951_868_800_000_000_000),
            "2000-03-01T00:00:00.000000000Z"
        );
        assert_eq!(
            rfc3339_ns(1_795_010_587_120_451_022),
            "2026-11-18T14:03:07.120451022Z"
        );
        assert_eq!(rfc3339_ns(u64::MAX), "2554-07-21T23:34:33.709551615Z");
    }

    #[test]
    fn quote_body_two_sided() {
        let q = bbo(Some((97012.0, 1.82)), Some((97013.0, 0.4)));
        let t = trade(Side::Sell);
        let body = quote_body(
            "hyperliquid",
            &inst("BTC", 3),
            Some((&q, 1_795_010_587_118_000_000)),
            Some(&t),
            &live(),
            &links(),
        );
        assert_eq!(
            body,
            json!({
                "venue": "hyperliquid",
                "instrument": {
                    "id": "1:hl-perps:0:3",
                    "symbol": "BTC",
                    "category": "hl-perps",
                    "asset_class": "perpetual_future",
                    "quote_currency": "USDC",
                },
                "quote": {
                    "bid": { "price": "97012.0", "size": "1.82" },
                    "ask": { "price": "97013.0", "size": "0.40" },
                    "as_of": "2026-11-18T14:03:07.120451022Z",
                    "source_time": "2026-11-18T14:03:07.118000000Z",
                    "source": "top_of_book",
                    "empty_reason": null,
                },
                "last_trade": {
                    "price": "97012.0",
                    "size": "0.01",
                    "side": "sell",
                    "as_of": "2026-11-18T14:03:06.902114377Z",
                    "source_time": "2026-11-18T14:03:06.900000000Z",
                },
                "freshness": {
                    "status": "live",
                    "live_at": "1970-01-01T00:00:00.000002000Z",
                    "threshold_ms": 5000,
                    "built_at": "1970-01-01T00:00:00.000003000Z",
                },
                "attribution": {
                    "venue": "hyperliquid",
                    "source": "DoubleZero",
                    "url": "https://doublezero.xyz",
                },
                "links": {
                    "self": "https://md.example/v1/quote?venue=hyperliquid&id=1%3Ahl-perps%3A0%3A3",
                    "markets": "https://md.example/v1/markets?venue=hyperliquid",
                    "docs": "https://docs.example/md/",
                },
            })
        );
    }

    #[test]
    fn quote_body_one_sided() {
        let mut q = bbo(None, Some((97013.0, 0.4)));
        q.source = QuoteSource::Mbp;
        let body = quote_body(
            "hyperliquid",
            &inst("BTC", 3),
            Some((&q, 0)),
            None,
            &live(),
            &links(),
        );
        let quote = &body["quote"];
        assert_eq!(quote["bid"], Value::Null);
        assert_eq!(quote["ask"]["price"], "97013.0");
        assert_eq!(quote["as_of"], "2026-11-18T14:03:07.120451022Z");
        assert_eq!(quote["source"], "market_by_price");
        assert_eq!(quote["empty_reason"], "one_sided");
    }

    #[test]
    fn quote_body_no_quote_yet() {
        let stale = Freshness::of(false, true, false, 0, 5_000, 3_000);
        for q in [None, Some(bbo(None, None))] {
            let body = quote_body(
                "hyperliquid",
                &inst("BTC", 3),
                q.as_ref().map(|q| (q, 123)),
                None,
                &stale,
                &links(),
            );
            assert_eq!(
                body["quote"],
                json!({
                    "bid": null, "ask": null, "as_of": null, "source_time": null,
                    "source": null, "empty_reason": "no_quote_yet",
                })
            );
            assert_eq!(body["freshness"]["status"], "stale");
        }
    }

    #[test]
    fn quote_body_halted() {
        let halted = Freshness::of(true, true, true, 2_000, 5_000, 3_000);
        let t = trade(Side::Buy);
        let body = quote_body(
            "hyperliquid",
            &inst("BTC", 3),
            None,
            Some(&t),
            &halted,
            &links(),
        );
        assert_eq!(body["quote"]["empty_reason"], "halted");
        assert_eq!(body["quote"]["bid"], Value::Null);
        assert_eq!(body["last_trade"]["side"], "buy");
        assert_eq!(body["freshness"]["status"], "halted");
        // A halted feed still holding a value keeps it, and its reason describes the value.
        let q = bbo(Some((1.0, 1.0)), Some((2.0, 1.0)));
        let body = quote_body(
            "hyperliquid",
            &inst("BTC", 3),
            Some((&q, 0)),
            None,
            &halted,
            &links(),
        );
        assert_eq!(body["quote"]["empty_reason"], Value::Null);
        assert_eq!(body["quote"]["bid"]["price"], "1.0");
    }

    #[test]
    fn quote_body_venue_time_zero_is_null() {
        let q = bbo(Some((1.0, 1.0)), Some((2.0, 1.0)));
        let mut t = trade(Side::Buy);
        t.source_ts_ns = 0;
        let body = quote_body(
            "hyperliquid",
            &inst("BTC", 3),
            Some((&q, 0)),
            Some(&t),
            &live(),
            &links(),
        );
        assert_eq!(body["quote"]["source_time"], Value::Null);
        assert_eq!(body["last_trade"]["source_time"], Value::Null);
        assert_eq!(
            body["last_trade"]["as_of"],
            "2026-11-18T14:03:06.902114377Z"
        );
    }

    #[test]
    fn trade_with_unknown_side_is_null() {
        let t = trade(Side::Unknown);
        let body = quote_body(
            "hyperliquid",
            &inst("BTC", 3),
            None,
            Some(&t),
            &live(),
            &links(),
        );
        assert_eq!(body["last_trade"]["side"], Value::Null);
        assert_eq!(body["last_trade"]["price"], "97012.0");
    }

    #[test]
    fn no_trade_is_null() {
        let body = quote_body(
            "hyperliquid",
            &inst("BTC", 3),
            None,
            None,
            &live(),
            &links(),
        );
        assert_eq!(body["last_trade"], Value::Null);
    }

    #[test]
    fn asset_class_absent_when_unset() {
        let mut i = inst("BTC", 3);
        i.asset_class = None;
        i.quote_currency = "USDT".into();
        let body = quote_body("hyperliquid", &i, None, None, &live(), &links());
        let instrument = body["instrument"].as_object().unwrap();
        assert!(!instrument.contains_key("asset_class"));
        assert_eq!(instrument["quote_currency"], "USDT");
    }

    #[test]
    fn docs_link_absent_when_unset() {
        let mut l = links();
        l.docs = None;
        let body = quote_body("hyperliquid", &inst("BTC", 3), None, None, &live(), &l);
        assert!(!body["links"].as_object().unwrap().contains_key("docs"));
        let body = markets_body("hyperliquid", &[], None, None, 0, true, &l);
        assert!(!body["links"].as_object().unwrap().contains_key("docs"));
    }

    fn market(symbol: &str, instrument_id: u32, symbol_unique: bool) -> MarketView {
        MarketView {
            instrument: inst(symbol, instrument_id),
            retiring_on: None,
            symbol_unique,
        }
    }

    fn keys(v: &Value, out: &mut Vec<String>) {
        match v {
            Value::Object(m) => {
                for (k, v) in m {
                    out.push(k.clone());
                    keys(v, out);
                }
            }
            Value::Array(a) => a.iter().for_each(|v| keys(v, out)),
            _ => {}
        }
    }

    #[test]
    fn markets_body_has_no_price_or_status() {
        let mut m = market("BTC", 3, true);
        m.retiring_on = Some("2026-12-31".into());
        let body = markets_body("hyperliquid", &[m], None, None, 1, false, &links());
        let mut all = Vec::new();
        keys(&body, &mut all);
        for banned in [
            "price",
            "size",
            "bid",
            "ask",
            "as_of",
            "last_trade",
            "status",
            "freshness",
        ] {
            assert!(!all.iter().any(|k| k == banned), "{banned} in {body}");
        }
        assert_eq!(body["markets"][0]["retiring_on"], "2026-12-31");
        assert_eq!(body["complete"], false);
        assert_eq!(body["total"], 1);
        assert_eq!(body["attribution"]["source"], "DoubleZero");
    }

    #[test]
    fn markets_quote_link_by_symbol_or_id() {
        let page = [market("BTC", 3, true), market("ETH", 4, false)];
        let body = markets_body("hyperliquid", &page, None, None, 2, true, &links());
        assert_eq!(
            body["markets"][0]["links"]["quote"],
            "https://md.example/v1/quote?venue=hyperliquid&symbol=BTC"
        );
        assert_eq!(
            body["markets"][1]["links"]["quote"],
            "https://md.example/v1/quote?venue=hyperliquid&id=1%3Ahl-perps%3A0%3A4"
        );
        assert!(body["markets"][1].get("retiring_on").is_none());
    }

    #[test]
    fn markets_links_page_with_cursors() {
        let (cursor, next) = (id(1, "hl-perps", 0, 3), id(1, "hl-perps", 0, 900));
        let body = markets_body(
            "hyperliquid",
            &[],
            Some(&cursor),
            Some(&next),
            1201,
            true,
            &links(),
        );
        assert_eq!(
            body["links"]["self"],
            "https://md.example/v1/markets?venue=hyperliquid&cursor=1%3Ahl-perps%3A0%3A3"
        );
        assert_eq!(
            body["links"]["next"],
            "https://md.example/v1/markets?venue=hyperliquid&cursor=1%3Ahl-perps%3A0%3A900"
        );
        let last = markets_body("hyperliquid", &[], Some(&next), None, 1201, true, &links());
        assert!(!last["links"].as_object().unwrap().contains_key("next"));
    }

    /// Percent-decodes a query value the way a strict server would (`+` is a literal).
    fn decode(s: &str) -> String {
        let b = s.as_bytes();
        let mut out = Vec::new();
        let mut i = 0;
        while i < b.len() {
            if b[i] == b'%' {
                out.push(u8::from_str_radix(&s[i + 1..i + 3], 16).unwrap());
                i += 3;
            } else {
                out.push(b[i]);
                i += 1;
            }
        }
        String::from_utf8(out).unwrap()
    }

    #[test]
    fn links_are_absolute_and_encoded() {
        let mut l = links();
        l.base = "https://md.example/".into();
        for symbol in ["xyz:TSLA/USD.1", "A&B=C+D E", "é%#?"] {
            let url = l.quote_by_symbol("hyperliquid", symbol);
            let query = url
                .strip_prefix("https://md.example/v1/quote?venue=hyperliquid&symbol=")
                .unwrap_or_else(|| panic!("{url}"));
            assert!(query.bytes().all(
                |b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'.' | b'_' | b'~' | b'%')
            ));
            assert_eq!(decode(query), symbol);
        }
    }

    #[test]
    fn candidates_each_link_their_own_id() {
        let a = inst("BTC", 3);
        let mut b = inst("BTC", 3);
        b.id.category = "xyz-perps".into();
        let problem = ambiguous_instrument("hyperliquid", &[a, b], &links());
        let body: Value = serde_json::from_slice(&problem.to_json("")).unwrap();
        assert_eq!(body["status"], 409);
        assert_eq!(body["code"], "ambiguous-instrument");
        let c = body["candidates"].as_array().unwrap();
        assert_eq!(c.len(), 2);
        assert_eq!(c[0]["instrument"]["id"], "1:hl-perps:0:3");
        assert_eq!(c[1]["instrument"]["id"], "1:xyz-perps:0:3");
        for entry in c {
            let id = entry["instrument"]["id"].as_str().unwrap();
            let url = entry["links"]["quote"].as_str().unwrap();
            let (_, encoded) = url.split_once("&id=").unwrap();
            assert_eq!(decode(encoded), id);
        }
        assert_eq!(body["attribution"]["venue"], "hyperliquid");
        assert_eq!(body.as_object().unwrap().len(), 7);
    }
}
