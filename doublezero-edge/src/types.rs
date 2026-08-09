//! Response shapes for the edge-connect `/v1` API, duplicated here as small plain `serde` types.
//!
//! **Rule: no type here is shared with the bridge crate.** This CLI has no path dependency on
//! `doublezero-edge-connect` and never will — it must keep working against a container built from
//! a different revision of that crate than the one this binary shipped with. Concretely that means:
//!
//! - No `#[serde(deny_unknown_fields)]` anywhere in this module. A field a newer container adds
//!   must be silently ignored by an older CLI, not rejected.
//! - Every numeric wire value that the API renders as a string (all price/size/time fields — see
//!   `sinks/api.rs`'s `decimal_string`) stays a `String` here too. Parsing it into a float would
//!   both lose precision the server deliberately preserved and crash on a value this CLI doesn't
//!   recognise the shape of.
//!
//! These types are used **only** for the `--output table` rendering path. `--output json` (the
//! default) and `--jq` both work directly off the raw [`serde_json::Value`] the server returned, so
//! a field this module doesn't know about is never dropped from JSON output — only from the table
//! view, which is a lossy human convenience by design.

use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct ProductsListResponse {
    #[serde(default)]
    pub products: Vec<Product>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ProductResponse {
    pub product: Product,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Product {
    pub product_id: String,
    pub source_id: u32,
    pub source: String,
    pub symbol: String,
    pub channel: u32,
    pub instrument_id: u32,
    pub price_increment: String,
    pub base_increment: String,
    pub status: String,
    pub feed_kind: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TickerResponse {
    #[serde(default)]
    pub trades: Vec<Trade>,
    #[serde(default)]
    pub best_bid: Option<String>,
    #[serde(default)]
    pub best_ask: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Trade {
    pub time_ns: String,
    pub price: String,
    pub size: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct CandlesResponse {
    #[serde(default)]
    pub candles: Vec<Candle>,
    pub retention: Retention,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Candle {
    pub start: String,
    pub low: String,
    pub high: String,
    pub open: String,
    pub close: String,
    pub volume: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Retention {
    pub window_seconds: u64,
    pub oldest: String,
    pub newest: String,
    pub truncated: bool,
}

#[derive(Debug, Clone, Deserialize)]
pub struct BookResponse {
    pub pricebook: Pricebook,
    pub coverage: Coverage,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Pricebook {
    pub product_id: String,
    #[serde(default)]
    pub bids: Vec<[String; 2]>,
    #[serde(default)]
    pub asks: Vec<[String; 2]>,
    pub time: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Coverage {
    pub levels_returned: usize,
    pub levels_capped_at: usize,
    pub complete: bool,
}

#[derive(Debug, Clone, Deserialize)]
pub struct BestBidAskResponse {
    #[serde(default)]
    pub pricebooks: Vec<BestBidAskEntry>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct BestBidAskEntry {
    pub product_id: String,
    #[serde(default)]
    pub bids: Vec<[String; 2]>,
    #[serde(default)]
    pub asks: Vec<[String; 2]>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct StatusResponse {
    #[serde(default)]
    pub venues: Vec<VenueStatus>,
    pub history: HistoryStatus,
}

#[derive(Debug, Clone, Deserialize)]
pub struct VenueStatus {
    pub venue: String,
    pub status: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct HistoryStatus {
    pub products_tracked: usize,
    pub late_drops: u64,
    pub evicted: u64,
    pub window_seconds: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Rule 1's guarantee, pinned directly: a response body carrying a field this CLI has never
    /// heard of must still parse, with the known fields intact. This is the one test in the suite
    /// most likely to rot silently, because there is no compile-time signal when it stops meaning
    /// anything — see the revert note in the task report.
    #[test]
    fn a_product_with_unknown_extra_fields_still_parses() {
        let body = r#"{
            "product_id": "HYPERLIQUID:BTC",
            "source_id": 1,
            "source": "Hyperliquid",
            "symbol": "BTC",
            "channel": 0,
            "instrument_id": 41,
            "price_increment": "0.01",
            "base_increment": "0.00001",
            "status": "online",
            "feed_kind": "top_of_book",
            "volume_24h": "1234.5",
            "a_field_from_the_future": {"nested": [1, 2, 3]}
        }"#;
        let p: Product =
            serde_json::from_str(body).expect("unknown fields must not reject parsing");
        assert_eq!(p.product_id, "HYPERLIQUID:BTC");
        assert_eq!(p.price_increment, "0.01");
        assert_eq!(p.feed_kind, "top_of_book");
    }

    #[test]
    fn an_unknown_top_level_response_field_still_parses() {
        let body = r#"{
            "products": [],
            "server_build_id": "2026-08-09-abcdef"
        }"#;
        let r: ProductsListResponse =
            serde_json::from_str(body).expect("unknown top-level fields must not reject parsing");
        assert!(r.products.is_empty());
    }
}
