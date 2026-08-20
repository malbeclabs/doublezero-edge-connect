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
    pub channel: u8,
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
    /// Whether the server's history store still tracks this product at all — distinct from an
    /// empty `candles` array, which a genuinely quiet (but still tracked) market and an evicted one
    /// both produce. Defaults to `true` (the old, non-distinguishing behavior) against a server
    /// predating this field, rather than `bool::default()`'s `false`, which would misreport a
    /// tracked market as evicted purely because an old server never sent the field at all.
    #[serde(default = "held_default")]
    pub held: bool,
}

fn held_default() -> bool {
    true
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
    /// Absent on an older server that predates the channel filter — default to empty rather than
    /// fail the whole response.
    #[serde(default)]
    pub channels: ChannelsBlock,
    /// Absent on an older server, or when the process collector isn't registered for this
    /// build/platform (Linux-only) — default rather than fail the whole response.
    #[serde(default)]
    pub process: ProcessBlock,
    /// Absent on an older server that predates the feed-registry resolution report.
    #[serde(default)]
    pub registry: RegistryBlock,
}

#[derive(Debug, Clone, Deserialize)]
pub struct VenueStatus {
    pub venue: String,
    pub status: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct HistoryStatus {
    /// Superseded by `products` (kept for a server old enough to only send this name).
    #[serde(default)]
    pub products_tracked: usize,
    #[serde(default)]
    pub products: usize,
    /// The one figure a raw memory number can't tell you: the bucket budget holds RSS flat while
    /// silently evicting products, so a healthy-looking RSS is exactly what an over-wide channel
    /// filter produces. This flag (not an inferred "products == some hardcoded cap") is the honest
    /// signal, and the table renderer must show it as its own marker rather than leave a caller to
    /// notice two numbers are equal.
    #[serde(default)]
    pub products_at_cap: bool,
    #[serde(default)]
    pub buckets: u64,
    #[serde(default)]
    pub bucket_budget: u64,
    #[serde(default)]
    pub est_bytes: u64,
    pub window_seconds: u64,
    pub evicted: u64,
    pub late_drops: u64,
}

/// The `channels` block of `/v1/status`: per enabled row, its channels' admission by the channel
/// filter, real receiver liveness and current product count.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct ChannelsBlock {
    #[serde(default)]
    pub rows: Vec<ChannelRow>,
    #[serde(default)]
    pub excluded_by_filter: usize,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ChannelRow {
    pub venue: String,
    pub category: String,
    pub code: String,
    #[serde(default)]
    pub channels: Vec<ChannelEntry>,
    #[serde(default)]
    pub excluded: usize,
}

/// One channel of one row. `label`/`symbol_prefixes` are **display-only** derived names — see
/// [`ChannelEntry::display_name`]. Neither is a wire identity: the channel **id** is the only
/// contract this crate resolves anything by (see `ingest/channel_filter.rs`'s docs for why a name here
/// would drift). Both are optional: today's server sends neither, a channel with no reference
/// data yet (a normal startup state, not an error) sends neither either, and a future server may
/// send one or both — this type must keep parsing regardless.
#[derive(Debug, Clone, Deserialize)]
pub struct ChannelEntry {
    pub channel: u8,
    pub allowed: bool,
    pub bound: bool,
    pub products: u64,
    /// A short human label for the channel (e.g. `sports.nfl`), owned by the upstream deployment
    /// inventory and passed through verbatim when the server has one. Never used for lookup,
    /// matching or filtering — display only.
    #[serde(default)]
    pub label: Option<String>,
    /// The most common symbol prefixes (the portion of each instrument symbol before its first
    /// `-`) seen on this channel, ranked by how many instruments carry each one and capped by the
    /// server — small and stable, unlike full symbols, which are per-event and churn as contracts
    /// expire. Never used for lookup, matching or filtering — display only.
    #[serde(default)]
    pub symbol_prefixes: Vec<String>,
    /// The true number of **distinct** prefixes on this channel — sent whenever `symbol_prefixes`
    /// is, regardless of whether the list was actually capped. `None` on an older server that
    /// predates this field, or when `symbol_prefixes` itself is absent; the renderer then falls
    /// back to the local list's own length (see [`render_symbol_prefixes`]), which cannot express
    /// a remainder past what was sent.
    #[serde(default)]
    pub symbol_prefixes_total: Option<usize>,
}

/// How many symbol prefixes [`ChannelEntry::display_name`] shows before eliding the rest with a
/// `+N more` marker — never silently dropping entries with no indication there were more.
const MAX_PREFIXES_SHOWN: usize = 3;

impl ChannelEntry {
    /// The name to show a human for this channel. **Display only** — never resolve, filter or
    /// match on this. Precedence: `label` (an upstream human name, when the server supplies one)
    /// wins; else the channel's `symbol_prefixes` (truncated with a `+N more` marker if there are
    /// more than a few); else the bare channel id, which is the only real contract. All three
    /// shapes must render cleanly, since which the server sends varies by deployment, by how new
    /// the server is, and by whether this channel has reference data yet.
    pub fn display_name(&self) -> String {
        if let Some(label) = self.label.as_deref() {
            if !label.trim().is_empty() {
                return label.to_string();
            }
        }
        if !self.symbol_prefixes.is_empty() {
            return render_symbol_prefixes(&self.symbol_prefixes, self.symbol_prefixes_total);
        }
        self.channel.to_string()
    }
}

/// Render the prefix list for a human, eliding the tail with an exact remainder.
///
/// `total` is the server's true distinct-prefix count. When present, the remainder is `total -
/// shown.len()` — an exact count, not a lower bound, so `KXNFLGAME, KXNFLSPREAD, KXNFLPLAYOFF +19
/// more` states a fact rather than hedging with a "the list was capped" marker. When absent (an
/// older server that predates the field), this falls back to the local list's own length — the
/// best this client can do without a real count, and identical to this function's pre-total
/// behaviour.
fn render_symbol_prefixes(prefixes: &[String], total: Option<usize>) -> String {
    let shown: Vec<&str> = prefixes
        .iter()
        .take(MAX_PREFIXES_SHOWN)
        .map(String::as_str)
        .collect();
    let mut out = shown.join(", ");
    let total = total.unwrap_or(prefixes.len());
    let remaining = total.saturating_sub(shown.len());
    if remaining > 0 {
        out.push_str(&format!(" +{remaining} more"));
    }
    out
}

/// The `process` block of `/v1/status`: resident memory and cumulative CPU seconds, read off the
/// process collector. Both fields are `None` (rather than a fabricated `0`) when the collector
/// isn't registered for this build/platform.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct ProcessBlock {
    #[serde(default)]
    pub resident_memory_bytes: Option<f64>,
    #[serde(default)]
    pub cpu_seconds_total: Option<f64>,
}

/// The `registry` block of `/v1/status`: which feed-registry document this process resolved (a
/// URL, a bind-mounted file path, or `"built-in"`), its `version`, and how many rows/receivers it
/// carries — the same figures the bridge logs once at startup as "feed registry resolved," so a
/// fleet-wide check doesn't need log access. Empty `source` (the `Default`) on an older server that
/// predates this field, rendered as a blank line rather than a guess — see [`crate::render`].
#[derive(Debug, Clone, Default, Deserialize)]
pub struct RegistryBlock {
    #[serde(default)]
    pub source: String,
    #[serde(default)]
    pub version: u32,
    #[serde(default)]
    pub rows: usize,
    #[serde(default)]
    pub receivers: usize,
}

/// `GET /admin/channels`'s response shape — only the field `channels list` needs (the channel
/// filter spec currently in force, as `ChannelFilter::summary` renders it). Other fields on that response
/// (`rows`, `note`) are read straight off the raw JSON where needed rather than typed here, since
/// this crate's only use for them is display.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct AdminChannelsResponse {
    #[serde(default)]
    pub summary: Vec<String>,
}

/// `POST /admin/channels`'s success response shape: the channel filter spec that is now in force.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct AdminApplyResponse {
    #[serde(default)]
    pub applied: Vec<String>,
}

/// `GET /admin/diagnostics`'s body — why this process is (or is not) serving data. **Every** field
/// below defaults, right down to the verdict: this is the one command an operator runs when
/// everything else has failed, so a block a newer bridge added (or an older one never sent) must
/// cost them a line of the table, never the whole report.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct DiagnosticsResponse {
    #[serde(default)]
    pub diagnosis: Diagnosis,
    #[serde(default)]
    pub tunnel: TunnelBlock,
    #[serde(default)]
    pub subscriptions: SubscriptionsBlock,
    #[serde(default)]
    pub activation: ActivationBlock,
    /// `doublezero latency`, probed by the container on its own coarse cadence. Absent (rather
    /// than empty) means it has never probed; empty means it probed and nothing answered.
    #[serde(default)]
    pub latency: Option<Vec<DeviceLatency>>,
    /// When that probe ran. Routinely older than `tunnel.checked_at_unix`, since the probe is
    /// paced far coarser than the subscription poll.
    #[serde(default)]
    pub latency_at_unix: Option<u64>,
    #[serde(default)]
    pub registry: RegistryBlock,
    #[serde(default)]
    pub binds: BindsBlock,
}

/// The verdict. `code` is the stable machine-readable one an agent reads via `--jq`; the other two
/// are prose for a human.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct Diagnosis {
    #[serde(default)]
    pub code: String,
    #[serde(default)]
    pub summary: String,
    #[serde(default)]
    pub remediation: String,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct TunnelBlock {
    #[serde(default)]
    pub detection: String,
    #[serde(default)]
    pub detail: Option<String>,
    #[serde(default)]
    pub checked_at_unix: Option<u64>,
    /// When the last *successful* poll landed. Diverges from `checked_at_unix` exactly when a tick
    /// failed and the server kept the previous poll's data, which is the only thing that makes the
    /// sessions below readable as stale rather than current.
    #[serde(default)]
    pub last_ok_at_unix: Option<u64>,
    #[serde(default)]
    pub poll_seconds: u64,
    #[serde(default)]
    pub sessions: Vec<TunnelSession>,
}

/// One `doublezero status` session, passed through as the client reported it. Every field is
/// optional because the client itself omits or stubs them (`"N/A"`) depending on session state.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct TunnelSession {
    #[serde(default)]
    pub session_status: Option<String>,
    #[serde(default)]
    pub tunnel_name: Option<String>,
    #[serde(default)]
    pub user_type: Option<String>,
    #[serde(default)]
    pub current_device: Option<String>,
    #[serde(default)]
    pub lowest_latency_device: Option<String>,
    #[serde(default)]
    pub metro: Option<String>,
    #[serde(default)]
    pub network: Option<String>,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct SubscriptionsBlock {
    #[serde(default)]
    pub market_data_codes: Vec<String>,
    #[serde(default)]
    pub shred_codes: Vec<String>,
    /// Subscribed groups this build has no feed row for — the ones a `no_market_data_subscriptions`
    /// verdict is asking the operator to look at.
    #[serde(default)]
    pub other_codes: Vec<String>,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct ActivationBlock {
    #[serde(default)]
    pub receivers: Vec<ReceiverEntry>,
    #[serde(default)]
    pub ws_on: bool,
    #[serde(default)]
    pub api_on: bool,
    #[serde(default)]
    pub shred_sources: Vec<String>,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct ReceiverEntry {
    #[serde(default)]
    pub venue: String,
    #[serde(default)]
    pub category: String,
    #[serde(default)]
    pub kind: String,
    /// The publisher's base port, which is a publisher's whole identity in the registry.
    #[serde(default)]
    pub publisher: u32,
    #[serde(default)]
    pub liveness: String,
}

/// One device's probe result from the container's `doublezero latency` read.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct DeviceLatency {
    #[serde(default)]
    pub device_code: Option<String>,
    #[serde(default)]
    pub device_ip: Option<String>,
    #[serde(default)]
    pub min_latency_ns: Option<u64>,
    #[serde(default)]
    pub avg_latency_ns: Option<u64>,
    #[serde(default)]
    pub max_latency_ns: Option<u64>,
    #[serde(default)]
    pub reachable: bool,
}

/// Which surfaces the container was configured with. Empty means "not configured", which for `api`
/// is a genuinely different answer from "configured but not activated" (`ActivationBlock::api_on`).
#[derive(Debug, Clone, Default, Deserialize)]
pub struct BindsBlock {
    #[serde(default)]
    pub ws: String,
    #[serde(default)]
    pub api: String,
    #[serde(default)]
    pub admin: String,
    #[serde(default)]
    pub metrics: String,
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

    /// The diagnostics body is the report an operator falls back to when nothing else works, so
    /// version skew in either direction must cost at most a block: a body carrying only a verdict
    /// (an older bridge) and one carrying a block this build has never heard of both parse.
    #[test]
    fn a_diagnostics_body_parses_with_blocks_missing_and_with_blocks_unknown() {
        let sparse: DiagnosticsResponse =
            serde_json::from_str(r#"{"diagnosis":{"code":"tunnel_down"}}"#).unwrap();
        assert_eq!(sparse.diagnosis.code, "tunnel_down");
        assert!(sparse.activation.receivers.is_empty());

        let future: DiagnosticsResponse = serde_json::from_str(
            r#"{"diagnosis":{"code":"ok","summary":"s","remediation":"r"},
                "activation":{"receivers":[{"venue":"LASHAY","kind":"market_by_price",
                                            "publisher":32000,"liveness":"up","unheard_of":"a"}],
                              "ws_on":true,"api_on":true},
                "peering":{"nothing":"this build knows"}}"#,
        )
        .unwrap();
        assert_eq!(future.activation.receivers[0].publisher, 32000);
        assert!(future.activation.api_on);
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

    // -----------------------------------------------------------------------------------------
    // ChannelEntry::display_name — label / symbol_prefixes precedence. Four genuinely distinct
    // documents (never one parameterised fixture reused four ways — that exact failure mode has
    // recurred on this plan): a channel with only `label`, one with only `symbol_prefixes`, one
    // with both (label must win), and one with neither (must fall back to the bare id).
    // -----------------------------------------------------------------------------------------

    fn channel_entry(json: &str) -> ChannelEntry {
        serde_json::from_str(json).expect("fixture must parse as a ChannelEntry")
    }

    #[test]
    fn a_channel_with_only_a_label_shows_the_label() {
        let c = channel_entry(
            r#"{"channel": 10, "allowed": true, "bound": true, "products": 412, "label": "sports.nfl"}"#,
        );
        assert_eq!(c.display_name(), "sports.nfl");
    }

    #[test]
    fn a_channel_with_only_symbol_prefixes_shows_them() {
        let c = channel_entry(
            r#"{"channel": 11, "allowed": true, "bound": true, "products": 287, "symbol_prefixes": ["KXNFLGAME", "KXNFLSPREAD"]}"#,
        );
        assert_eq!(c.display_name(), "KXNFLGAME, KXNFLSPREAD");
    }

    /// Both present: `label` wins outright, `symbol_prefixes` must not leak into the output.
    #[test]
    fn a_label_wins_over_symbol_prefixes_when_both_are_present() {
        let c = channel_entry(
            r#"{"channel": 43, "allowed": true, "bound": true, "products": 9, "label": "sports.combat", "symbol_prefixes": ["KXUFC"]}"#,
        );
        let name = c.display_name();
        assert_eq!(name, "sports.combat");
        assert!(!name.contains("KXUFC"), "label must win outright: {name}");
    }

    /// Neither present — a channel bound but with no reference data yet is a normal startup
    /// state, not an error, and must still render using the one real contract: the id.
    #[test]
    fn a_channel_with_neither_field_falls_back_to_the_bare_id() {
        let c = channel_entry(r#"{"channel": 49, "allowed": true, "bound": false, "products": 0}"#);
        assert_eq!(c.display_name(), "49");
    }

    /// No `symbol_prefixes_total` at all (an older server that predates the field): the renderer
    /// falls back to the local list's own length, which is the pre-total behaviour and the best
    /// this client can do without a real count.
    #[test]
    fn a_long_symbol_prefix_list_falls_back_to_the_local_length_without_a_total() {
        let c = channel_entry(
            r#"{"channel": 20, "allowed": true, "bound": true, "products": 5,
                "symbol_prefixes": ["A", "B", "C", "D", "E"]}"#,
        );
        assert_eq!(c.display_name(), "A, B, C +2 more");
    }

    /// The exact remainder, computed from the server's real total rather than the sent list's
    /// length — mirrors the task's own example: three shown, nineteen more, because the server
    /// held twenty-two distinct prefixes and sent only the top three.
    #[test]
    fn the_remainder_is_computed_exactly_from_the_servers_total() {
        let c = channel_entry(
            r#"{"channel": 21, "allowed": true, "bound": true, "products": 5,
                "symbol_prefixes": ["A", "B", "C"], "symbol_prefixes_total": 22}"#,
        );
        assert_eq!(c.display_name(), "A, B, C +19 more");
    }

    /// A list short enough to show whole can still have a real remainder the server counted but
    /// did not send — this is the case a `remaining > 0` guard keyed on the *sent* length alone
    /// would lose, since nothing was elided locally.
    #[test]
    fn a_short_list_with_a_total_past_it_still_shows_the_remainder() {
        let c = channel_entry(
            r#"{"channel": 22, "allowed": true, "bound": true, "products": 5,
                "symbol_prefixes": ["A", "B"], "symbol_prefixes_total": 5}"#,
        );
        assert_eq!(c.display_name(), "A, B +3 more");
    }

    /// A total equal to the sent length means nothing was capped — no `+N more` marker at all,
    /// which falls out of the arithmetic rather than needing a separate "was this capped" flag.
    #[test]
    fn a_total_matching_the_sent_length_shows_no_remainder() {
        let c = channel_entry(
            r#"{"channel": 23, "allowed": true, "bound": true, "products": 1,
                "symbol_prefixes": ["A", "B"], "symbol_prefixes_total": 2}"#,
        );
        assert_eq!(c.display_name(), "A, B");
    }
}
