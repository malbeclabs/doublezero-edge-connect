//! `channels list` / `channels set` support: pure logic over already-fetched JSON, kept separate
//! from the network wiring in `main.rs` so it is unit-testable with fixture bodies alone (no
//! server, no mock listener).
//!
//! Two independent things live here:
//! - A **client-side, best-effort** reading of the `<code>=<id>[,<id>...][;<code>=...]`
//!   channel-filter-spec syntax ([`FilterSpec`]), used only to preview which currently-populated
//!   channels a new channel filter would stop admitting. This is deliberately **not** a validator:
//!   the server (via `ChannelFilter::parse`, the exact function `--channels`/`DZ_CHANNELS` uses at
//!   startup) is the only place a spec is authoritatively checked, and its error is what gets
//!   surfaced to the caller on a `400` — this module never rejects a spec, it only estimates a
//!   preview from it.
//! - [`render_channels_list`], the `--output table` renderer for `channels list`'s merged body
//!   (`{"admin": ..., "status": ...}` — see `endpoint::Endpoint::ChannelsList`'s docs for why the
//!   two are merged into one value before this runs).

use std::collections::{BTreeSet, HashMap};

use serde_json::Value;

use crate::{
    render,
    types::{AdminChannelsResponse, ChannelsBlock},
};

/// Parse `value` as `T`, treating a **missing** key (indexing yields `Value::Null`) as an empty
/// object so `#[serde(default)]` fields take effect, rather than failing to deserialize `Null`
/// against a struct that expects a map. `status_body["channels"]` on a server predating this block
/// is exactly that: plain indexing gives `Null`, not an absent key `render::parse` could special-case,
/// so without this a server one version behind loses the drop preview and, without `--force`,
/// refuses `channels set` outright — the one case `#[serde(default)]` exists to degrade gracefully
/// on. A present-but-empty object is unaffected: it already deserializes via the same defaults.
fn parse_defaulting_null<T: serde::de::DeserializeOwned>(value: &Value) -> Result<T, String> {
    if value.is_null() {
        render::parse(&serde_json::json!({}))
    } else {
        render::parse(value)
    }
}

/// A client-side reading of a channel filter spec: which channel ids each group code would admit
/// under it. A code **absent** from the spec means "admit all" — the same semantic
/// `ChannelFilter::parse` documents, and the reason an unmentioned row is never reported as a drop
/// below.
#[derive(Debug, Clone, Default)]
pub struct FilterSpec {
    clauses: HashMap<String, BTreeSet<u64>>,
}

impl FilterSpec {
    /// Parse `spec` leniently: a clause this can't make sense of (no `=`, a non-numeric id) is
    /// simply skipped rather than reported, because reporting it here would be a second, laxer
    /// validator exactly where the docs on this module say there must not be one. An invalid spec
    /// still reaches the server unchanged and is refused there with the real error.
    pub fn parse(spec: &str) -> FilterSpec {
        let mut clauses: HashMap<String, BTreeSet<u64>> = HashMap::new();
        for clause in spec.split(';') {
            let clause = clause.trim();
            if clause.is_empty() {
                continue;
            }
            let Some((code, ids)) = clause.split_once('=') else {
                continue;
            };
            let code = code.trim().to_string();
            let ids: BTreeSet<u64> = ids
                .split(',')
                .filter_map(|s| s.trim().parse::<u64>().ok())
                .collect();
            clauses.insert(code, ids);
        }
        FilterSpec { clauses }
    }

    /// Would this spec admit `channel` on `code`? A code this spec never mentions admits
    /// everything (matches the server's "an absent code means admit all").
    pub fn admits(&self, code: &str, channel: u64) -> bool {
        match self.clauses.get(code) {
            None => true,
            Some(ids) => ids.contains(&channel),
        }
    }
}

/// One channel a proposed channel filter would stop admitting, carrying enough to both identify it
/// to an operator and show what is at stake (its current product count).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DropRow {
    pub venue: String,
    pub category: String,
    pub code: String,
    pub channel: u32,
    pub name: String,
    pub products: u64,
}

/// Compute which currently-populated, currently-admitted channels `spec` would drop, reading the
/// current state from a `/v1/status` body's `channels` block. Only a channel that (a) is admitted
/// today and (b) holds at least one product is reported — an already-excluded channel has nothing
/// left to lose, and this is a preview of loss, not of the channel filter's full shape.
pub fn compute_drops(status_body: &Value, spec: &str) -> Result<Vec<DropRow>, String> {
    let channels: ChannelsBlock = parse_defaulting_null(&status_body["channels"])?;
    let new_filter = FilterSpec::parse(spec);
    let mut drops = Vec::new();
    for row in &channels.rows {
        for c in &row.channels {
            if c.allowed && c.products > 0 && !new_filter.admits(&row.code, c.channel as u64) {
                drops.push(DropRow {
                    venue: row.venue.clone(),
                    category: row.category.clone(),
                    code: row.code.clone(),
                    channel: c.channel,
                    name: c.display_name(),
                    products: c.products,
                });
            }
        }
    }
    Ok(drops)
}

/// Render the drop preview `channels set` shows before asking for confirmation.
pub fn render_drop_preview(drops: &[DropRow]) -> String {
    if drops.is_empty() {
        return "no currently-admitted channel with products would be dropped by this channel \
            filter."
            .to_string();
    }
    let mut out = "the following channels would be DROPPED — their books, history and catalog \
        entries are removed on the next reconcile tick, and re-adding is a cold start:\n"
        .to_string();
    let headers = ["VENUE", "CATEGORY", "CODE", "CHANNEL", "NAME", "PRODUCTS"];
    let rows: Vec<Vec<String>> = drops
        .iter()
        .map(|d| {
            vec![
                d.venue.clone(),
                d.category.clone(),
                d.code.clone(),
                d.channel.to_string(),
                d.name.clone(),
                d.products.to_string(),
            ]
        })
        .collect();
    out.push_str(&render::table(&headers, &rows));
    out
}

/// `--output table` for `channels list`'s merged body (`{"admin": <GET /admin/channels>, "status":
/// <GET /v1/status>}`). The channel filter summary comes from `admin` (the surface this command is
/// gated on — see `main.rs`'s `--admin-url` handling); the per-channel bound state and product
/// counts come from `status`, which is the only place real receiver liveness is computed
/// (`sinks::admin::get_channels`'s own docs: its `allowed` is the channel filter's opinion, not the
/// running receiver set).
pub fn render_channels_list(body: &Value) -> Result<String, String> {
    let admin: AdminChannelsResponse = render::parse(&body["admin"])?;
    let channels: ChannelsBlock = parse_defaulting_null(&body["status"]["channels"])?;

    let mut out = if admin.summary.is_empty() {
        "channel filter: (unrestricted — every enabled row's channels are admitted)".to_string()
    } else {
        format!("channel filter: {}", admin.summary.join(", "))
    };
    out.push_str("\n\n");
    out.push_str(&render::render_channels_block(&channels));
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    // -----------------------------------------------------------------------------------------
    // FilterSpec
    // -----------------------------------------------------------------------------------------

    #[test]
    fn a_mentioned_code_admits_only_its_listed_ids() {
        let f = FilterSpec::parse("lashay-4=10,11");
        assert!(f.admits("lashay-4", 10));
        assert!(f.admits("lashay-4", 11));
        assert!(!f.admits("lashay-4", 12));
    }

    /// The semantic this whole preview leans on: a code the spec never names admits everything,
    /// exactly like `ChannelFilter::parse`'s own documented behaviour for an unmentioned row.
    #[test]
    fn an_unmentioned_code_admits_everything() {
        let f = FilterSpec::parse("lashay-4=10,11");
        assert!(f.admits("lashay-2", 999));
    }

    #[test]
    fn multiple_clauses_are_each_scoped_to_their_own_code() {
        let f = FilterSpec::parse("lashay-4=10,11;lashay-2=5");
        assert!(f.admits("lashay-4", 10));
        assert!(!f.admits("lashay-4", 5));
        assert!(f.admits("lashay-2", 5));
        assert!(!f.admits("lashay-2", 10));
    }

    // -----------------------------------------------------------------------------------------
    // compute_drops
    // -----------------------------------------------------------------------------------------

    fn status_fixture() -> Value {
        serde_json::json!({
            "channels": {
                "rows": [{
                    "venue": "KALSHI",
                    "category": "sports",
                    "code": "lashay-4",
                    "excluded": 29,
                    "channels": [
                        {"channel": 10, "allowed": true, "bound": true, "products": 412},
                        {"channel": 11, "allowed": true, "bound": true, "products": 287},
                        {"channel": 12, "allowed": false, "bound": false, "products": 0}
                    ]
                }],
                "excluded_by_filter": 29
            }
        })
    }

    /// The core case: narrowing to `10` alone must report channel 11 (currently admitted, holding
    /// products) as a drop, and must not report channel 10 itself (still admitted) or channel 12
    /// (already excluded, holds nothing — there is nothing left to lose there).
    #[test]
    fn a_channel_losing_admission_with_products_is_reported_as_a_drop() {
        let drops = compute_drops(&status_fixture(), "lashay-4=10").unwrap();
        assert_eq!(drops.len(), 1, "{drops:?}");
        assert_eq!(drops[0].channel, 11);
        assert_eq!(drops[0].products, 287);
        assert_eq!(drops[0].code, "lashay-4");
    }

    /// A row whose code the new spec never mentions is untouched (admit-all), so nothing under it
    /// is ever reported as a drop, however wide its current occupancy.
    #[test]
    fn a_row_whose_code_is_absent_from_the_new_spec_drops_nothing() {
        let drops = compute_drops(&status_fixture(), "lashay-2=1").unwrap();
        assert!(drops.is_empty(), "{drops:?}");
    }

    /// An empty spec clears every restriction — admits everything — so nothing already admitted
    /// can be a drop.
    #[test]
    fn an_empty_spec_drops_nothing() {
        let drops = compute_drops(&status_fixture(), "").unwrap();
        assert!(drops.is_empty(), "{drops:?}");
    }

    /// The regression this pins: a `/v1/status` body with the `channels` key **absent** (a server
    /// predating the block), not present-and-empty. Plain indexing yields `Value::Null`, and
    /// `serde_json::from_value::<ChannelsBlock>(Null)` errors rather than defaulting, so without
    /// `parse_defaulting_null` this returns `Err` and the drop preview is lost.
    #[test]
    fn a_status_body_with_no_channels_key_defaults_to_no_drops() {
        let status_body = serde_json::json!({});
        let drops = compute_drops(&status_body, "lashay-4=10").unwrap();
        assert!(drops.is_empty(), "{drops:?}");
    }

    // -----------------------------------------------------------------------------------------
    // render_drop_preview
    // -----------------------------------------------------------------------------------------

    #[test]
    fn an_empty_drop_list_reads_as_no_channel_would_be_dropped() {
        let out = render_drop_preview(&[]);
        assert!(out.contains("no currently-admitted channel"), "{out}");
    }

    #[test]
    fn a_nonempty_drop_list_names_the_channel_and_its_product_count() {
        let drops = vec![DropRow {
            venue: "KALSHI".to_string(),
            category: "sports".to_string(),
            code: "lashay-4".to_string(),
            channel: 11,
            name: "11".to_string(),
            products: 287,
        }];
        let out = render_drop_preview(&drops);
        assert!(out.contains("DROPPED"), "{out}");
        assert!(out.contains("lashay-4"), "{out}");
        assert!(out.contains("287"), "{out}");
    }

    // -----------------------------------------------------------------------------------------
    // render_channels_list
    // -----------------------------------------------------------------------------------------

    #[test]
    fn channels_list_shows_the_filter_summary_and_the_channel_table() {
        let body = serde_json::json!({
            "admin": {"summary": ["lashay-4=2 of 31"]},
            "status": status_fixture(),
        });
        let out = render_channels_list(&body).unwrap();
        assert!(out.contains("channel filter: lashay-4=2 of 31"), "{out}");
        assert!(out.contains("lashay-4"), "{out}");
        assert!(out.contains("412"), "{out}");
    }

    #[test]
    fn channels_list_reports_an_unrestricted_filter_plainly() {
        let body = serde_json::json!({
            "admin": {"summary": []},
            "status": status_fixture(),
        });
        let out = render_channels_list(&body).unwrap();
        assert!(out.contains("unrestricted"), "{out}");
    }

    /// The `--output table` regression this pins: a `status` body with no `channels` key at all (a
    /// server predating the block). Without defaulting-on-null this is an `Err` and `channels list`
    /// hard-refuses against exactly the server skew `#[serde(default)]` exists to tolerate.
    #[test]
    fn channels_list_tolerates_a_status_body_with_no_channels_key() {
        let body = serde_json::json!({
            "admin": {"summary": []},
            "status": {},
        });
        let out = render_channels_list(&body).unwrap();
        assert!(out.contains("unrestricted"), "{out}");
    }
}
