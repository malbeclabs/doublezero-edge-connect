//! `channels list` / `channels set` support: pure logic over already-fetched JSON, kept separate
//! from the network wiring in `main.rs` so it is unit-testable with fixture bodies alone (no
//! server, no mock listener).
//!
//! Two independent things live here:
//! - A **client-side, best-effort** reading of the `<code>=<id>[,<id>...][;<code>=...]` floor-spec
//!   syntax ([`FloorSpec`]), used only to preview which currently-populated channels a new floor
//!   would stop admitting. This is deliberately **not** a validator: the server (via
//!   `ChannelFloor::parse`, the exact function `--channels`/`DZ_CHANNELS` uses at startup) is the
//!   only place a spec is authoritatively checked, and its error is what gets surfaced to the
//!   caller on a `400` — this module never rejects a spec, it only estimates a preview from it.
//! - [`render_channels_list`], the `--output table` renderer for `channels list`'s merged body
//!   (`{"admin": ..., "status": ...}` — see `endpoint::Endpoint::ChannelsList`'s docs for why the
//!   two are merged into one value before this runs).

use std::collections::{BTreeSet, HashMap};

use serde_json::Value;

use crate::render;
use crate::types::{AdminChannelsResponse, ChannelsBlock};

/// A client-side reading of a floor spec: which channel ids each group code would admit under it.
/// A code **absent** from the spec means "admit all" — the same semantic `ChannelFloor::parse`
/// documents, and the reason an unmentioned row is never reported as a drop below.
#[derive(Debug, Clone, Default)]
pub struct FloorSpec {
    clauses: HashMap<String, BTreeSet<u64>>,
}

impl FloorSpec {
    /// Parse `spec` leniently: a clause this can't make sense of (no `=`, a non-numeric id) is
    /// simply skipped rather than reported, because reporting it here would be a second, laxer
    /// validator exactly where the docs on this module say there must not be one. An invalid spec
    /// still reaches the server unchanged and is refused there with the real error.
    pub fn parse(spec: &str) -> FloorSpec {
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
        FloorSpec { clauses }
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

/// One channel a proposed floor would stop admitting, carrying enough to both identify it to an
/// operator and show what is at stake (its current product count).
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
/// left to lose, and this is a preview of loss, not of the floor's full shape.
pub fn compute_drops(status_body: &Value, spec: &str) -> Result<Vec<DropRow>, String> {
    let channels: ChannelsBlock = render::parse(&status_body["channels"])?;
    let new_floor = FloorSpec::parse(spec);
    let mut drops = Vec::new();
    for row in &channels.rows {
        for c in &row.channels {
            if c.floor_admits && c.products > 0 && !new_floor.admits(&row.code, c.channel as u64) {
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
        return "no currently-admitted channel with products would be dropped by this floor."
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
/// <GET /v1/status>}`). The floor summary comes from `admin` (the surface this command is gated
/// on — see `main.rs`'s `--admin-url` handling); the per-channel bound state and product counts
/// come from `status`, which is the only place real receiver liveness is computed
/// (`sinks::admin::get_channels`'s own docs: its `floor_admits` is the floor's opinion, not the
/// running receiver set).
pub fn render_channels_list(body: &Value) -> Result<String, String> {
    let admin: AdminChannelsResponse = render::parse(&body["admin"])?;
    let channels: ChannelsBlock = render::parse(&body["status"]["channels"])?;

    let mut out = if admin.summary.is_empty() {
        "floor: (unrestricted — every enabled row's channels are admitted)".to_string()
    } else {
        format!("floor: {}", admin.summary.join(", "))
    };
    out.push_str("\n\n");
    out.push_str(&render::render_channels_block(&channels));
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    // -----------------------------------------------------------------------------------------
    // FloorSpec
    // -----------------------------------------------------------------------------------------

    #[test]
    fn a_mentioned_code_admits_only_its_listed_ids() {
        let f = FloorSpec::parse("lashay-4=10,11");
        assert!(f.admits("lashay-4", 10));
        assert!(f.admits("lashay-4", 11));
        assert!(!f.admits("lashay-4", 12));
    }

    /// The semantic this whole preview leans on: a code the spec never names admits everything,
    /// exactly like `ChannelFloor::parse`'s own documented behaviour for an unmentioned row.
    #[test]
    fn an_unmentioned_code_admits_everything() {
        let f = FloorSpec::parse("lashay-4=10,11");
        assert!(f.admits("lashay-2", 999));
    }

    #[test]
    fn multiple_clauses_are_each_scoped_to_their_own_code() {
        let f = FloorSpec::parse("lashay-4=10,11;lashay-2=5");
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
                        {"channel": 10, "floor_admits": true, "bound": true, "products": 412},
                        {"channel": 11, "floor_admits": true, "bound": true, "products": 287},
                        {"channel": 12, "floor_admits": false, "bound": false, "products": 0}
                    ]
                }],
                "excluded_by_floor": 29
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
    fn channels_list_shows_the_floor_summary_and_the_channel_table() {
        let body = serde_json::json!({
            "admin": {"summary": ["lashay-4=2 of 31"]},
            "status": status_fixture(),
        });
        let out = render_channels_list(&body).unwrap();
        assert!(out.contains("floor: lashay-4=2 of 31"), "{out}");
        assert!(out.contains("lashay-4"), "{out}");
        assert!(out.contains("412"), "{out}");
    }

    #[test]
    fn channels_list_reports_an_unrestricted_floor_plainly() {
        let body = serde_json::json!({
            "admin": {"summary": []},
            "status": status_fixture(),
        });
        let out = render_channels_list(&body).unwrap();
        assert!(out.contains("unrestricted"), "{out}");
    }
}
