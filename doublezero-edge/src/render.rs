//! `--output table` rendering: a lossy, human-readable view built off the typed response structs
//! in `types.rs`. `--output json` (the default) never goes through this module — it prints the raw
//! [`serde_json::Value`] the server returned, untouched, so a field this module doesn't render is
//! still visible to a caller reading JSON (see `types.rs`'s module docs).
//!
//! Rule 2 (surface the coverage blocks): [`render_candles`] and [`render_book`] both print their
//! `retention`/`coverage` block below the data table — a caller must never be left staring at 60
//! candles with no way to tell the window only holds an hour.

use serde_json::Value;

use crate::{endpoint::Endpoint, types::*};

/// Render `body` (a parsed `2xx` response) as a table for `endpoint`. Returns `Err` if `body`
/// doesn't have the shape this endpoint promises — a version-skew problem worth reporting plainly
/// rather than panicking on, though in practice a real container's response always matches.
pub fn render_table(endpoint: Endpoint, body: &Value) -> Result<String, String> {
    match endpoint {
        Endpoint::ProductsList => render_products_list(body),
        Endpoint::ProductGet => render_product_get(body),
        Endpoint::Ticker => render_ticker(body),
        Endpoint::Candles => render_candles(body),
        Endpoint::Book => render_book(body),
        Endpoint::BestBidAsk => render_best_bid_ask(body),
        Endpoint::Status => render_status(body),
        Endpoint::ChannelsList => crate::channels::render_channels_list(body),
        Endpoint::ChannelsSet => render_channels_set(body),
        Endpoint::Diagnose => crate::diagnose::render_diagnose(body),
    }
}

fn render_channels_set(body: &Value) -> Result<String, String> {
    let parsed: AdminApplyResponse = parse(body)?;
    if parsed.applied.is_empty() {
        Ok("applied: (unrestricted — every enabled row's channels are admitted)".to_string())
    } else {
        Ok(format!("applied: {}", parsed.applied.join(", ")))
    }
}

/// Deserialize `body` into `T`, tolerantly (no type in `types.rs` uses
/// `#[serde(deny_unknown_fields)]`). `pub(crate)` so `channels.rs` — which renders the same
/// `/v1/status` and `/admin/channels` shapes for `doublezero-edge channels list` — can reuse it
/// rather than hand-rolling a second JSON-to-struct step.
pub(crate) fn parse<T: serde::de::DeserializeOwned>(body: &Value) -> Result<T, String> {
    serde_json::from_value(body.clone())
        .map_err(|e| format!("response did not match the expected shape: {e}"))
}

/// Build an aligned text table. Every column but the last is padded to its widest cell; the last
/// column is left unpadded so no row ends in trailing whitespace. `pub(crate)` so `channels.rs`
/// renders its own tables (the drop preview, the merged `channels list` view) in the same visual
/// style as every other command here rather than duplicating a second table builder.
pub(crate) fn table(headers: &[&str], rows: &[Vec<String>]) -> String {
    let mut widths: Vec<usize> = headers.iter().map(|h| h.len()).collect();
    for row in rows {
        for (i, cell) in row.iter().enumerate() {
            if i < widths.len() {
                widths[i] = widths[i].max(cell.len());
            }
        }
    }
    let header_cells: Vec<String> = headers.iter().map(|h| h.to_string()).collect();
    let mut lines = vec![pad_row(&header_cells, &widths), rule(&widths)];
    for row in rows {
        lines.push(pad_row(row, &widths));
    }
    lines.join("\n")
}

fn rule(widths: &[usize]) -> String {
    widths
        .iter()
        .map(|w| "-".repeat(*w))
        .collect::<Vec<_>>()
        .join("  ")
}

fn pad_row(cells: &[String], widths: &[usize]) -> String {
    let n = cells.len();
    cells
        .iter()
        .enumerate()
        .map(|(i, c)| {
            if i + 1 == n {
                c.clone()
            } else {
                format!("{:<width$}", c, width = widths[i])
            }
        })
        .collect::<Vec<_>>()
        .join("  ")
}

fn render_products_list(body: &Value) -> Result<String, String> {
    let parsed: ProductsListResponse = parse(body)?;
    let headers = [
        "PRODUCT_ID",
        "SOURCE_NAME",
        "STATUS",
        "FEED_KIND",
        "PRICE_INCR",
        "BASE_INCR",
    ];
    let rows: Vec<Vec<String>> = parsed
        .products
        .iter()
        .map(|p| {
            vec![
                p.product_id.clone(),
                p.source_name().to_string(),
                p.status.clone(),
                p.feed_kind.clone(),
                p.price_increment.clone(),
                p.base_increment.clone(),
            ]
        })
        .collect();
    Ok(table(&headers, &rows))
}

fn render_product_get(body: &Value) -> Result<String, String> {
    let parsed: ProductResponse = parse(body)?;
    let p = parsed.product;
    let source_name = p.source_name().to_string();
    let mut rows = vec![
        vec!["product_id".to_string(), p.product_id],
        vec!["source_name".to_string(), source_name],
        vec!["symbol".to_string(), p.symbol],
        vec!["channel".to_string(), p.channel.to_string()],
        vec!["instrument_id".to_string(), p.instrument_id.to_string()],
        vec!["price_increment".to_string(), p.price_increment],
        vec!["base_increment".to_string(), p.base_increment],
    ];
    // Only when the venue stated a tick: the row's presence is what tells a reader whether
    // `price_increment` above is that tick or the fixed-point granularity.
    if let Some(tick) = p.tick_size {
        rows.push(vec!["tick_size".to_string(), tick.to_string()]);
    }
    rows.extend([
        vec!["status".to_string(), p.status],
        vec!["feed_kind".to_string(), p.feed_kind],
    ]);
    Ok(table(&["FIELD", "VALUE"], &rows))
}

fn render_ticker(body: &Value) -> Result<String, String> {
    let parsed: TickerResponse = parse(body)?;
    let headers = ["TIME_NS", "PRICE", "SIZE"];
    let rows: Vec<Vec<String>> = parsed
        .trades
        .iter()
        .map(|t| vec![t.time_ns.clone(), t.price.clone(), t.size.clone()])
        .collect();
    let mut out = table(&headers, &rows);
    out.push_str("\n\n");
    out.push_str(&format!(
        "best_bid: {}\n",
        parsed.best_bid.as_deref().unwrap_or("-")
    ));
    out.push_str(&format!(
        "best_ask: {}",
        parsed.best_ask.as_deref().unwrap_or("-")
    ));
    Ok(out)
}

fn render_candles(body: &Value) -> Result<String, String> {
    let parsed: CandlesResponse = parse(body)?;
    let headers = ["START", "OPEN", "HIGH", "LOW", "CLOSE", "VOLUME"];
    let rows: Vec<Vec<String>> = parsed
        .candles
        .iter()
        .map(|c| {
            vec![
                c.start.clone(),
                c.open.clone(),
                c.high.clone(),
                c.low.clone(),
                c.close.clone(),
                c.volume.clone(),
            ]
        })
        .collect();
    let mut out = table(&headers, &rows);
    out.push_str("\n\n");
    out.push_str(&format!(
        "retention: window_seconds={} oldest={} newest={} truncated={} held={}",
        parsed.retention.window_seconds,
        parsed.retention.oldest,
        parsed.retention.newest,
        parsed.retention.truncated,
        parsed.retention.held,
    ));
    if !parsed.retention.held && parsed.candles.is_empty() {
        out.push_str(
            "\nnote: this product is not currently held in the history store (evicted for \
             capacity, or never seen) — this is not the same as a market with no trades.",
        );
    }
    Ok(out)
}

/// Render as a ladder: asks descending above, bids descending below, so the touch (best bid /
/// best ask) sits one row apart at the seam and depth falls away from it in both directions —
/// how a trading UI shows a book, and what makes the spread visible without scrolling past every
/// bid first. The wire's own `asks` array is ascending-from-best (`asks[0]` is the best ask, per
/// `best_levels`/`book` in `sinks/api.rs`), so only this table path reverses it for display; the
/// JSON response (`--output json`, `--jq`) is untouched — machine consumers keep the stable
/// per-side arrays.
fn render_book(body: &Value) -> Result<String, String> {
    let parsed: BookResponse = parse(body)?;
    let mut rows: Vec<Vec<String>> = Vec::new();
    for a in parsed.pricebook.asks.iter().rev() {
        rows.push(vec!["ask".to_string(), a[0].clone(), a[1].clone()]);
    }
    for b in &parsed.pricebook.bids {
        rows.push(vec!["bid".to_string(), b[0].clone(), b[1].clone()]);
    }
    let mut out = format!("product_id: {}\n\n", parsed.pricebook.product_id);
    out.push_str(&table(&["SIDE", "PRICE", "SIZE"], &rows));
    out.push_str("\n\n");
    out.push_str(&format!(
        "coverage: levels_returned={} levels_capped_at={} complete={}",
        parsed.coverage.levels_returned, parsed.coverage.levels_capped_at, parsed.coverage.complete
    ));
    Ok(out)
}

fn render_best_bid_ask(body: &Value) -> Result<String, String> {
    let parsed: BestBidAskResponse = parse(body)?;
    let headers = ["PRODUCT_ID", "BEST_BID", "BEST_ASK"];
    let rows: Vec<Vec<String>> = parsed
        .pricebooks
        .iter()
        .map(|p| {
            let bid = p
                .bids
                .first()
                .map(|b| format!("{} @ {}", b[0], b[1]))
                .unwrap_or_else(|| "-".to_string());
            let ask = p
                .asks
                .first()
                .map(|a| format!("{} @ {}", a[0], a[1]))
                .unwrap_or_else(|| "-".to_string());
            vec![p.product_id.clone(), bid, ask]
        })
        .collect();
    Ok(table(&headers, &rows))
}

fn render_status(body: &Value) -> Result<String, String> {
    let parsed: StatusResponse = parse(body)?;
    let mut out = String::new();
    if !parsed.registry.origin.is_empty() {
        out.push_str(&render_registry_line(&parsed.registry));
        out.push_str("\n\n");
    }

    let headers = ["VENUE", "STATUS"];
    let rows: Vec<Vec<String>> = parsed
        .venues
        .iter()
        .map(|v| vec![v.venue.clone(), v.status.clone()])
        .collect();
    out.push_str(&table(&headers, &rows));

    out.push_str("\n\n");
    out.push_str(&render_history_line(&parsed.history));

    if !parsed.channels.rows.is_empty() {
        out.push_str("\n\n");
        out.push_str(&render_channels_block(&parsed.channels));
    }

    out.push_str("\n\n");
    out.push_str(&render_process_line(&parsed.process));
    Ok(out)
}

/// The `history` summary line. Rule: the at-cap marker (`AT CAP`) is its own explicit token, never
/// left to be inferred from `products` happening to equal some hardcoded cap — `products_at_cap`
/// is the server's own honest signal (a bucket-budget eviction can hold `products` well below any
/// cap-looking number while still discarding markets), so this function trusts that flag alone.
fn render_history_line(h: &HistoryStatus) -> String {
    let products = if h.products > 0 {
        h.products
    } else {
        h.products_tracked
    };
    let cap_marker = if h.products_at_cap { " AT CAP" } else { "" };
    let pct = if h.bucket_budget > 0 {
        (h.buckets as f64 / h.bucket_budget as f64) * 100.0
    } else {
        0.0
    };
    format!(
        "history: products={products}{cap_marker}  buckets={}/{} ({pct:.0}%)  est_bytes={}  \
         window_seconds={}  evicted={}  late_drops={}",
        h.buckets, h.bucket_budget, h.est_bytes, h.window_seconds, h.evicted, h.late_drops
    )
}

/// The `channels` block, shared verbatim between `status`'s table view and `channels list`
/// (`channels.rs`) — one row per enabled feed row, one line per channel with its channel-filter
/// admission, real bound state and product count kept as visibly distinct columns (never collapsed
/// into one "admitted" notion — see the module-level rule this guards).
pub(crate) fn render_channels_block(channels: &ChannelsBlock) -> String {
    let mut sections = Vec::new();
    for row in &channels.rows {
        let mut out = format!("channels: {} ({}/{})\n", row.code, row.venue, row.category);
        let headers = ["CHANNEL", "NAME", "ALLOWED", "BOUND", "PRODUCTS"];
        let table_rows: Vec<Vec<String>> = row
            .channels
            .iter()
            .map(|c| {
                vec![
                    c.channel.to_string(),
                    c.display_name(),
                    c.allowed.to_string(),
                    c.bound.to_string(),
                    c.products.to_string(),
                ]
            })
            .collect();
        out.push_str(&table(&headers, &table_rows));
        out.push_str(&format!(
            "\n({} channels excluded by channel filter)",
            row.excluded
        ));
        sections.push(out);
    }
    sections.push(format!(
        "total channels excluded by channel filter: {}",
        channels.excluded_by_filter
    ));
    sections.join("\n\n")
}

/// The `registry` orientation line: which document resolved, its version, and how many rows/
/// receivers it carries. Deliberately one line — this is orientation for reading the rest of
/// `status`, not a report in its own right. `pub(crate)` because `diagnose` reports the identical
/// block off `/admin/diagnostics` and must read the same there.
pub(crate) fn render_registry_line(r: &RegistryBlock) -> String {
    format!(
        "registry: origin={}  version={}  rows={}  receivers={}",
        r.origin, r.version, r.rows, r.receivers
    )
}

fn render_process_line(p: &ProcessBlock) -> String {
    let rss = p
        .resident_memory_bytes
        .map(|v| format!("{v:.0}"))
        .unwrap_or_else(|| "-".to_string());
    let cpu = p
        .cpu_seconds_total
        .map(|v| format!("{v:.1}"))
        .unwrap_or_else(|| "-".to_string());
    format!("process: resident_memory_bytes={rss}  cpu_seconds_total={cpu}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_table_pads_columns_except_the_last() {
        let out = table(&["A", "BB"], &[vec!["1".to_string(), "two".to_string()]]);
        let lines: Vec<&str> = out.lines().collect();
        assert_eq!(lines[0], "A  BB");
        assert_eq!(lines[1], "-  ---");
        assert_eq!(lines[2], "1  two");
    }

    #[test]
    fn candles_table_surfaces_the_retention_block() {
        let body = serde_json::json!({
            "candles": [
                {"start": "1000", "low": "1", "high": "2", "open": "1", "close": "2", "volume": "5"}
            ],
            "retention": {"window_seconds": 3600, "oldest": "900", "newest": "1000", "truncated": true, "held": true}
        });
        let out = render_candles(&body).unwrap();
        assert!(
            out.contains(
                "retention: window_seconds=3600 oldest=900 newest=1000 truncated=true held=true"
            ),
            "{out}"
        );
    }

    /// The distinction the fix is for: an evicted product (`held: false`) with no candles must read
    /// differently from a genuinely quiet one — a fixture that only ever set `held: true` (or omitted
    /// it) could never express this, since both render an empty candle table otherwise.
    #[test]
    fn an_unheld_product_with_no_candles_gets_an_explanatory_note() {
        let body = serde_json::json!({
            "candles": [],
            "retention": {"window_seconds": 3600, "oldest": "1000", "newest": "1000", "truncated": false, "held": false}
        });
        let out = render_candles(&body).unwrap();
        assert!(out.contains("held=false"), "{out}");
        assert!(
            out.contains("not currently held"),
            "an unheld product with no candles must say so, not read like a quiet market: {out}"
        );
    }

    /// The mirror image: a genuinely quiet but still-tracked product gets no such note.
    #[test]
    fn a_held_product_with_no_candles_gets_no_note() {
        let body = serde_json::json!({
            "candles": [],
            "retention": {"window_seconds": 3600, "oldest": "1000", "newest": "1000", "truncated": false, "held": true}
        });
        let out = render_candles(&body).unwrap();
        assert!(out.contains("held=true"), "{out}");
        assert!(!out.contains("not currently held"), "{out}");
    }

    /// The ladder rule: asks descending above, bids descending below, touch at the seam. Three
    /// levels per side (never one — a single-level fixture can't tell ascending from descending
    /// apart) with prices that only read correctly under a real reversal of the wire's
    /// ascending-from-best `asks` array.
    #[test]
    fn book_table_renders_as_a_ladder_asks_above_bids_below() {
        let body = serde_json::json!({
            "pricebook": {
                "product_id": "HYPERLIQUID:BTC",
                "bids": [["0.9100", "1"], ["0.8000", "2"], ["0.0600", "3"]],
                "asks": [["0.9500", "4"], ["0.9700", "5"], ["0.9900", "6"]],
                "time": "5"
            },
            "coverage": {"levels_returned": 6, "levels_capped_at": 50, "complete": true}
        });
        let out = render_book(&body).unwrap();
        let price_rows: Vec<&str> = out
            .lines()
            .filter(|l| l.trim_start().starts_with("ask") || l.trim_start().starts_with("bid"))
            .collect();
        let prices: Vec<&str> = price_rows
            .iter()
            .map(|l| l.split_whitespace().nth(1).unwrap())
            .collect();
        assert_eq!(
            prices,
            vec!["0.9900", "0.9700", "0.9500", "0.9100", "0.8000", "0.0600"],
            "asks must descend to the touch, then bids continue descending away from it: {out}"
        );
        // The touch itself: best ask (0.9500) immediately above best bid (0.9100).
        assert_eq!(price_rows[2].split_whitespace().next().unwrap(), "ask");
        assert_eq!(price_rows[3].split_whitespace().next().unwrap(), "bid");
    }

    /// A product whose venue states a tick renders it beside the increment, so a reader can tell
    /// `price_increment` is that tick rather than the fixed-point granularity. The absent case is
    /// the `product_get` golden, whose Hyperliquid publisher states none.
    #[test]
    fn a_stated_tick_is_rendered_beside_the_increment() {
        let body = serde_json::json!({"product": {
            "product_id": "PHOENIX:BTC", "source_id": 2, "source_name": "Phoenix", "symbol": "BTC",
            "channel": 0, "instrument_id": 1, "price_increment": "1.00",
            "base_increment": "0.0001", "tick_size": 100,
            "status": "online", "feed_kind": "top_of_book"
        }});
        let out = render_product_get(&body).unwrap();
        assert!(out.contains("tick_size        100"), "{out}");
    }

    #[test]
    fn book_table_surfaces_the_coverage_block() {
        let body = serde_json::json!({
            "pricebook": {"product_id": "HYPERLIQUID:BTC", "bids": [["100.0", "1.0"]], "asks": [], "time": "5"},
            "coverage": {"levels_returned": 1, "levels_capped_at": 50, "complete": false}
        });
        let out = render_book(&body).unwrap();
        assert!(
            out.contains("coverage: levels_returned=1 levels_capped_at=50 complete=false"),
            "{out}"
        );
    }

    // -----------------------------------------------------------------------------------------
    // `status`'s at-cap marker (task brief Step 1). Two genuinely different fixture documents —
    // one with `products_at_cap: true` and a nonzero `evicted`, one with `products_at_cap: false`
    // and no evictions — never one fixture reused both ways.
    // -----------------------------------------------------------------------------------------

    fn fixture_status_at_cap() -> Value {
        serde_json::json!({
            "venues": [{"venue": "KALSHI", "status": "online"}],
            "history": {
                "products": 1024,
                "products_at_cap": true,
                "buckets": 900000,
                "bucket_budget": 1048576,
                "est_bytes": 90000000,
                "window_seconds": 3600,
                "evicted": 8214,
                "late_drops": 0
            }
        })
    }

    fn fixture_status_healthy() -> Value {
        serde_json::json!({
            "venues": [{"venue": "KALSHI", "status": "online"}],
            "history": {
                "products": 4,
                "products_at_cap": false,
                "buckets": 12,
                "bucket_budget": 1048576,
                "est_bytes": 1200,
                "window_seconds": 3600,
                "evicted": 0,
                "late_drops": 0
            }
        })
    }

    /// The at-cap marker is the operator's signal to narrow the channel filter, so it must be
    /// rendered distinctly rather than left to be inferred from two numbers being equal.
    #[test]
    fn the_table_marks_a_store_at_cap() {
        let out = render_status(&fixture_status_at_cap()).unwrap();
        assert!(out.contains("AT CAP"), "at-cap was not surfaced: {out}");
    }

    /// Below cap there is no warning — otherwise the marker means nothing.
    #[test]
    fn the_table_does_not_cry_wolf_below_cap() {
        let out = render_status(&fixture_status_healthy()).unwrap();
        assert!(!out.contains("AT CAP"), "{out}");
    }

    /// The `registry` line is orientation for the rest of `status`: which document, its version,
    /// and row/receiver counts, on one line.
    #[test]
    fn the_registry_line_reports_origin_version_and_counts() {
        let mut body = fixture_status_healthy();
        body["registry"] = serde_json::json!({
            "origin": "url https://get.doublezero.xyz/feeds/doublezero-edge-feeds-latest.json",
            "version": 3,
            "rows": 12,
            "receivers": 27
        });
        let out = render_status(&body).unwrap();
        assert!(
            out.contains(
                "registry: origin=url https://get.doublezero.xyz/feeds/doublezero-edge-feeds-latest.json  \
                 version=3  rows=12  receivers=27"
            ),
            "{out}"
        );
    }

    /// A server predating the `registry` block sends no such key, which deserializes to the
    /// `Default` (empty `origin`) — this must render as no line at all, never a blank
    /// "registry: origin=  version=0  rows=0  receivers=0" guess.
    #[test]
    fn the_registry_line_is_omitted_on_an_older_server() {
        let out = render_status(&fixture_status_healthy()).unwrap();
        assert!(!out.contains("registry:"), "{out}");
    }

    /// `bound` and `allowed` must read as visibly distinct columns, not be collapsed into one
    /// "admitted" notion — an admitted-but-not-bound channel (11 here) is the interesting case this
    /// project already shipped once as a single conflated field.
    #[test]
    fn the_channels_block_distinguishes_bound_from_allowed() {
        let body = serde_json::json!({
            "venues": [],
            "history": {
                "products": 2, "products_at_cap": false, "buckets": 2, "bucket_budget": 100,
                "est_bytes": 10, "window_seconds": 3600, "evicted": 0, "late_drops": 0
            },
            "channels": {
                "rows": [{
                    "venue": "KALSHI", "category": "sports", "code": "edge-kalshi-sports-mbp", "excluded": 29,
                    "channels": [
                        {"channel": 10, "allowed": true, "bound": true, "products": 2},
                        {"channel": 11, "allowed": true, "bound": false, "products": 0},
                        {"channel": 12, "allowed": false, "bound": false, "products": 0}
                    ]
                }],
                "excluded_by_filter": 29
            }
        });
        let out = render_status(&body).unwrap();
        // Locate each channel's row text and check its own two columns independently — this must
        // fail if `bound` and `allowed` were rendered as the same value.
        let line_10 = out
            .lines()
            .find(|l| l.trim_start().starts_with("10 "))
            .unwrap();
        assert!(
            line_10.contains("true") && line_10.matches("true").count() >= 2,
            "{line_10}"
        );
        let line_11 = out
            .lines()
            .find(|l| l.trim_start().starts_with("11 "))
            .unwrap();
        assert!(
            line_11.contains("true"),
            "channel 11 must still read allowed=true: {line_11}"
        );
        assert!(
            line_11.contains("false"),
            "channel 11 must read bound=false: {line_11}"
        );
        let line_12 = out
            .lines()
            .find(|l| l.trim_start().starts_with("12 "))
            .unwrap();
        assert!(
            line_12.matches("false").count() >= 2,
            "channel 12 is excluded and unbound, both false: {line_12}"
        );
    }
}
