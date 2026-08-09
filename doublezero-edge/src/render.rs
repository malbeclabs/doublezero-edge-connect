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
    }
}

fn parse<T: serde::de::DeserializeOwned>(body: &Value) -> Result<T, String> {
    serde_json::from_value(body.clone())
        .map_err(|e| format!("response did not match the expected shape: {e}"))
}

/// Build an aligned text table. Every column but the last is padded to its widest cell; the last
/// column is left unpadded so no row ends in trailing whitespace.
fn table(headers: &[&str], rows: &[Vec<String>]) -> String {
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
        "SOURCE",
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
                p.source.clone(),
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
    let rows = vec![
        vec!["product_id".to_string(), p.product_id],
        vec!["source".to_string(), p.source],
        vec!["symbol".to_string(), p.symbol],
        vec!["channel".to_string(), p.channel.to_string()],
        vec!["instrument_id".to_string(), p.instrument_id.to_string()],
        vec!["price_increment".to_string(), p.price_increment],
        vec!["base_increment".to_string(), p.base_increment],
        vec!["status".to_string(), p.status],
        vec!["feed_kind".to_string(), p.feed_kind],
    ];
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
        "retention: window_seconds={} oldest={} newest={} truncated={}",
        parsed.retention.window_seconds,
        parsed.retention.oldest,
        parsed.retention.newest,
        parsed.retention.truncated
    ));
    Ok(out)
}

fn render_book(body: &Value) -> Result<String, String> {
    let parsed: BookResponse = parse(body)?;
    let mut rows: Vec<Vec<String>> = Vec::new();
    for b in &parsed.pricebook.bids {
        rows.push(vec!["bid".to_string(), b[0].clone(), b[1].clone()]);
    }
    for a in &parsed.pricebook.asks {
        rows.push(vec!["ask".to_string(), a[0].clone(), a[1].clone()]);
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
    let headers = ["VENUE", "STATUS"];
    let rows: Vec<Vec<String>> = parsed
        .venues
        .iter()
        .map(|v| vec![v.venue.clone(), v.status.clone()])
        .collect();
    let mut out = table(&headers, &rows);
    out.push_str("\n\n");
    out.push_str(&format!(
        "history: products_tracked={} late_drops={} evicted={} window_seconds={}",
        parsed.history.products_tracked,
        parsed.history.late_drops,
        parsed.history.evicted,
        parsed.history.window_seconds
    ));
    Ok(out)
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
            "retention": {"window_seconds": 3600, "oldest": "900", "newest": "1000", "truncated": true}
        });
        let out = render_candles(&body).unwrap();
        assert!(
            out.contains("retention: window_seconds=3600 oldest=900 newest=1000 truncated=true"),
            "{out}"
        );
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
}
