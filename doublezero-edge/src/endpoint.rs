//! Which `/v1` route a command targets — shared between `main.rs` (path building, request
//! dispatch) and `render.rs` (which table shape to render), so the two can never drift out of sync
//! on what a given subcommand actually returns.

use serde_json::{json, Value};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Endpoint {
    ProductsList,
    ProductGet,
    Ticker,
    Candles,
    Book,
    BestBidAsk,
    Status,
    /// `channels list`'s synthetic body: `{"admin": <GET /admin/channels body>, "status": <GET
    /// /v1/status body>}` — two responses from two different binds (`--url` / `--admin-url`)
    /// merged into one value so `--jq`/`--output`/`--template` work the same way they do for
    /// every other command, over one JSON value.
    ChannelsList,
    /// `channels set`'s success body: `POST /admin/channels`'s own `{"applied": [...]}`.
    ChannelsSet,
    /// `diagnose`'s synthetic body: `{"diagnostics": <GET /admin/diagnostics body>, "status": <GET
    /// /v1/status body, or null>}` — same two-surface merge as [`Endpoint::ChannelsList`], except
    /// `/v1` is best-effort here, since it being down is what this command exists to explain.
    Diagnose,
}

impl Endpoint {
    /// The `--template` document: the query parameters this endpoint's `key==value` arguments are
    /// documented to accept. Deliberately static/hand-maintained rather than introspected from the
    /// server (there is nothing to introspect over a plain HTTP GET with no request body) — an
    /// agent uses this the same way it would read the emulated tool's own `--template` output, to
    /// learn the shape before spending a real request on it.
    pub fn template(self) -> Value {
        match self {
            Endpoint::Candles => json!({
                "granularity": "one of ONE_MINUTE, FIVE_MINUTE, FIFTEEN_MINUTE, THIRTY_MINUTE, \
                                ONE_HOUR, TWO_HOUR, SIX_HOUR, ONE_DAY (default: ONE_MINUTE)",
                "limit": "positive integer; server default 100, hard cap 350 \
                          (a capped request reports retention.truncated = true)",
            }),
            Endpoint::BestBidAsk => json!({
                "product_ids": "comma-separated product ids to filter to (e.g. A:X,A:Y); a bare \
                                positional id works too. Omit for every product.",
            }),
            Endpoint::ProductsList => json!({
                "limit": "positive integer page size; unset returns every product in one \
                          response (no server-side page size unless asked)",
                "cursor": "opaque pagination cursor from a prior response's \"cursor\" field; \
                           omit for the first page. See --paginate to follow it automatically.",
            }),
            Endpoint::ProductGet
            | Endpoint::Ticker
            | Endpoint::Book
            | Endpoint::Status
            | Endpoint::ChannelsList
            | Endpoint::ChannelsSet
            | Endpoint::Diagnose => json!({}),
        }
    }
}
