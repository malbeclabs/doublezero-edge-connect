//! The **admin** surface: the one mutation path in this crate, entirely separate from the
//! read-only `/v1` query API ([`crate::sinks::api`]).
//!
//! `/v1` must stay provably read-only — an agent pointed at it cannot change what a shared process
//! ingests regardless of what it sends (see `api::handle`'s method guard, and its pinning test) — so
//! runtime changes to the ingest floor live here, on their own bind, off unless `--admin-bind` /
//! `DZ_ADMIN_BIND` is set. There is **no authentication** on this surface: under host networking a
//! wildcard bind is genuinely network-reachable, so the documented recommendation is a loopback
//! bind (`127.0.0.1:<port>`), never a bare default-on wildcard — see the flag's doc comment in
//! `main.rs`.
//!
//! Two endpoints, both scoped to `/admin/channels`:
//! - `GET` — the floor in force (`ChannelFloor::summary`) plus, per row this process may run, which
//!   publishers/channels it currently binds.
//! - `POST ?channels=<spec>` — replace the floor with a new one, same syntax as `--channels` /
//!   `DZ_CHANNELS` and validated by the exact same [`ChannelFloor::parse`] — reusing it, rather than
//!   writing a second, laxer validator, is what keeps this surface unable to bind a row the startup
//!   path would have refused. An invalid spec is a `400` and changes nothing. A valid one takes
//!   effect on the reconciler's *next* tick, through the existing spawn/abort diff
//!   (`ingest::reconcile`) — which is also what drops a departing channel's history
//!   (`history::Store::forget_channel`) once its receiver is actually aborted, not the instant this
//!   handler returns.
//!
//! The spec travels as a query parameter, not a request body: this crate's hand-rolled
//! [`crate::sinks::http`] scaffolding only ever parses the request line (no body reading, by
//! design — every other sink here is `GET`-only), and a floor spec is small enough that a query
//! parameter costs nothing a body would have bought. Reuses [`crate::sinks::http::Request::query`]
//! exactly as `sinks::api`'s `candles`/`limit` parameters do.
//!
//! Bind/serve is split exactly as [`crate::sinks::ws`] / [`crate::sinks::api`]: a taken port
//! disables this surface without taking the tunnel down. Unlike those two, this surface is **not**
//! subscription-gated — an operator must be able to inspect or change the floor even when nothing is
//! currently subscribed, e.g. to prepare a narrowing before subscribing at all — so it is spawned
//! once at startup, gated only on `--admin-bind` being non-empty.

use std::sync::{Arc, Mutex};

use anyhow::Result;
use serde_json::{json, Value};
use tokio::net::TcpListener;
use tracing::info;

use super::http::{self, Request, Response};
use crate::ingest::{feeds::Feed, floor::ChannelFloor};

/// Small, operator-only surface: no need for the concurrency `sinks::api`/`sinks::metrics` allow.
const MAX_CONNS: usize = 8;

/// Shared state the admin handler reads and mutates.
struct AdminState {
    /// The runtime-mutable floor, shared with the reconciler (`ReconcilerConfig::floor`). A `POST`
    /// replaces its contents in place so the reconciler's very next tick sees the change with no
    /// other plumbing.
    floor: Arc<Mutex<ChannelFloor>>,
    /// The rows this process may run (`--feed`/`--publisher-port`-selected), for `GET`'s per-row
    /// report. Fixed for the process's lifetime — only the floor changes at runtime.
    enabled: Vec<Feed>,
}

/// Bind the listener up front so the caller (`main`) can decide what a bind failure means — mirrors
/// [`crate::sinks::ws::bind`] / [`crate::sinks::api::bind`]. A taken port must not be fatal to the
/// whole process.
pub async fn bind(addr: &str) -> Result<TcpListener> {
    let listener = TcpListener::bind(addr).await?;
    info!(bind = %addr, "admin surface listening (mutating — no authentication)");
    Ok(listener)
}

/// The accept loop, split out so tests can drive a pre-bound listener.
pub async fn serve(
    listener: TcpListener,
    floor: Arc<Mutex<ChannelFloor>>,
    enabled: Vec<Feed>,
) -> Result<()> {
    let state = Arc::new(AdminState { floor, enabled });
    http::serve_loop(
        listener,
        MAX_CONNS,
        Arc::new(move |req: &Request| handle(&state, req)),
    )
    .await
}

/// Answer one parsed request.
fn handle(state: &AdminState, req: &Request) -> Response {
    match (req.method.as_str(), req.path.as_str()) {
        ("GET", "/admin/channels") => get_channels(state),
        ("POST", "/admin/channels") => post_channels(state, req),
        (method, "/admin/channels") => method_not_allowed(method),
        _ => unknown_endpoint(&req.path),
    }
}

/// `GET /admin/channels` — the floor in force, plus which publishers each enabled row currently
/// binds under it. Read straight through `ChannelFloor::publishers_for`, the same seam the
/// reconciler's own desired-set computation uses, so this can never claim a row binds something the
/// reconciler would not actually spawn.
fn get_channels(state: &AdminState) -> Response {
    let floor = crate::model::lock(&state.floor).clone();
    let rows: Vec<Value> = state
        .enabled
        .iter()
        .map(|f| {
            let bound: Vec<Value> = floor
                .publishers_for(f)
                .iter()
                .map(|p| {
                    json!({
                        "base_port": p.base_port(),
                        "channel": p.channel,
                    })
                })
                .collect();
            json!({
                "venue": f.venue,
                "category": f.category,
                "code": f.code,
                "kind": f.kind.label(),
                "bound_publishers": bound,
            })
        })
        .collect();
    ok_json(json!({
        "summary": floor.summary(),
        "rows": rows,
    }))
}

/// `POST /admin/channels?channels=<spec>` — replace the floor. Validated by the exact same
/// [`ChannelFloor::parse`] the startup path uses: an invalid spec is a `400` and the floor is left
/// untouched, never partially applied — narrowing a flat row, an unknown code/id, and every other
/// startup refusal are refused here identically, for the same reasons (see `ingest::floor`'s docs).
fn post_channels(state: &AdminState, req: &Request) -> Response {
    let spec = req.query("channels").unwrap_or("");
    match ChannelFloor::parse(spec) {
        Ok(new_floor) => {
            let summary = new_floor.summary();
            *crate::model::lock(&state.floor) = new_floor;
            info!(
                channels = ?summary,
                "channel floor replaced via admin surface; applies on the next reconcile tick"
            );
            ok_json(json!({ "applied": summary }))
        }
        Err(e) => json_status(
            "400 Bad Request",
            json!({
                "error": "invalid_channel_floor",
                "message": e.to_string(),
                "remediation": "Use the same syntax as --channels/DZ_CHANNELS: \
                    <code>=<id>[,<id>...][;<code>=...].",
            }),
        ),
    }
}

fn method_not_allowed(method: &str) -> Response {
    json_status(
        "405 Method Not Allowed",
        json!({
            "error": "method_not_allowed",
            "message": format!("\"{method}\" is not supported on /admin/channels."),
            "remediation": "Use GET to read the floor or POST to replace it.",
        }),
    )
}

fn unknown_endpoint(path: &str) -> Response {
    json_status(
        "404 Not Found",
        json!({
            "error": "unknown_endpoint",
            "message": format!("\"{path}\" is not a route this surface serves."),
            "remediation": "Use GET or POST /admin/channels.",
        }),
    )
}

fn ok_json(v: Value) -> Response {
    json_status("200 OK", v)
}

fn json_status(status: &'static str, v: Value) -> Response {
    (
        status,
        "application/json".to_string(),
        serde_json::to_vec(&v).unwrap_or_default(),
    )
}

#[cfg(test)]
mod tests {
    use tokio::net::TcpListener;

    use super::*;

    /// The real built-in "sports" row (group code `lashay-4`) — a genuinely derived,
    /// multi-channel row already used the same way by `ingest::floor`'s and `ingest::reconcile`'s
    /// own tests. `ChannelFloor::parse` validates against the loaded registry
    /// ([`ChannelFloor::parse`]'s own docs), so a fixture built from an ad hoc `Feed` (an unknown
    /// code) would make every `POST` in this module 400 regardless of what this handler does; using
    /// the real row is what lets a valid spec actually apply.
    fn sports_row() -> Feed {
        *crate::ingest::feeds::feeds()
            .iter()
            .find(|f| f.category == "sports")
            .expect("the built-in registry has a sports row")
    }

    async fn spawn(floor: Arc<Mutex<ChannelFloor>>, enabled: Vec<Feed>) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let _ = serve(listener, floor, enabled).await;
        });
        format!("http://{addr}")
    }

    /// The default (empty) floor binds every publisher of every enabled row, and `GET` reports
    /// exactly that — the state every deployment that never touches this surface is in.
    #[tokio::test]
    async fn get_reports_the_floor_in_force_and_the_bound_publishers() {
        let floor = Arc::new(Mutex::new(ChannelFloor::default()));
        let feed = sports_row();
        let base = spawn(floor, vec![feed]).await;

        let resp = reqwest::get(format!("{base}/admin/channels"))
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
        let body: Value = resp.json().await.unwrap();
        assert_eq!(
            body["summary"].as_array().unwrap().len(),
            0,
            "empty floor narrows nothing"
        );
        let bound = body["rows"][0]["bound_publishers"].as_array().unwrap();
        assert_eq!(
            bound.len(),
            feed.publishers.len(),
            "an unnarrowed row binds every publisher"
        );
    }

    /// A valid `POST` replaces the shared floor in place — the same instance the reconciler reads —
    /// and a subsequent `GET` reflects it immediately, without needing a reconcile tick to observe
    /// the change (the tick is what applies it to the running receiver set, not what stores it).
    #[tokio::test]
    async fn post_replaces_the_shared_floor() {
        let floor = Arc::new(Mutex::new(ChannelFloor::default()));
        let feed = sports_row();
        let base = spawn(floor.clone(), vec![feed]).await;

        let resp = reqwest::Client::new()
            .post(format!("{base}/admin/channels"))
            .query(&[("channels", "lashay-4=10")])
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);

        let narrowed = crate::model::lock(&floor).clone();
        assert!(narrowed.admits("lashay-4", 10));
        assert!(
            !narrowed.admits("lashay-4", 11),
            "the POST must narrow the live floor"
        );

        let resp = reqwest::get(format!("{base}/admin/channels"))
            .await
            .unwrap();
        let body: Value = resp.json().await.unwrap();
        let bound = body["rows"][0]["bound_publishers"].as_array().unwrap();
        assert_eq!(bound.len(), 1, "GET must reflect the just-applied narrowing");
        assert_eq!(bound[0]["channel"], 10);
    }

    /// An invalid spec is a `400` and the floor is left exactly as it was — never a partial apply.
    /// An id outside the row's roster is one of `ChannelFloor::parse`'s refusals; reusing it here
    /// (rather than a second, laxer check) is what this test actually pins.
    #[tokio::test]
    async fn an_invalid_spec_is_rejected_and_changes_nothing() {
        let floor = Arc::new(Mutex::new(ChannelFloor::default()));
        let feed = sports_row();
        let base = spawn(floor.clone(), vec![feed]).await;

        let resp = reqwest::Client::new()
            .post(format!("{base}/admin/channels"))
            .query(&[("channels", "lashay-4=250")]) // 250 is well outside the 31-channel roster
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 400);
        let body: Value = resp.json().await.unwrap();
        assert_eq!(body["error"], "invalid_channel_floor");

        assert!(
            crate::model::lock(&floor).is_empty(),
            "a rejected spec must leave the floor untouched"
        );
    }

    /// A method other than GET/POST on `/admin/channels` is refused rather than silently routed
    /// somewhere.
    #[tokio::test]
    async fn an_unsupported_method_is_refused() {
        let floor = Arc::new(Mutex::new(ChannelFloor::default()));
        let base = spawn(floor, vec![]).await;

        let resp = reqwest::Client::new()
            .delete(format!("{base}/admin/channels"))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 405);
    }
}
