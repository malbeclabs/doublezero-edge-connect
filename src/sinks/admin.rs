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
//!   publishers/channels the floor currently **admits** (not necessarily running — see
//!   `get_channels`'s doc for why this surface can't yet say which receivers are actually up).
//! - `POST ?channels=<spec>` — replace the floor with a new one, same syntax as `--channels` /
//!   `DZ_CHANNELS` and validated by the exact same [`ChannelFloor::parse`] — reusing it, rather than
//!   writing a second, laxer validator, is what keeps this surface unable to bind a row the startup
//!   path would have refused. An invalid spec is a `400` and changes nothing. A valid one takes
//!   effect on the reconciler's *next* tick, through the existing spawn/abort diff
//!   (`ingest::reconcile`) — which is also what drops a departing channel's catalog/book/history
//!   state once the channel actually leaves the desired set, not the instant this handler returns.
//!
//! The spec travels as a query parameter, not a request body: this crate's hand-rolled
//! [`crate::sinks::http`] scaffolding never reads a request body (every other sink here is
//! `GET`-only), and a floor spec is small enough that a query parameter costs nothing a body would
//! have bought. Reuses [`crate::sinks::http::Request::query`] exactly as `sinks::api`'s
//! `candles`/`limit` parameters do. Because the natural client shape for a `POST` is a body (e.g.
//! `curl -d`, or an HTTP library's default `post(url, data=...)`), `POST` refuses two distinct
//! caller mistakes rather than silently doing the wrong thing: a **missing** `channels` parameter
//! (400 `missing_channels_parameter` — distinct from one present-and-empty, which is how an
//! operator explicitly clears the floor) and a **non-empty request body** (400
//! `unsupported_request_body`, detected via `Content-Length` — a body would otherwise be silently
//! ignored while the missing query parameter is read as "clear the floor," which is how this
//! becomes a production incident).
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

/// `GET /admin/channels` — the floor in force, plus which publishers each enabled row's floor
/// currently **admits**. Read straight through `ChannelFloor::publishers_for`, the same seam the
/// reconciler's own desired-set computation uses, so the admitted set here can never drift from
/// what the reconciler would compute from the same floor.
///
/// ⚠️ **`floor_admits` is the floor's admission, not the running receiver set.** A row's group must
/// also be subscribed (or the process running in the static always-on model) for any admitted
/// publisher to actually bind a socket — this surface has no handle on the reconciler's own `active`
/// map or `ingest::health::SharedFeedHealth` to report real liveness, so it reports what the floor
/// alone decides and says so explicitly (the `note` field) rather than naming the field "bound" and
/// leaving an operator to assume it means "currently receiving packets."
fn get_channels(state: &AdminState) -> Response {
    let floor = crate::model::lock(&state.floor).clone();
    let rows: Vec<Value> = state
        .enabled
        .iter()
        .map(|f| {
            let admitted: Vec<Value> = floor
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
                "floor_admits": admitted,
            })
        })
        .collect();
    ok_json(json!({
        "summary": floor.summary(),
        "rows": rows,
        "note": "floor_admits reflects the channel floor only, independent of subscription \
            gating — it is not the set of receivers actually running. A row's group must also be \
            subscribed for an admitted publisher to bind; see GET /v1/status for venue-level \
            liveness.",
    }))
}

/// `POST /admin/channels?channels=<spec>` — replace the floor. Validated by the exact same
/// [`ChannelFloor::parse`] the startup path uses: an invalid spec is a `400` and the floor is left
/// untouched, never partially applied — narrowing a flat row, an unknown code/id, and every other
/// startup refusal are refused here identically, for the same reasons (see `ingest::floor`'s docs).
///
/// Two request shapes are refused before `channels` is even looked at, both because the natural
/// client shape for a `POST` is a body and this endpoint does not read one:
/// - **No `channels` parameter at all** — `400 missing_channels_parameter`. Silently falling back
///   to `""` (as an absent parameter and an explicitly empty one would otherwise be indistinguishable)
///   would parse as "admit everything," replacing an operator's narrowing with the widest possible
///   floor on what looks like an unrelated typo or a library defaulting to a body.
/// - **A non-zero `Content-Length`** — `400 unsupported_request_body`, naming the query-parameter
///   form in the remedy. A body is otherwise silently ignored (this scaffolding never reads one),
///   which is exactly how `curl -XPOST -d 'lashay-4=10'` or `requests.post(url, data=...)` would
///   quietly widen the floor to admit-everything while looking, to the caller, like it worked.
fn post_channels(state: &AdminState, req: &Request) -> Response {
    if req.content_length > 0 {
        return json_status(
            "400 Bad Request",
            json!({
                "error": "unsupported_request_body",
                "message": "This endpoint does not read a request body.",
                "remediation": "Pass the floor spec as a query parameter: \
                    POST /admin/channels?channels=<spec>.",
            }),
        );
    }
    let Some(spec) = req.query("channels") else {
        return json_status(
            "400 Bad Request",
            json!({
                "error": "missing_channels_parameter",
                "message": "No `channels` query parameter was given.",
                "remediation": "Pass the floor spec as a query parameter, e.g. \
                    POST /admin/channels?channels=lashay-4=10,11. An empty value \
                    (?channels=) is how you explicitly clear the floor.",
            }),
        );
    };
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

    /// The default (empty) floor admits every publisher of every enabled row, and `GET` reports
    /// exactly that — the state every deployment that never touches this surface is in.
    #[tokio::test]
    async fn get_reports_the_floor_in_force_and_what_it_admits() {
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
        let admitted = body["rows"][0]["floor_admits"].as_array().unwrap();
        assert_eq!(
            admitted.len(),
            feed.publishers.len(),
            "an unnarrowed row admits every publisher"
        );
        assert!(
            body["note"].as_str().unwrap().contains("subscription"),
            "the response must say this reflects admission, not running receivers: {body}"
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
        let admitted = body["rows"][0]["floor_admits"].as_array().unwrap();
        assert_eq!(
            admitted.len(),
            1,
            "GET must reflect the just-applied narrowing"
        );
        assert_eq!(admitted[0]["channel"], 10);
    }

    /// An invalid spec is a `400` and the floor is left exactly as it was — never a partial apply,
    /// and never a silent reset to default. Starts from a **non-empty** floor (a prior valid `POST`)
    /// so an implementation that resets to empty on error cannot pass by coincidence — the failure
    /// mode I4 exists to catch. An id outside the row's roster is one of `ChannelFloor::parse`'s
    /// refusals; reusing it here (rather than a second, laxer check) is what this test actually
    /// pins.
    #[tokio::test]
    async fn an_invalid_spec_is_rejected_and_changes_nothing() {
        let floor = Arc::new(Mutex::new(ChannelFloor::default()));
        let feed = sports_row();
        let base = spawn(floor.clone(), vec![feed]).await;

        // Establish a non-empty starting floor.
        let setup = reqwest::Client::new()
            .post(format!("{base}/admin/channels"))
            .query(&[("channels", "lashay-4=10,11")])
            .send()
            .await
            .unwrap();
        assert_eq!(setup.status(), 200, "fixture sanity: the setup POST must apply");
        let before = crate::model::lock(&floor).clone();
        assert!(before.admits("lashay-4", 10) && before.admits("lashay-4", 11));
        assert!(!before.admits("lashay-4", 12));

        let resp = reqwest::Client::new()
            .post(format!("{base}/admin/channels"))
            .query(&[("channels", "lashay-4=250")]) // 250 is well outside the 31-channel roster
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 400);
        let body: Value = resp.json().await.unwrap();
        assert_eq!(body["error"], "invalid_channel_floor");

        let after = crate::model::lock(&floor).clone();
        assert_eq!(
            after.summary(),
            before.summary(),
            "a rejected spec must leave the prior non-empty floor exactly as it was"
        );
        assert!(after.admits("lashay-4", 10) && after.admits("lashay-4", 11));
        assert!(!after.admits("lashay-4", 12));
    }

    /// I2: an absent `channels` parameter is a `400`, distinct from one present-and-empty (which is
    /// how an operator explicitly clears the floor). The natural client shape for a `POST` is a
    /// body — this must not be read as "clear everything."
    #[tokio::test]
    async fn a_post_missing_the_channels_parameter_is_rejected_and_changes_nothing() {
        let floor = Arc::new(Mutex::new(ChannelFloor::default()));
        let feed = sports_row();
        let base = spawn(floor.clone(), vec![feed]).await;

        // Establish a non-empty starting floor, so a silent "clear" would be observable.
        reqwest::Client::new()
            .post(format!("{base}/admin/channels"))
            .query(&[("channels", "lashay-4=10")])
            .send()
            .await
            .unwrap();
        let before = crate::model::lock(&floor).clone();
        assert!(!before.is_empty(), "fixture sanity");

        let resp = reqwest::Client::new()
            .post(format!("{base}/admin/channels"))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 400);
        let body: Value = resp.json().await.unwrap();
        assert_eq!(body["error"], "missing_channels_parameter");

        let after = crate::model::lock(&floor).clone();
        assert_eq!(
            after.summary(),
            before.summary(),
            "a request naming no `channels` parameter must not silently clear the floor"
        );
    }

    /// I2: a non-empty request body is refused with a 400 naming the query-parameter form, rather
    /// than being silently ignored while an absent `channels` parameter widens the floor.
    #[tokio::test]
    async fn a_post_with_a_body_is_rejected_and_changes_nothing() {
        let floor = Arc::new(Mutex::new(ChannelFloor::default()));
        let feed = sports_row();
        let base = spawn(floor.clone(), vec![feed]).await;

        reqwest::Client::new()
            .post(format!("{base}/admin/channels"))
            .query(&[("channels", "lashay-4=10")])
            .send()
            .await
            .unwrap();
        let before = crate::model::lock(&floor).clone();
        assert!(!before.is_empty(), "fixture sanity");

        let resp = reqwest::Client::new()
            .post(format!("{base}/admin/channels"))
            .body("lashay-4=11")
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 400);
        let body: Value = resp.json().await.unwrap();
        assert_eq!(body["error"], "unsupported_request_body");
        assert!(
            body["remediation"].as_str().unwrap().contains("query parameter"),
            "the remedy must name the query-parameter form: {body}"
        );

        let after = crate::model::lock(&floor).clone();
        assert_eq!(
            after.summary(),
            before.summary(),
            "a body-bearing POST must not silently apply its body as the new floor"
        );
    }

    /// M7: covers the same breadth `sinks::api`'s method-refusal pinning does (`PUT`/`PATCH`/
    /// `DELETE`) rather than one method alone. `POST` is deliberately excluded from this set — it is
    /// the one method this surface legitimately accepts.
    #[tokio::test]
    async fn every_unsupported_method_is_refused() {
        let floor = Arc::new(Mutex::new(ChannelFloor::default()));
        let base = spawn(floor, vec![]).await;
        let client = reqwest::Client::new();

        for method in [reqwest::Method::PUT, reqwest::Method::PATCH, reqwest::Method::DELETE] {
            let resp = client
                .request(method.clone(), format!("{base}/admin/channels"))
                .send()
                .await
                .unwrap();
            assert_eq!(resp.status(), 405, "{method} was not refused by /admin/channels");
        }
    }
}
