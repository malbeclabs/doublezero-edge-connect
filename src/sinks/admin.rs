//! The **admin** surface: the one mutation path in this crate, entirely separate from the
//! read-only `/v1` query API ([`crate::sinks::api`]).
//!
//! `/v1` must stay provably read-only — an agent pointed at it cannot change what a shared process
//! ingests regardless of what it sends (see `api::handle`'s method guard, and its pinning test) — so
//! every runtime change lives here, on its own bind, **on by default at loopback**
//! (`--admin-bind` / `DZ_ADMIN_BIND`, `127.0.0.1:9098`; set it empty to disable the surface
//! outright). There is **no authentication**: under host networking a wildcard bind is genuinely
//! network-reachable, so the exposure is accepted on the condition that the default never reaches
//! past loopback — see the flag's doc comment in `main.rs`.
//!
//! Three routes. `GET /admin/diagnostics` is read-only and needs no header: this surface is **not**
//! subscription-gated, so on a host whose tunnel never came up — where `/v1` is not listening and
//! `doublezero-edge` can only report a transport error — it is the one thing that answers, and it
//! answers with a verdict ([`crate::ingest::diagnostics`]) rather than raw state. It only ever
//! *reports*; retrying the tunnel itself stays where it already is, `doublezero connect multicast`
//! inside the container, so nothing here can spend the container's onchain identity.
//!
//! The other two are scoped to `/admin/channels`:
//! - `GET` — the channel filter in force (`ChannelFilter::summary`) plus, per feed this process may
//!   run, which publishers/channels the channel filter currently **admits** (not necessarily
//!   running — see `get_channels`'s doc for why this surface can't yet say which receivers are
//!   actually up).
//! - `POST ?channels=<spec>` — replace the channel filter with a new one, same syntax as
//!   `--channels` / `DZ_CHANNELS` and validated by the exact same [`ChannelFilter::parse`] — reusing
//!   it, rather than writing a second, laxer validator, is what keeps this surface unable to bind a
//!   feed the startup path would have refused. An invalid spec is a `400` and changes nothing. A
//!   valid one takes effect on the reconciler's *next* tick, through the existing spawn/abort diff
//!   (`ingest::reconcile`) — which is also what drops a departing channel's catalog/book/history
//!   state once the channel actually leaves the desired set, not the instant this handler returns.
//!
//! The spec travels as a query parameter, not a request body: this crate's hand-rolled
//! [`crate::sinks::http`] scaffolding never reads a request body (every other sink here is
//! `GET`-only), and a channel filter spec is small enough that a query parameter costs nothing a
//! body would have bought. Reuses [`crate::sinks::http::Request::query`] exactly as `sinks::api`'s
//! `candles`/`limit` parameters do. Because the natural client shape for a `POST` is a body (e.g.
//! `curl -d`, or an HTTP library's default `post(url, data=...)`), `POST` refuses two distinct
//! caller mistakes rather than silently doing the wrong thing: a **missing** `channels` parameter
//! (400 `missing_channels_parameter` — distinct from one present-and-empty, which is how an
//! operator explicitly clears the channel filter) and a **non-empty request body** (400
//! `unsupported_request_body`, detected via `Content-Length` — a body would otherwise be silently
//! ignored while the missing query parameter is read as "clear the channel filter," which is how
//! this becomes a production incident).
//!
//! Bind/serve is split exactly as [`crate::sinks::ws`] / [`crate::sinks::api`]: a taken port
//! disables this surface without taking the tunnel down. Unlike those two, this surface is **not**
//! subscription-gated — an operator must be able to inspect or change the channel filter, and to
//! diagnose and retry a tunnel, when nothing is currently subscribed at all — so it is spawned once
//! at startup, gated only on `--admin-bind` being non-empty.
//!
//! **CSRF on the mutating endpoints.** Loopback does not protect a `POST` here from a web
//! page open in a browser on the same host: the page can point a plain HTML `<form>` at
//! `http://127.0.0.1:<port>/admin/channels?channels=<spec>`, and the browser sends that request with
//! no involvement from this crate at all. A `<form>` (or any request built from the small set of
//! CORS-"simple" content types) can only carry a handful of fixed headers — it cannot add an
//! arbitrary one — so every `POST` here requires [`REQUIRED_HEADER`] to be present
//! ([`mutation_guards`]). `GET` routes are exempt: the header guards mutations, and requiring it on
//! the diagnostics read would make the one command a stuck operator most needs harder to run than
//! `curl`.
//! The header only needs to be **present**, not carry a shared-secret value: this surface documents
//! itself as having no authentication (see above), and a value would imply exactly that, plus the
//! provisioning/rotation problem that comes with it. What the header actually rules out is not "an
//! unauthorized caller" but "a request a browser page could have caused by accident" — a
//! `curl -H 'x-dz-admin-request: 1'` from anyone reaching loopback still succeeds, same as today.
//!
//! ⚠️ **What `GET /admin/diagnostics` discloses, and to whom.** It is unauthenticated by the same
//! decision as the rest of this surface, and it is the most *informative* route here: device and
//! metro names, the tunnel name, every subscribed group code, the subscription rows' multicast IPs,
//! all four configured binds, and the feed-registry origin URL. On the loopback default that is the
//! same audience that could already run `doublezero status`. Two ways it widens, both stated rather
//! than defended against:
//! - **A non-loopback `--admin-bind`** hands all of the above to anyone who can reach the port. That
//!   is the operator's call, and the flag's doc says so.
//! - **DNS rebinding** — a page served from a name re-pointed at `127.0.0.1` is same-origin with
//!   this surface and can both set headers and read responses, so the CSRF header above does not
//!   stop it reading this route. Refusing a `Host` that names a DNS name would close that, at the
//!   cost of `--admin-url http://myhost.local:9098`; the read is left open deliberately, because
//!   this is inventory an attacker on the host can read directly anyway and the route exists for
//!   the operator who is already stuck.

use std::sync::{Arc, Mutex};

use anyhow::Result;
use serde_json::{json, Value};
use tokio::net::TcpListener;
use tracing::info;

use super::http::{self, Request, Response};
use crate::ingest::{
    channel_filter::ChannelFilter,
    diagnostics::{diagnose, SharedDiagnostics},
    feeds::Feed,
};

/// Small, operator-only surface: no need for the concurrency `sinks::api`/`sinks::metrics` allow.
const MAX_CONNS: usize = 8;

/// Which surfaces this process was *configured* with, reported by `GET /admin/diagnostics`. The
/// "is it even enabled" half of the question — whether a configured sink is currently *activated*
/// is the `activation` block's job, and the two disagree exactly when subscription gating is doing
/// its work.
#[derive(Debug, Default, Clone)]
pub struct Binds {
    pub ws: String,
    pub api: String,
    pub admin: String,
    pub metrics: String,
}

/// Everything [`serve`] needs. A struct rather than a widening argument list — this surface now
/// carries four unrelated pieces of shared state and positional arguments stop being readable.
pub struct AdminConfig {
    pub filter: Arc<Mutex<ChannelFilter>>,
    pub enabled: Vec<Feed>,
    pub diagnostics: SharedDiagnostics,
    pub binds: Binds,
}

/// Header a mutating request must carry (any value — see [`post_channels`]'s docs for why presence
/// alone is the right bar). Named on the `doublezero-edge` CLI side too (`client::admin_post_channels`);
/// there is no shared crate between them, so the literal is duplicated with a comment pointing here.
const REQUIRED_HEADER: &str = "x-dz-admin-request";

/// Shared state the admin handler reads and mutates.
struct AdminState {
    /// The runtime-mutable channel filter, shared with the reconciler
    /// (`ReconcilerConfig::filter`). A `POST` replaces its contents in place so the reconciler's
    /// very next tick sees the change with no other plumbing.
    filter: Arc<Mutex<ChannelFilter>>,
    /// The feeds this process may run (`--feed`/`--publisher-port`-selected), for `GET`'s per-feed
    /// report. Fixed for the process's lifetime — only the channel filter changes at runtime.
    enabled: Vec<Feed>,
    /// Tunnel/subscription/activation state, published by the reconciler each tick. This surface
    /// reads it and writes only [`crate::ingest::diagnostics::DiagnosticsSnapshot::last_attempt`].
    diagnostics: SharedDiagnostics,
    binds: Binds,
}

impl AdminState {
    fn new(cfg: AdminConfig) -> Self {
        Self {
            filter: cfg.filter,
            enabled: cfg.enabled,
            diagnostics: cfg.diagnostics,
            binds: cfg.binds,
        }
    }
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
pub async fn serve(listener: TcpListener, cfg: AdminConfig) -> Result<()> {
    serve_state(listener, Arc::new(AdminState::new(cfg))).await
}

/// [`serve`] over an already-built state — the seam a handler test uses to drive routes against a
/// hand-built snapshot.
async fn serve_state(listener: TcpListener, state: Arc<AdminState>) -> Result<()> {
    http::serve_loop(
        listener,
        MAX_CONNS,
        Arc::new(move |req: &Request| handle(&state, req)),
    )
    .await
}

/// Answer one parsed request.
fn handle(state: &Arc<AdminState>, req: &Request) -> Response {
    match (req.method.as_str(), req.path.as_str()) {
        ("GET", "/admin/channels") => get_channels(state),
        ("POST", "/admin/channels") => post_channels(state, req),
        (method, "/admin/channels") => method_not_allowed(method, "/admin/channels"),
        ("GET", "/admin/diagnostics") => get_diagnostics(state),
        (method, "/admin/diagnostics") => method_not_allowed(method, "/admin/diagnostics"),
        _ => unknown_endpoint(&req.path),
    }
}

/// `GET /admin/diagnostics` — why this process is (or is not) serving data, from the state the
/// reconciler already published. Read-only, so it needs no [`REQUIRED_HEADER`]: that header guards
/// mutations, and requiring it here would make the one command a stuck operator most needs harder
/// to run than `curl`.
///
/// **No shell-out and no blocking work.** Everything here is a lock, a clone and a pure function
/// ([`diagnose`]) over cached state, so this route can never add latency to the reconciler's poll
/// or wedge on a hung `doublezero` invocation — which is exactly the condition it exists to report.
fn get_diagnostics(state: &Arc<AdminState>) -> Response {
    let diag = crate::model::lock(&state.diagnostics);
    let mut body = diag.to_json();
    let verdict = diagnose(&diag);
    drop(diag);
    if let Some(map) = body.as_object_mut() {
        map.insert("diagnosis".to_string(), verdict.to_json());
        map.insert("registry".to_string(), crate::sinks::api::registry_block());
        map.insert("process".to_string(), crate::sinks::api::process_block());
        map.insert(
            "binds".to_string(),
            json!({
                "ws": state.binds.ws,
                "api": state.binds.api,
                "admin": state.binds.admin,
                "metrics": state.binds.metrics,
            }),
        );
    }
    ok_json(body)
}

/// The refusals `POST /admin/channels` applies before reading anything — see [`post_channels`].
/// Returns `None` when the request may proceed.
fn mutation_guards(req: &Request, route: &str) -> Option<Response> {
    if req.header(REQUIRED_HEADER).is_none() {
        return Some(json_status(
            "403 Forbidden",
            json!({
                "error": "missing_admin_header",
                "message": format!(
                    "Request did not carry the required `{REQUIRED_HEADER}` header."
                ),
                "remediation": format!(
                    "Add a `{REQUIRED_HEADER}` header (any value) — this is what stops a web \
                     page's form post from silently reaching {route}; see the \
                     admin-surface section of the README."
                ),
            }),
        ));
    }
    if req.content_length > 0 {
        return Some(json_status(
            "400 Bad Request",
            json!({
                "error": "unsupported_request_body",
                "message": "This endpoint does not read a request body.",
                "remediation": format!(
                    "Send {route} with no body; any parameters travel as query parameters."
                ),
            }),
        ));
    }
    None
}

/// `GET /admin/channels` — the channel filter in force, plus which publishers each enabled feed's
/// channel filter currently **admits**. Read straight through `ChannelFilter::publishers_for`, the
/// same seam the reconciler's own desired-set computation uses, so the admitted set here can never
/// drift from what the reconciler would compute from the same channel filter.
///
/// ⚠️ **`allowed` is the channel filter's admission, not the running receiver set.** A feed's group
/// must also be subscribed (or the process running in the static always-on model) for any admitted
/// publisher to actually bind a socket — this surface has no handle on the reconciler's own `active`
/// map or `ingest::health::SharedFeedHealth` to report real liveness, so it reports what the channel
/// filter alone decides and says so explicitly (the `note` field) rather than naming the field
/// "bound" and leaving an operator to assume it means "currently receiving packets."
fn get_channels(state: &AdminState) -> Response {
    let filter = crate::model::lock(&state.filter).clone();
    let rows: Vec<Value> = state
        .enabled
        .iter()
        .map(|f| {
            let admitted: Vec<Value> = filter
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
                "allowed": admitted,
            })
        })
        .collect();
    ok_json(json!({
        "summary": filter.summary(),
        "rows": rows,
        "note": "allowed reflects the channel filter only, independent of subscription \
            gating — it is not the set of receivers actually running. A feed's group must also be \
            subscribed for an admitted publisher to bind; see GET /v1/status for venue-level \
            liveness.",
    }))
}

/// `POST /admin/channels?channels=<spec>` — replace the channel filter. Validated by the exact same
/// [`ChannelFilter::parse`] the startup path uses: an invalid spec is a `400` and the channel filter
/// is left untouched, never partially applied — narrowing a flat feed, an unknown code/id, and every
/// other startup refusal are refused here identically, for the same reasons (see
/// `ingest::channel_filter`'s docs).
///
/// A spec that parses fine on its own can still, combined with `state.enabled` (this process's own
/// `--feed`/`--publisher-port` narrowing), admit **zero** publishers of an enabled feed —
/// `ChannelFilter::parse` only validates a clause against the whole registry, not against what this
/// process actually runs. At startup that combination is fatal; here it is refused with `400` and
/// the running channel filter is left exactly as it was, because a runtime misconfiguration must
/// never tear down every receiver (plus the WS sink and query API, if that was the only market-data
/// feed) on the next reconcile tick with no warning — the asymmetry startup/runtime already draws
/// everywhere else in this crate.
///
/// Three request shapes are refused before `channels` is even looked at:
/// - **No [`REQUIRED_HEADER`]** — `403 missing_admin_header`. This is the CSRF defense (see the
///   module docs): a browser `<form>` post cannot set this header, so its absence marks the request
///   as one this surface must not have honored, regardless of body or query string.
/// - **No `channels` parameter at all** — `400 missing_channels_parameter`. Silently falling back
///   to `""` (as an absent parameter and an explicitly empty one would otherwise be indistinguishable)
///   would parse as "admit everything," replacing an operator's narrowing with the widest possible
///   channel filter on what looks like an unrelated typo or a library defaulting to a body.
/// - **A non-zero `Content-Length`** — `400 unsupported_request_body`, naming the query-parameter
///   form in the remedy. A body is otherwise silently ignored (this scaffolding never reads one),
///   which is exactly how `curl -XPOST -d 'edge-kalshi-sports-mbp=10'` or `requests.post(url, data=...)` would
///   quietly widen the channel filter to admit-everything while looking, to the caller, like it
///   worked.
fn post_channels(state: &AdminState, req: &Request) -> Response {
    if let Some(refusal) = mutation_guards(req, "/admin/channels") {
        return refusal;
    }
    let Some(spec) = req.query("channels") else {
        return json_status(
            "400 Bad Request",
            json!({
                "error": "missing_channels_parameter",
                "message": "No `channels` query parameter was given.",
                "remediation": "Pass the channel filter spec as a query parameter, e.g. \
                    POST /admin/channels?channels=edge-kalshi-sports-mbp=10,11. An empty value \
                    (?channels=) is how you explicitly clear the channel filter.",
            }),
        );
    };
    match ChannelFilter::parse(spec) {
        Ok(new_filter) => {
            if let Some(f) = state
                .enabled
                .iter()
                .find(|f| new_filter.publishers_for(f).is_empty())
            {
                return json_status(
                    "400 Bad Request",
                    json!({
                        "error": "channel_filter_empties_a_feed",
                        "message": format!(
                            "this channel filter admits no publisher of enabled feed {} \
                             ({}, code {})",
                            f.venue, f.category, f.code
                        ),
                        "remediation": "Narrow --channels less aggressively, or leave that \
                            feed's code unmentioned so it keeps admitting every publisher this \
                            process runs. The prior channel filter is unchanged.",
                    }),
                );
            }
            let summary = new_filter.summary();
            *crate::model::lock(&state.filter) = new_filter;
            info!(
                channels = ?summary,
                "channel filter replaced via admin surface; applies on the next reconcile tick"
            );
            ok_json(json!({ "applied": summary }))
        }
        Err(e) => json_status(
            "400 Bad Request",
            json!({
                "error": "invalid_channel_filter",
                "message": e.to_string(),
                "remediation": "Use the same syntax as --channels/DZ_CHANNELS: \
                    <code>=<id>[,<id>...][;<code>=...].",
            }),
        ),
    }
}

fn method_not_allowed(method: &str, route: &str) -> Response {
    json_status(
        "405 Method Not Allowed",
        json!({
            "error": "method_not_allowed",
            "message": format!("\"{method}\" is not supported on {route}."),
            "remediation": ROUTE_LIST,
        }),
    )
}

fn unknown_endpoint(path: &str) -> Response {
    json_status(
        "404 Not Found",
        json!({
            "error": "unknown_endpoint",
            "message": format!("\"{path}\" is not a route this surface serves."),
            "remediation": ROUTE_LIST,
        }),
    )
}

const ROUTE_LIST: &str = "This surface serves GET/POST /admin/channels, GET /admin/diagnostics, \
     and POST /admin/connect and /admin/disconnect.";

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

    /// The real built-in "sports" feed (group code `edge-kalshi-sports-mbp`) — a genuinely derived,
    /// multi-channel feed already used the same way by `ingest::channel_filter`'s and
    /// `ingest::reconcile`'s own tests. `ChannelFilter::parse` validates against the loaded registry
    /// ([`ChannelFilter::parse`]'s own docs), so a fixture built from an ad hoc `Feed` (an unknown
    /// code) would make every `POST` in this module 400 regardless of what this handler does; using
    /// the real feed is what lets a valid spec actually apply.
    fn sports_row() -> Feed {
        *crate::ingest::feeds::feeds()
            .iter()
            .find(|f| f.category == "sports")
            .expect("the built-in registry has a sports row")
    }

    /// The sports row narrowed to a single publisher (`channel`'s), mirroring what
    /// `main.rs::filter_publishers` does to `state.enabled` when the process was started with
    /// `--publisher-port` pinned to that channel's derived port.
    fn sports_row_narrowed_to_channel(channel: u8) -> Feed {
        let feed = sports_row();
        let kept: Vec<crate::ingest::feeds::FeedPublisher> = feed
            .publishers
            .iter()
            .filter(|p| p.channel == Some(channel))
            .copied()
            .collect();
        assert_eq!(
            kept.len(),
            1,
            "channel {channel} must be in the sports roster exactly once"
        );
        Feed {
            publishers: Box::leak(kept.into_boxed_slice()),
            ..feed
        }
    }

    async fn spawn(filter: Arc<Mutex<ChannelFilter>>, enabled: Vec<Feed>) -> String {
        spawn_state(Arc::new(AdminState::new(AdminConfig {
            filter,
            enabled,
            diagnostics: Default::default(),
            binds: Binds::default(),
        })))
        .await
    }

    async fn spawn_state(state: Arc<AdminState>) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let _ = serve_state(listener, state).await;
        });
        format!("http://{addr}")
    }

    /// The default (empty) channel filter admits every publisher of every enabled feed, and `GET`
    /// reports exactly that — the state every deployment that never touches this surface is in.
    #[tokio::test]
    async fn get_reports_the_filter_in_force_and_what_it_admits() {
        let filter = Arc::new(Mutex::new(ChannelFilter::default()));
        let feed = sports_row();
        let base = spawn(filter, vec![feed]).await;

        let resp = reqwest::get(format!("{base}/admin/channels"))
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
        let body: Value = resp.json().await.unwrap();
        assert_eq!(
            body["summary"].as_array().unwrap().len(),
            0,
            "empty channel filter narrows nothing"
        );
        let admitted = body["rows"][0]["allowed"].as_array().unwrap();
        assert_eq!(
            admitted.len(),
            feed.publishers.len(),
            "an unnarrowed feed admits every publisher"
        );
        assert!(
            body["note"].as_str().unwrap().contains("subscription"),
            "the response must say this reflects admission, not running receivers: {body}"
        );
    }

    /// A valid `POST` replaces the shared channel filter in place — the same instance the
    /// reconciler reads — and a subsequent `GET` reflects it immediately, without needing a
    /// reconcile tick to observe the change (the tick is what applies it to the running receiver
    /// set, not what stores it).
    #[tokio::test]
    async fn post_replaces_the_shared_filter() {
        let filter = Arc::new(Mutex::new(ChannelFilter::default()));
        let feed = sports_row();
        let base = spawn(filter.clone(), vec![feed]).await;

        let resp = reqwest::Client::new()
            .post(format!("{base}/admin/channels"))
            .header(REQUIRED_HEADER, "1")
            .query(&[("channels", "edge-kalshi-sports-mbp=10")])
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);

        let narrowed = crate::model::lock(&filter).clone();
        assert!(narrowed.admits("edge-kalshi-sports-mbp", 10));
        assert!(
            !narrowed.admits("edge-kalshi-sports-mbp", 11),
            "the POST must narrow the live channel filter"
        );

        let resp = reqwest::get(format!("{base}/admin/channels"))
            .await
            .unwrap();
        let body: Value = resp.json().await.unwrap();
        let admitted = body["rows"][0]["allowed"].as_array().unwrap();
        assert_eq!(
            admitted.len(),
            1,
            "GET must reflect the just-applied narrowing"
        );
        assert_eq!(admitted[0]["channel"], 10);
    }

    /// The CSRF fix this module exists for: a `POST` with no [`REQUIRED_HEADER`] — exactly what a
    /// browser `<form>` submit can produce — is refused, and critically the **prior filter stays in
    /// force**. Asserting only the status code would pass even if the rejected request had already
    /// mutated the filter before answering `403`, so this checks the filter directly.
    #[tokio::test]
    async fn a_post_without_the_required_header_is_refused_and_the_prior_filter_stays_in_force() {
        let filter = Arc::new(Mutex::new(ChannelFilter::default()));
        let feed = sports_row();
        let base = spawn(filter.clone(), vec![feed]).await;

        // Establish a non-empty starting channel filter, with the header present.
        let setup = reqwest::Client::new()
            .post(format!("{base}/admin/channels"))
            .header(REQUIRED_HEADER, "1")
            .query(&[("channels", "edge-kalshi-sports-mbp=10")])
            .send()
            .await
            .unwrap();
        assert_eq!(
            setup.status(),
            200,
            "fixture sanity: the setup POST must apply"
        );
        let before = crate::model::lock(&filter).clone();
        assert!(
            before.admits("edge-kalshi-sports-mbp", 10)
                && !before.admits("edge-kalshi-sports-mbp", 11)
        );

        // The attack this module defends against: a POST with a `channels` query parameter but no
        // header at all — precisely what a browser `<form action=... method=POST>` produces.
        let resp = reqwest::Client::new()
            .post(format!("{base}/admin/channels"))
            .query(&[("channels", "edge-kalshi-sports-mbp=11")])
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 403);
        let body: Value = resp.json().await.unwrap();
        assert_eq!(body["error"], "missing_admin_header");

        let after = crate::model::lock(&filter).clone();
        assert_eq!(
            after.summary(),
            before.summary(),
            "a POST missing the required header must leave the prior channel filter untouched"
        );
        assert!(
            after.admits("edge-kalshi-sports-mbp", 10) && !after.admits("edge-kalshi-sports-mbp", 11),
            "the filter must still be the one the setup POST applied, not the attempted `edge-kalshi-sports-mbp=11`"
        );
    }

    /// The header alone (no secret value needed, see the module docs) is sufficient for a `POST` to
    /// succeed — the mirror image of the refusal test above.
    #[tokio::test]
    async fn a_post_with_the_required_header_present_succeeds() {
        let filter = Arc::new(Mutex::new(ChannelFilter::default()));
        let feed = sports_row();
        let base = spawn(filter.clone(), vec![feed]).await;

        let resp = reqwest::Client::new()
            .post(format!("{base}/admin/channels"))
            .header(REQUIRED_HEADER, "anything-at-all")
            .query(&[("channels", "edge-kalshi-sports-mbp=10")])
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
        assert!(crate::model::lock(&filter).admits("edge-kalshi-sports-mbp", 10));
    }

    /// An invalid spec is a `400` and the channel filter is left exactly as it was — never a
    /// partial apply, and never a silent reset to default. Starts from a **non-empty** channel
    /// filter (a prior valid `POST`) so an implementation that resets to empty on error cannot pass
    /// by coincidence — the failure mode I4 exists to catch. An id outside the feed's roster is one
    /// of `ChannelFilter::parse`'s refusals; reusing it here (rather than a second, laxer check) is
    /// what this test actually pins.
    #[tokio::test]
    async fn an_invalid_spec_is_rejected_and_changes_nothing() {
        let filter = Arc::new(Mutex::new(ChannelFilter::default()));
        let feed = sports_row();
        let base = spawn(filter.clone(), vec![feed]).await;

        // Establish a non-empty starting channel filter.
        let setup = reqwest::Client::new()
            .post(format!("{base}/admin/channels"))
            .header(REQUIRED_HEADER, "1")
            .query(&[("channels", "edge-kalshi-sports-mbp=10,11")])
            .send()
            .await
            .unwrap();
        assert_eq!(
            setup.status(),
            200,
            "fixture sanity: the setup POST must apply"
        );
        let before = crate::model::lock(&filter).clone();
        assert!(
            before.admits("edge-kalshi-sports-mbp", 10)
                && before.admits("edge-kalshi-sports-mbp", 11)
        );
        assert!(!before.admits("edge-kalshi-sports-mbp", 12));

        let resp = reqwest::Client::new()
            .post(format!("{base}/admin/channels"))
            .header(REQUIRED_HEADER, "1")
            .query(&[("channels", "edge-kalshi-sports-mbp=250")]) // 250 is well outside the 31-channel roster
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 400);
        let body: Value = resp.json().await.unwrap();
        assert_eq!(body["error"], "invalid_channel_filter");

        let after = crate::model::lock(&filter).clone();
        assert_eq!(
            after.summary(),
            before.summary(),
            "a rejected spec must leave the prior non-empty channel filter exactly as it was"
        );
        assert!(
            after.admits("edge-kalshi-sports-mbp", 10)
                && after.admits("edge-kalshi-sports-mbp", 11)
        );
        assert!(!after.admits("edge-kalshi-sports-mbp", 12));
    }

    /// Findings 3/4: a channel filter that is individually valid against the whole registry (11 is
    /// a real sports channel) but, crossed with an `enabled` set already narrowed to a single
    /// publisher (channel 10 only — the `--publisher-port` case), admits **zero** publishers of that
    /// feed. Must be a `400`, and must leave the prior (non-empty, so a reset-to-default can't pass
    /// by coincidence) channel filter exactly as it was — not merely refused, but the filter this
    /// process runs with must still be the one from before the POST.
    #[tokio::test]
    async fn a_post_that_would_empty_an_enabled_feed_is_refused_and_changes_nothing() {
        let filter = Arc::new(Mutex::new(ChannelFilter::default()));
        let narrowed = sports_row_narrowed_to_channel(10);
        let base = spawn(filter.clone(), vec![narrowed]).await;

        // Establish a non-empty starting channel filter that DOES admit the surviving publisher.
        let setup = reqwest::Client::new()
            .post(format!("{base}/admin/channels"))
            .header(REQUIRED_HEADER, "1")
            .query(&[("channels", "edge-kalshi-sports-mbp=10")])
            .send()
            .await
            .unwrap();
        assert_eq!(
            setup.status(),
            200,
            "fixture sanity: the setup POST must apply"
        );
        let before = crate::model::lock(&filter).clone();

        let resp = reqwest::Client::new()
            .post(format!("{base}/admin/channels"))
            .header(REQUIRED_HEADER, "1")
            .query(&[("channels", "edge-kalshi-sports-mbp=11")])
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 400);
        let body: Value = resp.json().await.unwrap();
        assert_eq!(body["error"], "channel_filter_empties_a_feed");

        let after = crate::model::lock(&filter).clone();
        assert_eq!(
            after.summary(),
            before.summary(),
            "a filter that would empty an enabled feed must leave the prior filter in force"
        );
        assert!(
            after.admits("edge-kalshi-sports-mbp", 10),
            "the prior filter's admission must be unchanged"
        );
    }

    /// I2: an absent `channels` parameter is a `400`, distinct from one present-and-empty (which is
    /// how an operator explicitly clears the channel filter). The natural client shape for a `POST`
    /// is a body — this must not be read as "clear everything."
    #[tokio::test]
    async fn a_post_missing_the_channels_parameter_is_rejected_and_changes_nothing() {
        let filter = Arc::new(Mutex::new(ChannelFilter::default()));
        let feed = sports_row();
        let base = spawn(filter.clone(), vec![feed]).await;

        // Establish a non-empty starting channel filter, so a silent "clear" would be observable.
        reqwest::Client::new()
            .post(format!("{base}/admin/channels"))
            .header(REQUIRED_HEADER, "1")
            .query(&[("channels", "edge-kalshi-sports-mbp=10")])
            .send()
            .await
            .unwrap();
        let before = crate::model::lock(&filter).clone();
        assert!(!before.is_empty(), "fixture sanity");

        let resp = reqwest::Client::new()
            .post(format!("{base}/admin/channels"))
            .header(REQUIRED_HEADER, "1")
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 400);
        let body: Value = resp.json().await.unwrap();
        assert_eq!(body["error"], "missing_channels_parameter");

        let after = crate::model::lock(&filter).clone();
        assert_eq!(
            after.summary(),
            before.summary(),
            "a request naming no `channels` parameter must not silently clear the channel filter"
        );
    }

    /// I2: a non-empty request body is refused with a 400 naming the query-parameter form, rather
    /// than being silently ignored while an absent `channels` parameter widens the channel filter.
    #[tokio::test]
    async fn a_post_with_a_body_is_rejected_and_changes_nothing() {
        let filter = Arc::new(Mutex::new(ChannelFilter::default()));
        let feed = sports_row();
        let base = spawn(filter.clone(), vec![feed]).await;

        reqwest::Client::new()
            .post(format!("{base}/admin/channels"))
            .header(REQUIRED_HEADER, "1")
            .query(&[("channels", "edge-kalshi-sports-mbp=10")])
            .send()
            .await
            .unwrap();
        let before = crate::model::lock(&filter).clone();
        assert!(!before.is_empty(), "fixture sanity");

        let resp = reqwest::Client::new()
            .post(format!("{base}/admin/channels"))
            .header(REQUIRED_HEADER, "1")
            .body("edge-kalshi-sports-mbp=11")
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 400);
        let body: Value = resp.json().await.unwrap();
        assert_eq!(body["error"], "unsupported_request_body");
        assert!(
            body["remediation"]
                .as_str()
                .unwrap()
                .contains("query parameter"),
            "the remedy must name the query-parameter form: {body}"
        );

        let after = crate::model::lock(&filter).clone();
        assert_eq!(
            after.summary(),
            before.summary(),
            "a body-bearing POST must not silently apply its body as the new channel filter"
        );
    }

    // ---------------------------------------------------------------------------------------------
    // GET /admin/diagnostics + POST /admin/{connect,disconnect}
    // ---------------------------------------------------------------------------------------------

    /// The route this whole change exists for: it must answer on a process with **nothing
    /// subscribed** — the exact state in which `/v1` is not listening and every other command
    /// fails with a transport error — and it must carry a verdict, not just raw state.
    #[tokio::test]
    async fn diagnostics_answers_with_a_verdict_when_nothing_is_subscribed() {
        let diagnostics: SharedDiagnostics = Default::default();
        {
            let mut d = crate::model::lock(&diagnostics);
            d.refresh_secs = 30;
            d.polled.detection = crate::ingest::diagnostics::Detection::Ok;
            d.polled.sessions = crate::ingest::subscriptions::parse_status_sessions(
                crate::ingest::subscriptions::DISCONNECTED_STATUS_JSON.as_bytes(),
            );
        }
        let base = spawn_state(Arc::new(AdminState::new(AdminConfig {
            filter: Arc::new(Mutex::new(ChannelFilter::default())),
            enabled: vec![],
            diagnostics,
            binds: Binds {
                api: "127.0.0.1:9099".to_string(),
                ..Binds::default()
            },
        })))
        .await;

        let body: Value = reqwest::get(format!("{base}/admin/diagnostics"))
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(body["diagnosis"]["code"], "tunnel_down");
        assert_eq!(
            body["tunnel"]["sessions"][0]["session_status"],
            "disconnected"
        );
        assert_eq!(
            body["binds"]["api"], "127.0.0.1:9099",
            "the configured binds must be reported — 'is it even enabled' is half the question"
        );
        assert!(
            !body["registry"].is_null(),
            "which registry document resolved is otherwise only visible in the startup log: {body}"
        );
    }

    /// `GET /admin/diagnostics` is read-only and must **not** require the CSRF header: that header
    /// guards mutations, and a stuck operator must be able to `curl` the diagnosis plainly.
    #[tokio::test]
    async fn diagnostics_needs_no_admin_header() {
        let base = spawn(Arc::new(Mutex::new(ChannelFilter::default())), vec![]).await;
        let resp = reqwest::get(format!("{base}/admin/diagnostics"))
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
    }

    /// M7: covers the same breadth `sinks::api`'s method-refusal pinning does (`PUT`/`PATCH`/
    /// `DELETE`) rather than one method alone. `POST` is deliberately excluded from this set — it is
    /// the one method this surface legitimately accepts.
    #[tokio::test]
    async fn every_unsupported_method_is_refused() {
        let filter = Arc::new(Mutex::new(ChannelFilter::default()));
        let base = spawn(filter, vec![]).await;
        let client = reqwest::Client::new();

        for method in [
            reqwest::Method::PUT,
            reqwest::Method::PATCH,
            reqwest::Method::DELETE,
        ] {
            let resp = client
                .request(method.clone(), format!("{base}/admin/channels"))
                .send()
                .await
                .unwrap();
            assert_eq!(
                resp.status(),
                405,
                "{method} was not refused by /admin/channels"
            );
        }
    }
}
