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
//! Four routes. `GET /admin/diagnostics` is why the other three now matter to a first-time
//! operator: this surface is **not** subscription-gated, so on a host whose tunnel never came up —
//! where `/v1` is not listening and `doublezero-edge` can only report a transport error — it is the
//! one thing that answers, and it answers with a verdict ([`crate::ingest::diagnostics`]) rather
//! than raw state. `POST /admin/connect` and `POST /admin/disconnect` are the retry that verdict
//! points at, re-running the DoubleZero client verb an operator would otherwise reach through
//! `docker exec` (root, and the container name). See [`post_command`] for what contains them.
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
//! ([`mutation_guards`]). It matters most on `/admin/connect`, where a form post would provision an
//! onchain user. `GET` routes are exempt: the header guards mutations, and requiring it on the
//! diagnostics read would make the one command a stuck operator most needs harder to run than
//! `curl`.
//! The header only needs to be **present**, not carry a shared-secret value: this surface documents
//! itself as having no authentication (see above), and a value would imply exactly that, plus the
//! provisioning/rotation problem that comes with it. What the header actually rules out is not "an
//! unauthorized caller" but "a request a browser page could have caused by accident" — a
//! `curl -H 'x-dz-admin-request: 1'` from anyone reaching loopback still succeeds, same as today.
//!
//! That header alone does **not** cover DNS rebinding, which is a different attack: a page served
//! from a name whose record is re-pointed at `127.0.0.1` becomes *same-origin* with this surface,
//! so it can set any header it likes and read the response too. Tearing down the tunnel or spending
//! the container's onchain identity is a much worse outcome than swapping a channel filter, so a
//! `POST` also requires `Host` to name an address rather than a name ([`host_is_an_address`]) — a
//! rebound request necessarily carries the attacker's name there.

use std::sync::{Arc, Mutex};

use anyhow::Result;
use serde_json::{json, Value};
use tokio::net::TcpListener;
use tracing::{info, warn};

use super::http::{self, Request, Response};
use crate::ingest::{
    channel_filter::ChannelFilter,
    diagnostics::{diagnose, CommandAttempt, SharedDiagnostics},
    feeds::Feed,
    subscriptions,
};

/// Small, operator-only surface: no need for the concurrency `sinks::api`/`sinks::metrics` allow.
const MAX_CONNS: usize = 8;

/// The two fixed argv this surface may run. **Constants, never composed from request input** — the
/// endpoints take no parameters at all, so nothing a caller sends can influence what is executed.
/// `connect multicast` is verbatim what `scripts/connect.sh` runs; `disconnect multicast` is its
/// scoped counterpart (a bare `doublezero disconnect` would also delete an unrelated IBRL user).
const CONNECT_ARGV: &[&str] = &["connect", "multicast"];
const DISCONNECT_ARGV: &[&str] = &["disconnect", "multicast"];

/// How a `POST /admin/{connect,disconnect}` actually runs the client. A field rather than a direct
/// call so a handler test can drive the single-flight and guard behaviour without a `doublezero`
/// binary on the test host — there is exactly one production implementation
/// ([`subscriptions::run_cli_reporting`]), installed by [`AdminState::new`].
type CommandRunner = Arc<dyn Fn(&'static [&'static str]) -> (Option<i32>, String) + Send + Sync>;

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
    runner: CommandRunner,
}

impl AdminState {
    fn new(cfg: AdminConfig) -> Self {
        Self {
            filter: cfg.filter,
            enabled: cfg.enabled,
            diagnostics: cfg.diagnostics,
            binds: cfg.binds,
            runner: Arc::new(subscriptions::run_cli_reporting),
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

/// [`serve`] over an already-built state — the seam a handler test uses to install a stub
/// [`CommandRunner`] instead of really shelling out to `doublezero`.
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
        ("POST", "/admin/connect") => post_command(state, req, "/admin/connect", CONNECT_ARGV),
        (method, "/admin/connect") => method_not_allowed(method, "/admin/connect"),
        ("POST", "/admin/disconnect") => {
            post_command(state, req, "/admin/disconnect", DISCONNECT_ARGV)
        }
        (method, "/admin/disconnect") => method_not_allowed(method, "/admin/disconnect"),
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

/// `POST /admin/connect` and `POST /admin/disconnect` — re-run the DoubleZero client verb an
/// operator would otherwise reach through `docker exec`, which needs root and the container name.
/// This is the retry path for the `tunnel_down` verdict [`get_diagnostics`] reports.
///
/// Four things contain what is genuinely a capability increase over a channel-filter swap (this
/// spends the container's onchain identity):
/// - **A fixed argv.** `argv` is one of two module constants. The endpoints accept no parameters,
///   so no request input reaches the child process — this is not a command runner.
/// - **The loopback default bind** the whole surface is documented under.
/// - **[`REQUIRED_HEADER`]**, the same CSRF guard `POST /admin/channels` carries: a browser
///   `<form>` on this host cannot set it, and a form post that provisioned an onchain user would
///   be a genuinely bad outcome.
/// - **Single-flight.** A second attempt while one is running is `409`, not a queued second run:
///   two concurrent `connect`s race onchain user creation. This is a correctness guard.
///
/// The child runs on a blocking thread behind `tokio::spawn`, and the handler answers `202`
/// immediately: `connect` probes device latency, creates an onchain user and polls for activation,
/// which takes minutes and must never sit on a runtime worker (nor on this surface's small
/// connection budget). The attempt's outcome lands back in the diagnostics snapshot, so
/// `GET /admin/diagnostics` reports what happened — a `202` with no follow-up would be useless.
fn post_command(
    state: &Arc<AdminState>,
    req: &Request,
    route: &str,
    argv: &'static [&'static str],
) -> Response {
    if let Some(refusal) = mutation_guards(req, route) {
        return refusal;
    }
    let command = format!("doublezero {}", argv.join(" "));
    {
        let mut diag = crate::model::lock(&state.diagnostics);
        if diag.attempt_running() {
            let running = diag
                .last_attempt
                .as_ref()
                .map(|a| a.command.clone())
                .unwrap_or_default();
            return json_status(
                "409 Conflict",
                json!({
                    "error": "attempt_already_running",
                    "message": format!("`{running}` is still running."),
                    "remediation": "Wait for it to finish — GET /admin/diagnostics reports the \
                        attempt's exit code and output once it does. Two concurrent runs would \
                        race onchain user creation.",
                }),
            );
        }
        diag.last_attempt = Some(CommandAttempt {
            command: command.clone(),
            started_at_unix: crate::model::now_ns() / 1_000_000_000,
            finished_at_unix: None,
            exit_code: None,
            output_tail: String::new(),
        });
    }

    info!(%command, "running DoubleZero client verb via the admin surface");
    let state = state.clone();
    tokio::spawn(async move {
        let runner = state.runner.clone();
        let result = tokio::task::spawn_blocking(move || runner(argv)).await;
        let (exit_code, output) = match result {
            Ok(v) => v,
            Err(e) => (None, format!("the attempt task failed: {e}")),
        };
        if exit_code != Some(0) {
            warn!(?exit_code, output = %output, "DoubleZero client verb did not succeed");
        }
        let mut diag = crate::model::lock(&state.diagnostics);
        if let Some(attempt) = diag.last_attempt.as_mut() {
            attempt.finished_at_unix = Some(crate::model::now_ns() / 1_000_000_000);
            attempt.exit_code = exit_code;
            attempt.output_tail = output;
        }
    });

    json_status(
        "202 Accepted",
        json!({
            "accepted": true,
            "command": command,
            "message": format!("`{command}` started."),
            "remediation": "It can take minutes. GET /admin/diagnostics reports its exit code and \
                output under `last_attempt`, and the verdict once the reconciler's next poll picks \
                up the new tunnel state.",
        }),
    )
}

/// The three refusals every mutating route on this surface shares — see [`post_channels`] and
/// [`host_is_an_address`] for why each exists. Returns `None` when the request may proceed.
fn mutation_guards(req: &Request, route: &str) -> Option<Response> {
    if !host_is_an_address(req) {
        return Some(json_status(
            "403 Forbidden",
            json!({
                "error": "host_header_not_an_address",
                "message": "Request carried a `Host` header naming a DNS name rather than an \
                    address.",
                "remediation": "Address this surface by IP and port (e.g. \
                    http://127.0.0.1:9098). A name is refused because a page in a browser on this \
                    host can point one at loopback, which would make its requests same-origin.",
            }),
        ));
    }
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

/// Does the request's `Host` name an address rather than a DNS name? Absent counts as yes — a
/// hand-rolled `curl`/HTTP/1.0 client may omit it, and there is nothing to rebind. Literal
/// `localhost` counts too: an attacker cannot make their page's origin be `localhost`, and
/// refusing it would break `--admin-url http://localhost:9098` for no gain.
///
/// [`REQUIRED_HEADER`] alone stops a **cross-origin** form post, which is the only shape a page can
/// produce against `http://127.0.0.1:9098` directly. It does **not** stop DNS rebinding: a page
/// served from a name whose record is then re-pointed at `127.0.0.1` becomes *same-origin* with
/// this surface, so it can set any header it likes with no preflight and both mutate and read
/// freely. That escalated from "swap a channel filter" to "tear down the tunnel, or spend the
/// container's onchain identity" when the connect routes landed, which is why the check is here.
/// A rebound request necessarily carries the attacker's **name** in `Host`; a legitimate client
/// addressing a loopback bind carries an address.
fn host_is_an_address(req: &Request) -> bool {
    let Some(host) = req.header("host") else {
        return true;
    };
    let host = host.trim();
    // An IPv6 literal is bracketed, so the last `:` only separates a port outside brackets.
    let name = match host.strip_prefix('[') {
        Some(rest) => return rest.split(']').next().is_some_and(host_is_local_literal),
        None => host.rsplit_once(':').map_or(host, |(h, _)| h),
    };
    host_is_local_literal(name)
}

fn host_is_local_literal(s: &str) -> bool {
    s.eq_ignore_ascii_case("localhost") || s.parse::<std::net::IpAddr>().is_ok()
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
///   which is exactly how `curl -XPOST -d 'lashay-4=10'` or `requests.post(url, data=...)` would
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
                    POST /admin/channels?channels=lashay-4=10,11. An empty value \
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

    /// The real built-in "sports" feed (group code `lashay-4`) — a genuinely derived,
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

    /// A state whose [`CommandRunner`] is a stub: it blocks until a token arrives on `release`,
    /// then reports `exit_code` and the argv it was handed. No test in this module ever spawns a
    /// real `doublezero` process — the point of the seam.
    fn stub_state(
        diagnostics: SharedDiagnostics,
        release: std::sync::mpsc::Receiver<()>,
        exit_code: i32,
    ) -> Arc<AdminState> {
        let release = Mutex::new(release);
        Arc::new(AdminState {
            filter: Arc::new(Mutex::new(ChannelFilter::default())),
            enabled: vec![],
            diagnostics,
            binds: Binds::default(),
            runner: Arc::new(move |args| {
                let _ = crate::model::lock(&release).recv();
                (Some(exit_code), format!("ran {}", args.join(" ")))
            }),
        })
    }

    /// Poll `f` until it holds or the deadline passes — the attempt's completion lands on a
    /// background task, so a bare assertion right after the `202` would race it.
    async fn eventually(mut f: impl FnMut() -> bool) -> bool {
        for _ in 0..200 {
            if f() {
                return true;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        false
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
            .query(&[("channels", "lashay-4=10")])
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);

        let narrowed = crate::model::lock(&filter).clone();
        assert!(narrowed.admits("lashay-4", 10));
        assert!(
            !narrowed.admits("lashay-4", 11),
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
            .query(&[("channels", "lashay-4=10")])
            .send()
            .await
            .unwrap();
        assert_eq!(
            setup.status(),
            200,
            "fixture sanity: the setup POST must apply"
        );
        let before = crate::model::lock(&filter).clone();
        assert!(before.admits("lashay-4", 10) && !before.admits("lashay-4", 11));

        // The attack this module defends against: a POST with a `channels` query parameter but no
        // header at all — precisely what a browser `<form action=... method=POST>` produces.
        let resp = reqwest::Client::new()
            .post(format!("{base}/admin/channels"))
            .query(&[("channels", "lashay-4=11")])
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
            after.admits("lashay-4", 10) && !after.admits("lashay-4", 11),
            "the filter must still be the one the setup POST applied, not the attempted `lashay-4=11`"
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
            .query(&[("channels", "lashay-4=10")])
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
        assert!(crate::model::lock(&filter).admits("lashay-4", 10));
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
            .query(&[("channels", "lashay-4=10,11")])
            .send()
            .await
            .unwrap();
        assert_eq!(
            setup.status(),
            200,
            "fixture sanity: the setup POST must apply"
        );
        let before = crate::model::lock(&filter).clone();
        assert!(before.admits("lashay-4", 10) && before.admits("lashay-4", 11));
        assert!(!before.admits("lashay-4", 12));

        let resp = reqwest::Client::new()
            .post(format!("{base}/admin/channels"))
            .header(REQUIRED_HEADER, "1")
            .query(&[("channels", "lashay-4=250")]) // 250 is well outside the 31-channel roster
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
        assert!(after.admits("lashay-4", 10) && after.admits("lashay-4", 11));
        assert!(!after.admits("lashay-4", 12));
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
            .query(&[("channels", "lashay-4=10")])
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
            .query(&[("channels", "lashay-4=11")])
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
            after.admits("lashay-4", 10),
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
            .query(&[("channels", "lashay-4=10")])
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
            .query(&[("channels", "lashay-4=10")])
            .send()
            .await
            .unwrap();
        let before = crate::model::lock(&filter).clone();
        assert!(!before.is_empty(), "fixture sanity");

        let resp = reqwest::Client::new()
            .post(format!("{base}/admin/channels"))
            .header(REQUIRED_HEADER, "1")
            .body("lashay-4=11")
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

    /// A `connect` runs the fixed argv, answers `202` immediately (it can take minutes), and the
    /// outcome lands back in the snapshot — a `202` nothing could follow up on would be useless.
    #[tokio::test]
    async fn connect_runs_the_fixed_argv_and_records_its_outcome() {
        let diagnostics: SharedDiagnostics = Default::default();
        let (tx, rx) = std::sync::mpsc::channel();
        let base = spawn_state(stub_state(diagnostics.clone(), rx, 0)).await;

        let resp = reqwest::Client::new()
            .post(format!("{base}/admin/connect"))
            .header(REQUIRED_HEADER, "1")
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 202);
        let body: Value = resp.json().await.unwrap();
        assert_eq!(body["command"], "doublezero connect multicast");

        tx.send(()).unwrap();
        let d = diagnostics.clone();
        assert!(
            eventually(|| crate::model::lock(&d)
                .last_attempt
                .as_ref()
                .is_some_and(|a| !a.running()))
            .await,
            "the attempt must be recorded as finished"
        );
        let attempt = crate::model::lock(&diagnostics)
            .last_attempt
            .clone()
            .unwrap();
        assert_eq!(attempt.exit_code, Some(0));
        assert_eq!(
            attempt.output_tail, "ran connect multicast",
            "the argv is a module constant; no request input reaches the child process"
        );
    }

    /// `disconnect` is the same shape with the scoped counterpart argv — a bare `doublezero
    /// disconnect` would also delete an unrelated IBRL user on the same host.
    #[tokio::test]
    async fn disconnect_runs_the_scoped_argv() {
        let diagnostics: SharedDiagnostics = Default::default();
        let (tx, rx) = std::sync::mpsc::channel();
        let base = spawn_state(stub_state(diagnostics.clone(), rx, 0)).await;
        tx.send(()).unwrap();

        let resp = reqwest::Client::new()
            .post(format!("{base}/admin/disconnect"))
            .header(REQUIRED_HEADER, "1")
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 202);
        let d = diagnostics.clone();
        assert!(
            eventually(|| crate::model::lock(&d)
                .last_attempt
                .as_ref()
                .is_some_and(|a| !a.running()))
            .await
        );
        assert_eq!(
            crate::model::lock(&diagnostics)
                .last_attempt
                .as_ref()
                .unwrap()
                .output_tail,
            "ran disconnect multicast"
        );
    }

    /// Single-flight: two concurrent runs would race onchain user creation, so the second is a
    /// `409` rather than a queued second attempt. The stub blocks until released, which is what
    /// makes "still running" a real state rather than a timing accident.
    #[tokio::test]
    async fn a_second_attempt_while_one_is_running_is_refused() {
        let diagnostics: SharedDiagnostics = Default::default();
        let (tx, rx) = std::sync::mpsc::channel();
        let base = spawn_state(stub_state(diagnostics.clone(), rx, 0)).await;
        let client = reqwest::Client::new();

        let first = client
            .post(format!("{base}/admin/connect"))
            .header(REQUIRED_HEADER, "1")
            .send()
            .await
            .unwrap();
        assert_eq!(first.status(), 202);

        // The first attempt is blocked in the stub, so this one lands mid-flight.
        let second = client
            .post(format!("{base}/admin/disconnect"))
            .header(REQUIRED_HEADER, "1")
            .send()
            .await
            .unwrap();
        assert_eq!(second.status(), 409);
        let body: Value = second.json().await.unwrap();
        assert_eq!(body["error"], "attempt_already_running");

        tx.send(()).unwrap();
        let d = diagnostics.clone();
        assert!(eventually(|| !crate::model::lock(&d).attempt_running()).await);
        assert_eq!(
            crate::model::lock(&diagnostics)
                .last_attempt
                .as_ref()
                .unwrap()
                .command,
            "doublezero connect multicast",
            "the refused second request must not have replaced the running attempt"
        );
    }

    /// The CSRF guard and the body refusal apply to `connect`/`disconnect` exactly as they do to
    /// `channels` — and here the stakes are higher: a browser form post that provisioned an
    /// onchain user is the outcome this rules out. Both must refuse **before** anything is spawned,
    /// which the untouched snapshot pins.
    #[tokio::test]
    async fn a_connect_without_the_header_or_with_a_body_is_refused_and_starts_nothing() {
        let diagnostics: SharedDiagnostics = Default::default();
        let (_tx, rx) = std::sync::mpsc::channel();
        let base = spawn_state(stub_state(diagnostics.clone(), rx, 0)).await;
        let client = reqwest::Client::new();

        let no_header = client
            .post(format!("{base}/admin/connect"))
            .send()
            .await
            .unwrap();
        assert_eq!(no_header.status(), 403);
        assert_eq!(
            no_header.json::<Value>().await.unwrap()["error"],
            "missing_admin_header"
        );

        let with_body = client
            .post(format!("{base}/admin/connect"))
            .header(REQUIRED_HEADER, "1")
            .body("device=whatever")
            .send()
            .await
            .unwrap();
        assert_eq!(with_body.status(), 400);
        assert_eq!(
            with_body.json::<Value>().await.unwrap()["error"],
            "unsupported_request_body"
        );

        assert!(
            crate::model::lock(&diagnostics).last_attempt.is_none(),
            "a refused request must not have started an attempt"
        );
    }

    /// The DNS-rebinding guard: a `Host` naming a DNS name is refused on a mutating route, because
    /// a page whose own name was re-pointed at loopback is *same-origin* with this surface and can
    /// therefore set [`REQUIRED_HEADER`] itself. Asserts the filter untouched too — the refusal
    /// must land before anything applies.
    #[tokio::test]
    async fn a_post_whose_host_names_a_dns_name_is_refused_and_changes_nothing() {
        let filter = Arc::new(Mutex::new(ChannelFilter::default()));
        let feed = sports_row();
        let base = spawn(filter.clone(), vec![feed]).await;

        let resp = reqwest::Client::new()
            .post(format!("{base}/admin/channels"))
            .header(REQUIRED_HEADER, "1")
            .header(reqwest::header::HOST, "evil.example")
            .query(&[("channels", "lashay-4=10")])
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 403);
        assert_eq!(
            resp.json::<Value>().await.unwrap()["error"],
            "host_header_not_an_address"
        );
        assert!(
            crate::model::lock(&filter).is_empty(),
            "a rebound request must not have applied its spec"
        );
    }

    /// The same guard on `/admin/connect`, where the outcome it rules out is a provisioning run —
    /// and the mirror image: an address-shaped `Host` (what every real client sends against a
    /// loopback bind) is accepted.
    #[tokio::test]
    async fn a_connect_is_refused_by_host_but_accepted_by_address() {
        let diagnostics: SharedDiagnostics = Default::default();
        let (tx, rx) = std::sync::mpsc::channel();
        let base = spawn_state(stub_state(diagnostics.clone(), rx, 0)).await;
        tx.send(()).unwrap();
        let client = reqwest::Client::new();

        let rebound = client
            .post(format!("{base}/admin/connect"))
            .header(REQUIRED_HEADER, "1")
            .header(reqwest::header::HOST, "attacker.test")
            .send()
            .await
            .unwrap();
        assert_eq!(rebound.status(), 403);
        assert!(
            crate::model::lock(&diagnostics).last_attempt.is_none(),
            "a rebound request must not have started a provisioning run"
        );

        // reqwest derives Host from the URL, which is `127.0.0.1:<port>` here.
        let ok = client
            .post(format!("{base}/admin/connect"))
            .header(REQUIRED_HEADER, "1")
            .send()
            .await
            .unwrap();
        assert_eq!(ok.status(), 202);
    }

    #[test]
    fn host_shapes_a_loopback_client_really_sends_are_accepted() {
        for host in [
            "127.0.0.1:9098",
            "127.0.0.1",
            "[::1]:9098",
            "localhost:9098",
        ] {
            let req = Request {
                method: "POST".into(),
                path: "/admin/connect".into(),
                params: vec![],
                content_length: 0,
                headers: vec![("Host".into(), host.into())],
            };
            assert!(host_is_an_address(&req), "{host} must be accepted");
        }
        for host in ["evil.example", "evil.example:9098", "sub.evil.example"] {
            let req = Request {
                method: "POST".into(),
                path: "/admin/connect".into(),
                params: vec![],
                content_length: 0,
                headers: vec![("Host".into(), host.into())],
            };
            assert!(!host_is_an_address(&req), "{host} must be refused");
        }
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
