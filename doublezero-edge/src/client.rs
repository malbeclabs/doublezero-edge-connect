//! The one place this crate talks to the network: a blocking GET against the edge-connect `/v1`
//! API, plus the percent-encoding a product id needs when it becomes a URL path segment.
//!
//! There is no concurrency requirement (one request per CLI invocation), so a blocking
//! `reqwest::blocking::Client` is used rather than pulling in an async runtime for its own sake.

use serde_json::{json, Value};

/// What a request against the API produced, already classified into the four shapes the rest of
/// this crate cares about. `status` on the response-bearing variants is the raw HTTP status
/// code — the exit-code mapping in `main.rs` is the only other place that needs it.
#[derive(Debug)]
pub enum Outcome {
    /// A `2xx` response with a JSON body.
    Ok { body: Value },
    /// A non-`2xx` response the server still answered — its body is already the
    /// `{"error","message","remediation"}` envelope `sinks/api.rs` promises.
    Failed { status: u16, body: Value },
    /// A response body that did not decode as JSON, whatever the status. This can never be the
    /// server's own error envelope (that's `Failed`), so a `2xx` here is not success either — it
    /// means something other than edge-connect answered (a proxy, an ingress banner, the wrong
    /// port). Every caller treats this the same as `Unreachable`: print the synthesized envelope
    /// and exit non-zero, never `emit` the body as if it were real data.
    Invalid { status: u16, body: Value },
    /// No response at all (connection refused, DNS failure, timeout, ...). Synthesized into the
    /// same envelope shape as a server-side error so a caller never has to special-case "the
    /// transport failed" versus "the server said no" — see [`unreachable_envelope`].
    Unreachable { body: Value },
}

/// Perform one `GET {url}{path}?{query}` and classify the result. `path` must already be a
/// complete, correctly-encoded URL path (see [`encode_path_segment`] for the one piece that needs
/// it); `query` is applied via `reqwest`'s own encoder, which handles arbitrary key/value bytes
/// correctly without this crate hand-rolling a second encoder for it.
pub fn get(
    client: &reqwest::blocking::Client,
    base_url: &str,
    path: &str,
    query: &[(String, String)],
) -> Outcome {
    let full = format!("{}{}", base_url.trim_end_matches('/'), path);
    let resp = match client.get(&full).query(query).send() {
        Ok(r) => r,
        Err(_e) => {
            return Outcome::Unreachable {
                body: unreachable_envelope(base_url),
            }
        }
    };
    let status = resp.status().as_u16();
    match resp.json::<Value>() {
        Ok(body) if (200..300).contains(&status) => Outcome::Ok { body },
        Ok(body) => Outcome::Failed { status, body },
        Err(_e) => Outcome::Invalid {
            status,
            body: json!({
                "error": "invalid_response",
                "message": format!("Response from {full} was not valid JSON."),
                "remediation": "Check --url points at an edge-connect instance's /v1 API, not something else on that port.",
            }),
        },
    }
}

/// The remediation-carrying envelope synthesized when there is no response to read at all — the
/// one case the server's own `{"error","message","remediation"}` shape can't produce for us,
/// because there is no server in the loop to produce it. Matches that shape exactly so downstream
/// code (rendering, exit-code mapping) never needs to know which side produced an error body.
///
/// This is the message for "nothing answered on either surface". When the admin surface *does*
/// answer, the container is up and this is the wrong story — see [`api_inactive_envelope`].
pub fn unreachable_envelope(base_url: &str) -> Value {
    json!({
        "error": "api_unreachable",
        "message": format!("No response at {base_url}."),
        "remediation": "Is edge-connect running? Check `docker ps`, or pass --url. A running \
            container can also answer nothing here: /v1 activates only once a market-data feed is \
            subscribed, re-checked every --subscription-refresh-secs (30s by default). Run \
            `doublezero-edge diagnose`, which reads the admin surface and is not gated that way.",
    })
}

/// `/v1` did not answer but the admin surface did: the container is running and its query API is
/// simply not activated. Distinct from [`unreachable_envelope`] because the remediations have
/// nothing in common — one is "start the container", the other is "fix the tunnel".
///
/// Names `admin_url` explicitly. [`same_host`] can only compare host strings, so a `--url` that
/// reaches a *remote* bridge through a local port forward looks local and the probe fires against
/// the local container. Saying which process answered is what lets an operator spot that.
pub fn api_inactive_envelope(base_url: &str, admin_url: &str, summary: &str) -> Value {
    json!({
        "error": "api_inactive",
        "message": format!(
            "No response at {base_url}, but edge-connect is running — the admin surface at \
             {admin_url} answered. The /v1 API activates only when a market-data feed is \
             subscribed. {summary}"
        ),
        "remediation": "Run `doublezero-edge diagnose` for the full verdict, and \
            `doublezero-edge connect` if it reports the tunnel down.",
    })
}

/// The admin-surface twin of [`unreachable_envelope`]. It exists as a separate message because a
/// connection-refused against the admin surface has a genuinely different likely cause: it is a
/// different bind from `/v1`, on by default at loopback, and the one way to turn it off is to set
/// `DZ_ADMIN_BIND` empty. Naming the env var here is what lets a caller tell that apart from a
/// wrong `--admin-url` without guessing.
pub fn admin_unreachable_envelope(admin_url: &str) -> Value {
    json!({
        "error": "admin_api_unreachable",
        "message": format!("No response at {admin_url}."),
        "remediation": "The admin surface is on by default at 127.0.0.1:9098 — a different bind \
            from the read-only /v1 API. It is off only if the container set DZ_ADMIN_BIND empty; \
            otherwise check --admin-url, and that this command runs somewhere the container's \
            loopback is reachable (the default bind never reaches past it).",
    })
}

/// Ask the admin surface for its diagnostics, returning the body only if it answered — the probe
/// behind the [`api_inactive_envelope`] distinction. A failure of any kind is `None`: this runs
/// only to improve another error's wording and must never produce an error of its own.
pub fn probe_diagnostics(client: &reqwest::blocking::Client, admin_url: &str) -> Option<Value> {
    match admin_get(client, admin_url, "/admin/diagnostics") {
        Outcome::Ok { body } => Some(body),
        _ => None,
    }
}

/// Do two URLs name the same host, ignoring scheme and port? The guard on that probe: against a
/// remote bridge with the default loopback `--admin-url`, probing would report the *local*
/// container's state as the remote one's. Two spellings of one host (`localhost` vs `127.0.0.1`)
/// compare unequal, which suppresses the probe — the safe direction.
pub fn same_host(a: &str, b: &str) -> bool {
    match (host_of(a), host_of(b)) {
        (Some(a), Some(b)) => a == b,
        _ => false,
    }
}

fn host_of(url: &str) -> Option<String> {
    let rest = url.split_once("://").map_or(url, |(_, r)| r);
    let authority = rest.split(['/', '?', '#']).next().unwrap_or("");
    let authority = authority.rsplit_once('@').map_or(authority, |(_, h)| h);
    let host = match authority.strip_prefix('[') {
        Some(v6) => v6.split(']').next().unwrap_or(""),
        None => authority.split(':').next().unwrap_or(""),
    };
    (!host.is_empty()).then(|| host.to_ascii_lowercase())
}

/// `GET {admin_url}{path}` — the admin-surface counterpart of [`get`], differing only in which
/// unreachable envelope it synthesizes (see [`admin_unreachable_envelope`]) and in taking no query
/// parameters (`GET /admin/channels` takes none).
pub fn admin_get(client: &reqwest::blocking::Client, admin_url: &str, path: &str) -> Outcome {
    let full = format!("{}{}", admin_url.trim_end_matches('/'), path);
    let resp = match client.get(&full).send() {
        Ok(r) => r,
        Err(_e) => {
            return Outcome::Unreachable {
                body: admin_unreachable_envelope(admin_url),
            }
        }
    };
    classify(resp)
}

/// `POST {admin_url}/admin/channels?channels={spec}` — the one mutation this crate ever performs.
///
/// Two things matter here, both forced by the server (`sinks::admin::post_channels`):
/// - The spec travels as a **query parameter**, not a request body — the server 400s a
///   non-zero `Content-Length` outright, specifically to catch a body-bearing client rather than
///   silently ignoring it. `reqwest`'s query serializer percent-encodes the value (`,`/`;`/`=` all
///   need it — the spec syntax is `<code>=<id>[,<id>...][;<code>=...]`), and issuing the request
///   via `.query(&[...])` with **no** `.body(...)` call sends no request body and no
///   `Content-Length` header at all — pinned by
///   `client::tests::a_channels_post_sends_no_body_and_percent_encodes_the_query`, which reads the
///   raw bytes off the wire rather than trusting `reqwest`'s behaviour by assumption.
/// - An empty `spec` is a legitimate "clear the channel filter" (still sent as `?channels=`), distinct from
///   omitting the parameter altogether (which the server 400s) — `reqwest` always includes a
///   `key=value` pair for `.query(&[(k, v)])` even when `v` is `""`, so that distinction is
///   preserved without this function doing anything special.
///
/// Also sets `X-DZ-Admin-Request` (any value — the server checks presence, not content): the
/// server refuses a `POST` without it as a CSRF defense, since a browser `<form>` post cannot add
/// an arbitrary header but this CLI, run deliberately by an operator, can (see
/// `doublezero-edge-connect::sinks::admin`'s module docs for why presence alone is the bar).
pub fn admin_post_channels(
    client: &reqwest::blocking::Client,
    admin_url: &str,
    spec: &str,
) -> Outcome {
    admin_post_with(client, admin_url, "/admin/channels", &[("channels", spec)])
}

/// The one POST this CLI's admin mutation runs through.
fn admin_post_with(
    client: &reqwest::blocking::Client,
    admin_url: &str,
    path: &str,
    query: &[(&str, &str)],
) -> Outcome {
    let full = format!("{}{}", admin_url.trim_end_matches('/'), path);
    let resp = match client
        .post(&full)
        .header("X-DZ-Admin-Request", "1")
        .query(query)
        .send()
    {
        Ok(r) => r,
        Err(_e) => {
            return Outcome::Unreachable {
                body: admin_unreachable_envelope(admin_url),
            }
        }
    };
    classify(resp)
}

/// Shared response classification for the admin-surface calls above — identical to the tail of
/// [`get`], factored out so both admin functions apply it the same way.
fn classify(resp: reqwest::blocking::Response) -> Outcome {
    let status = resp.status().as_u16();
    let full = resp.url().to_string();
    match resp.json::<Value>() {
        Ok(body) if (200..300).contains(&status) => Outcome::Ok { body },
        Ok(body) => Outcome::Failed { status, body },
        Err(_e) => Outcome::Invalid {
            status,
            body: json!({
                "error": "invalid_response",
                "message": format!("Response from {full} was not valid JSON."),
                "remediation": "Check --admin-url points at an edge-connect instance's admin surface.",
            }),
        },
    }
}

/// Percent-encode one path segment (e.g. a product id) for use directly after a `/` in a URL path.
/// Hand-rolled rather than pulled from a dependency: this crate already depends on `reqwest`
/// (which pulls in a URL encoder transitively), but reaching into its private internals isn't a
/// stable API to build on, and a product id is a handful of ASCII bytes — the encoding rule is
/// exactly "leave unreserved and sub-delim bytes alone, percent-encode everything else",
/// mirroring the decoder the server runs on the other end
/// (`doublezero-edge-connect::sinks::http::percent_decode`).
///
/// The byte that matters most here is `#`: a product id disambiguation suffix
/// (`SOURCE:SYMBOL#<channel>.<instrument_id>`) uses it, and left unescaped it would be read as a
/// URL fragment delimiter and never reach the server at all.
pub fn encode_path_segment(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        let unreserved_or_subdelim = matches!(
            b,
            b'A'..=b'Z'
                | b'a'..=b'z'
                | b'0'..=b'9'
                | b'-' | b'.' | b'_' | b'~'
                | b':' | b'@'
                | b'!' | b'$' | b'&' | b'\'' | b'(' | b')' | b'*' | b'+' | b',' | b';' | b'='
        );
        if unreserved_or_subdelim {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_plain_product_id_is_unchanged() {
        assert_eq!(encode_path_segment("HYPERLIQUID:BTC"), "HYPERLIQUID:BTC");
    }

    /// The one byte that must never survive unescaped: left alone, a URL parser reads `#` as the
    /// start of a fragment, so `LASHAY:EAVE-27JAN01-YES#120.1165` would arrive at the server missing
    /// everything from `#` onward.
    #[test]
    fn a_disambiguation_suffix_hash_is_percent_encoded() {
        let got = encode_path_segment("LASHAY:EAVE-27JAN01-YES#120.1165");
        assert_eq!(got, "LASHAY:EAVE-27JAN01-YES%23120.1165");
        assert!(!got.contains('#'));
    }

    #[test]
    fn a_literal_percent_sign_is_itself_escaped() {
        assert_eq!(encode_path_segment("100%"), "100%25");
    }

    /// The env var still has to be named — it is the one way to turn the surface off — but the
    /// default flipped to `127.0.0.1:9098`, so the remediation must not send a caller looking for
    /// a `DZ_ADMIN_BIND` they were never meant to set.
    #[test]
    fn the_admin_unreachable_envelope_names_dz_admin_bind() {
        let body = admin_unreachable_envelope("http://127.0.0.1:9098");
        let msg = body["remediation"].as_str().unwrap();
        assert!(
            msg.contains("DZ_ADMIN_BIND"),
            "a connection-refused against the admin surface must name DZ_ADMIN_BIND, since it is \
             otherwise indistinguishable from a wrong --admin-url: {msg}"
        );
        assert!(
            msg.contains("on by default"),
            "the surface is on by default at loopback; saying otherwise is the stale message this \
             replaced: {msg}"
        );
    }

    /// The `api_inactive` story is only useful if it points at the command that explains why.
    #[test]
    fn the_api_inactive_envelope_names_diagnose_and_quotes_the_verdict() {
        let body = api_inactive_envelope(
            "http://127.0.0.1:9099",
            "http://127.0.0.1:9098",
            "The tunnel is not up.",
        );
        assert_eq!(body["error"], "api_inactive");
        assert!(body["message"]
            .as_str()
            .unwrap()
            .contains("The tunnel is not up."));
        assert!(body["remediation"]
            .as_str()
            .unwrap()
            .contains("doublezero-edge diagnose"));
    }

    // -----------------------------------------------------------------------------------------
    // same_host — the guard on the diagnostics probe. Getting this wrong reports the local
    // container's state as a remote bridge's, which is worse than the vague answer it replaces.
    // -----------------------------------------------------------------------------------------

    #[test]
    fn the_same_host_on_different_ports_and_schemes_matches() {
        assert!(same_host("http://127.0.0.1:9099", "http://127.0.0.1:9098"));
        assert!(same_host("https://Edge.Example:443", "http://edge.example"));
        assert!(same_host("http://[::1]:9099/", "http://[::1]:9098"));
    }

    #[test]
    fn a_remote_url_never_matches_a_loopback_admin_url() {
        assert!(!same_host(
            "http://edge-1.example:9099",
            "http://127.0.0.1:9098"
        ));
        assert!(!same_host("http://10.0.0.4:9099", "http://127.0.0.1:9098"));
    }

    /// Two spellings of the same host still compare unequal, and a string with no host at all
    /// never matches — both suppress the probe, which is the safe direction.
    #[test]
    fn an_unresolvable_comparison_suppresses_the_probe() {
        assert!(!same_host("http://localhost:9099", "http://127.0.0.1:9098"));
        assert!(!same_host("", "http://127.0.0.1:9098"));
        assert!(!same_host("http:///v1", "http://127.0.0.1:9098"));
    }

    // -----------------------------------------------------------------------------------------
    // Raw-wire checks for the one POST this crate makes. These read the literal bytes off a real
    // TCP socket rather than trusting reqwest's behaviour by assumption — the server-side 400 on
    // any nonzero Content-Length exists specifically to catch a body-bearing client, so this must
    // be verified, not assumed.
    // -----------------------------------------------------------------------------------------

    use std::{
        io::{Read, Write},
        net::TcpListener,
        sync::mpsc,
        thread,
        time::Duration,
    };

    /// Bind an ephemeral listener, capture the raw bytes of exactly one request, answer it with a
    /// canned `200 {"applied":[]}`, and hand the captured request text back over a channel.
    fn capture_one_request() -> (String, mpsc::Receiver<String>) {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
        let addr = listener.local_addr().expect("local_addr");
        let (tx, rx) = mpsc::channel();
        thread::spawn(move || {
            if let Ok((mut stream, _)) = listener.accept() {
                let mut buf = [0u8; 8192];
                let n = stream.read(&mut buf).unwrap_or(0);
                let text = String::from_utf8_lossy(&buf[..n]).to_string();
                let _ = tx.send(text);
                let body = br#"{"applied":[]}"#;
                let resp = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\
                     Connection: close\r\n\r\n",
                    body.len()
                );
                let _ = stream.write_all(resp.as_bytes());
                let _ = stream.write_all(body);
                let _ = stream.flush();
            }
        });
        (format!("http://{addr}"), rx)
    }

    /// The load-bearing test from the task brief: verify, against real bytes on the wire, that
    /// `admin_post_channels` sends the spec as a percent-encoded query parameter and attaches
    /// **no** request body — not even an explicit `Content-Length: 0` — since the server 400s any
    /// nonzero `Content-Length` specifically to catch a body-bearing client rather than silently
    /// ignoring it.
    #[test]
    fn a_channels_post_sends_no_body_and_percent_encodes_the_query() {
        let (base, rx) = capture_one_request();
        let client = reqwest::blocking::Client::new();
        let outcome = admin_post_channels(&client, &base, "lashay-4=10,11;lashay-2=5");
        assert!(matches!(outcome, Outcome::Ok { .. }), "{outcome:?}");

        let raw = rx
            .recv_timeout(Duration::from_secs(2))
            .expect("the server thread must have captured a request");

        let request_line = raw.lines().next().unwrap_or_default().to_string();
        assert!(
            request_line.starts_with("POST /admin/channels?channels="),
            "the spec must travel as a query parameter on POST: {request_line}"
        );

        let query = request_line
            .split("channels=")
            .nth(1)
            .unwrap_or_default()
            .split_whitespace()
            .next()
            .unwrap_or_default()
            .to_string();
        // The spec syntax's own reserved bytes (`,` `;` `=`) must not survive literally in the
        // query string — left unescaped they would be ambiguous with query-string delimiters.
        assert!(
            !query.contains(','),
            "comma must be percent-encoded: {query}"
        );
        assert!(
            !query.contains(';'),
            "semicolon must be percent-encoded: {query}"
        );
        assert!(!query.contains('='), "'=' must be percent-encoded: {query}");
        let upper = query.to_uppercase();
        assert!(upper.contains("%2C"), "comma must encode to %2C: {query}");
        assert!(
            upper.contains("%3B"),
            "semicolon must encode to %3B: {query}"
        );
        assert!(upper.contains("%3D"), "'=' must encode to %3D: {query}");

        // The header block (everything up to the blank line) must carry no Content-Length at
        // all: a bodyless POST that still sent "Content-Length: 0" would technically satisfy the
        // server's `content_length > 0` check, but the task brief calls this out as a real defect
        // to catch rather than assume away, so it is pinned here explicitly.
        let head = raw.split("\r\n\r\n").next().unwrap_or(&raw);
        assert!(
            !head.to_lowercase().contains("content-length"),
            "a bodyless POST must not send a Content-Length header at all: {head}"
        );
        assert!(
            head.to_lowercase().contains("x-dz-admin-request"),
            "the CSRF-defeating header must be sent on every admin POST: {head}"
        );
    }

    /// `admin_get` issues a plain `GET` with no query string at all.
    #[test]
    fn admin_get_sends_a_plain_get_with_no_query_string() {
        let (base, rx) = capture_one_request();
        let client = reqwest::blocking::Client::new();
        let outcome = admin_get(&client, &base, "/admin/channels");
        assert!(matches!(outcome, Outcome::Ok { .. }), "{outcome:?}");
        let raw = rx
            .recv_timeout(Duration::from_secs(2))
            .expect("captured request");
        let request_line = raw.lines().next().unwrap_or_default();
        assert_eq!(request_line, "GET /admin/channels HTTP/1.1");
    }

    // -----------------------------------------------------------------------------------------
    // A 2xx with an undecodable body must never classify as `Ok`. A 500 with HTML would already
    // fall into `Failed` under the old code and prove nothing about this bug — the fixture here is
    // genuinely a 2xx, which is the one case the old `if (200..300).contains(&status)` let through
    // as success regardless of whether `resp.json()` actually decoded.
    // -----------------------------------------------------------------------------------------

    /// Bind an ephemeral listener and answer exactly one request with a canned, fixed response —
    /// the mirror image of `capture_one_request`, which captures the request instead of scripting
    /// the response. Used to serve a genuinely non-JSON 2xx.
    fn serve_one_response(
        status_line: &'static str,
        content_type: &'static str,
        body: &'static [u8],
    ) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
        let addr = listener.local_addr().expect("local_addr");
        thread::spawn(move || {
            if let Ok((mut stream, _)) = listener.accept() {
                let mut buf = [0u8; 8192];
                let _ = stream.read(&mut buf);
                let resp = format!(
                    "HTTP/1.1 {status_line}\r\nContent-Type: {content_type}\r\n\
                     Content-Length: {}\r\nConnection: close\r\n\r\n",
                    body.len()
                );
                let _ = stream.write_all(resp.as_bytes());
                let _ = stream.write_all(body);
                let _ = stream.flush();
            }
        });
        format!("http://{addr}")
    }

    /// The finding this pins: `--url` pointed at something that answers `200` with an HTML body
    /// (a WebSocket port's upgrade-refusal page, an ingress's default page, ...) must not be read
    /// as a successful JSON response.
    #[test]
    fn get_treats_a_2xx_non_json_body_as_invalid_not_ok() {
        let base = serve_one_response("200 OK", "text/html", b"<html>not json</html>");
        let client = reqwest::blocking::Client::new();
        let outcome = get(&client, &base, "/v1/products", &[]);
        assert!(
            matches!(outcome, Outcome::Invalid { status: 200, .. }),
            "a 2xx with a non-JSON body must classify as Invalid, not Ok: {outcome:?}"
        );
    }

    /// `classify` (the admin-surface path `admin_get`/`admin_post_channels` share) has the exact
    /// same shape and must be pinned separately — a fix to `get` alone would leave this one broken.
    #[test]
    fn classify_treats_a_2xx_non_json_body_as_invalid_not_ok() {
        let base = serve_one_response("200 OK", "text/html", b"<html>not json</html>");
        let client = reqwest::blocking::Client::new();
        let outcome = admin_get(&client, &base, "/admin/channels");
        assert!(
            matches!(outcome, Outcome::Invalid { status: 200, .. }),
            "a 2xx with a non-JSON body must classify as Invalid, not Ok: {outcome:?}"
        );
    }
}
