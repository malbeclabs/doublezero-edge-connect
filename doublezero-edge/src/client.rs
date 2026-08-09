//! The one place this crate talks to the network: a blocking GET against the edge-connect `/v1`
//! API, plus the percent-encoding a product id needs when it becomes a URL path segment.
//!
//! There is no concurrency requirement (one request per CLI invocation), so a blocking
//! `reqwest::blocking::Client` is used rather than pulling in an async runtime for its own sake.

use serde_json::{json, Value};

/// What a request against the API produced, already classified into the three shapes the rest of
/// this crate cares about. `status` on the two response-bearing variants is the raw HTTP status
/// code — the exit-code mapping in `main.rs` is the only other place that needs it.
pub enum Outcome {
    /// A `2xx` response with a JSON body.
    Ok { body: Value },
    /// A non-`2xx` response the server still answered — its body is already the
    /// `{"error","message","remediation"}` envelope `sinks/api.rs` promises.
    Failed { status: u16, body: Value },
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
    let body: Value = match resp.json() {
        Ok(v) => v,
        Err(_e) => json!({
            "error": "invalid_response",
            "message": format!("Response from {full} was not valid JSON."),
            "remediation": "Check --url points at an edge-connect instance's /v1 API, not something else on that port.",
        }),
    };
    if (200..300).contains(&status) {
        Outcome::Ok { body }
    } else {
        Outcome::Failed { status, body }
    }
}

/// The remediation-carrying envelope synthesized when there is no response to read at all — the
/// one case the server's own `{"error","message","remediation"}` shape can't produce for us,
/// because there is no server in the loop to produce it. Matches that shape exactly so downstream
/// code (rendering, exit-code mapping) never needs to know which side produced an error body.
pub fn unreachable_envelope(base_url: &str) -> Value {
    json!({
        "error": "api_unreachable",
        "message": format!("No response at {base_url}."),
        "remediation": "Is edge-connect running? Check `docker ps`, or pass --url.",
    })
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
}
