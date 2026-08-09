//! Shared hand-rolled HTTP/1.1 scaffolding for the crate's small on-demand HTTP sinks (currently
//! [`crate::sinks::metrics`]; a query API sink is a second consumer). This crate deliberately
//! carries no HTTP framework dependency, so the accept loop, request-line parsing and response
//! writing live here once rather than being duplicated per sink.
//!
//! A sink supplies a handler — `Fn(&Request) -> (status, content_type, body)` — and gets a
//! connection-accepting server built on it via [`serve_loop`]. Every request is answered with
//! `Connection: close`, so there is no keep-alive bookkeeping. No TLS (consistent with the rest of
//! the service surface); terminate at a reverse proxy if a sink built on this is exposed beyond a
//! trusted network.

use std::{sync::Arc, time::Duration};

use anyhow::Result;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::Semaphore,
    time::timeout,
};
use tracing::debug;

/// Cap on request bytes read before giving up parsing the request line — these are tiny GET
/// requests; anything larger is malformed or hostile and gets a `400`.
pub const MAX_REQUEST_BYTES: usize = 8192;

/// Per-connection read/write deadline. An exchange is a few KiB and completes in milliseconds; a
/// client that trickles a request (slowloris) or stops reading the response is dropped at this
/// bound rather than parking a task + fd indefinitely.
pub const IO_TIMEOUT: Duration = Duration::from_secs(5);

/// `text/plain` content type shared by the plain-text (non-encoded-body) responses.
pub const TEXT_PLAIN: &str = "text/plain; charset=utf-8";

/// A parsed request line: method, path (query stripped) and the decoded query parameters.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Request {
    pub method: String,
    pub path: String,
    /// `(key, value)` pairs from the query string, in wire order, percent-decoded.
    pub params: Vec<(String, String)>,
    /// The `Content-Length` header value, `0` if absent or unparseable. This scaffolding never
    /// reads a request body (every sink built on it takes its input from the query string), so this
    /// exists only so a handler can **detect and refuse** one — see `sinks::admin`'s `POST`, which
    /// must not silently ignore a caller's body while treating an empty one as "clear everything".
    pub content_length: usize,
}

impl Request {
    /// The first value bound to `key` in the query string, if present.
    pub fn query(&self, key: &str) -> Option<&str> {
        self.params
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v.as_str())
    }
}

/// Extract a [`Request`] from the request line (e.g. `GET /v1/p?id=A%3AB HTTP/1.1`), uppercasing
/// the method and percent-decoding query keys/values. Returns `None` if the line is missing or
/// malformed (no method, or no target).
pub fn parse_request(buf: &[u8]) -> Option<Request> {
    let text = std::str::from_utf8(buf).ok()?;
    let mut lines = text.lines();
    let line = lines.next()?;
    let mut parts = line.split_whitespace();
    let method = parts.next()?.to_ascii_uppercase();
    let target = parts.next()?;
    let mut target_parts = target.splitn(2, '?');
    let path = target_parts.next().unwrap_or(target).to_string();
    let params = target_parts
        .next()
        .map(parse_query_params)
        .unwrap_or_default();
    let content_length = content_length(lines);
    Some(Request {
        method,
        path,
        params,
        content_length,
    })
}

/// `Content-Length`, scanned case-insensitively from the header lines (everything after the
/// request line, up to the blank-line terminator or the end of what's buffered so far). `0` if
/// absent or not a valid number — never treated as an error, since nothing here needs the exact
/// value beyond "is there a body at all" (see `Request::content_length`'s doc).
fn content_length<'a>(header_lines: impl Iterator<Item = &'a str>) -> usize {
    for line in header_lines {
        if line.is_empty() {
            break; // the blank line ends the headers
        }
        if let Some((name, value)) = line.split_once(':') {
            if name.trim().eq_ignore_ascii_case("content-length") {
                return value.trim().parse().unwrap_or(0);
            }
        }
    }
    0
}

/// Split a query string on `&` into percent-decoded `(key, value)` pairs. A pair with no `=` gets
/// an empty value; empty segments (e.g. a trailing `&`) are skipped.
fn parse_query_params(query: &str) -> Vec<(String, String)> {
    query
        .split('&')
        .filter(|pair| !pair.is_empty())
        .map(|pair| {
            let mut kv = pair.splitn(2, '=');
            let key = kv.next().unwrap_or("");
            let value = kv.next().unwrap_or("");
            (percent_decode(key), percent_decode(value))
        })
        .collect()
}

/// Hand-written percent-decoder: maps each `%XX` escape to its byte; anything else passes through
/// unchanged. This is a query-string decoder, not full `application/x-www-form-urlencoded` — `+`
/// is left as a literal plus, not turned into a space. A truncated or non-hex escape (malformed
/// input) is left as literal characters rather than rejected, so one bad escape doesn't take an
/// otherwise-parseable request down.
///
/// `pub` (not just used internally for query values): a sink that decodes its own path segments —
/// e.g. `sinks::api`, where a suffixed product id's `#` must survive as a literal path byte rather
/// than being read as a URI fragment by a client that builds the request through URL parsing — reuses
/// this rather than carrying a second decoder that could silently drift from it.
pub fn percent_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            let hi = (bytes[i + 1] as char).to_digit(16);
            let lo = (bytes[i + 2] as char).to_digit(16);
            if let (Some(hi), Some(lo)) = (hi, lo) {
                out.push(((hi * 16) + lo) as u8);
                i += 3;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// Accumulate the request head until the blank-line terminator (`\r\n\r\n`) or the byte cap. Scans
/// only the freshly-appended tail (plus a 3-byte carry across the append boundary) instead of
/// rescanning the whole buffer each read.
pub async fn read_request_head(stream: &mut TcpStream, buf: &mut Vec<u8>) -> Result<()> {
    let mut tmp = [0u8; 1024];
    loop {
        let n = stream.read(&mut tmp).await?;
        if n == 0 {
            break; // client closed before sending a full request
        }
        let scan_from = buf.len().saturating_sub(3);
        buf.extend_from_slice(&tmp[..n]);
        if buf[scan_from..].windows(4).any(|w| w == b"\r\n\r\n") || buf.len() > MAX_REQUEST_BYTES {
            break;
        }
    }
    Ok(())
}

/// Write a complete `HTTP/1.1` response and flush it, all under [`IO_TIMEOUT`]. Bounding the write
/// too matters: a client that opened the connection but never drains the response must not pin the
/// task/fd past the deadline.
pub async fn write_response(
    stream: &mut TcpStream,
    status: &str,
    content_type: &str,
    body: &[u8],
) -> Result<()> {
    let header = format!(
        "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    timeout(IO_TIMEOUT, async {
        stream.write_all(header.as_bytes()).await?;
        stream.write_all(body).await?;
        stream.flush().await?;
        Ok::<_, anyhow::Error>(())
    })
    .await??;
    Ok(())
}

/// A response as `(status line, content type, body)`; `status` and a fixed set of content types are
/// always `'static` string literals in practice, but `content_type` is owned since an encoder (e.g.
/// the Prometheus text encoder) may hand back a computed value.
pub type Response = (&'static str, String, Vec<u8>);

/// Read one request off `stream`, dispatch it to `handler`, and write the response — all the
/// per-connection plumbing every hand-rolled sink here needs: a read-deadline `408`, a
/// malformed-request `400`, then whatever `handler` decides for a well-formed request (a `405` for
/// an unsupported method is the handler's call, since some future sink might reasonably serve more
/// than `GET`).
pub async fn handle_conn<H>(mut stream: TcpStream, handler: &H) -> Result<()>
where
    H: Fn(&Request) -> Response,
{
    let mut buf = Vec::with_capacity(1024);
    match timeout(IO_TIMEOUT, read_request_head(&mut stream, &mut buf)).await {
        Ok(Ok(())) => {}
        Ok(Err(e)) => return Err(e),
        Err(_elapsed) => {
            return write_response(
                &mut stream,
                "408 Request Timeout",
                TEXT_PLAIN,
                b"request timeout\n",
            )
            .await;
        }
    }

    let Some(request) = parse_request(&buf) else {
        return write_response(&mut stream, "400 Bad Request", TEXT_PLAIN, b"bad request\n").await;
    };

    let (status, content_type, body) = handler(&request);
    write_response(&mut stream, status, &content_type, &body).await
}

/// The shared accept loop: bounds concurrent connections to `max_conns` (excess clients wait in the
/// OS accept queue instead of each costing a task + fd) and spawns [`handle_conn`] per connection
/// with `handler`. Returns only on a fatal accept error.
pub async fn serve_loop<H>(listener: TcpListener, max_conns: usize, handler: Arc<H>) -> Result<()>
where
    H: Fn(&Request) -> Response + Send + Sync + 'static,
{
    let limiter = Arc::new(Semaphore::new(max_conns));
    loop {
        // Acquire a slot *before* accepting, so at most `max_conns` connections are in flight.
        let permit = limiter
            .clone()
            .acquire_owned()
            .await
            .expect("http connection semaphore never closed");
        let (stream, _peer) = listener.accept().await?;
        let handler = handler.clone();
        tokio::spawn(async move {
            let _permit = permit; // held for the connection's lifetime; released on task end
            if let Err(e) = handle_conn(stream, handler.as_ref()).await {
                debug!("http connection ended: {e}");
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_request_line_splits_into_method_path_and_query() {
        let r = parse_request(b"GET /v1/products?limit=5&x=1 HTTP/1.1\r\nHost: x\r\n\r\n").unwrap();
        assert_eq!(r.method, "GET");
        assert_eq!(r.path, "/v1/products");
        assert_eq!(r.query("limit"), Some("5"));
        assert_eq!(r.query("x"), Some("1"));
        assert_eq!(r.query("absent"), None);
    }

    #[test]
    fn a_path_without_a_query_yields_no_params() {
        let r = parse_request(b"GET /metrics HTTP/1.1\r\n\r\n").unwrap();
        assert_eq!(r.path, "/metrics");
        assert_eq!(r.query("anything"), None);
    }

    /// Percent-encoding matters: a product id carries `:` and `#`, and `#` in particular must survive
    /// as a value rather than being read as a fragment delimiter.
    #[test]
    fn percent_encoded_values_are_decoded() {
        let r = parse_request(b"GET /v1/p?id=HYPERLIQUID%3ABTC%232.41 HTTP/1.1\r\n\r\n").unwrap();
        assert_eq!(r.query("id"), Some("HYPERLIQUID:BTC#2.41"));
    }

    #[test]
    fn a_malformed_request_line_is_rejected() {
        assert!(parse_request(b"nonsense\r\n\r\n").is_none());
    }

    /// A `Content-Length` header is picked up case-insensitively — the one signal
    /// `sinks::admin`'s `POST` handler needs to refuse a body-bearing request.
    #[test]
    fn a_content_length_header_is_parsed_case_insensitively() {
        let r = parse_request(b"POST /admin/channels HTTP/1.1\r\ncontent-length: 11\r\n\r\n").unwrap();
        assert_eq!(r.content_length, 11);
    }

    /// No header at all (the common case: every real sink here is `GET`-only, or a `POST` with
    /// nothing but a query string) reads as zero, not an error.
    #[test]
    fn a_missing_content_length_header_reads_as_zero() {
        let r = parse_request(b"GET /v1/products HTTP/1.1\r\nHost: x\r\n\r\n").unwrap();
        assert_eq!(r.content_length, 0);
    }
}
