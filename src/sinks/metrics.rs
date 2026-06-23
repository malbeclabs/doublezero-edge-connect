//! Prometheus metrics HTTP exposer: serves the [`crate::metrics`] registry in the Prometheus text
//! format at `GET /metrics`, plus a `GET /` / `GET /healthz` liveness probe.
//!
//! Unlike [`crate::sinks::ws`], this sink does **not** subscribe to the `FeedMessage` broadcast — it
//! only encodes the metric registry on demand, so it is fully off the ingest hot path. The handler
//! is a hand-rolled minimal HTTP/1.1 responder over a [`TcpListener`] (no HTTP framework dependency,
//! matching the project's hand-rolled socket plumbing). Each request is answered with
//! `Connection: close`, so there is no keep-alive bookkeeping.
//!
//! No TLS (consistent with the rest of the service surface); terminate at a reverse proxy if the
//! endpoint is exposed beyond a trusted network.

use std::{sync::Arc, time::Duration};

use anyhow::Result;
use prometheus::{Encoder, TextEncoder};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::Semaphore,
    time::timeout,
};
use tracing::{debug, info, warn};

use crate::metrics::metrics;

/// Cap on request bytes read before giving up parsing the request line — a metrics scrape request is
/// tiny; anything larger is malformed or hostile and gets a `400`.
const MAX_REQUEST_BYTES: usize = 8192;

/// Per-connection read/write deadline. A scrape exchanges a few KiB and completes in milliseconds; a
/// client that trickles a request (slowloris) or stops reading the response is dropped at this bound
/// rather than parking a task + fd indefinitely.
const IO_TIMEOUT: Duration = Duration::from_secs(5);

/// Max connections handled concurrently. Bounds fd/task usage so a flood of half-open connections
/// can't exhaust descriptors; combined with [`IO_TIMEOUT`], stuck slots free within the deadline.
const MAX_CONNS: usize = 32;

/// `text/plain` content type reused for the non-metrics responses.
const TEXT: &str = "text/plain; charset=utf-8";

/// Bind `bind` and serve the metrics endpoint forever. Returns only on a fatal accept/bind error.
pub async fn run(bind: String) -> Result<()> {
    let listener = TcpListener::bind(&bind).await?;
    info!(%bind, "metrics endpoint listening (GET /metrics)");
    serve(listener).await
}

/// The accept loop, split out so tests can drive a pre-bound listener on an ephemeral port.
async fn serve(listener: TcpListener) -> Result<()> {
    let limiter = Arc::new(Semaphore::new(MAX_CONNS));
    loop {
        // Acquire a slot *before* accepting, so at most `MAX_CONNS` connections are in flight and
        // excess clients wait in the OS accept queue instead of each costing a task + fd.
        let permit = limiter
            .clone()
            .acquire_owned()
            .await
            .expect("metrics connection semaphore never closed");
        let (stream, _peer) = listener.accept().await?;
        tokio::spawn(async move {
            let _permit = permit; // held for the connection's lifetime; released on task end
            if let Err(e) = handle_conn(stream).await {
                debug!("metrics connection ended: {e}");
            }
        });
    }
}

async fn handle_conn(mut stream: TcpStream) -> Result<()> {
    // Read the request head under a deadline; a client that never completes it is dropped.
    let mut buf = Vec::with_capacity(1024);
    match timeout(IO_TIMEOUT, read_request_head(&mut stream, &mut buf)).await {
        Ok(Ok(())) => {}
        Ok(Err(e)) => return Err(e),
        Err(_elapsed) => {
            return write_response(
                &mut stream,
                "408 Request Timeout",
                TEXT,
                b"request timeout\n",
            )
            .await;
        }
    }

    let Some((method, path)) = parse_request(&buf) else {
        return write_response(&mut stream, "400 Bad Request", TEXT, b"bad request\n").await;
    };
    // Read-only endpoint: only GET is meaningful. Reject other verbs rather than serving them.
    if method != "GET" {
        return write_response(
            &mut stream,
            "405 Method Not Allowed",
            TEXT,
            b"method not allowed\n",
        )
        .await;
    }

    match path.as_str() {
        "/metrics" => {
            let encoder = TextEncoder::new();
            let mut body = Vec::new();
            if let Err(e) = encoder.encode(&metrics().registry().gather(), &mut body) {
                // A persistently-failing exposer should be visible at the default `info` level, not
                // swallowed into the connection-level `debug!`.
                warn!(error = %e, "metrics encode failed");
                return write_response(
                    &mut stream,
                    "500 Internal Server Error",
                    TEXT,
                    b"encode error\n",
                )
                .await;
            }
            write_response(&mut stream, "200 OK", encoder.format_type(), &body).await
        }
        "/" | "/healthz" => write_response(&mut stream, "200 OK", TEXT, b"ok\n").await,
        _ => write_response(&mut stream, "404 Not Found", TEXT, b"not found\n").await,
    }
}

/// Accumulate the request head until the blank-line terminator (`\r\n\r\n`) or the byte cap. Scans
/// only the freshly-appended tail (plus a 3-byte carry across the append boundary) instead of
/// rescanning the whole buffer each read.
async fn read_request_head(stream: &mut TcpStream, buf: &mut Vec<u8>) -> Result<()> {
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

/// Extract `(method, path)` from the request line (`GET /metrics?x=1 HTTP/1.1`), uppercasing the
/// method and stripping any query string. Returns `None` if the line is missing or malformed.
fn parse_request(buf: &[u8]) -> Option<(String, String)> {
    let text = std::str::from_utf8(buf).ok()?;
    let line = text.lines().next()?;
    let mut parts = line.split_whitespace();
    let method = parts.next()?.to_ascii_uppercase();
    let target = parts.next()?;
    let path = target.split('?').next().unwrap_or(target);
    Some((method, path.to_string()))
}

async fn write_response(
    stream: &mut TcpStream,
    status: &str,
    content_type: &str,
    body: &[u8],
) -> Result<()> {
    let header = format!(
        "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    // Bound the write too: a client that opened the connection but never drains the response must
    // not pin the task/fd past the deadline.
    timeout(IO_TIMEOUT, async {
        stream.write_all(header.as_bytes()).await?;
        stream.write_all(body).await?;
        stream.flush().await?;
        Ok::<_, anyhow::Error>(())
    })
    .await??;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_request_extracts_method_and_strips_query() {
        assert_eq!(
            parse_request(b"GET /metrics HTTP/1.1\r\nHost: x\r\n\r\n"),
            Some(("GET".to_string(), "/metrics".to_string()))
        );
        assert_eq!(
            parse_request(b"GET /metrics?foo=bar HTTP/1.1\r\n\r\n"),
            Some(("GET".to_string(), "/metrics".to_string()))
        );
        assert_eq!(
            parse_request(b"POST /metrics HTTP/1.1\r\n\r\n"),
            Some(("POST".to_string(), "/metrics".to_string()))
        );
        assert_eq!(parse_request(b""), None);
    }

    #[tokio::test]
    async fn serves_metrics_and_404s_unknown_paths() {
        // Bump a metric so the body is non-trivial.
        metrics()
            .emit
            .with_label_values(&["Hyperliquid", "quote"])
            .inc();

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let _ = serve(listener).await;
        });

        let base = format!("http://{addr}");
        let resp = reqwest::get(format!("{base}/metrics")).await.unwrap();
        assert_eq!(resp.status(), 200);
        assert_eq!(
            resp.headers()
                .get("content-type")
                .and_then(|v| v.to_str().ok()),
            Some("text/plain; version=0.0.4")
        );
        let body = resp.text().await.unwrap();
        assert!(body.contains("dz_emit_total"), "metrics body: {body}");

        let resp = reqwest::get(format!("{base}/nope")).await.unwrap();
        assert_eq!(resp.status(), 404);

        // A non-GET verb to a valid path is rejected, not served.
        let resp = reqwest::Client::new()
            .post(format!("{base}/metrics"))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 405);
    }
}
