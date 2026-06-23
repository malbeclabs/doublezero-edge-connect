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

use anyhow::Result;
use prometheus::{Encoder, TextEncoder};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};
use tracing::{debug, info};

use crate::metrics::metrics;

/// Cap on request bytes read before giving up parsing the request line — a metrics scrape request is
/// tiny; anything larger is malformed or hostile and gets a `400`.
const MAX_REQUEST_BYTES: usize = 8192;

/// Bind `bind` and serve the metrics endpoint forever. Returns only on a fatal accept/bind error.
pub async fn run(bind: String) -> Result<()> {
    let listener = TcpListener::bind(&bind).await?;
    info!(%bind, "metrics endpoint listening (GET /metrics)");
    serve(listener).await
}

/// The accept loop, split out so tests can drive a pre-bound listener on an ephemeral port.
async fn serve(listener: TcpListener) -> Result<()> {
    loop {
        let (stream, _peer) = listener.accept().await?;
        tokio::spawn(async move {
            if let Err(e) = handle_conn(stream).await {
                debug!("metrics connection ended: {e}");
            }
        });
    }
}

async fn handle_conn(mut stream: TcpStream) -> Result<()> {
    // Read until the end of the request headers (or a sane cap); we only need the request line.
    let mut buf = Vec::with_capacity(1024);
    let mut tmp = [0u8; 1024];
    loop {
        let n = stream.read(&mut tmp).await?;
        if n == 0 {
            break; // client closed before sending a full request
        }
        buf.extend_from_slice(&tmp[..n]);
        if buf.windows(4).any(|w| w == b"\r\n\r\n") || buf.len() > MAX_REQUEST_BYTES {
            break;
        }
    }

    match request_path(&buf) {
        Some(path) if path == "/metrics" => {
            let encoder = TextEncoder::new();
            let mut body = Vec::new();
            encoder.encode(&metrics().registry().gather(), &mut body)?;
            write_response(&mut stream, "200 OK", encoder.format_type(), &body).await
        }
        Some(path) if path == "/" || path == "/healthz" => {
            write_response(&mut stream, "200 OK", "text/plain; charset=utf-8", b"ok\n").await
        }
        Some(_) => {
            write_response(
                &mut stream,
                "404 Not Found",
                "text/plain; charset=utf-8",
                b"not found\n",
            )
            .await
        }
        None => {
            write_response(
                &mut stream,
                "400 Bad Request",
                "text/plain; charset=utf-8",
                b"bad request\n",
            )
            .await
        }
    }
}

/// Extract the request-target path from the first line (`GET /metrics?x=1 HTTP/1.1`), stripping any
/// query string. Returns `None` if the request line is missing or malformed.
fn request_path(buf: &[u8]) -> Option<String> {
    let text = std::str::from_utf8(buf).ok()?;
    let line = text.lines().next()?;
    let target = line.split_whitespace().nth(1)?;
    let path = target.split('?').next().unwrap_or(target);
    Some(path.to_string())
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
    stream.write_all(header.as_bytes()).await?;
    stream.write_all(body).await?;
    stream.flush().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_path_parses_target_and_strips_query() {
        assert_eq!(
            request_path(b"GET /metrics HTTP/1.1\r\nHost: x\r\n\r\n").as_deref(),
            Some("/metrics")
        );
        assert_eq!(
            request_path(b"GET /metrics?foo=bar HTTP/1.1\r\n\r\n").as_deref(),
            Some("/metrics")
        );
        assert_eq!(request_path(b"").as_deref(), None);
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
    }
}
