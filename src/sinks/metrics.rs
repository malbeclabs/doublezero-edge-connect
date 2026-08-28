//! Prometheus metrics HTTP exposer: serves the [`crate::metrics`] registry in the Prometheus text
//! format at `GET /metrics`, plus a `GET /` / `GET /healthz` liveness probe.
//!
//! Unlike [`crate::sinks::ws`], this sink does **not** subscribe to the `FeedMessage` broadcast — it
//! only encodes the metric registry on demand, so it is fully off the ingest hot path. The
//! connection handling and response writing are the shared [`crate::sinks::http`] scaffolding (no
//! HTTP framework dependency, matching the project's hand-rolled socket plumbing); this module only
//! supplies the request handler.
//!
//! No TLS (consistent with the rest of the service surface); terminate at a reverse proxy if the
//! endpoint is exposed beyond a trusted network.

use std::sync::Arc;

use anyhow::Result;
use prometheus::{Encoder, TextEncoder};
use tokio::net::TcpListener;
use tracing::{info, warn};

use super::http::{self, Request, Response};
use crate::metrics::metrics;

/// Max connections handled concurrently. Bounds fd/task usage so a flood of half-open connections
/// can't exhaust descriptors; combined with `http::IO_TIMEOUT`, stuck slots free within the
/// deadline.
const MAX_CONNS: usize = 32;

/// Bind `bind` and serve the metrics endpoint forever. Returns only on a fatal accept/bind error.
pub async fn run(bind: String) -> Result<()> {
    let listener = TcpListener::bind(&bind).await?;
    info!(%bind, "metrics endpoint listening (GET /metrics)");
    serve(listener).await
}

/// The accept loop, split out so tests can drive a pre-bound listener on an ephemeral port.
async fn serve(listener: TcpListener) -> Result<()> {
    http::serve_loop(listener, MAX_CONNS, Arc::new(handle_request)).await
}

/// Answer one parsed request. Read-only endpoint: only `GET` is meaningful, so other verbs are
/// rejected rather than served.
fn handle_request(req: &Request) -> Response {
    if req.method != "GET" {
        return (
            "405 Method Not Allowed",
            http::TEXT_PLAIN.to_string(),
            b"method not allowed\n".to_vec(),
        );
    }

    match req.path.as_str() {
        "/metrics" => {
            let encoder = TextEncoder::new();
            let mut body = Vec::new();
            if let Err(e) = encoder.encode(&metrics().registry().gather(), &mut body) {
                // A persistently-failing exposer should be visible at the default `info` level, not
                // swallowed into the connection-level `debug!`.
                warn!(error = %e, "metrics encode failed");
                return (
                    "500 Internal Server Error",
                    http::TEXT_PLAIN.to_string(),
                    b"encode error\n".to_vec(),
                );
            }
            ("200 OK", encoder.format_type().to_string(), body)
        }
        "/" | "/healthz" => ("200 OK", http::TEXT_PLAIN.to_string(), b"ok\n".to_vec()),
        _ => (
            "404 Not Found",
            http::TEXT_PLAIN.to_string(),
            b"not found\n".to_vec(),
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Kept in this module (rather than deleted) so the pre-extraction test below still exercises
    /// this sink's exact `(method, path)` outcome, unmodified, against the now-shared parser.
    fn parse_request(buf: &[u8]) -> Option<(String, String)> {
        let r = http::parse_request(buf)?;
        Some((r.method, r.path))
    }

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
            .with_label_values(&["HYPERLIQUID", "quote"])
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
