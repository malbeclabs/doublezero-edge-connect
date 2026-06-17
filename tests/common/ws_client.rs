//! Minimal WebSocket client that connects to the bridge and collects JSON messages.

use std::time::Duration;

use futures_util::StreamExt;
use serde_json::Value;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;

/// Connect to `ws://{ws_addr}` and collect parsed JSON text frames until `stop` is
/// satisfied or `timeout` elapses. The empty subscription default (no subscribe sent)
/// is a firehose, so every message is delivered.
pub async fn collect(
    ws_addr: &str,
    timeout: Duration,
    stop: impl Fn(&[Value]) -> bool,
) -> Vec<Value> {
    let url = format!("ws://{ws_addr}");
    let (mut ws, _resp) = connect_async(&url).await.expect("ws connect");
    let mut msgs: Vec<Value> = Vec::new();
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            break;
        }
        match tokio::time::timeout(remaining, ws.next()).await {
            Err(_) => break,   // overall timeout
            Ok(None) => break, // server closed
            Ok(Some(Ok(Message::Text(txt)))) => {
                if let Ok(v) = serde_json::from_str::<Value>(&txt) {
                    msgs.push(v);
                    if stop(&msgs) {
                        break;
                    }
                }
            }
            // tokio-tungstenite answers pings automatically as the stream is polled.
            Ok(Some(Ok(_))) => {}
            Ok(Some(Err(_))) => break,
        }
    }
    msgs
}

/// All messages whose `"type"` tag equals `t`.
pub fn by_type<'a>(msgs: &'a [Value], t: &str) -> Vec<&'a Value> {
    msgs.iter()
        .filter(|m| m.get("type").and_then(|v| v.as_str()) == Some(t))
        .collect()
}
