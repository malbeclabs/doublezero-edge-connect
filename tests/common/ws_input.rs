//! Mock public WebSocket **input** server for E2E tests (Hyperliquid and Phoenix shapes).
//!
//! Stands in for a venue's public `wss://`: it accepts the bridge's WS-feeder connection, drains its
//! `subscribe` frames, and emits scripted JSON on demand (the test pushes frames through a channel,
//! so it controls exactly when each public update lands relative to the multicast replay). It speaks
//! both the Hyperliquid `bbo`/`trades` shape ([`Self::send_bbo`]/[`Self::send_trade`]) and the
//! Phoenix `trades` shape ([`Self::send_phoenix_trade`]). This is the input-side counterpart to
//! `replay` (multicast) — the output side (`ws_client` + `assertions`) is reused unchanged.

use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use tokio::{net::TcpListener, sync::mpsc, task::JoinHandle};
use tokio_tungstenite::{accept_async, tungstenite::Message};

/// A running mock HL public WS server on loopback. Drop it to stop the server.
pub struct MockWsInput {
    addr: String,
    tx: mpsc::UnboundedSender<String>,
    _handle: JoinHandle<()>,
}

impl MockWsInput {
    /// Bind a loopback port and start accepting WS connections. Each accepted connection drains the
    /// feeder's subscribe frames and then forwards any JSON pushed via [`Self::send_raw`] to the
    /// socket, reconnecting (re-accepting) if the feeder's connection drops.
    pub async fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind mock ws");
        let addr = listener.local_addr().unwrap().to_string();
        let (tx, mut rx) = mpsc::unbounded_channel::<String>();

        let handle = tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    return;
                };
                let Ok(ws) = accept_async(stream).await else {
                    continue;
                };
                let (mut write, mut read) = ws.split();
                // Pump test-scripted frames out while draining (and ignoring) inbound subscribes.
                // A send error or a closed read half drops back to re-accept; a closed channel ends
                // the server.
                loop {
                    tokio::select! {
                        outgoing = rx.recv() => match outgoing {
                            Some(json) => {
                                if write.send(Message::Text(json.into())).await.is_err() {
                                    break;
                                }
                            }
                            None => return, // channel closed: server shutting down
                        },
                        incoming = read.next() => match incoming {
                            Some(Ok(_)) => continue, // subscribe frame (or ping) — ignore
                            _ => break,              // peer closed/errored: re-accept
                        },
                    }
                }
            }
        });

        // Give the listener a beat to be ready before the bridge tries to connect.
        tokio::time::sleep(Duration::from_millis(50)).await;
        Self {
            addr,
            tx,
            _handle: handle,
        }
    }

    /// The `ws://host:port` URL to pass to the bridge's `--ws-input-url`.
    pub fn url(&self) -> String {
        format!("ws://{}", self.addr)
    }

    /// Queue a raw JSON frame to be sent to the connected feeder.
    pub fn send_raw(&self, json: String) {
        let _ = self.tx.send(json);
    }

    /// Queue a two-sided `bbo` frame: `time_ms` is the venue block time in milliseconds (the bridge
    /// scales it to ns as `time_ms × 1_000_000`, the same canonical `source_ts` the edge copy
    /// carries). px/sz are sent as decimal strings, exactly as the real HL API encodes them.
    pub fn send_bbo(
        &self,
        coin: &str,
        time_ms: u64,
        bid_px: f64,
        bid_sz: f64,
        ask_px: f64,
        ask_sz: f64,
    ) {
        self.send_raw(format!(
            r#"{{"channel":"bbo","data":{{"coin":"{coin}","time":{time_ms},"bbo":[{{"px":"{bid_px}","sz":"{bid_sz}","n":1}},{{"px":"{ask_px}","sz":"{ask_sz}","n":1}}]}}}}"#
        ));
    }

    /// Queue a one-element `trades` frame. `side` is `"B"` (buy) or `"A"` (sell); `tid` is the
    /// venue trade id (the edge feed's `trade_id`).
    pub fn send_trade(&self, coin: &str, side: &str, px: f64, sz: f64, time_ms: u64, tid: u64) {
        self.send_raw(format!(
            r#"{{"channel":"trades","data":[{{"coin":"{coin}","side":"{side}","px":"{px}","sz":"{sz}","time":{time_ms},"tid":{tid},"hash":"0x0"}}]}}"#
        ));
    }

    /// Queue a one-element Phoenix `trades` frame, exactly as `perp-api.phoenix.trade/v1/ws` encodes
    /// it. `side` is `"bid"` (aggressing buy) or `"ask"` (aggressing sell); `seq` is the public
    /// `tradeSequenceNumber` (= the edge `trade_id`); `ts_secs` is the Unix-seconds timestamp string.
    /// The feeder derives price as `quote / base`.
    pub fn send_phoenix_trade(
        &self,
        symbol: &str,
        seq: u64,
        side: &str,
        base: f64,
        quote: f64,
        ts_secs: u64,
    ) {
        self.send_raw(format!(
            r#"{{"channel":"trades","symbol":"{symbol}","trades":[{{"tradeSequenceNumber":"{seq}","side":"{side}","baseAmount":{base},"quoteAmount":{quote},"timestamp":"{ts_secs}","slot":1,"taker":"x"}}]}}"#
        ));
    }
}
