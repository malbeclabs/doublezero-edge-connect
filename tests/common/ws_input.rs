//! Mock public WebSocket **input** server for E2E tests (Hyperliquid and Phoenix shapes).
//!
//! Stands in for a venue's public `wss://`: it accepts the bridge's WS-input connection, answers its
//! `subscribe` frames as the venue does, and emits scripted JSON on demand (the test pushes frames
//! through a channel, so it controls exactly when each public update lands relative to the multicast
//! replay). It speaks both the Hyperliquid `bbo`/`trades` shape ([`Self::send_bbo`]/
//! [`Self::send_trade`]) and the Phoenix `trades` shape ([`Self::send_phoenix_trade`]). This is the
//! input-side counterpart to `replay` (multicast) — the output side (`ws_client` + `assertions`) is
//! reused unchanged.

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
    /// Bind a loopback port and start accepting WS connections. Each accepted connection acks the
    /// input's subscribe frames and forwards any JSON pushed via [`Self::send_raw`] to the socket,
    /// reconnecting (re-accepting) if the input's connection drops.
    pub async fn start() -> Self {
        Self::serve(None).await
    }

    /// Like [`Self::start`], but a subscription to a name outside `known` is refused as the venue
    /// refuses it: Hyperliquid drops the connection without a frame, Phoenix answers `400`.
    pub async fn start_knowing(known: &[&str]) -> Self {
        Self::serve(Some(known.iter().map(|s| s.to_string()).collect())).await
    }

    async fn serve(known: Option<Vec<String>>) -> Self {
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
                // Pump test-scripted frames out while answering inbound subscribes. A send error, a
                // closed read half or a refusal by disconnect drops back to re-accept; a closed
                // channel ends the server.
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
                            Some(Ok(Message::Text(txt))) => match answer(&txt, known.as_deref()) {
                                Answer::Reply(reply) => {
                                    if write.send(Message::Text(reply.into())).await.is_err() {
                                        break;
                                    }
                                }
                                Answer::Drop => break, // no close frame, as Hyperliquid does
                                Answer::Nothing => continue,
                            },
                            Some(Ok(_)) => continue, // ping/pong
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

    /// Queue a raw JSON frame to be sent to the connected input.
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
    /// The input derives price as `quote / base`.
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

enum Answer {
    Reply(String),
    Drop,
    Nothing,
}

/// The venue's answer to one inbound text frame, in the shape of whichever venue's subscribe it is.
fn answer(txt: &str, known: Option<&[String]>) -> Answer {
    let frame: serde_json::Value = serde_json::from_str(txt).unwrap_or_default();
    let sub = &frame["subscription"];
    let is_known = |name: &str| known.is_none_or(|k| k.iter().any(|n| n == name));
    if frame["method"] == "subscribe" {
        if !is_known(sub["coin"].as_str().unwrap_or_default()) {
            return Answer::Drop;
        }
        return Answer::Reply(format!(
            r#"{{"channel":"subscriptionResponse","data":{{"method":"subscribe","subscription":{sub}}}}}"#
        ));
    }
    if frame["type"] == "subscribe" {
        let symbol = sub["symbol"].as_str().unwrap_or_default();
        let answered = format!(r#"{{"channel":"trades","marketSymbol":"{symbol}"}}"#);
        return Answer::Reply(if is_known(symbol) {
            format!(
                r#"{{"channel":"subscriptionStatus","status":"subscribed","subscription":{answered}}}"#
            )
        } else {
            format!(
                r#"{{"channel":"error","error":"unknown market symbol '{symbol}'","code":400,"subscription":{answered}}}"#
            )
        });
    }
    Answer::Nothing
}
