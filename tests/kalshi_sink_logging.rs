//! A client's signed handshake headers can be replayed against Kalshi while their timestamp holds,
//! so the sink must never log one. Its own binary: the test installs a global subscriber.

use std::{
    collections::HashMap,
    io::Write,
    sync::{Arc, Mutex},
    time::Duration,
};

use doublezero_edge_connect::{
    model::{BookReplay, FeedMessage},
    sinks::kalshi,
};
use futures_util::{SinkExt, StreamExt};
use tokio::sync::broadcast;
use tokio_tungstenite::tungstenite::{client::IntoClientRequest, Message};

const SECRETS: [(&str, &str); 3] = [
    ("KALSHI-ACCESS-KEY", "secret-key-id-0f3c"),
    ("KALSHI-ACCESS-SIGNATURE", "c2VjcmV0LXNpZ25hdHVyZS03YjE5"),
    ("KALSHI-ACCESS-TIMESTAMP", "1799999999123"),
];

#[derive(Clone, Default)]
struct Captured(Arc<Mutex<Vec<u8>>>);

impl Write for Captured {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[tokio::test]
async fn signed_handshake_headers_are_never_logged() {
    let captured = Captured::default();
    let writer = captured.clone();
    // Everything but this test's own client, whose handshake trace prints the request it sends.
    tracing_subscriber::fmt()
        .with_env_filter("trace,tungstenite::handshake::client=off")
        .with_ansi(false)
        .with_writer(move || writer.clone())
        .init();

    let listener = kalshi::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (tx, _rx) = broadcast::channel::<Arc<FeedMessage>>(16);
    let books = Arc::new(Mutex::new(BookReplay::default()));
    tokio::spawn(kalshi::serve(
        listener,
        tx,
        books,
        Arc::new(Mutex::new(HashMap::new())),
    ));

    for path in ["/trade-api/ws/v2", "/trade-api/ws/v2/margin"] {
        let mut request = format!("ws://{addr}{path}").into_client_request().unwrap();
        for (name, value) in SECRETS {
            request.headers_mut().insert(name, value.parse().unwrap());
        }
        let Ok((mut ws, _)) = tokio_tungstenite::connect_async(request).await else {
            continue;
        };
        let subscribe = r#"{"id":1,"cmd":"subscribe","params":{"channels":["fill"]}}"#;
        ws.send(Message::Text(subscribe.into())).await.unwrap();
        ws.next().await.unwrap().unwrap();
        ws.close(None).await.unwrap();
    }
    let markers = ["tungstenite::handshake::server", "kalshi sink client ended"];
    let log = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let log = String::from_utf8(captured.0.lock().unwrap().clone()).unwrap();
            if markers.iter().all(|m| log.contains(m)) {
                return log;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("the server's handshake and refusal were captured");
    for (name, value) in SECRETS {
        assert!(
            !log.contains(value),
            "{name}'s value reached the log:\n{log}"
        );
    }
}
