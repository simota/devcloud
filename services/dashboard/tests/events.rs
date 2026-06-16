//! Integration test for the `/api/events` WebSocket proxy.
//!
//! Topology under test (the real production path, end to end):
//!
//! ```text
//!   WS client (this test)  ⇄  real dashboard /api/events  ⇄  mock relay /_events
//! ```
//!
//! A mock relay speaks the `internal/eventsrelay` frame protocol (JSON text
//! frames `hello`/`event`/`ping` with a monotonic `seq`) and echoes the
//! `?topics=` query it received back inside the `hello` frame so we can assert
//! the filter passes through. We connect a real WebSocket client to the
//! dashboard and assert: (1) the dashboard upgraded our connection, (2) we
//! receive the `hello` frame (with the forwarded topics), (3) we receive the
//! `event` frame verbatim with its `seq`, and (4) a clean close propagates.

use std::sync::Arc;

use devcloud_dashboard::{http, Config};
use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpListener;
use tokio_tungstenite::tungstenite::protocol::Message;

/// Spawns a mock relay that, on the first `/_events` connection, sends a
/// `hello` frame (echoing the received `topics` query), then one `event` frame,
/// then closes. Returns the `ws://host:port` base.
async fn mock_relay() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        // Capture the request target during the handshake to echo the topics.
        let captured: Arc<std::sync::Mutex<String>> =
            Arc::new(std::sync::Mutex::new(String::new()));
        let cap = Arc::clone(&captured);
        let callback =
            |req: &tokio_tungstenite::tungstenite::handshake::server::Request,
             resp: tokio_tungstenite::tungstenite::handshake::server::Response| {
                *cap.lock().unwrap() = req.uri().to_string();
                Ok(resp)
            };
        let mut ws = tokio_tungstenite::accept_hdr_async(stream, callback)
            .await
            .unwrap();

        let uri = captured.lock().unwrap().clone();
        let topics = uri
            .split_once("topics=")
            .map(|(_, t)| t.to_string())
            .unwrap_or_default();

        // hello frame, echoing the forwarded topics filter.
        let hello = format!(r#"{{"type":"hello","seq":0,"topics":"{topics}"}}"#);
        ws.send(Message::Text(hello)).await.unwrap();

        // one event frame with a monotonic seq.
        let event = r#"{"type":"event","seq":1,"event":{"type":"sqs.queue.created"}}"#;
        ws.send(Message::Text(event.to_string())).await.unwrap();

        ws.close(None).await.unwrap();
    });

    format!("ws://{addr}")
}

/// Spawns the real dashboard server bound to an ephemeral port, pointed at the
/// given relay base. Returns the dashboard's `ws://host:port` base.
async fn dashboard(relay_base: &str) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let mut cfg = Config::default();
    cfg.event_relay_endpoint = relay_base.to_string();
    let cfg = Arc::new(cfg);

    tokio::spawn(async move {
        // never-resolving shutdown future: the test process tears it down.
        let never = std::future::pending::<()>();
        let _ = http::serve(listener, cfg, never).await;
    });

    format!("ws://{addr}")
}

#[tokio::test]
async fn proxies_relay_frames_to_browser_with_topics_filter() {
    let relay = mock_relay().await;
    let dash = dashboard(&relay).await;

    // Connect a real WS client to the dashboard /api/events with a topics filter.
    let url = format!("{dash}/api/events?topics=sqs,s3");
    let (mut client, resp) = tokio_tungstenite::connect_async(&url)
        .await
        .expect("dashboard should upgrade the connection");
    assert_eq!(resp.status(), 101, "expected 101 Switching Protocols");

    // 1. hello frame, with the topics filter forwarded through to the relay.
    let hello = next_text(&mut client).await;
    let v: serde_json::Value = serde_json::from_str(&hello).unwrap();
    assert_eq!(v["type"], "hello");
    assert_eq!(v["seq"], 0);
    assert_eq!(v["topics"], "sqs,s3", "topics filter must pass through");

    // 2. event frame, verbatim, preserving seq.
    let event = next_text(&mut client).await;
    let v: serde_json::Value = serde_json::from_str(&event).unwrap();
    assert_eq!(v["type"], "event");
    assert_eq!(v["seq"], 1);
    assert_eq!(v["event"]["type"], "sqs.queue.created");

    // 3. clean close propagates from the relay through the dashboard.
    loop {
        match client.next().await {
            Some(Ok(Message::Close(_))) | None => break,
            Some(Ok(_)) => continue,
            Some(Err(e)) => {
                // A reset on an already-closing stream is an acceptable close.
                let _ = e;
                break;
            }
        }
    }
}

/// Reads the next text frame from the client, skipping control frames.
async fn next_text(
    client: &mut tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
) -> String {
    loop {
        match client.next().await {
            Some(Ok(Message::Text(t))) => return t,
            Some(Ok(_)) => continue,
            other => panic!("expected a text frame, got {other:?}"),
        }
    }
}
