//! `/api/events` WebSocket proxy.
//!
//! The legacy dashboard (`internal/dashboard/events_handler.rs`) upgrades the
//! browser request to a WebSocket and streams events from the in-process bus.
//! The Rust dashboard runs out-of-process and cannot reach the bus directly, so
//! it instead acts as a **transparent relay proxy**:
//!
//! ```text
//!   browser  ⇄  dashboard /api/events  ⇄  daemon event relay (/_events)
//! ```
//!
//! On a browser WS connect we open a WS *client* to the daemon relay
//! (`ws://<relay>/_events`, forwarding the `?topics=` query verbatim) and pump
//! frames between the two sockets without reshaping them: the relay's
//! `hello`/`event`/`ping`/`resync` JSON text frames (each carrying a monotonic
//! `seq`) are forwarded byte-for-byte to the browser, and any browser frames are
//! forwarded upstream. Either side closing — or process shutdown — tears the
//! whole proxy down cleanly.
//!
//! ## WS integration with the hand-rolled HTTP server
//!
//! The crate's HTTP server (`http.rs`) reads a [`Request`] off the raw
//! `TcpStream` and normally returns a [`Response`]. WebSocket needs the opposite:
//! after the RFC6455 handshake we must *keep* the TCP stream and drive frames on
//! it. So `http.rs` special-cases `/api/events` **before** routing: it hands the
//! already-read [`Request`] plus the live `TcpStream` to [`handle`], which:
//!   1. validates the upgrade headers and computes `Sec-WebSocket-Accept`
//!      (SHA-1 of the client key + the RFC6455 GUID, base64-encoded — done by
//!      `tungstenite::handshake::derive_accept_key`),
//!   2. writes the `101 Switching Protocols` response by hand,
//!   3. wraps the hijacked stream as a server-role
//!      [`tokio_tungstenite::WebSocketStream`] (`from_raw_socket`, no extra
//!      handshake I/O since we already wrote the 101), and
//!   4. opens the upstream client and runs the bidirectional pump.
//!
//! ## Keepalive / cadence parity
//!
//! `events_handler.rs` sends a WS ping every 15s. Here the keepalive is covered
//! two ways: (a) the daemon relay emits its own `{"type":"ping","seq":N}` text
//! frame every 15s, which we forward to the browser verbatim (this is the frame
//! the dashboard UI actually consumes), and (b) tungstenite answers any
//! control-level WS Ping with a Pong automatically on both legs. We do not
//! synthesize additional pings — doing so would diverge from the relay's
//! authoritative cadence and double up frames.

use std::sync::Arc;

use futures_util::{SinkExt, StreamExt};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::handshake::derive_accept_key;
use tokio_tungstenite::tungstenite::protocol::{Message, Role};
use tokio_tungstenite::WebSocketStream;

use crate::config::Config;
use crate::http::Request;

/// Returns true when the request is a WebSocket upgrade for `/api/events`.
///
/// Mirrors `events_handler.rs`: only `GET` is accepted, and the standard
/// RFC6455 upgrade headers must be present (`Upgrade: websocket`,
/// `Connection: …Upgrade…`, a `Sec-WebSocket-Key`).
pub fn is_events_upgrade(req: &Request) -> bool {
    if req.path != "/api/events" {
        return false;
    }
    req.method.eq_ignore_ascii_case("GET")
        && req.header("upgrade").eq_ignore_ascii_case("websocket")
        && req
            .header("connection")
            .to_ascii_lowercase()
            .contains("upgrade")
        && !req.header("sec-websocket-key").is_empty()
}

/// Drives the `/api/events` proxy on a hijacked stream.
///
/// `req` is the already-parsed browser request (its `?topics=` query and
/// `Sec-WebSocket-Key` are used here); `stream` is the live browser TCP socket
/// taken over from the HTTP server. The function performs the server-side
/// handshake, connects upstream to the daemon relay, and pumps until either side
/// closes. All errors are swallowed into a clean teardown — this is a
/// best-effort local relay, not a request/response handler.
pub async fn handle(stream: TcpStream, req: &Request, config: Arc<Config>) {
    let key = req.header("sec-websocket-key");
    if key.is_empty() {
        return;
    }

    // 1. Write the 101 handshake by hand over the hijacked stream.
    let accept = derive_accept_key(key.as_bytes());
    let response = format!(
        "HTTP/1.1 101 Switching Protocols\r\n\
         Upgrade: websocket\r\n\
         Connection: Upgrade\r\n\
         Sec-WebSocket-Accept: {accept}\r\n\r\n"
    );
    let mut stream = stream;
    if stream.write_all(response.as_bytes()).await.is_err() {
        return;
    }
    if stream.flush().await.is_err() {
        return;
    }

    // 2. Wrap the hijacked stream as a server-role WS (handshake already done).
    let mut browser = WebSocketStream::from_raw_socket(stream, Role::Server, None).await;

    // 3. Connect upstream to the daemon relay, forwarding the topics filter.
    let relay_url = match relay_url(&config.event_relay_endpoint, &req.query) {
        Some(u) => u,
        None => {
            // No relay configured (or a bad base): close the browser cleanly.
            let _ = browser.close(None).await;
            return;
        }
    };
    let relay = match tokio_tungstenite::connect_async(&relay_url).await {
        Ok((ws, _resp)) => ws,
        Err(_) => {
            let _ = browser.close(None).await;
            return;
        }
    };

    pump(browser, relay).await;
}

/// Bidirectionally copies frames between the browser and relay sockets verbatim
/// until either side closes or errors. Returns when both directions are done.
async fn pump(
    browser: WebSocketStream<TcpStream>,
    relay: WebSocketStream<tokio_tungstenite::MaybeTlsStream<TcpStream>>,
) {
    let (mut browser_tx, mut browser_rx) = browser.split();
    let (mut relay_tx, mut relay_rx) = relay.split();

    // relay -> browser: forward hello/event/ping/resync frames verbatim.
    let downstream = async {
        while let Some(msg) = relay_rx.next().await {
            let msg = match msg {
                Ok(m) => m,
                Err(_) => break,
            };
            let stop = matches!(msg, Message::Close(_));
            // tungstenite answers Ping with Pong itself; don't forward Pong.
            if matches!(msg, Message::Pong(_)) {
                continue;
            }
            if browser_tx.send(msg).await.is_err() {
                break;
            }
            if stop {
                break;
            }
        }
        let _ = browser_tx.close().await;
    };

    // browser -> relay: forward any client frames (and close) upstream.
    let upstream = async {
        while let Some(msg) = browser_rx.next().await {
            let msg = match msg {
                Ok(m) => m,
                Err(_) => break,
            };
            let stop = matches!(msg, Message::Close(_));
            if matches!(msg, Message::Pong(_)) {
                continue;
            }
            if relay_tx.send(msg).await.is_err() {
                break;
            }
            if stop {
                break;
            }
        }
        let _ = relay_tx.close().await;
    };

    // Run both directions; finish as soon as either completes (one side closed),
    // then the other future's drop closes its sink.
    tokio::select! {
        _ = downstream => {}
        _ = upstream => {}
    }
}

/// Builds the upstream relay WS URL from the configured base and the browser's
/// query string, forwarding only the `topics` filter (the relay ignores other
/// params, but we keep the surface minimal and matching `events_handler.rs`).
///
/// Returns `None` when no relay base is configured.
fn relay_url(base: &str, query: &str) -> Option<String> {
    let base = base.trim_end_matches('/');
    if base.is_empty() {
        return None;
    }
    let topics = query_param(query, "topics");
    let mut url = format!("{base}/_events");
    if let Some(t) = topics {
        if !t.is_empty() {
            url.push_str("?topics=");
            url.push_str(&t);
        }
    }
    Some(url)
}

/// Extracts the first value of `key` from a raw `a=b&c=d` query string. The
/// value is returned exactly as received (already percent-encoded by the
/// browser), so it can be appended to the upstream URL verbatim.
fn query_param(query: &str, key: &str) -> Option<String> {
    for pair in query.split('&') {
        if let Some((k, v)) = pair.split_once('=') {
            if k == key {
                return Some(v.to_string());
            }
        } else if pair == key {
            return Some(String::new());
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn req(method: &str, path: &str, headers: &[(&str, &str)]) -> Request {
        let mut map = HashMap::new();
        for (k, v) in headers {
            map.insert(k.to_ascii_lowercase(), v.to_string());
        }
        Request {
            method: method.to_string(),
            path: path.to_string(),
            raw_path: path.to_string(),
            query: String::new(),
            headers: map,
            body: Vec::new(),
        }
    }

    #[test]
    fn detects_valid_upgrade() {
        let r = req(
            "GET",
            "/api/events",
            &[
                ("Upgrade", "websocket"),
                ("Connection", "keep-alive, Upgrade"),
                ("Sec-WebSocket-Key", "dGhlIHNhbXBsZSBub25jZQ=="),
            ],
        );
        assert!(is_events_upgrade(&r));
    }

    #[test]
    fn rejects_non_events_path() {
        let r = req(
            "GET",
            "/api/sqs/status",
            &[
                ("Upgrade", "websocket"),
                ("Connection", "Upgrade"),
                ("Sec-WebSocket-Key", "x"),
            ],
        );
        assert!(!is_events_upgrade(&r));
    }

    #[test]
    fn rejects_missing_upgrade_header() {
        let r = req(
            "GET",
            "/api/events",
            &[("Connection", "Upgrade"), ("Sec-WebSocket-Key", "x")],
        );
        assert!(!is_events_upgrade(&r));
    }

    #[test]
    fn rejects_non_get() {
        let r = req(
            "POST",
            "/api/events",
            &[
                ("Upgrade", "websocket"),
                ("Connection", "Upgrade"),
                ("Sec-WebSocket-Key", "x"),
            ],
        );
        assert!(!is_events_upgrade(&r));
    }

    #[test]
    fn relay_url_appends_events_path() {
        assert_eq!(
            relay_url("ws://127.0.0.1:8027", "").unwrap(),
            "ws://127.0.0.1:8027/_events"
        );
        assert_eq!(
            relay_url("ws://127.0.0.1:8027/", "").unwrap(),
            "ws://127.0.0.1:8027/_events"
        );
    }

    #[test]
    fn relay_url_forwards_topics() {
        assert_eq!(
            relay_url("ws://127.0.0.1:8027", "topics=sqs,s3").unwrap(),
            "ws://127.0.0.1:8027/_events?topics=sqs,s3"
        );
        // Other params are dropped; topics among them is still forwarded.
        assert_eq!(
            relay_url("ws://127.0.0.1:8027", "from=5&topics=mail").unwrap(),
            "ws://127.0.0.1:8027/_events?topics=mail"
        );
    }

    #[test]
    fn relay_url_none_when_unconfigured() {
        assert!(relay_url("", "topics=x").is_none());
    }

    #[test]
    fn query_param_extracts_value() {
        assert_eq!(
            query_param("a=1&topics=x,y&b=2", "topics"),
            Some("x,y".to_string())
        );
        assert_eq!(query_param("topics=", "topics"), Some(String::new()));
        assert_eq!(query_param("a=1", "topics"), None);
    }
}
