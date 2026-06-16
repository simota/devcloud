//! WebSocket event relay — Rust port of legacy `internal/eventsrelay`.
//!
//! Taps a single `tokio::sync::mpsc::UnboundedReceiver<String>` (each item is
//! a JSON object `{"type":..,"service":..,"payload":..}` emitted by a service
//! crate) and fans each event out to every connected WebSocket client over a
//! plain TCP listener on `addr`.
//!
//! ## Wire protocol
//!
//! Matches legacy `eventsrelay` frame-for-frame:
//!
//! | Frame | JSON |
//! |-------|------|
//! | on connect    | `{"type":"hello","seq":<current>}` |
//! | per event     | `{"type":"event","seq":<n>,"event":{..+timestamp..}}` |
//! | keepalive     | `{"type":"ping","seq":<current>}` |
//! | slow consumer | `{"type":"resync","seq":<n>,"reason":"slow-consumer"}` |
//!
//! Fields that would be zero/false/empty are omitted (`omitempty`-equivalent via
//! `#[serde(skip_serializing_if)]`).
//!
//! ## Public entry point
//!
//! ```ignore
//! pub async fn run(
//!     addr: String,
//!     events: tokio::sync::mpsc::UnboundedReceiver<String>,
//!     shutdown: impl std::future::Future<Output = ()> + Send + 'static,
//! ) -> Result<(), String>
//! ```

use std::collections::HashMap;
use std::collections::HashSet;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::SystemTime;

use futures_util::{SinkExt, StreamExt};
use serde::Serialize;
use tokio::net::TcpListener;
use tokio::sync::mpsc::{self, UnboundedReceiver};
use tokio::time::{self, Duration};
use tokio_tungstenite::tungstenite::handshake::server::{
    Request as WsRequest, Response as WsResponse,
};
use tokio_tungstenite::tungstenite::Message;

// ---------------------------------------------------------------------------
// Constants (mirror legacy eventsrelay)
// ---------------------------------------------------------------------------

/// Bounded send-queue per client. On overflow the event is dropped and the
/// client receives a resync frame before the next delivered event.
const CLIENT_QUEUE_SIZE: usize = 256;

/// Keepalive ping interval — mirrors legacy `pingInterval = 15 * time.Second`.
const PING_INTERVAL: Duration = Duration::from_secs(15);

// ---------------------------------------------------------------------------
// Frame (wire format)
// ---------------------------------------------------------------------------

/// A single JSON text frame sent to a relay client.
///
/// Serialisation matches legacy `json:",omitempty"` semantics: `event`, `dropped`,
/// and `reason` are skipped when absent/false/empty.
#[derive(Debug, Serialize)]
struct Frame {
    #[serde(rename = "type")]
    kind: &'static str,
    seq: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    event: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "is_false")]
    dropped: bool,
    #[serde(skip_serializing_if = "str::is_empty")]
    reason: &'static str,
}

fn is_false(b: &bool) -> bool {
    !b
}

impl Frame {
    fn hello(seq: u64) -> Self {
        Frame {
            kind: "hello",
            seq,
            event: None,
            dropped: false,
            reason: "",
        }
    }

    fn ping(seq: u64) -> Self {
        Frame {
            kind: "ping",
            seq,
            event: None,
            dropped: false,
            reason: "",
        }
    }

    fn event_frame(seq: u64, event: serde_json::Value) -> Self {
        Frame {
            kind: "event",
            seq,
            event: Some(event),
            dropped: false,
            reason: "",
        }
    }

    fn resync(seq: u64) -> Self {
        Frame {
            kind: "resync",
            seq,
            event: None,
            dropped: false,
            reason: "slow-consumer",
        }
    }
}

// ---------------------------------------------------------------------------
// SeqEvent — an event tagged with a relay-assigned monotonic sequence number
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct SeqEvent {
    seq: u64,
    /// Enriched event object: original fields + injected `timestamp`.
    event_json: serde_json::Value,
    /// Service name extracted from the event JSON (for topic filtering).
    service: String,
}

// ---------------------------------------------------------------------------
// Client
// ---------------------------------------------------------------------------

struct Client {
    /// Bounded async channel. The fanout loop tries to send without blocking;
    /// on full the try_send fails and the dropped flag is set.
    tx: mpsc::Sender<SeqEvent>,
    /// Set of allowed services; empty = all.
    topics: HashSet<String>,
    /// Set when an event was dropped; cleared by `take_dropped`.
    dropped: AtomicBool,
}

impl Client {
    fn new(tx: mpsc::Sender<SeqEvent>, topics: HashSet<String>) -> Arc<Self> {
        Arc::new(Client {
            tx,
            topics,
            dropped: AtomicBool::new(false),
        })
    }

    /// Non-blocking enqueue. Silently skips events that don't match the topic
    /// filter. Sets `dropped` on queue overflow (mirrors legacy client.enqueue).
    fn enqueue(&self, se: &SeqEvent) {
        if !self.topics.is_empty() && !self.topics.contains(&se.service) {
            return;
        }
        if self.tx.try_send(se.clone()).is_err() {
            self.dropped.store(true, Ordering::Relaxed);
        }
    }

    /// Atomically reports and clears the dropped flag (mirrors legacy takeDropped).
    fn take_dropped(&self) -> bool {
        self.dropped.swap(false, Ordering::AcqRel)
    }
}

// ---------------------------------------------------------------------------
// Shared relay state
// ---------------------------------------------------------------------------

struct RelayState {
    /// Process-monotonic sequence, shared across all clients.
    seq: AtomicU64,
    /// Connected clients keyed by an auto-incrementing ID.
    clients: Mutex<HashMap<u64, Arc<Client>>>,
    next_id: AtomicU64,
}

impl RelayState {
    fn new() -> Arc<Self> {
        Arc::new(RelayState {
            seq: AtomicU64::new(0),
            clients: Mutex::new(HashMap::new()),
            next_id: AtomicU64::new(0),
        })
    }

    fn add_client(&self, client: Arc<Client>) -> u64 {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        self.clients.lock().unwrap().insert(id, client);
        id
    }

    fn remove_client(&self, id: u64) {
        self.clients.lock().unwrap().remove(&id);
    }

    fn fanout(&self, se: &SeqEvent) {
        let clients = self.clients.lock().unwrap();
        for c in clients.values() {
            c.enqueue(se);
        }
    }
}

// ---------------------------------------------------------------------------
// Timestamp helper (no chrono dependency)
// ---------------------------------------------------------------------------

/// Returns an RFC3339 / ISO-8601 UTC timestamp string from `SystemTime`.
///
/// Produces the same layout as legacy `time.Time` JSON marshaling:
/// `"2006-01-02T15:04:05.999999Z"` (sub-second digits omitted when zero,
/// microseconds when non-zero). Matches what the legacy relay injects when it
/// wraps `events.Event`.
fn now_rfc3339() -> String {
    let now = SystemTime::now();
    let dur = now
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default();
    let secs = dur.as_secs();
    let nanos = dur.subsec_nanos();

    // Decompose Unix seconds into year/month/day/hour/min/sec (Gregorian UTC).
    let days_since_epoch = secs / 86400;
    let time_of_day = secs % 86400;
    let h = time_of_day / 3600;
    let m = (time_of_day % 3600) / 60;
    let s = time_of_day % 60;

    let (year, month, day) = days_to_ymd(days_since_epoch);

    if nanos == 0 {
        format!("{year:04}-{month:02}-{day:02}T{h:02}:{m:02}:{s:02}Z")
    } else {
        // Trim trailing zeros from nanoseconds, down to microsecond precision.
        let micros = nanos / 1000;
        let frac_str = format!("{micros:06}");
        let frac = frac_str.trim_end_matches('0');
        format!("{year:04}-{month:02}-{day:02}T{h:02}:{m:02}:{s:02}.{frac}Z")
    }
}

/// Converts days since Unix epoch (1970-01-01) to (year, month, day).
fn days_to_ymd(mut days: u64) -> (u64, u64, u64) {
    let mut year = 1970u64;
    loop {
        let dy = days_in_year(year);
        if days < dy {
            break;
        }
        days -= dy;
        year += 1;
    }
    let leap = is_leap(year);
    let month_days: &[u64] = if leap {
        &[31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    } else {
        &[31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    };
    let mut month = 1u64;
    for &md in month_days {
        if days < md {
            break;
        }
        days -= md;
        month += 1;
    }
    (year, month, days + 1)
}

fn days_in_year(y: u64) -> u64 {
    if is_leap(y) {
        366
    } else {
        365
    }
}

fn is_leap(y: u64) -> bool {
    (y % 4 == 0 && y % 100 != 0) || y % 400 == 0
}

// ---------------------------------------------------------------------------
// Event JSON enrichment
// ---------------------------------------------------------------------------

/// Parses the incoming event JSON string and injects a `"timestamp"` field if
/// absent (the source service crates omit it — the relay is the authority for
/// wire timestamps, mirroring legacy `events.Bus.Publish` behaviour).
///
/// Returns the enriched `serde_json::Value` plus the `service` string for topic
/// filtering.
fn enrich_event(raw: &str) -> Option<(serde_json::Value, String)> {
    let mut obj: serde_json::Value = serde_json::from_str(raw).ok()?;
    let map = obj.as_object_mut()?;

    // Inject timestamp when absent (matches legacy bus.Publish: "if zero, set now").
    if !map.contains_key("timestamp") {
        map.insert(
            "timestamp".to_string(),
            serde_json::Value::String(now_rfc3339()),
        );
    }

    let service = map
        .get("service")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();

    Some((obj, service))
}

// ---------------------------------------------------------------------------
// Topic parsing
// ---------------------------------------------------------------------------

/// Parses `?topics=a,b,c` into a `HashSet<String>`.
/// Returns an empty set (= all topics) when raw is blank or contains no tokens.
fn parse_topics(raw: &str) -> HashSet<String> {
    if raw.is_empty() {
        return HashSet::new();
    }
    raw.split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .collect()
}

/// Extracts the first value of `key` from a raw `a=b&c=d` query string.
fn query_param(query: &str, key: &str) -> String {
    for pair in query.split('&') {
        if let Some((k, v)) = pair.split_once('=') {
            if k == key {
                return v.to_string();
            }
        } else if pair == key {
            return String::new();
        }
    }
    String::new()
}

// ---------------------------------------------------------------------------
// Per-client serve loop
// ---------------------------------------------------------------------------

/// Drives a single WebSocket connection: sends hello, then relays events from
/// the client queue interleaved with keepalive pings.
///
/// Mirrors legacy `Relay.ServeHTTP` inner loop.
async fn serve_client(
    ws: tokio_tungstenite::WebSocketStream<tokio::net::TcpStream>,
    state: Arc<RelayState>,
    topics: HashSet<String>,
    mut shutdown_rx: tokio::sync::watch::Receiver<bool>,
) {
    let (tx, mut rx) = mpsc::channel::<SeqEvent>(CLIENT_QUEUE_SIZE);
    let client = Client::new(tx, topics);
    let client_id = state.add_client(Arc::clone(&client));

    let (mut ws_tx, mut ws_rx) = ws.split();

    // Send hello frame with the current high-water seq.
    let hello_seq = state.seq.load(Ordering::Relaxed);
    let hello_json = match serde_json::to_string(&Frame::hello(hello_seq)) {
        Ok(s) => s,
        Err(_) => {
            state.remove_client(client_id);
            return;
        }
    };
    if ws_tx.send(Message::Text(hello_json)).await.is_err() {
        state.remove_client(client_id);
        return;
    }

    let mut ping_interval = time::interval(PING_INTERVAL);
    // Skip the first immediate tick — we just sent the hello frame.
    ping_interval.tick().await;

    // Drain inbound frames so close frames are observed (mirrors legacy drain goroutine).
    let (client_closed_tx, mut client_closed_rx) = mpsc::channel::<()>(1);
    tokio::spawn(async move {
        while let Some(msg) = ws_rx.next().await {
            match msg {
                Ok(Message::Close(_)) | Err(_) => break,
                _ => {}
            }
        }
        let _ = client_closed_tx.send(()).await;
    });

    loop {
        tokio::select! {
            // Shutdown signal.
            _ = shutdown_rx.changed() => {
                if *shutdown_rx.borrow() {
                    let _ = ws_tx.close().await;
                    break;
                }
            }
            // Client disconnected.
            _ = client_closed_rx.recv() => {
                break;
            }
            // Keepalive ping.
            _ = ping_interval.tick() => {
                let seq = state.seq.load(Ordering::Relaxed);
                let json = match serde_json::to_string(&Frame::ping(seq)) {
                    Ok(s) => s,
                    Err(_) => break,
                };
                if ws_tx.send(Message::Text(json)).await.is_err() {
                    break;
                }
            }
            // Incoming event from fanout.
            se = rx.recv() => {
                let se = match se {
                    Some(s) => s,
                    None => break,
                };
                // Resync before the next real event if we dropped something.
                if client.take_dropped() {
                    let resync_json = match serde_json::to_string(&Frame::resync(se.seq)) {
                        Ok(s) => s,
                        Err(_) => break,
                    };
                    if ws_tx.send(Message::Text(resync_json)).await.is_err() {
                        break;
                    }
                }
                let frame_json = match serde_json::to_string(&Frame::event_frame(se.seq, se.event_json)) {
                    Ok(s) => s,
                    Err(_) => break,
                };
                if ws_tx.send(Message::Text(frame_json)).await.is_err() {
                    break;
                }
            }
        }
    }

    state.remove_client(client_id);
}

// ---------------------------------------------------------------------------
// Public entry point
// ---------------------------------------------------------------------------

/// Starts the WebSocket event relay on `addr`, consuming events from `events`
/// and broadcasting them to all connected clients until `shutdown` resolves.
///
/// Each `String` in `events` must be a JSON object of the form
/// `{"type":..,"service":..,"payload":..}`. A `"timestamp"` key is injected if
/// absent before forwarding.
///
/// Returns `Ok(())` on clean shutdown, `Err(String)` if the TCP listener cannot
/// be bound.
pub async fn run(
    addr: String,
    mut events: UnboundedReceiver<String>,
    shutdown: impl std::future::Future<Output = ()> + Send + 'static,
) -> Result<(), String> {
    let listener = TcpListener::bind(&addr)
        .await
        .map_err(|e| format!("event-relay: bind {addr}: {e}"))?;

    let state = RelayState::new();

    // Shutdown watch channel — broadcast `true` when the shutdown future resolves.
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

    {
        let tx = shutdown_tx.clone();
        tokio::spawn(async move {
            shutdown.await;
            let _ = tx.send(true);
        });
    }

    // Event reader loop: receives event JSON strings, enriches them with a
    // timestamp, assigns a monotonic seq, and fans out to all clients.
    {
        let state = Arc::clone(&state);
        let mut srx = shutdown_rx.clone();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = srx.changed() => {
                        if *srx.borrow() { break; }
                    }
                    raw = events.recv() => {
                        let raw = match raw {
                            Some(s) => s,
                            None => break,
                        };
                        let (event_json, service) = match enrich_event(&raw) {
                            Some(v) => v,
                            None => continue, // malformed JSON — skip silently
                        };
                        // fetch_add returns previous; +1 gives the new seq.
                        let seq = state.seq.fetch_add(1, Ordering::Relaxed) + 1;
                        let se = SeqEvent { seq, event_json, service };
                        state.fanout(&se);
                    }
                }
            }
        });
    }

    // Accept loop: upgrades each TCP connection to a WebSocket and spawns a
    // per-client serve task. Uses `accept_hdr_async` to extract `?topics=` from
    // the HTTP upgrade request during the handshake (mirrors legacy parseTopics
    // call inside ServeHTTP).
    let mut srx_accept = shutdown_rx.clone();
    loop {
        tokio::select! {
            _ = srx_accept.changed() => {
                if *srx_accept.borrow() { break; }
            }
            accepted = listener.accept() => {
                let (stream, _): (tokio::net::TcpStream, SocketAddr) = match accepted {
                    Ok(p) => p,
                    Err(_) => break,
                };

                // Capture the topics query param during the WS handshake via the
                // header callback (the only point where the raw HTTP request is
                // visible when using tokio-tungstenite).
                let topics_cell: Arc<Mutex<HashSet<String>>> =
                    Arc::new(Mutex::new(HashSet::new()));
                let topics_inner = Arc::clone(&topics_cell);

                let callback = move |req: &WsRequest, resp: WsResponse| {
                    let uri = req.uri();
                    let query = uri.query().unwrap_or("");
                    let raw_topics = query_param(query, "topics");
                    *topics_inner.lock().unwrap() = parse_topics(&raw_topics);
                    Ok(resp)
                };

                let ws = match tokio_tungstenite::accept_hdr_async(stream, callback).await {
                    Ok(ws) => ws,
                    Err(_) => continue, // bad handshake — skip
                };

                let topics = topics_cell.lock().unwrap().clone();
                let state_clone = Arc::clone(&state);
                let srx = shutdown_rx.clone();
                tokio::spawn(serve_client(ws, state_clone, topics, srx));
            }
        }
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_topics_empty_string_returns_all() {
        assert!(parse_topics("").is_empty());
    }

    #[test]
    fn parse_topics_single() {
        let t = parse_topics("s3");
        assert_eq!(t.len(), 1);
        assert!(t.contains("s3"));
    }

    #[test]
    fn parse_topics_multiple() {
        let t = parse_topics("s3,gcs, sqs ");
        assert!(t.contains("s3"));
        assert!(t.contains("gcs"));
        assert!(t.contains("sqs"));
        assert_eq!(t.len(), 3);
    }

    #[test]
    fn parse_topics_trailing_comma_ignored() {
        let t = parse_topics("s3,");
        assert_eq!(t.len(), 1);
        assert!(t.contains("s3"));
    }

    #[test]
    fn enrich_event_injects_timestamp() {
        let raw = r#"{"type":"created","service":"s3","payload":{"key":"foo"}}"#;
        let (val, svc) = enrich_event(raw).unwrap();
        assert_eq!(svc, "s3");
        assert!(val.get("timestamp").is_some());
        let ts = val["timestamp"].as_str().unwrap();
        assert!(ts.ends_with('Z'), "timestamp should be UTC: {ts}");
    }

    #[test]
    fn enrich_event_preserves_existing_timestamp() {
        let raw =
            r#"{"type":"x","service":"mail","timestamp":"2024-01-01T00:00:00Z","payload":{}}"#;
        let (val, _svc) = enrich_event(raw).unwrap();
        assert_eq!(val["timestamp"].as_str().unwrap(), "2024-01-01T00:00:00Z");
    }

    #[test]
    fn enrich_event_returns_none_on_invalid_json() {
        assert!(enrich_event("not json").is_none());
    }

    #[test]
    fn frame_hello_serializes_correctly() {
        let f = Frame::hello(42);
        let s = serde_json::to_string(&f).unwrap();
        let v: serde_json::Value = serde_json::from_str(&s).unwrap();
        assert_eq!(v["type"], "hello");
        assert_eq!(v["seq"], 42);
        // dropped=false and empty reason are omitted (omitempty-equivalent).
        assert!(v.get("dropped").is_none());
        assert!(v.get("event").is_none());
        assert!(v.get("reason").is_none());
    }

    #[test]
    fn frame_ping_no_event_no_reason() {
        let f = Frame::ping(7);
        let s = serde_json::to_string(&f).unwrap();
        let v: serde_json::Value = serde_json::from_str(&s).unwrap();
        assert_eq!(v["type"], "ping");
        assert_eq!(v["seq"], 7);
        assert!(v.get("event").is_none());
        assert!(v.get("reason").is_none());
    }

    #[test]
    fn frame_resync_has_reason() {
        let f = Frame::resync(99);
        let s = serde_json::to_string(&f).unwrap();
        let v: serde_json::Value = serde_json::from_str(&s).unwrap();
        assert_eq!(v["type"], "resync");
        assert_eq!(v["reason"], "slow-consumer");
        assert!(v.get("event").is_none());
    }

    #[test]
    fn frame_event_includes_event_object() {
        let event = serde_json::json!({
            "type": "created",
            "service": "s3",
            "timestamp": "2024-01-01T00:00:00Z",
            "payload": {}
        });
        let f = Frame::event_frame(3, event);
        let s = serde_json::to_string(&f).unwrap();
        let v: serde_json::Value = serde_json::from_str(&s).unwrap();
        assert_eq!(v["type"], "event");
        assert_eq!(v["seq"], 3);
        assert!(v.get("event").is_some());
        assert_eq!(v["event"]["service"], "s3");
    }

    #[test]
    fn client_enqueue_filters_by_topic() {
        let (tx, _rx) = mpsc::channel(8);
        let mut topics = HashSet::new();
        topics.insert("s3".to_string());
        let c = Client::new(tx, topics);

        let se_match = SeqEvent {
            seq: 1,
            event_json: serde_json::json!({}),
            service: "s3".to_string(),
        };
        let se_no_match = SeqEvent {
            seq: 2,
            event_json: serde_json::json!({}),
            service: "gcs".to_string(),
        };

        c.enqueue(&se_match);
        // gcs doesn't match topics filter — skipped, not a drop.
        c.enqueue(&se_no_match);
        assert!(!c.take_dropped());
    }

    #[test]
    fn client_take_dropped_clears_flag() {
        let (tx, _rx) = mpsc::channel(1); // capacity 1
        let c = Client::new(tx, HashSet::new());

        let se = SeqEvent {
            seq: 1,
            event_json: serde_json::json!({}),
            service: String::new(),
        };
        c.enqueue(&se); // fills the queue
        c.enqueue(&se); // overflow → dropped
        assert!(c.take_dropped());
        // Flag cleared after first take.
        assert!(!c.take_dropped());
    }

    #[test]
    fn now_rfc3339_looks_valid() {
        let ts = now_rfc3339();
        assert!(ts.ends_with('Z'), "should end with Z: {ts}");
        assert!(ts.contains('T'), "should contain T separator: {ts}");
        assert!(ts.starts_with("20"), "should start with 20xx year: {ts}");
    }

    #[test]
    fn query_param_extracts_value() {
        assert_eq!(query_param("a=1&topics=x,y&b=2", "topics"), "x,y");
        assert_eq!(query_param("topics=", "topics"), "");
        assert_eq!(query_param("a=1", "topics"), "");
    }
}
