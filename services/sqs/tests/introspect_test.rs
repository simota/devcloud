//! Parity tests for the read-only introspection API
//! (`/_introspect/queues[...]`), asserting the routes and JSON shapes captured
//! from the legacy server (`internal/services/sqs/{introspect,dashboard}.rs`).
//!
//! `handle_introspect` serializes the body to bytes in struct field-declaration
//! order (matching legacy `encoding/json`). Content assertions parse those bytes
//! into a `serde_json::Value` (which sorts keys — fine for value equality),
//! while field-order assertions inspect the raw JSON string directly.

use devcloud_sqs::introspect::{handle_introspect, IntrospectOutcome};
use devcloud_sqs::{Config, Server};
use serde_json::{json, Value};

fn cfg() -> Config {
    Config {
        region: "us-east-1".to_string(),
        account_id: "000000000000".to_string(),
        queue_url_host: "127.0.0.1:9324".to_string(),
        ..Default::default()
    }
}

fn target(op: &str) -> String {
    format!("AmazonSQS.{op}")
}

fn create_queue(s: &mut Server, name: &str) {
    let out = s.dispatch_json(
        &target("CreateQueue"),
        serde_json::to_vec(&json!({ "QueueName": name }))
            .unwrap()
            .as_slice(),
    );
    assert_eq!(out.status, 200, "CreateQueue {name}");
}

fn send(s: &mut Server, url: &str, body: &str) {
    let out = s.dispatch_json(
        &target("SendMessage"),
        serde_json::to_vec(&json!({ "QueueUrl": url, "MessageBody": body }))
            .unwrap()
            .as_slice(),
    );
    assert_eq!(out.status, 200, "SendMessage");
}

/// Parses the serialized body into a `Value` for content assertions.
fn body(out: &IntrospectOutcome) -> Value {
    serde_json::from_slice(&out.body).expect("introspect body is valid JSON")
}

/// The serialized body as a UTF-8 string (for field-order assertions, which the
/// `Value` round-trip would lose by sorting keys).
fn body_str(out: &IntrospectOutcome) -> String {
    String::from_utf8(out.body.clone()).expect("introspect body is UTF-8")
}

#[test]
fn non_get_method_is_405_read_only() {
    let s = Server::new(cfg());
    let out = handle_introspect(&s, "POST", "/_introspect/queues");
    assert_eq!(out.status, 405);
    assert!(out.allow_get, "405 must carry Allow: GET");
    assert_eq!(
        body(&out),
        json!({ "__type": "InvalidAction", "message": "introspection endpoints are read-only" })
    );
}

#[test]
fn unknown_subpath_is_404() {
    let s = Server::new(cfg());
    for path in [
        "/_introspect/",
        "/_introspect/bogus",
        "/_introspect/queues/",
        "/_introspect/queues/Orders/bogus",
    ] {
        let out = handle_introspect(&s, "GET", path);
        assert_eq!(out.status, 404, "path {path}");
        assert!(!out.allow_get, "404 must not carry Allow: GET");
        assert_eq!(body(&out)["__type"], json!("InvalidAddress"), "path {path}");
    }
}

#[test]
fn snapshot_shape_and_counts() {
    let mut s = Server::new(cfg());
    create_queue(&mut s, "Orders");
    let url = "http://127.0.0.1:9324/000000000000/Orders";
    send(&mut s, url, "hello");

    let out = handle_introspect(&s, "GET", "/_introspect/queues");
    assert_eq!(out.status, 200);
    let value = body(&out);
    assert_eq!(value["status"], json!("running"));
    assert_eq!(value["running"], json!(true));
    assert_eq!(value["region"], json!("us-east-1"));

    let queues = value["queues"].as_array().expect("queues array");
    assert_eq!(queues.len(), 1);
    let q = &queues[0];
    assert_eq!(q["name"], json!("Orders"));
    assert_eq!(q["url"], json!(url));
    assert_eq!(q["arn"], json!("arn:aws:sqs:us-east-1:000000000000:Orders"));
    assert_eq!(q["visibleMessages"], json!(1));
    assert_eq!(q["notVisibleMessages"], json!(0));
    assert_eq!(q["delayedMessages"], json!(0));
    assert_eq!(q["totalRetainedMessages"], json!(1));
    // Default attributes are present; `tags` is omitted when empty.
    assert!(q.get("attributes").is_some());
    assert!(q.get("tags").is_none(), "empty tags must be omitted");
    assert!(q.get("createdAt").is_some());

    // Field order matches the legacy QueueSnapshot declaration order. Asserted on
    // the raw JSON string, since a Value round-trip would re-sort the keys.
    let raw = body_str(&out);
    let order = [
        "\"name\"",
        "\"url\"",
        "\"arn\"",
        "\"attributes\"",
        "\"createdAt\"",
        "\"visibleMessages\"",
        "\"notVisibleMessages\"",
        "\"delayedMessages\"",
        "\"totalRetainedMessages\"",
    ];
    assert_keys_in_order(&raw, &order);
}

#[test]
fn queue_detail_messages_and_state() {
    let mut s = Server::new(cfg());
    create_queue(&mut s, "Orders");
    let url = "http://127.0.0.1:9324/000000000000/Orders";
    send(&mut s, url, "hello");

    let out = handle_introspect(&s, "GET", "/_introspect/queues/Orders");
    assert_eq!(out.status, 200);
    let value = body(&out);
    assert_eq!(value["queue"]["name"], json!("Orders"));
    let messages = value["messages"].as_array().expect("messages array");
    assert_eq!(messages.len(), 1);
    let m = &messages[0];
    assert_eq!(m["body"], json!("hello"));
    assert_eq!(m["state"], json!("visible"));
    assert_eq!(m["receiveCount"], json!(0));
    // A freshly-sent, unreceived message has no lease and no firstReceiveAt.
    assert!(m.get("firstReceiveAt").is_none());
    // invisibleUntil is always present (legacy time.Time omitempty keeps the zero).
    assert_eq!(m["invisibleUntil"], json!("0001-01-01T00:00:00Z"));
    assert_eq!(value["leases"], json!([]));

    // Message field order matches the legacy MessageSnapshot declaration order.
    let raw = body_str(&out);
    assert_keys_in_order(
        &raw,
        &[
            "\"messageId\"",
            "\"body\"",
            "\"md5OfMessageBody\"",
            "\"sentAt\"",
            "\"availableAt\"",
            "\"invisibleUntil\"",
            "\"receiveCount\"",
            "\"state\"",
        ],
    );

    // Missing queue → 404 NonExistentQueue.
    let out = handle_introspect(&s, "GET", "/_introspect/queues/Nope");
    assert_eq!(out.status, 404);
    assert_eq!(
        body(&out)["__type"],
        json!("AWS.SimpleQueueService.NonExistentQueue")
    );
}

#[test]
fn dead_letter_snapshot_links_source_and_target() {
    let mut s = Server::new(cfg());
    create_queue(&mut s, "DLQ");
    create_queue(&mut s, "Orders");
    let orders_url = "http://127.0.0.1:9324/000000000000/Orders";
    let dlq_arn = "arn:aws:sqs:us-east-1:000000000000:DLQ";
    // Attach a redrive policy on Orders pointing at DLQ.
    let policy = json!({ "deadLetterTargetArn": dlq_arn, "maxReceiveCount": 3 }).to_string();
    let out = s.dispatch_json(
        &target("SetQueueAttributes"),
        serde_json::to_vec(&json!({
            "QueueUrl": orders_url,
            "Attributes": { "RedrivePolicy": policy },
        }))
        .unwrap()
        .as_slice(),
    );
    assert_eq!(out.status, 200, "SetQueueAttributes");

    // From the source queue: deadLetterQueue is DLQ, no source queues.
    let out = handle_introspect(&s, "GET", "/_introspect/queues/Orders/dlq");
    assert_eq!(out.status, 200);
    let value = body(&out);
    assert_eq!(value["deadLetterQueue"]["name"], json!("DLQ"));
    assert_eq!(value["deadLetterSourceQueues"], json!([]));

    // From the DLQ: no deadLetterQueue field, Orders listed as a source.
    let out = handle_introspect(&s, "GET", "/_introspect/queues/DLQ/dlq");
    assert_eq!(out.status, 200);
    let value = body(&out);
    assert!(
        value.get("deadLetterQueue").is_none(),
        "DLQ has no upstream dead-letter queue"
    );
    let sources = value["deadLetterSourceQueues"].as_array().unwrap();
    assert_eq!(sources.len(), 1);
    assert_eq!(sources[0]["name"], json!("Orders"));

    // dlq for a missing queue → 404.
    let out = handle_introspect(&s, "GET", "/_introspect/queues/Nope/dlq");
    assert_eq!(out.status, 404);
}

/// Asserts the given quoted key tokens appear in the JSON string in the given
/// order (each strictly after the previous).
fn assert_keys_in_order(raw: &str, keys: &[&str]) {
    let mut cursor = 0usize;
    for key in keys {
        let pos = raw[cursor..]
            .find(key)
            .unwrap_or_else(|| panic!("missing key {key} after offset {cursor} in {raw}"));
        cursor += pos + key.len();
    }
}
