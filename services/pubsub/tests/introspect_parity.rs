//! Parity tests for the read-only `/_introspect/` API (dashboard browse),
//! mirroring `internal/services/pubsub/introspect.rs` + `dashboard.rs`. The
//! snapshot field names/shape must match the legacy `Snapshot` / `MessageSnapshot`
//! structs so the Rust dashboard crate browses topics/subscriptions identically.

use std::collections::BTreeMap;

use devcloud_pubsub::http::{route, Request};
use devcloud_pubsub::model::{Subscription, Topic};
use devcloud_pubsub::server::{Config, Server};

const NOW: &str = "2026-05-30T12:00:00Z";

fn server(dir: &std::path::Path) -> Server {
    let mut s = Server::new(Config {
        project: "devcloud".to_string(),
        storage_path: dir.to_string_lossy().to_string(),
        ..Default::default()
    });
    s.set_fixed_now(NOW);
    s
}

fn get(path: &str) -> Request {
    Request {
        method: "GET".to_string(),
        path: path.to_string(),
        query: BTreeMap::new(),
        headers: BTreeMap::new(),
        body: Vec::new(),
    }
}

fn body_json(server: &mut Server, req: &Request) -> (u16, serde_json::Value) {
    let resp = route(server, req);
    let value = serde_json::from_slice(&resp.body).expect("valid json body");
    (resp.status, value)
}

fn tempdir() -> std::path::PathBuf {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let mut dir = std::env::temp_dir();
    dir.push(format!(
        "devcloud-ps-introspect-{}-{}",
        std::process::id(),
        n
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create tempdir");
    dir
}

/// Reproduces the dashboard golden fixture setup (parity-topic + parity-sub,
/// ackDeadline 30) and asserts the snapshot shape after timestamp normalization.
#[test]
fn snapshot_matches_dashboard_shape() {
    let dir = tempdir();
    let mut s = server(&dir);
    s.create_topic("devcloud", "parity-topic", &Topic::default())
        .expect("create topic");
    let sub = Subscription {
        topic: "projects/devcloud/topics/parity-topic".to_string(),
        ack_deadline_seconds: 30,
        ..Default::default()
    };
    s.create_subscription("devcloud", "parity-sub", &sub)
        .expect("create subscription");

    let (status, snap) = body_json(&mut s, &get("/_introspect/snapshot"));
    assert_eq!(status, 200);
    assert_eq!(snap["status"], "running");
    assert_eq!(snap["running"], true);
    assert_eq!(snap["project"], "devcloud");

    let topics = snap["topics"].as_array().expect("topics array");
    assert_eq!(topics.len(), 1);
    let topic = &topics[0];
    assert_eq!(topic["name"], "projects/devcloud/topics/parity-topic");
    assert_eq!(topic["subscriptionCount"], 1);
    assert!(topic.get("createdAt").is_some());
    assert!(topic.get("updatedAt").is_some());

    let subs = snap["subscriptions"].as_array().expect("subs array");
    assert_eq!(subs.len(), 1);
    let sub = &subs[0];
    assert_eq!(sub["name"], "projects/devcloud/subscriptions/parity-sub");
    assert_eq!(sub["topic"], "projects/devcloud/topics/parity-topic");
    assert_eq!(sub["ackDeadlineSeconds"], 30);
    assert_eq!(sub["backlogMessages"], 0);
    assert_eq!(sub["inFlightMessages"], 0);
    assert_eq!(sub["totalRetainedMessages"], 0);
    assert_eq!(sub["maxDeliveryAttemptSeen"], 0);
    // No payload content is ever exposed in the snapshot.
    assert!(sub.get("recentDeliveries").is_none());
}

#[test]
fn snapshot_is_empty_arrays_when_no_resources() {
    let dir = tempdir();
    let mut s = server(&dir);
    let (status, snap) = body_json(&mut s, &get("/_introspect/snapshot"));
    assert_eq!(status, 200);
    assert_eq!(snap["topics"].as_array().map(Vec::len), Some(0));
    assert_eq!(snap["subscriptions"].as_array().map(Vec::len), Some(0));
}

#[test]
fn missing_message_returns_404() {
    let dir = tempdir();
    let mut s = server(&dir);
    let (status, body) = body_json(&mut s, &get("/_introspect/messages/does-not-exist"));
    assert_eq!(status, 404);
    assert_eq!(body["error"]["status"], "NOT_FOUND");
    assert_eq!(body["error"]["message"], "message does not exist");
}

#[test]
fn unknown_introspect_endpoint_returns_404() {
    let dir = tempdir();
    let mut s = server(&dir);
    let (status, body) = body_json(&mut s, &get("/_introspect/bogus"));
    assert_eq!(status, 404);
    assert_eq!(body["error"]["status"], "NOT_FOUND");
    assert_eq!(body["error"]["message"], "introspection endpoint not found");
}

#[test]
fn non_get_is_method_not_allowed() {
    let dir = tempdir();
    let mut s = server(&dir);
    let mut req = get("/_introspect/snapshot");
    req.method = "POST".to_string();
    let resp = route(&mut s, &req);
    assert_eq!(resp.status, 405);
    assert_eq!(resp.allow.as_deref(), Some("GET"));
}

#[test]
fn message_snapshot_lists_pending_deliveries_without_payload() {
    let dir = tempdir();
    let mut s = server(&dir);
    s.create_topic("devcloud", "parity-topic", &Topic::default())
        .expect("create topic");
    let sub = Subscription {
        topic: "projects/devcloud/topics/parity-topic".to_string(),
        ack_deadline_seconds: 30,
        ..Default::default()
    };
    s.create_subscription("devcloud", "parity-sub", &sub)
        .expect("create subscription");
    // Publish one message: `data` is base64 "hello".
    let messages = vec![serde_json::json!({ "data": "aGVsbG8=" })];
    let resp = s
        .publish("devcloud", "parity-topic", &messages)
        .expect("publish");
    assert_eq!(resp.status, 200);
    let published: serde_json::Value = serde_json::from_slice(&resp.body).unwrap();
    let message_id = published["messageIds"][0].as_str().unwrap().to_string();

    let (status, snap) = body_json(&mut s, &get(&format!("/_introspect/messages/{message_id}")));
    assert_eq!(status, 200);
    assert_eq!(snap["messageId"], message_id);
    // The message snapshot exposes ids/bookkeeping only — never the payload.
    assert!(snap.get("data").is_none());
    let subs = snap["subscriptions"].as_array().expect("subscriptions");
    assert_eq!(subs.len(), 1);
    assert_eq!(
        subs[0]["subscription"],
        "projects/devcloud/subscriptions/parity-sub"
    );
    assert_eq!(subs[0]["state"], "backlog");
    assert!(subs[0].get("data").is_none());
}

#[test]
fn metrics_snapshot_reports_monitoring_counters_and_ages_without_payload() {
    let dir = tempdir();
    let mut s = server(&dir);
    s.create_topic("devcloud", "orders", &Topic::default())
        .expect("create topic");
    let sub1 = Subscription {
        topic: "projects/devcloud/topics/orders".to_string(),
        ack_deadline_seconds: 30,
        ..Default::default()
    };
    s.create_subscription("devcloud", "sub-active", &sub1)
        .expect("create sub-active");
    let sub2 = Subscription {
        topic: "projects/devcloud/topics/orders".to_string(),
        ack_deadline_seconds: 30,
        ..Default::default()
    };
    s.create_subscription("devcloud", "sub-detached", &sub2)
        .expect("create sub-detached");
    s.detach_subscription("devcloud", "sub-detached")
        .expect("detach sub-detached");

    let messages = vec![
        serde_json::json!({ "data": "aGVsbG8=" }),
        serde_json::json!({ "data": "d29ybGQ=" }),
    ];
    s.publish("devcloud", "orders", &messages).expect("publish");

    s.set_fixed_now("2026-05-30T12:00:25Z");
    s.pull("devcloud", "sub-active", 1).expect("pull one");

    let (status, metrics) = body_json(&mut s, &get("/_introspect/metrics"));
    assert_eq!(status, 200);
    assert_eq!(metrics["project"], "devcloud");
    assert_eq!(metrics["topicCount"], 1);
    assert_eq!(metrics["subscriptionCount"], 2);
    assert_eq!(metrics["snapshotCount"], 0);
    assert_eq!(metrics["schemaCount"], 0);
    assert_eq!(metrics["retainedMessages"], 2);
    assert_eq!(metrics["numUndeliveredMessages"], 1);
    assert_eq!(metrics["numInFlightMessages"], 1);
    assert_eq!(metrics["oldestUnackedMessageAgeSeconds"], 25);

    let topics = metrics["topics"].as_array().expect("topics");
    assert_eq!(topics.len(), 1);
    assert_eq!(topics[0]["name"], "projects/devcloud/topics/orders");
    assert_eq!(topics[0]["attachedSubscriptionCount"], 1);
    assert_eq!(topics[0]["detachedSubscriptionCount"], 1);
    assert_eq!(topics[0]["snapshotCount"], 0);
    assert_eq!(topics[0]["retainedMessageCount"], 2);
    assert_eq!(topics[0]["unackedMessageCount"], 2);

    let subs = metrics["subscriptions"].as_array().expect("subscriptions");
    assert_eq!(subs.len(), 2);
    assert_eq!(
        subs[0]["name"],
        "projects/devcloud/subscriptions/sub-active"
    );
    assert_eq!(subs[0]["detached"], false);
    assert_eq!(subs[0]["numUndeliveredMessages"], 1);
    assert_eq!(subs[0]["numInFlightMessages"], 1);
    assert_eq!(subs[0]["totalRetainedMessages"], 2);
    assert_eq!(subs[0]["oldestUnackedMessageAgeSeconds"], 25);
    assert_eq!(subs[0]["maxDeliveryAttemptSeen"], 1);
    assert!(subs[0].get("data").is_none());

    assert_eq!(
        subs[1]["name"],
        "projects/devcloud/subscriptions/sub-detached"
    );
    assert_eq!(subs[1]["detached"], true);
    assert_eq!(subs[1]["numUndeliveredMessages"], 0);
}

#[test]
fn quotas_snapshot_reports_effective_limits_and_usage() {
    let dir = tempdir();
    let mut s = Server::new(Config {
        project: "devcloud".to_string(),
        storage_path: dir.to_string_lossy().to_string(),
        default_ack_deadline_seconds: 15,
        max_ack_deadline_seconds: 300,
        max_pull_messages: 500,
        message_retention_seconds: 86_400,
        ..Default::default()
    });
    s.set_fixed_now(NOW);

    s.create_topic("devcloud", "t1", &Topic::default())
        .expect("create t1");
    let sub = Subscription {
        topic: "projects/devcloud/topics/t1".to_string(),
        ..Default::default()
    };
    s.create_subscription("devcloud", "s1", &sub)
        .expect("create s1");
    s.create_snapshot("devcloud", "snap1", "projects/devcloud/subscriptions/s1")
        .expect("create snap1");
    let messages = vec![serde_json::json!({ "data": "aGVsbG8=" })];
    s.publish("devcloud", "t1", &messages).expect("publish");

    let (status, quotas) = body_json(&mut s, &get("/_introspect/quotas"));
    assert_eq!(status, 200);
    assert_eq!(quotas["project"], "devcloud");
    assert_eq!(quotas["limits"]["defaultAckDeadlineSeconds"], 15);
    assert_eq!(quotas["limits"]["maxAckDeadlineSeconds"], 300);
    assert_eq!(quotas["limits"]["maxPullMessages"], 500);
    assert_eq!(quotas["limits"]["messageRetentionSeconds"], 86_400);
    assert_eq!(quotas["limits"]["maxTopicsPerProject"], 10_000);
    assert_eq!(quotas["limits"]["maxSubscriptionsPerProject"], 10_000);
    assert_eq!(quotas["limits"]["maxDeliveryAttemptsMin"], 5);
    assert_eq!(quotas["limits"]["maxDeliveryAttemptsMax"], 100);

    assert_eq!(quotas["usage"]["topics"], 1);
    assert_eq!(quotas["usage"]["subscriptions"], 1);
    assert_eq!(quotas["usage"]["snapshots"], 1);
    assert_eq!(quotas["usage"]["schemas"], 0);
    assert_eq!(quotas["usage"]["retainedMessages"], 1);
    assert_eq!(quotas["usage"]["unackedDeliveries"], 1);

    let entries = quotas["quotas"].as_array().expect("quotas array");
    assert_eq!(entries.len(), 6);
    assert_eq!(
        entries[0]["metric"],
        "pubsub.googleapis.com/topics_per_project"
    );
    assert_eq!(entries[0]["limit"], 10_000);
    assert_eq!(entries[0]["usage"], 1);
    assert_eq!(
        entries[4]["metric"],
        "pubsub.googleapis.com/max_pull_messages"
    );
    assert_eq!(entries[4]["limit"], 500);
}
