//! Differential-parity tests for message operations (Publish / Pull /
//! Acknowledge / ModifyAckDeadline + lease expiry / redelivery) against golden
//! oracles captured from the Go Pub/Sub REST service.

use devcloud_pubsub::model::Subscription;
use devcloud_pubsub::server::{Config, Server};
use serde_json::Value;

const NOW: &str = "2026-05-30T12:00:00Z";

fn server(dir: &std::path::Path, msg_dir: &std::path::Path) -> Server {
    let mut s = Server::new(Config {
        project: "devcloud".to_string(),
        storage_path: dir.to_string_lossy().to_string(),
        message_storage_path: msg_dir.to_string_lossy().to_string(),
        default_ack_deadline_seconds: 10,
        max_ack_deadline_seconds: 600,
        max_pull_messages: 1000,
        message_retention_seconds: 604_800,
        ..Default::default()
    });
    s.set_fixed_now(NOW);
    s.create_topic("devcloud", "orders", &Default::default())
        .expect("orders");
    s.create_subscription(
        "devcloud",
        "sub1",
        &serde_json::from_str::<Subscription>(r#"{"topic":"projects/devcloud/topics/orders"}"#)
            .unwrap(),
    )
    .expect("sub1");
    s
}

fn matches(got: &[u8], fixture: &[u8], label: &str) {
    assert_eq!(
        String::from_utf8_lossy(got),
        String::from_utf8_lossy(fixture),
        "{label}"
    );
}

fn msgs(json: &str) -> Vec<Value> {
    serde_json::from_str::<Value>(json).unwrap()["messages"]
        .as_array()
        .unwrap()
        .clone()
}

#[test]
fn publish_matches_oracle() {
    let (dir, md) = (tempdir(), tempdir());
    let mut s = server(&dir, &md);
    let resp = s
        .publish(
            "devcloud",
            "orders",
            &msgs(
                r#"{"messages":[{"data":"aGVsbG8=","attributes":{"k":"v"}},{"data":"d29ybGQ="}]}"#,
            ),
        )
        .expect("publish");
    matches(
        &resp.body,
        include_bytes!("fixtures/m_publish.json"),
        "m_publish",
    );
}

#[test]
fn publish_errors() {
    let (dir, md) = (tempdir(), tempdir());
    let mut s = server(&dir, &md);
    assert_eq!(
        s.publish("devcloud", "orders", &[])
            .expect_err("empty")
            .message,
        "messages are required"
    );
    assert_eq!(
        s.publish(
            "devcloud",
            "orders",
            &msgs(r#"{"messages":[{"data":"!!!"}]}"#)
        )
        .expect_err("badb64")
        .message,
        "message data must be base64 encoded"
    );
    let err = s
        .publish(
            "devcloud",
            "nope",
            &msgs(r#"{"messages":[{"data":"aGk="}]}"#),
        )
        .expect_err("notopic");
    assert_eq!(err.status, 404);
    assert_eq!(err.message, "topic not found");
}

#[test]
fn pull_matches_oracle_and_holds_lease() {
    let (dir, md) = (tempdir(), tempdir());
    let mut s = server(&dir, &md);
    s.publish(
        "devcloud",
        "orders",
        &msgs(r#"{"messages":[{"data":"aGVsbG8=","attributes":{"k":"v"}},{"data":"d29ybGQ="}]}"#),
    )
    .expect("publish");

    let resp = s.pull("devcloud", "sub1", 10).expect("pull");
    matches(&resp.body, include_bytes!("fixtures/m_pull.json"), "m_pull");

    // Second pull immediately: all leased, empty.
    let resp = s.pull("devcloud", "sub1", 10).expect("pull2");
    assert_eq!(String::from_utf8_lossy(&resp.body), "{}\n");

    // pubsub.json holds the leases.
    let on_disk = std::fs::read(md.join("pubsub.json")).expect("read");
    matches(
        &on_disk,
        include_bytes!("fixtures/m_state_pulled.json"),
        "m_state_pulled",
    );
}

#[test]
fn lease_expiry_redelivers() {
    let (dir, md) = (tempdir(), tempdir());
    let mut s = server(&dir, &md);
    s.publish(
        "devcloud",
        "orders",
        &msgs(r#"{"messages":[{"data":"aGVsbG8="}]}"#),
    )
    .expect("publish");

    let first = s.pull("devcloud", "sub1", 10).expect("pull1");
    matches(
        &first.body,
        include_bytes!("fixtures/ml_pull.json"),
        "ml_pull",
    );

    // Advance 15s: lease (10s) expired → redelivered with attempt 2, ackId 1-2.
    s.set_fixed_now("2026-05-30T12:00:15Z");
    let redelivered = s.pull("devcloud", "sub1", 10).expect("pull2");
    matches(
        &redelivered.body,
        include_bytes!("fixtures/ml_pull_redeliver.json"),
        "ml_pull_redeliver",
    );

    let on_disk = std::fs::read(md.join("pubsub.json")).expect("read");
    matches(
        &on_disk,
        include_bytes!("fixtures/ml_state.json"),
        "ml_state",
    );
}

#[test]
fn acknowledge_removes_delivery() {
    let (dir, md) = (tempdir(), tempdir());
    let mut s = server(&dir, &md);
    s.publish(
        "devcloud",
        "orders",
        &msgs(r#"{"messages":[{"data":"aGk="}]}"#),
    )
    .expect("publish");
    let pulled: Value =
        serde_json::from_slice(&s.pull("devcloud", "sub1", 10).expect("pull").body).unwrap();
    let ack_id = pulled["receivedMessages"][0]["ackId"]
        .as_str()
        .unwrap()
        .to_string();

    let resp = s.acknowledge("devcloud", "sub1", &[ack_id]).expect("ack");
    assert_eq!(String::from_utf8_lossy(&resp.body), "{}\n");

    // After ack, the unreferenced message is cleaned up; next pull is empty.
    s.set_fixed_now("2026-05-30T12:00:30Z");
    let resp = s.pull("devcloud", "sub1", 10).expect("pull2");
    assert_eq!(String::from_utf8_lossy(&resp.body), "{}\n");
}

#[test]
fn modify_ack_deadline_extends_lease() {
    let (dir, md) = (tempdir(), tempdir());
    let mut s = server(&dir, &md);
    s.publish(
        "devcloud",
        "orders",
        &msgs(r#"{"messages":[{"data":"aGk="}]}"#),
    )
    .expect("publish");
    let pulled: Value =
        serde_json::from_slice(&s.pull("devcloud", "sub1", 10).expect("pull").body).unwrap();
    let ack_id = pulled["receivedMessages"][0]["ackId"]
        .as_str()
        .unwrap()
        .to_string();

    // Extend deadline to 60s.
    s.modify_ack_deadline("devcloud", "sub1", std::slice::from_ref(&ack_id), 60)
        .expect("modack");

    // At +30s the lease (now 60s) is still held → empty pull.
    s.set_fixed_now("2026-05-30T12:00:30Z");
    let resp = s.pull("devcloud", "sub1", 10).expect("pull2");
    assert_eq!(String::from_utf8_lossy(&resp.body), "{}\n");

    // modifyAckDeadline=0 releases immediately.
    s.set_fixed_now(NOW);
    s.modify_ack_deadline("devcloud", "sub1", &[ack_id], 0)
        .expect("release");
    // Now redelivered (attempt 2).
    let resp: Value =
        serde_json::from_slice(&s.pull("devcloud", "sub1", 10).expect("pull3").body).unwrap();
    assert_eq!(resp["receivedMessages"][0]["deliveryAttempt"], 2);
}

#[test]
fn ack_validation() {
    let (dir, md) = (tempdir(), tempdir());
    let mut s = server(&dir, &md);
    // Empty ackIds → ok empty body.
    let resp = s.acknowledge("devcloud", "sub1", &[]).expect("empty");
    assert_eq!(String::from_utf8_lossy(&resp.body), "{}\n");
    // Empty value rejected.
    assert_eq!(
        s.acknowledge("devcloud", "sub1", &["".to_string()])
            .expect_err("empty val")
            .message,
        "ackIds must not contain empty values"
    );
    // modifyAckDeadline over max.
    assert_eq!(
        s.modify_ack_deadline("devcloud", "sub1", &["x".to_string()], 700)
            .expect_err("over")
            .message,
        "ackDeadlineSeconds exceeds maxAckDeadlineSeconds"
    );
}

#[test]
fn pull_detached_and_push_rejected() {
    let (dir, md) = (tempdir(), tempdir());
    let mut s = server(&dir, &md);
    // Detached.
    s.detach_subscription("devcloud", "sub1").expect("detach");
    assert_eq!(
        s.pull("devcloud", "sub1", 1).expect_err("detached").message,
        "subscription is detached"
    );
    // Push-configured.
    s.create_subscription(
        "devcloud",
        "sub2",
        &serde_json::from_str::<Subscription>(
            r#"{"topic":"projects/devcloud/topics/orders","pushConfig":{"pushEndpoint":"https://x.com/p"}}"#,
        )
        .unwrap(),
    )
    .expect("sub2");
    assert_eq!(
        s.pull("devcloud", "sub2", 1).expect_err("push").message,
        "subscription is configured for push delivery"
    );
}

// --- minimal tempdir -------------------------------------------------------

fn tempdir() -> std::path::PathBuf {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let mut dir = std::env::temp_dir();
    dir.push(format!("devcloud-ps-msg-{}-{}", std::process::id(), n));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create tempdir");
    dir
}
