//! Differential-parity tests for subscription operations against golden oracles
//! captured from the legacy Pub/Sub REST service.

use devcloud_pubsub::model::{Subscription, Topic};
use devcloud_pubsub::patch::decode_subscription_patch;
use devcloud_pubsub::server::{Config, Server};

const NOW: &str = "2026-05-30T12:00:00Z";
const NOW_PLUS_H: &str = "2026-05-30T13:00:00Z";

fn server(dir: &std::path::Path) -> Server {
    let mut s = Server::new(Config {
        project: "devcloud".to_string(),
        storage_path: dir.to_string_lossy().to_string(),
        ..Default::default()
    });
    s.set_fixed_now(NOW);
    // Seed topics.
    s.create_topic("devcloud", "orders", &Topic::default())
        .expect("orders");
    s.create_topic("devcloud", "dlq", &Topic::default())
        .expect("dlq");
    s
}

fn sub_from(json: &str) -> Subscription {
    serde_json::from_str(json).unwrap()
}

fn matches(got: &[u8], fixture: &[u8], label: &str) {
    assert_eq!(
        String::from_utf8_lossy(got),
        String::from_utf8_lossy(fixture),
        "{label}"
    );
}

const FULL_SUB: &str = r#"{"topic":"projects/devcloud/topics/orders","ackDeadlineSeconds":30,"labels":{"env":"prod"},"filter":"attributes.region = \"us\"","retryPolicy":{"minimumBackoff":"10s","maximumBackoff":"600s"},"deadLetterPolicy":{"deadLetterTopic":"projects/devcloud/topics/dlq","maxDeliveryAttempts":5}}"#;

#[test]
fn create_get_subscription_matches_oracle() {
    let dir = tempdir();
    let mut s = server(&dir);
    let resp = s
        .create_subscription("devcloud", "sub1", &sub_from(FULL_SUB))
        .expect("create");
    matches(
        &resp.body,
        include_bytes!("fixtures/s_create.json"),
        "s_create",
    );

    let got = s.get_subscription("devcloud", "sub1").expect("get");
    matches(&got.body, include_bytes!("fixtures/s_get.json"), "s_get");
}

#[test]
fn create_default_ack_deadline() {
    let dir = tempdir();
    let mut s = server(&dir);
    let resp = s
        .create_subscription(
            "devcloud",
            "sub2",
            &sub_from(r#"{"topic":"projects/devcloud/topics/orders"}"#),
        )
        .expect("create");
    matches(
        &resp.body,
        include_bytes!("fixtures/s_create_default.json"),
        "s_create_default",
    );
}

#[test]
fn create_errors_match_oracle() {
    let dir = tempdir();
    let mut s = server(&dir);
    assert_eq!(
        s.create_subscription("devcloud", "notopic", &Subscription::default())
            .expect_err("notopic")
            .message,
        "subscription topic is required"
    );
    assert_eq!(
        s.create_subscription("devcloud", "badtopic", &sub_from(r#"{"topic":"bad"}"#))
            .expect_err("badtopic")
            .message,
        "invalid topic name"
    );
    let err = s
        .create_subscription(
            "devcloud",
            "missingtopic",
            &sub_from(r#"{"topic":"projects/devcloud/topics/nope"}"#),
        )
        .expect_err("missing");
    assert_eq!(err.status, 404);
    assert_eq!(err.message, "topic not found");
    assert_eq!(
        s.create_subscription(
            "devcloud",
            "badfilter",
            &sub_from(r#"{"topic":"projects/devcloud/topics/orders","filter":"bogus"}"#)
        )
        .expect_err("badfilter")
        .message,
        "unsupported subscription filter"
    );
    assert_eq!(
        s.create_subscription(
            "devcloud",
            "baddlq",
            &sub_from(r#"{"topic":"projects/devcloud/topics/orders","deadLetterPolicy":{"deadLetterTopic":"projects/devcloud/topics/dlq","maxDeliveryAttempts":2}}"#)
        )
        .expect_err("baddlq")
        .message,
        "deadLetterPolicy.maxDeliveryAttempts must be between 5 and 100"
    );
    // Duplicate.
    s.create_subscription(
        "devcloud",
        "dup",
        &sub_from(r#"{"topic":"projects/devcloud/topics/orders"}"#),
    )
    .expect("first");
    let dup = s
        .create_subscription(
            "devcloud",
            "dup",
            &sub_from(r#"{"topic":"projects/devcloud/topics/orders"}"#),
        )
        .expect_err("dup");
    assert_eq!(dup.status, 409);
    assert_eq!(dup.message, "subscription already exists");
}

#[test]
fn patch_subscription_matches_oracle() {
    let dir = tempdir();
    let mut s = server(&dir);
    s.create_subscription("devcloud", "sub1", &sub_from(FULL_SUB))
        .expect("create");
    s.set_fixed_now(NOW_PLUS_H);
    let p = decode_subscription_patch(
        br#"{"labels":{"team":"core"},"ackDeadlineSeconds":45}"#,
        "labels,ackDeadlineSeconds",
    )
    .unwrap();
    let resp = s
        .patch_subscription("devcloud", "sub1", &p.subscription, &p.fields)
        .expect("patch");
    matches(
        &resp.body,
        include_bytes!("fixtures/s_patch.json"),
        "s_patch",
    );

    // Topic change rejected.
    let p =
        decode_subscription_patch(br#"{"topic":"projects/devcloud/topics/dlq"}"#, "topic").unwrap();
    let err = s
        .patch_subscription("devcloud", "sub1", &p.subscription, &p.fields)
        .expect_err("topic change");
    assert_eq!(err.status, 400);
    assert_eq!(err.message, "subscription topic cannot be changed");
}

#[test]
fn modify_push_config_and_detach() {
    let dir = tempdir();
    let mut s = server(&dir);
    s.create_subscription(
        "devcloud",
        "sub2",
        &sub_from(r#"{"topic":"projects/devcloud/topics/orders"}"#),
    )
    .expect("create");
    s.set_fixed_now(NOW_PLUS_H);
    let cfg = serde_json::json!({"pushEndpoint": "https://example.com/push"});
    let resp = s
        .modify_push_config("devcloud", "sub2", Some(&cfg))
        .expect("modpush");
    assert_eq!(String::from_utf8_lossy(&resp.body), "{}\n");

    let bad = serde_json::json!({"pushEndpoint": "ftp://bad"});
    assert_eq!(
        s.modify_push_config("devcloud", "sub2", Some(&bad))
            .expect_err("bad")
            .message,
        "pushConfig.pushEndpoint must be an http or https URL"
    );

    let resp = s.detach_subscription("devcloud", "sub2").expect("detach");
    assert_eq!(String::from_utf8_lossy(&resp.body), "{}\n");
    let got = s.get_subscription("devcloud", "sub2").expect("get");
    let parsed: serde_json::Value = serde_json::from_slice(&got.body).unwrap();
    assert_eq!(parsed["detached"], true);
    assert_eq!(
        parsed["pushConfig"]["pushEndpoint"],
        "https://example.com/push"
    );
}

#[test]
fn list_subscriptions_matches_oracle() {
    // Reproduce the oracle scenario: dupsub, sub1 (patched), sub2 (push+detach).
    let dir = tempdir();
    let mut s = server(&dir);
    s.create_subscription("devcloud", "sub1", &sub_from(FULL_SUB))
        .expect("sub1");
    s.create_subscription(
        "devcloud",
        "sub2",
        &sub_from(r#"{"topic":"projects/devcloud/topics/orders"}"#),
    )
    .expect("sub2");
    s.create_subscription(
        "devcloud",
        "dupsub",
        &sub_from(r#"{"topic":"projects/devcloud/topics/orders"}"#),
    )
    .expect("dupsub");
    s.set_fixed_now(NOW_PLUS_H);
    let p = decode_subscription_patch(
        br#"{"labels":{"team":"core"},"ackDeadlineSeconds":45}"#,
        "labels,ackDeadlineSeconds",
    )
    .unwrap();
    s.patch_subscription("devcloud", "sub1", &p.subscription, &p.fields)
        .expect("patch");
    let cfg = serde_json::json!({"pushEndpoint": "https://example.com/push"});
    s.modify_push_config("devcloud", "sub2", Some(&cfg))
        .expect("push");
    s.detach_subscription("devcloud", "sub2").expect("detach");

    let resp = s.list_subscriptions("devcloud", 0, 0).expect("list");
    matches(&resp.body, include_bytes!("fixtures/s_list.json"), "s_list");
}

#[test]
fn delete_subscription() {
    let dir = tempdir();
    let mut s = server(&dir);
    s.create_subscription(
        "devcloud",
        "sub1",
        &sub_from(r#"{"topic":"projects/devcloud/topics/orders"}"#),
    )
    .expect("create");
    let resp = s.delete_subscription("devcloud", "sub1").expect("delete");
    assert_eq!(resp.status, 204);
    assert!(resp.body.is_empty());
    let err = s.delete_subscription("devcloud", "sub1").expect_err("gone");
    assert_eq!(err.status, 404);
    assert_eq!(err.message, "subscription not found");
}

#[test]
fn full_scenario_state_matches_oracle() {
    let dir = tempdir();
    let mut s = server(&dir);
    // The oracle leaves: sub1 (patched), sub2 (push+detached). dupsub deleted.
    s.create_subscription("devcloud", "sub1", &sub_from(FULL_SUB))
        .expect("sub1");
    s.create_subscription(
        "devcloud",
        "sub2",
        &sub_from(r#"{"topic":"projects/devcloud/topics/orders"}"#),
    )
    .expect("sub2");
    s.create_subscription(
        "devcloud",
        "dupsub",
        &sub_from(r#"{"topic":"projects/devcloud/topics/orders"}"#),
    )
    .expect("dupsub");
    s.set_fixed_now(NOW_PLUS_H);
    let p = decode_subscription_patch(
        br#"{"labels":{"team":"core"},"ackDeadlineSeconds":45}"#,
        "labels,ackDeadlineSeconds",
    )
    .unwrap();
    s.patch_subscription("devcloud", "sub1", &p.subscription, &p.fields)
        .expect("patch");
    let cfg = serde_json::json!({"pushEndpoint": "https://example.com/push"});
    s.modify_push_config("devcloud", "sub2", Some(&cfg))
        .expect("push");
    s.detach_subscription("devcloud", "sub2").expect("detach");
    s.delete_subscription("devcloud", "dupsub").expect("delete");

    let on_disk = std::fs::read(dir.join("resources.json")).expect("read");
    matches(
        &on_disk,
        include_bytes!("fixtures/s_resources.json"),
        "s_resources",
    );
}

// --- minimal tempdir -------------------------------------------------------

fn tempdir() -> std::path::PathBuf {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let mut dir = std::env::temp_dir();
    dir.push(format!("devcloud-ps-sub-{}-{}", std::process::id(), n));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create tempdir");
    dir
}
