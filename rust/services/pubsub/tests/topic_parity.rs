//! Differential-parity tests for topic operations against golden oracles
//! captured from the Go Pub/Sub REST service.

use devcloud_pubsub::model::Topic;
use devcloud_pubsub::patch::decode_topic_patch;
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
    s
}

fn matches(got: &[u8], fixture: &[u8], label: &str) {
    assert_eq!(
        String::from_utf8_lossy(got),
        String::from_utf8_lossy(fixture),
        "{label}"
    );
}

fn topic_from(json: &str) -> Topic {
    serde_json::from_str(json).unwrap()
}

#[test]
fn create_get_topic_matches_oracle() {
    let dir = tempdir();
    let mut s = server(&dir);
    let resp = s
        .create_topic(
            "devcloud",
            "orders",
            &topic_from(r#"{"labels":{"env":"prod"},"messageRetentionDuration":"3600s"}"#),
        )
        .expect("create");
    assert_eq!(resp.status, 200);
    matches(
        &resp.body,
        include_bytes!("fixtures/t_create.json"),
        "t_create",
    );

    let got = s.get_topic("devcloud", "orders").expect("get");
    matches(&got.body, include_bytes!("fixtures/t_get.json"), "t_get");
}

#[test]
fn create_errors_match_oracle() {
    let dir = tempdir();
    let mut s = server(&dir);
    assert_eq!(
        s.create_topic(
            "devcloud",
            "bad",
            &topic_from(r#"{"name":"projects/devcloud/topics/other"}"#)
        )
        .expect_err("badname")
        .message,
        "topic name does not match request path"
    );
    assert_eq!(
        s.create_topic(
            "devcloud",
            "baddur",
            &topic_from(r#"{"messageRetentionDuration":"abc"}"#)
        )
        .expect_err("baddur")
        .message,
        "messageRetentionDuration must be a non-negative duration"
    );
    s.create_topic("devcloud", "orders", &Topic::default())
        .expect("first");
    let dup = s
        .create_topic("devcloud", "orders", &Topic::default())
        .expect_err("dup");
    assert_eq!(dup.status, 409);
    assert_eq!(dup.message, "topic already exists");
}

#[test]
fn get_missing_topic() {
    let dir = tempdir();
    let s = server(&dir);
    let err = s.get_topic("devcloud", "missing").expect_err("missing");
    assert_eq!(err.status, 404);
    assert_eq!(err.message, "topic not found");
}

#[test]
fn patch_topic_matches_oracle() {
    let dir = tempdir();
    let mut s = server(&dir);
    s.create_topic(
        "devcloud",
        "orders",
        &topic_from(r#"{"labels":{"env":"prod"},"messageRetentionDuration":"3600s"}"#),
    )
    .expect("create");

    // Patch labels via updateMask query.
    s.set_fixed_now(NOW_PLUS_H);
    let patch = decode_topic_patch(br#"{"labels":{"team":"core"}}"#, "labels").unwrap();
    let resp = s
        .patch_topic("devcloud", "orders", &patch.topic, &patch.fields)
        .expect("patch");
    matches(
        &resp.body,
        include_bytes!("fixtures/t_patch.json"),
        "t_patch",
    );

    // Patch wrapped body + body updateMask (kmsKeyName).
    let patch = decode_topic_patch(
        br#"{"topic":{"kmsKeyName":"k1"},"updateMask":"kmsKeyName"}"#,
        "",
    )
    .unwrap();
    let resp = s
        .patch_topic("devcloud", "orders", &patch.topic, &patch.fields)
        .expect("patch wrapped");
    matches(
        &resp.body,
        include_bytes!("fixtures/t_patch_wrapped.json"),
        "t_patch_wrapped",
    );
}

#[test]
fn patch_unsupported_field_is_invalid_json() {
    // The handler maps a bad update mask to a decode failure → "invalid json
    // request"; `decode_topic_patch` returns None for that.
    assert!(decode_topic_patch(b"{}", "bogus").is_none());
}

#[test]
fn list_topics_and_pagination_match_oracle() {
    let dir = tempdir();
    let mut s = server(&dir);
    // Recreate the oracle scenario: orders (with patch), alpha, beta, gamma.
    s.create_topic(
        "devcloud",
        "orders",
        &topic_from(r#"{"labels":{"env":"prod"},"messageRetentionDuration":"3600s"}"#),
    )
    .expect("orders");
    s.set_fixed_now(NOW_PLUS_H);
    let p = decode_topic_patch(br#"{"labels":{"team":"core"}}"#, "labels").unwrap();
    s.patch_topic("devcloud", "orders", &p.topic, &p.fields)
        .expect("patch1");
    let p = decode_topic_patch(
        br#"{"topic":{"kmsKeyName":"k1"},"updateMask":"kmsKeyName"}"#,
        "",
    )
    .unwrap();
    s.patch_topic("devcloud", "orders", &p.topic, &p.fields)
        .expect("patch2");
    s.set_fixed_now(NOW);
    for id in ["alpha", "beta", "gamma"] {
        s.create_topic("devcloud", id, &Topic::default())
            .expect("create");
    }

    matches(
        &s.list_topics("devcloud", 0, 0).expect("list").body,
        include_bytes!("fixtures/t_list.json"),
        "t_list",
    );
    matches(
        &s.list_topics("devcloud", 2, 0).expect("page").body,
        include_bytes!("fixtures/t_list_page.json"),
        "t_list_page",
    );
    matches(
        &s.list_topics("devcloud", 2, 2).expect("page2").body,
        include_bytes!("fixtures/t_list_page2.json"),
        "t_list_page2",
    );
}

#[test]
fn list_topics_bad_project() {
    let dir = tempdir();
    let s = server(&dir);
    let err = s.list_topics("Invalid!", 0, 0).expect_err("badproj");
    assert_eq!(err.message, "invalid project name");
}

#[test]
fn topic_subscriptions_and_snapshots_empty() {
    let dir = tempdir();
    let mut s = server(&dir);
    s.create_topic("devcloud", "orders", &Topic::default())
        .expect("create");
    matches(
        &s.list_topic_subscriptions("devcloud", "orders", 0, 0)
            .expect("subs")
            .body,
        include_bytes!("fixtures/t_subs.json"),
        "t_subs",
    );
    matches(
        &s.list_topic_snapshots("devcloud", "orders", 0, 0)
            .expect("snaps")
            .body,
        include_bytes!("fixtures/t_snaps.json"),
        "t_snaps",
    );
}

#[test]
fn delete_topic() {
    let dir = tempdir();
    let mut s = server(&dir);
    s.create_topic("devcloud", "orders", &Topic::default())
        .expect("create");
    let resp = s.delete_topic("devcloud", "orders").expect("delete");
    assert_eq!(resp.status, 204);
    assert!(resp.body.is_empty());
    let err = s.delete_topic("devcloud", "orders").expect_err("gone");
    assert_eq!(err.status, 404);
    assert_eq!(err.message, "topic not found");
}

#[test]
fn full_scenario_state_matches_oracle() {
    let dir = tempdir();
    let mut s = server(&dir);
    s.create_topic(
        "devcloud",
        "orders",
        &topic_from(r#"{"labels":{"env":"prod"},"messageRetentionDuration":"3600s"}"#),
    )
    .expect("orders");
    s.set_fixed_now(NOW_PLUS_H);
    let p = decode_topic_patch(br#"{"labels":{"team":"core"}}"#, "labels").unwrap();
    s.patch_topic("devcloud", "orders", &p.topic, &p.fields)
        .expect("patch1");
    let p = decode_topic_patch(
        br#"{"topic":{"kmsKeyName":"k1"},"updateMask":"kmsKeyName"}"#,
        "",
    )
    .unwrap();
    s.patch_topic("devcloud", "orders", &p.topic, &p.fields)
        .expect("patch2");
    s.set_fixed_now(NOW);
    for id in ["alpha", "beta", "gamma"] {
        s.create_topic("devcloud", id, &Topic::default())
            .expect("create");
    }
    s.delete_topic("devcloud", "alpha").expect("delete alpha");
    let on_disk = std::fs::read(dir.join("resources.json")).expect("read");
    matches(
        &on_disk,
        include_bytes!("fixtures/t_resources.json"),
        "t_resources",
    );
}

// --- minimal tempdir -------------------------------------------------------

fn tempdir() -> std::path::PathBuf {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let mut dir = std::env::temp_dir();
    dir.push(format!("devcloud-ps-topic-{}-{}", std::process::id(), n));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create tempdir");
    dir
}
