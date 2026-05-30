//! Differential-parity tests for snapshot + schema operations against golden
//! oracles captured from the Go Pub/Sub REST service.

use devcloud_pubsub::model::{Schema, Subscription, Topic};
use devcloud_pubsub::server::{Config, Server};
use serde_json::json;

const NOW: &str = "2026-05-30T12:00:00Z";

fn server(dir: &std::path::Path) -> Server {
    let mut s = Server::new(Config {
        project: "devcloud".to_string(),
        storage_path: dir.to_string_lossy().to_string(),
        ..Default::default()
    });
    s.set_fixed_now(NOW);
    s.create_topic("devcloud", "orders", &Topic::default())
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

fn schema_from(json: &str) -> Schema {
    serde_json::from_str(json).unwrap()
}

fn matches(got: &[u8], fixture: &[u8], label: &str) {
    assert_eq!(
        String::from_utf8_lossy(got),
        String::from_utf8_lossy(fixture),
        "{label}"
    );
}

#[test]
fn snapshot_create_get_list_matches_oracle() {
    let dir = tempdir();
    let mut s = server(&dir);
    let resp = s
        .create_snapshot("devcloud", "snap1", "projects/devcloud/subscriptions/sub1")
        .expect("create");
    matches(
        &resp.body,
        include_bytes!("fixtures/snap_create.json"),
        "snap_create",
    );

    let got = s.get_snapshot("devcloud", "snap1").expect("get");
    matches(
        &got.body,
        include_bytes!("fixtures/snap_get.json"),
        "snap_get",
    );

    let list = s.list_snapshots("devcloud", 0, 0).expect("list");
    matches(
        &list.body,
        include_bytes!("fixtures/snap_list.json"),
        "snap_list",
    );
}

#[test]
fn snapshot_errors() {
    let dir = tempdir();
    let mut s = server(&dir);
    assert_eq!(
        s.create_snapshot("devcloud", "bad", "bad")
            .expect_err("badsub")
            .message,
        "invalid subscription name"
    );
    let missing = s
        .create_snapshot("devcloud", "m", "projects/devcloud/subscriptions/nope")
        .expect_err("missing");
    assert_eq!(missing.status, 404);
    assert_eq!(missing.message, "subscription not found");
    s.create_snapshot("devcloud", "snap1", "projects/devcloud/subscriptions/sub1")
        .expect("first");
    let dup = s
        .create_snapshot("devcloud", "snap1", "projects/devcloud/subscriptions/sub1")
        .expect_err("dup");
    assert_eq!(dup.status, 409);
    assert_eq!(dup.message, "snapshot already exists");
    assert_eq!(
        s.get_snapshot("devcloud", "missing")
            .expect_err("gm")
            .message,
        "snapshot not found"
    );
}

#[test]
fn snapshot_delete() {
    let dir = tempdir();
    let mut s = server(&dir);
    s.create_snapshot("devcloud", "snap1", "projects/devcloud/subscriptions/sub1")
        .expect("create");
    let resp = s.delete_snapshot("devcloud", "snap1").expect("delete");
    assert_eq!(resp.status, 204);
    assert!(s.delete_snapshot("devcloud", "snap1").is_err());
}

#[test]
fn schema_create_get_list_matches_oracle() {
    let dir = tempdir();
    let mut s = server(&dir);
    let resp = s
        .create_schema(
            "devcloud",
            "sch1",
            &schema_from(r#"{"type":"AVRO","definition":"{\"type\":\"record\",\"name\":\"R\",\"fields\":[]}"}"#),
        )
        .expect("create");
    matches(
        &resp.body,
        include_bytes!("fixtures/sch_create.json"),
        "sch_create",
    );

    let put = s
        .create_schema(
            "devcloud",
            "sch2",
            &schema_from(r#"{"type":"PROTOCOL_BUFFER","definition":"syntax=proto3;"}"#),
        )
        .expect("create put");
    matches(
        &put.body,
        include_bytes!("fixtures/sch_create_put.json"),
        "sch_create_put",
    );

    let got = s.get_schema("devcloud", "sch1", "").expect("get");
    matches(
        &got.body,
        include_bytes!("fixtures/sch_get.json"),
        "sch_get",
    );

    let basic = s
        .get_schema("devcloud", "sch1", "BASIC")
        .expect("get basic");
    matches(
        &basic.body,
        include_bytes!("fixtures/sch_get_basic.json"),
        "sch_get_basic",
    );

    let list = s.list_schemas("devcloud", "", 0, 0).expect("list");
    matches(
        &list.body,
        include_bytes!("fixtures/sch_list.json"),
        "sch_list",
    );

    let list_basic = s
        .list_schemas("devcloud", "BASIC", 0, 0)
        .expect("list basic");
    matches(
        &list_basic.body,
        include_bytes!("fixtures/sch_list_basic.json"),
        "sch_list_basic",
    );
}

#[test]
fn schema_errors() {
    let dir = tempdir();
    let mut s = server(&dir);
    assert_eq!(
        s.create_schema("devcloud", "badtype", &schema_from(r#"{"type":"BOGUS"}"#))
            .expect_err("badtype")
            .message,
        "invalid schema type"
    );
    assert_eq!(
        s.create_schema(
            "devcloud",
            "badavro",
            &schema_from(r#"{"type":"AVRO","definition":"not json"}"#)
        )
        .expect_err("badavro")
        .message,
        "avro schema definition must be valid json"
    );
    assert_eq!(
        s.create_schema("devcloud", "", &Schema::default())
            .expect_err("noid")
            .message,
        "schemaId is required"
    );
    s.create_schema(
        "devcloud",
        "sch1",
        &schema_from(r#"{"type":"AVRO","definition":"{}"}"#),
    )
    .expect("first");
    let dup = s
        .create_schema("devcloud", "sch1", &schema_from(r#"{"type":"AVRO"}"#))
        .expect_err("dup");
    assert_eq!(dup.status, 409);
    assert_eq!(dup.message, "schema already exists");
}

#[test]
fn validate_message() {
    let dir = tempdir();
    let mut s = server(&dir);
    s.create_schema(
        "devcloud",
        "sch1",
        &schema_from(r#"{"type":"AVRO","definition":"{}"}"#),
    )
    .expect("create");

    // By name.
    let resp = s
        .validate_message(
            "devcloud",
            &json!({"name": "projects/devcloud/schemas/sch1"}),
        )
        .expect("by name");
    assert_eq!(String::from_utf8_lossy(&resp.body), "{}\n");

    // Inline.
    let resp = s
        .validate_message(
            "devcloud",
            &json!({"schema": {"type": "AVRO", "definition": "{\"type\":\"record\"}"}}),
        )
        .expect("inline");
    assert_eq!(String::from_utf8_lossy(&resp.body), "{}\n");

    // Both set.
    let err = s
        .validate_message(
            "devcloud",
            &json!({"name": "projects/devcloud/schemas/sch1", "schema": {"type": "AVRO", "definition": "{}"}}),
        )
        .expect_err("both");
    assert_eq!(
        err.message,
        "only one of schema name or inline schema may be set"
    );

    // Neither.
    let err = s
        .validate_message("devcloud", &json!({}))
        .expect_err("neither");
    assert_eq!(err.message, "schema name or inline schema is required");

    // Name not found.
    let err = s
        .validate_message(
            "devcloud",
            &json!({"name": "projects/devcloud/schemas/nope"}),
        )
        .expect_err("notfound");
    assert_eq!(err.status, 404);
}

#[test]
fn full_scenario_state_matches_oracle() {
    let dir = tempdir();
    let mut s = server(&dir);
    s.create_snapshot("devcloud", "snap1", "projects/devcloud/subscriptions/sub1")
        .expect("snap");
    s.create_schema(
        "devcloud",
        "sch1",
        &schema_from(
            r#"{"type":"AVRO","definition":"{\"type\":\"record\",\"name\":\"R\",\"fields\":[]}"}"#,
        ),
    )
    .expect("sch1");
    s.create_schema(
        "devcloud",
        "sch2",
        &schema_from(r#"{"type":"PROTOCOL_BUFFER","definition":"syntax=proto3;"}"#),
    )
    .expect("sch2");
    s.delete_schema("devcloud", "sch2").expect("del sch2");
    s.delete_snapshot("devcloud", "snap1").expect("del snap");

    let on_disk = std::fs::read(dir.join("resources.json")).expect("read");
    matches(
        &on_disk,
        include_bytes!("fixtures/ss_resources.json"),
        "ss_resources",
    );
}

// --- minimal tempdir -------------------------------------------------------

fn tempdir() -> std::path::PathBuf {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let mut dir = std::env::temp_dir();
    dir.push(format!("devcloud-ps-ss-{}-{}", std::process::id(), n));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create tempdir");
    dir
}
