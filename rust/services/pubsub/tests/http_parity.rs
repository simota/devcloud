//! Parity tests for IAM, Seek, health, auth, and the HTTP routing layer against
//! golden oracles captured from the Go Pub/Sub REST service.

use devcloud_pubsub::http::{route, Request};
use devcloud_pubsub::model::Subscription;
use devcloud_pubsub::server::{Config, Server};
use std::collections::BTreeMap;

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

fn req(method: &str, path: &str, body: &str) -> Request {
    let (path, query) = match path.split_once('?') {
        Some((p, q)) => {
            let mut params = BTreeMap::new();
            for pair in q.split('&') {
                let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
                params.insert(k.to_string(), v.to_string());
            }
            (p.to_string(), params)
        }
        None => (path.to_string(), BTreeMap::new()),
    };
    Request {
        method: method.to_string(),
        path,
        query,
        headers: BTreeMap::new(),
        body: body.as_bytes().to_vec(),
    }
}

fn matches(got: &[u8], fixture: &[u8], label: &str) {
    assert_eq!(
        String::from_utf8_lossy(got),
        String::from_utf8_lossy(fixture),
        "{label}"
    );
}

#[test]
fn iam_operations_match_oracle() {
    let (dir, md) = (tempdir(), tempdir());
    let mut s = server(&dir, &md);

    let get = route(
        &mut s,
        &req(
            "POST",
            "/v1/projects/devcloud/topics/orders:getIamPolicy",
            "",
        ),
    );
    matches(
        &get.body,
        include_bytes!("fixtures/iam_get.json"),
        "iam_get",
    );

    let set = route(
        &mut s,
        &req(
            "POST",
            "/v1/projects/devcloud/topics/orders:setIamPolicy",
            r#"{"policy":{"version":3,"bindings":[{"role":"roles/pubsub.viewer","members":["user:a@b.com"]}]}}"#,
        ),
    );
    matches(
        &set.body,
        include_bytes!("fixtures/iam_set.json"),
        "iam_set",
    );

    let set_empty = route(
        &mut s,
        &req(
            "POST",
            "/v1/projects/devcloud/topics/orders:setIamPolicy",
            "{}",
        ),
    );
    matches(
        &set_empty.body,
        include_bytes!("fixtures/iam_set_empty.json"),
        "iam_set_empty",
    );

    let test = route(
        &mut s,
        &req(
            "POST",
            "/v1/projects/devcloud/topics/orders:testIamPermissions",
            r#"{"permissions":["pubsub.topics.publish","pubsub.topics.get"]}"#,
        ),
    );
    matches(
        &test.body,
        include_bytes!("fixtures/iam_test.json"),
        "iam_test",
    );

    let sub_get = route(
        &mut s,
        &req(
            "POST",
            "/v1/projects/devcloud/subscriptions/sub1:getIamPolicy",
            "",
        ),
    );
    matches(
        &sub_get.body,
        include_bytes!("fixtures/iam_sub_get.json"),
        "iam_sub_get",
    );

    let notopic = route(
        &mut s,
        &req("POST", "/v1/projects/devcloud/topics/nope:getIamPolicy", ""),
    );
    assert_eq!(notopic.status, 404);
}

#[test]
fn seek_match_and_errors() {
    let (dir, md) = (tempdir(), tempdir());
    let mut s = server(&dir, &md);

    let by_time = route(
        &mut s,
        &req(
            "POST",
            "/v1/projects/devcloud/subscriptions/sub1:seek",
            r#"{"time":"2026-05-30T11:00:00Z"}"#,
        ),
    );
    matches(
        &by_time.body,
        include_bytes!("fixtures/seek_time.json"),
        "seek_time",
    );

    let empty = route(
        &mut s,
        &req(
            "POST",
            "/v1/projects/devcloud/subscriptions/sub1:seek",
            "{}",
        ),
    );
    assert_eq!(empty.status, 400);
    let both = route(
        &mut s,
        &req(
            "POST",
            "/v1/projects/devcloud/subscriptions/sub1:seek",
            r#"{"snapshot":"x","time":"y"}"#,
        ),
    );
    assert_eq!(both.status, 400);
    let badtime = route(
        &mut s,
        &req(
            "POST",
            "/v1/projects/devcloud/subscriptions/sub1:seek",
            r#"{"time":"not a time"}"#,
        ),
    );
    assert_eq!(badtime.status, 400);

    // Seek to snapshot.
    s.create_snapshot("devcloud", "snap1", "projects/devcloud/subscriptions/sub1")
        .expect("snap");
    let by_snap = route(
        &mut s,
        &req(
            "POST",
            "/v1/projects/devcloud/subscriptions/sub1:seek",
            r#"{"snapshot":"projects/devcloud/snapshots/snap1"}"#,
        ),
    );
    assert_eq!(String::from_utf8_lossy(&by_snap.body), "{}\n");
}

#[test]
fn health_and_notfound() {
    let (dir, md) = (tempdir(), tempdir());
    let mut s = server(&dir, &md);
    let health = route(&mut s, &req("GET", "/healthz", ""));
    matches(
        &health.body,
        include_bytes!("fixtures/health.json"),
        "health",
    );
    let ready = route(&mut s, &req("GET", "/readyz", ""));
    matches(&ready.body, include_bytes!("fixtures/ready.json"), "ready");
    let nf = route(&mut s, &req("GET", "/v1/projects/devcloud/bogus", ""));
    matches(
        &nf.body,
        include_bytes!("fixtures/notfound.json"),
        "notfound",
    );
    assert_eq!(nf.status, 404);
}

#[test]
fn auth_rejects_without_token() {
    let mut s = Server::new(Config {
        project: "devcloud".to_string(),
        auth_mode: "strict".to_string(),
        bearer_token: "secret".to_string(),
        ..Default::default()
    });
    let resp = route(&mut s, &req("GET", "/v1/projects/devcloud/topics", ""));
    assert_eq!(resp.status, 401);
    assert!(resp.www_authenticate);
    matches(
        &resp.body,
        include_bytes!("fixtures/auth_noheader.json"),
        "auth_noheader",
    );

    // With the right token, authorized.
    let mut authed = req("GET", "/v1/projects/devcloud/topics", "");
    authed
        .headers
        .insert("authorization".to_string(), "Bearer secret".to_string());
    let ok = route(&mut s, &authed);
    assert_eq!(ok.status, 200);
}

#[test]
fn full_publish_pull_via_router() {
    let (dir, md) = (tempdir(), tempdir());
    let mut s = server(&dir, &md);
    let pub_resp = route(
        &mut s,
        &req(
            "POST",
            "/v1/projects/devcloud/topics/orders:publish",
            r#"{"messages":[{"data":"aGk="}]}"#,
        ),
    );
    assert_eq!(pub_resp.status, 200);
    assert_eq!(
        String::from_utf8_lossy(&pub_resp.body),
        "{\"messageIds\":[\"1\"]}\n"
    );

    let pull = route(
        &mut s,
        &req(
            "POST",
            "/v1/projects/devcloud/subscriptions/sub1:pull",
            r#"{"maxMessages":10}"#,
        ),
    );
    let parsed: serde_json::Value = serde_json::from_slice(&pull.body).unwrap();
    assert_eq!(parsed["receivedMessages"][0]["message"]["messageId"], "1");
}

#[test]
fn router_method_not_allowed() {
    let (dir, md) = (tempdir(), tempdir());
    let mut s = server(&dir, &md);
    let resp = route(&mut s, &req("DELETE", "/v1/projects/devcloud/topics", ""));
    assert_eq!(resp.status, 405);
    assert_eq!(resp.allow.as_deref(), Some("GET"));
}

// --- minimal tempdir -------------------------------------------------------

fn tempdir() -> std::path::PathBuf {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let mut dir = std::env::temp_dir();
    dir.push(format!("devcloud-ps-http-{}-{}", std::process::id(), n));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create tempdir");
    dir
}
