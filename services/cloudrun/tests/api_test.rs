//! Cloud Run control plane through `http::process`, and the data plane through
//! a real listener proxying to a local python HTTP server.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use devcloud_cloudrun::http::{process, serve, Request};
use devcloud_cloudrun::{Config, Server};
use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const PARENT: &str = "/v2/projects/demo/locations/us-central1";

fn temp_dir(tag: &str) -> PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let dir = std::env::temp_dir().join(format!(
        "devcloud-cloudrun-{tag}-{}-{nanos}",
        std::process::id()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn config(dir: &std::path::Path, addr: &str) -> Config {
    Config {
        addr: addr.into(),
        project: "demo".into(),
        region: "us-central1".into(),
        auth_mode: "relaxed".into(),
        storage_path: dir.to_string_lossy().into_owned(),
        ..Config::default()
    }
}

async fn call(server: &Arc<Server>, method: &str, target: &str, body: Value) -> (u16, Value) {
    call_auth(server, method, target, body, "").await
}

async fn call_auth(
    server: &Arc<Server>,
    method: &str,
    target: &str,
    body: Value,
    auth: &str,
) -> (u16, Value) {
    call_with(server, method, target, body, &[("authorization", auth)]).await
}

async fn call_with(
    server: &Arc<Server>,
    method: &str,
    target: &str,
    body: Value,
    extra: &[(&str, &str)],
) -> (u16, Value) {
    let (path, query) = match target.split_once('?') {
        Some((p, q)) => (p.to_string(), q.to_string()),
        None => (target.to_string(), String::new()),
    };
    let mut headers = HashMap::new();
    for (k, v) in extra {
        if !v.is_empty() {
            headers.insert(k.to_string(), v.to_string());
        }
    }
    let req = Request {
        method: method.into(),
        path,
        query,
        headers,
        body: if body.is_null() {
            Vec::new()
        } else {
            body.to_string().into_bytes()
        },
    };
    let r = process(server, &req).await;
    (
        r.status,
        serde_json::from_slice(&r.body).unwrap_or(Value::Null),
    )
}

fn has_python() -> bool {
    std::process::Command::new("python3")
        .arg("--version")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

/// A tiny HTTP app: echoes method, path, and a few env vars as JSON.
const APP: &str = r#"
import http.server, json, os
class H(http.server.BaseHTTPRequestHandler):
    def _send(self):
        n = int(self.headers.get("Content-Length") or 0)
        body = self.rfile.read(n).decode() if n else ""
        out = json.dumps({"method": self.command, "path": self.path, "body": body, "greeting": os.environ.get("GREETING"), "k_service": os.environ.get("K_SERVICE"), "k_revision": os.environ.get("K_REVISION"), "host": self.headers.get("Host")}).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(out)))
        self.end_headers()
        self.wfile.write(out)
    do_GET = _send
    do_POST = _send
    def log_message(self, *a):
        print("request", self.path, flush=True)
http.server.HTTPServer(("127.0.0.1", int(os.environ["PORT"])), H).serve_forever()
"#;

fn service_body(greeting: &str) -> Value {
    json!({
        "labels": { "team": "dev" },
        "template": {
            "containers": [{
                "image": "us-docker.pkg.dev/cloudrun/container/hello",
                "command": ["python3", "-c", APP],
                "env": [{ "name": "GREETING", "value": greeting }],
            }]
        }
    })
}

async fn raw_http(port: u16, request: String) -> (u16, String) {
    let mut s = tokio::net::TcpStream::connect(("127.0.0.1", port))
        .await
        .unwrap();
    s.write_all(request.as_bytes()).await.unwrap();
    let mut buf = Vec::new();
    s.read_to_end(&mut buf).await.unwrap();
    let text = String::from_utf8_lossy(&buf).into_owned();
    let status = text
        .split(' ')
        .nth(1)
        .and_then(|c| c.parse().ok())
        .unwrap_or(0);
    let body = text
        .split_once("\r\n\r\n")
        .map(|(_, b)| b.to_string())
        .unwrap_or_default();
    (status, body)
}

#[tokio::test]
async fn service_lifecycle_and_revisions() {
    let dir = temp_dir("crud");
    let server = Arc::new(Server::new(config(&dir, "127.0.0.1:18095")));

    let (status, op) = call(
        &server,
        "POST",
        &format!("{PARENT}/services?serviceId=web"),
        service_body("v1"),
    )
    .await;
    assert_eq!(status, 200, "{op}");
    assert_eq!(op["done"], true);
    let svc = &op["response"];
    assert_eq!(
        svc["@type"],
        "type.googleapis.com/google.cloud.run.v2.Service"
    );
    assert_eq!(
        svc["name"],
        "projects/demo/locations/us-central1/services/web"
    );
    assert_eq!(svc["generation"], "1");
    assert_eq!(
        svc["uri"],
        "http://web.us-central1.demo.run.localhost:18095"
    );
    assert_eq!(
        svc["urls"][1],
        "http://127.0.0.1:18095/_run/demo/us-central1/web/"
    );
    assert_eq!(svc["terminalCondition"]["state"], "CONDITION_SUCCEEDED");
    assert_eq!(
        svc["template"]["containers"][0]["ports"][0]["containerPort"],
        8080
    );
    let rev1 = svc["latestReadyRevision"].as_str().unwrap().to_string();
    assert!(
        rev1.starts_with("projects/demo/locations/us-central1/services/web/revisions/web-00001-")
    );

    let (status, op_get) = call(
        &server,
        "GET",
        &format!("/v2/{}", op["name"].as_str().unwrap()),
        Value::Null,
    )
    .await;
    assert_eq!(status, 200);
    assert_eq!(op_get["done"], true);

    let (status, _) = call(
        &server,
        "POST",
        &format!("{PARENT}/services?serviceId=web"),
        service_body("v1"),
    )
    .await;
    assert_eq!(status, 409);

    // Template change → new revision; label-only change → same revision.
    let (status, op2) = call(
        &server,
        "PATCH",
        &format!("{PARENT}/services/web"),
        service_body("v2"),
    )
    .await;
    assert_eq!(status, 200, "{op2}");
    assert_eq!(op2["response"]["generation"], "2");
    let rev2 = op2["response"]["latestReadyRevision"]
        .as_str()
        .unwrap()
        .to_string();
    assert_ne!(rev1, rev2);

    let (_, op3) = call(
        &server,
        "PATCH",
        &format!("{PARENT}/services/web?updateMask=labels"),
        json!({ "labels": { "team": "platform" } }),
    )
    .await;
    assert_eq!(op3["response"]["labels"]["team"], "platform");
    assert_eq!(op3["response"]["latestReadyRevision"], rev2.as_str());
    assert_eq!(op3["response"]["generation"], "3");

    let (_, revisions) = call(
        &server,
        "GET",
        &format!("{PARENT}/services/web/revisions"),
        Value::Null,
    )
    .await;
    let names: Vec<&str> = revisions["revisions"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["name"].as_str().unwrap())
        .collect();
    assert_eq!(names.len(), 2);
    assert!(names.contains(&rev1.as_str()) && names.contains(&rev2.as_str()));

    let (status, _) = call(&server, "DELETE", &format!("/v2/{rev2}"), Value::Null).await;
    assert_eq!(status, 400, "latest revision cannot be deleted");
    let (status, _) = call(&server, "DELETE", &format!("/v2/{rev1}"), Value::Null).await;
    assert_eq!(status, 200);

    // Stale etag is rejected.
    let (status, _) = call(
        &server,
        "PATCH",
        &format!("{PARENT}/services/web"),
        json!({ "etag": "stale", "labels": {} }),
    )
    .await;
    assert_eq!(status, 409);

    let (_, list) = call(&server, "GET", &format!("{PARENT}/services"), Value::Null).await;
    assert_eq!(list["services"].as_array().unwrap().len(), 1);
    let (_, all) = call(
        &server,
        "GET",
        "/v2/projects/demo/locations/-/services",
        Value::Null,
    )
    .await;
    assert_eq!(all["services"].as_array().unwrap().len(), 1);
    let (_, everywhere) = call(&server, "GET", "/_introspect/services", Value::Null).await;
    assert_eq!(
        everywhere["services"][0]["name"],
        "projects/demo/locations/us-central1/services/web"
    );

    // IAM policy round trip.
    let (status, policy) = call(
        &server,
        "POST",
        &format!("{PARENT}/services/web:setIamPolicy"),
        json!({ "policy": { "bindings": [{ "role": "roles/run.invoker", "members": ["allUsers"] }] } }),
    )
    .await;
    assert_eq!(status, 200);
    assert!(policy["etag"].is_string());
    let (_, got) = call(
        &server,
        "GET",
        &format!("{PARENT}/services/web:getIamPolicy"),
        Value::Null,
    )
    .await;
    assert_eq!(got["bindings"][0]["members"][0], "allUsers");

    // State survives a restart.
    let restarted = Arc::new(Server::new(config(&dir, "127.0.0.1:18095")));
    let (status, svc) = call(
        &restarted,
        "GET",
        &format!("{PARENT}/services/web"),
        Value::Null,
    )
    .await;
    assert_eq!(status, 200);
    assert_eq!(svc["labels"]["team"], "platform");

    let (status, del) = call(
        &server,
        "DELETE",
        &format!("{PARENT}/services/web"),
        Value::Null,
    )
    .await;
    assert_eq!(status, 200);
    assert!(del["response"]["deleteTime"].is_string());
    let (status, err) = call(
        &server,
        "GET",
        &format!("{PARENT}/services/web"),
        Value::Null,
    )
    .await;
    assert_eq!(status, 404);
    assert_eq!(err["error"]["status"], "NOT_FOUND");
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test]
async fn validation_errors() {
    let dir = temp_dir("validation");
    let server = Arc::new(Server::new(config(&dir, "127.0.0.1:18095")));
    let (status, err) = call(
        &server,
        "POST",
        &format!("{PARENT}/services?serviceId=Bad_Name"),
        service_body("x"),
    )
    .await;
    assert_eq!(status, 400);
    assert_eq!(err["error"]["status"], "INVALID_ARGUMENT");
    let (status, _) = call(
        &server,
        "POST",
        &format!("{PARENT}/services?serviceId=ok"),
        json!({ "template": { "containers": [{}] } }),
    )
    .await;
    assert_eq!(status, 400);
    let (status, _) = call(
        &server,
        "POST",
        &format!("{PARENT}/services?serviceId=ok"),
        json!({}),
    )
    .await;
    assert_eq!(status, 400);
    let (status, _) = call(
        &server,
        "POST",
        &format!("{PARENT}/services?serviceId=ok&validateOnly=true"),
        service_body("x"),
    )
    .await;
    assert_eq!(status, 200);
    let (_, list) = call(&server, "GET", &format!("{PARENT}/services"), Value::Null).await;
    assert_eq!(
        list["services"].as_array().unwrap().len(),
        0,
        "validateOnly must not persist"
    );
    let (status, _) = call(
        &server,
        "PATCH",
        &format!("{PARENT}/services/missing"),
        service_body("x"),
    )
    .await;
    assert_eq!(status, 404);
    let (status, op) = call(
        &server,
        "PATCH",
        &format!("{PARENT}/services/created?allowMissing=true"),
        service_body("x"),
    )
    .await;
    assert_eq!(status, 200);
    assert_eq!(
        op["response"]["name"],
        "projects/demo/locations/us-central1/services/created"
    );
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test]
async fn proxies_requests_to_local_process_and_rolls_revisions() {
    if !has_python() {
        eprintln!("skipping: python3 not available");
        return;
    }
    let dir = temp_dir("proxy");
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = Arc::new(Server::new(config(&dir, &format!("127.0.0.1:{port}"))));
    let (tx, rx) = tokio::sync::oneshot::channel::<()>();
    let serve_task = tokio::spawn(serve(listener, Arc::clone(&server), async {
        let _ = rx.await;
    }));

    let (status, _) = call(
        &server,
        "POST",
        &format!("{PARENT}/services?serviceId=web"),
        service_body("v1"),
    )
    .await;
    assert_eq!(status, 200);

    let host = format!("web.us-central1.demo.run.localhost:{port}");
    let (status, body) = raw_http(
        port,
        format!("POST /hello?x=1 HTTP/1.1\r\nHost: {host}\r\nContent-Length: 5\r\nConnection: keep-alive\r\n\r\nworld"),
    )
    .await;
    assert_eq!(status, 200, "{body}");
    let v: Value = serde_json::from_str(&body).unwrap();
    assert_eq!(v["path"], "/hello?x=1");
    assert_eq!(v["method"], "POST");
    assert_eq!(v["body"], "world");
    assert_eq!(v["greeting"], "v1");
    assert_eq!(v["k_service"], "web");
    assert_eq!(v["host"], host);

    // Path-routed fallback reaches the same instance.
    let (status, body) = raw_http(
        port,
        format!("GET /_run/demo/us-central1/web/api HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\n\r\n"),
    )
    .await;
    assert_eq!(status, 200);
    assert_eq!(
        serde_json::from_str::<Value>(&body).unwrap()["path"],
        "/api"
    );

    let (_, instances) = call(&server, "GET", "/_introspect/instances", Value::Null).await;
    assert_eq!(instances["instances"][0]["requestCount"], 2);

    // A new revision replaces the running instance on the next request.
    let (_, op) = call(
        &server,
        "PATCH",
        &format!("{PARENT}/services/web"),
        service_body("v2"),
    )
    .await;
    let rev2 = op["response"]["latestReadyRevision"]
        .as_str()
        .unwrap()
        .rsplit('/')
        .next()
        .unwrap()
        .to_string();
    let (st, body) = raw_http(port, format!("GET / HTTP/1.1\r\nHost: {host}\r\n\r\n")).await;
    let v: Value = serde_json::from_str(&body)
        .unwrap_or_else(|_| panic!("non-JSON reply: status={st} body={body:?}"));
    assert_eq!(v["greeting"], "v2");
    assert_eq!(v["k_revision"], rev2.as_str());

    let (_, logs) = call(
        &server,
        "GET",
        "/_introspect/logs/demo/us-central1/web",
        Value::Null,
    )
    .await;
    assert!(logs["lines"]
        .as_array()
        .unwrap()
        .iter()
        .any(|l| l.as_str().unwrap_or("").contains("request")));

    // Unknown service → 404; deleted service stops its instance.
    let (status, _) = raw_http(
        port,
        format!("GET / HTTP/1.1\r\nHost: nope.us-central1.demo.run.localhost:{port}\r\n\r\n"),
    )
    .await;
    assert_eq!(status, 404);
    call(
        &server,
        "DELETE",
        &format!("{PARENT}/services/web"),
        Value::Null,
    )
    .await;
    let (_, instances) = call(&server, "GET", "/_introspect/instances", Value::Null).await;
    assert_eq!(instances["instances"].as_array().unwrap().len(), 0);

    let _ = tx.send(());
    serve_task.await.unwrap().unwrap();
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test]
async fn image_only_container_without_docker_returns_503() {
    let dir = temp_dir("nodocker");
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = Arc::new(Server::new(config(&dir, &format!("127.0.0.1:{port}"))));
    let (tx, rx) = tokio::sync::oneshot::channel::<()>();
    let serve_task = tokio::spawn(serve(listener, Arc::clone(&server), async {
        let _ = rx.await;
    }));
    let body = json!({ "template": { "containers": [{ "image": "gcr.io/demo/app" }] } });
    call(
        &server,
        "POST",
        &format!("{PARENT}/services?serviceId=img"),
        body,
    )
    .await;
    let (status, text) = raw_http(
        port,
        format!("GET / HTTP/1.1\r\nHost: img.us-central1.demo.run.localhost:{port}\r\n\r\n"),
    )
    .await;
    assert_eq!(status, 503);
    assert!(text.contains("has no command"), "{text}");
    let _ = tx.send(());
    serve_task.await.unwrap().unwrap();
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test]
async fn strict_mode_requires_bearer_token_and_invoker_binding() {
    let dir = temp_dir("strict");
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = Arc::new(Server::new(Config {
        auth_mode: "strict".into(),
        bearer_token: "secret".into(),
        ..config(&dir, &format!("127.0.0.1:{port}"))
    }));
    let (tx, rx) = tokio::sync::oneshot::channel::<()>();
    let serve_task = tokio::spawn(serve(listener, Arc::clone(&server), async {
        let _ = rx.await;
    }));

    let (status, _) = call(&server, "GET", &format!("{PARENT}/services"), Value::Null).await;
    assert_eq!(status, 401);
    let (status, _) = call_auth(
        &server,
        "POST",
        &format!("{PARENT}/services?serviceId=img"),
        json!({ "template": { "containers": [{ "image": "x" }] } }),
        "Bearer secret",
    )
    .await;
    assert_eq!(status, 200);

    // Private service: unauthenticated data-plane request is forbidden.
    let (status, _) = raw_http(
        port,
        format!("GET / HTTP/1.1\r\nHost: img.us-central1.demo.run.localhost:{port}\r\n\r\n"),
    )
    .await;
    assert_eq!(status, 403);
    // With the bearer token it reaches the (non-runnable) instance → 503.
    let (status, _) = raw_http(port, format!("GET / HTTP/1.1\r\nHost: img.us-central1.demo.run.localhost:{port}\r\nAuthorization: Bearer secret\r\n\r\n")).await;
    assert_eq!(status, 503);
    // allUsers invoker makes it public.
    call_auth(
        &server,
        "POST",
        &format!("{PARENT}/services/img:setIamPolicy"),
        json!({ "policy": { "bindings": [{ "role": "roles/run.invoker", "members": ["allUsers"] }] } }),
        "Bearer secret",
    )
    .await;
    let (status, _) = raw_http(
        port,
        format!("GET / HTTP/1.1\r\nHost: img.us-central1.demo.run.localhost:{port}\r\n\r\n"),
    )
    .await;
    assert_eq!(status, 503);

    // Introspection exposes env values and logs: it needs the token too.
    let (status, _) = call(&server, "GET", "/_introspect/services", Value::Null).await;
    assert_eq!(status, 401);
    let (status, _) = call_auth(
        &server,
        "GET",
        "/_introspect/services",
        Value::Null,
        "Bearer secret",
    )
    .await;
    assert_eq!(status, 200);

    let _ = tx.send(());
    serve_task.await.unwrap().unwrap();
    let _ = std::fs::remove_dir_all(dir);
}

// ── regression tests for the review findings ────────────────────────────────

#[tokio::test]
async fn validate_only_patch_and_delete_have_no_side_effects() {
    let dir = temp_dir("validateonly");
    let server = Arc::new(Server::new(config(&dir, "127.0.0.1:18095")));
    call(
        &server,
        "POST",
        &format!("{PARENT}/services?serviceId=web"),
        service_body("v1"),
    )
    .await;
    call(
        &server,
        "POST",
        &format!("{PARENT}/services/web:setIamPolicy"),
        json!({ "policy": { "bindings": [{ "role": "roles/run.invoker", "members": ["allUsers"] }] } }),
    )
    .await;

    let (status, op) = call(
        &server,
        "PATCH",
        &format!("{PARENT}/services/web?validateOnly=true"),
        service_body("v2"),
    )
    .await;
    assert_eq!(status, 200, "{op}");
    assert_eq!(
        op["response"]["generation"], "2",
        "validation reports the would-be result"
    );
    let (_, svc) = call(
        &server,
        "GET",
        &format!("{PARENT}/services/web"),
        Value::Null,
    )
    .await;
    assert_eq!(svc["generation"], "1");
    assert_eq!(svc["template"]["containers"][0]["env"][0]["value"], "v1");

    let (status, _) = call(
        &server,
        "DELETE",
        &format!("{PARENT}/services/web?validateOnly=true"),
        Value::Null,
    )
    .await;
    assert_eq!(status, 200);
    let (status, _) = call(
        &server,
        "GET",
        &format!("{PARENT}/services/web"),
        Value::Null,
    )
    .await;
    assert_eq!(status, 200, "validateOnly delete must keep the service");
    let (_, revisions) = call(
        &server,
        "GET",
        &format!("{PARENT}/services/web/revisions"),
        Value::Null,
    )
    .await;
    assert_eq!(revisions["revisions"].as_array().unwrap().len(), 1);
    let (_, policy) = call(
        &server,
        "GET",
        &format!("{PARENT}/services/web:getIamPolicy"),
        Value::Null,
    )
    .await;
    assert_eq!(policy["bindings"][0]["members"][0], "allUsers");

    let (status, _) = call(
        &server,
        "POST",
        &format!("{PARENT}/services?serviceId=web&validateOnly=true"),
        service_body("x"),
    )
    .await;
    assert_eq!(status, 409, "validateOnly create still detects conflicts");
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test]
async fn nested_update_mask_applies_only_the_named_fields() {
    let dir = temp_dir("mask");
    let server = Arc::new(Server::new(config(&dir, "127.0.0.1:18095")));
    let mut body = service_body("v1");
    body["template"]["timeout"] = json!("90s");
    call(
        &server,
        "POST",
        &format!("{PARENT}/services?serviceId=web"),
        body,
    )
    .await;

    let (status, op) = call(
        &server,
        "PATCH",
        &format!("{PARENT}/services/web?updateMask=template.containers"),
        json!({ "template": { "containers": [{ "image": "gcr.io/demo/v2" }] } }),
    )
    .await;
    assert_eq!(status, 200, "{op}");
    assert_eq!(op["response"]["template"]["timeout"], "90s");
    assert_eq!(
        op["response"]["template"]["containers"][0]["image"],
        "gcr.io/demo/v2"
    );

    let (status, op) = call(
        &server,
        "PATCH",
        &format!("{PARENT}/services/web?updateMask=template.timeout"),
        json!({ "template": { "timeout": "120s" } }),
    )
    .await;
    assert_eq!(status, 200, "{op}");
    assert_eq!(op["response"]["template"]["timeout"], "120s");
    assert_eq!(
        op["response"]["template"]["containers"][0]["image"],
        "gcr.io/demo/v2"
    );

    let (status, _) = call(
        &server,
        "PATCH",
        &format!("{PARENT}/services/web?updateMask=uri"),
        json!({ "uri": "x" }),
    )
    .await;
    assert_eq!(status, 400, "output-only fields are not maskable");
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test]
async fn malformed_traffic_is_rejected_without_poisoning_state() {
    let dir = temp_dir("traffic");
    let server = Arc::new(Server::new(config(&dir, "127.0.0.1:18095")));
    let mut body = service_body("v1");
    body["traffic"] = json!([100]);
    let (status, err) = call(
        &server,
        "POST",
        &format!("{PARENT}/services?serviceId=web"),
        body,
    )
    .await;
    assert_eq!(status, 400, "{err}");
    let mut body = service_body("v1");
    body["traffic"] = json!([{ "type": "TRAFFIC_TARGET_ALLOCATION_TYPE_LATEST", "percent": 50 }]);
    let (status, _) = call(
        &server,
        "POST",
        &format!("{PARENT}/services?serviceId=web"),
        body,
    )
    .await;
    assert_eq!(status, 400, "percentages must sum to 100");
    // The API keeps working afterwards.
    let (status, _) = call(
        &server,
        "POST",
        &format!("{PARENT}/services?serviceId=web"),
        service_body("v1"),
    )
    .await;
    assert_eq!(status, 200);
    let (status, _) = call(
        &server,
        "GET",
        &format!("{PARENT}/services/web"),
        Value::Null,
    )
    .await;
    assert_eq!(status, 200);
    let _ = std::fs::remove_dir_all(dir);
}

async fn serve_on_free_port(
    tag: &str,
) -> (
    Arc<Server>,
    u16,
    PathBuf,
    tokio::sync::oneshot::Sender<()>,
    tokio::task::JoinHandle<std::io::Result<()>>,
) {
    let dir = temp_dir(tag);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = Arc::new(Server::new(config(&dir, &format!("127.0.0.1:{port}"))));
    let (tx, rx) = tokio::sync::oneshot::channel::<()>();
    let task = tokio::spawn(serve(listener, Arc::clone(&server), async {
        let _ = rx.await;
    }));
    (server, port, dir, tx, task)
}

fn shell_service(script: &str) -> Value {
    json!({
        "template": { "containers": [{ "image": "x", "command": ["sh", "-c", script], "env": [{ "name": "GREETING", "value": "sh" }] }] }
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stopping_an_instance_kills_its_whole_process_tree() {
    if !has_python() {
        eprintln!("skipping: python3 not available");
        return;
    }
    let (server, port, dir, tx, task) = serve_on_free_port("tree").await;
    let app = dir.join("app.py");
    std::fs::write(&app, APP).unwrap();
    // The HTTP server is a grandchild: sh forks python and waits.
    let script = format!("python3 {} & wait", app.display());
    call(
        &server,
        "POST",
        &format!("{PARENT}/services?serviceId=tree"),
        shell_service(&script),
    )
    .await;
    let host = format!("tree.us-central1.demo.run.localhost:{port}");
    let (status, _) = raw_http(port, format!("GET / HTTP/1.1\r\nHost: {host}\r\n\r\n")).await;
    assert_eq!(status, 200);
    let (_, instances) = call(&server, "GET", "/_introspect/instances", Value::Null).await;
    let instance_port = instances["instances"][0]["port"].as_u64().unwrap() as u16;

    call(
        &server,
        "DELETE",
        &format!("{PARENT}/services/tree"),
        Value::Null,
    )
    .await;
    let mut alive = true;
    for _ in 0..40 {
        if tokio::net::TcpStream::connect(("127.0.0.1", instance_port))
            .await
            .is_err()
        {
            alive = false;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    assert!(
        !alive,
        "the forked HTTP server must not outlive its instance"
    );
    let _ = tx.send(());
    task.await.unwrap().unwrap();
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_slow_cold_start_does_not_block_other_services() {
    if !has_python() {
        eprintln!("skipping: python3 not available");
        return;
    }
    let (server, port, dir, tx, task) = serve_on_free_port("coldstart").await;
    let app = dir.join("app.py");
    std::fs::write(&app, APP).unwrap();
    call(
        &server,
        "POST",
        &format!("{PARENT}/services?serviceId=fast"),
        shell_service(&format!("exec python3 {}", app.display())),
    )
    .await;
    call(
        &server,
        "POST",
        &format!("{PARENT}/services?serviceId=slow"),
        shell_service(&format!("sleep 3; exec python3 {}", app.display())),
    )
    .await;
    let (status, _) = raw_http(
        port,
        format!("GET / HTTP/1.1\r\nHost: fast.us-central1.demo.run.localhost:{port}\r\n\r\n"),
    )
    .await;
    assert_eq!(status, 200);

    let slow = tokio::spawn(raw_http(
        port,
        format!("GET / HTTP/1.1\r\nHost: slow.us-central1.demo.run.localhost:{port}\r\n\r\n"),
    ));
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    let started = std::time::Instant::now();
    let (status, _) = raw_http(
        port,
        format!("GET / HTTP/1.1\r\nHost: fast.us-central1.demo.run.localhost:{port}\r\n\r\n"),
    )
    .await;
    assert_eq!(status, 200);
    let (_, instances) = call(&server, "GET", "/_introspect/instances", Value::Null).await;
    assert_eq!(
        instances["instances"].as_array().unwrap().len(),
        1,
        "the starting slot is skipped"
    );
    assert!(
        started.elapsed() < std::time::Duration::from_secs(1),
        "blocked for {:?}",
        started.elapsed()
    );
    assert_eq!(slow.await.unwrap().0, 200);

    let _ = tx.send(());
    task.await.unwrap().unwrap();
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test]
async fn huge_page_token_does_not_panic_or_poison_state() {
    let dir = temp_dir("pagetoken");
    let server = Arc::new(Server::new(config(&dir, "127.0.0.1:18095")));
    call(
        &server,
        "POST",
        &format!("{PARENT}/services?serviceId=web"),
        service_body("v1"),
    )
    .await;
    let (status, page) = call(
        &server,
        "GET",
        &format!("{PARENT}/services?pageToken={}", u64::MAX),
        Value::Null,
    )
    .await;
    assert_eq!(status, 200);
    assert_eq!(page["services"], json!([]));
    let (status, _) = call(
        &server,
        "GET",
        &format!("{PARENT}/services/web"),
        Value::Null,
    )
    .await;
    assert_eq!(status, 200, "state lock still usable");
    let _ = std::fs::remove_dir_all(dir);
}

// ── regression tests for the sixth review round ─────────────────────────────

#[tokio::test]
async fn cross_site_browser_requests_cannot_deploy_services() {
    let dir = temp_dir("csrf");
    let server = Arc::new(Server::new(config(&dir, "127.0.0.1:18095")));
    let evil = [
        ("origin", "https://evil.example"),
        ("content-type", "text/plain"),
    ];
    let (status, err) = call_with(
        &server,
        "POST",
        &format!("{PARENT}/services?serviceId=web"),
        service_body("x"),
        &evil,
    )
    .await;
    assert_eq!(status, 403);
    assert_eq!(err["error"]["status"], "PERMISSION_DENIED");
    let (status, _) = call(
        &server,
        "GET",
        &format!("{PARENT}/services/web"),
        Value::Null,
    )
    .await;
    assert_eq!(status, 404);

    let local = [("origin", "http://127.0.0.1:18025")];
    let (status, _) = call_with(
        &server,
        "POST",
        &format!("{PARENT}/services?serviceId=web"),
        service_body("x"),
        &local,
    )
    .await;
    assert_eq!(status, 200);
    let (status, _) = call_with(
        &server,
        "PATCH",
        &format!("{PARENT}/services/web"),
        service_body("y"),
        &evil,
    )
    .await;
    assert_eq!(status, 403);
    let (status, _) = call_with(
        &server,
        "DELETE",
        &format!("{PARENT}/services/web"),
        Value::Null,
        &evil,
    )
    .await;
    assert_eq!(status, 403);
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test]
async fn validate_only_revision_delete_keeps_the_revision() {
    let dir = temp_dir("revvalidate");
    let server = Arc::new(Server::new(config(&dir, "127.0.0.1:18095")));
    let (_, op) = call(
        &server,
        "POST",
        &format!("{PARENT}/services?serviceId=web"),
        service_body("v1"),
    )
    .await;
    let rev1 = op["response"]["latestReadyRevision"]
        .as_str()
        .unwrap()
        .to_string();
    call(
        &server,
        "PATCH",
        &format!("{PARENT}/services/web"),
        service_body("v2"),
    )
    .await;

    let (status, op) = call(
        &server,
        "DELETE",
        &format!("/v2/{rev1}?validateOnly=true"),
        Value::Null,
    )
    .await;
    assert_eq!(status, 200, "{op}");
    assert_eq!(
        op["response"]["@type"],
        "type.googleapis.com/google.cloud.run.v2.Revision"
    );
    let (status, _) = call(&server, "GET", &format!("/v2/{rev1}"), Value::Null).await;
    assert_eq!(status, 200, "validateOnly must not delete the revision");
    let (_, ops) = call(&server, "GET", &format!("{PARENT}/operations"), Value::Null).await;
    assert!(
        !ops["operations"]
            .as_array()
            .unwrap()
            .iter()
            .any(|o| o["name"]
                .as_str()
                .unwrap_or("")
                .ends_with("/operations/validate")),
        "validation is not recorded as an operation"
    );
    let (status, _) = call(&server, "DELETE", &format!("/v2/{rev1}"), Value::Null).await;
    assert_eq!(status, 200);
    let (status, _) = call(&server, "GET", &format!("/v2/{rev1}"), Value::Null).await;
    assert_eq!(status, 404);
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test]
async fn custom_revision_names_must_be_single_valid_segments() {
    let dir = temp_dir("revname");
    let server = Arc::new(Server::new(config(&dir, "127.0.0.1:18095")));
    for bad in ["web-a/b", "web-A", "web-", "web-a_b"] {
        let mut body = service_body("v1");
        body["template"]["revision"] = json!(bad);
        let (status, err) = call(
            &server,
            "POST",
            &format!("{PARENT}/services?serviceId=web"),
            body,
        )
        .await;
        assert_eq!(status, 400, "{bad}: {err}");
    }
    let (status, _) = call(
        &server,
        "GET",
        &format!("{PARENT}/services/web"),
        Value::Null,
    )
    .await;
    assert_eq!(status, 404, "rejected creates leave nothing behind");

    let mut body = service_body("v1");
    body["template"]["revision"] = json!("web-v1");
    let (status, op) = call(
        &server,
        "POST",
        &format!("{PARENT}/services?serviceId=web"),
        body,
    )
    .await;
    assert_eq!(status, 200);
    let rev = op["response"]["latestCreatedRevision"]
        .as_str()
        .unwrap()
        .to_string();
    let (status, _) = call(&server, "GET", &format!("/v2/{rev}"), Value::Null).await;
    assert_eq!(status, 200);
    let _ = std::fs::remove_dir_all(dir);
}

// ── regression tests for the fourteenth review round ────────────────────────

/// Raw-socket app: `/ws` completes an upgrade handshake only when both
/// `Upgrade: websocket` and `Connection: Upgrade` arrive, then echoes bytes;
/// `/docs` redirects to `/docs/`.
const RAW_APP: &str = r#"
import os, socket, threading
def handle(conn):
    data = b""
    while b"\r\n\r\n" not in data:
        chunk = conn.recv(4096)
        if not chunk:
            return
        data += chunk
    head = data.split(b"\r\n\r\n", 1)[0].decode()
    lines = head.split("\r\n")
    path = lines[0].split(" ")[1]
    headers = {k.strip().lower(): v.strip() for k, v in (l.split(":", 1) for l in lines[1:] if ":" in l)}
    if path == "/ws":
        upgrade_ok = headers.get("upgrade", "").lower() == "websocket" and "upgrade" in headers.get("connection", "").lower()
        if not upgrade_ok:
            conn.sendall(b"HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
            conn.close()
            return
        conn.sendall(b"HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\r\n")
        while True:
            chunk = conn.recv(4096)
            if not chunk:
                break
            conn.sendall(chunk)
        conn.close()
        return
    if path == "/docs":
        conn.sendall(b"HTTP/1.1 301 Moved Permanently\r\nLocation: /docs/\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
    else:
        conn.sendall(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok")
    conn.close()
s = socket.socket()
s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
s.bind(("127.0.0.1", int(os.environ["PORT"])))
s.listen(16)
while True:
    c, _ = s.accept()
    threading.Thread(target=handle, args=(c,), daemon=True).start()
"#;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn websocket_upgrades_and_path_routed_redirects_work() {
    if !has_python() {
        eprintln!("skipping: python3 not available");
        return;
    }
    let (server, port, dir, tx, task) = serve_on_free_port("upgrade").await;
    let body = json!({ "template": { "containers": [{ "image": "x", "command": ["python3", "-c", RAW_APP] }] } });
    call(
        &server,
        "POST",
        &format!("{PARENT}/services?serviceId=raw"),
        body,
    )
    .await;
    let host = format!("raw.us-central1.demo.run.localhost:{port}");

    // WebSocket handshake through the proxy, then bytes flow both ways.
    let mut s = tokio::net::TcpStream::connect(("127.0.0.1", port))
        .await
        .unwrap();
    s.write_all(
        format!("GET /ws HTTP/1.1\r\nHost: {host}\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n").as_bytes(),
    )
    .await
    .unwrap();
    let mut buf = vec![0u8; 4096];
    let n = s.read(&mut buf).await.unwrap();
    let response = String::from_utf8_lossy(&buf[..n]).into_owned();
    assert!(response.starts_with("HTTP/1.1 101"), "{response}");
    s.write_all(b"ping").await.unwrap();
    let mut echo = [0u8; 4];
    s.read_exact(&mut echo).await.unwrap();
    assert_eq!(&echo, b"ping");
    drop(s);

    // A path-routed redirect keeps the /_run/ prefix.
    let (status, raw) = raw_http_head(
        port,
        format!("GET /_run/demo/us-central1/raw/docs HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\n\r\n"),
    )
    .await;
    assert_eq!(status, 301);
    assert!(
        raw.contains("Location: /_run/demo/us-central1/raw/docs/\r\n"),
        "{raw}"
    );
    // Host-routed requests are not rewritten.
    let (status, raw) =
        raw_http_head(port, format!("GET /docs HTTP/1.1\r\nHost: {host}\r\n\r\n")).await;
    assert_eq!(status, 301);
    assert!(raw.contains("Location: /docs/\r\n"), "{raw}");

    let _ = tx.send(());
    task.await.unwrap().unwrap();
    let _ = std::fs::remove_dir_all(dir);
}

async fn raw_http_head(port: u16, request: String) -> (u16, String) {
    let mut s = tokio::net::TcpStream::connect(("127.0.0.1", port))
        .await
        .unwrap();
    s.write_all(request.as_bytes()).await.unwrap();
    let mut buf = Vec::new();
    s.read_to_end(&mut buf).await.unwrap();
    let text = String::from_utf8_lossy(&buf).into_owned();
    let status = text
        .split(' ')
        .nth(1)
        .and_then(|c| c.parse().ok())
        .unwrap_or(0);
    (status, text)
}

// ── regression tests for the fifteenth review round ─────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn large_path_routed_bodies_do_not_deadlock() {
    if !has_python() {
        eprintln!("skipping: python3 not available");
        return;
    }
    let (server, port, dir, tx, task) = serve_on_free_port("bigbody").await;
    call(
        &server,
        "POST",
        &format!("{PARENT}/services?serviceId=web"),
        service_body("v1"),
    )
    .await;
    // The app reads the whole body before answering (and echoes it back).
    let body = "x".repeat(1024 * 1024);
    let request = format!(
        "POST /_run/demo/us-central1/web/upload HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nContent-Length: {}\r\n\r\n{body}",
        body.len()
    );
    let (status, response) =
        tokio::time::timeout(std::time::Duration::from_secs(20), raw_http(port, request))
            .await
            .expect("path-routed POST with a large body deadlocked");
    assert_eq!(status, 200);
    let v: Value = serde_json::from_str(&response).unwrap();
    assert_eq!(v["body"].as_str().unwrap().len(), body.len());
    let _ = tx.send(());
    task.await.unwrap().unwrap();
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test]
async fn wildcard_update_mask_replaces_every_writable_field() {
    let dir = temp_dir("wildcard");
    let server = Arc::new(Server::new(config(&dir, "127.0.0.1:18095")));
    call(
        &server,
        "POST",
        &format!("{PARENT}/services?serviceId=web"),
        service_body("v1"),
    )
    .await;
    let (status, op) = call(
        &server,
        "PATCH",
        &format!("{PARENT}/services/web?updateMask=*"),
        json!({ "template": { "containers": [{ "image": "gcr.io/demo/v2" }] } }),
    )
    .await;
    assert_eq!(status, 200, "{op}");
    let svc = &op["response"];
    assert_eq!(svc["template"]["containers"][0]["image"], "gcr.io/demo/v2");
    assert!(
        svc.get("labels").is_none(),
        "fields absent from the body are cleared by `*`"
    );
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test]
async fn path_routed_root_redirects_to_its_trailing_slash() {
    let (server, port, dir, tx, task) = serve_on_free_port("slash").await;
    let (_, op) = call(
        &server,
        "POST",
        &format!("{PARENT}/services?serviceId=web"),
        service_body("v1"),
    )
    .await;
    assert!(op["response"]["urls"][1]
        .as_str()
        .unwrap()
        .ends_with("/_run/demo/us-central1/web/"));
    let (status, raw) = raw_http_head(
        port,
        format!("GET /_run/demo/us-central1/web?x=1 HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\n\r\n"),
    )
    .await;
    assert_eq!(status, 308, "method-preserving redirect");
    assert!(
        raw.contains("Location: /_run/demo/us-central1/web/?x=1\r\n"),
        "{raw}"
    );
    let _ = tx.send(());
    task.await.unwrap().unwrap();
    let _ = std::fs::remove_dir_all(dir);
}

// ── regression tests for the sixteenth review round ─────────────────────────

#[tokio::test]
async fn out_of_range_container_ports_are_rejected() {
    let dir = temp_dir("ports");
    let server = Arc::new(Server::new(config(&dir, "127.0.0.1:18095")));
    let with_port = |port: Value| json!({ "template": { "containers": [{ "image": "x", "ports": [{ "containerPort": port }] }] } });
    for bad in [
        json!(65536),
        json!(0),
        json!(-1),
        json!(70000),
        json!("8080"),
        json!(80.5),
    ] {
        let (status, err) = call(
            &server,
            "POST",
            &format!("{PARENT}/services?serviceId=web"),
            with_port(bad.clone()),
        )
        .await;
        assert_eq!(status, 400, "{bad}: {err}");
    }
    let (status, _) = call(
        &server,
        "POST",
        &format!("{PARENT}/services?serviceId=web"),
        with_port(json!(65535)),
    )
    .await;
    assert_eq!(status, 200);
    let (status, _) = call(
        &server,
        "PATCH",
        &format!("{PARENT}/services/web"),
        with_port(json!(65536)),
    )
    .await;
    assert_eq!(status, 400, "updates are validated too");
    let (_, svc) = call(
        &server,
        "GET",
        &format!("{PARENT}/services/web"),
        Value::Null,
    )
    .await;
    assert_eq!(
        svc["template"]["containers"][0]["ports"][0]["containerPort"],
        65535
    );
    let _ = std::fs::remove_dir_all(dir);
}

// ── regression tests for the seventeenth review round ───────────────────────

#[tokio::test]
async fn published_urls_follow_the_current_port_after_restart() {
    let dir = temp_dir("portchange");
    let first = Arc::new(Server::new(config(&dir, "127.0.0.1:18095")));
    let (_, op) = call(
        &first,
        "POST",
        &format!("{PARENT}/services?serviceId=web"),
        service_body("v1"),
    )
    .await;
    let rev = op["response"]["latestCreatedRevision"]
        .as_str()
        .unwrap()
        .to_string();
    drop(first);

    let restarted = Arc::new(Server::new(config(&dir, "127.0.0.1:18096")));
    let (_, svc) = call(
        &restarted,
        "GET",
        &format!("{PARENT}/services/web"),
        Value::Null,
    )
    .await;
    assert_eq!(
        svc["uri"],
        "http://web.us-central1.demo.run.localhost:18096"
    );
    assert_eq!(
        svc["urls"][1],
        "http://127.0.0.1:18096/_run/demo/us-central1/web/"
    );
    let (_, list) = call(
        &restarted,
        "GET",
        &format!("{PARENT}/services"),
        Value::Null,
    )
    .await;
    assert_eq!(
        list["services"][0]["uri"],
        "http://web.us-central1.demo.run.localhost:18096"
    );
    let (_, revision) = call(&restarted, "GET", &format!("/v2/{rev}"), Value::Null).await;
    assert!(revision["logUri"]
        .as_str()
        .unwrap()
        .starts_with("http://127.0.0.1:18096/"));
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test]
async fn injected_header_bytes_in_routing_names_never_reach_a_response() {
    let (_server, port, dir, tx, task) = serve_on_free_port("crlf").await;
    for target in [
        "/_run/p%0D%0AX-Injected%3A%20yes/us-central1/web",
        "/_run/p/us-central1/web?a%0D%0AX-Injected:%20yes",
    ] {
        let (_, raw) = raw_http_head(
            port,
            format!("GET {target} HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\n\r\n"),
        )
        .await;
        let head = raw.split("\r\n\r\n").next().unwrap_or("");
        assert!(
            !head.lines().any(|l| l.starts_with("X-Injected")),
            "{target}: {raw}"
        );
    }
    let _ = tx.send(());
    task.await.unwrap().unwrap();
    let _ = std::fs::remove_dir_all(dir);
}

// ── regression tests for the twenty-first review round ──────────────────────

#[tokio::test]
async fn image_references_cannot_pose_as_docker_options() {
    let dir = temp_dir("imageflag");
    let server = Arc::new(Server::new(config(&dir, "127.0.0.1:18095")));
    for bad in ["--privileged", "-v", "alpine latest", "alpine\nx"] {
        let body = json!({ "template": { "containers": [{ "image": bad, "args": ["alpine"] }] } });
        let (status, err) = call(
            &server,
            "POST",
            &format!("{PARENT}/services?serviceId=web"),
            body,
        )
        .await;
        assert_eq!(status, 400, "{bad:?}: {err}");
    }
    let ok = json!({ "template": { "containers": [{ "image": "us-docker.pkg.dev/p/repo/app@sha256:abc" }] } });
    let (status, _) = call(
        &server,
        "POST",
        &format!("{PARENT}/services?serviceId=web"),
        ok,
    )
    .await;
    assert_eq!(status, 200);
    let _ = std::fs::remove_dir_all(dir);
}

// ── regression tests for the twenty-fourth review round ─────────────────────

#[tokio::test]
async fn non_string_argv_and_env_entries_are_rejected() {
    let dir = temp_dir("argv");
    let server = Arc::new(Server::new(config(&dir, "127.0.0.1:18095")));
    for bad in [
        json!({ "image": "x", "command": [null, "python3", "-c", "print(1)"] }),
        json!({ "image": "x", "args": ["--port", 8080] }),
        json!({ "image": "x", "command": "python3 app.py" }),
        json!({ "image": "x", "env": [{ "name": "A", "value": 1 }] }),
        json!({ "image": "x", "env": [{ "value": "nameless" }] }),
        json!({ "image": "x", "workingDir": 7 }),
    ] {
        let body = json!({ "template": { "containers": [bad.clone()] } });
        let (status, err) = call(
            &server,
            "POST",
            &format!("{PARENT}/services?serviceId=web"),
            body,
        )
        .await;
        assert_eq!(status, 400, "{bad}: {err}");
    }
    let (status, _) = call(
        &server,
        "POST",
        &format!("{PARENT}/services?serviceId=web"),
        service_body("v1"),
    )
    .await;
    assert_eq!(status, 200);
    let patch =
        json!({ "template": { "containers": [{ "image": "x", "command": [null, "sh"] }] } });
    let (status, _) = call(&server, "PATCH", &format!("{PARENT}/services/web"), patch).await;
    assert_eq!(status, 400);
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test]
async fn domain_scoped_projects_publish_a_reachable_uri() {
    let dir = temp_dir("domainproj");
    let server = Arc::new(Server::new(config(&dir, "127.0.0.1:18095")));
    let (status, op) = call(
        &server,
        "POST",
        "/v2/projects/example.com:proj/locations/us-central1/services?serviceId=web",
        service_body("v1"),
    )
    .await;
    assert_eq!(status, 200, "{op}");
    let svc = &op["response"];
    assert_eq!(
        svc["uri"],
        "http://127.0.0.1:18095/_run/example.com:proj/us-central1/web/"
    );
    assert_eq!(
        svc["urls"],
        json!(["http://127.0.0.1:18095/_run/example.com:proj/us-central1/web/"])
    );

    let (_, plain) = call(
        &server,
        "POST",
        &format!("{PARENT}/services?serviceId=web"),
        service_body("v1"),
    )
    .await;
    assert_eq!(
        plain["response"]["uri"],
        "http://web.us-central1.demo.run.localhost:18095"
    );
    let _ = std::fs::remove_dir_all(dir);
}

// ── regression tests for the twenty-fifth review round ──────────────────────

#[tokio::test]
async fn chunked_admin_api_bodies_are_applied() {
    let (server, port, dir, tx, task) = serve_on_free_port("chunkedadmin").await;
    call(
        &server,
        "POST",
        &format!("{PARENT}/services?serviceId=web"),
        service_body("v1"),
    )
    .await;
    let patch = r#"{"labels":{"team":"chunked"}}"#;
    let (head_split, tail) = patch.split_at(10);
    let request = format!(
        "PATCH {PARENT}/services/web?updateMask=labels HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nTransfer-Encoding: chunked\r\n\r\n{:x}\r\n{head_split}\r\n{:x}\r\n{tail}\r\n0\r\n\r\n",
        head_split.len(),
        tail.len()
    );
    let (status, _) = raw_http(port, request).await;
    assert_eq!(status, 200);
    let (_, svc) = call(
        &server,
        "GET",
        &format!("{PARENT}/services/web"),
        Value::Null,
    )
    .await;
    assert_eq!(
        svc["labels"]["team"], "chunked",
        "the chunked body was applied, not ignored"
    );
    let (status, _) = raw_http(
        port,
        format!("PATCH {PARENT}/services/web HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nTransfer-Encoding: br\r\n\r\n"),
    )
    .await;
    assert_eq!(status, 501);
    let _ = tx.send(());
    task.await.unwrap().unwrap();
    let _ = std::fs::remove_dir_all(dir);
}

/// Echoes the request headers as a JSON list of `[name, value]` pairs.
const HEADER_APP: &str = r#"
import http.server, json, os
class H(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        out = json.dumps([[k.lower(), v] for k, v in self.headers.items()]).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(out)))
        self.end_headers()
        self.wfile.write(out)
    def log_message(self, *a):
        pass
http.server.HTTPServer(("127.0.0.1", int(os.environ["PORT"])), H).serve_forever()
"#;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn proxied_requests_get_clean_forwarding_and_rebinding_hosts_are_refused() {
    if !has_python() {
        eprintln!("skipping: python3 not available");
        return;
    }
    let dir = temp_dir("fwd");
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = Arc::new(Server::new(config(&dir, &format!("127.0.0.1:{port}"))));
    let (tx, rx) = tokio::sync::oneshot::channel::<()>();
    let serve_task = tokio::spawn(serve(listener, Arc::clone(&server), async {
        let _ = rx.await;
    }));
    let body = json!({
        "template": { "containers": [{
            "image": "us-docker.pkg.dev/cloudrun/container/hello",
            "command": ["python3", "-c", HEADER_APP],
        }]}
    });
    let (status, _) = call(
        &server,
        "POST",
        &format!("{PARENT}/services?serviceId=hdr"),
        body,
    )
    .await;
    assert_eq!(status, 200);

    let host = format!("hdr.us-central1.demo.run.localhost:{port}");
    let (status, body) = raw_http(
        port,
        format!(
            "GET / HTTP/1.1\r\nHost: {host}\r\nX-Forwarded-For: 10.0.0.9\r\nX-Forwarded-Proto: https\r\nTE: trailers\r\nProxy-Authorization: Basic eDp5\r\nX-Secret-Hop: 1\r\nConnection: X-Secret-Hop, Host\r\nUpgrade: h2c\r\nX-Kept: yes\r\n\r\n"
        ),
    )
    .await;
    assert_eq!(status, 200, "{body}");
    let headers: Vec<(String, String)> = serde_json::from_str(&body).unwrap();
    let all = |name: &str| -> Vec<&str> {
        headers
            .iter()
            .filter(|(k, _)| k == name)
            .map(|(_, v)| v.as_str())
            .collect()
    };
    assert_eq!(all("x-forwarded-for"), vec!["10.0.0.9, 127.0.0.1"]);
    assert_eq!(all("x-forwarded-proto"), vec!["http"]);
    for gone in ["te", "proxy-authorization", "x-secret-hop", "upgrade"] {
        assert!(all(gone).is_empty(), "{gone} was forwarded: {headers:?}");
    }
    assert_eq!(all("x-kept"), vec!["yes"]);
    // Naming a framing/routing header in Connection does not drop it.
    assert_eq!(all("host"), vec![host.as_str()]);

    // A DNS-rebinding page keeps its own Host: refused on both planes.
    for target in ["/", &format!("{PARENT}/services")] {
        let (status, _) = raw_http(
            port,
            format!("GET {target} HTTP/1.1\r\nHost: evil.example.com:{port}\r\n\r\n"),
        )
        .await;
        assert_eq!(status, 403, "{target}");
    }

    let _ = tx.send(());
    serve_task.await.unwrap().unwrap();
    let _ = std::fs::remove_dir_all(dir);
}
