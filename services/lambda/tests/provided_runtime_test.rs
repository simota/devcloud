//! `provided*` (custom runtime) functions: the package's `bootstrap` talks to
//! devcloud's Lambda Runtime API. The bootstrap here is a small Python script
//! (skipped when `python3` is absent) that follows the same protocol as
//! `aws-lambda-go` and the other runtime interface clients.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use base64::Engine;
use devcloud_lambda::http::{process, Request};
use devcloud_lambda::zip::build_stored_with_modes;
use devcloud_lambda::{Config, Server};
use serde_json::{json, Value};

const EXECUTABLE: u32 = 0o100755;

/// A Runtime API client. `event["mode"]` selects the outcome: "error" posts
/// to /error, "sleep" sleeps past the timeout, "exit" exits mid-invocation;
/// anything else echoes the event with a per-process counter. With
/// `INIT_FAIL` set, it reports an init error instead of polling.
const BOOTSTRAP: &str = r#"#!/usr/bin/env python3
import json, os, sys, time, urllib.request

api = "http://" + os.environ["AWS_LAMBDA_RUNTIME_API"] + "/2018-06-01/runtime"

def post(path, body, headers=None):
    req = urllib.request.Request(api + path, data=json.dumps(body).encode(), method="POST", headers=headers or {})
    urllib.request.urlopen(req).read()

print("init ran", flush=True)
if os.environ.get("INIT_FAIL"):
    post("/init/error", {"errorMessage": "cannot init", "errorType": "Runtime.InitError"})
    sys.exit(1)

count = 0
while True:
    with urllib.request.urlopen(api + "/invocation/next") as r:
        request_id = r.headers["Lambda-Runtime-Aws-Request-Id"]
        deadline = int(r.headers["Lambda-Runtime-Deadline-Ms"])
        arn = r.headers["Lambda-Runtime-Invoked-Function-Arn"]
        event = json.loads(r.read() or b"{}")
    count += 1
    print("handling", count, event.get("tag"), flush=True)
    mode = event.get("mode")
    if mode == "error":
        post("/invocation/%s/error" % request_id, {"errorMessage": "boom", "errorType": "MyError"},
             {"Lambda-Runtime-Function-Error-Type": "MyError"})
    elif mode == "sleep":
        time.sleep(30)
    elif mode == "exit":
        sys.exit(3)
    else:
        post("/invocation/%s/response" % request_id, {
            "event": event, "count": count, "pid": os.getpid(), "requestId": request_id,
            "remainingMs": deadline - int(time.time() * 1000), "arn": arn,
            "runtimeApi": os.environ["AWS_LAMBDA_RUNTIME_API"],
            "executionEnv": os.environ.get("AWS_EXECUTION_ENV"),
            "devcloudVars": [k for k in os.environ if k.startswith("_DEVCLOUD_")],
        })
"#;

struct Env {
    server: Arc<Server>,
    dir: PathBuf,
}

impl Drop for Env {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn env(tag: &str) -> Env {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let dir = std::env::temp_dir().join(format!(
        "devcloud-lambda-provided-{tag}-{}-{nanos}",
        std::process::id()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let server = Arc::new(Server::new(Config {
        region: "us-east-1".into(),
        account_id: "000000000000".into(),
        auth_mode: "relaxed".into(),
        storage_path: dir.join("lambda").to_string_lossy().into_owned(),
        ..Config::default()
    }));
    Env { server, dir }
}

fn has_python() -> bool {
    std::process::Command::new("python3")
        .arg("--version")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

struct Resp {
    status: u16,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}

impl Resp {
    fn json(&self) -> Value {
        serde_json::from_slice(&self.body).unwrap_or(Value::Null)
    }
    fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }
    fn log(&self) -> String {
        let raw = base64::engine::general_purpose::STANDARD
            .decode(self.header("X-Amz-Log-Result").unwrap_or_default())
            .unwrap();
        String::from_utf8(raw).unwrap()
    }
}

async fn call(
    server: &Arc<Server>,
    method: &str,
    path: &str,
    headers: &[(&str, &str)],
    body: &[u8],
) -> Resp {
    let req = Request {
        method: method.into(),
        raw_path: path.into(),
        query: String::new(),
        headers: headers
            .iter()
            .map(|(k, v)| (k.to_ascii_lowercase(), v.to_string()))
            .collect::<HashMap<_, _>>(),
        body: body.to_vec(),
    };
    let r = process(server, &req).await;
    Resp {
        status: r.status,
        headers: r.headers,
        body: r.body,
    }
}

async fn create(
    server: &Arc<Server>,
    name: &str,
    files: &[(&str, &[u8], u32)],
    timeout: i64,
    vars: Value,
) {
    let zip = build_stored_with_modes(files);
    let body = json!({
        "FunctionName": name,
        "Runtime": "provided.al2023",
        "Role": "r",
        "Handler": "bootstrap",
        "Timeout": timeout,
        "Environment": { "Variables": vars },
        "Code": { "ZipFile": base64::engine::general_purpose::STANDARD.encode(&zip) },
    });
    let r = call(
        server,
        "POST",
        "/2015-03-31/functions",
        &[],
        body.to_string().as_bytes(),
    )
    .await;
    assert_eq!(r.status, 201, "{}", String::from_utf8_lossy(&r.body));
}

async fn invoke(server: &Arc<Server>, name: &str, payload: Value) -> Resp {
    let r = call(
        server,
        "POST",
        &format!("/2015-03-31/functions/{name}/invocations"),
        &[("X-Amz-Log-Type", "Tail")],
        payload.to_string().as_bytes(),
    )
    .await;
    assert_eq!(r.status, 200, "{}", String::from_utf8_lossy(&r.body));
    r
}

#[tokio::test]
async fn bootstrap_serves_invocations_through_the_runtime_api() {
    if !has_python() {
        eprintln!("skipping: python3 not available");
        return;
    }
    let e = env("serve");
    create(
        &e.server,
        "custom",
        &[("bootstrap", BOOTSTRAP.as_bytes(), EXECUTABLE)],
        10,
        json!({}),
    )
    .await;

    let first = invoke(&e.server, "custom", json!({"tag": "one"})).await;
    assert!(
        first.header("X-Amz-Function-Error").is_none(),
        "{}",
        first.log()
    );
    let a = first.json();
    assert_eq!(a["event"], json!({"tag": "one"}));
    assert_eq!(a["count"], 1);
    assert_eq!(a["requestId"], first.header("X-Amzn-RequestId").unwrap());
    assert_eq!(
        a["arn"],
        "arn:aws:lambda:us-east-1:000000000000:function:custom"
    );
    assert!(a["remainingMs"].as_i64().unwrap() > 5_000);
    assert!(a["runtimeApi"].as_str().unwrap().starts_with("127.0.0.1:"));
    assert_eq!(a["executionEnv"], Value::Null);
    assert_eq!(a["devcloudVars"], json!([]));

    let log = first.log();
    assert!(log.contains("INIT_START"), "{log}");
    assert!(log.contains("init ran"), "{log}");
    assert!(log.contains("handling 1 one"), "{log}");
    assert!(log.contains("Init Duration"), "{log}");

    // Warm start: same process, its own slice of the log.
    let second = invoke(&e.server, "custom", json!({"tag": "two"})).await;
    let b = second.json();
    assert_eq!(b["pid"], a["pid"]);
    assert_eq!(b["count"], 2);
    let log = second.log();
    assert!(log.contains("handling 2 two"), "{log}");
    assert!(!log.contains("handling 1"), "{log}");
    assert!(!log.contains("INIT_START"), "{log}");
}

#[tokio::test]
async fn function_error_keeps_the_environment() {
    if !has_python() {
        eprintln!("skipping: python3 not available");
        return;
    }
    let e = env("error");
    create(
        &e.server,
        "custom",
        &[("bootstrap", BOOTSTRAP.as_bytes(), EXECUTABLE)],
        10,
        json!({}),
    )
    .await;
    let failed = invoke(&e.server, "custom", json!({"mode": "error"})).await;
    assert_eq!(failed.header("X-Amz-Function-Error"), Some("Unhandled"));
    assert_eq!(
        failed.json(),
        json!({"errorMessage": "boom", "errorType": "MyError"})
    );

    let ok = invoke(&e.server, "custom", json!({})).await;
    assert!(ok.header("X-Amz-Function-Error").is_none());
    assert_eq!(
        ok.json()["count"],
        2,
        "same environment after a function error"
    );
}

#[tokio::test]
async fn init_error_is_reported() {
    if !has_python() {
        eprintln!("skipping: python3 not available");
        return;
    }
    let e = env("init");
    create(
        &e.server,
        "custom",
        &[("bootstrap", BOOTSTRAP.as_bytes(), EXECUTABLE)],
        10,
        json!({"INIT_FAIL": "1"}),
    )
    .await;
    let r = invoke(&e.server, "custom", json!({})).await;
    assert_eq!(r.header("X-Amz-Function-Error"), Some("Unhandled"));
    assert_eq!(r.json()["errorType"], "Runtime.InitError");
    assert!(r.log().contains("init ran"), "{}", r.log());
}

#[tokio::test]
async fn crash_and_timeout_end_the_environment() {
    if !has_python() {
        eprintln!("skipping: python3 not available");
        return;
    }
    let e = env("crash");
    create(
        &e.server,
        "custom",
        &[("bootstrap", BOOTSTRAP.as_bytes(), EXECUTABLE)],
        1,
        json!({}),
    )
    .await;

    let crashed = invoke(&e.server, "custom", json!({"mode": "exit"})).await;
    assert_eq!(crashed.json()["errorType"], "Runtime.ExitError");
    assert!(crashed.json()["errorMessage"]
        .as_str()
        .unwrap()
        .ends_with("exit status 3"));

    let timed_out = invoke(&e.server, "custom", json!({"mode": "sleep"})).await;
    assert_eq!(timed_out.json()["errorType"], "Sandbox.Timedout");

    // Each failure discarded its environment: the next call is a cold start.
    let ok = invoke(&e.server, "custom", json!({})).await;
    assert_eq!(ok.json()["count"], 1);
    assert!(ok.log().contains("INIT_START"));
}

#[tokio::test]
async fn missing_or_foreign_bootstrap_is_an_invalid_entrypoint() {
    let e = env("entrypoint");
    create(
        &e.server,
        "nobootstrap",
        &[("main", b"x", EXECUTABLE)],
        3,
        json!({}),
    )
    .await;
    let r = invoke(&e.server, "nobootstrap", json!({})).await;
    assert_eq!(r.header("X-Amz-Function-Error"), Some("Unhandled"));
    let doc = r.json();
    assert_eq!(doc["errorType"], "Runtime.InvalidEntrypoint");
    assert!(doc["errorMessage"]
        .as_str()
        .unwrap()
        .contains("Couldn't find valid bootstrap(s)"));

    // A binary for another OS (e.g. a Linux build on macOS).
    let garbage: &[u8] = if cfg!(target_os = "linux") {
        &[0xcf, 0xfa, 0xed, 0xfe, 0, 0, 0, 0, 1, 2, 3, 4] // Mach-O
    } else {
        &[0x7f, b'E', b'L', b'F', 0, 0, 0, 0, 1, 2, 3, 4] // ELF
    };
    create(
        &e.server,
        "foreign",
        &[("bootstrap", garbage, EXECUTABLE)],
        3,
        json!({}),
    )
    .await;
    let r = invoke(&e.server, "foreign", json!({})).await;
    let doc = r.json();
    assert_eq!(doc["errorType"], "Runtime.InvalidEntrypoint");
    assert!(
        doc["errorMessage"]
            .as_str()
            .unwrap()
            .contains("exec format error"),
        "{doc}"
    );
    assert!(
        r.log().contains("not an executable for this host"),
        "{}",
        r.log()
    );
}

#[tokio::test]
async fn concurrent_invocations_get_separate_environments() {
    if !has_python() {
        eprintln!("skipping: python3 not available");
        return;
    }
    let e = env("concurrent");
    create(
        &e.server,
        "custom",
        &[("bootstrap", BOOTSTRAP.as_bytes(), EXECUTABLE)],
        10,
        json!({}),
    )
    .await;
    let (a, b) = tokio::join!(
        invoke(&e.server, "custom", json!({"tag": "a"})),
        invoke(&e.server, "custom", json!({"tag": "b"})),
    );
    assert_eq!(a.json()["event"]["tag"], "a");
    assert_eq!(b.json()["event"]["tag"], "b");
    assert_ne!(a.json()["pid"], b.json()["pid"]);
    tokio::time::sleep(Duration::from_millis(10)).await;
}
