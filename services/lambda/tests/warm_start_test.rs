//! Execution-environment reuse (warm starts): module state survives between
//! invocations of one environment, cold starts report `Init Duration`, and
//! environments are stopped on idle timeout, reconfiguration, failure, and
//! shutdown. Tests skip themselves when the interpreter is absent.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use base64::Engine;
use devcloud_lambda::http::{process, Request};
use devcloud_lambda::zip::build_stored;
use devcloud_lambda::{Config, Server};
use serde_json::{json, Value};

struct Env {
    server: Arc<Server>,
    dir: PathBuf,
}

impl Drop for Env {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn env_with(tag: &str, idle_timeout: Option<Duration>) -> Env {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let dir = std::env::temp_dir().join(format!(
        "devcloud-lambda-warm-{tag}-{}-{nanos}",
        std::process::id()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let server = Arc::new(Server::new(Config {
        region: "us-east-1".into(),
        account_id: "000000000000".into(),
        auth_mode: "relaxed".into(),
        storage_path: dir.join("lambda").to_string_lossy().into_owned(),
        idle_timeout,
        ..Config::default()
    }));
    Env { server, dir }
}

fn has(bin: &str) -> bool {
    std::process::Command::new(bin)
        .arg("--version")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

fn pid_alive(pid: i64) -> bool {
    std::process::Command::new("kill")
        .args(["-0", &pid.to_string()])
        .stderr(std::process::Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

/// True once `pid` is gone; a killed child may stay a zombie until it is
/// reaped in the background, so poll for a while.
async fn gone(pid: i64) -> bool {
    for _ in 0..60 {
        if !pid_alive(pid) {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    false
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
    runtime: &str,
    handler: &str,
    file: (&str, &[u8]),
    timeout: i64,
) {
    let zip = build_stored(&[file]);
    let body = json!({
        "FunctionName": name,
        "Runtime": runtime,
        "Role": "r",
        "Handler": handler,
        "Timeout": timeout,
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

async fn invoke(server: &Arc<Server>, name: &str, payload: &[u8]) -> Resp {
    let r = call(
        server,
        "POST",
        &format!("/2015-03-31/functions/{name}/invocations"),
        &[("X-Amz-Log-Type", "Tail")],
        payload,
    )
    .await;
    assert_eq!(r.status, 200, "{}", String::from_utf8_lossy(&r.body));
    r
}

/// Counts invocations per process; `sleep` (seconds) delays the reply.
const PY_COUNTER: &[u8] = b"import os, time\nprint('init ran')\nCOUNT = 0\ndef handler(event, context):\n    global COUNT\n    COUNT += 1\n    time.sleep(event.get('sleep', 0))\n    print('invocation', COUNT)\n    return {'pid': os.getpid(), 'count': COUNT, 'requestId': context.aws_request_id}\n";

const NODE_COUNTER: &[u8] = b"console.log('init ran');\nlet count = 0;\nexports.handler = async (event, context) => {\n  count += 1;\n  console.log('invocation', count);\n  return { pid: process.pid, count, requestId: context.awsRequestId };\n};\n";

#[tokio::test]
async fn module_state_survives_between_invocations() {
    for (runtime, handler, file, bin) in [
        (
            "python3.12",
            "app.handler",
            ("app.py", PY_COUNTER),
            "python3",
        ),
        (
            "nodejs20.x",
            "index.handler",
            ("index.js", NODE_COUNTER),
            "node",
        ),
    ] {
        if !has(bin) {
            eprintln!("skipping {runtime}: {bin} not available");
            continue;
        }
        let e = env_with("reuse", None);
        create(&e.server, "warm-fn", runtime, handler, file, 10).await;

        let first = invoke(&e.server, "warm-fn", b"{}").await;
        let second = invoke(&e.server, "warm-fn", b"{}").await;
        let (a, b) = (first.json(), second.json());
        assert_eq!(a["pid"], b["pid"], "{runtime}: same process serves both");
        assert_eq!(
            (a["count"].as_i64(), b["count"].as_i64()),
            (Some(1), Some(2)),
            "{runtime}"
        );
        assert_ne!(
            a["requestId"], b["requestId"],
            "{runtime}: context is per invocation"
        );

        let cold = first.log();
        assert!(cold.starts_with("INIT_START Runtime Version: "), "{cold}");
        assert!(cold.contains("init ran"), "{cold}");
        assert!(cold.contains("Init Duration: "), "{cold}");
        assert!(cold.contains("invocation 1"), "{cold}");
        let warm = second.log();
        assert!(warm.starts_with("START RequestId: "), "{warm}");
        assert!(!warm.contains("Init Duration"), "{warm}");
        assert!(
            !warm.contains("init ran") && !warm.contains("invocation 1"),
            "{warm}"
        );
        assert!(warm.contains("invocation 2"), "{warm}");
        assert_eq!(e.server.idle_environments("warm-fn"), 1);
    }
}

#[tokio::test]
async fn zero_idle_timeout_cold_starts_every_invocation() {
    if !has("python3") {
        eprintln!("skipping: python3 not available");
        return;
    }
    let e = env_with("zero", Some(Duration::ZERO));
    create(
        &e.server,
        "cold-fn",
        "python3.12",
        "app.handler",
        ("app.py", PY_COUNTER),
        10,
    )
    .await;
    let a = invoke(&e.server, "cold-fn", b"{}").await;
    let b = invoke(&e.server, "cold-fn", b"{}").await;
    assert_ne!(a.json()["pid"], b.json()["pid"]);
    assert_eq!(b.json()["count"], 1);
    assert!(b.log().contains("Init Duration: "));
    assert_eq!(e.server.idle_environments("cold-fn"), 0);
    assert!(gone(a.json()["pid"].as_i64().unwrap()).await);
}

#[tokio::test]
async fn idle_environments_stop_after_the_timeout() {
    if !has("python3") {
        eprintln!("skipping: python3 not available");
        return;
    }
    let e = env_with("expire", Some(Duration::from_millis(300)));
    create(
        &e.server,
        "idle-fn",
        "python3.12",
        "app.handler",
        ("app.py", PY_COUNTER),
        10,
    )
    .await;
    let pid = invoke(&e.server, "idle-fn", b"{}").await.json()["pid"]
        .as_i64()
        .unwrap();
    assert!(pid_alive(pid));
    assert_eq!(e.server.idle_environments("idle-fn"), 1);
    tokio::time::sleep(Duration::from_millis(800)).await;
    assert_eq!(e.server.idle_environments("idle-fn"), 0);
    assert!(gone(pid).await, "the idle environment was stopped");
    let next = invoke(&e.server, "idle-fn", b"{}").await;
    assert_eq!(next.json()["count"], 1, "a cold start after expiry");
}

#[tokio::test]
async fn reconfiguring_or_deleting_a_function_stops_its_environments() {
    if !has("python3") {
        eprintln!("skipping: python3 not available");
        return;
    }
    let e = env_with("reconf", None);
    create(
        &e.server,
        "conf-fn",
        "python3.12",
        "app.handler",
        ("app.py", PY_COUNTER),
        10,
    )
    .await;
    let pid = invoke(&e.server, "conf-fn", b"{}").await.json()["pid"]
        .as_i64()
        .unwrap();
    let r = call(
        &e.server,
        "PUT",
        "/2015-03-31/functions/conf-fn/configuration",
        &[],
        json!({ "Environment": { "Variables": { "MODE": "new" } } })
            .to_string()
            .as_bytes(),
    )
    .await;
    assert_eq!(r.status, 200, "{}", String::from_utf8_lossy(&r.body));
    assert_eq!(e.server.idle_environments("conf-fn"), 0);
    assert!(
        gone(pid).await,
        "the old revision's environment was stopped"
    );
    let after = invoke(&e.server, "conf-fn", b"{}").await.json();
    assert_eq!(after["count"], 1, "the new configuration starts cold");

    let pid = after["pid"].as_i64().unwrap();
    let r = call(
        &e.server,
        "DELETE",
        "/2015-03-31/functions/conf-fn",
        &[],
        b"",
    )
    .await;
    assert_eq!(r.status, 204);
    assert!(
        gone(pid).await,
        "deleting the function stopped its environment"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_invocations_get_separate_environments() {
    if !has("python3") {
        eprintln!("skipping: python3 not available");
        return;
    }
    let e = env_with("concurrent", None);
    create(
        &e.server,
        "par-fn",
        "python3.12",
        "app.handler",
        ("app.py", PY_COUNTER),
        10,
    )
    .await;
    let (a, b) = tokio::join!(
        invoke(&e.server, "par-fn", br#"{"sleep": 0.5}"#),
        invoke(&e.server, "par-fn", br#"{"sleep": 0.5}"#),
    );
    assert_ne!(a.json()["pid"], b.json()["pid"]);
    assert_eq!(e.server.idle_environments("par-fn"), 2);
}

#[tokio::test]
async fn timeouts_and_crashes_discard_the_environment() {
    if !has("python3") {
        eprintln!("skipping: python3 not available");
        return;
    }
    let e = env_with("discard", None);
    let code = b"import os, time\ndef handler(event, context):\n    if event.get('crash'):\n        os._exit(3)\n    time.sleep(event.get('sleep', 0))\n    return os.getpid()\n";
    create(
        &e.server,
        "fragile-fn",
        "python3.12",
        "app.handler",
        ("app.py", code),
        1,
    )
    .await;
    let pid = invoke(&e.server, "fragile-fn", b"{}")
        .await
        .json()
        .as_i64()
        .unwrap();

    let timed_out = invoke(&e.server, "fragile-fn", br#"{"sleep": 5}"#).await;
    assert_eq!(timed_out.header("X-Amz-Function-Error"), Some("Unhandled"));
    assert_eq!(timed_out.json()["errorType"], "Sandbox.Timedout");
    assert!(gone(pid).await, "a timed-out environment is stopped");
    assert_eq!(e.server.idle_environments("fragile-fn"), 0);

    invoke(&e.server, "fragile-fn", b"{}").await;
    let crashed = invoke(&e.server, "fragile-fn", br#"{"crash": true}"#).await;
    assert_eq!(crashed.json()["errorType"], "Runtime.ExitError");
    assert_eq!(e.server.idle_environments("fragile-fn"), 0);
    let fresh = invoke(&e.server, "fragile-fn", b"{}").await;
    assert!(fresh.log().contains("Init Duration: "), "{}", fresh.log());
}

#[tokio::test]
async fn init_failures_are_reported_and_not_kept() {
    if !has("python3") {
        eprintln!("skipping: python3 not available");
        return;
    }
    let e = env_with("initfail", None);
    create(
        &e.server,
        "broken-fn",
        "python3.12",
        "app.handler",
        ("app.py", b"import does_not_exist\n"),
        10,
    )
    .await;
    for _ in 0..2 {
        let r = invoke(&e.server, "broken-fn", b"{}").await;
        assert_eq!(r.json()["errorType"], "Runtime.ImportModuleError");
        assert!(r.log().contains("Init Duration: "), "{}", r.log());
        assert_eq!(e.server.idle_environments("broken-fn"), 0);
    }
}

#[tokio::test]
async fn handlers_cannot_read_the_control_channel() {
    if !has("python3") {
        eprintln!("skipping: python3 not available");
        return;
    }
    let e = env_with("stdin", None);
    let code = b"import sys\ndef handler(event, context):\n    return sys.stdin.read()\n";
    create(
        &e.server,
        "stdin-fn",
        "python3.12",
        "app.handler",
        ("app.py", code),
        10,
    )
    .await;
    assert_eq!(invoke(&e.server, "stdin-fn", b"{}").await.body, b"\"\"");
    assert_eq!(invoke(&e.server, "stdin-fn", b"{}").await.body, b"\"\"");
    if has("node") {
        let code = b"exports.handler = async () => require('fs').readFileSync(0, 'utf8');\n";
        create(
            &e.server,
            "stdin-node",
            "nodejs20.x",
            "index.handler",
            ("index.js", code),
            3,
        )
        .await;
        for _ in 0..2 {
            let r = invoke(&e.server, "stdin-node", b"{}").await;
            assert_eq!(r.header("X-Amz-Function-Error"), None, "{}", r.log());
            assert_eq!(r.body, b"\"\"", "stdin is empty, not the control channel");
        }
    }
}

#[tokio::test]
async fn shutdown_stops_idle_environments() {
    if !has("python3") {
        eprintln!("skipping: python3 not available");
        return;
    }
    let e = env_with("shutdown", None);
    create(
        &e.server,
        "bye-fn",
        "python3.12",
        "app.handler",
        ("app.py", PY_COUNTER),
        10,
    )
    .await;
    let pid = invoke(&e.server, "bye-fn", b"{}").await.json()["pid"]
        .as_i64()
        .unwrap();
    assert!(pid_alive(pid));
    e.server.shutdown().await;
    assert!(gone(pid).await);
    assert_eq!(e.server.idle_environments("bye-fn"), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn environments_busy_during_an_update_or_delete_are_not_kept() {
    if !has("python3") {
        eprintln!("skipping: python3 not available");
        return;
    }
    let e = env_with("retire", None);
    create(
        &e.server,
        "busy-fn",
        "python3.12",
        "app.handler",
        ("app.py", PY_COUNTER),
        10,
    )
    .await;
    for change in ["update", "delete"] {
        if change == "delete" {
            // Recreate a fresh revision to delete.
            invoke(&e.server, "busy-fn", b"{}").await;
        }
        let server = Arc::clone(&e.server);
        let running =
            tokio::spawn(async move { invoke(&server, "busy-fn", br#"{"sleep": 0.5}"#).await });
        tokio::time::sleep(Duration::from_millis(200)).await;
        let r = if change == "update" {
            call(
                &e.server,
                "PUT",
                "/2015-03-31/functions/busy-fn/configuration",
                &[],
                json!({ "Description": "changed" }).to_string().as_bytes(),
            )
            .await
        } else {
            call(
                &e.server,
                "DELETE",
                "/2015-03-31/functions/busy-fn",
                &[],
                b"",
            )
            .await
        };
        assert!(matches!(r.status, 200 | 204), "{change}: {}", r.status);
        let pid = running.await.unwrap().json()["pid"].as_i64().unwrap();
        assert_eq!(e.server.idle_environments("busy-fn"), 0, "{change}");
        assert!(
            gone(pid).await,
            "{change}: the retired environment was stopped"
        );
    }
}
