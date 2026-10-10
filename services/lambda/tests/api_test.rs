//! End-to-end tests of the Lambda REST surface through the full request
//! pipeline (`http::process`), including real python/node handler execution.
//! Runtime-execution tests skip themselves when the interpreter is absent.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

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

fn temp_dir(tag: &str) -> PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let dir = std::env::temp_dir().join(format!(
        "devcloud-lambda-{tag}-{}-{nanos}",
        std::process::id()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn config_for(dir: &std::path::Path) -> Config {
    Config {
        addr: "127.0.0.1:19010".into(),
        region: "us-east-1".into(),
        account_id: "000000000000".into(),
        auth_mode: "relaxed".into(),
        storage_path: dir.join("lambda").to_string_lossy().into_owned(),
        object_store_root: Some(dir.join("s3/buckets")),
        ..Config::default()
    }
}

fn env(tag: &str) -> Env {
    let dir = temp_dir(tag);
    Env {
        server: Arc::new(Server::new(config_for(&dir))),
        dir,
    }
}

fn has(bin: &str) -> bool {
    std::process::Command::new(bin)
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
}

async fn call(
    server: &Arc<Server>,
    method: &str,
    target: &str,
    headers: &[(&str, &str)],
    body: &[u8],
) -> Resp {
    let (raw_path, query) = match target.split_once('?') {
        Some((p, q)) => (p.to_string(), q.to_string()),
        None => (target.to_string(), String::new()),
    };
    let req = Request {
        method: method.into(),
        raw_path,
        query,
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

fn b64(data: &[u8]) -> String {
    base64::engine::general_purpose::STANDARD.encode(data)
}

const PY_HANDLER: &str = r#"
import os
def handler(event, context):
    print("hello from python")
    if event.get("fail"):
        raise ValueError("boom")
    return {"echo": event, "fn": context.function_name, "greeting": os.environ.get("GREETING"), "remaining": context.get_remaining_time_in_millis() > 0}
"#;

async fn create_python(server: &Arc<Server>, name: &str) -> Resp {
    let zip = build_stored(&[("app.py", PY_HANDLER.as_bytes())]);
    let body = json!({
        "FunctionName": name,
        "Runtime": "python3.12",
        "Role": "arn:aws:iam::000000000000:role/lambda",
        "Handler": "app.handler",
        "Timeout": 10,
        "Environment": { "Variables": { "GREETING": "hi" } },
        "Tags": { "team": "dev" },
        "Code": { "ZipFile": b64(&zip) },
    });
    call(
        server,
        "POST",
        "/2015-03-31/functions",
        &[],
        body.to_string().as_bytes(),
    )
    .await
}

#[tokio::test]
async fn function_crud_lifecycle() {
    let e = env("crud");
    let created = create_python(&e.server, "crud-fn").await;
    assert_eq!(
        created.status,
        201,
        "{}",
        String::from_utf8_lossy(&created.body)
    );
    let cfg = created.json();
    assert_eq!(
        cfg["FunctionArn"],
        "arn:aws:lambda:us-east-1:000000000000:function:crud-fn"
    );
    assert_eq!(cfg["State"], "Active");
    assert_eq!(cfg["Environment"]["Variables"]["GREETING"], "hi");
    assert_eq!(cfg["Version"], "$LATEST");

    let dup = create_python(&e.server, "crud-fn").await;
    assert_eq!(dup.status, 409);
    assert_eq!(
        dup.header("X-Amzn-ErrorType"),
        Some("ResourceConflictException")
    );

    let list = call(&e.server, "GET", "/2015-03-31/functions/", &[], b"").await;
    assert_eq!(list.json()["Functions"].as_array().unwrap().len(), 1);

    // Lookup by percent-encoded ARN, as the AWS SDKs send it.
    let got = call(
        &e.server,
        "GET",
        "/2015-03-31/functions/arn%3Aaws%3Alambda%3Aus-east-1%3A000000000000%3Afunction%3Acrud-fn",
        &[],
        b"",
    )
    .await;
    assert_eq!(got.status, 200);
    assert_eq!(got.json()["Tags"]["team"], "dev");
    assert!(got.json()["Code"]["Location"]
        .as_str()
        .unwrap()
        .contains("/_devcloud/functions/crud-fn/code.zip?CodeSha256="));

    let pkg = call(
        &e.server,
        "GET",
        "/_devcloud/functions/crud-fn/code.zip",
        &[],
        b"",
    )
    .await;
    assert_eq!(pkg.status, 200);
    assert!(pkg.body.starts_with(b"PK"));

    let updated = call(
        &e.server,
        "PUT",
        "/2015-03-31/functions/crud-fn/configuration",
        &[],
        br#"{"Timeout": 30, "Description": "updated"}"#,
    )
    .await;
    assert_eq!(updated.status, 200);
    assert_eq!(updated.json()["Timeout"], 30);
    assert_eq!(updated.json()["Description"], "updated");
    assert_ne!(updated.json()["RevisionId"], cfg["RevisionId"]);

    let stale = call(
        &e.server,
        "PUT",
        "/2015-03-31/functions/crud-fn/configuration",
        &[],
        format!(
            r#"{{"Timeout": 5, "RevisionId": "{}"}}"#,
            cfg["RevisionId"].as_str().unwrap()
        )
        .as_bytes(),
    )
    .await;
    assert_eq!(stale.status, 412);

    let qualified = call(
        &e.server,
        "GET",
        "/2015-03-31/functions/crud-fn?Qualifier=prod",
        &[],
        b"",
    )
    .await;
    assert_eq!(qualified.status, 404);

    // `$LATEST` cannot be deleted on its own; the function must survive.
    for target in [
        "/2015-03-31/functions/crud-fn?Qualifier=%24LATEST",
        "/2015-03-31/functions/crud-fn%3A%24LATEST",
    ] {
        let latest = call(&e.server, "DELETE", target, &[], b"").await;
        assert_eq!(latest.status, 400, "{target}");
        assert_eq!(
            latest.header("X-Amzn-ErrorType"),
            Some("InvalidParameterValueException")
        );
    }
    let still = call(&e.server, "GET", "/2015-03-31/functions/crud-fn", &[], b"").await;
    assert_eq!(still.status, 200);

    let deleted = call(
        &e.server,
        "DELETE",
        "/2015-03-31/functions/crud-fn",
        &[],
        b"",
    )
    .await;
    assert_eq!(deleted.status, 204);
    let gone = call(&e.server, "GET", "/2015-03-31/functions/crud-fn", &[], b"").await;
    assert_eq!(gone.status, 404);
    assert_eq!(
        gone.header("X-Amzn-ErrorType"),
        Some("ResourceNotFoundException")
    );
}

#[tokio::test]
async fn validation_errors() {
    let e = env("validation");
    let bad_name = call(
        &e.server,
        "POST",
        "/2015-03-31/functions",
        &[],
        br#"{"FunctionName":"bad name","Runtime":"python3.12","Role":"r","Handler":"a.b","Code":{"ZipFile":""}}"#,
    )
    .await;
    assert_eq!(bad_name.status, 400);
    assert_eq!(
        bad_name.header("X-Amzn-ErrorType"),
        Some("ValidationException")
    );

    let bad_zip = call(
        &e.server,
        "POST",
        "/2015-03-31/functions",
        &[],
        format!(r#"{{"FunctionName":"f","Runtime":"python3.12","Role":"r","Handler":"a.b","Code":{{"ZipFile":"{}"}}}}"#, b64(b"not a zip")).as_bytes(),
    )
    .await;
    assert_eq!(bad_zip.status, 400);
    assert_eq!(
        bad_zip.header("X-Amzn-ErrorType"),
        Some("InvalidParameterValueException")
    );

    let reserved = call(
        &e.server,
        "POST",
        "/2015-03-31/functions",
        &[],
        format!(r#"{{"FunctionName":"f","Runtime":"python3.12","Role":"r","Handler":"a.b","Environment":{{"Variables":{{"AWS_REGION":"x"}}}},"Code":{{"ZipFile":"{}"}}}}"#, b64(&build_stored(&[("a.py", b"")]))).as_bytes(),
    )
    .await;
    assert_eq!(reserved.status, 400);

    let bad_runtime = call(
        &e.server,
        "POST",
        "/2015-03-31/functions",
        &[],
        br#"{"FunctionName":"f","Runtime":"cobol85","Role":"r","Handler":"a.b","Code":{"ZipFile":""}}"#,
    )
    .await;
    assert_eq!(bad_runtime.status, 400);

    let unknown = call(&e.server, "GET", "/2099-01-01/nothing", &[], b"").await;
    assert_eq!(unknown.status, 404);
}

#[tokio::test]
async fn python_invoke_success_error_and_tail_log() {
    if !has("python3") {
        eprintln!("skipping: python3 not available");
        return;
    }
    let e = env("py");
    assert_eq!(create_python(&e.server, "py-fn").await.status, 201);

    let ok = call(
        &e.server,
        "POST",
        "/2015-03-31/functions/py-fn/invocations",
        &[("X-Amz-Log-Type", "Tail")],
        br#"{"n": 1}"#,
    )
    .await;
    assert_eq!(ok.status, 200, "{}", String::from_utf8_lossy(&ok.body));
    assert_eq!(ok.header("X-Amz-Function-Error"), None);
    let body = ok.json();
    assert_eq!(body["echo"]["n"], 1);
    assert_eq!(body["fn"], "py-fn");
    assert_eq!(body["greeting"], "hi");
    assert_eq!(body["remaining"], true);
    let log = base64::engine::general_purpose::STANDARD
        .decode(ok.header("X-Amz-Log-Result").unwrap())
        .unwrap();
    let log = String::from_utf8(log).unwrap();
    assert!(log.contains("START RequestId:"), "{log}");
    assert!(log.contains("hello from python"), "{log}");
    assert!(log.contains("REPORT RequestId:"), "{log}");

    let failed = call(
        &e.server,
        "POST",
        "/2015-03-31/functions/py-fn/invocations",
        &[],
        br#"{"fail": true}"#,
    )
    .await;
    assert_eq!(failed.status, 200);
    assert_eq!(failed.header("X-Amz-Function-Error"), Some("Unhandled"));
    assert_eq!(failed.json()["errorType"], "ValueError");
    assert_eq!(failed.json()["errorMessage"], "boom");

    let bad_json = call(
        &e.server,
        "POST",
        "/2015-03-31/functions/py-fn/invocations",
        &[],
        b"{nope",
    )
    .await;
    assert_eq!(bad_json.status, 400);
    assert_eq!(
        bad_json.header("X-Amzn-ErrorType"),
        Some("InvalidRequestContentException")
    );

    let dry = call(
        &e.server,
        "POST",
        "/2015-03-31/functions/py-fn/invocations",
        &[("X-Amz-Invocation-Type", "DryRun")],
        b"{}",
    )
    .await;
    assert_eq!(dry.status, 204);

    let records = call(&e.server, "GET", "/_introspect/invocations", &[], b"").await;
    let list = records.json()["invocations"].as_array().unwrap().clone();
    assert_eq!(list.len(), 2);
    assert_eq!(list[0]["status"], "Unhandled");
    assert_eq!(list[1]["status"], "Success");
}

#[tokio::test]
async fn python_timeout_and_async_event() {
    if !has("python3") {
        eprintln!("skipping: python3 not available");
        return;
    }
    let e = env("timeout");
    let zip = build_stored(&[(
        "slow.py",
        b"import time\ndef handler(event, context):\n    time.sleep(event.get('sleep', 0))\n    return 'done'\n",
    )]);
    let body = json!({
        "FunctionName": "slow",
        "Runtime": "python3.11",
        "Role": "r",
        "Handler": "slow.handler",
        "Timeout": 1,
        "Code": { "ZipFile": b64(&zip) },
    });
    assert_eq!(
        call(
            &e.server,
            "POST",
            "/2015-03-31/functions",
            &[],
            body.to_string().as_bytes()
        )
        .await
        .status,
        201
    );

    let timed_out = call(
        &e.server,
        "POST",
        "/2015-03-31/functions/slow/invocations",
        &[],
        br#"{"sleep": 5}"#,
    )
    .await;
    assert_eq!(timed_out.status, 200);
    assert_eq!(timed_out.header("X-Amz-Function-Error"), Some("Unhandled"));
    assert!(timed_out.json()["errorMessage"]
        .as_str()
        .unwrap()
        .contains("Task timed out after 1.00 seconds"));

    let fast = call(
        &e.server,
        "POST",
        "/2015-03-31/functions/slow/invocations",
        &[],
        b"",
    )
    .await;
    assert_eq!(fast.body, b"\"done\"");

    let queued = call(
        &e.server,
        "POST",
        "/2015-03-31/functions/slow/invocations",
        &[("X-Amz-Invocation-Type", "Event")],
        b"{}",
    )
    .await;
    assert_eq!(queued.status, 202);
    for _ in 0..100 {
        let records = call(&e.server, "GET", "/_introspect/invocations", &[], b"").await;
        if records.json()["invocations"]
            .as_array()
            .unwrap()
            .iter()
            .any(|r| r["invocationType"] == "Event")
        {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    panic!("async Event invocation never completed");
}

#[tokio::test]
async fn node_invoke_async_and_callback_handlers() {
    if !has("node") {
        eprintln!("skipping: node not available");
        return;
    }
    let e = env("node");
    let zip = build_stored(&[
        (
            "index.js",
            b"exports.handler = async (event, context) => { console.log('node log'); if (event.fail) throw new TypeError('bad input'); return { sum: event.a + event.b, fn: context.functionName }; };\nexports.cb = (event, context, callback) => callback(null, 'called back');\n",
        ),
        ("esm.mjs", b"export const handler = async () => ({ esm: true });\n"),
    ]);
    for (name, handler) in [
        ("node-fn", "index.handler"),
        ("node-cb", "index.cb"),
        ("node-esm", "esm.handler"),
    ] {
        let body = json!({
            "FunctionName": name,
            "Runtime": "nodejs20.x",
            "Role": "r",
            "Handler": handler,
            "Code": { "ZipFile": b64(&zip) },
        });
        let r = call(
            &e.server,
            "POST",
            "/2015-03-31/functions",
            &[],
            body.to_string().as_bytes(),
        )
        .await;
        assert_eq!(r.status, 201);
    }

    let ok = call(
        &e.server,
        "POST",
        "/2015-03-31/functions/node-fn/invocations",
        &[],
        br#"{"a": 2, "b": 3}"#,
    )
    .await;
    assert_eq!(ok.status, 200, "{}", String::from_utf8_lossy(&ok.body));
    assert_eq!(ok.json()["sum"], 5);
    assert_eq!(ok.json()["fn"], "node-fn");

    let failed = call(
        &e.server,
        "POST",
        "/2015-03-31/functions/node-fn/invocations",
        &[],
        br#"{"fail": true}"#,
    )
    .await;
    assert_eq!(failed.header("X-Amz-Function-Error"), Some("Unhandled"));
    assert_eq!(failed.json()["errorType"], "TypeError");
    assert_eq!(failed.json()["errorMessage"], "bad input");

    let cb = call(
        &e.server,
        "POST",
        "/2015-03-31/functions/node-cb/invocations",
        &[],
        b"",
    )
    .await;
    assert_eq!(cb.body, b"\"called back\"");

    let esm = call(
        &e.server,
        "POST",
        "/2015-03-31/functions/node-esm/invocations",
        &[],
        b"",
    )
    .await;
    assert_eq!(esm.json()["esm"], true);
}

#[tokio::test]
async fn update_code_changes_behavior_and_state_survives_restart() {
    if !has("python3") {
        eprintln!("skipping: python3 not available");
        return;
    }
    let e = env("persist");
    assert_eq!(create_python(&e.server, "persist-fn").await.status, 201);
    let v2 = build_stored(&[("app.py", b"def handler(event, context):\n    return 'v2'\n")]);
    let updated = call(
        &e.server,
        "PUT",
        "/2015-03-31/functions/persist-fn/code",
        &[],
        json!({ "ZipFile": b64(&v2) }).to_string().as_bytes(),
    )
    .await;
    assert_eq!(updated.status, 200);
    let sha = updated.json()["CodeSha256"].clone();

    // A fresh server over the same storage sees the function and its new code.
    let restarted = Arc::new(Server::new(config_for(&e.dir)));
    assert!(restarted.load_err().is_none());
    let cfg = call(
        &restarted,
        "GET",
        "/2015-03-31/functions/persist-fn/configuration",
        &[],
        b"",
    )
    .await;
    assert_eq!(cfg.json()["CodeSha256"], sha);
    let out = call(
        &restarted,
        "POST",
        "/2015-03-31/functions/persist-fn/invocations",
        &[],
        b"",
    )
    .await;
    assert_eq!(out.body, b"\"v2\"");
}

#[tokio::test]
async fn tags_and_s3_code_source() {
    let e = env("tags");
    let store = devcloud_s3::store::FileBucketStore::new(e.dir.join("s3/buckets"));
    store.create_bucket("artifacts").unwrap();
    let zip = build_stored(&[("app.py", PY_HANDLER.as_bytes())]);
    store
        .put_object(devcloud_s3::objops::PutObjectInput {
            bucket: "artifacts".into(),
            key: "fn.zip".into(),
            body: zip,
            ..Default::default()
        })
        .unwrap();
    let body = json!({
        "FunctionName": "s3-fn",
        "Runtime": "python3.12",
        "Role": "r",
        "Handler": "app.handler",
        "Code": { "S3Bucket": "artifacts", "S3Key": "fn.zip" },
    });
    let created = call(
        &e.server,
        "POST",
        "/2015-03-31/functions",
        &[],
        body.to_string().as_bytes(),
    )
    .await;
    assert_eq!(
        created.status,
        201,
        "{}",
        String::from_utf8_lossy(&created.body)
    );

    let missing = json!({
        "FunctionName": "s3-missing",
        "Runtime": "python3.12",
        "Role": "r",
        "Handler": "app.handler",
        "Code": { "S3Bucket": "artifacts", "S3Key": "nope.zip" },
    });
    let r = call(
        &e.server,
        "POST",
        "/2015-03-31/functions",
        &[],
        missing.to_string().as_bytes(),
    )
    .await;
    assert_eq!(r.status, 400);

    let arn = "arn%3Aaws%3Alambda%3Aus-east-1%3A000000000000%3Afunction%3As3-fn";
    let tag = call(
        &e.server,
        "POST",
        &format!("/2017-03-31/tags/{arn}"),
        &[],
        br#"{"Tags":{"a":"1","b":"2"}}"#,
    )
    .await;
    assert_eq!(tag.status, 204);
    let untag = call(
        &e.server,
        "DELETE",
        &format!("/2017-03-31/tags/{arn}?tagKeys=a"),
        &[],
        b"",
    )
    .await;
    assert_eq!(untag.status, 204);
    let list = call(
        &e.server,
        "GET",
        &format!("/2017-03-31/tags/{arn}"),
        &[],
        b"",
    )
    .await;
    assert_eq!(list.json()["Tags"], json!({ "b": "2" }));

    // Tags belong to the function: a qualified ARN is rejected, not
    // silently applied to the unqualified function.
    let qualified = call(
        &e.server,
        "POST",
        &format!("/2017-03-31/tags/{arn}%3A%24LATEST"),
        &[],
        br#"{"Tags":{"c":"3"}}"#,
    )
    .await;
    assert_eq!(qualified.status, 400);
    assert_eq!(
        qualified.header("X-Amzn-ErrorType"),
        Some("InvalidParameterValueException")
    );

    // A delete marker version has no package: it is a missing version, not
    // a corrupt zip.
    store.put_bucket_versioning("artifacts", "Enabled").unwrap();
    let (marker, _) = store
        .delete_object_with_result("artifacts", "fn.zip", false)
        .unwrap();
    assert!(marker.delete_marker);
    let from_marker = json!({
        "FunctionName": "s3-marker",
        "Runtime": "python3.12",
        "Role": "r",
        "Handler": "app.handler",
        "Code": {
            "S3Bucket": "artifacts",
            "S3Key": "fn.zip",
            "S3ObjectVersion": marker.version_id,
        },
    });
    let r = call(
        &e.server,
        "POST",
        "/2015-03-31/functions",
        &[],
        from_marker.to_string().as_bytes(),
    )
    .await;
    assert_eq!(r.status, 400);
    assert!(
        String::from_utf8_lossy(&r.body).contains("NoSuchVersion"),
        "{}",
        String::from_utf8_lossy(&r.body)
    );

    let settings = call(&e.server, "GET", "/2016-08-19/account-settings/", &[], b"").await;
    assert_eq!(settings.json()["AccountUsage"]["FunctionCount"], 1);
}

#[tokio::test]
async fn unsupported_runtime_is_deployable_but_not_invocable() {
    let e = env("java");
    let body = json!({
        "FunctionName": "java-fn",
        "Runtime": "java21",
        "Role": "r",
        "Handler": "example.Handler::handleRequest",
        "Code": { "ZipFile": b64(&build_stored(&[("x.class", b"")])) },
    });
    assert_eq!(
        call(
            &e.server,
            "POST",
            "/2015-03-31/functions",
            &[],
            body.to_string().as_bytes()
        )
        .await
        .status,
        201
    );
    let r = call(
        &e.server,
        "POST",
        "/2015-03-31/functions/java-fn/invocations",
        &[],
        b"{}",
    )
    .await;
    assert_eq!(r.status, 502);
    assert_eq!(
        r.header("X-Amzn-ErrorType"),
        Some("InvalidRuntimeException")
    );
}

#[tokio::test]
async fn relative_storage_path_still_invokes() {
    if !has("python3") {
        eprintln!("skipping: python3 not available");
        return;
    }
    // The orchestrator passes `.devcloud/data/lambda`, relative to its cwd.
    let dir = temp_dir("relative");
    let cwd = std::env::current_dir().unwrap();
    let rel = pathdiff(&dir.join("lambda"), &cwd);
    let server = Arc::new(Server::new(Config {
        storage_path: rel,
        ..config_for(&dir)
    }));
    assert_eq!(create_python(&server, "rel-fn").await.status, 201);
    let out = call(
        &server,
        "POST",
        "/2015-03-31/functions/rel-fn/invocations",
        &[],
        b"{}",
    )
    .await;
    assert_eq!(
        out.header("X-Amz-Function-Error"),
        None,
        "{}",
        String::from_utf8_lossy(&out.body)
    );
    let _ = std::fs::remove_dir_all(dir);
}

/// `target` relative to `base` (both absolute), via `..` segments.
fn pathdiff(target: &std::path::Path, base: &std::path::Path) -> String {
    let t: Vec<_> = target.components().collect();
    let b: Vec<_> = base.components().collect();
    let common = t.iter().zip(&b).take_while(|(x, y)| x == y).count();
    let mut out = std::path::PathBuf::new();
    for _ in common..b.len() {
        out.push("..");
    }
    for c in &t[common..] {
        out.push(c);
    }
    out.to_string_lossy().into_owned()
}

#[tokio::test]
async fn strict_mode_rejects_unsigned_requests() {
    let dir = temp_dir("strict");
    let server = Arc::new(Server::new(Config {
        auth_mode: "strict".into(),
        access_key_id: "dev".into(),
        secret_access_key: "dev".into(),
        ..config_for(&dir)
    }));
    let r = call(&server, "GET", "/2015-03-31/functions/", &[], b"").await;
    assert_eq!(r.status, 403);
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test]
async fn list_functions_paginates_with_marker() {
    let e = env("paging");
    for name in ["a-fn", "b-fn", "c-fn"] {
        assert_eq!(create_python(&e.server, name).await.status, 201);
    }
    let first = call(
        &e.server,
        "GET",
        "/2015-03-31/functions/?MaxItems=2",
        &[],
        b"",
    )
    .await
    .json();
    assert_eq!(first["Functions"].as_array().unwrap().len(), 2);
    let marker = first["NextMarker"].as_str().unwrap().to_string();
    let second = call(
        &e.server,
        "GET",
        &format!("/2015-03-31/functions/?MaxItems=2&Marker={marker}"),
        &[],
        b"",
    )
    .await
    .json();
    assert_eq!(second["Functions"][0]["FunctionName"], "c-fn");
    assert!(second.get("NextMarker").is_none());
}

// ── regression tests for the review findings ────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_creates_never_destroy_the_winner_package() {
    let e = env("race");
    let mut tasks = Vec::new();
    for i in 0..8 {
        let server = Arc::clone(&e.server);
        tasks.push(tokio::spawn(async move {
            let src = format!("def handler(event, context):\n    return {i}\n");
            let zip = build_stored(&[("app.py", src.as_bytes())]);
            let body = json!({
                "FunctionName": "race-fn",
                "Runtime": "python3.12",
                "Role": "r",
                "Handler": "app.handler",
                "Code": { "ZipFile": b64(&zip) },
            });
            call(
                &server,
                "POST",
                "/2015-03-31/functions",
                &[],
                body.to_string().as_bytes(),
            )
            .await
            .status
        }));
    }
    let mut statuses = Vec::new();
    for t in tasks {
        statuses.push(t.await.unwrap());
    }
    assert_eq!(
        statuses.iter().filter(|s| **s == 201).count(),
        1,
        "{statuses:?}"
    );
    assert!(
        statuses.iter().all(|s| *s == 201 || *s == 409),
        "{statuses:?}"
    );
    let pkg = call(
        &e.server,
        "GET",
        "/_devcloud/functions/race-fn/code.zip",
        &[],
        b"",
    )
    .await;
    assert_eq!(pkg.status, 200, "winner package must survive the losers");
    if has("python3") {
        let out = call(
            &e.server,
            "POST",
            "/2015-03-31/functions/race-fn/invocations",
            &[],
            b"{}",
        )
        .await;
        assert_eq!(
            out.header("X-Amz-Function-Error"),
            None,
            "{}",
            String::from_utf8_lossy(&out.body)
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn running_invocation_keeps_its_code_across_update() {
    if !has("python3") {
        eprintln!("skipping: python3 not available");
        return;
    }
    let e = env("inflight");
    let handler = b"import time\ndef handler(event, context):\n    time.sleep(event.get('wait', 0))\n    return open('data.txt').read()\n";
    let v1 = build_stored(&[("app.py", handler), ("data.txt", b"v1")]);
    let body = json!({
        "FunctionName": "inflight",
        "Runtime": "python3.12",
        "Role": "r",
        "Handler": "app.handler",
        "Timeout": 10,
        "Code": { "ZipFile": b64(&v1) },
    });
    assert_eq!(
        call(
            &e.server,
            "POST",
            "/2015-03-31/functions",
            &[],
            body.to_string().as_bytes()
        )
        .await
        .status,
        201
    );

    let server = Arc::clone(&e.server);
    let slow = tokio::spawn(async move {
        call(
            &server,
            "POST",
            "/2015-03-31/functions/inflight/invocations",
            &[],
            br#"{"wait": 1.0}"#,
        )
        .await
    });
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    let v2 = build_stored(&[("app.py", handler), ("data.txt", b"v2")]);
    let updated = call(
        &e.server,
        "PUT",
        "/2015-03-31/functions/inflight/code",
        &[],
        json!({ "ZipFile": b64(&v2) }).to_string().as_bytes(),
    )
    .await;
    assert_eq!(updated.status, 200);

    let old = slow.await.unwrap();
    assert_eq!(
        old.header("X-Amz-Function-Error"),
        None,
        "{}",
        String::from_utf8_lossy(&old.body)
    );
    assert_eq!(old.body, b"\"v1\"");
    let new = call(
        &e.server,
        "POST",
        "/2015-03-31/functions/inflight/invocations",
        &[],
        b"{}",
    )
    .await;
    assert_eq!(new.body, b"\"v2\"");
    // The retired tree is gone once its last invocation finished.
    let trees = std::fs::read_dir(e.dir.join("lambda/functions/inflight"))
        .unwrap()
        .filter(|d| d.as_ref().unwrap().path().is_dir())
        .count();
    assert_eq!(trees, 1);
}

#[tokio::test]
async fn timeout_kills_descendants_holding_the_log_pipes() {
    if !has("python3") {
        eprintln!("skipping: python3 not available");
        return;
    }
    let e = env("descendants");
    let zip = build_stored(&[(
        "app.py",
        b"import subprocess, time\ndef handler(event, context):\n    subprocess.Popen(['sleep', '30'])\n    time.sleep(30)\n",
    )]);
    let body = json!({
        "FunctionName": "spawner",
        "Runtime": "python3.12",
        "Role": "r",
        "Handler": "app.handler",
        "Timeout": 1,
        "Code": { "ZipFile": b64(&zip) },
    });
    assert_eq!(
        call(
            &e.server,
            "POST",
            "/2015-03-31/functions",
            &[],
            body.to_string().as_bytes()
        )
        .await
        .status,
        201
    );
    let started = std::time::Instant::now();
    let out = call(
        &e.server,
        "POST",
        "/2015-03-31/functions/spawner/invocations",
        &[],
        b"{}",
    )
    .await;
    let elapsed = started.elapsed();
    assert_eq!(out.header("X-Amz-Function-Error"), Some("Unhandled"));
    assert!(
        elapsed < std::time::Duration::from_millis(2500),
        "took {elapsed:?}"
    );
}

#[tokio::test]
async fn update_code_enforces_revision_id() {
    let e = env("coderev");
    let created = create_python(&e.server, "rev-fn").await.json();
    let v2 = build_stored(&[("app.py", b"def handler(e, c):\n    return 2\n")]);
    let stale = call(
        &e.server,
        "PUT",
        "/2015-03-31/functions/rev-fn/code",
        &[],
        json!({ "ZipFile": b64(&v2), "RevisionId": "stale-revision" })
            .to_string()
            .as_bytes(),
    )
    .await;
    assert_eq!(stale.status, 412);
    let cfg = call(
        &e.server,
        "GET",
        "/2015-03-31/functions/rev-fn/configuration",
        &[],
        b"",
    )
    .await
    .json();
    assert_eq!(cfg["CodeSha256"], created["CodeSha256"]);
    let fresh = call(
        &e.server,
        "PUT",
        "/2015-03-31/functions/rev-fn/code",
        &[],
        json!({ "ZipFile": b64(&v2), "RevisionId": created["RevisionId"] })
            .to_string()
            .as_bytes(),
    )
    .await;
    assert_eq!(fresh.status, 200);
}

#[tokio::test]
async fn failed_persist_keeps_package_and_configuration_consistent() {
    let e = env("persistfail");
    let created = create_python(&e.server, "pf-fn").await.json();
    let old_pkg = call(
        &e.server,
        "GET",
        "/_devcloud/functions/pf-fn/code.zip",
        &[],
        b"",
    )
    .await
    .body;
    // A directory where the temp state file goes makes the next persist fail.
    std::fs::create_dir_all(e.dir.join("lambda/state.json.tmp")).unwrap();
    let v2 = build_stored(&[("app.py", b"def handler(e, c):\n    return 2\n")]);
    let r = call(
        &e.server,
        "PUT",
        "/2015-03-31/functions/pf-fn/code",
        &[],
        json!({ "ZipFile": b64(&v2) }).to_string().as_bytes(),
    )
    .await;
    assert_eq!(r.status, 500);
    let cfg = call(
        &e.server,
        "GET",
        "/2015-03-31/functions/pf-fn/configuration",
        &[],
        b"",
    )
    .await
    .json();
    assert_eq!(cfg["CodeSha256"], created["CodeSha256"]);
    let pkg = call(
        &e.server,
        "GET",
        "/_devcloud/functions/pf-fn/code.zip",
        &[],
        b"",
    )
    .await
    .body;
    assert_eq!(
        pkg, old_pkg,
        "Code.Location must still serve the committed package"
    );
}

/// Minimal SigV4 signer (service `lambda`), deliberately without the
/// `x-amz-content-sha256` header as the non-S3 AWS SDKs send it.
fn sign_without_hash_header(
    method: &str,
    path: &str,
    body: &[u8],
    host: &str,
) -> Vec<(String, String)> {
    use hmac::{Hmac, Mac};
    use sha2::{Digest, Sha256};
    type H = Hmac<Sha256>;
    fn mac(key: &[u8], data: &str) -> Vec<u8> {
        let mut m = H::new_from_slice(key).unwrap();
        m.update(data.as_bytes());
        m.finalize().into_bytes().to_vec()
    }
    let amz_date = "20261005T000000Z";
    let date = &amz_date[..8];
    let canonical = format!(
        "{method}\n{path}\n\nhost:{host}\nx-amz-date:{amz_date}\n\nhost;x-amz-date\n{}",
        hex::encode(Sha256::digest(body))
    );
    let scope = format!("{date}/us-east-1/lambda/aws4_request");
    let to_sign = format!(
        "AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{}",
        hex::encode(Sha256::digest(canonical.as_bytes()))
    );
    let k = mac(b"AWS4dev", date);
    let k = mac(&k, "us-east-1");
    let k = mac(&k, "lambda");
    let k = mac(&k, "aws4_request");
    let sig = hex::encode(mac(&k, &to_sign));
    vec![
        ("host".into(), host.into()),
        ("x-amz-date".into(), amz_date.into()),
        (
            "authorization".into(),
            format!("AWS4-HMAC-SHA256 Credential=dev/{scope}, SignedHeaders=host;x-amz-date, Signature={sig}"),
        ),
    ]
}

#[tokio::test]
async fn strict_mode_accepts_sigv4_without_content_hash_header() {
    let dir = temp_dir("sigv4");
    let server = Arc::new(Server::new(Config {
        auth_mode: "strict".into(),
        access_key_id: "dev".into(),
        secret_access_key: "dev".into(),
        ..config_for(&dir)
    }));
    let headers = sign_without_hash_header("GET", "/2015-03-31/functions/", b"", "127.0.0.1:19010");
    let refs: Vec<(&str, &str)> = headers
        .iter()
        .map(|(k, v)| (k.as_str(), v.as_str()))
        .collect();
    let ok = call(&server, "GET", "/2015-03-31/functions/", &refs, b"").await;
    assert_eq!(ok.status, 200, "{}", String::from_utf8_lossy(&ok.body));
    // Tampered body no longer matches the signed hash.
    let bad = call(&server, "GET", "/2015-03-31/functions/", &refs, b"x").await;
    assert_eq!(bad.status, 403);
    let _ = std::fs::remove_dir_all(dir);
}

// ── regression tests for the second review round ────────────────────────────

#[tokio::test]
async fn huge_marker_does_not_panic_or_poison_state() {
    let e = env("marker");
    assert_eq!(create_python(&e.server, "m-fn").await.status, 201);
    let r = call(
        &e.server,
        "GET",
        &format!("/2015-03-31/functions/?Marker={}&MaxItems=1", u64::MAX),
        &[],
        b"",
    )
    .await;
    assert_eq!(r.status, 200);
    assert_eq!(r.json()["Functions"], json!([]));
    let list = call(&e.server, "GET", "/2015-03-31/functions/", &[], b"").await;
    assert_eq!(
        list.json()["Functions"].as_array().unwrap().len(),
        1,
        "state lock still usable"
    );
}

#[tokio::test]
async fn dry_run_does_not_need_a_local_runtime() {
    let e = env("dryrun");
    let body = json!({
        "FunctionName": "java-dry",
        "Runtime": "java21",
        "Role": "r",
        "Handler": "example.Handler::handleRequest",
        "Code": { "ZipFile": b64(&build_stored(&[("x.class", b"")])) },
    });
    assert_eq!(
        call(
            &e.server,
            "POST",
            "/2015-03-31/functions",
            &[],
            body.to_string().as_bytes()
        )
        .await
        .status,
        201
    );
    let r = call(
        &e.server,
        "POST",
        "/2015-03-31/functions/java-dry/invocations",
        &[("X-Amz-Invocation-Type", "DryRun")],
        b"{}",
    )
    .await;
    assert_eq!(r.status, 204);
}

#[tokio::test]
async fn node_log_tail_survives_large_output() {
    if !has("node") {
        eprintln!("skipping: node not available");
        return;
    }
    let e = env("nodelog");
    let zip = build_stored(&[(
        "index.js",
        b"exports.handler = async () => { console.log('x'.repeat(1000000)); console.log('END-MARKER'); return 'ok'; };\n",
    )]);
    let body = json!({
        "FunctionName": "noisy",
        "Runtime": "nodejs20.x",
        "Role": "r",
        "Handler": "index.handler",
        "Code": { "ZipFile": b64(&zip) },
    });
    assert_eq!(
        call(
            &e.server,
            "POST",
            "/2015-03-31/functions",
            &[],
            body.to_string().as_bytes()
        )
        .await
        .status,
        201
    );
    let r = call(
        &e.server,
        "POST",
        "/2015-03-31/functions/noisy/invocations",
        &[("X-Amz-Log-Type", "Tail")],
        b"{}",
    )
    .await;
    assert_eq!(r.body, b"\"ok\"");
    let log = base64::engine::general_purpose::STANDARD
        .decode(r.header("X-Amz-Log-Result").unwrap())
        .unwrap();
    assert!(
        String::from_utf8_lossy(&log).contains("END-MARKER"),
        "log tail lost the marker"
    );
}

fn pid_alive(pid: &str) -> bool {
    std::process::Command::new("kill")
        .args(["-0", pid])
        .stderr(std::process::Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancelled_invocation_still_kills_descendants() {
    if !has("python3") {
        eprintln!("skipping: python3 not available");
        return;
    }
    let e = env("cancel");
    let pid_file = e.dir.join("child.pid");
    let zip = build_stored(&[(
        "app.py",
        b"import os, subprocess, time\ndef handler(event, context):\n    p = subprocess.Popen(['sleep', '30'])\n    open(os.environ['PID_FILE'], 'w').write(str(p.pid))\n    time.sleep(30)\n",
    )]);
    let body = json!({
        "FunctionName": "cancel-me",
        "Runtime": "python3.12",
        "Role": "r",
        "Handler": "app.handler",
        "Timeout": 60,
        "Environment": { "Variables": { "PID_FILE": pid_file.to_string_lossy() } },
        "Code": { "ZipFile": b64(&zip) },
    });
    assert_eq!(
        call(
            &e.server,
            "POST",
            "/2015-03-31/functions",
            &[],
            body.to_string().as_bytes()
        )
        .await
        .status,
        201
    );
    let server = Arc::clone(&e.server);
    let running = tokio::spawn(async move {
        call(
            &server,
            "POST",
            "/2015-03-31/functions/cancel-me/invocations",
            &[],
            b"{}",
        )
        .await
        .status
    });
    let mut pid = String::new();
    for _ in 0..100 {
        if let Ok(p) = std::fs::read_to_string(&pid_file) {
            if !p.is_empty() {
                pid = p;
                break;
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    assert!(!pid.is_empty(), "handler never started its child");
    assert!(pid_alive(&pid));
    // What a devcloud shutdown does to an in-flight invocation.
    running.abort();
    let _ = running.await;
    let mut alive = true;
    for _ in 0..40 {
        if !pid_alive(&pid) {
            alive = false;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    if alive {
        let _ = std::process::Command::new("kill")
            .args(["-9", &pid])
            .status();
    }
    assert!(!alive, "descendant {pid} survived the cancelled invocation");
}

// ── regression tests for the third review round ─────────────────────────────

#[tokio::test]
async fn function_environment_overrides_unreserved_defaults() {
    if !has("python3") {
        eprintln!("skipping: python3 not available");
        return;
    }
    let e = env("envprec");
    let zip = build_stored(&[(
        "app.py",
        b"import os, time\ndef handler(event, context):\n    return {'TZ': os.environ.get('TZ'), 'LANG': os.environ.get('LANG'), 'tzname': time.strftime('%Z'), 'region': os.environ.get('AWS_REGION')}\n",
    )]);
    let body = json!({
        "FunctionName": "env-fn",
        "Runtime": "python3.12",
        "Role": "r",
        "Handler": "app.handler",
        "Environment": { "Variables": { "TZ": "Asia/Tokyo", "LANG": "ja_JP.UTF-8" } },
        "Code": { "ZipFile": b64(&zip) },
    });
    assert_eq!(
        call(
            &e.server,
            "POST",
            "/2015-03-31/functions",
            &[],
            body.to_string().as_bytes()
        )
        .await
        .status,
        201
    );
    let out = call(
        &e.server,
        "POST",
        "/2015-03-31/functions/env-fn/invocations",
        &[],
        b"{}",
    )
    .await
    .json();
    assert_eq!(out["TZ"], "Asia/Tokyo");
    assert_eq!(out["LANG"], "ja_JP.UTF-8");
    assert_eq!(out["tzname"], "JST");
    assert_eq!(
        out["region"], "us-east-1",
        "reserved variables still come from devcloud"
    );

    // Without an override the defaults apply.
    assert_eq!(create_python(&e.server, "default-env").await.status, 201);
    let zip = build_stored(&[(
        "app.py",
        b"import os\ndef handler(e, c):\n    return os.environ.get('TZ')\n",
    )]);
    call(
        &e.server,
        "PUT",
        "/2015-03-31/functions/default-env/code",
        &[],
        json!({ "ZipFile": b64(&zip) }).to_string().as_bytes(),
    )
    .await;
    let out = call(
        &e.server,
        "POST",
        "/2015-03-31/functions/default-env/invocations",
        &[],
        b"{}",
    )
    .await;
    assert_eq!(out.body, b"\"UTC\"");
}

#[tokio::test]
async fn ephemeral_storage_is_validated_and_updatable() {
    let e = env("ephemeral");
    let created = create_python(&e.server, "eph-fn").await.json();
    assert_eq!(created["EphemeralStorage"]["Size"], 512);
    let updated = call(
        &e.server,
        "PUT",
        "/2015-03-31/functions/eph-fn/configuration",
        &[],
        br#"{"EphemeralStorage":{"Size":1024}}"#,
    )
    .await;
    assert_eq!(updated.status, 200);
    assert_eq!(updated.json()["EphemeralStorage"]["Size"], 1024);
    let cfg = call(
        &e.server,
        "GET",
        "/2015-03-31/functions/eph-fn/configuration",
        &[],
        b"",
    )
    .await
    .json();
    assert_eq!(cfg["EphemeralStorage"]["Size"], 1024);

    let invalid = call(
        &e.server,
        "PUT",
        "/2015-03-31/functions/eph-fn/configuration",
        &[],
        br#"{"EphemeralStorage":{"Size":100}}"#,
    )
    .await;
    assert_eq!(invalid.status, 400);
    assert_eq!(
        invalid.header("X-Amzn-ErrorType"),
        Some("ValidationException")
    );
    let cfg = call(
        &e.server,
        "GET",
        "/2015-03-31/functions/eph-fn/configuration",
        &[],
        b"",
    )
    .await
    .json();
    assert_eq!(
        cfg["RevisionId"],
        updated.json()["RevisionId"],
        "rejected update must not commit"
    );
}

// ── regression tests for the fourth review round ────────────────────────────

#[tokio::test]
async fn python_output_before_a_timeout_reaches_the_log() {
    if !has("python3") {
        eprintln!("skipping: python3 not available");
        return;
    }
    let e = env("unbuffered");
    let zip = build_stored(&[(
        "app.py",
        b"import time\ndef handler(event, context):\n    print('started')\n    time.sleep(5)\n",
    )]);
    let body = json!({
        "FunctionName": "sleepy",
        "Runtime": "python3.12",
        "Role": "r",
        "Handler": "app.handler",
        "Timeout": 1,
        "Code": { "ZipFile": b64(&zip) },
    });
    assert_eq!(
        call(
            &e.server,
            "POST",
            "/2015-03-31/functions",
            &[],
            body.to_string().as_bytes()
        )
        .await
        .status,
        201
    );
    let r = call(
        &e.server,
        "POST",
        "/2015-03-31/functions/sleepy/invocations",
        &[("X-Amz-Log-Type", "Tail")],
        b"{}",
    )
    .await;
    assert_eq!(r.header("X-Amz-Function-Error"), Some("Unhandled"));
    let log = base64::engine::general_purpose::STANDARD
        .decode(r.header("X-Amz-Log-Result").unwrap())
        .unwrap();
    assert!(
        String::from_utf8_lossy(&log).contains("started"),
        "pre-timeout output lost"
    );
}

#[tokio::test]
async fn strict_mode_accepts_correctly_ordered_utf8_query_signature() {
    use hmac::{Hmac, Mac};
    use sha2::{Digest, Sha256};
    type H = Hmac<Sha256>;
    fn mac(key: &[u8], data: &str) -> Vec<u8> {
        let mut m = H::new_from_slice(key).unwrap();
        m.update(data.as_bytes());
        m.finalize().into_bytes().to_vec()
    }
    let dir = temp_dir("sigv4query");
    let relaxed = Arc::new(Server::new(config_for(&dir)));
    assert_eq!(create_python(&relaxed, "q-fn").await.status, 201);
    drop(relaxed);
    let server = Arc::new(Server::new(Config {
        auth_mode: "strict".into(),
        access_key_id: "dev".into(),
        secret_access_key: "dev".into(),
        ..config_for(&dir)
    }));

    let arn = "arn%3Aaws%3Alambda%3Aus-east-1%3A000000000000%3Afunction%3Aq-fn";
    let path = format!("/2017-03-31/tags/{arn}");
    // What the AWS SDKs send: canonical query sorted on encoded bytes.
    let canonical_query = "tagKeys=%C3%A9&tagKeys=z";
    let host = "127.0.0.1:19010";
    let amz_date = "20261005T000000Z";
    let canonical_uri = path.replace('%', "%25");
    let canonical = format!(
        "DELETE\n{canonical_uri}\n{canonical_query}\nhost:{host}\nx-amz-date:{amz_date}\n\nhost;x-amz-date\n{}",
        hex::encode(Sha256::digest(b""))
    );
    let scope = "20261005/us-east-1/lambda/aws4_request";
    let to_sign = format!(
        "AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{}",
        hex::encode(Sha256::digest(canonical.as_bytes()))
    );
    let k = mac(b"AWS4dev", "20261005");
    let k = mac(&k, "us-east-1");
    let k = mac(&k, "lambda");
    let k = mac(&k, "aws4_request");
    let sig = hex::encode(mac(&k, &to_sign));
    let auth = format!(
        "AWS4-HMAC-SHA256 Credential=dev/{scope}, SignedHeaders=host;x-amz-date, Signature={sig}"
    );
    let r = call(
        &server,
        "DELETE",
        &format!("{path}?tagKeys=z&tagKeys=%C3%A9"),
        &[
            ("host", host),
            ("x-amz-date", amz_date),
            ("authorization", &auth),
        ],
        b"",
    )
    .await;
    assert_eq!(r.status, 204, "{}", String::from_utf8_lossy(&r.body));
    let _ = std::fs::remove_dir_all(dir);
}

// ── regression tests for the fifth review round ─────────────────────────────

#[tokio::test]
async fn lingering_python_thread_does_not_turn_success_into_timeout() {
    if !has("python3") {
        eprintln!("skipping: python3 not available");
        return;
    }
    let e = env("thread");
    let zip = build_stored(&[(
        "app.py",
        b"import threading, time\ndef handler(event, context):\n    threading.Thread(target=time.sleep, args=(5,)).start()\n    return 'done'\n",
    )]);
    let body = json!({
        "FunctionName": "threaded",
        "Runtime": "python3.12",
        "Role": "r",
        "Handler": "app.handler",
        "Timeout": 1,
        "Code": { "ZipFile": b64(&zip) },
    });
    assert_eq!(
        call(
            &e.server,
            "POST",
            "/2015-03-31/functions",
            &[],
            body.to_string().as_bytes()
        )
        .await
        .status,
        201
    );
    let started = std::time::Instant::now();
    let r = call(
        &e.server,
        "POST",
        "/2015-03-31/functions/threaded/invocations",
        &[],
        b"{}",
    )
    .await;
    assert_eq!(
        r.header("X-Amz-Function-Error"),
        None,
        "{}",
        String::from_utf8_lossy(&r.body)
    );
    assert_eq!(r.body, b"\"done\"");
    assert!(started.elapsed() < std::time::Duration::from_secs(1));
}

#[tokio::test]
async fn whitespace_payload_is_invalid_json() {
    let e = env("whitespace");
    assert_eq!(create_python(&e.server, "ws-fn").await.status, 201);
    let r = call(
        &e.server,
        "POST",
        "/2015-03-31/functions/ws-fn/invocations",
        &[],
        b"   ",
    )
    .await;
    assert_eq!(r.status, 400);
    assert_eq!(
        r.header("X-Amzn-ErrorType"),
        Some("InvalidRequestContentException")
    );
}

// ── regression tests for the sixth review round ─────────────────────────────

#[tokio::test]
async fn cross_site_browser_requests_cannot_create_or_invoke() {
    let e = env("csrf");
    let zip = build_stored(&[("app.py", PY_HANDLER.as_bytes())]);
    let body = json!({
        "FunctionName": "csrf-fn",
        "Runtime": "python3.12",
        "Role": "r",
        "Handler": "app.handler",
        "Code": { "ZipFile": b64(&zip) },
    })
    .to_string();
    let evil = [
        ("Origin", "https://evil.example"),
        ("Content-Type", "text/plain"),
    ];
    let r = call(
        &e.server,
        "POST",
        "/2015-03-31/functions",
        &evil,
        body.as_bytes(),
    )
    .await;
    assert_eq!(r.status, 403);
    assert_eq!(r.header("X-Amzn-ErrorType"), Some("AccessDeniedException"));
    let r = call(
        &e.server,
        "POST",
        "/2015-03-31/functions",
        &[("Sec-Fetch-Site", "cross-site")],
        body.as_bytes(),
    )
    .await;
    assert_eq!(r.status, 403);
    assert_eq!(
        call(&e.server, "GET", "/2015-03-31/functions/csrf-fn", &[], b"")
            .await
            .status,
        404
    );

    // Local front-ends (loopback origins) and SDKs (no Origin) still work.
    let local = [("Origin", "http://localhost:3000")];
    assert_eq!(
        call(
            &e.server,
            "POST",
            "/2015-03-31/functions",
            &local,
            body.as_bytes()
        )
        .await
        .status,
        201
    );
    let r = call(
        &e.server,
        "POST",
        "/2015-03-31/functions/csrf-fn/invocations",
        &evil,
        b"{}",
    )
    .await;
    assert_eq!(r.status, 403);
    let r = call(
        &e.server,
        "DELETE",
        "/2015-03-31/functions/csrf-fn",
        &evil,
        b"",
    )
    .await;
    assert_eq!(r.status, 403);
}

// ── regression tests for the seventh review round ───────────────────────────

#[tokio::test]
async fn arns_for_another_account_or_region_do_not_match_local_functions() {
    let e = env("arnscope");
    assert_eq!(create_python(&e.server, "f").await.status, 201);
    for foreign in [
        "arn%3Aaws%3Alambda%3Aeu-west-1%3A999999999999%3Afunction%3Af",
        "arn%3Aaws%3Alambda%3Aus-east-1%3A999999999999%3Afunction%3Af",
        "arn%3Aaws%3Alambda%3Aeu-west-1%3A000000000000%3Afunction%3Af",
        "999999999999%3Afunction%3Af",
    ] {
        let del = call(
            &e.server,
            "DELETE",
            &format!("/2015-03-31/functions/{foreign}"),
            &[],
            b"",
        )
        .await;
        assert_eq!(del.status, 404, "{foreign}");
        assert_eq!(
            del.header("X-Amzn-ErrorType"),
            Some("ResourceNotFoundException")
        );
    }
    let own = "arn%3Aaws%3Alambda%3Aus-east-1%3A000000000000%3Afunction%3Af";
    assert_eq!(
        call(
            &e.server,
            "GET",
            &format!("/2015-03-31/functions/{own}"),
            &[],
            b""
        )
        .await
        .status,
        200
    );
    assert_eq!(
        call(
            &e.server,
            "GET",
            "/2015-03-31/functions/000000000000%3Afunction%3Af",
            &[],
            b""
        )
        .await
        .status,
        200
    );
    let tags = call(
        &e.server,
        "GET",
        "/2017-03-31/tags/arn%3Aaws%3Alambda%3Aeu-west-1%3A999999999999%3Afunction%3Af",
        &[],
        b"",
    )
    .await;
    assert_eq!(tags.status, 404);
}

#[tokio::test]
async fn code_location_is_pinned_to_the_version_it_was_issued_for() {
    let e = env("pinned");
    let v1 = build_stored(&[("app.py", b"def handler(e, c):\n    return 1\n")]);
    let body = json!({
        "FunctionName": "pin-fn",
        "Runtime": "python3.12",
        "Role": "r",
        "Handler": "app.handler",
        "Code": { "ZipFile": b64(&v1) },
    });
    assert_eq!(
        call(
            &e.server,
            "POST",
            "/2015-03-31/functions",
            &[],
            body.to_string().as_bytes()
        )
        .await
        .status,
        201
    );
    let location = |resp: &Resp| -> String {
        let url = resp.json()["Code"]["Location"]
            .as_str()
            .unwrap()
            .to_string();
        url.split_once("19010").unwrap().1.to_string()
    };
    let old_link =
        location(&call(&e.server, "GET", "/2015-03-31/functions/pin-fn", &[], b"").await);
    let r = call(&e.server, "GET", &old_link, &[], b"").await;
    assert_eq!(r.status, 200);
    assert_eq!(r.body, v1);

    let v2 = build_stored(&[("app.py", b"def handler(e, c):\n    return 2\n")]);
    call(
        &e.server,
        "PUT",
        "/2015-03-31/functions/pin-fn/code",
        &[],
        json!({ "ZipFile": b64(&v2) }).to_string().as_bytes(),
    )
    .await;
    let stale = call(&e.server, "GET", &old_link, &[], b"").await;
    assert_eq!(stale.status, 410, "an old link must not serve the new code");

    let new_link =
        location(&call(&e.server, "GET", "/2015-03-31/functions/pin-fn", &[], b"").await);
    let r = call(&e.server, "GET", &new_link, &[], b"").await;
    assert_eq!(r.status, 200);
    assert_eq!(r.body, v2);
}

#[tokio::test]
async fn function_path_does_not_hide_the_interpreter() {
    if !has("python3") {
        eprintln!("skipping: python3 not available");
        return;
    }
    let e = env("pathenv");
    let zip = build_stored(&[(
        "app.py",
        b"import os\ndef handler(e, c):\n    return os.environ['PATH']\n",
    )]);
    let body = json!({
        "FunctionName": "path-fn",
        "Runtime": "python3.12",
        "Role": "r",
        "Handler": "app.handler",
        "Environment": { "Variables": { "PATH": "/opt/bin" } },
        "Code": { "ZipFile": b64(&zip) },
    });
    assert_eq!(
        call(
            &e.server,
            "POST",
            "/2015-03-31/functions",
            &[],
            body.to_string().as_bytes()
        )
        .await
        .status,
        201
    );
    let r = call(
        &e.server,
        "POST",
        "/2015-03-31/functions/path-fn/invocations",
        &[],
        b"{}",
    )
    .await;
    assert_eq!(r.status, 200, "{}", String::from_utf8_lossy(&r.body));
    assert_eq!(
        r.body, b"\"/opt/bin\"",
        "the handler still sees the function's PATH"
    );
}

// ── regression tests for the eighth review round ────────────────────────────

#[tokio::test]
async fn non_function_arns_never_resolve_to_a_local_function() {
    let e = env("arnkind");
    assert_eq!(create_python(&e.server, "demo").await.status, 201);
    for other in [
        "arn%3Aaws%3Acloudwatch%3Aus-east-1%3A000000000000%3Aalarm%3Ademo",
        "arn%3Aaws%3Alambda%3Aus-east-1%3A000000000000%3Alayer%3Ademo",
        "arn%3Aaws%3As3%3A%3A%3Ademo",
    ] {
        let del = call(
            &e.server,
            "DELETE",
            &format!("/2015-03-31/functions/{other}"),
            &[],
            b"",
        )
        .await;
        assert_eq!(del.status, 400, "{other}");
        assert_eq!(del.header("X-Amzn-ErrorType"), Some("ValidationException"));
    }
    let tag = call(
        &e.server,
        "GET",
        "/2017-03-31/tags/arn%3Aaws%3Alambda%3Aus-east-1%3A000000000000%3Alayer%3Ademo",
        &[],
        b"",
    )
    .await;
    assert_eq!(tag.status, 400);
    assert_eq!(
        call(&e.server, "GET", "/2015-03-31/functions/demo", &[], b"")
            .await
            .status,
        200,
        "demo survived"
    );
}

// ── regression tests for the tenth review round ─────────────────────────────

#[tokio::test]
async fn s3_object_version_selects_the_deployed_package() {
    let e = env("s3version");
    let store = devcloud_s3::store::FileBucketStore::new(e.dir.join("s3/buckets"));
    store.create_bucket("artifacts").unwrap();
    store.put_bucket_versioning("artifacts", "Enabled").unwrap();
    let put = |n: u8| {
        let src = format!("def handler(e, c):\n    return {n}\n");
        store
            .put_object(devcloud_s3::objops::PutObjectInput {
                bucket: "artifacts".into(),
                key: "fn.zip".into(),
                body: build_stored(&[("app.py", src.as_bytes())]),
                ..Default::default()
            })
            .unwrap()
            .version_id
    };
    let (v1, v2) = (put(1), put(2));
    assert!(
        !v1.is_empty() && v1 != v2,
        "versioning must be on: {v1:?} {v2:?}"
    );

    let create = |name: &str, version: &str| {
        json!({
            "FunctionName": name,
            "Runtime": "python3.12",
            "Role": "r",
            "Handler": "app.handler",
            "Code": { "S3Bucket": "artifacts", "S3Key": "fn.zip", "S3ObjectVersion": version },
        })
        .to_string()
    };
    let r = call(
        &e.server,
        "POST",
        "/2015-03-31/functions",
        &[],
        create("v1-fn", &v1).as_bytes(),
    )
    .await;
    assert_eq!(r.status, 201, "{}", String::from_utf8_lossy(&r.body));
    let r = call(
        &e.server,
        "POST",
        "/2015-03-31/functions",
        &[],
        create("bad-version", "no-such-version").as_bytes(),
    )
    .await;
    assert_eq!(r.status, 400);

    let pkg_of = |resp: &Resp| resp.body.clone();
    let deployed = pkg_of(
        &call(
            &e.server,
            "GET",
            "/_devcloud/functions/v1-fn/code.zip",
            &[],
            b"",
        )
        .await,
    );
    let expected = store
        .get_object_version("artifacts", "fn.zip", &v1)
        .unwrap()
        .unwrap()
        .1;
    assert_eq!(
        deployed, expected,
        "the requested version, not the latest, is deployed"
    );

    let upd =
        json!({ "S3Bucket": "artifacts", "S3Key": "fn.zip", "S3ObjectVersion": v2 }).to_string();
    assert_eq!(
        call(
            &e.server,
            "PUT",
            "/2015-03-31/functions/v1-fn/code",
            &[],
            upd.as_bytes()
        )
        .await
        .status,
        200
    );
    let deployed = pkg_of(
        &call(
            &e.server,
            "GET",
            "/_devcloud/functions/v1-fn/code.zip",
            &[],
            b"",
        )
        .await,
    );
    assert_eq!(
        deployed,
        store
            .get_object_version("artifacts", "fn.zip", &v2)
            .unwrap()
            .unwrap()
            .1
    );
}

#[tokio::test]
async fn node_handlers_use_node_module_resolution() {
    if !has("node") {
        eprintln!("skipping: node not available");
        return;
    }
    let e = env("noderesolve");
    let zip = build_stored(&[
        (
            "pkg/index.js",
            b"exports.handler = async () => 'dir-index';\n",
        ),
        ("lib/package.json", br#"{"main": "entry.js"}"#),
        (
            "lib/entry.js",
            b"exports.handler = async () => 'package-main';\n",
        ),
    ]);
    for (name, handler, expected) in [
        ("dir-fn", "pkg.handler", &b"\"dir-index\""[..]),
        ("main-fn", "lib.handler", &b"\"package-main\""[..]),
    ] {
        let body = json!({
            "FunctionName": name,
            "Runtime": "nodejs20.x",
            "Role": "r",
            "Handler": handler,
            "Code": { "ZipFile": b64(&zip) },
        });
        assert_eq!(
            call(
                &e.server,
                "POST",
                "/2015-03-31/functions",
                &[],
                body.to_string().as_bytes()
            )
            .await
            .status,
            201
        );
        let r = call(
            &e.server,
            "POST",
            &format!("/2015-03-31/functions/{name}/invocations"),
            &[],
            b"{}",
        )
        .await;
        assert_eq!(
            r.header("X-Amz-Function-Error"),
            None,
            "{name}: {}",
            String::from_utf8_lossy(&r.body)
        );
        assert_eq!(r.body, expected);
    }
}

// ── regression tests for the eleventh review round ──────────────────────────

#[tokio::test]
async fn node_callback_handlers_are_awaited_regardless_of_arity() {
    if !has("node") {
        eprintln!("skipping: node not available");
        return;
    }
    let e = env("nodecb");
    let zip = build_stored(&[(
        "index.js",
        b"exports.defaulted = (event, context, callback = () => {}) => { setTimeout(() => callback(null, 'defaulted'), 50); };\n\
exports.rest = (...args) => { setTimeout(() => args[2](null, 'rest'), 50); };\n\
exports.errored = (event, context, callback = () => {}) => { setTimeout(() => callback(new RangeError('late failure')), 50); };\n\
exports.silent = (event) => { setTimeout(() => {}, 20); };\n\
exports.sync = () => 'ignored';\n",
    )]);
    for (name, handler) in [
        ("defaulted", "index.defaulted"),
        ("rest", "index.rest"),
        ("errored", "index.errored"),
        ("silent", "index.silent"),
        ("sync", "index.sync"),
    ] {
        let body = json!({
            "FunctionName": name,
            "Runtime": "nodejs20.x",
            "Role": "r",
            "Handler": handler,
            "Timeout": 5,
            "Code": { "ZipFile": b64(&zip) },
        });
        assert_eq!(
            call(
                &e.server,
                "POST",
                "/2015-03-31/functions",
                &[],
                body.to_string().as_bytes()
            )
            .await
            .status,
            201
        );
    }
    let invoke = |name: &'static str| {
        let server = Arc::clone(&e.server);
        async move {
            call(
                &server,
                "POST",
                &format!("/2015-03-31/functions/{name}/invocations"),
                &[],
                b"{}",
            )
            .await
        }
    };
    assert_eq!(invoke("defaulted").await.body, b"\"defaulted\"");
    assert_eq!(invoke("rest").await.body, b"\"rest\"");
    let err = invoke("errored").await;
    assert_eq!(err.header("X-Amz-Function-Error"), Some("Unhandled"));
    assert_eq!(err.json()["errorType"], "RangeError");
    // No callback and no promise: once the event loop drains, the result is null
    // (as on AWS) rather than a hang until the timeout.
    let silent = invoke("silent").await;
    assert_eq!(silent.header("X-Amz-Function-Error"), None);
    assert_eq!(silent.body, b"null");
    assert_eq!(invoke("sync").await.body, b"null");
}

// ── regression tests for the twelfth review round ───────────────────────────

#[tokio::test]
async fn node_callback_waits_for_the_event_loop_unless_disabled() {
    if !has("node") {
        eprintln!("skipping: node not available");
        return;
    }
    let e = env("eventloop");
    let marker = e.dir.join("marker");
    let zip = build_stored(&[(
        "index.js",
        b"const fs = require('fs');\n\
exports.waits = (event, context, callback) => { setTimeout(() => fs.writeFileSync(process.env.MARKER + '-waits', 'x'), 300); callback(null, 'ok'); };\n\
exports.nowait = (event, context, callback) => { context.callbackWaitsForEmptyEventLoop = false; setTimeout(() => fs.writeFileSync(process.env.MARKER + '-nowait', 'x'), 2000); callback(null, 'fast'); };\n\
exports.promise = async () => { setTimeout(() => {}, 2000); return 'promise'; };\n",
    )]);
    for (name, handler) in [
        ("waits", "index.waits"),
        ("nowait", "index.nowait"),
        ("promise", "index.promise"),
    ] {
        let body = json!({
            "FunctionName": name,
            "Runtime": "nodejs20.x",
            "Role": "r",
            "Handler": handler,
            "Timeout": 10,
            "Environment": { "Variables": { "MARKER": marker.to_string_lossy() } },
            "Code": { "ZipFile": b64(&zip) },
        });
        assert_eq!(
            call(
                &e.server,
                "POST",
                "/2015-03-31/functions",
                &[],
                body.to_string().as_bytes()
            )
            .await
            .status,
            201
        );
    }
    let r = call(
        &e.server,
        "POST",
        "/2015-03-31/functions/waits/invocations",
        &[],
        b"{}",
    )
    .await;
    assert_eq!(r.body, b"\"ok\"");
    assert!(
        e.dir.join("marker-waits").exists(),
        "pending timer must finish before the invocation ends"
    );

    let started = std::time::Instant::now();
    let r = call(
        &e.server,
        "POST",
        "/2015-03-31/functions/nowait/invocations",
        &[],
        b"{}",
    )
    .await;
    assert_eq!(r.body, b"\"fast\"");
    assert!(
        started.elapsed() < std::time::Duration::from_millis(1500),
        "false means respond immediately"
    );

    let started = std::time::Instant::now();
    let r = call(
        &e.server,
        "POST",
        "/2015-03-31/functions/promise/invocations",
        &[],
        b"{}",
    )
    .await;
    assert_eq!(r.body, b"\"promise\"");
    assert!(
        started.elapsed() < std::time::Duration::from_millis(1500),
        "promise results do not wait for the loop"
    );
}

// ── regression tests for the thirteenth review round ────────────────────────

#[tokio::test]
async fn node_failures_with_undefined_reasons_and_lingering_work() {
    if !has("node") {
        eprintln!("skipping: node not available");
        return;
    }
    let e = env("nodefail");
    let zip = build_stored(&[(
        "index.js",
        b"exports.rejectUndefined = () => Promise.reject();\n\
exports.throwUndefined = async () => { throw undefined; };\n\
exports.cbErrorLingering = (event, context, callback) => { setInterval(() => {}, 1000); callback(new RangeError('boom')); };\n",
    )]);
    for (name, handler) in [
        ("reject-undef", "index.rejectUndefined"),
        ("throw-undef", "index.throwUndefined"),
        ("cb-error", "index.cbErrorLingering"),
    ] {
        let body = json!({
            "FunctionName": name,
            "Runtime": "nodejs20.x",
            "Role": "r",
            "Handler": handler,
            "Timeout": 3,
            "Code": { "ZipFile": b64(&zip) },
        });
        assert_eq!(
            call(
                &e.server,
                "POST",
                "/2015-03-31/functions",
                &[],
                body.to_string().as_bytes()
            )
            .await
            .status,
            201
        );
    }
    for name in ["reject-undef", "throw-undef"] {
        let r = call(
            &e.server,
            "POST",
            &format!("/2015-03-31/functions/{name}/invocations"),
            &[],
            b"{}",
        )
        .await;
        assert_eq!(
            r.header("X-Amz-Function-Error"),
            Some("Unhandled"),
            "{name}: {}",
            String::from_utf8_lossy(&r.body)
        );
    }
    let started = std::time::Instant::now();
    let r = call(
        &e.server,
        "POST",
        "/2015-03-31/functions/cb-error/invocations",
        &[],
        b"{}",
    )
    .await;
    assert_eq!(r.header("X-Amz-Function-Error"), Some("Unhandled"));
    assert_eq!(
        r.json()["errorType"],
        "RangeError",
        "the callback error, not a timeout"
    );
    assert!(started.elapsed() < std::time::Duration::from_secs(2));
}

#[tokio::test]
async fn node_esm_package_with_top_level_await_loads() {
    if !has("node") {
        eprintln!("skipping: node not available");
        return;
    }
    let e = env("esmtla");
    let zip = build_stored(&[
        ("package.json", br#"{"type": "module"}"#),
        (
            "index.js",
            b"const config = await Promise.resolve({ greeting: 'tla' });\nexport const handler = async () => config.greeting;\n",
        ),
    ]);
    let body = json!({
        "FunctionName": "esm-tla",
        "Runtime": "nodejs22.x",
        "Role": "r",
        "Handler": "index.handler",
        "Code": { "ZipFile": b64(&zip) },
    });
    assert_eq!(
        call(
            &e.server,
            "POST",
            "/2015-03-31/functions",
            &[],
            body.to_string().as_bytes()
        )
        .await
        .status,
        201
    );
    let r = call(
        &e.server,
        "POST",
        "/2015-03-31/functions/esm-tla/invocations",
        &[],
        b"{}",
    )
    .await;
    assert_eq!(
        r.header("X-Amz-Function-Error"),
        None,
        "{}",
        String::from_utf8_lossy(&r.body)
    );
    assert_eq!(r.body, b"\"tla\"");
}

// ── regression tests for the fourteenth review round ────────────────────────

#[tokio::test]
async fn malformed_query_escapes_get_an_http_error_in_strict_mode() {
    let dir = temp_dir("badescape");
    let server = Arc::new(Server::new(Config {
        auth_mode: "strict".into(),
        access_key_id: "dev".into(),
        secret_access_key: "dev".into(),
        ..config_for(&dir)
    }));
    // Matching credential scope, so verification reaches the canonical query.
    let auth = "AWS4-HMAC-SHA256 Credential=dev/20261005/us-east-1/lambda/aws4_request, SignedHeaders=host;x-amz-date, Signature=0000000000000000000000000000000000000000000000000000000000000000";
    let r = call(
        &server,
        "GET",
        "/2015-03-31/functions/?Marker=%あ",
        &[
            ("host", "127.0.0.1:19010"),
            ("x-amz-date", "20261005T000000Z"),
            ("authorization", auth),
        ],
        b"",
    )
    .await;
    assert_eq!(r.status, 403, "a bad signature, not a panic");
    let _ = std::fs::remove_dir_all(dir);
}

// ── regression tests for the sixteenth review round ─────────────────────────

#[tokio::test]
async fn corrupted_packages_are_rejected_on_create_and_update() {
    let e = env("corrupt");
    let mut zip = build_stored(&[("app.py", PY_HANDLER.as_bytes())]);
    zip[30 + "app.py".len()] ^= 0xFF; // damage the file data, keep the headers
    let body = json!({
        "FunctionName": "corrupt-fn",
        "Runtime": "python3.12",
        "Role": "r",
        "Handler": "app.handler",
        "Code": { "ZipFile": b64(&zip) },
    });
    let r = call(
        &e.server,
        "POST",
        "/2015-03-31/functions",
        &[],
        body.to_string().as_bytes(),
    )
    .await;
    assert_eq!(r.status, 400);
    assert_eq!(
        r.header("X-Amzn-ErrorType"),
        Some("InvalidParameterValueException")
    );

    assert_eq!(create_python(&e.server, "ok-fn").await.status, 201);
    let r = call(
        &e.server,
        "PUT",
        "/2015-03-31/functions/ok-fn/code",
        &[],
        json!({ "ZipFile": b64(&zip) }).to_string().as_bytes(),
    )
    .await;
    assert_eq!(r.status, 400);
}

// ── regression tests for the eighteenth review round ────────────────────────

#[tokio::test]
async fn node_nested_handler_paths_resolve_like_aws() {
    if !has("node") {
        eprintln!("skipping: node not available");
        return;
    }
    let e = env("nestedhandler");
    let zip = build_stored(&[
        (
            "index.js",
            b"exports.api = { v1: { handler: async () => 'nested' } };\n",
        ),
        ("src/app.js", b"exports.handler = async () => 'subdir';\n"),
    ]);
    for (name, handler, expected) in [
        ("nested", "index.api.v1.handler", &b"\"nested\""[..]),
        ("subdir", "src/app.handler", &b"\"subdir\""[..]),
    ] {
        let body = json!({
            "FunctionName": name,
            "Runtime": "nodejs20.x",
            "Role": "r",
            "Handler": handler,
            "Code": { "ZipFile": b64(&zip) },
        });
        assert_eq!(
            call(
                &e.server,
                "POST",
                "/2015-03-31/functions",
                &[],
                body.to_string().as_bytes()
            )
            .await
            .status,
            201
        );
        let r = call(
            &e.server,
            "POST",
            &format!("/2015-03-31/functions/{name}/invocations"),
            &[],
            b"{}",
        )
        .await;
        assert_eq!(
            r.header("X-Amz-Function-Error"),
            None,
            "{name}: {}",
            String::from_utf8_lossy(&r.body)
        );
        assert_eq!(r.body, expected);
    }
}

// ── regression tests for the nineteenth review round ────────────────────────

#[tokio::test]
async fn update_function_code_persists_architectures() {
    let e = env("arch");
    assert_eq!(
        create_python(&e.server, "arch-fn").await.json()["Architectures"],
        json!(["x86_64"])
    );
    let zip = build_stored(&[("app.py", PY_HANDLER.as_bytes())]);
    let r = call(
        &e.server,
        "PUT",
        "/2015-03-31/functions/arch-fn/code",
        &[],
        json!({ "ZipFile": b64(&zip), "Architectures": ["arm64"] })
            .to_string()
            .as_bytes(),
    )
    .await;
    assert_eq!(r.status, 200);
    assert_eq!(r.json()["Architectures"], json!(["arm64"]));
    let cfg = call(
        &e.server,
        "GET",
        "/2015-03-31/functions/arch-fn/configuration",
        &[],
        b"",
    )
    .await
    .json();
    assert_eq!(cfg["Architectures"], json!(["arm64"]));

    for bad in [
        json!(["sparc"]),
        json!(["x86_64", "arm64"]),
        json!([]),
        json!("arm64"),
    ] {
        let r = call(
            &e.server,
            "PUT",
            "/2015-03-31/functions/arch-fn/code",
            &[],
            json!({ "ZipFile": b64(&zip), "Architectures": bad })
                .to_string()
                .as_bytes(),
        )
        .await;
        assert_eq!(r.status, 400, "{bad}");
    }
}

#[tokio::test]
async fn create_function_accepts_own_scope_arns_as_names() {
    let e = env("createarn");
    let zip = b64(&build_stored(&[("app.py", PY_HANDLER.as_bytes())]));
    let create = |name: &str| {
        json!({
            "FunctionName": name,
            "Runtime": "python3.12",
            "Role": "r",
            "Handler": "app.handler",
            "Code": { "ZipFile": zip },
        })
        .to_string()
    };
    for (raw, stored) in [
        (
            "arn:aws:lambda:us-east-1:000000000000:function:full-arn",
            "full-arn",
        ),
        ("000000000000:function:partial-arn", "partial-arn"),
    ] {
        let r = call(
            &e.server,
            "POST",
            "/2015-03-31/functions",
            &[],
            create(raw).as_bytes(),
        )
        .await;
        assert_eq!(r.status, 201, "{raw}: {}", String::from_utf8_lossy(&r.body));
        assert_eq!(r.json()["FunctionName"], stored);
        assert_eq!(
            call(
                &e.server,
                "GET",
                &format!("/2015-03-31/functions/{stored}"),
                &[],
                b""
            )
            .await
            .status,
            200
        );
    }
    for raw in [
        "arn:aws:lambda:eu-west-1:000000000000:function:other-region",
        "arn:aws:lambda:us-east-1:999999999999:function:other-account",
        "999999999999:function:other-account",
        "arn:aws:lambda:us-east-1:000000000000:function:qualified:prod",
        "arn:aws:s3:::bucket",
    ] {
        let r = call(
            &e.server,
            "POST",
            "/2015-03-31/functions",
            &[],
            create(raw).as_bytes(),
        )
        .await;
        assert_eq!(r.status, 400, "{raw}");
    }
}

// ── regression tests for the twenty-first review round ──────────────────────

#[tokio::test]
async fn node_callback_from_before_exit_returns_its_result() {
    if !has("node") {
        eprintln!("skipping: node not available");
        return;
    }
    let e = env("beforeexit");
    let zip = build_stored(&[(
        "index.js",
        b"exports.handler = (event, context, callback) => { process.once('beforeExit', () => callback(null, 'from-before-exit')); };\n",
    )]);
    let body = json!({
        "FunctionName": "be-fn",
        "Runtime": "nodejs20.x",
        "Role": "r",
        "Handler": "index.handler",
        "Timeout": 5,
        "Code": { "ZipFile": b64(&zip) },
    });
    assert_eq!(
        call(
            &e.server,
            "POST",
            "/2015-03-31/functions",
            &[],
            body.to_string().as_bytes()
        )
        .await
        .status,
        201
    );
    let r = call(
        &e.server,
        "POST",
        "/2015-03-31/functions/be-fn/invocations",
        &[],
        b"{}",
    )
    .await;
    assert_eq!(
        r.header("X-Amz-Function-Error"),
        None,
        "{}",
        String::from_utf8_lossy(&r.body)
    );
    assert_eq!(r.body, b"\"from-before-exit\"");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shutdown_cancels_in_flight_and_async_invocations() {
    if !has("python3") {
        eprintln!("skipping: python3 not available");
        return;
    }
    let e = env("shutdown");
    let zip = build_stored(&[(
        "app.py",
        b"import os, subprocess, time\ndef handler(event, context):\n    p = subprocess.Popen(['sleep', '30'])\n    open(os.path.join(os.environ['PID_DIR'], event['tag']), 'w').write(str(p.pid))\n    time.sleep(30)\n",
    )]);
    let body = json!({
        "FunctionName": "long-fn",
        "Runtime": "python3.12",
        "Role": "r",
        "Handler": "app.handler",
        "Timeout": 60,
        "Environment": { "Variables": { "PID_DIR": e.dir.to_string_lossy() } },
        "Code": { "ZipFile": b64(&zip) },
    });
    assert_eq!(
        call(
            &e.server,
            "POST",
            "/2015-03-31/functions",
            &[],
            body.to_string().as_bytes()
        )
        .await
        .status,
        201
    );

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let (tx, rx) = tokio::sync::oneshot::channel::<()>();
    let serving = tokio::spawn(devcloud_lambda::http::serve(
        listener,
        Arc::clone(&e.server),
        async {
            let _ = rx.await;
        },
    ));
    let send = |tag: &'static str, invocation_type: &'static str| async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let payload = format!(r#"{{"tag":"{tag}"}}"#);
        let mut s = tokio::net::TcpStream::connect(("127.0.0.1", port))
            .await
            .unwrap();
        s.write_all(
            format!(
                "POST /2015-03-31/functions/long-fn/invocations HTTP/1.1\r\nHost: x\r\nX-Amz-Invocation-Type: {invocation_type}\r\nContent-Length: {}\r\n\r\n{payload}",
                payload.len()
            )
            .as_bytes(),
        )
        .await
        .unwrap();
        let mut buf = Vec::new();
        let _ = s.read_to_end(&mut buf).await;
    };
    let sync_request = tokio::spawn(send("sync", "RequestResponse"));
    send("event", "Event").await;

    let read_pid = |tag: &str| {
        std::fs::read_to_string(e.dir.join(tag))
            .ok()
            .filter(|p| !p.is_empty())
    };
    let mut pids = Vec::new();
    for _ in 0..100 {
        if let (Some(a), Some(b)) = (read_pid("sync"), read_pid("event")) {
            pids = vec![a, b];
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    assert_eq!(pids.len(), 2, "both invocations should have started");
    assert!(pids.iter().all(|p| pid_alive(p)));

    let _ = tx.send(());
    tokio::time::timeout(std::time::Duration::from_secs(5), serving)
        .await
        .expect("serve must return promptly on shutdown")
        .unwrap()
        .unwrap();
    let _ = sync_request.await;
    let mut survivors = pids.clone();
    for _ in 0..40 {
        survivors.retain(|p| pid_alive(p));
        if survivors.is_empty() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    for p in &survivors {
        let _ = std::process::Command::new("kill").args(["-9", p]).status();
    }
    assert!(
        survivors.is_empty(),
        "descendants survived shutdown: {survivors:?}"
    );
}

// ── regression tests for the twenty-second review round ─────────────────────

#[tokio::test]
async fn crash_after_success_callback_is_a_function_error() {
    if !has("node") {
        eprintln!("skipping: node not available");
        return;
    }
    let e = env("crashafter");
    let zip = build_stored(&[(
        "index.js",
        b"exports.handler = (event, context, callback) => { setTimeout(() => { throw new Error('late crash'); }, 50); callback(null, 'ok'); };\n",
    )]);
    let body = json!({
        "FunctionName": "crash-fn",
        "Runtime": "nodejs20.x",
        "Role": "r",
        "Handler": "index.handler",
        "Timeout": 5,
        "Code": { "ZipFile": b64(&zip) },
    });
    assert_eq!(
        call(
            &e.server,
            "POST",
            "/2015-03-31/functions",
            &[],
            body.to_string().as_bytes()
        )
        .await
        .status,
        201
    );
    let r = call(
        &e.server,
        "POST",
        "/2015-03-31/functions/crash-fn/invocations",
        &[],
        b"{}",
    )
    .await;
    assert_eq!(
        r.header("X-Amz-Function-Error"),
        Some("Unhandled"),
        "{}",
        String::from_utf8_lossy(&r.body)
    );
    // As in the AWS Node runtime, the uncaught exception fails the
    // invocation with its own error, not as an anonymous runtime exit.
    assert_eq!(r.json()["errorType"], "Error");
    assert_eq!(r.json()["errorMessage"], "late crash");
}

#[tokio::test]
async fn malformed_environment_or_tags_are_rejected_without_losing_settings() {
    let e = env("badmaps");
    let created = create_python(&e.server, "maps-fn").await.json();
    assert_eq!(created["Environment"]["Variables"]["GREETING"], "hi");
    for bad in [
        json!({ "Environment": { "Variables": { "STAGE": 42 } } }),
        json!({ "Environment": { "Variables": ["STAGE"] } }),
        json!({ "Environment": "STAGE=dev" }),
    ] {
        let r = call(
            &e.server,
            "PUT",
            "/2015-03-31/functions/maps-fn/configuration",
            &[],
            bad.to_string().as_bytes(),
        )
        .await;
        assert_eq!(r.status, 400, "{bad}");
    }
    let cfg = call(
        &e.server,
        "GET",
        "/2015-03-31/functions/maps-fn/configuration",
        &[],
        b"",
    )
    .await
    .json();
    assert_eq!(
        cfg["Environment"]["Variables"]["GREETING"], "hi",
        "existing variables survive"
    );
    assert_eq!(cfg["RevisionId"], created["RevisionId"]);

    let arn = "arn%3Aaws%3Alambda%3Aus-east-1%3A000000000000%3Afunction%3Amaps-fn";
    let r = call(
        &e.server,
        "POST",
        &format!("/2017-03-31/tags/{arn}"),
        &[],
        br#"{"Tags":{"team":7}}"#,
    )
    .await;
    assert_eq!(r.status, 400);
    let tags = call(
        &e.server,
        "GET",
        &format!("/2017-03-31/tags/{arn}"),
        &[],
        b"",
    )
    .await
    .json();
    assert_eq!(tags["Tags"], json!({ "team": "dev" }));
}

// ── regression tests for the twenty-third review round ──────────────────────

#[tokio::test]
async fn s3_packages_are_not_held_to_the_direct_upload_cap() {
    let e = env("bigs3");
    let store = devcloud_s3::store::FileBucketStore::new(e.dir.join("s3/buckets"));
    store.create_bucket("artifacts").unwrap();
    // 51 MiB stored zip: over the 50 MB direct-upload limit, well under the
    // 250 MB unzipped limit.
    let blob = vec![b'z'; 51 * 1024 * 1024];
    let zip = build_stored(&[("app.py", PY_HANDLER.as_bytes()), ("blob.bin", &blob)]);
    store
        .put_object(devcloud_s3::objops::PutObjectInput {
            bucket: "artifacts".into(),
            key: "big.zip".into(),
            body: zip.clone(),
            ..Default::default()
        })
        .unwrap();
    let body = json!({
        "FunctionName": "big-fn",
        "Runtime": "python3.12",
        "Role": "r",
        "Handler": "app.handler",
        "Code": { "S3Bucket": "artifacts", "S3Key": "big.zip" },
    });
    let r = call(
        &e.server,
        "POST",
        "/2015-03-31/functions",
        &[],
        body.to_string().as_bytes(),
    )
    .await;
    assert_eq!(r.status, 201, "{}", String::from_utf8_lossy(&r.body));

    let direct = json!({
        "FunctionName": "big-direct",
        "Runtime": "python3.12",
        "Role": "r",
        "Handler": "app.handler",
        "Code": { "ZipFile": b64(&zip) },
    });
    let r = call(
        &e.server,
        "POST",
        "/2015-03-31/functions",
        &[],
        direct.to_string().as_bytes(),
    )
    .await;
    assert_eq!(r.status, 413, "direct uploads keep the 50 MB cap");
}

// ── regression tests for the twenty-fifth review round ──────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn chunked_invoke_bodies_reach_the_handler() {
    if !has("python3") {
        eprintln!("skipping: python3 not available");
        return;
    }
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let e = env("chunked");
    assert_eq!(create_python(&e.server, "chunk-fn").await.status, 201);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let (tx, rx) = tokio::sync::oneshot::channel::<()>();
    let serving = tokio::spawn(devcloud_lambda::http::serve(
        listener,
        Arc::clone(&e.server),
        async {
            let _ = rx.await;
        },
    ));
    let send = |request: String| async move {
        let mut s = tokio::net::TcpStream::connect(("127.0.0.1", port))
            .await
            .unwrap();
        s.write_all(request.as_bytes()).await.unwrap();
        let mut buf = Vec::new();
        s.read_to_end(&mut buf).await.unwrap();
        String::from_utf8_lossy(&buf).into_owned()
    };

    let chunked = "POST /2015-03-31/functions/chunk-fn/invocations HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\n5\r\n{\"val\r\n6\r\nue\":1}\r\n0\r\n\r\n";
    let resp = send(chunked.to_string()).await;
    assert!(resp.starts_with("HTTP/1.1 200"), "{resp}");
    let body = resp.split("\r\n\r\n").nth(1).unwrap();
    let v: Value = serde_json::from_str(body).unwrap();
    assert_eq!(
        v["echo"],
        json!({ "value": 1 }),
        "the handler got the real event, not {{}}"
    );

    let gzip = "POST /2015-03-31/functions/chunk-fn/invocations HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: gzip, chunked\r\n\r\n0\r\n\r\n";
    assert!(send(gzip.to_string()).await.starts_with("HTTP/1.1 501"));
    let broken = "POST /2015-03-31/functions/chunk-fn/invocations HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\nzz\r\n";
    assert!(send(broken.to_string()).await.starts_with("HTTP/1.1 400"));

    let _ = tx.send(());
    serving.await.unwrap().unwrap();
}

// ── runtime environment: layers (/opt), credentials, runtime versions ──────

/// Creates `name` from a single-file package and invokes it once with a log tail.
async fn deploy_and_invoke(
    server: &Arc<Server>,
    name: &str,
    runtime: &str,
    handler: &str,
    file: (&str, &[u8]),
) -> (Resp, String) {
    let zip = build_stored(&[file]);
    let body = json!({
        "FunctionName": name,
        "Runtime": runtime,
        "Role": "r",
        "Handler": handler,
        "Code": { "ZipFile": b64(&zip) },
    });
    let created = call(
        server,
        "POST",
        "/2015-03-31/functions",
        &[],
        body.to_string().as_bytes(),
    )
    .await;
    assert_eq!(
        created.status,
        201,
        "{}",
        String::from_utf8_lossy(&created.body)
    );
    let r = call(
        server,
        "POST",
        &format!("/2015-03-31/functions/{name}/invocations"),
        &[("X-Amz-Log-Type", "Tail")],
        b"{}",
    )
    .await;
    let log = base64::engine::general_purpose::STANDARD
        .decode(r.header("X-Amz-Log-Result").unwrap_or_default())
        .unwrap();
    (r, String::from_utf8(log).unwrap())
}

#[tokio::test]
async fn layer_directories_under_opt_are_importable() {
    let dir = temp_dir("optdir");
    let opt = dir.join("opt");
    let site = opt.join("python/lib/python3.12/site-packages");
    std::fs::create_dir_all(&site).unwrap();
    std::fs::write(opt.join("python/toplib.py"), "NAME = 'top'\n").unwrap();
    std::fs::write(site.join("sitelib.py"), "NAME = 'site'\n").unwrap();
    let node_modules = opt.join("nodejs/node_modules/nodelib");
    std::fs::create_dir_all(&node_modules).unwrap();
    std::fs::write(node_modules.join("index.js"), "exports.NAME = 'node';\n").unwrap();
    let server = Arc::new(Server::new(Config {
        opt_dir: Some(opt.clone()),
        ..config_for(&dir)
    }));

    if has("python3") {
        let (r, log) = deploy_and_invoke(
            &server,
            "py-layer",
            "python3.12",
            "app.handler",
            (
                "app.py",
                b"import toplib, sitelib\ndef handler(e, c):\n    return [toplib.NAME, sitelib.NAME]\n",
            ),
        )
        .await;
        assert_eq!(r.header("X-Amz-Function-Error"), None, "{log}");
        assert_eq!(r.json(), json!(["top", "site"]));
    } else {
        eprintln!("skipping python part: python3 not available");
    }
    if has("node") {
        let (r, log) = deploy_and_invoke(
            &server,
            "node-layer",
            "nodejs20.x",
            "index.handler",
            (
                "index.js",
                b"exports.handler = async () => require('nodelib').NAME;\n",
            ),
        )
        .await;
        assert_eq!(r.header("X-Amz-Function-Error"), None, "{log}");
        assert_eq!(r.body, b"\"node\"");
        // ES module handlers resolve layer dependencies too (Node's ESM
        // loader alone ignores NODE_PATH).
        let (r, log) = deploy_and_invoke(
            &server,
            "esm-layer",
            "nodejs20.x",
            "index.handler",
            (
                "index.mjs",
                b"import { NAME } from 'nodelib';\nexport const handler = async () => NAME;\n",
            ),
        )
        .await;
        assert_eq!(r.header("X-Amz-Function-Error"), None, "{log}");
        assert_eq!(r.body, b"\"node\"");
        // `.js` that is ESM by its package.json `"type": "module"`.
        let zip = build_stored(&[
            ("package.json", br#"{"type": "module"}"#),
            (
                "index.js",
                b"import { NAME } from 'nodelib';\nexport const handler = async () => NAME;\n",
            ),
        ]);
        let body = json!({
            "FunctionName": "esm-js-layer", "Runtime": "nodejs20.x", "Role": "r",
            "Handler": "index.handler", "Code": { "ZipFile": b64(&zip) },
        });
        let created = call(
            &server,
            "POST",
            "/2015-03-31/functions",
            &[],
            body.to_string().as_bytes(),
        )
        .await;
        assert_eq!(created.status, 201);
        let r = call(
            &server,
            "POST",
            "/2015-03-31/functions/esm-js-layer/invocations",
            &[],
            b"{}",
        )
        .await;
        assert_eq!(
            r.header("X-Amz-Function-Error"),
            None,
            "{}",
            String::from_utf8_lossy(&r.body)
        );
        assert_eq!(r.body, b"\"node\"");
    } else {
        eprintln!("skipping node part: node not available");
    }
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test]
async fn configured_function_credentials_reach_the_handler() {
    if !has("python3") {
        eprintln!("skipping: python3 not available");
        return;
    }
    const READ_CREDS: &[u8] = b"import os\ndef handler(e, c):\n    return [os.environ.get(k) for k in ('AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY', 'AWS_SESSION_TOKEN')]\n";

    let e = env("nocreds");
    let (r, _) = deploy_and_invoke(
        &e.server,
        "creds-fn",
        "python3.12",
        "app.handler",
        ("app.py", READ_CREDS),
    )
    .await;
    assert_eq!(
        r.json(),
        json!([null, null, null]),
        "no credentials unless configured"
    );

    let dir = temp_dir("creds");
    let server = Arc::new(Server::new(Config {
        function_credentials: Some(devcloud_lambda::runtime::FunctionCredentials {
            access_key_id: "AKIDLOCAL".into(),
            secret_access_key: "local-secret".into(),
            session_token: String::new(),
        }),
        ..config_for(&dir)
    }));
    let (r, log) = deploy_and_invoke(
        &server,
        "creds-fn",
        "python3.12",
        "app.handler",
        ("app.py", READ_CREDS),
    )
    .await;
    assert_eq!(r.json(), json!(["AKIDLOCAL", "local-secret", null]));
    assert!(
        !log.contains("local-secret"),
        "credentials never reach the log: {log}"
    );
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test]
async fn runtime_version_mismatch_is_logged_as_a_warning() {
    let e = env("vermismatch");
    if has("python3") {
        // No host has python3.99: the default python3 runs it, with a warning.
        let (r, log) = deploy_and_invoke(
            &e.server,
            "py-ver",
            "python3.99",
            "app.handler",
            ("app.py", b"def handler(e, c):\n    return 1\n"),
        )
        .await;
        assert_eq!(r.header("X-Amz-Function-Error"), None, "{log}");
        assert!(
            log.contains("[WARNING] devcloud: runtime python3.99 is running on Python "),
            "{log}"
        );
    } else {
        eprintln!("skipping python part: python3 not available");
    }
    if has("node") {
        let (r, log) = deploy_and_invoke(
            &e.server,
            "node-ver",
            "nodejs99.x",
            "index.handler",
            ("index.js", b"exports.handler = async () => 1;\n"),
        )
        .await;
        assert_eq!(r.header("X-Amz-Function-Error"), None, "{log}");
        assert!(
            log.contains("[WARNING] devcloud: runtime nodejs99.x is running on Node.js "),
            "{log}"
        );
    } else {
        eprintln!("skipping node part: node not available");
    }
}

#[tokio::test]
async fn relative_opt_dir_is_resolved_against_devcloud_cwd() {
    if !has("python3") {
        eprintln!("skipping: python3 not available");
        return;
    }
    let dir = temp_dir("relopt");
    let opt = dir.join("opt");
    std::fs::create_dir_all(opt.join("python")).unwrap();
    std::fs::write(opt.join("python/rellib.py"), "NAME = 'rel'\n").unwrap();
    let rel = pathdiff(&opt, &std::env::current_dir().unwrap());
    let server = Arc::new(Server::new(Config {
        opt_dir: Some(PathBuf::from(rel)),
        ..config_for(&dir)
    }));
    let (r, log) = deploy_and_invoke(
        &server,
        "rel-opt",
        "python3.12",
        "app.handler",
        (
            "app.py",
            b"import rellib\ndef handler(e, c):\n    return rellib.NAME\n",
        ),
    )
    .await;
    assert_eq!(r.header("X-Amz-Function-Error"), None, "{log}");
    assert_eq!(r.body, b"\"rel\"");
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test]
async fn environment_variable_names_follow_the_lambda_pattern() {
    let e = env("envnames");
    assert_eq!(create_python(&e.server, "names-fn").await.status, 201);
    for bad in [
        "ZZZ=ignored\nAWS_LAMBDA_FUNCTION_NAME",
        "1BAD",
        "_X",
        "WITH-DASH",
        "with space",
    ] {
        let r = call(
            &e.server,
            "PUT",
            "/2015-03-31/functions/names-fn/configuration",
            &[],
            json!({ "Environment": { "Variables": { bad: "x" } } })
                .to_string()
                .as_bytes(),
        )
        .await;
        assert_eq!(
            r.status,
            400,
            "{bad:?}: {}",
            String::from_utf8_lossy(&r.body)
        );
    }
    let r = call(
        &e.server,
        "PUT",
        "/2015-03-31/functions/names-fn/configuration",
        &[],
        json!({ "Environment": { "Variables": { "Ok_Name2": "x", "A": "1" } } })
            .to_string()
            .as_bytes(),
    )
    .await;
    assert_eq!(r.status, 200, "{}", String::from_utf8_lossy(&r.body));
}

// ── runtime bootstrap fidelity ──────────────────────────────────────────────

#[tokio::test]
async fn python_sys_exit_reports_its_exit_status() {
    if !has("python3") {
        eprintln!("skipping: python3 not available");
        return;
    }
    let e = env("pyexit");
    let (r, _) = deploy_and_invoke(
        &e.server,
        "exit-fn",
        "python3.12",
        "app.handler",
        (
            "app.py",
            b"import sys\ndef handler(event, context):\n    sys.exit(3)\n",
        ),
    )
    .await;
    assert_eq!(r.header("X-Amz-Function-Error"), Some("Unhandled"));
    let doc = r.json();
    assert_eq!(doc["errorType"], "Runtime.ExitError");
    let msg = doc["errorMessage"].as_str().unwrap();
    assert!(msg.starts_with("RequestId: "), "{msg}");
    assert!(
        msg.ends_with("Error: Runtime exited with error: exit status 3"),
        "{msg}"
    );
}

#[tokio::test]
async fn python_decimal_results_marshal_as_numbers() {
    if !has("python3") {
        eprintln!("skipping: python3 not available");
        return;
    }
    let e = env("pydecimal");
    let (r, _) = deploy_and_invoke(
        &e.server,
        "decimal-fn",
        "python3.12",
        "app.handler",
        (
            "app.py",
            b"from decimal import Decimal\ndef handler(event, context):\n    return {'count': Decimal('3'), 'price': Decimal('1.5')}\n",
        ),
    )
    .await;
    assert_eq!(
        r.header("X-Amz-Function-Error"),
        None,
        "{}",
        String::from_utf8_lossy(&r.body)
    );
    assert_eq!(r.json(), json!({ "count": 3, "price": 1.5 }));
}

#[tokio::test]
async fn node_unhandled_rejection_fails_the_invocation_and_recovers() {
    if !has("node") {
        eprintln!("skipping: node not available");
        return;
    }
    let e = env("noderej");
    let (r, _) = deploy_and_invoke(
        &e.server,
        "reject-fn",
        "nodejs20.x",
        "index.handler",
        (
            "index.js",
            b"exports.handler = async (event) => { if (!event.ok) { Promise.reject(new Error('dangling')); await new Promise((r) => setTimeout(r, 200)); } return 'fine'; };\n",
        ),
    )
    .await;
    assert_eq!(r.header("X-Amz-Function-Error"), Some("Unhandled"));
    assert_eq!(r.json()["errorType"], "Runtime.UnhandledPromiseRejection");
    assert_eq!(r.json()["errorMessage"], "Error: dangling");
    // The broken environment is replaced; the next invocation succeeds.
    let ok = call(
        &e.server,
        "POST",
        "/2015-03-31/functions/reject-fn/invocations",
        &[],
        br#"{"ok":true}"#,
    )
    .await;
    assert_eq!(
        ok.header("X-Amz-Function-Error"),
        None,
        "{}",
        String::from_utf8_lossy(&ok.body)
    );
    assert_eq!(ok.json(), json!("fine"));
}

#[tokio::test]
async fn node_null_module_exports_is_handler_not_found() {
    if !has("node") {
        eprintln!("skipping: node not available");
        return;
    }
    let e = env("nodenull");
    let (r, _) = deploy_and_invoke(
        &e.server,
        "null-fn",
        "nodejs20.x",
        "index.handler",
        ("index.js", b"module.exports = null;\n"),
    )
    .await;
    assert_eq!(r.header("X-Amz-Function-Error"), Some("Unhandled"));
    assert_eq!(r.json()["errorType"], "Runtime.HandlerNotFound");
}
