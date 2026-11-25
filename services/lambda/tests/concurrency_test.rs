//! Concurrency: Put/Get/DeleteFunctionConcurrency, runtime 429
//! TooManyRequestsException enforcement, GetAccountSettings unreserved
//! accounting, and Put/Get/List/DeleteProvisionedConcurrencyConfig.

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

fn temp_dir(tag: &str) -> PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let dir = std::env::temp_dir().join(format!(
        "devcloud-lambda-conc-{tag}-{}-{nanos}",
        std::process::id()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn config_for(dir: &std::path::Path) -> Config {
    Config {
        addr: "127.0.0.1:19010".into(),
        endpoint: "http://127.0.0.1:19010".into(),
        region: "us-east-1".into(),
        account_id: "000000000000".into(),
        auth_mode: "relaxed".into(),
        storage_path: dir.join("lambda").to_string_lossy().into_owned(),
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

fn has_python() -> bool {
    std::process::Command::new("python3")
        .arg("--version")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

fn b64(data: &[u8]) -> String {
    base64::engine::general_purpose::STANDARD.encode(data)
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

#[tokio::test]
async fn reserved_concurrency_crud_account_limits_and_runtime_throttling() {
    if !has_python() {
        eprintln!("skipping: python3 not available");
        return;
    }
    let e = env("reserved");
    let zip = build_stored(&[(
        "app.py",
        b"import time\ndef handler(event, context):\n    time.sleep(event.get('sleep', 0))\n    return {'ok': True}\n",
    )]);
    assert_eq!(
        call(
            &e.server,
            "POST",
            "/2015-03-31/functions",
            &[],
            json!({
                "FunctionName": "conc-fn",
                "Runtime": "python3.12",
                "Role": "r",
                "Handler": "app.handler",
                "Timeout": 10,
                "Publish": true,
                "Code": { "ZipFile": b64(&zip) },
            })
            .to_string()
            .as_bytes(),
        )
        .await
        .status,
        201
    );

    // Before setting reserved concurrency, GetFunctionConcurrency returns {}.
    let empty = call(
        &e.server,
        "GET",
        "/2019-09-30/functions/conc-fn/concurrency",
        &[],
        b"",
    )
    .await;
    assert_eq!(empty.status, 200);
    assert_eq!(empty.json(), json!({}));

    // Validation: negative, qualified, and exceeding account unreserved floor (100).
    assert_eq!(
        call(
            &e.server,
            "PUT",
            "/2017-10-31/functions/conc-fn/concurrency",
            &[],
            br#"{"ReservedConcurrentExecutions": -1}"#,
        )
        .await
        .status,
        400
    );
    assert_eq!(
        call(
            &e.server,
            "PUT",
            "/2017-10-31/functions/conc-fn%3A1/concurrency",
            &[],
            br#"{"ReservedConcurrentExecutions": 5}"#,
        )
        .await
        .status,
        400
    );
    let too_high = call(
        &e.server,
        "PUT",
        "/2017-10-31/functions/conc-fn/concurrency",
        &[],
        br#"{"ReservedConcurrentExecutions": 901}"#,
    )
    .await;
    assert_eq!(too_high.status, 400);
    assert_eq!(
        too_high.header("X-Amzn-ErrorType"),
        Some("LimitExceededException")
    );

    // Reserved = 0 throttles all invocations (and Function URLs) with 429.
    let put0 = call(
        &e.server,
        "PUT",
        "/2017-10-31/functions/conc-fn/concurrency",
        &[],
        br#"{"ReservedConcurrentExecutions": 0}"#,
    )
    .await;
    assert_eq!(put0.status, 200);
    assert_eq!(put0.json()["ReservedConcurrentExecutions"], 0);

    let got_fn = call(&e.server, "GET", "/2015-03-31/functions/conc-fn", &[], b"").await;
    assert_eq!(
        got_fn.json()["Concurrency"]["ReservedConcurrentExecutions"],
        0
    );

    let throttled = call(
        &e.server,
        "POST",
        "/2015-03-31/functions/conc-fn/invocations",
        &[],
        b"{}",
    )
    .await;
    assert_eq!(throttled.status, 429);
    assert_eq!(
        throttled.header("X-Amzn-ErrorType"),
        Some("TooManyRequestsException")
    );

    // DryRun still succeeds with 204.
    let dry = call(
        &e.server,
        "POST",
        "/2015-03-31/functions/conc-fn/invocations",
        &[("X-Amz-Invocation-Type", "DryRun")],
        b"{}",
    )
    .await;
    assert_eq!(dry.status, 204);

    // Function URL maps 429 cleanly too.
    let url = call(
        &e.server,
        "POST",
        "/2021-10-31/functions/conc-fn/url",
        &[],
        br#"{"AuthType": "NONE"}"#,
    )
    .await;
    assert_eq!(url.status, 201);
    let url_hit = call(&e.server, "GET", "/urls/conc-fn/", &[], b"").await;
    assert_eq!(url_hit.status, 429);

    // Reserved = 1 updates account settings and throttles a second overlapping invoke.
    assert_eq!(
        call(
            &e.server,
            "PUT",
            "/2017-10-31/functions/conc-fn/concurrency",
            &[],
            br#"{"ReservedConcurrentExecutions": 1}"#,
        )
        .await
        .status,
        200
    );
    let settings = call(&e.server, "GET", "/2016-08-19/account-settings/", &[], b"").await;
    assert_eq!(
        settings.json()["AccountLimit"]["UnreservedConcurrentExecutions"],
        999
    );

    // Warm up once so the sleeping invocation enters Python immediately.
    assert_eq!(
        call(
            &e.server,
            "POST",
            "/2015-03-31/functions/conc-fn/invocations",
            &[],
            b"{}",
        )
        .await
        .status,
        200
    );

    let srv = Arc::clone(&e.server);
    let first = tokio::spawn(async move {
        call(
            &srv,
            "POST",
            "/2015-03-31/functions/conc-fn/invocations",
            &[],
            br#"{"sleep": 0.5}"#,
        )
        .await
    });
    tokio::time::sleep(Duration::from_millis(120)).await;
    let second = call(
        &e.server,
        "POST",
        "/2015-03-31/functions/conc-fn/invocations",
        &[],
        b"{}",
    )
    .await;
    assert_eq!(second.status, 429);
    assert_eq!(
        second.header("X-Amzn-ErrorType"),
        Some("TooManyRequestsException")
    );
    let first_resp = first.await.unwrap();
    assert_eq!(first_resp.status, 200);

    // Once the first finishes, the permit is released and the next invoke succeeds.
    let third = call(
        &e.server,
        "POST",
        "/2015-03-31/functions/conc-fn/invocations",
        &[],
        b"{}",
    )
    .await;
    assert_eq!(third.status, 200);

    // DeleteFunctionConcurrency clears it and restores account unreserved concurrency.
    assert_eq!(
        call(
            &e.server,
            "DELETE",
            "/2017-10-31/functions/conc-fn/concurrency",
            &[],
            b"",
        )
        .await
        .status,
        204
    );
    let after_del = call(
        &e.server,
        "GET",
        "/2019-09-30/functions/conc-fn/concurrency",
        &[],
        b"",
    )
    .await;
    assert_eq!(after_del.json(), json!({}));
    let settings_after = call(&e.server, "GET", "/2016-08-19/account-settings/", &[], b"").await;
    assert_eq!(
        settings_after.json()["AccountLimit"]["UnreservedConcurrentExecutions"],
        1000
    );

    e.server.shutdown().await;
}

#[tokio::test]
async fn provisioned_concurrency_crud_validation_and_persistence() {
    let e = env("provisioned");
    let zip = build_stored(&[("app.py", b"def handler(e, c):\n    return 1\n")]);
    assert_eq!(
        call(
            &e.server,
            "POST",
            "/2015-03-31/functions",
            &[],
            json!({
                "FunctionName": "prov-fn",
                "Runtime": "python3.12",
                "Role": "r",
                "Handler": "app.handler",
                "Publish": true,
                "Code": { "ZipFile": b64(&zip) },
            })
            .to_string()
            .as_bytes(),
        )
        .await
        .status,
        201
    );
    assert_eq!(
        call(
            &e.server,
            "POST",
            "/2015-03-31/functions/prov-fn/aliases",
            &[],
            br#"{"Name": "prod", "FunctionVersion": "1"}"#,
        )
        .await
        .status,
        201
    );

    // Unqualified and $LATEST are rejected.
    for target in [
        "/2019-09-30/functions/prov-fn/provisioned-concurrency",
        "/2019-09-30/functions/prov-fn/provisioned-concurrency?Qualifier=%24LATEST",
    ] {
        let r = call(
            &e.server,
            "PUT",
            target,
            &[],
            br#"{"ProvisionedConcurrentExecutions": 2}"#,
        )
        .await;
        assert_eq!(r.status, 400, "{target}");
    }

    // Put on version 1 and alias prod.
    let put_v1 = call(
        &e.server,
        "PUT",
        "/2019-09-30/functions/prov-fn/provisioned-concurrency?Qualifier=1",
        &[],
        br#"{"ProvisionedConcurrentExecutions": 2}"#,
    )
    .await;
    assert_eq!(put_v1.status, 202);
    assert_eq!(put_v1.json()["Status"], "READY");
    assert_eq!(put_v1.json()["RequestedProvisionedConcurrentExecutions"], 2);
    assert_eq!(put_v1.json()["AllocatedProvisionedConcurrentExecutions"], 2);
    assert_eq!(put_v1.json()["AvailableProvisionedConcurrentExecutions"], 2);

    let put_alias = call(
        &e.server,
        "PUT",
        "/2019-09-30/functions/prov-fn/provisioned-concurrency?Qualifier=prod",
        &[],
        br#"{"ProvisionedConcurrentExecutions": 3}"#,
    )
    .await;
    assert_eq!(put_alias.status, 202);

    // Reserved concurrency cannot be set below total provisioned (5).
    assert_eq!(
        call(
            &e.server,
            "PUT",
            "/2017-10-31/functions/prov-fn/concurrency",
            &[],
            br#"{"ReservedConcurrentExecutions": 4}"#,
        )
        .await
        .status,
        400
    );
    assert_eq!(
        call(
            &e.server,
            "PUT",
            "/2017-10-31/functions/prov-fn/concurrency",
            &[],
            br#"{"ReservedConcurrentExecutions": 5}"#,
        )
        .await
        .status,
        200
    );
    // Raising provisioned above reserved is rejected.
    assert_eq!(
        call(
            &e.server,
            "PUT",
            "/2019-09-30/functions/prov-fn/provisioned-concurrency?Qualifier=prod",
            &[],
            br#"{"ProvisionedConcurrentExecutions": 4}"#,
        )
        .await
        .status,
        400
    );

    // Get & ListProvisionedConcurrencyConfigs.
    let got = call(
        &e.server,
        "GET",
        "/2019-09-30/functions/prov-fn/provisioned-concurrency?Qualifier=1",
        &[],
        b"",
    )
    .await;
    assert_eq!(got.status, 200);
    assert_eq!(got.json()["RequestedProvisionedConcurrentExecutions"], 2);

    let listed = call(
        &e.server,
        "GET",
        "/2019-09-30/functions/prov-fn/provisioned-concurrency?List=ALL",
        &[],
        b"",
    )
    .await;
    assert_eq!(listed.status, 200);
    assert_eq!(
        listed.json()["ProvisionedConcurrencyConfigs"]
            .as_array()
            .unwrap()
            .len(),
        2
    );

    // Survives restart.
    let reloaded = Arc::new(Server::new(config_for(&e.dir)));
    assert!(reloaded.load_err().is_none());
    let got_after = call(
        &reloaded,
        "GET",
        "/2019-09-30/functions/prov-fn/provisioned-concurrency?Qualifier=prod",
        &[],
        b"",
    )
    .await;
    assert_eq!(got_after.status, 200);
    assert_eq!(
        got_after.json()["RequestedProvisionedConcurrentExecutions"],
        3
    );

    // Delete & 404.
    assert_eq!(
        call(
            &reloaded,
            "DELETE",
            "/2019-09-30/functions/prov-fn/provisioned-concurrency?Qualifier=1",
            &[],
            b"",
        )
        .await
        .status,
        204
    );
    let gone = call(
        &reloaded,
        "GET",
        "/2019-09-30/functions/prov-fn/provisioned-concurrency?Qualifier=1",
        &[],
        b"",
    )
    .await;
    assert_eq!(gone.status, 404);
    assert_eq!(
        gone.header("X-Amzn-ErrorType"),
        Some("ProvisionedConcurrencyConfigNotFoundException")
    );
}
