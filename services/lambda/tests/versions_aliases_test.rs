//! Versions and aliases: PublishVersion, ListVersionsByFunction,
//! ListFunctions?FunctionVersion=ALL, Publish on CreateFunction/UpdateFunctionCode,
//! DeleteFunction?Qualifier=<version>, Create/Get/Update/Delete/ListAliases,
//! deterministic weighted routing, alias Function URLs, and warm pool isolation.

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
        "devcloud-lambda-va-{tag}-{}-{nanos}",
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

fn py_zip(label: &str) -> Vec<u8> {
    let src = format!(
        "import os\ndef handler(event, context):\n    return {{\n        'label': '{label}',\n        'stage': os.environ.get('STAGE'),\n        'version': context.function_version,\n        'arn': context.invoked_function_arn,\n        'envVersion': os.environ.get('AWS_LAMBDA_FUNCTION_VERSION'),\n    }}\n"
    );
    build_stored(&[("app.py", src.as_bytes())])
}

#[tokio::test]
async fn publish_version_snapshots_invoke_and_persistence() {
    if !has_python() {
        eprintln!("skipping: python3 not available");
        return;
    }
    let e = env("versions");
    let create = json!({
        "FunctionName": "ver-fn",
        "Runtime": "python3.12",
        "Role": "arn:aws:iam::000000000000:role/lambda",
        "Handler": "app.handler",
        "Description": "initial",
        "Environment": { "Variables": { "STAGE": "v1" } },
        "Code": { "ZipFile": b64(&py_zip("code-v1")) },
    });
    let created = call(
        &e.server,
        "POST",
        "/2015-03-31/functions",
        &[],
        create.to_string().as_bytes(),
    )
    .await;
    assert_eq!(created.status, 201);
    let rev1 = created.json()["RevisionId"].as_str().unwrap().to_string();
    let sha1 = created.json()["CodeSha256"].as_str().unwrap().to_string();

    // Precondition checks on PublishVersion.
    let bad_rev = call(
        &e.server,
        "POST",
        "/2015-03-31/functions/ver-fn/versions",
        &[],
        br#"{"RevisionId": "wrong"}"#,
    )
    .await;
    assert_eq!(bad_rev.status, 412);
    let bad_sha = call(
        &e.server,
        "POST",
        "/2015-03-31/functions/ver-fn/versions",
        &[],
        br#"{"CodeSha256": "wrong"}"#,
    )
    .await;
    assert_eq!(bad_sha.status, 400);

    // Publish version 1.
    let v1 = call(
        &e.server,
        "POST",
        "/2015-03-31/functions/ver-fn/versions",
        &[],
        json!({ "RevisionId": rev1, "CodeSha256": sha1 })
            .to_string()
            .as_bytes(),
    )
    .await;
    assert_eq!(v1.status, 201, "{}", String::from_utf8_lossy(&v1.body));
    assert_eq!(v1.json()["Version"], "1");
    assert_eq!(
        v1.json()["FunctionArn"],
        "arn:aws:lambda:us-east-1:000000000000:function:ver-fn:1"
    );

    // Idempotent when nothing changed since latest published version.
    let v1_again = call(
        &e.server,
        "POST",
        "/2015-03-31/functions/ver-fn/versions",
        &[],
        b"{}",
    )
    .await;
    assert_eq!(v1_again.status, 201);
    assert_eq!(v1_again.json()["Version"], "1");

    // Update $LATEST code and env, then publish version 2 with custom description.
    let upd_code = call(
        &e.server,
        "PUT",
        "/2015-03-31/functions/ver-fn/code",
        &[],
        json!({ "ZipFile": b64(&py_zip("code-v2")) })
            .to_string()
            .as_bytes(),
    )
    .await;
    assert_eq!(upd_code.status, 200);
    let upd_cfg = call(
        &e.server,
        "PUT",
        "/2015-03-31/functions/ver-fn/configuration",
        &[],
        json!({ "Environment": { "Variables": { "STAGE": "v2" } } })
            .to_string()
            .as_bytes(),
    )
    .await;
    assert_eq!(upd_cfg.status, 200);
    let v2 = call(
        &e.server,
        "POST",
        "/2015-03-31/functions/ver-fn/versions",
        &[],
        br#"{"Description": "release 2"}"#,
    )
    .await;
    assert_eq!(v2.status, 201);
    assert_eq!(v2.json()["Version"], "2");
    assert_eq!(v2.json()["Description"], "release 2");

    // Update $LATEST again without publishing.
    assert_eq!(
        call(
            &e.server,
            "PUT",
            "/2015-03-31/functions/ver-fn/code",
            &[],
            json!({ "ZipFile": b64(&py_zip("code-latest")) })
                .to_string()
                .as_bytes(),
        )
        .await
        .status,
        200
    );

    // ListVersionsByFunction with pagination.
    let page1 = call(
        &e.server,
        "GET",
        "/2015-03-31/functions/ver-fn/versions?MaxItems=2",
        &[],
        b"",
    )
    .await;
    assert_eq!(page1.status, 200);
    let v_p1 = page1.json()["Versions"].as_array().unwrap().clone();
    assert_eq!(v_p1.len(), 2);
    assert_eq!(v_p1[0]["Version"], "$LATEST");
    assert_eq!(v_p1[1]["Version"], "1");
    let marker = page1.json()["NextMarker"].as_str().unwrap().to_string();
    let page2 = call(
        &e.server,
        "GET",
        &format!("/2015-03-31/functions/ver-fn/versions?MaxItems=2&Marker={marker}"),
        &[],
        b"",
    )
    .await;
    let v_p2 = page2.json()["Versions"].as_array().unwrap().clone();
    assert_eq!(v_p2.len(), 1);
    assert_eq!(v_p2[0]["Version"], "2");
    assert!(page2.json().get("NextMarker").is_none());

    // ListFunctions?FunctionVersion=ALL includes every version; default returns only $LATEST.
    let all = call(
        &e.server,
        "GET",
        "/2015-03-31/functions?FunctionVersion=ALL",
        &[],
        b"",
    )
    .await;
    assert_eq!(all.json()["Functions"].as_array().unwrap().len(), 3);
    let default_list = call(&e.server, "GET", "/2015-03-31/functions", &[], b"").await;
    assert_eq!(
        default_list.json()["Functions"].as_array().unwrap().len(),
        1
    );

    // GetFunction?Qualifier=1 still serves version 1's zip package.
    let got_v1 = call(
        &e.server,
        "GET",
        "/2015-03-31/functions/ver-fn?Qualifier=1",
        &[],
        b"",
    )
    .await;
    assert_eq!(got_v1.status, 200);
    let loc = got_v1.json()["Code"]["Location"]
        .as_str()
        .unwrap()
        .to_string();
    let loc_path = loc.strip_prefix("http://127.0.0.1:19010").unwrap();
    let v1_zip = call(&e.server, "GET", loc_path, &[], b"").await;
    assert_eq!(v1_zip.status, 200);
    assert!(v1_zip.body.starts_with(b"PK"));

    // Invoke v1, v2, and $LATEST: each runs its own immutable code & config.
    let r1 = call(
        &e.server,
        "POST",
        "/2015-03-31/functions/ver-fn/invocations?Qualifier=1",
        &[("X-Amz-Log-Type", "Tail")],
        b"{}",
    )
    .await;
    assert_eq!(r1.status, 200, "{}", String::from_utf8_lossy(&r1.body));
    assert_eq!(r1.header("X-Amz-Executed-Version"), Some("1"));
    assert_eq!(r1.json()["label"], "code-v1");
    assert_eq!(r1.json()["stage"], "v1");
    assert_eq!(r1.json()["version"], "1");
    assert_eq!(r1.json()["envVersion"], "1");
    assert_eq!(
        r1.json()["arn"],
        "arn:aws:lambda:us-east-1:000000000000:function:ver-fn:1"
    );
    assert!(r1.log().contains("Version: 1\n"), "{}", r1.log());

    let r2 = call(
        &e.server,
        "POST",
        "/2015-03-31/functions/ver-fn%3A2/invocations",
        &[],
        b"{}",
    )
    .await;
    assert_eq!(r2.status, 200);
    assert_eq!(r2.header("X-Amz-Executed-Version"), Some("2"));
    assert_eq!(r2.json()["label"], "code-v2");
    assert_eq!(r2.json()["stage"], "v2");

    let rl = call(
        &e.server,
        "POST",
        "/2015-03-31/functions/ver-fn/invocations",
        &[],
        b"{}",
    )
    .await;
    assert_eq!(rl.status, 200);
    assert_eq!(rl.header("X-Amz-Executed-Version"), Some("$LATEST"));
    assert_eq!(rl.json()["label"], "code-latest");
    // All three versions remain warm concurrently without evicting each other.
    assert_eq!(e.server.idle_environments("ver-fn"), 3);

    // Updating $LATEST retires only $LATEST's warm environment, not v1 or v2.
    assert_eq!(
        call(
            &e.server,
            "PUT",
            "/2015-03-31/functions/ver-fn/configuration",
            &[],
            br#"{"Description": "changed latest"}"#,
        )
        .await
        .status,
        200
    );
    assert_eq!(e.server.idle_environments("ver-fn"), 2);

    // Deleting version 1 removes only version 1; next publish gets version 3.
    let del_v1 = call(
        &e.server,
        "DELETE",
        "/2015-03-31/functions/ver-fn?Qualifier=1",
        &[],
        b"",
    )
    .await;
    assert_eq!(del_v1.status, 204);
    assert_eq!(
        call(
            &e.server,
            "GET",
            "/2015-03-31/functions/ver-fn?Qualifier=1",
            &[],
            b"",
        )
        .await
        .status,
        404
    );
    let v3 = call(
        &e.server,
        "POST",
        "/2015-03-31/functions/ver-fn/versions",
        &[],
        b"{}",
    )
    .await;
    assert_eq!(v3.status, 201);
    assert_eq!(v3.json()["Version"], "3");

    e.server.shutdown().await;

    // Versions survive a server restart.
    let reloaded = Arc::new(Server::new(config_for(&e.dir)));
    assert!(reloaded.load_err().is_none());
    let r2_after = call(
        &reloaded,
        "POST",
        "/2015-03-31/functions/ver-fn/invocations?Qualifier=2",
        &[],
        b"{}",
    )
    .await;
    assert_eq!(r2_after.status, 200);
    assert_eq!(r2_after.json()["label"], "code-v2");
    reloaded.shutdown().await;
}

#[tokio::test]
async fn create_and_update_code_with_publish_flag() {
    let e = env("publish-flag");
    let created = call(
        &e.server,
        "POST",
        "/2015-03-31/functions",
        &[],
        json!({
            "FunctionName": "pub-fn",
            "Runtime": "python3.12",
            "Role": "r",
            "Handler": "app.handler",
            "Publish": true,
            "Code": { "ZipFile": b64(&py_zip("v1")) },
        })
        .to_string()
        .as_bytes(),
    )
    .await;
    assert_eq!(created.status, 201);
    assert_eq!(created.json()["Version"], "1");
    assert_eq!(
        created.json()["FunctionArn"],
        "arn:aws:lambda:us-east-1:000000000000:function:pub-fn:1"
    );

    let updated = call(
        &e.server,
        "PUT",
        "/2015-03-31/functions/pub-fn/code",
        &[],
        json!({
            "ZipFile": b64(&py_zip("v2")),
            "Publish": true,
        })
        .to_string()
        .as_bytes(),
    )
    .await;
    assert_eq!(updated.status, 200);
    assert_eq!(updated.json()["Version"], "2");

    let versions = call(
        &e.server,
        "GET",
        "/2015-03-31/functions/pub-fn/versions",
        &[],
        b"",
    )
    .await;
    let versions_json = versions.json();
    let list: Vec<&str> = versions_json["Versions"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v["Version"].as_str().unwrap())
        .collect();
    assert_eq!(list, vec!["$LATEST", "1", "2"]);
}

#[tokio::test]
async fn alias_crud_weighted_routing_and_alias_function_url() {
    if !has_python() {
        eprintln!("skipping: python3 not available");
        return;
    }
    let e = env("aliases");
    assert_eq!(
        call(
            &e.server,
            "POST",
            "/2015-03-31/functions",
            &[],
            json!({
                "FunctionName": "al-fn",
                "Runtime": "python3.12",
                "Role": "r",
                "Handler": "app.handler",
                "Publish": true,
                "Code": { "ZipFile": b64(&py_zip("v1")) },
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
            "PUT",
            "/2015-03-31/functions/al-fn/code",
            &[],
            json!({ "ZipFile": b64(&py_zip("v2")), "Publish": true })
                .to_string()
                .as_bytes(),
        )
        .await
        .status,
        200
    );

    // Invalid alias names are rejected.
    for bad_name in ["123", "bad name", "$LATEST"] {
        let r = call(
            &e.server,
            "POST",
            "/2015-03-31/functions/al-fn/aliases",
            &[],
            json!({ "Name": bad_name, "FunctionVersion": "1" })
                .to_string()
                .as_bytes(),
        )
        .await;
        assert_eq!(r.status, 400, "{bad_name}");
    }

    // Create alias `prod -> 1`.
    let prod = call(
        &e.server,
        "POST",
        "/2015-03-31/functions/al-fn/aliases",
        &[],
        json!({ "Name": "prod", "FunctionVersion": "1", "Description": "production" })
            .to_string()
            .as_bytes(),
    )
    .await;
    assert_eq!(prod.status, 201, "{}", String::from_utf8_lossy(&prod.body));
    assert_eq!(
        prod.json()["AliasArn"],
        "arn:aws:lambda:us-east-1:000000000000:function:al-fn:prod"
    );
    let rev = prod.json()["RevisionId"].as_str().unwrap().to_string();

    // Duplicate alias rejected.
    let dup = call(
        &e.server,
        "POST",
        "/2015-03-31/functions/al-fn/aliases",
        &[],
        json!({ "Name": "prod", "FunctionVersion": "1" })
            .to_string()
            .as_bytes(),
    )
    .await;
    assert_eq!(dup.status, 409);

    // Deleting a version referenced by an alias is refused; deleting via alias is refused.
    assert_eq!(
        call(
            &e.server,
            "DELETE",
            "/2015-03-31/functions/al-fn?Qualifier=1",
            &[],
            b"",
        )
        .await
        .status,
        409
    );
    assert_eq!(
        call(
            &e.server,
            "DELETE",
            "/2015-03-31/functions/al-fn?Qualifier=prod",
            &[],
            b"",
        )
        .await
        .status,
        400
    );

    // GetAlias & ListAliases with FunctionVersion filter.
    let got = call(
        &e.server,
        "GET",
        "/2015-03-31/functions/al-fn/aliases/prod",
        &[],
        b"",
    )
    .await;
    assert_eq!(got.status, 200);
    assert_eq!(got.json()["FunctionVersion"], "1");

    let list_v1 = call(
        &e.server,
        "GET",
        "/2015-03-31/functions/al-fn/aliases?FunctionVersion=1",
        &[],
        b"",
    )
    .await;
    assert_eq!(list_v1.json()["Aliases"].as_array().unwrap().len(), 1);
    let list_v2 = call(
        &e.server,
        "GET",
        "/2015-03-31/functions/al-fn/aliases?FunctionVersion=2",
        &[],
        b"",
    )
    .await;
    assert_eq!(list_v2.json()["Aliases"].as_array().unwrap().len(), 0);

    // UpdateAlias precondition check and weighted routing (25% -> v2, 75% -> v1).
    let stale = call(
        &e.server,
        "PUT",
        "/2015-03-31/functions/al-fn/aliases/prod",
        &[],
        br#"{"RevisionId": "stale", "Description": "nope"}"#,
    )
    .await;
    assert_eq!(stale.status, 412);

    let weighted = call(
        &e.server,
        "PUT",
        "/2015-03-31/functions/al-fn/aliases/prod",
        &[],
        json!({
            "RevisionId": rev,
            "RoutingConfig": { "AdditionalVersionWeights": { "2": 0.25 } },
        })
        .to_string()
        .as_bytes(),
    )
    .await;
    assert_eq!(weighted.status, 200);
    assert_eq!(
        weighted.json()["RoutingConfig"]["AdditionalVersionWeights"]["2"],
        0.25
    );

    // 8 invocations produce deterministic 6x v1 and 2x v2, all reporting alias ARN.
    let mut executed = Vec::new();
    for _ in 0..8 {
        let r = call(
            &e.server,
            "POST",
            "/2015-03-31/functions/al-fn/invocations?Qualifier=prod",
            &[],
            b"{}",
        )
        .await;
        assert_eq!(r.status, 200);
        assert_eq!(
            r.json()["arn"],
            "arn:aws:lambda:us-east-1:000000000000:function:al-fn:prod"
        );
        let ver = r.header("X-Amz-Executed-Version").unwrap().to_string();
        assert_eq!(r.json()["version"], ver);
        executed.push(ver);
    }
    let v1_count = executed.iter().filter(|v| v.as_str() == "1").count();
    let v2_count = executed.iter().filter(|v| v.as_str() == "2").count();
    assert_eq!((v1_count, v2_count), (6, 2), "{executed:?}");

    // Function URL on alias works; numeric version is refused.
    assert_eq!(
        call(
            &e.server,
            "POST",
            "/2021-10-31/functions/al-fn/url?Qualifier=1",
            &[],
            br#"{"AuthType": "NONE"}"#,
        )
        .await
        .status,
        400
    );
    let alias_url = call(
        &e.server,
        "POST",
        "/2021-10-31/functions/al-fn/url?Qualifier=prod",
        &[],
        br#"{"AuthType": "NONE"}"#,
    )
    .await;
    assert_eq!(alias_url.status, 201);
    assert_eq!(
        alias_url.json()["FunctionArn"],
        "arn:aws:lambda:us-east-1:000000000000:function:al-fn:prod"
    );
    let url_str = alias_url.json()["FunctionUrl"]
        .as_str()
        .unwrap()
        .to_string();
    let host = url_str
        .strip_prefix("http://")
        .unwrap()
        .trim_end_matches('/');
    let hit = call(&e.server, "GET", "/hello", &[("Host", host)], b"").await;
    assert_eq!(hit.status, 200);
    assert_eq!(
        hit.json()["arn"],
        "arn:aws:lambda:us-east-1:000000000000:function:al-fn:prod"
    );

    // DeleteAlias allows deleting version 1 afterwards.
    assert_eq!(
        call(
            &e.server,
            "DELETE",
            "/2015-03-31/functions/al-fn/aliases/prod",
            &[],
            b"",
        )
        .await
        .status,
        204
    );
    assert_eq!(
        call(
            &e.server,
            "DELETE",
            "/2015-03-31/functions/al-fn?Qualifier=1",
            &[],
            b"",
        )
        .await
        .status,
        204
    );
    e.server.shutdown().await;
}
