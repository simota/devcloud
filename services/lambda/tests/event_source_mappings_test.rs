//! Event source mappings control plane: CreateEventSourceMapping,
//! GetEventSourceMapping, ListEventSourceMappings (with EventSourceArn,
//! FunctionName, and pagination), UpdateEventSourceMapping,
//! DeleteEventSourceMapping, validation, and persistence across restarts.

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
        "devcloud-lambda-esm-{tag}-{}-{nanos}",
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

async fn call(server: &Arc<Server>, method: &str, target: &str, body: &[u8]) -> Resp {
    let (raw_path, query) = match target.split_once('?') {
        Some((p, q)) => (p.to_string(), q.to_string()),
        None => (target.to_string(), String::new()),
    };
    let req = Request {
        method: method.into(),
        raw_path,
        query,
        headers: HashMap::new(),
        body: body.to_vec(),
    };
    let r = process(server, &req).await;
    Resp {
        status: r.status,
        headers: r.headers,
        body: r.body,
    }
}

async fn create_fn(server: &Arc<Server>, name: &str) {
    let zip = build_stored(&[("app.py", b"def handler(e, c):\n    return e\n")]);
    let r = call(
        server,
        "POST",
        "/2015-03-31/functions",
        json!({
            "FunctionName": name,
            "Runtime": "python3.12",
            "Role": "r",
            "Handler": "app.handler",
            "Publish": true,
            "Code": { "ZipFile": b64(&zip) },
        })
        .to_string()
        .as_bytes(),
    )
    .await;
    assert_eq!(r.status, 201);
}

#[tokio::test]
async fn event_source_mapping_crud_filters_and_persistence() {
    let e = env("crud");
    create_fn(&e.server, "worker").await;
    assert_eq!(
        call(
            &e.server,
            "POST",
            "/2015-03-31/functions/worker/aliases",
            br#"{"Name": "prod", "FunctionVersion": "1"}"#,
        )
        .await
        .status,
        201
    );

    let sqs_arn = "arn:aws:sqs:us-east-1:000000000000:jobs";
    let ddb_arn = "arn:aws:dynamodb:us-east-1:000000000000:table/orders/stream/2026";

    // Missing function is rejected.
    let missing_fn = call(
        &e.server,
        "POST",
        "/2015-03-31/event-source-mappings",
        json!({ "FunctionName": "nope", "EventSourceArn": sqs_arn })
            .to_string()
            .as_bytes(),
    )
    .await;
    assert_eq!(missing_fn.status, 404);

    // Invalid filter pattern / scaling config / batch size rejected.
    assert_eq!(
        call(
            &e.server,
            "POST",
            "/2015-03-31/event-source-mappings",
            json!({
                "FunctionName": "worker",
                "EventSourceArn": sqs_arn,
                "FilterCriteria": { "Filters": [{ "Pattern": "not json" }] },
            })
            .to_string()
            .as_bytes(),
        )
        .await
        .status,
        400
    );
    assert_eq!(
        call(
            &e.server,
            "POST",
            "/2015-03-31/event-source-mappings",
            json!({
                "FunctionName": "worker",
                "EventSourceArn": sqs_arn,
                "ScalingConfig": { "MaximumConcurrency": 1 },
            })
            .to_string()
            .as_bytes(),
        )
        .await
        .status,
        400
    );

    // Create SQS mapping (default BatchSize=10).
    let created_sqs = call(
        &e.server,
        "POST",
        "/2015-03-31/event-source-mappings",
        json!({
            "FunctionName": "worker",
            "EventSourceArn": sqs_arn,
            "FunctionResponseTypes": ["ReportBatchItemFailures"],
        })
        .to_string()
        .as_bytes(),
    )
    .await;
    assert_eq!(
        created_sqs.status,
        202,
        "{}",
        String::from_utf8_lossy(&created_sqs.body)
    );
    let m1 = created_sqs.json();
    let uuid1 = m1["UUID"].as_str().unwrap().to_string();
    assert_eq!(m1["BatchSize"], 10);
    assert_eq!(m1["State"], "Enabled");
    assert_eq!(
        m1["FunctionArn"],
        "arn:aws:lambda:us-east-1:000000000000:function:worker"
    );
    assert_eq!(
        m1["EventSourceMappingArn"],
        format!("arn:aws:lambda:us-east-1:000000000000:event-source-mapping:{uuid1}")
    );

    // Duplicate (EventSourceArn, FunctionArn) returns 409.
    let dup = call(
        &e.server,
        "POST",
        "/2015-03-31/event-source-mappings",
        json!({ "FunctionName": "worker", "EventSourceArn": sqs_arn })
            .to_string()
            .as_bytes(),
    )
    .await;
    assert_eq!(dup.status, 409);
    assert_eq!(
        dup.header("X-Amzn-ErrorType"),
        Some("ResourceConflictException")
    );

    // Create DynamoDB Streams mapping targeting alias `worker:prod` (default BatchSize=100).
    let created_ddb = call(
        &e.server,
        "POST",
        "/2015-03-31/event-source-mappings",
        json!({
            "FunctionName": "worker:prod",
            "EventSourceArn": ddb_arn,
            "StartingPosition": "LATEST",
            "ParallelizationFactor": 2,
            "BisectBatchOnFunctionError": true,
        })
        .to_string()
        .as_bytes(),
    )
    .await;
    assert_eq!(created_ddb.status, 202);
    let m2 = created_ddb.json();
    let uuid2 = m2["UUID"].as_str().unwrap().to_string();
    assert_eq!(m2["BatchSize"], 100);
    assert_eq!(
        m2["FunctionArn"],
        "arn:aws:lambda:us-east-1:000000000000:function:worker:prod"
    );

    // GetEventSourceMapping.
    let got = call(
        &e.server,
        "GET",
        &format!("/2015-03-31/event-source-mappings/{uuid1}"),
        b"",
    )
    .await;
    assert_eq!(got.status, 200);
    assert_eq!(got.json()["UUID"], uuid1);

    // ListEventSourceMappings with filters and pagination.
    let list_page1 = call(
        &e.server,
        "GET",
        "/2015-03-31/event-source-mappings?MaxItems=1",
        b"",
    )
    .await;
    assert_eq!(
        list_page1.json()["EventSourceMappings"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    let marker = list_page1.json()["NextMarker"]
        .as_str()
        .unwrap()
        .to_string();
    let list_page2 = call(
        &e.server,
        "GET",
        &format!("/2015-03-31/event-source-mappings?MaxItems=1&Marker={marker}"),
        b"",
    )
    .await;
    assert_eq!(
        list_page2.json()["EventSourceMappings"]
            .as_array()
            .unwrap()
            .len(),
        1
    );

    let by_source = call(
        &e.server,
        "GET",
        &format!("/2015-03-31/event-source-mappings?EventSourceArn={sqs_arn}"),
        b"",
    )
    .await;
    assert_eq!(
        by_source.json()["EventSourceMappings"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    assert_eq!(by_source.json()["EventSourceMappings"][0]["UUID"], uuid1);

    let by_alias = call(
        &e.server,
        "GET",
        "/2015-03-31/event-source-mappings?FunctionName=worker%3Aprod",
        b"",
    )
    .await;
    assert_eq!(
        by_alias.json()["EventSourceMappings"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    assert_eq!(by_alias.json()["EventSourceMappings"][0]["UUID"], uuid2);

    // UpdateEventSourceMapping.
    let updated = call(
        &e.server,
        "PUT",
        &format!("/2015-03-31/event-source-mappings/{uuid1}"),
        json!({
            "Enabled": false,
            "BatchSize": 5,
            "FilterCriteria": { "Filters": [{ "Pattern": "{\"body\":{\"kind\":[\"order\"]}}" }] },
            "ScalingConfig": { "MaximumConcurrency": 4 },
        })
        .to_string()
        .as_bytes(),
    )
    .await;
    assert_eq!(updated.status, 202);
    assert_eq!(updated.json()["State"], "Disabled");
    assert_eq!(updated.json()["BatchSize"], 5);
    assert_eq!(updated.json()["ScalingConfig"]["MaximumConcurrency"], 4);

    // Survives restart.
    let reloaded = Arc::new(Server::new(config_for(&e.dir)));
    assert!(reloaded.load_err().is_none());
    let reloaded_m1 = call(
        &reloaded,
        "GET",
        &format!("/2015-03-31/event-source-mappings/{uuid1}"),
        b"",
    )
    .await;
    assert_eq!(reloaded_m1.status, 200);
    assert_eq!(reloaded_m1.json()["State"], "Disabled");
    assert_eq!(reloaded_m1.json()["BatchSize"], 5);

    // DeleteEventSourceMapping returns 202 with State=Deleting, then 404 on Get.
    let deleted = call(
        &reloaded,
        "DELETE",
        &format!("/2015-03-31/event-source-mappings/{uuid1}"),
        b"",
    )
    .await;
    assert_eq!(deleted.status, 202);
    assert_eq!(deleted.json()["State"], "Deleting");
    assert_eq!(
        call(
            &reloaded,
            "GET",
            &format!("/2015-03-31/event-source-mappings/{uuid1}"),
            b"",
        )
        .await
        .status,
        404
    );
}
