//! Event source mapping polling against the local SQS and DynamoDB services:
//! batching, FilterCriteria, ReportBatchItemFailures, function errors,
//! retries, disabled mappings, SigV4 against a strict SQS, and the
//! background polling loop.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use base64::Engine;
use devcloud_lambda::aws_client::{call as aws, AwsEndpoint};
use devcloud_lambda::event_poller::EventSources;
use devcloud_lambda::http::{process, Request};
use devcloud_lambda::zip::build_stored;
use devcloud_lambda::{Config, Server};
use serde_json::{json, Value};

const ACCOUNT: &str = "000000000000";
const REGION: &str = "us-east-1";

/// Records every event, fails the whole batch on an SQS body `explode`, and
/// reports SQS bodies `bad` and DynamoDB items with `fail: true` as batch
/// item failures.
const HANDLER: &[u8] = br#"import json, os
def handler(event, context):
    with open(os.environ['OUT'], 'a') as f:
        f.write(json.dumps(event) + '\n')
    recs = event['Records']
    if any(r.get('body') == 'explode' for r in recs):
        raise Exception('boom')
    fails = []
    for r in recs:
        if r.get('body') == 'bad':
            fails.append({'itemIdentifier': r['messageId']})
        if r.get('eventSource') == 'aws:dynamodb':
            if r['dynamodb'].get('NewImage', {}).get('fail', {}).get('BOOL'):
                fails.append({'itemIdentifier': r['dynamodb']['SequenceNumber']})
    return {'batchItemFailures': fails}
"#;

fn has_python() -> bool {
    std::process::Command::new("python3")
        .arg("--version")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

fn temp_dir(tag: &str) -> PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let dir = std::env::temp_dir().join(format!(
        "devcloud-lambda-poll-{tag}-{}-{nanos}",
        std::process::id()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// A shutdown signal for an in-test service.
struct Stop(
    Option<tokio::sync::oneshot::Sender<()>>,
    tokio::task::JoinHandle<()>,
);

impl Stop {
    async fn stop(mut self) {
        if let Some(tx) = self.0.take() {
            let _ = tx.send(());
        }
        let _ = (&mut self.1).await;
    }
}

async fn start_sqs(dir: &Path, auth_mode: &str) -> (String, Stop) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let server = devcloud_sqs::Server::new(devcloud_sqs::Config {
        addr: addr.clone(),
        region: REGION.into(),
        account_id: ACCOUNT.into(),
        auth_mode: auth_mode.into(),
        access_key_id: "sqs-key".into(),
        secret_access_key: "sqs-secret".into(),
        storage_path: dir.join("sqs").to_string_lossy().into_owned(),
        max_queues: 100,
        max_message_bytes: 262_144,
        max_receive_batch_size: 10,
        default_visibility_timeout_seconds: 30,
        default_message_retention_seconds: 345_600,
        ..Default::default()
    });
    let (tx, rx) = tokio::sync::oneshot::channel::<()>();
    let task = tokio::spawn(async move {
        let _ = devcloud_sqs::http::serve(listener, Arc::new(Mutex::new(server)), async {
            let _ = rx.await;
        })
        .await;
    });
    (format!("http://{addr}"), Stop(Some(tx), task))
}

async fn start_dynamodb(dir: &Path, auth_mode: &str) -> (String, Stop) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let server = devcloud_dynamodb::server::Server::new(devcloud_dynamodb::server::Config {
        addr: addr.clone(),
        region: REGION.into(),
        auth_mode: auth_mode.into(),
        access_key_id: "ddb-key".into(),
        secret_access_key: "ddb-secret".into(),
        storage_path: dir.join("dynamodb").to_string_lossy().into_owned(),
        max_item_bytes: 400_000,
        max_tables: 16,
    });
    let (tx, rx) = tokio::sync::oneshot::channel::<()>();
    let auth_mode = auth_mode.to_string();
    let task = tokio::spawn(async move {
        let _ = devcloud_dynamodb::http::serve(
            listener,
            Arc::new(Mutex::new(server)),
            auth_mode,
            async {
                let _ = rx.await;
            },
        )
        .await;
    });
    (format!("http://{addr}"), Stop(Some(tx), task))
}

fn endpoint(url: &str, key: &str, secret: &str) -> AwsEndpoint {
    AwsEndpoint {
        url: url.to_string(),
        region: REGION.into(),
        access_key_id: key.into(),
        secret_access_key: secret.into(),
    }
}

fn lambda(dir: &Path, sources: EventSources) -> Arc<Server> {
    Arc::new(Server::new(Config {
        addr: "127.0.0.1:19010".into(),
        endpoint: "http://127.0.0.1:19010".into(),
        region: REGION.into(),
        account_id: ACCOUNT.into(),
        auth_mode: "relaxed".into(),
        storage_path: dir.join("lambda").to_string_lossy().into_owned(),
        event_sources: sources,
        ..Config::default()
    }))
}

async fn api(server: &Arc<Server>, method: &str, target: &str, body: &Value) -> (u16, Value) {
    let (raw_path, query) = match target.split_once('?') {
        Some((p, q)) => (p.to_string(), q.to_string()),
        None => (target.to_string(), String::new()),
    };
    let req = Request {
        method: method.into(),
        raw_path,
        query,
        headers: HashMap::new(),
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

async fn create_worker(server: &Arc<Server>, out: &Path) {
    let zip = build_stored(&[("app.py", HANDLER)]);
    let (status, body) = api(
        server,
        "POST",
        "/2015-03-31/functions",
        &json!({
            "FunctionName": "worker",
            "Runtime": "python3.12",
            "Role": "r",
            "Handler": "app.handler",
            "Code": { "ZipFile": base64::engine::general_purpose::STANDARD.encode(zip) },
            "Environment": { "Variables": { "OUT": out.to_string_lossy() } },
        }),
    )
    .await;
    assert_eq!(status, 201, "{body}");
}

async fn create_mapping(server: &Arc<Server>, req: Value) -> String {
    let (status, body) = api(server, "POST", "/2015-03-31/event-source-mappings", &req).await;
    assert_eq!(status, 202, "{body}");
    body["UUID"].as_str().unwrap().to_string()
}

/// Events the handler recorded so far.
fn events(out: &Path) -> Vec<Value> {
    std::fs::read_to_string(out)
        .unwrap_or_default()
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect()
}

fn bodies(event: &Value) -> Vec<String> {
    event["Records"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["body"].as_str().unwrap().to_string())
        .collect()
}

async fn create_queue(sqs: &AwsEndpoint, name: &str, visibility: &str) -> String {
    let r = aws(
        sqs,
        "sqs",
        "AmazonSQS.CreateQueue",
        &json!({ "QueueName": name, "Attributes": { "VisibilityTimeout": visibility } }),
    )
    .await
    .unwrap();
    r["QueueUrl"].as_str().unwrap().to_string()
}

async fn send(sqs: &AwsEndpoint, queue_url: &str, body: &str) {
    aws(
        sqs,
        "sqs",
        "AmazonSQS.SendMessage",
        &json!({
            "QueueUrl": queue_url,
            "MessageBody": body,
            "MessageAttributes": { "origin": { "DataType": "String", "StringValue": "test" } },
        }),
    )
    .await
    .unwrap();
}

/// Bodies of the messages visible right now, without hiding them (a
/// receive with `VisibilityTimeout: 0`; it does count as a receive).
async fn visible(sqs: &AwsEndpoint, queue_url: &str) -> Vec<String> {
    let r = aws(
        sqs,
        "sqs",
        "AmazonSQS.ReceiveMessage",
        &json!({ "QueueUrl": queue_url, "MaxNumberOfMessages": 10, "VisibilityTimeout": 0 }),
    )
    .await
    .unwrap();
    let mut out: Vec<String> = r["Messages"]
        .as_array()
        .map(|a| {
            a.iter()
                .map(|m| m["Body"].as_str().unwrap().to_string())
                .collect()
        })
        .unwrap_or_default();
    out.sort();
    out
}

#[tokio::test]
async fn sqs_mapping_filters_batches_and_deletes_all_but_reported_failures() {
    if !has_python() {
        eprintln!("skipping: python3 not available");
        return;
    }
    let dir = temp_dir("sqs");
    let (sqs_url, sqs_stop) = start_sqs(&dir, "relaxed").await;
    let sqs = endpoint(&sqs_url, "sqs-key", "sqs-secret");
    let server = lambda(
        &dir,
        EventSources {
            sqs: Some(sqs.clone()),
            ..Default::default()
        },
    );
    let out = dir.join("events.jsonl");
    create_worker(&server, &out).await;
    let queue_url = create_queue(&sqs, "jobs", "1").await;
    let queue_arn = format!("arn:aws:sqs:{REGION}:{ACCOUNT}:jobs");
    let uuid = create_mapping(
        &server,
        json!({
            "FunctionName": "worker",
            "EventSourceArn": queue_arn,
            "BatchSize": 5,
            "FunctionResponseTypes": ["ReportBatchItemFailures"],
            "FilterCriteria": { "Filters": [
                { "Pattern": "{\"body\":[{\"anything-but\":[\"skip\"]}]}" }
            ]},
        }),
    )
    .await;

    // Nothing queued: no invocation.
    assert_eq!(server.poll_event_sources_once().await, 0);

    for body in ["a", "bad", "skip"] {
        send(&sqs, &queue_url, body).await;
    }
    assert_eq!(server.poll_event_sources_once().await, 1);
    let seen = events(&out);
    assert_eq!(seen.len(), 1);
    let mut got = bodies(&seen[0]);
    got.sort();
    assert_eq!(got, vec!["a", "bad"]);
    let rec = &seen[0]["Records"][0];
    assert_eq!(rec["eventSource"], "aws:sqs");
    assert_eq!(rec["eventSourceARN"], queue_arn);
    assert_eq!(rec["awsRegion"], REGION);
    assert_eq!(rec["attributes"]["ApproximateReceiveCount"], "1");
    assert_eq!(rec["messageAttributes"]["origin"]["stringValue"], "test");
    assert!(rec["receiptHandle"].as_str().is_some_and(|h| !h.is_empty()));

    let (_, mapping) = api(
        &server,
        "GET",
        &format!("/2015-03-31/event-source-mappings/{uuid}"),
        &Value::Null,
    )
    .await;
    assert_eq!(mapping["LastProcessingResult"], "OK");

    // `a` and the filtered-out `skip` were deleted; the reported failure
    // reappears after the visibility timeout and is delivered again.
    tokio::time::sleep(Duration::from_millis(1300)).await;
    assert_eq!(visible(&sqs, &queue_url).await, vec!["bad"]);
    assert_eq!(server.poll_event_sources_once().await, 1);
    let seen = events(&out);
    assert_eq!(seen.len(), 2);
    assert_eq!(bodies(&seen[1]), vec!["bad"]);
    assert_ne!(
        seen[1]["Records"][0]["attributes"]["ApproximateReceiveCount"],
        "1"
    );

    // A disabled mapping is not polled.
    let (status, _) = api(
        &server,
        "PUT",
        &format!("/2015-03-31/event-source-mappings/{uuid}"),
        &json!({ "Enabled": false }),
    )
    .await;
    assert_eq!(status, 202);
    tokio::time::sleep(Duration::from_millis(1300)).await;
    assert_eq!(server.poll_event_sources_once().await, 0);
    assert_eq!(events(&out).len(), 2);

    server.shutdown().await;
    sqs_stop.stop().await;
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn sqs_function_errors_keep_the_batch_for_redelivery() {
    if !has_python() {
        eprintln!("skipping: python3 not available");
        return;
    }
    let dir = temp_dir("sqs-err");
    let (sqs_url, sqs_stop) = start_sqs(&dir, "relaxed").await;
    let sqs = endpoint(&sqs_url, "sqs-key", "sqs-secret");
    let server = lambda(
        &dir,
        EventSources {
            sqs: Some(sqs.clone()),
            ..Default::default()
        },
    );
    let out = dir.join("events.jsonl");
    create_worker(&server, &out).await;
    let queue_url = create_queue(&sqs, "errs", "1").await;
    let uuid = create_mapping(
        &server,
        json!({
            "FunctionName": "worker",
            "EventSourceArn": format!("arn:aws:sqs:{REGION}:{ACCOUNT}:errs"),
        }),
    )
    .await;
    send(&sqs, &queue_url, "explode").await;
    send(&sqs, &queue_url, "ok").await;
    assert_eq!(server.poll_event_sources_once().await, 1);
    let (_, mapping) = api(
        &server,
        "GET",
        &format!("/2015-03-31/event-source-mappings/{uuid}"),
        &Value::Null,
    )
    .await;
    assert_eq!(
        mapping["LastProcessingResult"],
        "PROBLEM: Function call failed"
    );

    // Nothing was deleted: the whole batch is delivered again.
    tokio::time::sleep(Duration::from_millis(1300)).await;
    assert_eq!(server.poll_event_sources_once().await, 1);
    let seen = events(&out);
    assert_eq!(seen.len(), 2);
    let mut again = bodies(&seen[1]);
    again.sort();
    assert_eq!(again, vec!["explode", "ok"]);

    server.shutdown().await;
    sqs_stop.stop().await;
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn sqs_polling_signs_requests_for_a_strict_queue_service() {
    if !has_python() {
        eprintln!("skipping: python3 not available");
        return;
    }
    let dir = temp_dir("sqs-strict");
    let (sqs_url, sqs_stop) = start_sqs(&dir, "strict").await;
    let sqs = endpoint(&sqs_url, "sqs-key", "sqs-secret");
    let queue_url = create_queue(&sqs, "signed", "1").await;
    send(&sqs, &queue_url, "hello").await;
    let queue_arn = format!("arn:aws:sqs:{REGION}:{ACCOUNT}:signed");

    // Wrong credentials: the queue service refuses the poller.
    let wrong_dir = dir.join("wrong");
    let wrong = lambda(
        &wrong_dir,
        EventSources {
            sqs: Some(endpoint(&sqs_url, "sqs-key", "not-the-secret")),
            ..Default::default()
        },
    );
    let out = dir.join("events.jsonl");
    create_worker(&wrong, &out).await;
    let uuid = create_mapping(
        &wrong,
        json!({ "FunctionName": "worker", "EventSourceArn": queue_arn }),
    )
    .await;
    assert_eq!(wrong.poll_event_sources_once().await, 0);
    let (_, mapping) = api(
        &wrong,
        "GET",
        &format!("/2015-03-31/event-source-mappings/{uuid}"),
        &Value::Null,
    )
    .await;
    assert!(
        mapping["LastProcessingResult"]
            .as_str()
            .is_some_and(|r| r.starts_with("PROBLEM: AmazonSQS.GetQueueUrl")),
        "{mapping}"
    );
    wrong.shutdown().await;

    // The configured credentials sign valid requests.
    let server = lambda(
        &dir,
        EventSources {
            sqs: Some(sqs.clone()),
            ..Default::default()
        },
    );
    create_worker(&server, &out).await;
    create_mapping(
        &server,
        json!({ "FunctionName": "worker", "EventSourceArn": queue_arn }),
    )
    .await;
    assert_eq!(server.poll_event_sources_once().await, 1);
    assert_eq!(bodies(&events(&out)[0]), vec!["hello"]);
    // Deleted after the successful invocation (DeleteMessageBatch signed too).
    tokio::time::sleep(Duration::from_millis(1300)).await;
    assert!(visible(&sqs, &queue_url).await.is_empty());

    server.shutdown().await;
    sqs_stop.stop().await;
    let _ = std::fs::remove_dir_all(&dir);
}

async fn put_item(ddb: &AwsEndpoint, pk: &str, fail: bool) {
    aws(
        ddb,
        "dynamodb",
        "DynamoDB_20120810.PutItem",
        &json!({
            "TableName": "orders",
            "Item": { "pk": { "S": pk }, "fail": { "BOOL": fail } },
        }),
    )
    .await
    .unwrap();
}

fn stream_keys(event: &Value) -> Vec<String> {
    event["Records"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| {
            r["dynamodb"]["Keys"]["pk"]["S"]
                .as_str()
                .unwrap()
                .to_string()
        })
        .collect()
}

#[tokio::test]
async fn dynamodb_stream_mapping_checkpoints_retries_and_skips() {
    if !has_python() {
        eprintln!("skipping: python3 not available");
        return;
    }
    let dir = temp_dir("ddb");
    // Strict: every stream request the poller makes must be signed.
    let (ddb_url, ddb_stop) = start_dynamodb(&dir, "strict").await;
    let ddb = endpoint(&ddb_url, "ddb-key", "ddb-secret");
    let table = aws(
        &ddb,
        "dynamodb",
        "DynamoDB_20120810.CreateTable",
        &json!({
            "TableName": "orders",
            "AttributeDefinitions": [{ "AttributeName": "pk", "AttributeType": "S" }],
            "KeySchema": [{ "AttributeName": "pk", "KeyType": "HASH" }],
            "BillingMode": "PAY_PER_REQUEST",
            "StreamSpecification": { "StreamEnabled": true, "StreamViewType": "NEW_AND_OLD_IMAGES" },
        }),
    )
    .await
    .unwrap();
    let stream_arn = table["TableDescription"]["LatestStreamArn"]
        .as_str()
        .unwrap()
        .to_string();

    let server = lambda(
        &dir,
        EventSources {
            dynamodb: Some(ddb.clone()),
            ..Default::default()
        },
    );
    let out = dir.join("events.jsonl");
    create_worker(&server, &out).await;
    let uuid = create_mapping(
        &server,
        json!({
            "FunctionName": "worker",
            "EventSourceArn": stream_arn,
            "StartingPosition": "TRIM_HORIZON",
            "BatchSize": 10,
            "MaximumRetryAttempts": 1,
            "FunctionResponseTypes": ["ReportBatchItemFailures"],
            // Only inserts and modifications reach the function.
            "FilterCriteria": { "Filters": [
                { "Pattern": "{\"eventName\":[\"INSERT\",\"MODIFY\"]}" }
            ]},
        }),
    )
    .await;

    put_item(&ddb, "a", false).await;
    put_item(&ddb, "b", true).await;
    put_item(&ddb, "c", false).await;
    assert_eq!(server.poll_event_sources_once().await, 1);
    let seen = events(&out);
    assert_eq!(stream_keys(&seen[0]), vec!["a", "b", "c"]);
    let rec = &seen[0]["Records"][0];
    assert_eq!(rec["eventSource"], "aws:dynamodb");
    assert_eq!(rec["eventName"], "INSERT");
    assert_eq!(rec["eventSourceARN"], stream_arn);

    // `b` failed: the batch resumes from it (retry 1 of 1)...
    assert_eq!(server.poll_event_sources_once().await, 1);
    assert_eq!(stream_keys(&events(&out)[1]), vec!["b", "c"]);
    let (_, mapping) = api(
        &server,
        "GET",
        &format!("/2015-03-31/event-source-mappings/{uuid}"),
        &Value::Null,
    )
    .await;
    assert_eq!(
        mapping["LastProcessingResult"],
        "PROBLEM: Function call failed"
    );

    // ...and is skipped once the retries are used up.
    assert_eq!(server.poll_event_sources_once().await, 0);
    assert_eq!(events(&out).len(), 2);

    // Deletes are filtered out; new writes continue from the checkpoint.
    aws(
        &ddb,
        "dynamodb",
        "DynamoDB_20120810.DeleteItem",
        &json!({ "TableName": "orders", "Key": { "pk": { "S": "a" } } }),
    )
    .await
    .unwrap();
    put_item(&ddb, "d", false).await;
    assert_eq!(server.poll_event_sources_once().await, 1);
    let seen = events(&out);
    assert_eq!(seen.len(), 3);
    assert_eq!(stream_keys(&seen[2]), vec!["d"]);

    server.shutdown().await;
    ddb_stop.stop().await;
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn background_loop_polls_until_shutdown() {
    if !has_python() {
        eprintln!("skipping: python3 not available");
        return;
    }
    let dir = temp_dir("loop");
    let (sqs_url, sqs_stop) = start_sqs(&dir, "relaxed").await;
    let sqs = endpoint(&sqs_url, "sqs-key", "sqs-secret");
    let server = lambda(
        &dir,
        EventSources {
            sqs: Some(sqs.clone()),
            poll_interval: Some(Duration::from_millis(100)),
            ..Default::default()
        },
    );
    let out = dir.join("events.jsonl");
    create_worker(&server, &out).await;
    let queue_url = create_queue(&sqs, "loop", "30").await;
    create_mapping(
        &server,
        json!({
            "FunctionName": "worker",
            "EventSourceArn": format!("arn:aws:sqs:{REGION}:{ACCOUNT}:loop"),
        }),
    )
    .await;
    server.start_event_source_polling();
    send(&sqs, &queue_url, "tick").await;
    let mut delivered = false;
    for _ in 0..100 {
        if !events(&out).is_empty() {
            delivered = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(
        delivered,
        "the background poller never invoked the function"
    );
    assert_eq!(bodies(&events(&out)[0]), vec!["tick"]);

    server.shutdown().await;
    // Stopped: later messages stay queued.
    send(&sqs, &queue_url, "after").await;
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert_eq!(events(&out).len(), 1);
    assert_eq!(visible(&sqs, &queue_url).await, vec!["after"]);

    sqs_stop.stop().await;
    let _ = std::fs::remove_dir_all(&dir);
}
