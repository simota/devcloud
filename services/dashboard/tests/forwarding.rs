//! Integration tests: drive the dashboard router against a mock upstream service
//! and assert each `/api/<svc>/*` route forwards to the correct path/protocol and
//! reshapes the response into the byte-identical legacy envelope.
//!
//! The mock upstream is a one-shot HTTP/1.1 server that records the request line
//! + headers + body it received and replies with a canned response. It mirrors
//! how the real `/_introspect/`, `/_control/`, and provider-protocol endpoints
//! behave from the dashboard's point of view.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use devcloud_dashboard::config::Config;
use devcloud_dashboard::http::{route, Request};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

#[derive(Clone, Default)]
struct Recorded {
    method: String,
    target: String,
    headers: HashMap<String, String>,
    body: Vec<u8>,
}

/// Spawns a one-shot mock upstream that records the first request and replies
/// with `status` + `body` (JSON). Returns the base URL and a handle to the record.
async fn mock_upstream(status: u16, body: &'static str) -> (String, Arc<Mutex<Recorded>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let base = format!("http://{addr}");
    let record = Arc::new(Mutex::new(Recorded::default()));
    let record_clone = Arc::clone(&record);

    tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut buf = Vec::new();
        let mut tmp = [0u8; 4096];
        // Read until headers complete.
        let header_end = loop {
            if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                break pos;
            }
            let n = stream.read(&mut tmp).await.unwrap();
            if n == 0 {
                break buf.len();
            }
            buf.extend_from_slice(&tmp[..n]);
        };
        let head = String::from_utf8_lossy(&buf[..header_end]).into_owned();
        let mut lines = head.split("\r\n");
        let request_line = lines.next().unwrap_or("");
        let mut rl = request_line.split(' ');
        let method = rl.next().unwrap_or("").to_string();
        let target = rl.next().unwrap_or("").to_string();
        let mut headers = HashMap::new();
        for line in lines {
            if let Some((k, v)) = line.split_once(':') {
                headers.insert(k.trim().to_ascii_lowercase(), v.trim().to_string());
            }
        }
        let content_length: usize = headers
            .get("content-length")
            .and_then(|v| v.parse().ok())
            .unwrap_or(0);
        let mut req_body = buf[(header_end + 4).min(buf.len())..].to_vec();
        while req_body.len() < content_length {
            let n = stream.read(&mut tmp).await.unwrap();
            if n == 0 {
                break;
            }
            req_body.extend_from_slice(&tmp[..n]);
        }
        req_body.truncate(content_length);
        *record_clone.lock().unwrap() = Recorded {
            method,
            target,
            headers,
            body: req_body,
        };

        let head = format!(
            "HTTP/1.1 {status} OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            body.len()
        );
        stream.write_all(head.as_bytes()).await.unwrap();
        stream.write_all(body.as_bytes()).await.unwrap();
        stream.flush().await.unwrap();
    });

    (base, record)
}

fn req(method: &str, path: &str, body: &[u8]) -> Request {
    Request {
        method: method.to_string(),
        path: path.to_string(),
        raw_path: path.to_string(),
        query: String::new(),
        headers: HashMap::new(),
        body: body.to_vec(),
    }
}

fn req_q(method: &str, path: &str, query: &str, body: &[u8]) -> Request {
    let mut r = req(method, path, body);
    r.query = query.to_string();
    r
}

// ── Redis: a `/_control/` mutation service ──────────────────────────────────

#[tokio::test]
async fn redis_command_forwards_to_control_exec() {
    let (base, record) =
        mock_upstream(200, r#"{"command":"GET","class":"read","rows":["v"]}"#).await;
    let mut cfg = Config::default();
    cfg.redis_base = base;

    let body = br#"{"command":"GET","args":["k"]}"#;
    let resp = route(&cfg, &req("POST", "/api/redis/command", body)).await;
    assert_eq!(resp.status, 200);

    let rec = record.lock().unwrap().clone();
    assert_eq!(rec.method, "POST");
    assert_eq!(rec.target, "/_control/exec");
    assert_eq!(rec.body, body);
    // Envelope relayed verbatim.
    let v: serde_json::Value = serde_json::from_slice(&resp.body).unwrap();
    assert_eq!(v["command"], "GET");
    assert_eq!(v["rows"][0], "v");
}

#[tokio::test]
async fn redis_keys_get_forwards_to_introspect_keys() {
    let (base, record) = mock_upstream(200, r#"{"cursor":0,"nextCursor":0,"keys":[]}"#).await;
    let mut cfg = Config::default();
    cfg.redis_base = base;

    let resp = route(
        &cfg,
        &req_q("GET", "/api/redis/keys", "match=a*&count=10", b""),
    )
    .await;
    assert_eq!(resp.status, 200);
    let rec = record.lock().unwrap().clone();
    assert_eq!(rec.method, "GET");
    assert_eq!(rec.target, "/_introspect/keys?match=a*&count=10");
}

// ── Redshift: query -> /_control/query ──────────────────────────────────────

#[tokio::test]
async fn redshift_query_forwards_to_control_query() {
    let (base, record) = mock_upstream(200, r#"{"result":{"statement":{"id":"s1"}}}"#).await;
    let mut cfg = Config::default();
    cfg.redshift_base = base;

    let body = br#"{"sql":"SELECT 1","maxRows":10}"#;
    let resp = route(&cfg, &req("POST", "/api/redshift/query", body)).await;
    assert_eq!(resp.status, 200);
    let rec = record.lock().unwrap().clone();
    assert_eq!(rec.method, "POST");
    assert_eq!(rec.target, "/_control/query");
    assert_eq!(rec.body, body);
    let v: serde_json::Value = serde_json::from_slice(&resp.body).unwrap();
    assert_eq!(v["result"]["statement"]["id"], "s1");
}

#[tokio::test]
async fn redshift_clusters_rewraps_snapshot() {
    let (base, record) = mock_upstream(
        200,
        r#"{"status":"running","running":true,"clusters":[{"clusterIdentifier":"c1"}]}"#,
    )
    .await;
    let mut cfg = Config::default();
    cfg.redshift_base = base;

    let resp = route(&cfg, &req("GET", "/api/redshift/clusters", b"")).await;
    assert_eq!(resp.status, 200);
    let rec = record.lock().unwrap().clone();
    assert_eq!(rec.target, "/_introspect/clusters");
    let v: serde_json::Value = serde_json::from_slice(&resp.body).unwrap();
    // Re-wrapped into {clusters: [...]}.
    assert_eq!(v["clusters"][0]["clusterIdentifier"], "c1");
    assert!(v.get("status").is_none());
}

// ── DynamoDB: provider-protocol mutation ────────────────────────────────────

#[tokio::test]
async fn dynamodb_create_table_forwards_provider_protocol() {
    let (base, record) = mock_upstream(200, r#"{"TableDescription":{"TableName":"t"}}"#).await;
    let mut cfg = Config::default();
    cfg.dynamodb_base = base;

    let body = br#"{"input":{"TableName":"t","KeySchema":[]}}"#;
    let resp = route(&cfg, &req("POST", "/api/dynamodb/tables", body)).await;
    assert_eq!(resp.status, 200);
    let rec = record.lock().unwrap().clone();
    assert_eq!(rec.method, "POST");
    assert_eq!(rec.target, "/");
    assert_eq!(
        rec.headers.get("x-amz-target").map(String::as_str),
        Some("DynamoDB_20120810.CreateTable")
    );
    assert_eq!(
        rec.headers.get("content-type").map(String::as_str),
        Some("application/x-amz-json-1.0")
    );
    // The forwarded body is the normalized `input` object (not the envelope).
    let sent: serde_json::Value = serde_json::from_slice(&rec.body).unwrap();
    assert_eq!(sent["TableName"], "t");
    assert!(sent.get("input").is_none());
}

#[tokio::test]
async fn dynamodb_tables_get_rewraps_snapshot() {
    let (base, record) = mock_upstream(
        200,
        r#"{"status":"running","running":true,"tables":[{"tableName":"t"}]}"#,
    )
    .await;
    let mut cfg = Config::default();
    cfg.dynamodb_base = base;

    let resp = route(&cfg, &req("GET", "/api/dynamodb/tables", b"")).await;
    assert_eq!(resp.status, 200);
    let rec = record.lock().unwrap().clone();
    assert_eq!(rec.target, "/_introspect/tables");
    let v: serde_json::Value = serde_json::from_slice(&resp.body).unwrap();
    assert_eq!(v["tables"][0]["tableName"], "t");
    assert!(v.get("status").is_none());
}

// ── Mail: introspect read + control delete ──────────────────────────────────

#[tokio::test]
async fn mail_messages_get_forwards_to_introspect() {
    let (base, record) = mock_upstream(200, r#"{"messages":[]}"#).await;
    let mut cfg = Config::default();
    cfg.mail_base = base;

    let resp = route(&cfg, &req("GET", "/api/messages", b"")).await;
    assert_eq!(resp.status, 200);
    let rec = record.lock().unwrap().clone();
    assert_eq!(rec.target, "/_introspect/messages?limit=100");
}

#[tokio::test]
async fn mail_message_delete_forwards_to_control() {
    let (base, record) = mock_upstream(204, "").await;
    let mut cfg = Config::default();
    cfg.mail_base = base;

    let resp = route(&cfg, &req("DELETE", "/api/messages/abc", b"")).await;
    assert_eq!(resp.status, 204);
    let rec = record.lock().unwrap().clone();
    assert_eq!(rec.method, "DELETE");
    assert_eq!(rec.target, "/_control/messages/abc");
}

// ── BigQuery: REST provider-protocol forward + read re-wrap ──────────────────

#[tokio::test]
async fn bigquery_query_forwards_rest_path() {
    let (base, record) = mock_upstream(200, r#"{"kind":"bigquery#queryResponse"}"#).await;
    let mut cfg = Config::default();
    cfg.bigquery_base = base;

    let body = br#"{"query":"SELECT 1"}"#;
    let resp = route(
        &cfg,
        &req("POST", "/api/bigquery/projects/devcloud/queries", body),
    )
    .await;
    assert_eq!(resp.status, 200);
    let rec = record.lock().unwrap().clone();
    assert_eq!(rec.method, "POST");
    assert_eq!(rec.target, "/bigquery/v2/projects/devcloud/queries");
    assert_eq!(rec.body, body);
}

// ── Pub/Sub: snapshot read re-wrap ──────────────────────────────────────────

#[tokio::test]
async fn pubsub_topics_get_rewraps_snapshot() {
    let (base, record) = mock_upstream(
        200,
        r#"{"project":"devcloud","status":"running","running":true,"topics":[{"name":"projects/devcloud/topics/t"}]}"#,
    )
    .await;
    let mut cfg = Config::default();
    cfg.pubsub_base = base;

    let resp = route(&cfg, &req("GET", "/api/pubsub/topics", b"")).await;
    assert_eq!(resp.status, 200);
    let rec = record.lock().unwrap().clone();
    assert_eq!(rec.target, "/_introspect/snapshot");
    let v: serde_json::Value = serde_json::from_slice(&resp.body).unwrap();
    assert_eq!(v["project"], "devcloud");
    assert_eq!(v["topics"][0]["name"], "projects/devcloud/topics/t");
}

// ── GCS: introspect reads + provider-protocol/control mutations ─────────────

#[tokio::test]
async fn gcs_uploads_forwards_to_introspect() {
    let (base, record) = mock_upstream(200, r#"{"sessions":[]}"#).await;
    let mut cfg = Config::default();
    cfg.gcs_base = base;

    let resp = route(&cfg, &req("GET", "/api/gcs/uploads", b"")).await;
    assert_eq!(resp.status, 200);
    let rec = record.lock().unwrap().clone();
    assert_eq!(rec.target, "/_introspect/uploads");
}

#[tokio::test]
async fn gcs_buckets_get_forwards_to_introspect() {
    let (base, record) =
        mock_upstream(200, r#"{"buckets":[{"name":"alpha","objectCount":2}]}"#).await;
    let mut cfg = Config::default();
    cfg.gcs_base = base;

    let resp = route(&cfg, &req("GET", "/api/gcs/buckets", b"")).await;
    assert_eq!(resp.status, 200);
    let rec = record.lock().unwrap().clone();
    assert_eq!(rec.method, "GET");
    assert_eq!(rec.target, "/_introspect/buckets");
    let v: serde_json::Value = serde_json::from_slice(&resp.body).unwrap();
    assert_eq!(v["buckets"][0]["name"], "alpha");
    assert_eq!(v["buckets"][0]["objectCount"], 2);
}

#[tokio::test]
async fn gcs_bucket_detail_forwards_to_introspect() {
    let (base, record) = mock_upstream(
        200,
        r#"{"name":"alpha","objectCount":1,"gcsUri":"gs://alpha"}"#,
    )
    .await;
    let mut cfg = Config::default();
    cfg.gcs_base = base;

    let resp = route(&cfg, &req("GET", "/api/gcs/buckets/alpha", b"")).await;
    assert_eq!(resp.status, 200);
    let rec = record.lock().unwrap().clone();
    assert_eq!(rec.target, "/_introspect/buckets/alpha");
    let v: serde_json::Value = serde_json::from_slice(&resp.body).unwrap();
    assert_eq!(v["gcsUri"], "gs://alpha");
}

#[tokio::test]
async fn gcs_objects_forwards_prefix_to_introspect() {
    let (base, record) =
        mock_upstream(200, r#"{"bucket":"alpha","prefix":"logs/","objects":[]}"#).await;
    let mut cfg = Config::default();
    cfg.gcs_base = base;

    let resp = route(
        &cfg,
        &req_q(
            "GET",
            "/api/gcs/buckets/alpha/objects",
            "prefix=logs%2F",
            b"",
        ),
    )
    .await;
    assert_eq!(resp.status, 200);
    let rec = record.lock().unwrap().clone();
    assert_eq!(
        rec.target,
        "/_introspect/buckets/alpha/objects?prefix=logs%2F"
    );
}

#[tokio::test]
async fn gcs_object_detail_forwards_to_introspect() {
    let (base, record) =
        mock_upstream(200, r#"{"name":"a.txt","gcsUri":"gs://alpha/a.txt"}"#).await;
    let mut cfg = Config::default();
    cfg.gcs_base = base;

    let resp = route(
        &cfg,
        &req("GET", "/api/gcs/buckets/alpha/objects/a.txt", b""),
    )
    .await;
    assert_eq!(resp.status, 200);
    let rec = record.lock().unwrap().clone();
    assert_eq!(rec.target, "/_introspect/buckets/alpha/objects/a.txt");
}

#[tokio::test]
async fn gcs_create_bucket_forwards_provider_and_rewraps_201() {
    let (base, record) = mock_upstream(
        200,
        r#"{"kind":"storage#bucket","name":"alpha","timeCreated":"2024-06-01T12:00:00Z"}"#,
    )
    .await;
    let mut cfg = Config::default();
    cfg.gcs_base = base;

    let body = br#"{"name":"alpha"}"#;
    let resp = route(&cfg, &req("POST", "/api/gcs/buckets", body)).await;
    assert_eq!(resp.status, 201);
    let rec = record.lock().unwrap().clone();
    assert_eq!(rec.method, "POST");
    assert_eq!(rec.target, "/storage/v1/b");
    let sent: serde_json::Value = serde_json::from_slice(&rec.body).unwrap();
    assert_eq!(sent["name"], "alpha");
    let v: serde_json::Value = serde_json::from_slice(&resp.body).unwrap();
    assert_eq!(v["name"], "alpha");
    assert_eq!(v["gcsUri"], "gs://alpha");
    assert_eq!(v["objectCount"], 0);
    assert_eq!(v["timeCreated"], "2024-06-01T12:00:00Z");
}

#[tokio::test]
async fn gcs_create_bucket_conflict_maps_409() {
    let (base, _record) = mock_upstream(409, r#"{"error":{"code":409,"message":"exists"}}"#).await;
    let mut cfg = Config::default();
    cfg.gcs_base = base;

    let resp = route(&cfg, &req("POST", "/api/gcs/buckets", br#"{"name":"a"}"#)).await;
    assert_eq!(resp.status, 409);
}

#[tokio::test]
async fn gcs_delete_bucket_forwards_provider_204() {
    let (base, record) = mock_upstream(204, "").await;
    let mut cfg = Config::default();
    cfg.gcs_base = base;

    let resp = route(&cfg, &req("DELETE", "/api/gcs/buckets/alpha", b"")).await;
    assert_eq!(resp.status, 204);
    let rec = record.lock().unwrap().clone();
    assert_eq!(rec.method, "DELETE");
    assert_eq!(rec.target, "/storage/v1/b/alpha");
}

#[tokio::test]
async fn gcs_delete_object_forwards_provider_204() {
    let (base, record) = mock_upstream(204, "").await;
    let mut cfg = Config::default();
    cfg.gcs_base = base;

    let resp = route(
        &cfg,
        &req("DELETE", "/api/gcs/buckets/alpha/objects/a.txt", b""),
    )
    .await;
    assert_eq!(resp.status, 204);
    let rec = record.lock().unwrap().clone();
    assert_eq!(rec.method, "DELETE");
    assert_eq!(rec.target, "/storage/v1/b/alpha/o/a.txt");
}

#[tokio::test]
async fn gcs_delete_upload_session_forwards_to_control() {
    let (base, record) = mock_upstream(204, "").await;
    let mut cfg = Config::default();
    cfg.gcs_base = base;

    let resp = route(&cfg, &req("DELETE", "/api/gcs/uploads/aaa111", b"")).await;
    assert_eq!(resp.status, 204);
    let rec = record.lock().unwrap().clone();
    assert_eq!(rec.method, "DELETE");
    assert_eq!(rec.target, "/_control/uploads/aaa111");
}

// ── S3: introspect reads + provider-protocol mutations ──────────────────────

#[tokio::test]
async fn s3_buckets_get_forwards_to_introspect() {
    let (base, record) = mock_upstream(
        200,
        r#"{"buckets":[{"name":"demo","creationDate":"2020-01-01T00:00:00Z","objectCount":2}]}"#,
    )
    .await;
    let mut cfg = Config::default();
    cfg.s3_base = base;

    let resp = route(&cfg, &req("GET", "/api/s3/buckets", b"")).await;
    assert_eq!(resp.status, 200);
    let rec = record.lock().unwrap().clone();
    assert_eq!(rec.method, "GET");
    assert_eq!(rec.target, "/_introspect/buckets");
    // Envelope relayed byte-identically.
    let v: serde_json::Value = serde_json::from_slice(&resp.body).unwrap();
    assert_eq!(v["buckets"][0]["name"], "demo");
    assert_eq!(v["buckets"][0]["objectCount"], 2);
}

#[tokio::test]
async fn s3_objects_get_forwards_to_introspect_with_prefix() {
    let (base, record) = mock_upstream(
        200,
        r#"{"bucket":"demo","prefix":"docs/","objects":[{"key":"docs/a.txt","s3Uri":"s3://demo/docs/a.txt"}]}"#,
    )
    .await;
    let mut cfg = Config::default();
    cfg.s3_base = base;

    let resp = route(
        &cfg,
        &req_q("GET", "/api/s3/buckets/demo/objects", "prefix=docs%2F", b""),
    )
    .await;
    assert_eq!(resp.status, 200);
    let rec = record.lock().unwrap().clone();
    assert_eq!(
        rec.target,
        "/_introspect/buckets/demo/objects?prefix=docs%2F"
    );
    let v: serde_json::Value = serde_json::from_slice(&resp.body).unwrap();
    assert_eq!(v["objects"][0]["key"], "docs/a.txt");
}

#[tokio::test]
async fn s3_multipart_get_forwards_to_introspect() {
    let (base, record) = mock_upstream(200, r#"{"bucket":"demo","uploads":[]}"#).await;
    let mut cfg = Config::default();
    cfg.s3_base = base;

    let resp = route(&cfg, &req("GET", "/api/s3/buckets/demo/multipart", b"")).await;
    assert_eq!(resp.status, 200);
    let rec = record.lock().unwrap().clone();
    assert_eq!(rec.target, "/_introspect/buckets/demo/multipart");
}

#[tokio::test]
async fn s3_bucket_detail_get_forwards_to_introspect() {
    let (base, record) = mock_upstream(
        200,
        r#"{"name":"demo","creationDate":"2020-01-01T00:00:00Z","objectCount":0}"#,
    )
    .await;
    let mut cfg = Config::default();
    cfg.s3_base = base;

    let resp = route(&cfg, &req("GET", "/api/s3/buckets/demo", b"")).await;
    assert_eq!(resp.status, 200);
    let rec = record.lock().unwrap().clone();
    assert_eq!(rec.target, "/_introspect/buckets/demo");
}

#[tokio::test]
async fn s3_bucket_delete_forwards_provider_protocol() {
    let (base, record) = mock_upstream(204, "").await;
    let mut cfg = Config::default();
    cfg.s3_base = base;

    let resp = route(&cfg, &req("DELETE", "/api/s3/buckets/demo", b"")).await;
    assert_eq!(resp.status, 204);
    let rec = record.lock().unwrap().clone();
    assert_eq!(rec.method, "DELETE");
    assert_eq!(rec.target, "/demo");
}

#[tokio::test]
async fn s3_object_delete_forwards_to_control_endpoint() {
    // Object delete forwards to the S3 service's /_control/ object-delete (NOT
    // the idempotent provider DELETE), relaying its 204 verbatim. The key is
    // encoded as a single path segment so a literal "/" becomes "%2F".
    let (base, record) = mock_upstream(204, "").await;
    let mut cfg = Config::default();
    cfg.s3_base = base;

    let resp = route(
        &cfg,
        &req("DELETE", "/api/s3/buckets/demo/objects/docs/a.txt", b""),
    )
    .await;
    assert_eq!(resp.status, 204);
    let rec = record.lock().unwrap().clone();
    assert_eq!(rec.method, "DELETE");
    assert_eq!(rec.target, "/_control/buckets/demo/objects/docs%2Fa.txt");
}

#[tokio::test]
async fn s3_object_delete_absent_relays_control_404() {
    // The /_control/ endpoint returns 404 when the object was absent (unlike the
    // provider DELETE, which is idempotent). The dashboard relays that 404.
    let (base, _record) = mock_upstream(404, "{\"error\":\"object does not exist\"}").await;
    let mut cfg = Config::default();
    cfg.s3_base = base;

    let resp = route(
        &cfg,
        &req("DELETE", "/api/s3/buckets/demo/objects/gone.txt", b""),
    )
    .await;
    assert_eq!(resp.status, 404);
}

#[tokio::test]
async fn s3_object_download_forwards_provider_get_and_defaults_disposition() {
    let (base, record) = mock_upstream(200, "hello body").await;
    let mut cfg = Config::default();
    cfg.s3_base = base;

    let resp = route(
        &cfg,
        &req(
            "GET",
            "/api/s3/buckets/demo/objects/docs/a.txt/download",
            b"",
        ),
    )
    .await;
    assert_eq!(resp.status, 200);
    let rec = record.lock().unwrap().clone();
    assert_eq!(rec.method, "GET");
    assert_eq!(rec.target, "/demo/docs/a.txt");
    assert_eq!(resp.body, b"hello body");
    // The mock upstream sends Content-Type: application/json and no
    // Content-Disposition, so the dashboard synthesizes a filename disposition.
    let disposition = resp
        .headers
        .iter()
        .find(|(k, _)| k == "Content-Disposition")
        .map(|(_, v)| v.clone())
        .unwrap_or_default();
    assert_eq!(disposition, r#"attachment; filename="a.txt""#);
}

#[tokio::test]
async fn s3_disabled_service_returns_503() {
    let cfg = Config::default(); // no s3_base
    let resp = route(&cfg, &req("GET", "/api/s3/buckets", b"")).await;
    assert_eq!(resp.status, 503);
}

#[tokio::test]
async fn disabled_service_returns_503() {
    let cfg = Config::default(); // no bases set
    let resp = route(&cfg, &req("GET", "/api/redis/keys", b"")).await;
    assert_eq!(resp.status, 503);
}
