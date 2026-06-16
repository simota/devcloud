//! Dashboard GOLDEN CONFORMANCE replay (docs/ROADMAP-go-removal.md Phase 3).
//!
//! This is the legacy-independent half of the dashboard cross-engine oracle. The legacy
//! side (internal/app/dashboard_golden_test.rs, env-gated capture) froze the legacy
//! dashboard's normalized responses to repo-root testdata/golden/dashboard/*.json.
//! This test boots the REAL Rust product (`devcloud up`, the in-process
//! orchestrator that runs every service + the dashboard), replays the IDENTICAL
//! seed sequence and route probes, applies the IDENTICAL normalization, and
//! asserts the normalized live response equals each committed golden.
//!
//! Together they replace the live legacy<->Rust differential harness once legacy is gone:
//! the goldens are the frozen legacy behavior, this replay proves the Rust product
//! still matches it byte-for-byte (post-normalization).
//!
//! Boot model: subprocess. We spawn the built `devcloud` binary
//! (env!("CARGO_BIN_EXE_devcloud")) with `up` inside a temp workspace, after a
//! `devcloud init` whose config DISABLES redshift (so the test needs no external
//! PostgreSQL) and leaves redis disabled (default). The dashboard + the in-process
//! services come up on the DEFAULT ports; we wait for the dashboard + each backend
//! port, seed, probe, assert, then SIGTERM the process.
//!
//! Seeding reuses `devcloud_dashboard::forward` (the crate's hand-rolled HTTP/1.1
//! client) for every provider-protocol call, and a raw tokio TCP SMTP dialog for
//! the mail message — mirroring seedMail in the legacy harness.
//!
//! ── NORMALIZATION SPEC (BYTE-CRITICAL — IDENTICAL to dashboard_golden_test.rs) ─
//!
//! The legacy golden was captured from random temp dirs + random ports; this replay
//! runs the default config (fixed ports, .devcloud/data). Nondeterministic AND
//! config-dependent fields are collapsed to placeholders on BOTH sides before
//! compare. Applied to the PARSED JSON body:
//!
//!  1. Re-serialize canonically with SORTED keys (drops trailing-newline + map
//!     key-order differences).
//!  2. For EVERY string scalar, in this order:
//!     a. RFC3339 / RFC3339Nano timestamp                         -> "<TS>"
//!     b. URL authority host:port for http/https/ws/wss/smtp/redis
//!        (scheme://HOST:PORT...) -> scheme + "//<ADDR>" + remaining path
//!     c. absolute-ish storage path (has "/" + a volatile data segment,
//!        or starts with "/")                                     -> "<PATH>"
//!     d. long hex token (>=32 hex chars: etag/md5/crc/sha)        -> "<HASH>"
//!  3. By KEY NAME, generated identifiers with no distinctive value pattern
//!     -> "<ID>". Keys: messageId, ackId, uploadId, jobId, generation,
//!     nextMessageId, id.
//!
//! Plus ONE documented wiring-difference mask, applied to the two registry routes
//! (/api/services, /api/dashboard/services) on BOTH sides: the `status` field of
//! the `redshift` and `redis` service entries is replaced with "<WIRING>". The legacy
//! golden world (bootParityWorld) does not boot redshift/redis, so it reports them
//! "disabled"; the real orchestrator always wires the redshift base (-> "running")
//! while redis stays "disabled". That status is pure wiring, not behavior, and is
//! the only field that legitimately differs between the artificial parity world
//! and the shipped product, so it is masked symmetrically. Every other service's
//! status is asserted unmasked.
//!
//! ── ROUTE SET ─────────────────────────────────────────────────────────────────
//!
//! The 20 body-bearing routes captured by the legacy side (compareBody=true rows).
//! Header-only/SPA/legacy-redirect rows and /api/events (WS) are intentionally NOT
//! golden-replayed here (no stable body); their status/header/substring parity is
//! covered by the differential harness while it lives, and by the dashboard
//! crate's own unit tests. Omissions are listed in the Builder report.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::Child;
use std::time::Duration;

use devcloud_dashboard::forward::{forward, ForwardRequest};
use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

// Ports for the test's `devcloud up`. These are the default ports + 20000 to
// avoid colliding with anything the developer already runs on the defaults
// (e.g. Docker/LocalStack on 4566/8000). `make_workspace` writes a matching
// `server:` block into the config, and the golden normalization masks
// host:port -> "<ADDR>" so the alternate ports do not affect the byte compare.
const DASHBOARD_PORT: u16 = 28025;
const SMTP_PORT: u16 = 21025;
const S3_PORT: u16 = 24566;
const GCS_PORT: u16 = 24443;
const DYNAMODB_PORT: u16 = 28000;
const BIGQUERY_PORT: u16 = 29050;
const SQS_PORT: u16 = 29324;
const PUBSUB_REST_PORT: u16 = 28086;

fn dashboard_base() -> String {
    format!("http://127.0.0.1:{DASHBOARD_PORT}")
}
fn svc_base(port: u16) -> String {
    format!("http://127.0.0.1:{port}")
}

// ── golden file shape (mirrors goldenFile in dashboard_golden_test.rs) ─────────

#[derive(serde::Deserialize)]
struct GoldenFile {
    #[allow(dead_code)]
    route: String,
    path: String,
    status: i64,
    #[serde(rename = "contentType")]
    content_type: String,
    #[serde(rename = "cacheControl")]
    cache_control: String,
    location: String,
    body: Value,
}

/// Route names captured by the legacy side, in the same order.
const GOLDEN_ROUTE_NAMES: &[&str] = &[
    "api-services",
    "api-dashboard-services",
    "sqs-status",
    "sqs-queues",
    "sqs-queue-detail",
    "dynamodb-status",
    "dynamodb-tables",
    "dynamodb-table-detail",
    "bigquery-status",
    "bigquery-projects",
    "pubsub-status",
    "pubsub-topics",
    "pubsub-subscriptions",
    "s3-status",
    "s3-buckets",
    "s3-bucket-detail",
    "gcs-status",
    "gcs-buckets",
    "gcs-uploads",
    "mail-messages",
];

fn golden_dir() -> PathBuf {
    // CARGO_MANIFEST_DIR = <repo>/orchestrator
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    manifest
        .parent() // <repo>
        .expect("resolve repo root from CARGO_MANIFEST_DIR")
        .join("testdata")
        .join("golden")
        .join("dashboard")
}

// ── the test ───────────────────────────────────────────────────────────────--

#[test]
fn dashboard_golden_conformance() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("build tokio runtime");
    rt.block_on(run());
}

async fn run() {
    let dir = golden_dir();
    assert!(
        dir.join("api-services.json").exists(),
        "goldens missing at {} — run the legacy capture first:\n  \
         DEVCLOUD_DASHBOARD_GOLDEN=write cargo test -p devcloud-orchestrator --test dashboard_conformance",
        dir.display()
    );

    let workspace = tempdir();
    let mut guard = ChildGuard(spawn_devcloud_up(&workspace));

    // Wait for the dashboard + every backend the goldens touch.
    wait_for_tcp(DASHBOARD_PORT).await;
    for p in [
        SMTP_PORT,
        S3_PORT,
        GCS_PORT,
        DYNAMODB_PORT,
        BIGQUERY_PORT,
        SQS_PORT,
        PUBSUB_REST_PORT,
    ] {
        wait_for_tcp(p).await;
    }

    // Seed identical state through the provider protocols + SMTP.
    seed_sqs().await;
    seed_dynamodb().await;
    seed_bigquery().await;
    seed_pubsub().await;
    seed_s3().await;
    seed_gcs().await;
    seed_mail().await;

    let mut failures: Vec<String> = Vec::new();
    for name in GOLDEN_ROUTE_NAMES {
        if let Err(e) = check_route(&dir, name).await {
            failures.push(format!("[{name}] {e}"));
        }
    }

    // Tear down BEFORE asserting so a failure still kills the process.
    guard.kill();

    assert!(
        failures.is_empty(),
        "dashboard golden conformance failures:\n{}",
        failures.join("\n")
    );
}

async fn check_route(dir: &Path, name: &str) -> Result<(), String> {
    let golden_bytes =
        std::fs::read(dir.join(format!("{name}.json"))).map_err(|e| format!("read golden: {e}"))?;
    let golden: GoldenFile =
        serde_json::from_slice(&golden_bytes).map_err(|e| format!("parse golden: {e}"))?;

    let resp = forward(ForwardRequest {
        base: &dashboard_base(),
        method: "GET",
        path: &golden.path,
        headers: vec![],
        body: vec![],
    })
    .await
    .map_err(|e| format!("GET {}: {e:?}", golden.path))?;

    if resp.status as i64 != golden.status {
        return Err(format!(
            "status: live={} golden={}\n body={}",
            resp.status,
            golden.status,
            String::from_utf8_lossy(&resp.body)
        ));
    }
    // Content-Type / Cache-Control / Location parity.
    if resp.header("content-type") != golden.content_type {
        return Err(format!(
            "Content-Type: live={:?} golden={:?}",
            resp.header("content-type"),
            golden.content_type
        ));
    }
    if resp.header("cache-control") != golden.cache_control {
        return Err(format!(
            "Cache-Control: live={:?} golden={:?}",
            resp.header("cache-control"),
            golden.cache_control
        ));
    }
    if resp.header("location") != golden.location {
        return Err(format!(
            "Location: live={:?} golden={:?}",
            resp.header("location"),
            golden.location
        ));
    }

    // Body parity, post-normalization.
    let live_val: Value = serde_json::from_slice(&resp.body).map_err(|e| {
        format!(
            "live body not JSON: {e}\n body={}",
            String::from_utf8_lossy(&resp.body)
        )
    })?;
    let live_norm = normalize_root(name, live_val);
    let golden_norm = normalize_root(name, golden.body.clone());

    let live_canon = canonical(&live_norm);
    let golden_canon = canonical(&golden_norm);
    if live_canon != golden_canon {
        return Err(format!(
            "body mismatch:\n live=  {live_canon}\n golden={golden_canon}"
        ));
    }
    Ok(())
}

// ── normalization (shared spec; mirror of dashboard_golden_test.rs) ────────────

/// Applies the per-route wiring mask (registry routes only), then the recursive
/// value normalization.
fn normalize_root(route: &str, mut v: Value) -> Value {
    if route == "api-services" || route == "api-dashboard-services" {
        mask_registry_wiring(&mut v);
    }
    normalize_value("", v)
}

/// Replaces the `status` of the redshift + redis service entries with "<WIRING>"
/// (the one documented parity-world vs shipped-product wiring difference).
fn mask_registry_wiring(v: &mut Value) {
    if let Some(services) = v.get_mut("services").and_then(Value::as_array_mut) {
        for entry in services {
            // Match on `name` (display name), NOT `id`: the golden is stored
            // already-normalized so its `id` is "<ID>", whereas the live response
            // still has the real id at mask time. `name` ("Redshift"/"Redis") is
            // never normalized, so matching it masks BOTH sides symmetrically.
            let name = entry.get("name").and_then(Value::as_str).unwrap_or("");
            if name == "Redshift" || name == "Redis" {
                if let Some(obj) = entry.as_object_mut() {
                    obj.insert("status".to_string(), Value::String("<WIRING>".to_string()));
                }
            }
        }
    }
}

fn normalize_value(key: &str, v: Value) -> Value {
    match v {
        Value::Object(map) => {
            let mut out = serde_json::Map::with_capacity(map.len());
            for (k, child) in map {
                let nv = normalize_value(&k, child);
                out.insert(k, nv);
            }
            Value::Object(out)
        }
        Value::Array(arr) => Value::Array(
            arr.into_iter()
                .map(|child| normalize_value(key, child)) // elements keep parent key
                .collect(),
        ),
        Value::String(s) => Value::String(normalize_scalar_string(key, &s)),
        other => other, // numbers, bools, null — deterministic
    }
}

fn normalize_scalar_string(key: &str, s: &str) -> String {
    if s.is_empty() {
        return s.to_string();
    }
    if is_rfc3339_timestamp(s) {
        return "<TS>".to_string();
    }
    if let Some(r) = normalize_url_authority(s) {
        return r;
    }
    if is_storage_path(s) {
        return "<PATH>".to_string();
    }
    if is_long_hex(s) {
        return "<HASH>".to_string();
    }
    if is_generated_id_key(key) {
        return "<ID>".to_string();
    }
    s.to_string()
}

fn is_generated_id_key(key: &str) -> bool {
    matches!(
        key.to_ascii_lowercase().as_str(),
        "messageid" | "ackid" | "uploadid" | "jobid" | "generation" | "nextmessageid" | "id"
    )
}

/// RFC3339 / RFC3339Nano: "YYYY-MM-DDTHH:MM:SS" + optional ".<digits>" +
/// ("Z" | "±HH:MM"). Hand-rolled to mirror the legacy isRFC3339Timestamp exactly.
fn is_rfc3339_timestamp(s: &str) -> bool {
    let b = s.as_bytes();
    if b.len() < 20 {
        return false;
    }
    if !all_digits(&b[0..4])
        || b[4] != b'-'
        || !all_digits(&b[5..7])
        || b[7] != b'-'
        || !all_digits(&b[8..10])
    {
        return false;
    }
    if b[10] != b'T' {
        return false;
    }
    if !all_digits(&b[11..13])
        || b[13] != b':'
        || !all_digits(&b[14..16])
        || b[16] != b':'
        || !all_digits(&b[17..19])
    {
        return false;
    }
    let rest = &b[19..];
    let rest = if !rest.is_empty() && rest[0] == b'.' {
        let mut i = 1;
        while i < rest.len() && rest[i].is_ascii_digit() {
            i += 1;
        }
        if i == 1 {
            return false; // dot with no digits
        }
        &rest[i..]
    } else {
        rest
    };
    match rest {
        b"Z" => true,
        _ if rest.len() == 6 && (rest[0] == b'+' || rest[0] == b'-') => {
            all_digits(&rest[1..3]) && rest[3] == b':' && all_digits(&rest[4..6])
        }
        _ => false,
    }
}

fn all_digits(b: &[u8]) -> bool {
    !b.is_empty() && b.iter().all(u8::is_ascii_digit)
}

/// Collapses the host:port authority of an http/https/ws/wss/smtp/redis URL to
/// "<ADDR>", preserving scheme + path/query. Returns None when not such a URL.
fn normalize_url_authority(s: &str) -> Option<String> {
    for scheme in [
        "https://", "http://", "wss://", "ws://", "smtp://", "redis://",
    ] {
        if let Some(rest) = s.strip_prefix(scheme) {
            let cut = rest
                .find(|c| c == '/' || c == '?' || c == '#')
                .unwrap_or(rest.len());
            return Some(format!("{scheme}<ADDR>{}", &rest[cut..]));
        }
    }
    None
}

/// Filesystem path that varies between runs. Mirror of legacy isStoragePath.
fn is_storage_path(s: &str) -> bool {
    if s.contains("://") {
        return false; // a URL, handled above
    }
    if s.starts_with('/') {
        return s.matches('/').count() >= 2;
    }
    s.contains(".devcloud") || s.contains("/buckets")
}

/// >=32 hex chars (ETag/md5/sha), optional surrounding quotes. Mirror of legacy.
fn is_long_hex(s: &str) -> bool {
    let t = s.trim_matches('"');
    if t.len() < 32 {
        return false;
    }
    t.bytes().all(|c| c.is_ascii_hexdigit())
}

/// Canonical JSON: sorted keys (BTreeMap), compact, no trailing newline.
/// Mirrors the legacy marshalCanonical sorted-key serialization.
fn canonical(v: &Value) -> String {
    let sorted = to_sorted(v);
    serde_json::to_string(&sorted).expect("serialize canonical")
}

fn to_sorted(v: &Value) -> Value {
    match v {
        Value::Object(map) => {
            let mut bt: BTreeMap<String, Value> = BTreeMap::new();
            for (k, child) in map {
                bt.insert(k.clone(), to_sorted(child));
            }
            // serde_json::Value::Object preserves insertion order; BTreeMap gives
            // us sorted keys, and serde_json serializes a Map in iteration order.
            let mut out = serde_json::Map::new();
            for (k, child) in bt {
                out.insert(k, child);
            }
            Value::Object(out)
        }
        Value::Array(arr) => Value::Array(arr.iter().map(to_sorted).collect()),
        other => other.clone(),
    }
}

// ── seeding (provider protocols via forward + raw SMTP) ────────────────────────

/// POST a JSON body to a base+path, asserting a 2xx. Used for REST seeds.
async fn post_json(base: &str, path: &str, body: &str) {
    do_req(
        base,
        "POST",
        path,
        vec![("Content-Type".into(), "application/json".into())],
        body,
    )
    .await;
}

async fn do_req(base: &str, method: &str, path: &str, headers: Vec<(String, String)>, body: &str) {
    let resp = forward(ForwardRequest {
        base,
        method,
        path,
        headers,
        body: body.as_bytes().to_vec(),
    })
    .await
    .unwrap_or_else(|e| panic!("seed {method} {base}{path}: {e:?}"));
    assert!(
        (200..300).contains(&resp.status),
        "seed {method} {base}{path} status={} body={}",
        resp.status,
        String::from_utf8_lossy(&resp.body)
    );
}

async fn seed_sqs() {
    let base = svc_base(SQS_PORT);
    let amz = |target: &str, body: &str| {
        let base = base.clone();
        let target = target.to_string();
        let body = body.to_string();
        async move {
            do_req(
                &base,
                "POST",
                "/",
                vec![
                    ("Content-Type".into(), "application/x-amz-json-1.0".into()),
                    ("X-Amz-Target".into(), format!("AmazonSQS.{target}")),
                ],
                &body,
            )
            .await;
        }
    };
    amz("CreateQueue", r#"{"QueueName":"parity-queue"}"#).await;
    let queue_url = format!("{base}/000000000000/parity-queue");
    amz(
        "SendMessage",
        &format!(r#"{{"QueueUrl":"{queue_url}","MessageBody":"parity body"}}"#),
    )
    .await;
}

async fn seed_dynamodb() {
    let base = svc_base(DYNAMODB_PORT);
    let amz = |target: &str, body: &str| {
        let base = base.clone();
        let target = target.to_string();
        let body = body.to_string();
        async move {
            do_req(
                &base,
                "POST",
                "/",
                vec![
                    ("Content-Type".into(), "application/x-amz-json-1.0".into()),
                    ("X-Amz-Target".into(), format!("DynamoDB_20120810.{target}")),
                ],
                &body,
            )
            .await;
        }
    };
    amz(
        "CreateTable",
        r#"{"TableName":"parity-table","KeySchema":[{"AttributeName":"id","KeyType":"HASH"}],"AttributeDefinitions":[{"AttributeName":"id","AttributeType":"S"}],"BillingMode":"PAY_PER_REQUEST"}"#,
    )
    .await;
    amz(
        "PutItem",
        r#"{"TableName":"parity-table","Item":{"id":{"S":"row-1"},"v":{"N":"42"}}}"#,
    )
    .await;
}

async fn seed_bigquery() {
    let base = svc_base(BIGQUERY_PORT);
    post_json(
        &base,
        "/bigquery/v2/projects/devcloud/datasets",
        r#"{"datasetReference":{"datasetId":"parity_ds"}}"#,
    )
    .await;
    post_json(
        &base,
        "/bigquery/v2/projects/devcloud/datasets/parity_ds/tables",
        r#"{"tableReference":{"tableId":"parity_tbl"},"schema":{"fields":[{"name":"id","type":"STRING"}]}}"#,
    )
    .await;
}

async fn seed_pubsub() {
    let base = svc_base(PUBSUB_REST_PORT);
    do_req(
        &base,
        "PUT",
        "/v1/projects/devcloud/topics/parity-topic",
        vec![("Content-Type".into(), "application/json".into())],
        "{}",
    )
    .await;
    do_req(
        &base,
        "PUT",
        "/v1/projects/devcloud/subscriptions/parity-sub",
        vec![("Content-Type".into(), "application/json".into())],
        r#"{"topic":"projects/devcloud/topics/parity-topic","ackDeadlineSeconds":30}"#,
    )
    .await;
}

async fn seed_s3() {
    let base = svc_base(S3_PORT);
    let put = |path: &str, body: &str| {
        let base = base.clone();
        let path = path.to_string();
        let body = body.to_string();
        async move {
            do_req(&base, "PUT", &path, vec![], &body).await;
        }
    };
    put("/parity-bucket", "").await;
    put("/parity-bucket/docs/a.txt", "hello parity").await;
    // Deletable keys (one per engine) mirror the legacy seed; harmless extras here.
    put("/parity-bucket/delete-go.txt", "delete me (go)").await;
    put("/parity-bucket/delete-rust.txt", "delete me (rust)").await;
}

async fn seed_gcs() {
    let base = svc_base(GCS_PORT);
    post_json(
        &base,
        "/storage/v1/b?project=devcloud",
        r#"{"name":"parity-gcs"}"#,
    )
    .await;
    do_req(
        &base,
        "POST",
        "/upload/storage/v1/b/parity-gcs/o?uploadType=media&name=g.txt",
        vec![("Content-Type".into(), "text/plain".into())],
        "gcs parity body",
    )
    .await;
}

/// Raw SMTP dialog over tokio TCP, mirroring seedMail: deliver one "Parity"
/// message. The Rust SMTP server greets 220, replies 250 to HELO/MAIL/RCPT, 354
/// to DATA, 250 after the terminating ".".
async fn seed_mail() {
    let mut stream = TcpStream::connect(("127.0.0.1", SMTP_PORT))
        .await
        .expect("smtp connect");
    read_smtp_reply(&mut stream, "220").await; // greeting
    smtp_cmd(&mut stream, "HELO parity\r\n", "250").await;
    smtp_cmd(&mut stream, "MAIL FROM:<a@example.com>\r\n", "250").await;
    smtp_cmd(&mut stream, "RCPT TO:<b@example.com>\r\n", "250").await;
    smtp_cmd(&mut stream, "DATA\r\n", "354").await;
    let body =
        "From: a@example.com\r\nTo: b@example.com\r\nSubject: Parity\r\n\r\nHello parity.\r\n.\r\n";
    stream
        .write_all(body.as_bytes())
        .await
        .expect("smtp write body");
    read_smtp_reply(&mut stream, "250").await; // accepted
    let _ = stream.write_all(b"QUIT\r\n").await;
}

async fn smtp_cmd(stream: &mut TcpStream, cmd: &str, want_prefix: &str) {
    stream
        .write_all(cmd.as_bytes())
        .await
        .expect("smtp write cmd");
    read_smtp_reply(stream, want_prefix).await;
}

async fn read_smtp_reply(stream: &mut TcpStream, want_prefix: &str) {
    let mut buf = [0u8; 1024];
    let n = stream.read(&mut buf).await.expect("smtp read");
    let reply = String::from_utf8_lossy(&buf[..n]);
    assert!(
        reply.starts_with(want_prefix),
        "smtp expected {want_prefix}, got {reply:?}"
    );
}

// ── process + workspace plumbing ──────────────────────────────────────────────

/// Creates a unique temp workspace dir for the devcloud subprocess.
fn tempdir() -> PathBuf {
    let base = std::env::temp_dir();
    let pid = std::process::id();
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let dir = base.join(format!("devcloud-dash-conf-{pid}-{nanos}"));
    std::fs::create_dir_all(&dir).expect("create temp workspace");
    dir
}

/// Runs `devcloud init` then writes a config that disables redshift (no external
/// PostgreSQL needed) and spawns `devcloud up`. cwd = the temp workspace so all
/// storage is rooted there.
fn spawn_devcloud_up(workspace: &Path) -> Child {
    let bin = env!("CARGO_BIN_EXE_devcloud");

    let init = std::process::Command::new(bin)
        .arg("init")
        .current_dir(workspace)
        .status()
        .expect("run devcloud init");
    assert!(init.success(), "devcloud init failed");

    // Override config via appended blocks (the parser is last-value-wins):
    //  1. Disable redshift so the supervisor does not start managed PostgreSQL.
    //  2. Move ALL server ports to the default + 20000 block so the test does
    //     not collide with anything on the developer's default ports (Docker /
    //     LocalStack commonly hold 4566/8000). The probe/seed constants above
    //     match this block; golden normalization masks host:port so the byte
    //     compare is unaffected.
    let cfg_path = workspace.join(".devcloud").join("config.yaml");
    let mut cfg = std::fs::read_to_string(&cfg_path).expect("read config.yaml");
    cfg.push_str("\nservices:\n  redshift:\n    enabled: false\n");
    cfg.push_str(
        "\nserver:\n\
\u{20}\u{20}smtpPort: 21025\n\
\u{20}\u{20}mailHttpPort: 21080\n\
\u{20}\u{20}dashboardPort: 28025\n\
\u{20}\u{20}eventRelayPort: 28027\n\
\u{20}\u{20}s3Port: 24566\n\
\u{20}\u{20}gcsPort: 24443\n\
\u{20}\u{20}dynamodbPort: 28000\n\
\u{20}\u{20}bigqueryPort: 29050\n\
\u{20}\u{20}redshiftPort: 25439\n\
\u{20}\u{20}redshiftAPIPort: 29099\n\
\u{20}\u{20}redisPort: 26379\n\
\u{20}\u{20}redisHttpPort: 26380\n\
\u{20}\u{20}sqsPort: 29324\n\
\u{20}\u{20}pubsubGrpcPort: 28085\n\
\u{20}\u{20}pubsubRestPort: 28086\n\
\u{20}\u{20}appAutoScalingPort: 28030\n",
    );
    std::fs::write(&cfg_path, cfg).expect("write config.yaml override");

    std::process::Command::new(bin)
        .arg("up")
        .current_dir(workspace)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .expect("spawn devcloud up")
}

/// Kills the devcloud process on drop / explicit teardown (SIGTERM then wait).
struct ChildGuard(Child);

impl ChildGuard {
    fn kill(&mut self) {
        #[cfg(unix)]
        {
            // SIGTERM for graceful shutdown (the supervisor handles it), then reap.
            let pid = self.0.id() as i32;
            unsafe {
                libc_kill(pid, 15 /* SIGTERM */);
            }
            // Give it a moment to exit gracefully, then force.
            for _ in 0..50 {
                match self.0.try_wait() {
                    Ok(Some(_)) => return,
                    Ok(None) => std::thread::sleep(Duration::from_millis(100)),
                    Err(_) => break,
                }
            }
        }
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

// Minimal SIGTERM via libc without adding a dependency: declare the symbol.
#[cfg(unix)]
extern "C" {
    #[link_name = "kill"]
    fn libc_kill(pid: i32, sig: i32) -> i32;
}

/// Polls a loopback port until it accepts a connection (bounded), so seeding and
/// probing see live listeners.
async fn wait_for_tcp(port: u16) {
    let addr = format!("127.0.0.1:{port}");
    for _ in 0..600 {
        if TcpStream::connect(&addr).await.is_ok() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("port {port} never came up");
}
