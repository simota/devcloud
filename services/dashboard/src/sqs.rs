//! SQS dashboard handler — the REUSABLE PER-SERVICE TEMPLATE for Phase 2.
//!
//! Ports `internal/dashboard/sqs_handlers.rs` to out-of-process forwarding. The
//! legacy dashboard held an in-process `*sqs.Server`; this handler instead reaches
//! the SQS service over the network. Two forwarding targets are used, matching
//! exactly what the legacy handler did in-process:
//!
//!   READS  -> the Phase-1 read-only introspection API:
//!             GET {sqs_base}/_introspect/queues            (Snapshot)
//!             GET {sqs_base}/_introspect/queues/{name}      (QueueDetailSnapshot)
//!             GET {sqs_base}/_introspect/queues/{name}/dlq  (DeadLetterSnapshot)
//!
//!   MUTATIONS -> the SQS PROVIDER PROTOCOL (AWS JSON 1.0). SQS has no
//!             `/_control/` namespace because every dashboard mutation maps to a
//!             real AWS action, so the legacy dashboard forwarded these via
//!             `s.sqs.ServeHTTP` posting `X-Amz-Target: AmazonSQS.<Action>` to
//!             `/`. We do the same over the network:
//!               CreateQueue / SendMessage / ReceiveMessage / DeleteMessage /
//!               ChangeMessageVisibility / PurgeQueue.
//!
//! The `/api/sqs/*` response shapes are IDENTICAL to the legacy dashboard (the source
//! of truth): the read endpoints re-wrap the introspection JSON into the same
//! `{"queues": ...}` / `{"queue": ...}` / `{"queueName": ..., "messages": ...}`
//! envelopes the legacy handler returned.
//!
//! ── HOW TO REPLICATE FOR ANOTHER SERVICE ────────────────────────────────────
//!   1. Add `<svc>_base` to `config.rs` + a `DEVCLOUD_DASHBOARD_<SVC>_BASE` env
//!      var in `dashboard_rust.rs`.
//!   2. Copy this file to `<svc>.rs`. Replace the introspection paths with that
//!      service's `/_introspect/...` routes (see its `introspect.rs`).
//!   3. For mutations: if the service has a `/_control/` namespace (mail, redis,
//!      redshift `/_control/query`), forward there. If mutations are real
//!      provider actions (s3/gcs/dynamodb/bigquery/sqs/pubsub — they forwarded
//!      via `ServeHTTP` in legacy), forward to the provider protocol like the
//!      `forward_provider_*` helpers below.
//!   4. Keep the response envelope byte-identical to the legacy `<svc>_handlers.rs`.
//!   5. Register the routes in `http.rs::route`.

use serde_json::Value;

use crate::config::Config;
use crate::forward::{forward, ForwardError, ForwardRequest, ForwardResponse};
use crate::http::{path_segment_decode, Request, Response};

/// `GET /api/sqs/status` — mirrors legacy `handleSQSStatus`. Reads the snapshot via
/// introspection and assembles the same status envelope. When SQS is disabled
/// (no base) or unreachable, it still returns 200 with `status:"disabled"`,
/// matching the legacy handler's behavior when `s.sqs == nil`.
pub async fn handle_status(config: &Config, req: &Request) -> Response {
    if req.method != "GET" {
        return Response::method_not_allowed("GET");
    }

    let mut status = "disabled";
    let mut running = false;
    let mut region = config.sqs_region.clone();
    let mut queue_count = 0usize;

    if !config.sqs_base.is_empty() {
        if let Ok(resp) = introspect(config, "/_introspect/queues").await {
            if resp.status == 200 {
                if let Ok(snapshot) = serde_json::from_slice::<Value>(&resp.body) {
                    status = snapshot_status(&snapshot);
                    running = snapshot
                        .get("running")
                        .and_then(Value::as_bool)
                        .unwrap_or(false);
                    if let Some(r) = snapshot.get("region").and_then(Value::as_str) {
                        if !r.is_empty() {
                            region = r.to_string();
                        }
                    }
                    queue_count = snapshot
                        .get("queues")
                        .and_then(Value::as_array)
                        .map(|q| q.len())
                        .unwrap_or(0);
                }
            }
        }
    }

    let endpoint = if config.sqs_base.is_empty() {
        "http://127.0.0.1:9324".to_string()
    } else {
        config.sqs_base.clone()
    };
    let region = if region.is_empty() {
        "us-east-1".to_string()
    } else {
        region
    };

    Response::json(
        200,
        &serde_json::json!({
            "service": "sqs",
            "status": status,
            "running": running,
            "endpoint": endpoint,
            "region": region,
            "authMode": config.sqs_auth_mode,
            "storagePath": config.sqs_storage_path,
            "queueCount": queue_count,
        }),
    )
}

/// `GET /api/sqs/queues`  -> `{"queues": [...]}` from the snapshot.
/// `POST /api/sqs/queues` -> CreateQueue via the provider protocol.
pub async fn handle_queues(config: &Config, req: &Request) -> Response {
    if req.method != "GET" && req.method != "POST" {
        return Response::method_not_allowed("GET, POST");
    }
    if config.sqs_base.is_empty() {
        return Response::text_error(503, "sqs service is disabled");
    }
    if req.method == "POST" {
        return forward_provider_operation(config, req, "CreateQueue", "", "").await;
    }

    match introspect(config, "/_introspect/queues").await {
        Ok(resp) if resp.status == 200 => {
            let snapshot: Value = match serde_json::from_slice(&resp.body) {
                Ok(v) => v,
                Err(_) => {
                    return Response::text_error(502, "sqs introspection returned invalid json")
                }
            };
            let queues = snapshot
                .get("queues")
                .cloned()
                .unwrap_or(Value::Array(vec![]));
            Response::json(200, &serde_json::json!({ "queues": queues }))
        }
        Ok(resp) => relay_error(resp),
        Err(e) => forward_failure(e),
    }
}

/// `/api/sqs/queues/{name}` and its sub-resources — mirrors legacy `handleSQSQueue`.
pub async fn handle_queue(config: &Config, req: &Request) -> Response {
    if config.sqs_base.is_empty() {
        return Response::text_error(503, "sqs service is disabled");
    }

    // Parse the path the same way legacy `dashboardPathParts` does, off the
    // ESCAPED path, so percent-encoded queue names round-trip safely.
    let parts = match path_parts(&req.raw_path, "/api/sqs/queues/") {
        Some(p) => p,
        None => return Response::text_error(400, "invalid sqs queue path"),
    };
    if parts.is_empty() {
        return Response::text_error(404, "404 page not found");
    }
    let queue_name = &parts[0];

    // Resolve the queue detail first (legacy fetches QueueDetailSnapshot up front and
    // 404s if the queue is unknown).
    let detail = match introspect(
        config,
        &format!("/_introspect/queues/{}", encode_segment(queue_name)),
    )
    .await
    {
        Ok(resp) if resp.status == 200 => match serde_json::from_slice::<Value>(&resp.body) {
            Ok(v) => v,
            Err(_) => return Response::text_error(502, "sqs introspection returned invalid json"),
        },
        Ok(resp) if resp.status == 404 => return Response::text_error(404, "404 page not found"),
        Ok(resp) => return relay_error(resp),
        Err(e) => return forward_failure(e),
    };
    let queue_url = detail
        .get("queue")
        .and_then(|q| q.get("url"))
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();

    if parts.len() == 1 {
        if req.method != "GET" {
            return Response::method_not_allowed("GET");
        }
        let queue = detail.get("queue").cloned().unwrap_or(Value::Null);
        return Response::json(200, &serde_json::json!({ "queue": queue }));
    }

    match parts[1].as_str() {
        "messages" => {
            if req.method != "GET" && req.method != "POST" {
                return Response::method_not_allowed("GET, POST");
            }
            if req.method == "POST" {
                return forward_provider_operation(
                    config,
                    req,
                    "SendMessage",
                    queue_name,
                    &queue_url,
                )
                .await;
            }
            let messages = detail
                .get("messages")
                .cloned()
                .unwrap_or(Value::Array(vec![]));
            Response::json(
                200,
                &serde_json::json!({
                    "queueName": queue_name,
                    "messages": messages,
                }),
            )
        }
        "receive" => {
            forward_provider_operation(config, req, "ReceiveMessage", queue_name, &queue_url).await
        }
        "delete" => {
            forward_provider_operation(config, req, "DeleteMessage", queue_name, &queue_url).await
        }
        "visibility" => {
            forward_provider_operation(
                config,
                req,
                "ChangeMessageVisibility",
                queue_name,
                &queue_url,
            )
            .await
        }
        "leases" => {
            if req.method != "GET" {
                return Response::method_not_allowed("GET");
            }
            let leases = detail
                .get("leases")
                .cloned()
                .unwrap_or(Value::Array(vec![]));
            Response::json(
                200,
                &serde_json::json!({
                    "queueName": queue_name,
                    "leases": leases,
                }),
            )
        }
        "dlq" => {
            if req.method != "GET" {
                return Response::method_not_allowed("GET");
            }
            match introspect(
                config,
                &format!("/_introspect/queues/{}/dlq", encode_segment(queue_name)),
            )
            .await
            {
                Ok(resp) if resp.status == 200 => {
                    let dlq: Value = match serde_json::from_slice(&resp.body) {
                        Ok(v) => v,
                        Err(_) => {
                            return Response::text_error(
                                502,
                                "sqs introspection returned invalid json",
                            )
                        }
                    };
                    Response::json(
                        200,
                        &serde_json::json!({
                            "queueName": queue_name,
                            "deadLetterQueue": dlq.get("deadLetterQueue").cloned().unwrap_or(Value::Null),
                            "deadLetterSourceQueues": dlq
                                .get("deadLetterSourceQueues")
                                .cloned()
                                .unwrap_or(Value::Array(vec![])),
                        }),
                    )
                }
                Ok(resp) if resp.status == 404 => Response::text_error(404, "404 page not found"),
                Ok(resp) => relay_error(resp),
                Err(e) => forward_failure(e),
            }
        }
        "purge" => {
            if req.method != "POST" {
                return Response::method_not_allowed("POST");
            }
            // legacy used the in-process PurgeQueueByName; the network equivalent is
            // the real AWS PurgeQueue action. On success the legacy dashboard
            // returned 204 No Content, so we normalize a successful provider
            // response to 204 as well.
            let input = serde_json::json!({ "QueueUrl": queue_url }).to_string();
            match forward_provider_json(config, "PurgeQueue", input.into_bytes()).await {
                Ok(resp) if (200..300).contains(&resp.status) => Response {
                    status: 204,
                    headers: Vec::new(),
                    body: Vec::new(),
                },
                Ok(resp) => relay_error(resp),
                Err(e) => forward_failure(e),
            }
        }
        _ => Response::text_error(404, "404 page not found"),
    }
}

// ── forwarding helpers ──────────────────────────────────────────────────────

/// GET a path on the SQS service (introspection or any read).
async fn introspect(config: &Config, path: &str) -> Result<ForwardResponse, ForwardError> {
    forward(ForwardRequest {
        base: &config.sqs_base,
        method: "GET",
        path,
        headers: Vec::new(),
        body: Vec::new(),
    })
    .await
}

/// Forwards a dashboard mutation request to the SQS provider protocol, mirroring
/// legacy `forwardSQSDashboardOperation`: it reads the `{"input": {...}}` envelope
/// from the request body, normalizes `QueueName`/`QueueUrl` against the selected
/// queue, then POSTs the AWS JSON 1.0 request to `/`.
async fn forward_provider_operation(
    config: &Config,
    req: &Request,
    operation: &str,
    queue_name: &str,
    queue_url: &str,
) -> Response {
    if req.method != "POST" {
        return Response::method_not_allowed("POST");
    }
    let envelope: Value = match serde_json::from_slice(&req.body) {
        Ok(v) => v,
        Err(_) => return Response::text_error(400, "invalid json request"),
    };
    let raw_input = match envelope.get("input") {
        Some(v) => v.clone(),
        None => return Response::text_error(400, "input is required"),
    };
    let input = match normalize_input(raw_input, queue_name, queue_url) {
        Ok(v) => v,
        Err(msg) => return Response::text_error(400, &msg),
    };
    match forward_provider_json(config, operation, input).await {
        Ok(resp) => relay(resp),
        Err(e) => forward_failure(e),
    }
}

/// POSTs an AWS JSON 1.0 request for `operation` with body `input` to the SQS
/// service root `/`, exactly as legacy set the `X-Amz-Target` + content-type.
async fn forward_provider_json(
    config: &Config,
    operation: &str,
    input: Vec<u8>,
) -> Result<ForwardResponse, ForwardError> {
    forward(ForwardRequest {
        base: &config.sqs_base,
        method: "POST",
        path: "/",
        headers: vec![
            (
                "Content-Type".to_string(),
                "application/x-amz-json-1.0".to_string(),
            ),
            ("X-Amz-Target".to_string(), format!("AmazonSQS.{operation}")),
        ],
        body: input,
    })
    .await
}

/// Mirrors legacy `normalizeSQSDashboardInput`: rejects a mismatched `QueueName`,
/// fills in / validates `QueueUrl`, and re-encodes the object.
fn normalize_input(raw: Value, queue_name: &str, queue_url: &str) -> Result<Vec<u8>, String> {
    if raw.is_null() {
        return Err("input is required".to_string());
    }
    let mut obj = match raw {
        Value::Object(m) => m,
        _ => return Err("input must be a JSON object".to_string()),
    };
    if !queue_name.is_empty() {
        if let Some(existing) = obj.get("QueueName") {
            match existing.as_str() {
                Some(name) if name == queue_name => {}
                _ => return Err("input QueueName must match the selected queue".to_string()),
            }
        }
    }
    if !queue_url.is_empty() {
        match obj.get("QueueUrl") {
            Some(existing) => match existing.as_str() {
                Some(u) if u == queue_url => {}
                _ => return Err("input QueueUrl must match the selected queue".to_string()),
            },
            None => {
                obj.insert("QueueUrl".to_string(), Value::String(queue_url.to_string()));
            }
        }
    }
    serde_json::to_vec(&Value::Object(obj)).map_err(|_| "input could not be encoded".to_string())
}

/// Relays a downstream response verbatim (status + body + content-type). Used
/// for provider-protocol responses so the dashboard surfaces the exact AWS
/// JSON the SQS service produced, like legacy `ServeHTTP` passthrough.
fn relay(resp: ForwardResponse) -> Response {
    let content_type = {
        let ct = resp.header("content-type");
        if ct.is_empty() {
            "application/json".to_string()
        } else {
            ct.to_string()
        }
    };
    Response::new(resp.status, &content_type, resp.body)
}

/// Relays an unexpected (non-2xx) downstream introspection response, preserving
/// its status and body.
fn relay_error(resp: ForwardResponse) -> Response {
    relay(resp)
}

/// Maps a forwarding failure to a dashboard response. An unreachable service is a
/// 502 (the dashboard is up but the upstream is not), a bad base/response a 500.
fn forward_failure(err: ForwardError) -> Response {
    match err {
        ForwardError::Unreachable(_) => Response::text_error(502, "sqs service is unreachable"),
        ForwardError::BadBase => Response::text_error(500, "sqs service address is misconfigured"),
        ForwardError::BadResponse => {
            Response::text_error(502, "sqs service returned an invalid response")
        }
    }
}

fn snapshot_status(snapshot: &Value) -> &'static str {
    // The introspection snapshot carries a "status" string; the dashboard only
    // ever surfaces "running"/"disabled" semantics, but we pass the snapshot's
    // own status through verbatim by mapping known values.
    match snapshot.get("status").and_then(Value::as_str) {
        Some("running") => "running",
        Some("disabled") => "disabled",
        Some(_) => "running",
        None => "disabled",
    }
}

/// Splits the path after `prefix` into decoded segments, rejecting traversal —
/// mirrors legacy `dashboardPathParts(r.URL.EscapedPath(), prefix)`.
fn path_parts(escaped_path: &str, prefix: &str) -> Option<Vec<String>> {
    let suffix = escaped_path.strip_prefix(prefix)?;
    let mut parts = Vec::new();
    for raw in suffix.trim_matches('/').split('/') {
        if raw.is_empty() {
            continue;
        }
        let decoded = path_segment_decode(raw)?;
        parts.push(decoded);
    }
    Some(parts)
}

/// Percent-encodes a single path segment for use in an outbound introspection
/// URL (queue names can contain characters that must be escaped). Encodes
/// everything outside the RFC 3986 unreserved set.
fn encode_segment(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char)
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_fills_queue_url_when_absent() {
        let input = serde_json::json!({ "MessageBody": "hi" });
        let out = normalize_input(input, "q", "http://x/q").unwrap();
        let v: Value = serde_json::from_slice(&out).unwrap();
        assert_eq!(v["QueueUrl"], "http://x/q");
        assert_eq!(v["MessageBody"], "hi");
    }

    #[test]
    fn normalize_rejects_mismatched_queue_name() {
        let input = serde_json::json!({ "QueueName": "other" });
        assert!(normalize_input(input, "q", "").is_err());
    }

    #[test]
    fn normalize_rejects_mismatched_queue_url() {
        let input = serde_json::json!({ "QueueUrl": "http://x/other" });
        assert!(normalize_input(input, "q", "http://x/q").is_err());
    }

    #[test]
    fn normalize_rejects_non_object() {
        let input = serde_json::json!([1, 2, 3]);
        assert!(normalize_input(input, "", "").is_err());
    }

    #[test]
    fn path_parts_decodes_segments() {
        let parts = path_parts("/api/sqs/queues/my%2Dqueue/dlq", "/api/sqs/queues/").unwrap();
        assert_eq!(parts, vec!["my-queue", "dlq"]);
    }

    #[test]
    fn path_parts_rejects_traversal() {
        assert!(path_parts("/api/sqs/queues/..%2Fetc", "/api/sqs/queues/").is_none());
    }

    #[test]
    fn encode_segment_escapes_reserved() {
        assert_eq!(encode_segment("a b/c"), "a%20b%2Fc");
        assert_eq!(encode_segment("plain-name_1.fifo"), "plain-name_1.fifo");
    }
}
