//! GCS dashboard handler — ports `internal/dashboard/gcs_handlers.rs`.
//!
//! The legacy dashboard held an in-process `s3svc.BucketStore` (shared with S3) and
//! called typed store methods. This handler reaches the GCS service over the
//! network instead, using three forwarding targets that match what the legacy
//! handler did in-process:
//!
//!   READS  -> the read-only introspection API (added to the GCS service):
//!             GET {gcs_base}/_introspect/uploads                          (sessions)
//!             GET {gcs_base}/_introspect/buckets                          ({buckets})
//!             GET {gcs_base}/_introspect/buckets/{b}                      (BucketSummary)
//!             GET {gcs_base}/_introspect/buckets/{b}/objects[?prefix]     ({objects})
//!             GET {gcs_base}/_introspect/buckets/{b}/objects/{name}       (ObjectSummary)
//!
//!   MUTATIONS -> the GCS JSON PROVIDER PROTOCOL (the legacy dashboard called the
//!             store directly; the network equivalent is the real GCS JSON API):
//!               POST   /storage/v1/b                  (CreateBucket)
//!               DELETE /storage/v1/b/{b}              (DeleteBucket)
//!               DELETE /storage/v1/b/{b}/o/{name}     (DeleteObject)
//!             The provider response is re-shaped back into the dashboard
//!             envelope (e.g. CreateBucket -> 201 + gcsBucketSummary).
//!
//!   CONTROL -> one mutation has no provider-protocol equivalent (the legacy handler
//!             used `os.RemoveAll` on the upload-session dir), so the GCS service
//!             exposes it under `/_control/`:
//!               DELETE /_control/uploads/{id}         (delete upload session)
//!
//! The `/api/gcs/*` response shapes are byte-identical to the legacy dashboard (the
//! source of truth): introspection read bodies are already the dashboard's
//! summary JSON, so reads relay them verbatim.

use serde_json::Value;

use crate::config::Config;
use crate::forward::{forward, ForwardError, ForwardRequest, ForwardResponse};
use crate::http::{path_segment_decode, Request, Response};

/// `GET /api/gcs/status` — config-only, mirrors `handleGCSStatus`.
pub async fn handle_status(config: &Config, req: &Request) -> Response {
    if req.method != "GET" {
        return Response::method_not_allowed("GET");
    }
    let running = !config.gcs_base.is_empty();
    Response::json(
        200,
        &serde_json::json!({
            "status": if running { "running" } else { "disabled" },
            "running": running,
            "endpoint": if config.gcs_endpoint.is_empty() {
                "http://127.0.0.1:14443".to_string()
            } else {
                config.gcs_endpoint.clone()
            },
            "project": "devcloud",
            "storagePath": if config.gcs_storage_path.is_empty() {
                ".devcloud/data/s3".to_string()
            } else {
                config.gcs_storage_path.clone()
            },
            "uploadSessionPath": ".devcloud/data/gcs/upload_sessions",
        }),
    )
}

/// `GET /api/gcs/uploads` (and `/api/gcs/upload-sessions`) — forwards to the GCS
/// `/_introspect/uploads` read API, whose `{sessions: [...]}` body is identical.
pub async fn handle_upload_sessions(config: &Config, req: &Request) -> Response {
    if req.method != "GET" {
        return Response::method_not_allowed("GET");
    }
    if config.gcs_base.is_empty() {
        return Response::text_error(503, "gcs service is disabled");
    }
    match get(config, "/_introspect/uploads").await {
        Ok(resp) => relay(resp),
        Err(e) => forward_failure(e),
    }
}

/// `/api/gcs/uploads/{id}` — DELETE forwards to `/_control/uploads/{id}`.
/// Mirrors `handleGCSUploadSession` (DELETE only; the legacy handler used
/// `os.RemoveAll`, now an out-of-process control endpoint). On success the legacy
/// handler returned 204 No Content.
pub async fn handle_upload_session(config: &Config, req: &Request) -> Response {
    if config.gcs_base.is_empty() {
        return Response::text_error(503, "gcs service is disabled");
    }
    let id = match req.raw_path.strip_prefix("/api/gcs/uploads/") {
        Some(seg) if !seg.is_empty() && !seg.contains('/') => seg,
        _ => return Response::text_error(404, "404 page not found"),
    };
    match req.method.as_str() {
        "DELETE" => match forward_provider(
            config,
            "DELETE",
            &format!("/_control/uploads/{id}"),
            Vec::new(),
            Vec::new(),
        )
        .await
        {
            Ok(resp) if (200..300).contains(&resp.status) => no_content(),
            Ok(resp) => relay(resp),
            Err(e) => forward_failure(e),
        },
        _ => Response::method_not_allowed("DELETE"),
    }
}

/// `/api/gcs/buckets` — GET lists buckets (introspect), POST creates a bucket
/// (provider protocol). Mirrors `handleGCSBuckets`.
pub async fn handle_buckets(config: &Config, req: &Request) -> Response {
    if config.gcs_base.is_empty() {
        return Response::text_error(503, "gcs service is disabled");
    }
    match req.method.as_str() {
        "GET" => match get(config, "/_introspect/buckets").await {
            Ok(resp) if resp.status == 200 => relay(resp),
            Ok(resp) => relay(resp),
            Err(e) => forward_failure(e),
        },
        "POST" => create_bucket(config, req).await,
        _ => Response::method_not_allowed("GET, POST"),
    }
}

/// `/api/gcs/buckets/...` — dispatches the bucket-detail, object-list,
/// object-detail, and object-media-download routes. Mirrors `handleGCSBucket`.
pub async fn handle_bucket(config: &Config, req: &Request) -> Response {
    if config.gcs_base.is_empty() {
        return Response::text_error(503, "gcs service is disabled");
    }

    // Parse off the ESCAPED path so percent-encoded bucket/object names
    // round-trip, exactly like the legacy handler's `r.URL.EscapedPath()` parsing.
    let rest = match req.raw_path.strip_prefix("/api/gcs/buckets/") {
        Some(r) => r,
        None => return Response::text_error(404, "404 page not found"),
    };
    let (escaped_bucket, suffix, has_suffix) = match rest.split_once('/') {
        Some((b, s)) => (b, s, true),
        None => (rest, "", false),
    };
    let bucket = match path_segment_decode(escaped_bucket) {
        Some(b) if !b.is_empty() => b,
        _ => return Response::text_error(400, "invalid bucket path"),
    };

    if !has_suffix {
        return bucket_detail(config, req, &bucket, escaped_bucket).await;
    }
    if suffix == "objects" {
        return objects(config, req, &bucket, escaped_bucket).await;
    }
    if let Some(object_path) = suffix.strip_prefix("objects/") {
        return object_or_download(config, req, &bucket, escaped_bucket, object_path).await;
    }
    Response::text_error(404, "404 page not found")
}

// ── reads ───────────────────────────────────────────────────────────────────

async fn bucket_detail(
    config: &Config,
    req: &Request,
    bucket: &str,
    escaped_bucket: &str,
) -> Response {
    match req.method.as_str() {
        "GET" => match get(
            config,
            &format!(
                "/_introspect/buckets/{}",
                encode_segment_str(escaped_bucket, bucket)
            ),
        )
        .await
        {
            Ok(resp) if resp.status == 200 => relay(resp),
            Ok(resp) if resp.status == 404 => Response::text_error(404, "404 page not found"),
            Ok(resp) => relay(resp),
            Err(e) => forward_failure(e),
        },
        "DELETE" => delete_bucket(config, bucket, escaped_bucket).await,
        _ => Response::method_not_allowed("GET, DELETE"),
    }
}

async fn objects(config: &Config, req: &Request, bucket: &str, escaped_bucket: &str) -> Response {
    if req.method != "GET" {
        return Response::method_not_allowed("GET");
    }
    let mut path = format!(
        "/_introspect/buckets/{}/objects",
        encode_segment_str(escaped_bucket, bucket)
    );
    // Forward the `prefix` query param the dashboard read from r.URL.Query().
    if let Some(prefix) = query_param(&req.query, "prefix") {
        path.push_str(&format!("?prefix={}", encode_query_value(&prefix)));
    }
    match get(config, &path).await {
        Ok(resp) if resp.status == 200 => relay(resp),
        Ok(resp) if resp.status == 404 => Response::text_error(404, "404 page not found"),
        Ok(resp) => relay(resp),
        Err(e) => forward_failure(e),
    }
}

async fn object_or_download(
    config: &Config,
    req: &Request,
    bucket: &str,
    escaped_bucket: &str,
    object_path: &str,
) -> Response {
    // `objects/{name}/download` -> media download (mirrors handleGCSObjectMediaDownload).
    if let Some(escaped_name) = object_path.strip_suffix("/download") {
        if req.method != "GET" {
            return Response::method_not_allowed("GET");
        }
        if escaped_name.is_empty() {
            return Response::text_error(404, "404 page not found");
        }
        // The download proxies object bytes; the GCS provider exposes them at
        // /download/storage/v1/b/{b}/o/{name}?alt=media.
        let name = match path_segment_decode(escaped_name) {
            Some(n) => n,
            None => return Response::text_error(400, "invalid object path"),
        };
        let path = format!(
            "/download/storage/v1/b/{}/o/{}?alt=media",
            encode_segment_str(escaped_bucket, bucket),
            encode_path_segment(&name),
        );
        return match get(config, &path).await {
            Ok(resp) if resp.status == 200 => relay(resp),
            Ok(resp) if resp.status == 404 => Response::text_error(404, "404 page not found"),
            Ok(resp) => relay(resp),
            Err(e) => forward_failure(e),
        };
    }

    let name = match path_segment_decode(object_path) {
        Some(n) if !n.is_empty() => n,
        _ => return Response::text_error(400, "invalid object path"),
    };
    match req.method.as_str() {
        "GET" => match get(
            config,
            &format!(
                "/_introspect/buckets/{}/objects/{}",
                encode_segment_str(escaped_bucket, bucket),
                encode_path_segment(&name),
            ),
        )
        .await
        {
            Ok(resp) if resp.status == 200 => relay(resp),
            Ok(resp) if resp.status == 404 => Response::text_error(404, "404 page not found"),
            Ok(resp) => relay(resp),
            Err(e) => forward_failure(e),
        },
        "DELETE" => delete_object(config, bucket, escaped_bucket, &name).await,
        _ => Response::method_not_allowed("GET, DELETE"),
    }
}

// ── mutations ─────────────────────────────────────────────────────────────

/// `POST /api/gcs/buckets` — decode `{name}`, POST to the provider `/storage/v1/b`,
/// then re-shape the bucketResource into a `gcsBucketSummary` with status 201
/// (matching handleGCSBuckets). A conflict (409) maps to "bucket already exists".
async fn create_bucket(config: &Config, req: &Request) -> Response {
    let body: Value = match serde_json::from_slice(&req.body) {
        Ok(v) => v,
        Err(_) => return Response::text_error(400, "invalid json request"),
    };
    let name = body.get("name").and_then(Value::as_str).unwrap_or("");
    let provider_body = serde_json::json!({ "name": name }).to_string();
    match forward_provider(
        config,
        "POST",
        "/storage/v1/b",
        vec![("Content-Type".to_string(), "application/json".to_string())],
        provider_body.into_bytes(),
    )
    .await
    {
        Ok(resp) if resp.status == 409 => Response::text_error(409, "bucket already exists"),
        Ok(resp) if (200..300).contains(&resp.status) => {
            let resource: Value = match serde_json::from_slice(&resp.body) {
                Ok(v) => v,
                Err(_) => {
                    return Response::text_error(502, "gcs service returned an invalid response")
                }
            };
            let created_name = resource
                .get("name")
                .and_then(Value::as_str)
                .unwrap_or(name)
                .to_string();
            // bucketResource.timeCreated is RFC3339Nano; gcsBucketSummary.TimeCreated
            // is a time.Time which serializes to RFC3339Nano too, so pass through.
            let time_created = resource
                .get("timeCreated")
                .cloned()
                .unwrap_or(Value::String(String::new()));
            Response::json(
                201,
                &serde_json::json!({
                    "name": created_name,
                    "timeCreated": time_created,
                    "objectCount": 0,
                    "gcsUri": format!("gs://{created_name}"),
                }),
            )
        }
        Ok(resp) => bad_request_from_provider(resp),
        Err(e) => forward_failure(e),
    }
}

/// `DELETE /api/gcs/buckets/{b}` — DELETE the provider bucket; 204 on success,
/// 404 when missing, 409 on a non-empty/error response (matches handleGCSBucketDetail).
async fn delete_bucket(config: &Config, bucket: &str, escaped_bucket: &str) -> Response {
    match forward_provider(
        config,
        "DELETE",
        &format!(
            "/storage/v1/b/{}",
            encode_segment_str(escaped_bucket, bucket)
        ),
        Vec::new(),
        Vec::new(),
    )
    .await
    {
        Ok(resp) if (200..300).contains(&resp.status) => no_content(),
        Ok(resp) if resp.status == 404 => Response::text_error(404, "404 page not found"),
        Ok(resp) => conflict_from_provider(resp),
        Err(e) => forward_failure(e),
    }
}

/// `DELETE /api/gcs/buckets/{b}/objects/{name}` — DELETE the provider object;
/// 204 on success, 404 when missing (matches handleGCSObjectDownload DELETE).
async fn delete_object(
    config: &Config,
    bucket: &str,
    escaped_bucket: &str,
    name: &str,
) -> Response {
    match forward_provider(
        config,
        "DELETE",
        &format!(
            "/storage/v1/b/{}/o/{}",
            encode_segment_str(escaped_bucket, bucket),
            encode_path_segment(name),
        ),
        Vec::new(),
        Vec::new(),
    )
    .await
    {
        Ok(resp) if (200..300).contains(&resp.status) => no_content(),
        Ok(resp) if resp.status == 404 => Response::text_error(404, "404 page not found"),
        Ok(resp) => bad_request_from_provider(resp),
        Err(e) => forward_failure(e),
    }
}

// ── forwarding helpers ──────────────────────────────────────────────────────

/// GET a path on the GCS service (introspection or provider read).
async fn get(config: &Config, path: &str) -> Result<ForwardResponse, ForwardError> {
    forward(ForwardRequest {
        base: &config.gcs_base,
        method: "GET",
        path,
        headers: Vec::new(),
        body: Vec::new(),
    })
    .await
}

/// Forwards an arbitrary method/path/body to the GCS service (provider protocol
/// or control endpoint).
async fn forward_provider(
    config: &Config,
    method: &str,
    path: &str,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
) -> Result<ForwardResponse, ForwardError> {
    forward(ForwardRequest {
        base: &config.gcs_base,
        method,
        path,
        headers,
        body,
    })
    .await
}

fn no_content() -> Response {
    Response {
        status: 204,
        headers: Vec::new(),
        body: Vec::new(),
    }
}

fn relay(resp: ForwardResponse) -> Response {
    // The legacy dashboard re-serializes every JSON API response with a bare
    // `application/json` Content-Type, whereas upstream services emit
    // `application/json; charset=utf-8`. Normalize any JSON content-type to the
    // bare form for byte-parity with the legacy dashboard; pass non-JSON (e.g. media
    // downloads) through unchanged.
    let content_type = {
        let ct = resp.header("content-type");
        if ct.is_empty() || ct.starts_with("application/json") {
            "application/json".to_string()
        } else {
            ct.to_string()
        }
    };
    Response::new(resp.status, &content_type, resp.body)
}

/// Maps an unexpected provider error response to a 400 (the legacy dashboard
/// surfaced store errors as http.StatusBadRequest with the error text).
fn bad_request_from_provider(resp: ForwardResponse) -> Response {
    Response::text_error(400, &provider_error_message(&resp.body))
}

/// Maps a provider error on bucket delete to a 409 (the legacy handler used
/// http.StatusConflict for delete errors).
fn conflict_from_provider(resp: ForwardResponse) -> Response {
    Response::text_error(409, &provider_error_message(&resp.body))
}

/// Extracts the GCS error message from a provider error body ({error:{message}}),
/// falling back to a generic string.
fn provider_error_message(body: &[u8]) -> String {
    serde_json::from_slice::<Value>(body)
        .ok()
        .and_then(|v| {
            v.get("error")
                .and_then(|e| e.get("message"))
                .and_then(Value::as_str)
                .map(str::to_string)
        })
        .unwrap_or_else(|| "gcs request failed".to_string())
}

fn forward_failure(err: ForwardError) -> Response {
    match err {
        ForwardError::Unreachable(_) => Response::text_error(502, "gcs service is unreachable"),
        ForwardError::BadBase => Response::text_error(500, "gcs service address is misconfigured"),
        ForwardError::BadResponse => {
            Response::text_error(502, "gcs service returned an invalid response")
        }
    }
}

/// Re-encodes a bucket path segment for an outbound URL. The bucket segment as
/// received is already escaped (`escaped_bucket`); we prefer it verbatim when it
/// is a clean single segment, otherwise re-encode the decoded value.
fn encode_segment_str(escaped: &str, decoded: &str) -> String {
    if escaped == encode_path_segment(decoded) {
        escaped.to_string()
    } else {
        encode_path_segment(decoded)
    }
}

/// Percent-encodes a single path segment outside the RFC 3986 unreserved set.
fn encode_path_segment(s: &str) -> String {
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

/// Percent-encodes a query parameter value (space and reserved chars).
fn encode_query_value(s: &str) -> String {
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

/// Reads a single query parameter from a raw query string, decoding `+` and
/// percent-escapes the same way the legacy dashboard's r.URL.Query().Get did.
fn query_param(query: &str, key: &str) -> Option<String> {
    for pair in query.split('&') {
        let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
        if k == key {
            return Some(decode_query_value(v));
        }
    }
    None
}

fn decode_query_value(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'+' => {
                out.push(b' ');
                i += 1;
            }
            b'%' if i + 2 < bytes.len() => {
                let hi = (bytes[i + 1] as char).to_digit(16);
                let lo = (bytes[i + 2] as char).to_digit(16);
                if let (Some(hi), Some(lo)) = (hi, lo) {
                    out.push((hi * 16 + lo) as u8);
                    i += 3;
                } else {
                    out.push(bytes[i]);
                    i += 1;
                }
            }
            b => {
                out.push(b);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn req(method: &str, path: &str) -> Request {
        Request {
            method: method.to_string(),
            path: path.to_string(),
            raw_path: path.to_string(),
            query: String::new(),
            headers: HashMap::new(),
            body: Vec::new(),
        }
    }

    #[tokio::test]
    async fn status_disabled_when_no_base() {
        let cfg = Config::default();
        let resp = handle_status(&cfg, &req("GET", "/api/gcs/status")).await;
        let v: serde_json::Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(v["status"], "disabled");
        assert_eq!(v["project"], "devcloud");
    }

    #[tokio::test]
    async fn buckets_disabled_returns_503() {
        let cfg = Config::default();
        let resp = handle_buckets(&cfg, &req("GET", "/api/gcs/buckets")).await;
        assert_eq!(resp.status, 503);
    }

    #[tokio::test]
    async fn buckets_rejects_unsupported_method() {
        let mut cfg = Config::default();
        cfg.gcs_base = "http://127.0.0.1:1".to_string();
        let resp = handle_buckets(&cfg, &req("PUT", "/api/gcs/buckets")).await;
        assert_eq!(resp.status, 405);
    }

    #[test]
    fn encode_path_segment_escapes_reserved() {
        assert_eq!(encode_path_segment("a b/c"), "a%20b%2Fc");
        assert_eq!(encode_path_segment("plain-name_1.txt"), "plain-name_1.txt");
    }

    #[test]
    fn query_param_decodes_value() {
        assert_eq!(
            query_param("prefix=logs%2F2024", "prefix").as_deref(),
            Some("logs/2024")
        );
        assert_eq!(query_param("other=x", "prefix"), None);
    }

    #[test]
    fn provider_error_message_extracts_nested() {
        let body = br#"{"error":{"code":409,"message":"bucket not empty"}}"#;
        assert_eq!(provider_error_message(body), "bucket not empty");
    }
}
