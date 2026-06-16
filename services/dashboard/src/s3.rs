//! S3 dashboard handler — ports `internal/dashboard/s3_handlers.rs` to
//! out-of-process forwarding.
//!
//! ── HOW THE GO DASHBOARD WORKED (and why this differs from the SQS template) ──
//! The legacy S3 dashboard's `s.s3` field was an in-process `s3svc.BucketStore` (the
//! shared on-disk object store), NOT an `*s3.Server`. So its handlers called
//! typed store methods directly (`ListBuckets`, `ListObjects`, `GetBucket`,
//! `GetObject`, `PutObject`, `DeleteObject`, …) and reshaped the results into
//! the dashboard JSON envelopes (`s3BucketSummary` / `s3ObjectSummary` /
//! `s3MultipartUploadSummary`).
//!
//! The Rust dashboard runs out-of-process, so it reaches the S3 service over the
//! network using two surfaces — exactly the legacy behavior, split by read/mutate:
//!
//!   READS  -> the S3 service's read-only introspection API (added in this
//!             increment; see `internal/services/s3/introspect.rs`):
//!               GET {s3_base}/_introspect/buckets
//!               GET {s3_base}/_introspect/buckets/{bucket}
//!               GET {s3_base}/_introspect/buckets/{bucket}/objects?prefix=
//!               GET {s3_base}/_introspect/buckets/{bucket}/objects/{key}
//!               GET {s3_base}/_introspect/buckets/{bucket}/multipart
//!             These serialize the SAME summary structs the legacy dashboard built,
//!             so the read envelopes are byte-identical.
//!
//!   MUTATIONS -> mostly the S3 PROVIDER protocol (path-style REST); the
//!             BucketStore mutations map 1:1 to AWS S3 actions:
//!               POST   /api/s3/buckets                       -> PUT    /{bucket}
//!               DELETE /api/s3/buckets/{bucket}              -> DELETE /{bucket}
//!               PUT    /api/s3/.../objects/{key}             -> PUT    /{bucket}/{key}
//!               DELETE /api/s3/.../multipart/{uploadId}      -> DELETE /{bucket}/{key}?uploadId=
//!             EXCEPTION — object delete goes through the S3 service's
//!             `/_control/` namespace, NOT the provider DELETE:
//!               DELETE /api/s3/.../objects/{key}
//!                   -> DELETE /_control/buckets/{bucket}/objects/{key}
//!             because the provider DELETE is idempotent (always 2xx, even when
//!             the object is absent), but the legacy dashboard returned 404 for an
//!             absent object. The control endpoint reproduces the store's
//!             404/204 so the dashboard delete stays byte/status-identical.
//!             For mutations whose dashboard response is a reshaped JSON envelope
//!             (create-bucket, put-object), we run the provider mutation and then
//!             re-read via introspection to emit the byte-identical envelope —
//!             mirroring how the legacy handler returned the store result.
//!
//! The `x-amz-copy-source` / `x-amz-metadata-directive: REPLACE` semantics for
//! object PUT are implemented by the S3 provider itself, so we forward those
//! headers straight through.

use serde_json::Value;

use crate::config::Config;
use crate::forward::{forward, ForwardError, ForwardRequest, ForwardResponse};
use crate::http::{path_segment_decode, Request, Response};

/// `GET /api/s3/status` — config-only, mirrors `handleS3Status`.
pub async fn handle_status(config: &Config, req: &Request) -> Response {
    if req.method != "GET" {
        return Response::method_not_allowed("GET");
    }
    let running = !config.s3_base.is_empty();
    Response::json(
        200,
        &serde_json::json!({
            "status": if running { "running" } else { "disabled" },
            "running": running,
            "endpoint": if config.s3_endpoint.is_empty() {
                "http://127.0.0.1:14566".to_string()
            } else {
                config.s3_endpoint.clone()
            },
            "region": "us-east-1",
            "authMode": "relaxed",
            "storagePath": if config.s3_storage_path.is_empty() {
                ".devcloud/data/s3".to_string()
            } else {
                config.s3_storage_path.clone()
            },
        }),
    )
}

/// `/api/s3/buckets`
///   GET  -> `{"buckets": [...]}` from `/_introspect/buckets`.
///   POST -> CreateBucket via provider `PUT /{name}`, then re-read the bucket
///           detail and emit `{name, creationDate}` (matching legacy POST shape:
///           no objectCount).
pub async fn handle_buckets(config: &Config, req: &Request) -> Response {
    if req.method != "GET" && req.method != "POST" {
        return Response::method_not_allowed("GET, POST");
    }
    if config.s3_base.is_empty() {
        return Response::text_error(503, "s3 service is disabled");
    }
    if req.method == "POST" {
        return create_bucket(config, req).await;
    }

    match introspect(config, "/_introspect/buckets").await {
        Ok(resp) if resp.status == 200 => relay_introspect_json(resp),
        Ok(resp) => relay_introspect_error(resp),
        Err(e) => forward_failure(e),
    }
}

/// `/api/s3/buckets/{bucket}` and its sub-resources — mirrors `handleS3Bucket`'s
/// path routing.
pub async fn handle_bucket(config: &Config, req: &Request) -> Response {
    if config.s3_base.is_empty() {
        return Response::text_error(503, "s3 service is disabled");
    }

    // Parse off the ESCAPED path exactly as legacy handleS3Bucket cut the suffix,
    // so a literal "%2F" inside an object key round-trips as part of the key.
    let suffix = match req.raw_path.strip_prefix("/api/s3/buckets/") {
        Some(s) => s,
        None => return Response::text_error(404, "404 page not found"),
    };
    let (escaped_bucket, rest, has_rest) = match suffix.split_once('/') {
        Some((b, r)) => (b, r, true),
        None => (suffix, "", false),
    };
    let bucket = match path_segment_decode(escaped_bucket) {
        Some(b) if !b.is_empty() => b,
        _ => return Response::text_error(404, "404 page not found"),
    };

    if !has_rest {
        return bucket_detail_or_delete(config, req, &bucket).await;
    }
    if rest == "objects" {
        return list_objects(config, req, &bucket).await;
    }
    if let Some(object_path) = rest.strip_prefix("objects/") {
        return object_route(config, req, &bucket, object_path).await;
    }
    if rest == "multipart" {
        return list_multipart(config, req, &bucket).await;
    }
    if let Some(upload_id) = rest.strip_prefix("multipart/") {
        return abort_multipart(config, req, &bucket, upload_id).await;
    }
    Response::text_error(404, "404 page not found")
}

// ── bucket detail / delete ──────────────────────────────────────────────────

async fn bucket_detail_or_delete(config: &Config, req: &Request, bucket: &str) -> Response {
    match req.method.as_str() {
        "GET" => match introspect(
            config,
            &format!("/_introspect/buckets/{}", encode_segment(bucket)),
        )
        .await
        {
            Ok(resp) if resp.status == 200 => relay_introspect_json(resp),
            Ok(resp) if resp.status == 404 => Response::text_error(404, "404 page not found"),
            Ok(resp) => relay_introspect_error(resp),
            Err(e) => forward_failure(e),
        },
        "DELETE" => {
            // Provider DELETE /{bucket}. 204 -> 204; 404 -> 404; 409 -> 409.
            match forward_provider(
                config,
                "DELETE",
                &format!("/{}", encode_path(bucket)),
                Vec::new(),
                Vec::new(),
            )
            .await
            {
                Ok(resp) if resp.status == 204 => no_content(),
                Ok(resp) if resp.status == 404 => Response::text_error(404, "404 page not found"),
                Ok(resp) if resp.status == 409 => Response::text_error(409, "bucket is not empty"),
                Ok(resp) => relay_provider_error(resp),
                Err(e) => forward_failure(e),
            }
        }
        _ => Response::method_not_allowed("GET, DELETE"),
    }
}

async fn create_bucket(config: &Config, req: &Request) -> Response {
    let envelope: Value = match serde_json::from_slice(&req.body) {
        Ok(v) => v,
        Err(_) => return Response::text_error(400, "invalid json request"),
    };
    let name = envelope.get("name").and_then(Value::as_str).unwrap_or("");
    if name.is_empty() {
        return Response::text_error(400, "invalid bucket name");
    }

    let created = match forward_provider(
        config,
        "PUT",
        &format!("/{}", encode_path(name)),
        Vec::new(),
        Vec::new(),
    )
    .await
    {
        Ok(resp) if (200..300).contains(&resp.status) => true,
        Ok(resp) if resp.status == 409 => false, // BucketAlreadyOwnedByYou
        Ok(resp) => return relay_provider_error(resp),
        Err(e) => return forward_failure(e),
    };

    // Re-read the bucket to source CreationDate. legacy POST response is
    // {name, creationDate} only (objectCount omitted via a separate struct
    // literal), so we project away objectCount.
    let detail = match introspect(
        config,
        &format!("/_introspect/buckets/{}", encode_segment(name)),
    )
    .await
    {
        Ok(resp) if resp.status == 200 => match serde_json::from_slice::<Value>(&resp.body) {
            Ok(v) => v,
            Err(_) => return Response::text_error(502, "s3 introspection returned invalid json"),
        },
        Ok(resp) => return relay_introspect_error(resp),
        Err(e) => return forward_failure(e),
    };
    let body = serde_json::json!({
        "name": detail.get("name").cloned().unwrap_or(Value::String(name.to_string())),
        "creationDate": detail.get("creationDate").cloned().unwrap_or(Value::Null),
    });
    let status = if created { 201 } else { 200 };
    Response::json(status, &body)
}

// ── object list / detail / put / delete / download ──────────────────────────

async fn list_objects(config: &Config, req: &Request, bucket: &str) -> Response {
    if req.method != "GET" {
        return Response::method_not_allowed("GET");
    }
    let prefix = query_param(&req.query, "prefix");
    let path = if prefix.is_empty() {
        format!("/_introspect/buckets/{}/objects", encode_segment(bucket))
    } else {
        format!(
            "/_introspect/buckets/{}/objects?prefix={}",
            encode_segment(bucket),
            encode_query_value(&prefix)
        )
    };
    match introspect(config, &path).await {
        Ok(resp) if resp.status == 200 => relay_introspect_json(resp),
        Ok(resp) if resp.status == 404 => Response::text_error(404, "404 page not found"),
        Ok(resp) => relay_introspect_error(resp),
        Err(e) => forward_failure(e),
    }
}

async fn object_route(config: &Config, req: &Request, bucket: &str, object_path: &str) -> Response {
    // Download path: ".../objects/{key}/download".
    if let Some(escaped_key) = object_path.strip_suffix("/download") {
        if escaped_key.is_empty() {
            return Response::text_error(404, "404 page not found");
        }
        if req.method != "GET" {
            return Response::method_not_allowed("GET, PUT, DELETE");
        }
        return download_object(config, bucket, escaped_key).await;
    }
    if object_path.is_empty() {
        return Response::text_error(404, "404 page not found");
    }
    match req.method.as_str() {
        "PUT" => put_object(config, req, bucket, object_path).await,
        "DELETE" => delete_object(config, bucket, object_path).await,
        "GET" => Response::text_error(404, "404 page not found"),
        _ => Response::method_not_allowed("GET, PUT, DELETE"),
    }
}

async fn put_object(config: &Config, req: &Request, bucket: &str, escaped_key: &str) -> Response {
    let key = match decode_object_key(escaped_key) {
        Some(k) if !k.is_empty() => k,
        _ => return Response::text_error(400, "invalid object path"),
    };

    // Forward the dashboard PUT body + content/metadata headers straight to the
    // provider PUT /{bucket}/{key}. copy-source / metadata-directive are handled
    // by the provider, so we relay those headers untouched.
    let headers = object_put_headers(req);
    let provider_path = format!("/{}/{}", encode_path(bucket), encode_path(&key));
    match forward_provider(config, "PUT", &provider_path, headers, req.body.clone()).await {
        Ok(resp) if (200..300).contains(&resp.status) => {
            // Re-read object detail to build the s3ObjectSummary envelope.
            object_detail_envelope(config, resp, bucket, &key).await
        }
        Ok(resp) if resp.status == 404 => Response::text_error(404, "404 page not found"),
        Ok(resp) => relay_provider_error(resp),
        Err(e) => forward_failure(e),
    }
}

async fn object_detail_envelope(
    config: &Config,
    put_resp: ForwardResponse,
    bucket: &str,
    key: &str,
) -> Response {
    let etag = put_resp.header("etag").to_string();
    match introspect(
        config,
        &format!(
            "/_introspect/buckets/{}/objects/{}",
            encode_segment(bucket),
            encode_segment(key)
        ),
    )
    .await
    {
        Ok(resp) if resp.status == 200 => {
            let summary: Value = match serde_json::from_slice(&resp.body) {
                Ok(v) => v,
                Err(_) => {
                    return Response::text_error(502, "s3 introspection returned invalid json")
                }
            };
            let mut out = Response::json(200, &summary);
            if !etag.is_empty() {
                out.headers.push(("ETag".to_string(), etag));
            }
            out
        }
        Ok(resp) => relay_introspect_error(resp),
        Err(e) => forward_failure(e),
    }
}

async fn delete_object(config: &Config, bucket: &str, escaped_key: &str) -> Response {
    let key = match decode_object_key(escaped_key) {
        Some(k) if !k.is_empty() => k,
        _ => return Response::text_error(400, "invalid object path"),
    };
    // Forward to the S3 service's `/_control/` object-delete (NOT the provider
    // DELETE). The provider DELETE is idempotent (always 2xx, even for an absent
    // object), but the legacy dashboard returned 404 when the store reported "not
    // deleted". The control endpoint reproduces that store call and its 404/204
    // status, so we relay it verbatim. Both bucket and key are encoded as single
    // path segments (a literal "/" inside the key becomes "%2F") so they
    // round-trip through the control endpoint's per-segment PathUnescape.
    let control_path = format!(
        "/_control/buckets/{}/objects/{}",
        encode_segment(bucket),
        encode_segment(&key)
    );
    match forward_provider(config, "DELETE", &control_path, Vec::new(), Vec::new()).await {
        Ok(resp) if resp.status == 204 => no_content(),
        Ok(resp) if resp.status == 404 => Response::text_error(404, "404 page not found"),
        Ok(resp) => relay_introspect_error(resp),
        Err(e) => forward_failure(e),
    }
}

/// Download: forward a provider object GET and rebuild the EXACT dashboard
/// download header set (mirrors legacy `handleS3ObjectDownload`), which differs from
/// the provider GET headers: it defaults Content-Type to application/octet-stream
/// and synthesizes a `Content-Disposition: attachment; filename="..."` when the
/// object has none, and does NOT emit Accept-Ranges / version headers.
async fn download_object(config: &Config, bucket: &str, escaped_key: &str) -> Response {
    let key = match decode_object_key(escaped_key) {
        Some(k) if !k.is_empty() => k,
        _ => return Response::text_error(400, "invalid object path"),
    };
    let provider_path = format!("/{}/{}", encode_path(bucket), encode_path(&key));
    let resp = match forward_provider(config, "GET", &provider_path, Vec::new(), Vec::new()).await {
        Ok(resp) if resp.status == 200 => resp,
        Ok(resp) if resp.status == 404 => return Response::text_error(404, "404 page not found"),
        Ok(resp) => return relay_provider_error(resp),
        Err(e) => return forward_failure(e),
    };

    let mut content_type = resp.header("content-type").to_string();
    if content_type.is_empty() {
        content_type = "application/octet-stream".to_string();
    }
    let mut headers: Vec<(String, String)> = vec![
        ("Content-Type".to_string(), content_type),
        ("Content-Length".to_string(), resp.body.len().to_string()),
    ];
    let etag = resp.header("etag");
    if !etag.is_empty() {
        headers.push(("ETag".to_string(), etag.to_string()));
    }
    let last_modified = resp.header("last-modified");
    if !last_modified.is_empty() {
        headers.push(("Last-Modified".to_string(), last_modified.to_string()));
    }
    let disposition = resp.header("content-disposition");
    if disposition.is_empty() {
        headers.push((
            "Content-Disposition".to_string(),
            format!("attachment; filename=\"{}\"", download_filename(&key)),
        ));
    } else {
        headers.push(("Content-Disposition".to_string(), disposition.to_string()));
    }
    // Relay any x-amz-meta-* headers verbatim.
    for (name, value) in resp.headers.iter() {
        if name.starts_with("x-amz-meta-") {
            headers.push((name.clone(), value.clone()));
        }
    }
    Response {
        status: 200,
        headers,
        body: resp.body,
    }
}

// ── multipart ───────────────────────────────────────────────────────────────

async fn list_multipart(config: &Config, req: &Request, bucket: &str) -> Response {
    if req.method != "GET" {
        return Response::method_not_allowed("GET");
    }
    match introspect(
        config,
        &format!("/_introspect/buckets/{}/multipart", encode_segment(bucket)),
    )
    .await
    {
        Ok(resp) if resp.status == 200 => relay_introspect_json(resp),
        Ok(resp) if resp.status == 404 => Response::text_error(404, "404 page not found"),
        Ok(resp) => relay_introspect_error(resp),
        Err(e) => forward_failure(e),
    }
}

/// Abort: the dashboard accepts only an uploadId, looks up the matching upload's
/// key, then aborts. The provider AbortMultipartUpload needs the key, so we list
/// uploads via introspection, resolve the key, then DELETE
/// `/{bucket}/{key}?uploadId=`.
async fn abort_multipart(
    config: &Config,
    req: &Request,
    bucket: &str,
    escaped_upload: &str,
) -> Response {
    if req.method != "DELETE" {
        return Response::method_not_allowed("DELETE");
    }
    let upload_id = match path_segment_decode(escaped_upload) {
        Some(u) if !u.is_empty() => u,
        _ => return Response::text_error(400, "invalid upload id"),
    };

    let uploads = match introspect(
        config,
        &format!("/_introspect/buckets/{}/multipart", encode_segment(bucket)),
    )
    .await
    {
        Ok(resp) if resp.status == 200 => match serde_json::from_slice::<Value>(&resp.body) {
            Ok(v) => v,
            Err(_) => return Response::text_error(502, "s3 introspection returned invalid json"),
        },
        Ok(resp) if resp.status == 404 => return Response::text_error(404, "404 page not found"),
        Ok(resp) => return relay_introspect_error(resp),
        Err(e) => return forward_failure(e),
    };

    let key = uploads
        .get("uploads")
        .and_then(Value::as_array)
        .and_then(|list| {
            list.iter().find_map(|u| {
                let id = u.get("uploadId").and_then(Value::as_str)?;
                if id == upload_id {
                    u.get("key").and_then(Value::as_str).map(str::to_string)
                } else {
                    None
                }
            })
        });
    let key = match key {
        Some(k) => k,
        None => return Response::text_error(404, "404 page not found"),
    };

    let provider_path = format!(
        "/{}/{}?uploadId={}",
        encode_path(bucket),
        encode_path(&key),
        encode_query_value(&upload_id)
    );
    match forward_provider(config, "DELETE", &provider_path, Vec::new(), Vec::new()).await {
        Ok(resp) if (200..300).contains(&resp.status) => no_content(),
        Ok(resp) if resp.status == 404 => Response::text_error(404, "404 page not found"),
        Ok(resp) => relay_provider_error(resp),
        Err(e) => forward_failure(e),
    }
}

// ── forwarding helpers ──────────────────────────────────────────────────────

/// GET a path on the S3 service (introspection read).
async fn introspect(config: &Config, path: &str) -> Result<ForwardResponse, ForwardError> {
    forward(ForwardRequest {
        base: &config.s3_base,
        method: "GET",
        path,
        headers: Vec::new(),
        body: Vec::new(),
    })
    .await
}

/// Forward a request to the S3 provider protocol (path-style REST).
async fn forward_provider(
    config: &Config,
    method: &str,
    path: &str,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
) -> Result<ForwardResponse, ForwardError> {
    forward(ForwardRequest {
        base: &config.s3_base,
        method,
        path,
        headers,
        body,
    })
    .await
}

/// Headers to forward on an object PUT: content metadata + user metadata
/// (x-amz-meta-*) + copy-source / metadata-directive. The provider applies them.
fn object_put_headers(req: &Request) -> Vec<(String, String)> {
    const PASSTHROUGH: &[&str] = &[
        "content-type",
        "content-encoding",
        "cache-control",
        "content-disposition",
        "x-amz-copy-source",
        "x-amz-metadata-directive",
    ];
    let mut out = Vec::new();
    for (name, value) in req.headers.iter() {
        let lower = name.to_ascii_lowercase();
        if PASSTHROUGH.contains(&lower.as_str()) || lower.starts_with("x-amz-meta-") {
            out.push((name.clone(), value.clone()));
        }
    }
    out
}

/// Relays a 200 introspection JSON body verbatim (envelopes are byte-identical
/// to the legacy dashboard's, so no re-wrap is needed).
fn relay_introspect_json(resp: ForwardResponse) -> Response {
    Response::new(200, "application/json", resp.body)
}

/// Maps an unexpected (non-2xx/404) introspection response into the dashboard's
/// 500 error shape, matching how the legacy handler surfaced store errors.
fn relay_introspect_error(resp: ForwardResponse) -> Response {
    let msg = error_message(&resp).unwrap_or_else(|| "s3 introspection failed".to_string());
    Response::text_error(if resp.status >= 400 { resp.status } else { 500 }, &msg)
}

/// Maps a provider-protocol error (S3 XML) into the dashboard's text error,
/// preserving the upstream status.
fn relay_provider_error(resp: ForwardResponse) -> Response {
    let status = if resp.status >= 400 { resp.status } else { 400 };
    Response::text_error(status, "s3 operation failed")
}

fn error_message(resp: &ForwardResponse) -> Option<String> {
    let v: Value = serde_json::from_slice(&resp.body).ok()?;
    v.get("error").and_then(Value::as_str).map(str::to_string)
}

fn forward_failure(err: ForwardError) -> Response {
    match err {
        ForwardError::Unreachable(_) => Response::text_error(502, "s3 service is unreachable"),
        ForwardError::BadBase => Response::text_error(500, "s3 service address is misconfigured"),
        ForwardError::BadResponse => {
            Response::text_error(502, "s3 service returned an invalid response")
        }
    }
}

fn no_content() -> Response {
    Response {
        status: 204,
        headers: Vec::new(),
        body: Vec::new(),
    }
}

// ── small utilities ─────────────────────────────────────────────────────────

/// Extracts a single query parameter value (decoded) from a raw query string.
fn query_param(query: &str, key: &str) -> String {
    for pair in query.split('&') {
        if let Some((k, v)) = pair.split_once('=') {
            if k == key {
                return percent_decode_value(v);
            }
        } else if pair == key {
            return String::new();
        }
    }
    String::new()
}

fn percent_decode_value(s: &str) -> String {
    let bytes = s.replace('+', " ");
    let mut out = Vec::with_capacity(bytes.len());
    let raw = bytes.as_bytes();
    let mut i = 0;
    while i < raw.len() {
        if raw[i] == b'%' && i + 2 < raw.len() {
            if let (Some(h), Some(l)) = (hex_val(raw[i + 1]), hex_val(raw[i + 2])) {
                out.push(h << 4 | l);
                i += 3;
                continue;
            }
        }
        out.push(raw[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn hex_val(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

/// Decodes an object key from the escaped URL suffix, mirroring legacy
/// `url.PathUnescape` on the object path: percent-escapes are decoded but a
/// LITERAL "/" stays a separator inside the key (path-style keys like
/// "docs/a.txt"). A "%2F" in the original key decodes to a literal "/" — exactly
/// as the legacy dashboard handled it. Traversal segments are rejected.
fn decode_object_key(escaped: &str) -> Option<String> {
    let decoded = percent_decode_path(escaped);
    for segment in decoded.split('/') {
        if segment == "." || segment == ".." {
            return None;
        }
    }
    if decoded.contains('\\') {
        return None;
    }
    Some(decoded)
}

/// Path-style percent decode: decodes `%XX` escapes but, unlike a query decode,
/// does NOT treat `+` as a space (matches legacy `url.PathUnescape`).
fn percent_decode_path(s: &str) -> String {
    let raw = s.as_bytes();
    let mut out = Vec::with_capacity(raw.len());
    let mut i = 0;
    while i < raw.len() {
        if raw[i] == b'%' && i + 2 < raw.len() {
            if let (Some(h), Some(l)) = (hex_val(raw[i + 1]), hex_val(raw[i + 2])) {
                out.push(h << 4 | l);
                i += 3;
                continue;
            }
        }
        out.push(raw[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// Percent-encodes a single path segment for an introspection URL (RFC 3986
/// unreserved set kept literal). Used where the upstream parses ONE segment.
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

/// Percent-encodes a path that MAY contain "/" separators that should remain
/// literal slashes (object keys in the provider path). Encodes everything except
/// the unreserved set and "/".
fn encode_path(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' | b'/' => {
                out.push(b as char)
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

fn encode_query_value(s: &str) -> String {
    encode_segment(s)
}

/// Mirrors legacy `downloadFilename`: the last path segment of the key.
fn download_filename(key: &str) -> String {
    match key.rsplit_once('/') {
        Some((_, last)) if !last.is_empty() => last.to_string(),
        _ => key.to_string(),
    }
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
        let resp = handle_status(&cfg, &req("GET", "/api/s3/status")).await;
        assert_eq!(resp.status, 200);
        let v: serde_json::Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(v["status"], "disabled");
        assert_eq!(v["region"], "us-east-1");
    }

    #[tokio::test]
    async fn status_running_when_base_set() {
        let mut cfg = Config::default();
        cfg.s3_base = "http://127.0.0.1:14566".to_string();
        let resp = handle_status(&cfg, &req("GET", "/api/s3/status")).await;
        let v: serde_json::Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(v["status"], "running");
        assert_eq!(v["running"], true);
    }

    #[tokio::test]
    async fn buckets_disabled_503() {
        let cfg = Config::default();
        let resp = handle_buckets(&cfg, &req("GET", "/api/s3/buckets")).await;
        assert_eq!(resp.status, 503);
    }

    #[test]
    fn query_param_decodes() {
        assert_eq!(query_param("prefix=docs%2F", "prefix"), "docs/");
        assert_eq!(query_param("a=1&prefix=x&b=2", "prefix"), "x");
        assert_eq!(query_param("other=1", "prefix"), "");
    }

    #[test]
    fn encode_path_keeps_slashes() {
        assert_eq!(encode_path("docs/read me.txt"), "docs/read%20me.txt");
        assert_eq!(encode_path("docs/read%2Fme.txt"), "docs/read%252Fme.txt");
    }

    #[test]
    fn encode_segment_escapes_slashes() {
        assert_eq!(encode_segment("docs/read me.txt"), "docs%2Fread%20me.txt");
    }

    #[test]
    fn download_filename_takes_last_segment() {
        assert_eq!(download_filename("docs/readme.txt"), "readme.txt");
        assert_eq!(download_filename("readme.txt"), "readme.txt");
        assert_eq!(download_filename("docs/read%2Fme.txt"), "read%2Fme.txt");
    }
}
