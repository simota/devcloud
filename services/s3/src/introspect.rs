//! Read-only introspection API, ported from `internal/services/s3/introspect.rs`.
//!
//! CONVENTION (reused verbatim across every service — see the legacy `introspect.rs`):
//!   - All introspection routes live under the "/_introspect/" prefix and are
//!     intercepted at the top of the service's HTTP router, BEFORE the
//!     provider-protocol dispatch (`parse_path_style`).
//!   - Methods are GET-only and read-only. No mutation endpoints here; the
//!     dashboard performs mutations through the S3 provider protocol.
//!   - Each response body reproduces, field-for-field and tag-for-tag, the
//!     summary structs the legacy dashboard serialized in-process from the
//!     BucketStore — so the Rust dashboard reconstructs byte-identical
//!     `/api/s3/*` envelopes.
//!   - A missing resource returns 404; an unsupported method returns 405.
//!   - Auth gating matches the rest of the server (`verify_signature` runs
//!     before this in `route_with_auth`), so relaxed mode stays open and
//!     strict mode is honored.

use serde::Serialize;

use crate::http::{Request, Response};
use crate::model::{MultipartUpload, Object};
use crate::store::{FileBucketStore, StoreError};

pub const INTROSPECT_PREFIX: &str = "/_introspect/";

// The summary structs below reproduce, field-for-field and tag-for-tag, the
// dashboard's `s3BucketSummary` / `s3ObjectSummary` / `s3MultipartUploadSummary`
// (internal/dashboard/s3_handlers.rs). serde serializes struct fields in
// declaration order, matching legacy struct marshaling exactly.

#[derive(Serialize)]
struct BucketSummary {
    name: String,
    #[serde(rename = "creationDate")]
    creation_date: String,
    #[serde(rename = "objectCount")]
    object_count: usize,
}

#[derive(Serialize)]
struct ObjectSummary {
    key: String,
    size: i64,
    etag: String,
    #[serde(rename = "contentType")]
    content_type: String,
    #[serde(rename = "lastModified")]
    last_modified: String,
    #[serde(rename = "metadata", skip_serializing_if = "Option::is_none")]
    metadata: Option<std::collections::BTreeMap<String, String>>,
    #[serde(rename = "s3Uri")]
    s3_uri: String,
    #[serde(rename = "downloadUrl")]
    download_url: String,
}

#[derive(Serialize)]
struct MultipartUploadSummary {
    key: String,
    #[serde(rename = "uploadId")]
    upload_id: String,
    initiated: String,
    #[serde(rename = "contentType")]
    content_type: String,
    #[serde(rename = "metadata", skip_serializing_if = "Option::is_none")]
    metadata: Option<std::collections::BTreeMap<String, String>>,
}

/// Reports whether the request targets the introspection API.
pub fn is_introspect_path(path: &str) -> bool {
    path.starts_with(INTROSPECT_PREFIX)
}

/// Serves the read-only introspection endpoints over the `FileBucketStore`:
///
/// ```text
/// GET /_introspect/buckets                          -> {"buckets": [bucketSummary...]}
/// GET /_introspect/buckets/{bucket}                 -> bucketSummary (detail)
/// GET /_introspect/buckets/{bucket}/objects?prefix  -> {"bucket","prefix","objects":[objectSummary...]}
/// GET /_introspect/buckets/{bucket}/objects/{key}   -> objectSummary (detail)
/// GET /_introspect/buckets/{bucket}/multipart       -> {"bucket","uploads":[uploadSummary...]}
/// ```
///
/// `req.path` is already percent-decoded by `parse_target`, so the bucket and
/// object key arrive here in their literal form — matching how the legacy handler
/// reconstructs them off the escaped path.
pub fn handle_introspect(store: &FileBucketStore, req: &Request) -> Response {
    if req.method != "GET" {
        let mut r = error_response(405, "introspection endpoints are read-only");
        r.headers.insert("Allow".to_string(), "GET".to_string());
        return r;
    }

    let rest = req
        .path
        .strip_prefix(INTROSPECT_PREFIX)
        .unwrap_or(&req.path);
    if rest == "buckets" {
        return introspect_buckets(store);
    }
    let Some(after) = rest.strip_prefix("buckets/") else {
        return error_response(404, "introspection endpoint not found");
    };

    let (bucket, suffix) = match after.split_once('/') {
        Some((b, s)) => (b, Some(s)),
        None => (after, None),
    };
    if bucket.is_empty() {
        return error_response(404, "introspection endpoint not found");
    }

    let Some(suffix) = suffix else {
        return introspect_bucket_detail(store, bucket);
    };
    if suffix == "objects" {
        introspect_objects(store, req, bucket)
    } else if let Some(key) = suffix.strip_prefix("objects/") {
        if key.is_empty() {
            return error_response(404, "introspection endpoint not found");
        }
        introspect_object_detail(store, bucket, key)
    } else if suffix == "multipart" {
        introspect_multipart(store, bucket)
    } else {
        error_response(404, "introspection endpoint not found")
    }
}

fn introspect_buckets(store: &FileBucketStore) -> Response {
    let buckets = match store.list_buckets() {
        Ok(buckets) => buckets,
        Err(err) => return internal_error(&err),
    };
    let mut summaries = Vec::with_capacity(buckets.len());
    for bucket in buckets {
        let object_count = match store.list_objects(&bucket.name, "") {
            Ok(Some(objects)) => objects.len(),
            // A bucket present in `list_buckets` cannot be absent here; treat the
            // empty/None case as zero objects rather than fabricating an error.
            Ok(None) => 0,
            Err(err) => return internal_error(&err),
        };
        summaries.push(BucketSummary {
            name: bucket.name,
            creation_date: bucket.created_at,
            object_count,
        });
    }
    json_response(200, &serde_json::json!({ "buckets": summaries }))
}

fn introspect_bucket_detail(store: &FileBucketStore, bucket: &str) -> Response {
    let item = match store.get_bucket(bucket) {
        Ok(Some(item)) => item,
        Ok(None) => return error_response(404, "bucket does not exist"),
        Err(err) => return internal_error(&err),
    };
    let object_count = match store.list_objects(bucket, "") {
        Ok(Some(objects)) => objects.len(),
        Ok(None) => return error_response(404, "bucket does not exist"),
        Err(err) => return internal_error(&err),
    };
    json_response(
        200,
        &BucketSummary {
            name: item.name,
            creation_date: item.created_at,
            object_count,
        },
    )
}

fn introspect_objects(store: &FileBucketStore, req: &Request, bucket: &str) -> Response {
    let prefix = req.query.get("prefix").map(String::as_str).unwrap_or("");
    let objects = match store.list_objects(bucket, prefix) {
        Ok(Some(objects)) => objects,
        Ok(None) => return error_response(404, "bucket does not exist"),
        Err(err) => return internal_error(&err),
    };
    let summaries: Vec<ObjectSummary> = objects
        .into_iter()
        .map(|object| object_summary(bucket, object))
        .collect();
    json_response(
        200,
        &serde_json::json!({
            "bucket": bucket,
            "prefix": prefix,
            "objects": summaries,
        }),
    )
}

fn introspect_object_detail(store: &FileBucketStore, bucket: &str, key: &str) -> Response {
    // legacy parity: `GetObject` returns an *error* ("bucket does not exist") when
    // the bucket is absent, which the legacy handler surfaces as 500 — the error
    // branch is checked before the not-found branch. Only a missing *object*
    // (found=false) yields 404 "object does not exist".
    match store.get_object(bucket, key) {
        Ok(Some((object, _body))) => json_response(200, &object_summary(bucket, object)),
        Ok(None) => error_response(404, "object does not exist"),
        Err(StoreError::BucketNotExist) => error_response(500, "bucket does not exist"),
        Err(err) => internal_error(&err),
    }
}

fn introspect_multipart(store: &FileBucketStore, bucket: &str) -> Response {
    let uploads = match store.list_multipart_uploads(bucket) {
        Ok(Some(uploads)) => uploads,
        Ok(None) => return error_response(404, "bucket does not exist"),
        Err(err) => return internal_error(&err),
    };
    let summaries: Vec<MultipartUploadSummary> =
        uploads.into_iter().map(multipart_upload_summary).collect();
    json_response(
        200,
        &serde_json::json!({
            "bucket": bucket,
            "uploads": summaries,
        }),
    )
}

/// Mirrors the dashboard's `s3ObjectResponse`: same `s3Uri` / `downloadUrl`
/// construction, so the Rust dashboard can re-wrap verbatim. Object body
/// content is never included — only read-only metadata.
fn object_summary(bucket: &str, object: Object) -> ObjectSummary {
    ObjectSummary {
        key: object.key.clone(),
        size: object.size,
        etag: object.etag,
        content_type: object.content_type,
        last_modified: object.last_modified,
        metadata: if object.metadata.is_empty() {
            None
        } else {
            Some(object.metadata)
        },
        s3_uri: format!("s3://{bucket}/{}", object.key),
        download_url: format!(
            "/api/s3/buckets/{}/objects/{}/download",
            path_escape(bucket),
            path_escape(&object.key)
        ),
    }
}

fn multipart_upload_summary(upload: MultipartUpload) -> MultipartUploadSummary {
    MultipartUploadSummary {
        key: upload.key,
        upload_id: upload.upload_id,
        initiated: upload.created_at,
        content_type: upload.content_type,
        metadata: if upload.metadata.is_empty() {
            None
        } else {
            Some(upload.metadata)
        },
    }
}

/// Faithful port of legacy `url.PathEscape` (encodePathSegment mode): keeps
/// unreserved bytes `A-Za-z0-9-_.~` plus the segment-permitted sub-delims
/// `$ & + ! * ' ( ) = : @`; escapes everything else (including `/ ; , ?` and
/// space) as uppercase `%XX`.
fn path_escape(value: &str) -> String {
    use std::fmt::Write;
    let mut out = String::with_capacity(value.len());
    for &c in value.as_bytes() {
        let ch = c as char;
        if ch.is_ascii_alphanumeric()
            || matches!(
                ch,
                '-' | '_'
                    | '.'
                    | '~'
                    | '$'
                    | '&'
                    | '+'
                    | '!'
                    | '*'
                    | '\''
                    | '('
                    | ')'
                    | '='
                    | ':'
                    | '@'
            )
        {
            out.push(ch);
        } else {
            let _ = write!(out, "%{c:02X}");
        }
    }
    out
}

fn json_response<T: Serialize>(status: u16, value: &T) -> Response {
    let body = serde_json::to_vec(value).unwrap_or_else(|_| b"{}".to_vec());
    let mut r = Response::with_body(status, body);
    r.headers
        .insert("Content-Type".to_string(), "application/json".to_string());
    r
}

fn error_response(status: u16, message: &str) -> Response {
    json_response(status, &serde_json::json!({ "error": message }))
}

fn internal_error(err: &StoreError) -> Response {
    error_response(500, &store_error_message(err))
}

/// Renders a `StoreError` into the read-only 500 body message. The introspect
/// API only reaches this path on unexpected I/O failures; the dashboard never
/// exercises it in the parity goldens.
fn store_error_message(err: &StoreError) -> String {
    match err {
        StoreError::Io(e) => e.to_string(),
        other => format!("{other:?}"),
    }
}
