//! GCS JSON API surface: routing, handlers, and the hand-rolled HTTP layer.
//!
//! Behavior-parity port of `internal/services/gcs` (the legacy implementation is
//! the oracle, quirks included):
//!
//! - JSON responses go through legacy `json.Encoder` wire format (compact, HTML
//!   escaping, trailing newline) via `devcloud_s3::wire_json`.
//! - `session.json` resumable-upload state is byte-compatible with legacy
//!   `json.MarshalIndent` of `resumableSession` (legacy field names, nested
//!   `Preconditions`, `null` metadata, RFC3339Nano `CreatedAt`).
//! - Error messages mirror the legacy handlers exactly, including store error
//!   strings (`bucket does not exist`, `source object not found`, …) and the
//!   non-source precondition names in copy errors.

use std::collections::BTreeMap;
use std::fs;
use std::future::Future;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use devcloud_s3::base64;
use devcloud_s3::model::{Bucket, Object};
use devcloud_s3::objops::{PutObjectInput, UpdateObjectMetadataInput};
use devcloud_s3::store::{FileBucketStore, StoreError};
use devcloud_s3::time_fmt::{now_rfc3339nano, parse_rfc3339};
use devcloud_s3::wire_json;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

const MAX_HEADER_BYTES: usize = 64 * 1024;
const MAX_BODY_BYTES: usize = 128 * 1024 * 1024;

#[derive(Debug, Clone, Default)]
pub struct Config {
    pub project: String,
    pub location: String,
    pub auth_mode: String,
    pub bearer_token: String,
    pub upload_session_path: String,
}

pub struct Server {
    config: Config,
    store: FileBucketStore,
    sessions: BTreeMap<String, ResumableSession>,
    next_upload_id: u64,
}

impl Server {
    pub fn new(config: Config, store: FileBucketStore) -> Self {
        let mut server = Self {
            config,
            store,
            sessions: BTreeMap::new(),
            next_upload_id: 1,
        };
        server.load_sessions();
        server
    }

    fn authorized(&self, token: &str) -> bool {
        match self.config.auth_mode.trim().to_ascii_lowercase().as_str() {
            "" | "off" | "relaxed" => true,
            "oauth-relaxed" => !token.trim().is_empty(),
            "bearer-dev" => !token.is_empty() && token == self.config.bearer_token.trim(),
            _ => false,
        }
    }

    /// Allocates a 32-hex-char upload id (the legacy server uses 16 random bytes;
    /// time + pid + sequence keeps ids unique across restarts without an RNG
    /// dependency).
    fn new_upload_id(&mut self) -> String {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0);
        let seq = self.next_upload_id;
        self.next_upload_id += 1;
        format!("{nanos:016x}{:08x}{seq:08x}", std::process::id())
    }
}

/// Pending resumable upload session. Serialized to `session.json` in the exact
/// byte layout the legacy server writes (`json.MarshalIndent(session, "", "  ")`
/// plus one trailing newline), so sessions survive switching engines.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct ResumableSession {
    #[serde(rename = "Bucket", default)]
    bucket: String,
    #[serde(rename = "Name", default)]
    name: String,
    #[serde(rename = "ContentType", default)]
    content_type: String,
    #[serde(rename = "ContentEncoding", default)]
    content_encoding: String,
    #[serde(rename = "CacheControl", default)]
    cache_control: String,
    #[serde(rename = "ContentDisposition", default)]
    content_disposition: String,
    #[serde(rename = "Metadata", default)]
    metadata: Option<BTreeMap<String, String>>,
    #[serde(rename = "Preconditions", default)]
    preconditions: ObjectPreconditions,
    #[serde(rename = "CreatedAt", default)]
    created_at: String,
    #[serde(rename = "ReceivedBytes", default)]
    received_bytes: i64,
}

/// The legacy `objectPreconditions` struct (raw query-string values; parsed lazily
/// so invalid values surface as `invalid if*` errors at check time).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct ObjectPreconditions {
    #[serde(rename = "IfGenerationMatch", default)]
    if_generation_match: String,
    #[serde(rename = "IfGenerationNotMatch", default)]
    if_generation_not_match: String,
    #[serde(rename = "IfMetagenerationMatch", default)]
    if_metageneration_match: String,
    #[serde(rename = "IfMetagenerationNotMatch", default)]
    if_metageneration_not_match: String,
}

#[derive(Debug, Clone)]
pub struct Request {
    pub method: String,
    pub path: String,
    pub query: BTreeMap<String, String>,
    pub headers: BTreeMap<String, String>,
    pub body: Vec<u8>,
}

impl Request {
    pub fn new(method: &str, target: &str, body: Vec<u8>) -> Self {
        let (path, query) = parse_target(target);
        Self {
            method: method.to_string(),
            path,
            query,
            headers: BTreeMap::new(),
            body,
        }
    }

    fn header(&self, name: &str) -> &str {
        self.headers
            .get(&name.to_ascii_lowercase())
            .map(String::as_str)
            .unwrap_or("")
    }

    fn query_value(&self, name: &str) -> &str {
        self.query.get(name).map(String::as_str).unwrap_or("")
    }

    fn bearer_token(&self) -> String {
        match self.header("authorization").trim().split_once(' ') {
            Some((scheme, token)) if scheme.eq_ignore_ascii_case("bearer") => {
                token.trim().to_string()
            }
            _ => String::new(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Response {
    pub status: u16,
    pub headers: BTreeMap<String, String>,
    pub body: Vec<u8>,
}

impl Response {
    /// JSON response in legacy `json.Encoder` wire format: compact encoding,
    /// HTML escaping, and a trailing newline.
    fn json<T: Serialize>(status: u16, value: &T) -> Self {
        let mut headers = BTreeMap::new();
        headers.insert(
            "Content-Type".to_string(),
            "application/json; charset=utf-8".to_string(),
        );
        let mut body = wire_json::to_vec_compact(value);
        body.push(b'\n');
        Self {
            status,
            headers,
            body,
        }
    }

    fn empty(status: u16) -> Self {
        Self {
            status,
            headers: BTreeMap::new(),
            body: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
struct BucketResource {
    kind: String,
    id: String,
    #[serde(rename = "selfLink")]
    self_link: String,
    #[serde(rename = "projectNumber", skip_serializing_if = "String::is_empty")]
    project_number: String,
    name: String,
    #[serde(rename = "timeCreated")]
    time_created: String,
    updated: String,
    location: String,
    #[serde(rename = "storageClass")]
    storage_class: String,
}

#[derive(Debug, Serialize)]
struct BucketsListResponse {
    kind: String,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    items: Vec<BucketResource>,
    #[serde(rename = "nextPageToken", skip_serializing_if = "String::is_empty")]
    next_page_token: String,
}

#[derive(Debug, Clone, Serialize)]
struct ObjectResource {
    kind: String,
    id: String,
    #[serde(rename = "selfLink")]
    self_link: String,
    name: String,
    bucket: String,
    generation: String,
    metageneration: String,
    #[serde(rename = "contentType")]
    content_type: String,
    size: String,
    #[serde(rename = "md5Hash", skip_serializing_if = "String::is_empty")]
    md5_hash: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    crc32c: String,
    etag: String,
    #[serde(rename = "timeCreated")]
    time_created: String,
    updated: String,
    #[serde(rename = "storageClass")]
    storage_class: String,
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    metadata: BTreeMap<String, String>,
    #[serde(rename = "cacheControl", skip_serializing_if = "String::is_empty")]
    cache_control: String,
    #[serde(rename = "contentEncoding", skip_serializing_if = "String::is_empty")]
    content_encoding: String,
    #[serde(
        rename = "contentDisposition",
        skip_serializing_if = "String::is_empty"
    )]
    content_disposition: String,
}

#[derive(Debug, Serialize)]
struct RewriteResponse {
    kind: String,
    #[serde(rename = "totalBytesRewritten")]
    total_bytes_rewritten: String,
    #[serde(rename = "objectSize")]
    object_size: String,
    done: bool,
    resource: ObjectResource,
}

#[derive(Debug, Serialize)]
struct ObjectsListResponse {
    kind: String,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    items: Vec<ObjectResource>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    prefixes: Vec<String>,
    #[serde(rename = "nextPageToken", skip_serializing_if = "String::is_empty")]
    next_page_token: String,
}

#[derive(Debug, Clone)]
struct ObjectListEntry {
    name: String,
    object: Option<ObjectResource>,
    prefix: String,
}

#[derive(Debug, Serialize)]
struct ErrorResponse {
    error: ErrorBody,
}

#[derive(Debug, Serialize)]
struct ErrorBody {
    code: u16,
    message: String,
    errors: Vec<ErrorItem>,
}

#[derive(Debug, Serialize)]
struct ErrorItem {
    domain: String,
    reason: String,
    message: String,
}

#[derive(Debug, Default, Deserialize)]
struct BucketRequest {
    #[serde(default)]
    name: String,
    #[serde(default)]
    location: String,
    #[serde(rename = "storageClass", default)]
    storage_class: String,
}

/// The legacy `multipartUploadMetadata` JSON shape, shared by multipart metadata
/// parts, resumable initiation bodies, copy destinations, and PATCH bodies.
/// `metadata: None` (absent or `null`) means "not provided" — PATCH preserves
/// the existing user metadata in that case, exactly like legacy nil map.
#[derive(Debug, Default, Deserialize)]
struct ObjectMetadataRequest {
    #[serde(default)]
    name: String,
    #[serde(rename = "contentType", default)]
    content_type: String,
    #[serde(rename = "contentEncoding", default)]
    content_encoding: String,
    #[serde(rename = "cacheControl", default)]
    cache_control: String,
    #[serde(rename = "contentDisposition", default)]
    content_disposition: String,
    #[serde(default)]
    metadata: Option<BTreeMap<String, String>>,
}

#[derive(Debug, Default, Deserialize)]
struct ComposeRequest {
    #[serde(rename = "sourceObjects", default)]
    source_objects: Vec<ComposeSourceObject>,
    #[serde(default)]
    destination: ObjectMetadataRequest,
}

#[derive(Debug, Default, Deserialize)]
struct ComposeSourceObject {
    #[serde(default)]
    name: String,
    #[serde(default)]
    generation: String,
    #[serde(rename = "objectPreconditions", default)]
    object_preconditions: ComposeObjectPreconditions,
}

#[derive(Debug, Default, Deserialize)]
struct ComposeObjectPreconditions {
    #[serde(rename = "ifGenerationMatch", default)]
    if_generation_match: String,
}

pub fn route(server: &mut Server, req: &Request) -> Response {
    if !server.authorized(&req.bearer_token()) {
        let mut r = json_error(401, "authError", "invalid authentication credentials");
        r.headers.insert(
            "WWW-Authenticate".to_string(),
            "Bearer realm=\"devcloud-gcs\"".to_string(),
        );
        return r;
    }
    if is_introspect_path(&req.path) {
        return handle_introspect(server, req);
    }
    match req.path.as_str() {
        "/storage/v1/b" => handle_buckets(server, req),
        p if p.starts_with("/storage/v1/b/") => handle_bucket_or_object(server, req),
        p if p.starts_with("/upload/storage/v1/b/") => handle_upload(server, req),
        p if p.starts_with("/download/storage/v1/b/") => handle_download(server, req),
        _ => json_error(404, "notFound", "not found"),
    }
}

// introspectPrefix namespaces the read-only introspection API so it can never
// collide with the GCS JSON protocol (/storage/v1/, /upload/, /download/).
// All routes are GET-only and read-only; a missing resource returns 404 and an
// unsupported method 405. Behavior-parity port of
// `internal/services/gcs/introspect.rs`.
const INTROSPECT_PREFIX: &str = "/_introspect/";

fn is_introspect_path(path: &str) -> bool {
    path.starts_with(INTROSPECT_PREFIX)
}

/// Upload-session summary; mirrors legacy `gcs.UploadSessionSummary`
/// (`introspect.rs`), including `contentType` omitempty and field order.
#[derive(Debug, Serialize)]
struct UploadSessionSummary {
    id: String,
    bucket: String,
    name: String,
    #[serde(rename = "contentType", skip_serializing_if = "String::is_empty")]
    content_type: String,
    #[serde(rename = "createdAt")]
    created_at: String,
    #[serde(rename = "receivedBytes")]
    received_bytes: i64,
}

/// Mirrors legacy `gcs.BucketSummary` field order (`name, timeCreated,
/// objectCount, gcsUri`).
#[derive(Debug, Serialize)]
struct BucketSummary {
    name: String,
    #[serde(rename = "timeCreated")]
    time_created: String,
    #[serde(rename = "objectCount")]
    object_count: i64,
    #[serde(rename = "gcsUri")]
    gcs_uri: String,
}

/// Mirrors legacy `gcs.ObjectSummary` exactly (field order, omitempty on
/// `crc32c`/`metadata`).
#[derive(Debug, Serialize)]
struct ObjectSummary {
    name: String,
    size: i64,
    etag: String,
    #[serde(rename = "contentType")]
    content_type: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    crc32c: String,
    #[serde(rename = "storageClass")]
    storage_class: String,
    updated: String,
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    metadata: BTreeMap<String, String>,
    generation: String,
    metageneration: String,
    #[serde(rename = "gcsUri")]
    gcs_uri: String,
    #[serde(rename = "downloadUrl")]
    download_url: String,
}

#[derive(Debug, Serialize)]
struct UploadsResponse {
    sessions: Vec<UploadSessionSummary>,
}

#[derive(Debug, Serialize)]
struct BucketsResponse {
    buckets: Vec<BucketSummary>,
}

#[derive(Debug, Serialize)]
struct ObjectsResponse {
    bucket: String,
    prefix: String,
    objects: Vec<ObjectSummary>,
}

/// legacy `objectSummaryFromObject`: `Generation` is the object's last-modified
/// time in Unix nanoseconds; `Metageneration` is `max(metageneration, 1)`.
fn object_summary_from_object(bucket: &str, object: &Object) -> ObjectSummary {
    ObjectSummary {
        name: object.key.clone(),
        size: object.size,
        etag: object.etag.clone(),
        content_type: object.content_type.clone(),
        crc32c: object.crc32c.clone(),
        storage_class: "STANDARD".to_string(),
        updated: object.last_modified.clone(),
        metadata: object.metadata.clone(),
        generation: generation_value(object).to_string(),
        metageneration: object.metageneration.max(1).to_string(),
        gcs_uri: format!("gs://{}/{}", bucket, object.key),
        download_url: format!(
            "/api/gcs/buckets/{}/objects/{}/download",
            url_encode(bucket),
            url_encode(&object.key)
        ),
    }
}

/// Reads upload-session summaries straight from the session storage root,
/// mirroring legacy `gcs.ListUploadSessions` (FS scan + CreatedAt-then-id sort).
fn list_upload_sessions(root: &str) -> Vec<UploadSessionSummary> {
    let entries = match fs::read_dir(root) {
        Ok(entries) => entries,
        Err(_) => return Vec::new(),
    };
    let mut sessions = Vec::new();
    for entry in entries.flatten() {
        if !entry.file_type().map(|t| t.is_dir()).unwrap_or(false) {
            continue;
        }
        let id = entry.file_name().to_string_lossy().to_string();
        let Ok(data) = fs::read(entry.path().join("session.json")) else {
            continue;
        };
        let Ok(session) = serde_json::from_slice::<ResumableSession>(&data) else {
            continue;
        };
        sessions.push(UploadSessionSummary {
            id,
            bucket: session.bucket,
            name: session.name,
            content_type: session.content_type,
            created_at: session.created_at,
            received_bytes: session.received_bytes,
        });
    }
    sessions.sort_by(|a, b| {
        a.created_at
            .cmp(&b.created_at)
            .then_with(|| a.id.cmp(&b.id))
    });
    sessions
}

/// JSON response with a bare `application/json` Content-Type (no charset),
/// matching the dashboard's normalized introspect content type — distinct from
/// the provider handlers' `application/json; charset=utf-8`.
fn introspect_json<T: Serialize>(status: u16, value: &T) -> Response {
    let mut r = Response::json(status, value);
    r.headers
        .insert("Content-Type".to_string(), "application/json".to_string());
    r
}

fn handle_introspect(server: &mut Server, req: &Request) -> Response {
    if req.method != "GET" {
        let mut r = json_error(
            405,
            "methodNotAllowed",
            "introspection endpoints are read-only",
        );
        r.headers.insert("Allow".to_string(), "GET".to_string());
        return r;
    }
    let rest = req.path.strip_prefix(INTROSPECT_PREFIX).unwrap_or("");
    if rest == "uploads" {
        return introspect_json(
            200,
            &UploadsResponse {
                sessions: list_upload_sessions(&server.config.upload_session_path),
            },
        );
    }
    if rest == "buckets" {
        return introspect_buckets(server);
    }
    if let Some(after) = rest.strip_prefix("buckets/") {
        let (escaped_bucket, suffix) = after.split_once('/').unwrap_or((after, ""));
        let bucket = url_decode(escaped_bucket);
        if bucket.is_empty() {
            return introspect_not_found();
        }
        if !after.contains('/') {
            return introspect_bucket_detail(server, &bucket);
        }
        if suffix == "objects" {
            return introspect_objects(server, req, &bucket);
        }
        if let Some(escaped_name) = suffix.strip_prefix("objects/") {
            let name = url_decode(escaped_name);
            if name.is_empty() {
                return introspect_not_found();
            }
            return introspect_object_detail(server, &bucket, &name);
        }
    }
    introspect_not_found()
}

fn introspect_not_found() -> Response {
    json_error(404, "notFound", "introspection endpoint not found")
}

fn introspect_backend_error(e: &StoreError) -> Response {
    let mut r = json_error(500, "backendError", &store_error_text(e, "", ""));
    r.headers
        .insert("Content-Type".to_string(), "application/json".to_string());
    r
}

fn introspect_buckets(server: &Server) -> Response {
    let buckets = match server.store.list_buckets() {
        Ok(v) => v,
        Err(e) => return introspect_backend_error(&e),
    };
    let mut response = BucketsResponse {
        buckets: Vec::with_capacity(buckets.len()),
    };
    for bucket in buckets {
        let objects = match server.store.list_objects(&bucket.name, "") {
            Ok(Some(v)) => v,
            Ok(None) => Vec::new(),
            Err(e) => return introspect_backend_error(&e),
        };
        response.buckets.push(BucketSummary {
            name: bucket.name.clone(),
            time_created: bucket.created_at.clone(),
            object_count: objects.len() as i64,
            gcs_uri: format!("gs://{}", bucket.name),
        });
    }
    introspect_json(200, &response)
}

fn introspect_bucket_detail(server: &Server, bucket: &str) -> Response {
    let item = match server.store.get_bucket(bucket) {
        Ok(Some(v)) => v,
        Ok(None) => {
            let mut r = json_error(404, "notFound", "bucket not found");
            r.headers
                .insert("Content-Type".to_string(), "application/json".to_string());
            return r;
        }
        Err(e) => return introspect_backend_error(&e),
    };
    let objects = match server.store.list_objects(bucket, "") {
        Ok(Some(v)) => v,
        Ok(None) => Vec::new(),
        Err(e) => return introspect_backend_error(&e),
    };
    introspect_json(
        200,
        &BucketSummary {
            name: item.name.clone(),
            time_created: item.created_at.clone(),
            object_count: objects.len() as i64,
            gcs_uri: format!("gs://{}", item.name),
        },
    )
}

fn introspect_objects(server: &Server, req: &Request, bucket: &str) -> Response {
    let prefix = req.query_value("prefix");
    let objects = match server.store.list_objects(bucket, prefix) {
        Ok(Some(v)) => v,
        Ok(None) => {
            let mut r = json_error(404, "notFound", "bucket not found");
            r.headers
                .insert("Content-Type".to_string(), "application/json".to_string());
            return r;
        }
        Err(e) => return introspect_backend_error(&e),
    };
    let mut response = ObjectsResponse {
        bucket: bucket.to_string(),
        prefix: prefix.to_string(),
        objects: Vec::with_capacity(objects.len()),
    };
    for object in &objects {
        response
            .objects
            .push(object_summary_from_object(bucket, object));
    }
    introspect_json(200, &response)
}

fn introspect_object_detail(server: &Server, bucket: &str, name: &str) -> Response {
    match server.store.get_object(bucket, name) {
        Ok(Some((object, _))) => introspect_json(200, &object_summary_from_object(bucket, &object)),
        Ok(None) => {
            let mut r = json_error(404, "notFound", "object not found");
            r.headers
                .insert("Content-Type".to_string(), "application/json".to_string());
            r
        }
        Err(e) => introspect_backend_error(&e),
    }
}

fn handle_buckets(server: &mut Server, req: &Request) -> Response {
    match req.method.as_str() {
        "GET" => {
            let prefix = req.query_value("prefix");
            match server.store.list_buckets() {
                Ok(buckets) => {
                    let mut items = Vec::new();
                    for bucket in buckets {
                        if prefix.is_empty() || bucket.name.starts_with(prefix) {
                            items.push(bucket_resource(server, &bucket, None, None));
                        }
                    }
                    let (start, end, next) = match page_window(&req.query, items.len()) {
                        Ok(v) => v,
                        Err(e) => return json_error(400, "invalid", &e),
                    };
                    Response::json(
                        200,
                        &BucketsListResponse {
                            kind: "storage#buckets".to_string(),
                            items: items[start..end].to_vec(),
                            next_page_token: next,
                        },
                    )
                }
                Err(_) => json_error(500, "backendError", "internal error"),
            }
        }
        "POST" => {
            let parsed: BucketRequest = match serde_json::from_slice(&req.body) {
                Ok(v) => v,
                Err(_) if req.body.is_empty() => BucketRequest::default(),
                Err(_) => return json_error(400, "invalid", "invalid json request"),
            };
            if parsed.name.is_empty() {
                return json_error(400, "required", "bucket name is required");
            }
            match server.store.create_bucket(&parsed.name) {
                Ok((bucket, true)) => {
                    let mut r = Response::json(
                        200,
                        &bucket_resource(
                            server,
                            &bucket,
                            Some(parsed.location.as_str()),
                            Some(parsed.storage_class.as_str()),
                        ),
                    );
                    r.headers.insert(
                        "Location".to_string(),
                        format!("/storage/v1/b/{}", url_encode(&bucket.name)),
                    );
                    emit_dashboard_event(
                        "gcs.bucket.created",
                        serde_json::json!({"bucket": bucket.name}),
                    );
                    r
                }
                Ok((_, false)) => json_error(409, "conflict", "bucket already exists"),
                Err(e) => json_error(400, "invalid", &store_error_text(&e, &parsed.name, "")),
            }
        }
        _ => method_not_allowed("GET, POST"),
    }
}

fn handle_bucket_or_object(server: &mut Server, req: &Request) -> Response {
    let Some((bucket, suffix)) = bucket_suffix(&req.path, "/storage/v1/b/") else {
        return json_error(404, "notFound", "not found");
    };
    if suffix.is_empty() {
        return handle_bucket(server, req, &bucket);
    }
    if suffix == "o" {
        return handle_objects(server, req, &bucket);
    }
    if let Some(object) = suffix.strip_prefix("o/") {
        if let Some(dest) = object.strip_suffix("/compose") {
            return handle_compose_object(server, req, &bucket, dest);
        }
        if let Some((source, copy_suffix)) = object.split_once("/copyTo/b/") {
            return handle_copy_object(server, req, &bucket, source, copy_suffix);
        }
        if let Some((source, rewrite_suffix)) = object.split_once("/rewriteTo/b/") {
            return handle_rewrite_object(server, req, &bucket, source, rewrite_suffix);
        }
        let key = url_decode(object);
        if key.is_empty() {
            return json_error(400, "invalid", "invalid object name");
        }
        return handle_object(server, req, &bucket, &key);
    }
    json_error(404, "notFound", "not found")
}

fn handle_bucket(server: &mut Server, req: &Request, name: &str) -> Response {
    match req.method.as_str() {
        "GET" => match server.store.get_bucket(name) {
            Ok(Some(bucket)) => Response::json(200, &bucket_resource(server, &bucket, None, None)),
            Ok(None) => json_error(404, "notFound", "bucket not found"),
            Err(e) => json_error(400, "invalid", &store_error_text(&e, name, "")),
        },
        "DELETE" => match server.store.delete_bucket(name) {
            Ok(true) => {
                emit_dashboard_event("gcs.bucket.deleted", serde_json::json!({"bucket": name}));
                Response::empty(204)
            }
            Ok(false) => json_error(404, "notFound", "bucket not found"),
            Err(e) => json_error(409, "conflict", &store_error_text(&e, name, "")),
        },
        _ => method_not_allowed("GET, DELETE"),
    }
}

fn handle_objects(server: &mut Server, req: &Request, bucket: &str) -> Response {
    if req.method != "GET" {
        return method_not_allowed("GET");
    }
    let prefix = req.query_value("prefix");
    let delimiter = req.query_value("delimiter");
    let start_offset = req.query_value("startOffset");
    let end_offset = req.query_value("endOffset");
    let include_trailing = req
        .query_value("includeTrailingDelimiter")
        .eq_ignore_ascii_case("true");
    let objects = match server.store.list_objects(bucket, prefix) {
        Ok(Some(v)) => v,
        Ok(None) => return json_error(404, "notFound", "bucket not found"),
        Err(e) => return json_error(400, "invalid", &store_error_text(&e, bucket, "")),
    };
    let mut items = Vec::new();
    let mut prefixes = BTreeMap::new();
    for object in objects {
        if !start_offset.is_empty() && object.key.as_str() < start_offset {
            continue;
        }
        if !end_offset.is_empty() && object.key.as_str() >= end_offset {
            continue;
        }
        if !delimiter.is_empty() {
            let remainder = object.key.strip_prefix(prefix).unwrap_or(&object.key);
            if let Some(index) = remainder.find(delimiter) {
                let p = format!("{}{}", prefix, &remainder[..index + delimiter.len()]);
                prefixes.insert(p, ());
                let trailing = include_trailing && index + delimiter.len() == remainder.len();
                if !trailing {
                    continue;
                }
            }
        }
        items.push(object_resource(&object));
    }
    let mut entries = Vec::with_capacity(items.len() + prefixes.len());
    for object in items {
        entries.push(ObjectListEntry {
            name: object.name.clone(),
            object: Some(object),
            prefix: String::new(),
        });
    }
    for prefix in prefixes.into_keys() {
        entries.push(ObjectListEntry {
            name: prefix.clone(),
            object: None,
            prefix,
        });
    }
    entries.sort_by(|a, b| a.name.cmp(&b.name));
    let (start, end, next) = match page_window(&req.query, entries.len()) {
        Ok(v) => v,
        Err(e) => return json_error(400, "invalid", &e),
    };
    let mut paged_items = Vec::new();
    let mut paged_prefixes = Vec::new();
    for entry in entries[start..end].iter().cloned() {
        if let Some(object) = entry.object {
            paged_items.push(object);
        } else {
            paged_prefixes.push(entry.prefix);
        }
    }
    Response::json(
        200,
        &ObjectsListResponse {
            kind: "storage#objects".to_string(),
            items: paged_items,
            prefixes: paged_prefixes,
            next_page_token: next,
        },
    )
}

/// Fetches an object and applies the `generation` query filter plus the
/// `if*` preconditions, exactly in the legacy handler order.
fn read_object(
    server: &Server,
    req: &Request,
    bucket: &str,
    key: &str,
) -> Result<(Object, Vec<u8>), Response> {
    let (object, body) = fetch_object(server, bucket, key)?;
    if !generation_matches(req, &object)? {
        return Err(json_error(404, "notFound", "object not found"));
    }
    check_object_preconditions(server, req, bucket, key)?;
    Ok((object, body))
}

fn handle_object(server: &mut Server, req: &Request, bucket: &str, key: &str) -> Response {
    match req.method.as_str() {
        "GET" | "HEAD" => {
            let (object, body) = match read_object(server, req, bucket, key) {
                Ok(v) => v,
                Err(r) => return r,
            };
            if req.query_value("alt") == "media" {
                return media_response(req, &object, &body);
            }
            if req.method == "HEAD" {
                let mut r = Response::empty(200);
                set_object_headers(&mut r, &object);
                r.headers
                    .insert("Content-Type".to_string(), "application/json".to_string());
                r
            } else {
                Response::json(200, &object_resource(&object))
            }
        }
        "PATCH" => {
            if let Err(r) = read_object(server, req, bucket, key) {
                return r;
            }
            let parsed: ObjectMetadataRequest = match serde_json::from_slice(&req.body) {
                Ok(v) => v,
                Err(_) if req.body.is_empty() => ObjectMetadataRequest::default(),
                Err(_) => return json_error(400, "invalid", "invalid json request"),
            };
            match server
                .store
                .update_object_metadata(UpdateObjectMetadataInput {
                    bucket: bucket.to_string(),
                    key: key.to_string(),
                    content_type: parsed.content_type,
                    content_encoding: parsed.content_encoding,
                    cache_control: parsed.cache_control,
                    content_disposition: parsed.content_disposition,
                    metadata: parsed.metadata,
                }) {
                Ok(Some(object)) => Response::json(200, &object_resource(&object)),
                Ok(None) => json_error(404, "notFound", "object not found"),
                Err(_) => json_error(404, "notFound", "bucket not found"),
            }
        }
        "DELETE" => {
            if let Err(r) = read_object(server, req, bucket, key) {
                return r;
            }
            match server.store.delete_object(bucket, key) {
                Ok(true) => {
                    emit_dashboard_event(
                        "gcs.object.deleted",
                        serde_json::json!({"bucket": bucket, "key": key}),
                    );
                    Response::empty(204)
                }
                Ok(false) => json_error(404, "notFound", "object not found"),
                Err(_) => json_error(404, "notFound", "bucket not found"),
            }
        }
        _ => method_not_allowed("GET, PATCH, DELETE"),
    }
}

fn handle_copy_object(
    server: &mut Server,
    req: &Request,
    source_bucket: &str,
    source_escaped: &str,
    copy_suffix: &str,
) -> Response {
    if req.method != "POST" {
        return method_not_allowed("POST");
    }
    match copy_object(server, req, source_bucket, source_escaped, copy_suffix) {
        Ok(object) => Response::json(200, &object_resource(&object)),
        Err(response) => response,
    }
}

fn handle_rewrite_object(
    server: &mut Server,
    req: &Request,
    source_bucket: &str,
    source_escaped: &str,
    rewrite_suffix: &str,
) -> Response {
    if req.method != "POST" {
        return method_not_allowed("POST");
    }
    match copy_object(server, req, source_bucket, source_escaped, rewrite_suffix) {
        Ok(object) => {
            let size = object.size.to_string();
            Response::json(
                200,
                &RewriteResponse {
                    kind: "storage#rewriteResponse".to_string(),
                    total_bytes_rewritten: size.clone(),
                    object_size: size,
                    done: true,
                    resource: object_resource(&object),
                },
            )
        }
        Err(response) => response,
    }
}

fn handle_compose_object(
    server: &mut Server,
    req: &Request,
    bucket: &str,
    dest_escaped: &str,
) -> Response {
    if req.method != "POST" {
        return method_not_allowed("POST");
    }
    let dest_key = url_decode(dest_escaped);
    if dest_key.is_empty() {
        return json_error(400, "invalid", "invalid destination object name");
    }
    let parsed: ComposeRequest = match serde_json::from_slice(&req.body) {
        Ok(v) => v,
        Err(_) if req.body.is_empty() => ComposeRequest::default(),
        Err(_) => return json_error(400, "invalid", "invalid json request"),
    };
    if parsed.source_objects.is_empty() {
        return json_error(400, "required", "sourceObjects is required");
    }
    if parsed.source_objects.len() > 32 {
        return json_error(
            400,
            "invalid",
            "sourceObjects must contain no more than 32 objects",
        );
    }
    if let Err(r) = check_object_preconditions(server, req, bucket, &dest_key) {
        return r;
    }

    let mut body = Vec::new();
    let mut first_source: Option<Object> = None;
    for source in &parsed.source_objects {
        if source.name.is_empty() {
            return json_error(400, "required", "source object name is required");
        }
        let (object, payload) = match server.store.get_object(bucket, &source.name) {
            Ok(Some(found)) => found,
            Ok(None) => return json_error(404, "notFound", "source object not found"),
            Err(_) => return json_error(404, "notFound", "bucket not found"),
        };
        let actual_generation = generation(&object);
        if (!source.generation.is_empty() && source.generation != actual_generation)
            || (!source.object_preconditions.if_generation_match.is_empty()
                && source.object_preconditions.if_generation_match != actual_generation)
        {
            return json_error(
                412,
                "conditionNotMet",
                "source generation precondition failed",
            );
        }
        if first_source.is_none() {
            first_source = Some(object);
        }
        body.extend_from_slice(&payload);
    }
    let first_source = first_source.unwrap_or_default();
    match server.store.put_object(PutObjectInput {
        bucket: bucket.to_string(),
        key: dest_key.clone(),
        body,
        content_type: first_non_empty(&parsed.destination.content_type, &first_source.content_type),
        content_encoding: parsed.destination.content_encoding,
        cache_control: parsed.destination.cache_control,
        content_disposition: parsed.destination.content_disposition,
        metadata: parsed.destination.metadata.unwrap_or_default(),
        ..Default::default()
    }) {
        Ok(object) => Response::json(200, &object_resource(&object)),
        Err(e) => json_error(404, "notFound", &store_error_text(&e, bucket, &dest_key)),
    }
}

fn copy_object(
    server: &mut Server,
    req: &Request,
    source_bucket: &str,
    source_escaped: &str,
    dest_suffix: &str,
) -> Result<Object, Response> {
    let destination: ObjectMetadataRequest = match serde_json::from_slice(&req.body) {
        Ok(v) => v,
        Err(_) if req.body.is_empty() => ObjectMetadataRequest::default(),
        Err(_) => return Err(json_error(400, "invalid", "invalid json request")),
    };
    let source_key = url_decode(source_escaped);
    if source_key.is_empty() {
        return Err(json_error(400, "invalid", "invalid source object name"));
    }
    let Some((dest_bucket_escaped, dest_escaped)) = dest_suffix.split_once("/o/") else {
        return Err(json_error(404, "notFound", "not found"));
    };
    let dest_bucket = url_decode(dest_bucket_escaped);
    if dest_bucket.is_empty() {
        return Err(json_error(400, "invalid", "invalid destination bucket"));
    }
    let dest_key = url_decode(dest_escaped);
    if dest_key.is_empty() {
        return Err(json_error(
            400,
            "invalid",
            "invalid destination object name",
        ));
    }
    let (source, body) = match server.store.get_object(source_bucket, &source_key) {
        Ok(Some(found)) => found,
        Ok(None) => return Err(json_error(404, "notFound", "source object not found")),
        Err(_) => return Err(json_error(404, "notFound", "source bucket not found")),
    };
    check_stored_preconditions(
        server,
        source_bucket,
        &source_key,
        &source_preconditions_from_request(req),
    )?;
    check_object_preconditions(server, req, &dest_bucket, &dest_key)?;

    let metadata = destination
        .metadata
        .unwrap_or_else(|| source.metadata.clone());
    match server.store.put_object(PutObjectInput {
        bucket: dest_bucket,
        key: dest_key,
        body,
        content_type: first_non_empty(&destination.content_type, &source.content_type),
        content_encoding: first_non_empty(&destination.content_encoding, &source.content_encoding),
        cache_control: first_non_empty(&destination.cache_control, &source.cache_control),
        content_disposition: first_non_empty(
            &destination.content_disposition,
            &source.content_disposition,
        ),
        metadata,
        ..Default::default()
    }) {
        Ok(object) => Ok(object),
        Err(_) => Err(json_error(404, "notFound", "destination bucket not found")),
    }
}

/// Object fetch with the legacy single-object handler error mapping: any store
/// error reads as a missing bucket.
fn fetch_object(server: &Server, bucket: &str, key: &str) -> Result<(Object, Vec<u8>), Response> {
    match server.store.get_object(bucket, key) {
        Ok(Some(found)) => Ok(found),
        Ok(None) => Err(json_error(404, "notFound", "object not found")),
        Err(_) => Err(json_error(404, "notFound", "bucket not found")),
    }
}

fn handle_upload(server: &mut Server, req: &Request) -> Response {
    let Some((bucket, suffix)) = bucket_suffix(&req.path, "/upload/storage/v1/b/") else {
        return json_error(404, "notFound", "not found");
    };
    if suffix != "o" {
        return json_error(404, "notFound", "not found");
    }
    match req.query_value("uploadType") {
        "media" => handle_media_upload(server, req, &bucket),
        "multipart" => handle_multipart_upload(server, req, &bucket),
        "resumable" => handle_resumable_upload(server, req, &bucket),
        _ => json_error(400, "invalid", "unsupported uploadType"),
    }
}

fn handle_media_upload(server: &mut Server, req: &Request, bucket: &str) -> Response {
    if req.method != "POST" {
        return method_not_allowed("POST");
    }
    let name = req.query_value("name").to_string();
    if name.is_empty() {
        return json_error(400, "required", "object name is required");
    }
    if let Err(r) = check_object_preconditions(server, req, bucket, &name) {
        return r;
    }
    put_object_response(
        server,
        PutObjectInput {
            bucket: bucket.to_string(),
            key: name,
            body: req.body.clone(),
            content_type: req.header("content-type").to_string(),
            content_encoding: req.header("content-encoding").to_string(),
            cache_control: req.header("cache-control").to_string(),
            content_disposition: req.header("content-disposition").to_string(),
            metadata: user_metadata(&req.headers),
            ..Default::default()
        },
    )
}

fn handle_multipart_upload(server: &mut Server, req: &Request, bucket: &str) -> Response {
    if req.method != "POST" {
        return method_not_allowed("POST");
    }
    let (parsed, body, body_content_type) = match parse_multipart_upload(req) {
        Ok(v) => v,
        Err(e) => return json_error(400, "invalid", &e),
    };
    let name = first_non_empty(&parsed.name, req.query_value("name"));
    if name.is_empty() {
        return json_error(400, "required", "object name is required");
    }
    if let Err(r) = check_object_preconditions(server, req, bucket, &name) {
        return r;
    }
    put_object_response(
        server,
        PutObjectInput {
            bucket: bucket.to_string(),
            key: name,
            body,
            content_type: first_non_empty(&parsed.content_type, &body_content_type),
            content_encoding: parsed.content_encoding,
            cache_control: parsed.cache_control,
            content_disposition: parsed.content_disposition,
            metadata: parsed.metadata.unwrap_or_default(),
            ..Default::default()
        },
    )
}

fn handle_resumable_upload(server: &mut Server, req: &Request, bucket: &str) -> Response {
    match req.method.as_str() {
        "POST" => create_resumable_upload(server, req, bucket),
        "PUT" => put_resumable_upload(server, req),
        _ => method_not_allowed("POST, PUT"),
    }
}

fn create_resumable_upload(server: &mut Server, req: &Request, bucket: &str) -> Response {
    let parsed: ObjectMetadataRequest = match serde_json::from_slice(&req.body) {
        Ok(v) => v,
        Err(_) if req.body.is_empty() => ObjectMetadataRequest::default(),
        Err(_) => return json_error(400, "invalid", "invalid json request"),
    };
    let name = first_non_empty(req.query_value("name"), &parsed.name);
    if name.is_empty() {
        return json_error(400, "required", "object name is required");
    }
    if let Err(r) = check_object_preconditions(server, req, bucket, &name) {
        return r;
    }
    let id = server.new_upload_id();
    let session = ResumableSession {
        bucket: bucket.to_string(),
        name,
        content_type: first_non_empty(req.header("x-upload-content-type"), &parsed.content_type),
        content_encoding: first_non_empty(req.header("content-encoding"), &parsed.content_encoding),
        cache_control: parsed.cache_control,
        content_disposition: parsed.content_disposition,
        metadata: merge_metadata(parsed.metadata, user_metadata(&req.headers)),
        preconditions: preconditions_from_request(req),
        created_at: now_rfc3339nano(),
        received_bytes: 0,
    };
    server.sessions.insert(id.clone(), session.clone());
    if server.save_session(&id, &session).is_err() {
        server.sessions.remove(&id);
        return json_error(500, "backendError", "internal error");
    }
    let mut r = Response::empty(200);
    let host = req.header("host");
    let base = if host.is_empty() {
        String::new()
    } else {
        format!("http://{host}")
    };
    r.headers.insert(
        "Location".to_string(),
        format!(
            "{base}/upload/storage/v1/b/{}/o?uploadType=resumable&upload_id={}",
            url_encode(bucket),
            url_encode(&id)
        ),
    );
    r.headers.insert("X-GUploader-UploadID".to_string(), id);
    r
}

fn put_resumable_upload(server: &mut Server, req: &Request) -> Response {
    let id = req.query_value("upload_id").to_string();
    if id.is_empty() {
        return json_error(400, "required", "upload_id is required");
    }
    let Some(mut session) = server.sessions.get(&id).cloned() else {
        return json_error(404, "notFound", "upload session not found");
    };
    if is_status_query(req.header("content-range")) {
        let mut r = Response::empty(308);
        if session.received_bytes > 0 {
            r.headers.insert(
                "Range".to_string(),
                format!("bytes=0-{}", session.received_bytes - 1),
            );
        }
        return r;
    }
    if let Err(r) = check_stored_preconditions(
        server,
        &session.bucket.clone(),
        &session.name.clone(),
        &session.preconditions,
    ) {
        return r;
    }
    let mut payload = req.body.clone();
    if !req.header("content-range").trim().is_empty() {
        let upload_range = match parse_resumable_content_range(
            req.header("content-range"),
            payload.len() as i64,
        ) {
            Ok(v) => v,
            Err(e) => return json_error(400, "invalid", &e),
        };
        if upload_range.start != session.received_bytes {
            return json_error(
                400,
                "invalid",
                "upload chunk does not start at committed offset",
            );
        }
        session.received_bytes = upload_range.end + 1;
        let one_shot = upload_range.start == 0 && session.received_bytes == upload_range.total;
        if !one_shot {
            if server.append_session_body(&id, &payload).is_err()
                || server.save_session(&id, &session).is_err()
            {
                return json_error(500, "backendError", "internal error");
            }
        }
        if session.received_bytes < upload_range.total {
            server.sessions.insert(id, session);
            let mut r = Response::empty(308);
            r.headers
                .insert("Range".to_string(), format!("bytes=0-{}", upload_range.end));
            return r;
        }
        if !one_shot {
            payload = match server.read_session_body(&id) {
                Ok(v) => v,
                Err(_) => return json_error(500, "backendError", "internal error"),
            };
        }
    }
    match server.store.put_object(PutObjectInput {
        bucket: session.bucket.clone(),
        key: session.name.clone(),
        body: payload,
        content_type: first_non_empty(req.header("content-type"), &session.content_type),
        content_encoding: session.content_encoding.clone(),
        cache_control: session.cache_control.clone(),
        content_disposition: session.content_disposition.clone(),
        metadata: session.metadata.clone().unwrap_or_default(),
        ..Default::default()
    }) {
        Ok(object) => {
            server.sessions.remove(&id);
            let _ = server.delete_session(&id);
            emit_dashboard_event(
                "gcs.object.put",
                serde_json::json!({
                    "bucket": object.bucket,
                    "key": object.key,
                    "etag": object.etag,
                    "contentLength": object.size,
                }),
            );
            Response::json(200, &object_resource(&object))
        }
        Err(e) => json_error(
            404,
            "notFound",
            &store_error_text(&e, &session.bucket, &session.name),
        ),
    }
}

/// Stores the object and writes the legacy upload-handler response: the object
/// resource on success, or `404 notFound` with the store error text (matching
/// legacy `writeError(w, http.StatusNotFound, "notFound", err.Error())`).
fn put_object_response(server: &mut Server, input: PutObjectInput) -> Response {
    let (bucket, key) = (input.bucket.clone(), input.key.clone());
    match server.store.put_object(input) {
        Ok(object) => {
            emit_dashboard_event(
                "gcs.object.put",
                serde_json::json!({
                    "bucket": bucket,
                    "key": key,
                    "etag": object.etag,
                    "contentLength": object.size,
                }),
            );
            Response::json(200, &object_resource(&object))
        }
        Err(e) => json_error(404, "notFound", &store_error_text(&e, &bucket, &key)),
    }
}

fn parse_multipart_upload(
    req: &Request,
) -> Result<(ObjectMetadataRequest, Vec<u8>, String), String> {
    let content_type = req.header("content-type");
    let media_type = content_type
        .split(';')
        .next()
        .unwrap_or("")
        .trim()
        .to_ascii_lowercase();
    if media_type.is_empty() {
        return Err("invalid multipart Content-Type".to_string());
    }
    if media_type != "multipart/related" && media_type != "multipart/form-data" {
        return Err("Content-Type must be multipart/related".to_string());
    }
    let boundary = multipart_boundary(content_type).ok_or("multipart boundary is required")?;
    let marker = format!("--{boundary}");
    let raw = String::from_utf8_lossy(&req.body);
    let mut parts = Vec::new();
    for section in raw.split(marker.as_str()).skip(1) {
        if section.starts_with("--") {
            break;
        }
        let section = section.strip_prefix("\r\n").unwrap_or(section);
        let section = section.strip_suffix("\r\n").unwrap_or(section);
        let (head, body) = match section.split_once("\r\n\r\n") {
            Some(v) => v,
            None => match section.strip_prefix("\r\n") {
                Some(rest) => ("", rest),
                None => return Err("multipart part is malformed".to_string()),
            },
        };
        parts.push((head.to_string(), body.as_bytes().to_vec()));
    }
    if parts.is_empty() {
        return Err("metadata part is required".to_string());
    }
    let metadata: ObjectMetadataRequest = serde_json::from_slice(&parts[0].1)
        .map_err(|_| "invalid metadata json part".to_string())?;
    if parts.len() < 2 {
        return Err("media part is required".to_string());
    }
    let body_content_type = header_value(&parts[1].0, "content-type");
    Ok((metadata, parts[1].1.clone(), body_content_type))
}

fn multipart_boundary(content_type: &str) -> Option<String> {
    for part in content_type.split(';').skip(1) {
        let (k, v) = part.trim().split_once('=')?;
        if k.trim().eq_ignore_ascii_case("boundary") {
            return Some(v.trim().trim_matches('"').to_string());
        }
    }
    None
}

fn header_value(headers: &str, name: &str) -> String {
    for line in headers.split("\r\n") {
        if let Some((k, v)) = line.split_once(':') {
            if k.trim().eq_ignore_ascii_case(name) {
                return v.trim().to_string();
            }
        }
    }
    String::new()
}

fn preconditions_from_request(req: &Request) -> ObjectPreconditions {
    ObjectPreconditions {
        if_generation_match: req.query_value("ifGenerationMatch").to_string(),
        if_generation_not_match: req.query_value("ifGenerationNotMatch").to_string(),
        if_metageneration_match: req.query_value("ifMetagenerationMatch").to_string(),
        if_metageneration_not_match: req.query_value("ifMetagenerationNotMatch").to_string(),
    }
}

/// `ifSource*` query parameters mapped into the same struct — note the legacy
/// quirk: invalid values still report the non-source field name
/// (`invalid ifGenerationMatch`).
fn source_preconditions_from_request(req: &Request) -> ObjectPreconditions {
    ObjectPreconditions {
        if_generation_match: req.query_value("ifSourceGenerationMatch").to_string(),
        if_generation_not_match: req.query_value("ifSourceGenerationNotMatch").to_string(),
        if_metageneration_match: req.query_value("ifSourceMetagenerationMatch").to_string(),
        if_metageneration_not_match: req
            .query_value("ifSourceMetagenerationNotMatch")
            .to_string(),
    }
}

fn check_object_preconditions(
    server: &Server,
    req: &Request,
    bucket: &str,
    key: &str,
) -> Result<(), Response> {
    check_stored_preconditions(server, bucket, key, &preconditions_from_request(req))
}

/// The legacy `checkStoredObjectPreconditions` + `writePreconditionError` pair: a
/// store read failure surfaces as `404 notFound` with the raw store error text,
/// an unparsable value as `400 invalid if*`, and a mismatch as
/// `412 precondition failed`. A missing object counts as generation 0 /
/// metageneration 0.
fn check_stored_preconditions(
    server: &Server,
    bucket: &str,
    key: &str,
    preconditions: &ObjectPreconditions,
) -> Result<(), Response> {
    let found = match server.store.get_object(bucket, key) {
        Ok(found) => found.map(|(object, _)| object),
        Err(e) => {
            return Err(json_error(
                404,
                "notFound",
                &store_error_text(&e, bucket, key),
            ))
        }
    };
    let generation = found.as_ref().map(generation_value).unwrap_or(0);
    let metageneration = found.as_ref().map(|o| o.metageneration.max(1)).unwrap_or(0);
    for (name, expected, actual, invert) in [
        (
            "ifGenerationMatch",
            preconditions.if_generation_match.as_str(),
            generation,
            false,
        ),
        (
            "ifGenerationNotMatch",
            preconditions.if_generation_not_match.as_str(),
            generation,
            true,
        ),
        (
            "ifMetagenerationMatch",
            preconditions.if_metageneration_match.as_str(),
            metageneration,
            false,
        ),
        (
            "ifMetagenerationNotMatch",
            preconditions.if_metageneration_not_match.as_str(),
            metageneration,
            true,
        ),
    ] {
        if expected.is_empty() {
            continue;
        }
        let Ok(value) = expected.parse::<i64>() else {
            return Err(json_error(400, "invalid", &format!("invalid {name}")));
        };
        if (value == actual) == invert {
            return Err(json_error(412, "conditionNotMet", "precondition failed"));
        }
    }
    Ok(())
}

#[derive(Debug)]
struct UploadRange {
    start: i64,
    end: i64,
    total: i64,
}

fn is_status_query(content_range: &str) -> bool {
    content_range.trim().starts_with("bytes */")
}

fn parse_resumable_content_range(header: &str, payload_size: i64) -> Result<UploadRange, String> {
    let Some(rest) = header.trim().strip_prefix("bytes ") else {
        return Err("invalid Content-Range".to_string());
    };
    let Some((span, total_value)) = rest.split_once('/') else {
        return Err("invalid Content-Range".to_string());
    };
    if total_value == "*" {
        return Err("invalid Content-Range".to_string());
    }
    let Some((left, right)) = span.split_once('-') else {
        return Err("invalid Content-Range".to_string());
    };
    let start = left
        .parse::<i64>()
        .map_err(|_| "invalid Content-Range".to_string())?;
    let end = right
        .parse::<i64>()
        .map_err(|_| "invalid Content-Range".to_string())?;
    let total = total_value
        .parse::<i64>()
        .map_err(|_| "invalid Content-Range".to_string())?;
    if start < 0 || end < start || total <= 0 || end >= total {
        return Err("invalid Content-Range".to_string());
    }
    if payload_size != end - start + 1 {
        return Err("Content-Range does not match payload size".to_string());
    }
    Ok(UploadRange { start, end, total })
}

impl Server {
    fn session_root(&self) -> Option<PathBuf> {
        if self.config.upload_session_path.is_empty() {
            None
        } else {
            Some(PathBuf::from(&self.config.upload_session_path))
        }
    }

    fn session_dir(&self, id: &str) -> Option<PathBuf> {
        Some(self.session_root()?.join(id))
    }

    fn load_sessions(&mut self) {
        let Some(root) = self.session_root() else {
            return;
        };
        let Ok(entries) = fs::read_dir(root) else {
            return;
        };
        for entry in entries.flatten() {
            let id = entry.file_name().to_string_lossy().to_string();
            let Ok(data) = fs::read(entry.path().join("session.json")) else {
                continue;
            };
            let Ok(session) = serde_json::from_slice::<ResumableSession>(&data) else {
                continue;
            };
            self.sessions.insert(id, session);
        }
    }

    /// Writes `session.json` in legacy `json.MarshalIndent(_, "", "  ")` + `\n`
    /// byte format.
    fn save_session(&self, id: &str, session: &ResumableSession) -> std::io::Result<()> {
        let Some(dir) = self.session_dir(id) else {
            return Ok(());
        };
        fs::create_dir_all(&dir)?;
        fs::write(dir.join("session.json"), wire_json::to_vec_indent(session))
    }

    fn append_session_body(&self, id: &str, payload: &[u8]) -> std::io::Result<()> {
        use std::io::Write;
        let Some(dir) = self.session_dir(id) else {
            return Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "upload session storage is not configured",
            ));
        };
        fs::create_dir_all(&dir)?;
        let mut f = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(dir.join("body.part"))?;
        f.write_all(payload)
    }

    fn read_session_body(&self, id: &str) -> std::io::Result<Vec<u8>> {
        let Some(dir) = self.session_dir(id) else {
            return Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "upload session storage is not configured",
            ));
        };
        fs::read(dir.join("body.part"))
    }

    fn delete_session(&self, id: &str) -> std::io::Result<()> {
        let Some(dir) = self.session_dir(id) else {
            return Ok(());
        };
        fs::remove_dir_all(dir)
    }
}

fn handle_download(server: &mut Server, req: &Request) -> Response {
    if req.method != "GET" && req.method != "HEAD" {
        return method_not_allowed("GET, HEAD");
    }
    let Some((bucket, suffix)) = bucket_suffix(&req.path, "/download/storage/v1/b/") else {
        return json_error(404, "notFound", "not found");
    };
    let Some(object) = suffix.strip_prefix("o/") else {
        return json_error(404, "notFound", "not found");
    };
    let key = url_decode(object);
    if key.is_empty() {
        return json_error(400, "invalid", "invalid object name");
    }
    match read_object(server, req, &bucket, &key) {
        Ok((object, body)) => media_response(req, &object, &body),
        Err(r) => r,
    }
}

fn media_response(req: &Request, object: &Object, body: &[u8]) -> Response {
    let (start, end, partial) = match parse_range(req.header("range"), body.len()) {
        Ok(v) => v,
        Err(_) => {
            let mut r = json_error(
                416,
                "requestedRangeNotSatisfiable",
                "requested range is not satisfiable",
            );
            r.headers.insert(
                "Content-Range".to_string(),
                format!("bytes */{}", body.len()),
            );
            return r;
        }
    };
    let payload = if body.is_empty() {
        Vec::new()
    } else {
        body[start..=end].to_vec()
    };
    let mut r = if req.method == "HEAD" {
        Response::empty(if partial { 206 } else { 200 })
    } else {
        Response {
            status: if partial { 206 } else { 200 },
            headers: BTreeMap::new(),
            body: payload,
        }
    };
    r.headers
        .insert("Content-Type".to_string(), object.content_type.clone());
    r.headers
        .insert("Accept-Ranges".to_string(), "bytes".to_string());
    let content_length = if body.is_empty() { 0 } else { end - start + 1 };
    r.headers
        .insert("Content-Length".to_string(), content_length.to_string());
    if partial {
        r.headers.insert(
            "Content-Range".to_string(),
            format!("bytes {}-{}/{}", start, end, body.len()),
        );
    }
    set_object_headers(&mut r, object);
    r
}

fn bucket_resource(
    server: &Server,
    bucket: &Bucket,
    location_override: Option<&str>,
    class_override: Option<&str>,
) -> BucketResource {
    let location = first_non_empty(
        location_override.unwrap_or(""),
        first_non_empty(&server.config.location, "US").as_str(),
    );
    let storage_class = first_non_empty(class_override.unwrap_or(""), "STANDARD");
    BucketResource {
        kind: "storage#bucket".to_string(),
        id: bucket.name.clone(),
        self_link: format!("/storage/v1/b/{}", url_encode(&bucket.name)),
        project_number: "0".to_string(),
        name: bucket.name.clone(),
        time_created: bucket.created_at.clone(),
        updated: bucket.created_at.clone(),
        location,
        storage_class,
    }
}

fn object_resource(object: &Object) -> ObjectResource {
    let created = if object.created_at.is_empty() || object.created_at == "0001-01-01T00:00:00Z" {
        object.last_modified.clone()
    } else {
        object.created_at.clone()
    };
    let updated = if object.updated_at.is_empty() || object.updated_at == "0001-01-01T00:00:00Z" {
        object.last_modified.clone()
    } else {
        object.updated_at.clone()
    };
    let generation = generation(object);
    let metageneration = object.metageneration.max(1).to_string();
    ObjectResource {
        kind: "storage#object".to_string(),
        id: format!("{}/{}/{}", object.bucket, object.key, generation),
        self_link: format!(
            "/storage/v1/b/{}/o/{}",
            url_encode(&object.bucket),
            url_encode(&object.key)
        ),
        name: object.key.clone(),
        bucket: object.bucket.clone(),
        generation,
        metageneration,
        content_type: object.content_type.clone(),
        size: object.size.to_string(),
        md5_hash: md5_hash_from_etag(&object.etag),
        crc32c: object.crc32c.clone(),
        etag: gcs_etag(object),
        time_created: created,
        updated,
        storage_class: "STANDARD".to_string(),
        metadata: object.metadata.clone(),
        cache_control: object.cache_control.clone(),
        content_encoding: object.content_encoding.clone(),
        content_disposition: object.content_disposition.clone(),
    }
}

fn set_object_headers(r: &mut Response, object: &Object) {
    let resource = object_resource(object);
    r.headers.insert("ETag".to_string(), resource.etag);
    r.headers
        .insert("X-Goog-Generation".to_string(), resource.generation);
    r.headers
        .insert("X-Goog-Metageneration".to_string(), resource.metageneration);
    r.headers
        .insert("X-Goog-Stored-Content-Length".to_string(), resource.size);
    if !object.cache_control.is_empty() {
        r.headers
            .insert("Cache-Control".to_string(), object.cache_control.clone());
    }
    if !object.content_encoding.is_empty() {
        r.headers.insert(
            "Content-Encoding".to_string(),
            object.content_encoding.clone(),
        );
    }
    if !object.content_disposition.is_empty() {
        r.headers.insert(
            "Content-Disposition".to_string(),
            object.content_disposition.clone(),
        );
    }
    let mut hashes = Vec::new();
    if !object.crc32c.is_empty() {
        hashes.push(format!("crc32c={}", object.crc32c));
    }
    let md5 = md5_hash_from_etag(&object.etag);
    if !md5.is_empty() {
        hashes.push(format!("md5={md5}"));
    }
    if !hashes.is_empty() {
        r.headers
            .insert("X-Goog-Hash".to_string(), hashes.join(","));
    }
    for (key, value) in &object.metadata {
        r.headers.insert(
            canonical_header(&format!("X-Goog-Meta-{key}")),
            value.clone(),
        );
    }
}

/// legacy `textproto.CanonicalMIMEHeaderKey` form: uppercase the first letter and
/// every letter following a hyphen, lowercase the rest.
fn canonical_header(name: &str) -> String {
    let mut out = String::with_capacity(name.len());
    let mut upper = true;
    for c in name.chars() {
        if upper {
            out.push(c.to_ascii_uppercase());
        } else {
            out.push(c.to_ascii_lowercase());
        }
        upper = c == '-';
    }
    out
}

fn generation_value(object: &Object) -> i64 {
    parse_rfc3339(&object.last_modified)
        .map(|(secs, nanos)| secs.saturating_mul(1_000_000_000) + nanos as i64)
        .unwrap_or(0)
}

fn generation(object: &Object) -> String {
    generation_value(object).to_string()
}

/// The `generation` query filter (legacy `requestedGenerationMatches`): blank
/// passes, non-positive or unparsable values are `400 invalid generation`.
fn generation_matches(req: &Request, object: &Object) -> Result<bool, Response> {
    let value = req.query_value("generation").trim();
    if value.is_empty() {
        return Ok(true);
    }
    match value.parse::<i64>() {
        Ok(generation) if generation > 0 => Ok(generation == generation_value(object)),
        _ => Err(json_error(400, "invalid", "invalid generation")),
    }
}

fn gcs_etag(object: &Object) -> String {
    format!("CN{}=", generation(object))
}

fn md5_hash_from_etag(etag: &str) -> String {
    let trimmed = etag.trim_matches('"');
    match hex::decode(trimmed) {
        Ok(bytes) => base64::std_encode(&bytes),
        Err(_) => String::new(),
    }
}

fn user_metadata(headers: &BTreeMap<String, String>) -> BTreeMap<String, String> {
    headers
        .iter()
        .filter_map(|(k, v)| {
            k.strip_prefix("x-goog-meta-")
                .map(|name| (name.to_string(), v.clone()))
        })
        .collect()
}

/// legacy `mergeMetadata`: override wins; both-empty collapses to nil (`None`).
fn merge_metadata(
    base: Option<BTreeMap<String, String>>,
    override_values: BTreeMap<String, String>,
) -> Option<BTreeMap<String, String>> {
    let mut merged = base.unwrap_or_default();
    merged.extend(override_values);
    if merged.is_empty() {
        None
    } else {
        Some(merged)
    }
}

fn first_non_empty(value: &str, fallback: &str) -> String {
    if value.is_empty() {
        fallback.to_string()
    } else {
        value.to_string()
    }
}

fn page_window(
    query: &BTreeMap<String, String>,
    total: usize,
) -> Result<(usize, usize, String), String> {
    let mut start = 0usize;
    if let Some(token) = query.get("pageToken").filter(|v| !v.trim().is_empty()) {
        let offset = token
            .trim()
            .parse::<i64>()
            .ok()
            .filter(|v| *v >= 0)
            .ok_or("invalid pageToken".to_string())?;
        start = (offset as usize).min(total);
    }
    let mut limit = total - start;
    if let Some(value) = query.get("maxResults").filter(|v| !v.trim().is_empty()) {
        let parsed = value
            .trim()
            .parse::<i64>()
            .ok()
            .filter(|v| *v >= 0)
            .ok_or("invalid maxResults".to_string())?;
        if (parsed as usize) < limit {
            limit = parsed as usize;
        }
    }
    let end = (start + limit).min(total);
    let next = if end < total {
        end.to_string()
    } else {
        String::new()
    };
    Ok((start, end, next))
}

fn parse_range(header: &str, size: usize) -> Result<(usize, usize, bool), ()> {
    if header.is_empty() {
        if size == 0 {
            return Ok((0, 0, false));
        }
        return Ok((0, size - 1, false));
    }
    if size == 0 || !header.starts_with("bytes=") {
        return Err(());
    }
    let spec = &header[6..];
    let (left, right) = spec.split_once('-').ok_or(())?;
    if left.is_empty() {
        let mut suffix = right.parse::<usize>().map_err(|_| ())?;
        if suffix == 0 {
            return Err(());
        }
        suffix = suffix.min(size);
        return Ok((size - suffix, size - 1, true));
    }
    let start = left.parse::<usize>().map_err(|_| ())?;
    if start >= size {
        return Err(());
    }
    let end = if right.is_empty() {
        size - 1
    } else {
        right.parse::<usize>().map_err(|_| ())?.min(size - 1)
    };
    if end < start {
        return Err(());
    }
    Ok((start, end, true))
}

fn bucket_suffix(path: &str, prefix: &str) -> Option<(String, String)> {
    let trimmed = path.strip_prefix(prefix)?;
    if trimmed.is_empty() {
        return None;
    }
    let (bucket, suffix) = trimmed.split_once('/').unwrap_or((trimmed, ""));
    let bucket = url_decode(bucket);
    if bucket.is_empty() {
        return None;
    }
    Some((bucket, suffix.to_string()))
}

fn json_error(status: u16, reason: &str, message: &str) -> Response {
    Response::json(
        status,
        &ErrorResponse {
            error: ErrorBody {
                code: status,
                message: message.to_string(),
                errors: vec![ErrorItem {
                    domain: "global".to_string(),
                    reason: reason.to_string(),
                    message: message.to_string(),
                }],
            },
        },
    )
}

fn method_not_allowed(allow: &str) -> Response {
    let mut r = json_error(405, "methodNotAllowed", "method not allowed");
    r.headers.insert("Allow".to_string(), allow.to_string());
    r
}

/// The legacy store's error strings, as surfaced verbatim by the GCS handlers via
/// `err.Error()`.
fn store_error_text(e: &StoreError, bucket: &str, key: &str) -> String {
    match e {
        StoreError::InvalidBucketName => format!("invalid bucket name {bucket:?}"),
        StoreError::InvalidObjectKey => {
            if key.is_empty() {
                "object key is required".to_string()
            } else {
                "object key contains null byte".to_string()
            }
        }
        StoreError::BucketNotEmpty => "bucket is not empty".to_string(),
        StoreError::BucketNotExist => "bucket does not exist".to_string(),
        StoreError::ObjectLocked => "object is locked".to_string(),
        _ => "internal error".to_string(),
    }
}

fn emit_dashboard_event(event_type: &str, payload: serde_json::Value) {
    let json = serde_json::json!({
        "type": event_type,
        "service": "gcs",
        "payload": payload,
    })
    .to_string();
    if let Some(tx) = crate::event_sink() {
        let _ = tx.send(json.clone());
    }
    println!("DEVCLOUD_EVENT {json}");
}

pub async fn serve(
    listener: TcpListener,
    server: Arc<Mutex<Server>>,
    shutdown: impl Future<Output = ()>,
) -> std::io::Result<()> {
    tokio::pin!(shutdown);
    loop {
        tokio::select! {
            _ = &mut shutdown => return Ok(()),
            accepted = listener.accept() => {
                let (stream, _) = accepted?;
                let server = Arc::clone(&server);
                tokio::spawn(async move {
                    let _ = handle_conn(stream, server).await;
                });
            }
        }
    }
}

async fn handle_conn(mut stream: TcpStream, server: Arc<Mutex<Server>>) -> std::io::Result<()> {
    let request = match read_request(&mut stream).await {
        Ok(Some(req)) => req,
        _ => return Ok(()),
    };
    let response = {
        let mut guard = server.lock().unwrap();
        route(&mut guard, &request)
    };
    write_response(&mut stream, response).await
}

async fn read_request(stream: &mut TcpStream) -> std::io::Result<Option<Request>> {
    let mut buf = Vec::new();
    let mut tmp = [0u8; 4096];
    let header_end = loop {
        if let Some(pos) = find_subslice(&buf, b"\r\n\r\n") {
            break pos;
        }
        if buf.len() > MAX_HEADER_BYTES {
            return Ok(None);
        }
        let n = stream.read(&mut tmp).await?;
        if n == 0 {
            return Ok(None);
        }
        buf.extend_from_slice(&tmp[..n]);
    };
    let head = String::from_utf8_lossy(&buf[..header_end]).into_owned();
    let mut lines = head.split("\r\n");
    let request_line = lines.next().unwrap_or("");
    let mut rl = request_line.split(' ');
    let method = rl.next().unwrap_or("").to_string();
    let target = rl.next().unwrap_or("/");
    let (path, query) = parse_target(target);

    let mut headers = BTreeMap::new();
    for line in lines {
        if let Some((k, v)) = line.split_once(':') {
            headers.insert(k.trim().to_ascii_lowercase(), v.trim().to_string());
        }
    }
    let mut body = buf[header_end + 4..].to_vec();
    if headers
        .get("transfer-encoding")
        .map(|v| v.to_ascii_lowercase().contains("chunked"))
        .unwrap_or(false)
    {
        body = read_chunked_body(stream, body).await?;
    } else {
        let content_length: usize = headers
            .get("content-length")
            .and_then(|v| v.parse().ok())
            .unwrap_or(0);
        if content_length > MAX_BODY_BYTES {
            return Ok(None);
        }
        while body.len() < content_length {
            let n = stream.read(&mut tmp).await?;
            if n == 0 {
                break;
            }
            body.extend_from_slice(&tmp[..n]);
        }
        body.truncate(content_length);
    }

    Ok(Some(Request {
        method,
        path,
        query,
        headers,
        body,
    }))
}

async fn read_chunked_body(stream: &mut TcpStream, mut buf: Vec<u8>) -> std::io::Result<Vec<u8>> {
    let mut out = Vec::new();
    let mut tmp = [0u8; 4096];
    let mut pos = 0;
    loop {
        let line_end = loop {
            if let Some(i) = find_subslice(&buf[pos..], b"\r\n") {
                break pos + i;
            }
            let n = stream.read(&mut tmp).await?;
            if n == 0 {
                return Ok(out);
            }
            buf.extend_from_slice(&tmp[..n]);
        };
        let size_line = String::from_utf8_lossy(&buf[pos..line_end]);
        let size_hex = size_line.split(';').next().unwrap_or("").trim();
        let size = usize::from_str_radix(size_hex, 16).unwrap_or(0);
        pos = line_end + 2;
        if size == 0 {
            return Ok(out);
        }
        while buf.len() < pos + size + 2 {
            let n = stream.read(&mut tmp).await?;
            if n == 0 {
                return Ok(out);
            }
            buf.extend_from_slice(&tmp[..n]);
        }
        out.extend_from_slice(&buf[pos..pos + size]);
        if out.len() > MAX_BODY_BYTES {
            return Ok(Vec::new());
        }
        pos += size + 2;
    }
}

fn parse_target(target: &str) -> (String, BTreeMap<String, String>) {
    let (path, query) = match target.split_once('?') {
        Some((p, q)) => (p.to_string(), q),
        None => (target.to_string(), ""),
    };
    let mut params = BTreeMap::new();
    for pair in query.split('&') {
        if pair.is_empty() {
            continue;
        }
        let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
        params.insert(url_decode(k), url_decode(v));
    }
    (path, params)
}

fn url_decode(value: &str) -> String {
    let bytes = value.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'%' if i + 2 < bytes.len() => {
                if let Ok(b) = u8::from_str_radix(&value[i + 1..i + 3], 16) {
                    out.push(b);
                    i += 3;
                    continue;
                }
                out.push(bytes[i]);
                i += 1;
            }
            b'+' => {
                out.push(b' ');
                i += 1;
            }
            b => {
                out.push(b);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn url_encode(value: &str) -> String {
    let mut out = String::new();
    for &b in value.as_bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b'~') {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

async fn write_response(stream: &mut TcpStream, resp: Response) -> std::io::Result<()> {
    let mut head = format!(
        "HTTP/1.1 {} {}\r\nServer: devcloud-gcs\r\n",
        resp.status,
        reason_phrase(resp.status)
    );
    for (k, v) in &resp.headers {
        head.push_str(&format!("{k}: {v}\r\n"));
    }
    if !resp.headers.contains_key("Content-Length") {
        head.push_str(&format!("Content-Length: {}\r\n", resp.body.len()));
    }
    head.push_str("Connection: close\r\n\r\n");
    stream.write_all(head.as_bytes()).await?;
    stream.write_all(&resp.body).await?;
    stream.flush().await
}

fn reason_phrase(status: u16) -> &'static str {
    match status {
        200 => "OK",
        204 => "No Content",
        206 => "Partial Content",
        400 => "Bad Request",
        401 => "Unauthorized",
        404 => "Not Found",
        405 => "Method Not Allowed",
        409 => "Conflict",
        416 => "Requested Range Not Satisfiable",
        500 => "Internal Server Error",
        501 => "Not Implemented",
        _ => "Status",
    }
}

fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack.windows(needle.len()).position(|w| w == needle)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Golden oracle: `session.json` must match legacy
    /// `json.MarshalIndent(resumableSession, "", "  ")` + `\n` byte-for-byte
    /// (captured from the legacy implementation).
    #[test]
    fn session_json_matches_legacy_marshal_indent() {
        let session = ResumableSession {
            bucket: "demo".to_string(),
            name: "docs/chunked.txt".to_string(),
            content_type: "text/plain".to_string(),
            preconditions: ObjectPreconditions {
                if_generation_match: "0".to_string(),
                ..Default::default()
            },
            created_at: "2026-06-10T01:02:03.123456789Z".to_string(),
            received_bytes: 10,
            ..Default::default()
        };
        let want = concat!(
            "{\n",
            "  \"Bucket\": \"demo\",\n",
            "  \"Name\": \"docs/chunked.txt\",\n",
            "  \"ContentType\": \"text/plain\",\n",
            "  \"ContentEncoding\": \"\",\n",
            "  \"CacheControl\": \"\",\n",
            "  \"ContentDisposition\": \"\",\n",
            "  \"Metadata\": null,\n",
            "  \"Preconditions\": {\n",
            "    \"IfGenerationMatch\": \"0\",\n",
            "    \"IfGenerationNotMatch\": \"\",\n",
            "    \"IfMetagenerationMatch\": \"\",\n",
            "    \"IfMetagenerationNotMatch\": \"\"\n",
            "  },\n",
            "  \"CreatedAt\": \"2026-06-10T01:02:03.123456789Z\",\n",
            "  \"ReceivedBytes\": 10\n",
            "}\n",
        );
        assert_eq!(
            String::from_utf8(wire_json::to_vec_indent(&session)).unwrap(),
            want
        );

        let with_metadata = ResumableSession {
            metadata: Some(BTreeMap::from([
                ("b".to_string(), "<&>".to_string()),
                ("source".to_string(), "x".to_string()),
            ])),
            ..session
        };
        let want_metadata = concat!(
            "{\n",
            "  \"Bucket\": \"demo\",\n",
            "  \"Name\": \"docs/chunked.txt\",\n",
            "  \"ContentType\": \"text/plain\",\n",
            "  \"ContentEncoding\": \"\",\n",
            "  \"CacheControl\": \"\",\n",
            "  \"ContentDisposition\": \"\",\n",
            "  \"Metadata\": {\n",
            "    \"b\": \"\\u003c\\u0026\\u003e\",\n",
            "    \"source\": \"x\"\n",
            "  },\n",
            "  \"Preconditions\": {\n",
            "    \"IfGenerationMatch\": \"0\",\n",
            "    \"IfGenerationNotMatch\": \"\",\n",
            "    \"IfMetagenerationMatch\": \"\",\n",
            "    \"IfMetagenerationNotMatch\": \"\"\n",
            "  },\n",
            "  \"CreatedAt\": \"2026-06-10T01:02:03.123456789Z\",\n",
            "  \"ReceivedBytes\": 10\n",
            "}\n",
        );
        assert_eq!(
            String::from_utf8(wire_json::to_vec_indent(&with_metadata)).unwrap(),
            want_metadata
        );
    }

    /// Introspect bucket/object/session summaries must serialize in legacy struct
    /// field order with a trailing newline, and omitempty must drop empty
    /// `crc32c`/`metadata`/`contentType`.
    #[test]
    fn introspect_summaries_match_legacy_field_order() {
        let bucket = BucketSummary {
            name: "parity-bucket".to_string(),
            time_created: "2026-06-10T01:02:03.123456789Z".to_string(),
            object_count: 3,
            gcs_uri: "gs://parity-bucket".to_string(),
        };
        assert_eq!(
            String::from_utf8(introspect_json(200, &bucket).body).unwrap(),
            "{\"name\":\"parity-bucket\",\"timeCreated\":\"2026-06-10T01:02:03.123456789Z\",\"objectCount\":3,\"gcsUri\":\"gs://parity-bucket\"}\n"
        );

        // crc32c, metadata, contentType all empty -> omitted.
        let object = ObjectSummary {
            name: "docs/readme.txt".to_string(),
            size: 12,
            etag: "abc".to_string(),
            content_type: "text/plain".to_string(),
            crc32c: String::new(),
            storage_class: "STANDARD".to_string(),
            updated: "2026-06-10T01:02:03.123456789Z".to_string(),
            metadata: BTreeMap::new(),
            generation: "1749517323123456789".to_string(),
            metageneration: "1".to_string(),
            gcs_uri: "gs://b/docs/readme.txt".to_string(),
            download_url: "/api/gcs/buckets/b/objects/docs%2Freadme.txt/download".to_string(),
        };
        assert_eq!(
            String::from_utf8(introspect_json(200, &object).body).unwrap(),
            "{\"name\":\"docs/readme.txt\",\"size\":12,\"etag\":\"abc\",\"contentType\":\"text/plain\",\"storageClass\":\"STANDARD\",\"updated\":\"2026-06-10T01:02:03.123456789Z\",\"generation\":\"1749517323123456789\",\"metageneration\":\"1\",\"gcsUri\":\"gs://b/docs/readme.txt\",\"downloadUrl\":\"/api/gcs/buckets/b/objects/docs%2Freadme.txt/download\"}\n"
        );

        let session = UploadSessionSummary {
            id: "abc123".to_string(),
            bucket: "demo".to_string(),
            name: "docs/chunked.txt".to_string(),
            content_type: String::new(),
            created_at: "2026-06-10T01:02:03.123456789Z".to_string(),
            received_bytes: 10,
        };
        assert_eq!(
            String::from_utf8(introspect_json(200, &session).body).unwrap(),
            "{\"id\":\"abc123\",\"bucket\":\"demo\",\"name\":\"docs/chunked.txt\",\"createdAt\":\"2026-06-10T01:02:03.123456789Z\",\"receivedBytes\":10}\n"
        );
    }

    /// Introspect emits a bare `application/json` Content-Type (no charset),
    /// matching the established dashboard-normalized parity contract.
    #[test]
    fn introspect_json_uses_bare_content_type() {
        let r = introspect_json(
            200,
            &BucketsResponse {
                buckets: Vec::new(),
            },
        );
        assert_eq!(
            r.headers.get("Content-Type").map(String::as_str),
            Some("application/json")
        );
        assert_eq!(String::from_utf8(r.body).unwrap(), "{\"buckets\":[]}\n");
    }

    /// Empty session list serializes as `{"sessions":[]}` (legacy emits the empty
    /// slice, not `null`).
    #[test]
    fn introspect_empty_uploads_serializes_empty_array() {
        let r = introspect_json(
            200,
            &UploadsResponse {
                sessions: Vec::new(),
            },
        );
        assert_eq!(String::from_utf8(r.body).unwrap(), "{\"sessions\":[]}\n");
    }

    /// Golden oracle: error responses must match legacy
    /// `json.NewEncoder(w).Encode(errorResponse{...})` wire bytes (compact,
    /// HTML-escaped, trailing newline).
    #[test]
    fn error_response_matches_legacy_encoder_bytes() {
        let r = json_error(404, "notFound", "object not found");
        assert_eq!(
            String::from_utf8(r.body).unwrap(),
            "{\"error\":{\"code\":404,\"message\":\"object not found\",\"errors\":[{\"domain\":\"global\",\"reason\":\"notFound\",\"message\":\"object not found\"}]}}\n"
        );
    }
}
