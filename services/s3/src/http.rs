//! Minimal HTTP/1.1 socket server for the S3 Rust increment.
//!
//! Scope: service listing, bucket CRUD, current-object PUT/GET/HEAD/DELETE,
//! object listing, selected bucket/object sub-resources, multipart upload, and
//! `If-None-Match` (GET/HEAD 304, conditional PUT/Copy/CompleteMultipartUpload),
//! bucket CORS, and `aws-chunked` streaming uploads.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use crate::aws_chunked;
use crate::cors::{self, CorsRule};
use crate::model::{
    AnalyticsConfiguration, AnalyticsDataExport, AnalyticsDestination, AnalyticsFilter,
    AnalyticsS3BucketDestination, DefaultRetention, InventoryConfiguration, InventoryDestination,
    InventoryS3BucketDestination, InventorySchedule, LifecycleConfiguration, LifecycleExpiration,
    LifecycleFilter, LifecycleRule, NotificationConfiguration, NotificationEventRecord,
    NotificationFilter, NotificationFilterRule, NotificationLambdaConfig, NotificationQueueConfig,
    NotificationS3KeyFilter, NotificationTopicConfig, Object, ObjectLegalHold,
    ObjectLockConfiguration, ObjectLockRule, ObjectRetention, ReplicationConfiguration,
    ReplicationDeleteMarkerSetting, ReplicationDestination, ReplicationFilter, ReplicationRule,
    ServerSideEncryption, StorageClassAnalysis,
};
use crate::objops::{CreateMultipartUploadInput, PutObjectInput};
use crate::percent::aws_percent_encode;
use crate::responses::{
    access_control_policy, analytics_configuration, build_object_listing, build_version_listing,
    complete_multipart_upload_result, copy_object_result, decode_continuation_token,
    encode_list_value, error_xml, initiate_multipart_upload_result, inventory_configuration,
    latest_object_version_ids, lifecycle_configuration, list_all_my_buckets,
    list_analytics_configurations_result, list_inventory_configurations_result,
    list_multipart_uploads_result, list_parts_result, location_constraint,
    notification_configuration, object_legal_hold, object_lock_configuration, object_retention,
    object_version_id, paginate_parts, parse_max_keys, parse_max_parts, parse_part_number_marker,
    replication_configuration, versioning_configuration, DeleteMarkerElement, ListBucketResult,
    ListVersionsResult, ObjectElement, VersionElement,
};
use crate::store::{FileBucketStore, StoreError};
use crate::time_fmt::{parse_lifecycle_date, parse_rfc3339, rfc3339_seconds_from_unix};
use crate::validation::valid_bucket_name;

const MAX_HEADER_BYTES: usize = 64 * 1024;
const MAX_BODY_BYTES: usize = 128 * 1024 * 1024;
const MAX_POLICY_BYTES: usize = 1024 * 1024;
const SIGV4_ALGORITHM: &str = "AWS4-HMAC-SHA256";
const SIGV4_SERVICE: &str = "s3";
const SIGV4_MAX_CLOCK_SKEW_SECONDS: i64 = 900;
const SIGV4_CHUNK_ALGORITHM: &str = "AWS4-HMAC-SHA256-PAYLOAD";
const SIGV4_TRAILER_ALGORITHM: &str = "AWS4-HMAC-SHA256-TRAILER";
const STREAMING_SIGNED: &str = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD";
const STREAMING_SIGNED_TRAILER: &str = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER";
const STREAMING_UNSIGNED_TRAILER: &str = "STREAMING-UNSIGNED-PAYLOAD-TRAILER";
const DASHBOARD_EVENT_PREFIX: &str = "DEVCLOUD_EVENT ";

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
        Request {
            method: method.to_string(),
            path,
            query,
            headers: BTreeMap::new(),
            body,
        }
    }

    pub fn header(&self, name: &str) -> &str {
        self.headers
            .get(&name.to_ascii_lowercase())
            .map(String::as_str)
            .unwrap_or("")
    }
}

#[derive(Debug, Clone)]
pub struct Response {
    pub status: u16,
    pub headers: BTreeMap<String, String>,
    pub body: Vec<u8>,
}

impl Response {
    fn new(status: u16, body: Vec<u8>) -> Self {
        Response {
            status,
            headers: BTreeMap::new(),
            body,
        }
    }

    fn xml(status: u16, body: Vec<u8>) -> Self {
        let mut r = Self::new(status, body);
        r.headers
            .insert("Content-Type".to_string(), "application/xml".to_string());
        r
    }

    fn empty(status: u16) -> Self {
        Self::new(status, Vec::new())
    }

    /// Public constructor used by the introspection module, which assembles its
    /// own JSON bodies and Content-Type header.
    pub fn with_body(status: u16, body: Vec<u8>) -> Self {
        Self::new(status, body)
    }
}

#[derive(Debug, Clone)]
pub struct AuthConfig {
    pub auth_mode: String,
    pub access_key_id: String,
    pub secret_access_key: String,
    pub region: String,
}

impl Default for AuthConfig {
    fn default() -> Self {
        Self {
            auth_mode: "relaxed".to_string(),
            access_key_id: "dev".to_string(),
            secret_access_key: "dev".to_string(),
            region: "us-east-1".to_string(),
        }
    }
}

/// Runs the accept loop until `shutdown` resolves.
pub async fn serve(
    listener: TcpListener,
    store: Arc<Mutex<FileBucketStore>>,
    shutdown: impl std::future::Future<Output = ()>,
) -> std::io::Result<()> {
    serve_with_auth(listener, store, AuthConfig::default(), shutdown).await
}

/// Runs the accept loop with explicit SigV4/auth settings and no server-wide
/// CORS rule; bucket CORS configurations still apply.
pub async fn serve_with_auth(
    listener: TcpListener,
    store: Arc<Mutex<FileBucketStore>>,
    auth: AuthConfig,
    shutdown: impl std::future::Future<Output = ()>,
) -> std::io::Result<()> {
    serve_with_cors(listener, store, auth, None, shutdown).await
}

/// Runs the accept loop. `default_cors` applies to buckets without their own
/// CORS configuration and to the service root.
pub async fn serve_with_cors(
    listener: TcpListener,
    store: Arc<Mutex<FileBucketStore>>,
    auth: AuthConfig,
    default_cors: Option<CorsRule>,
    shutdown: impl std::future::Future<Output = ()>,
) -> std::io::Result<()> {
    tokio::pin!(shutdown);
    let auth = Arc::new(auth);
    let default_cors = Arc::new(default_cors);
    loop {
        tokio::select! {
            _ = &mut shutdown => return Ok(()),
            accepted = listener.accept() => {
                let (stream, _) = match accepted {
                    Ok(a) => a,
                    // Out of descriptors, or a peer gone before the accept:
                    // back off and keep serving instead of stopping (which
                    // would take every devcloud service down with it).
                    Err(e) => {
                        if !is_transient_accept_error(&e) {
                            return Err(e);
                        }
                        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                        continue;
                    }
                };
                let store = Arc::clone(&store);
                let auth = Arc::clone(&auth);
                let default_cors = Arc::clone(&default_cors);
                tokio::spawn(async move {
                    let _ = handle_conn(stream, store, auth, default_cors).await;
                });
            }
        }
    }
}

/// Whether an accept failure should be retried. A broken listener still stops.
pub(crate) fn is_transient_accept_error(error: &std::io::Error) -> bool {
    matches!(
        error.kind(),
        std::io::ErrorKind::ConnectionAborted
            | std::io::ErrorKind::ConnectionReset
            | std::io::ErrorKind::Interrupted
            | std::io::ErrorKind::WouldBlock
            | std::io::ErrorKind::TimedOut
    ) || matches!(error.raw_os_error(), Some(12 | 23 | 24)) // ENOMEM, ENFILE, EMFILE
}

async fn handle_conn(
    mut stream: TcpStream,
    store: Arc<Mutex<FileBucketStore>>,
    auth: Arc<AuthConfig>,
    default_cors: Arc<Option<CorsRule>>,
) -> std::io::Result<()> {
    let request = match read_request(&mut stream).await {
        Ok(Some(req)) => req,
        _ => return Ok(()),
    };
    let response = {
        let store = store.lock().unwrap();
        route_with_cors(&store, &request, &auth, default_cors.as_ref().as_ref())
    };
    write_response(&mut stream, response).await
}

pub(crate) async fn read_request<S: AsyncRead + Unpin>(
    stream: &mut S,
) -> std::io::Result<Option<Request>> {
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
    let mut request_parts = request_line.split(' ');
    let method = request_parts.next().unwrap_or("").to_string();
    let target = request_parts.next().unwrap_or("/");
    let (path, query) = parse_target(target);

    let mut headers = BTreeMap::new();
    for line in lines {
        if let Some((k, v)) = line.split_once(':') {
            // Repeated headers fold with a comma, which SigV4 canonicalization also expects.
            headers
                .entry(k.trim().to_ascii_lowercase())
                .and_modify(|existing: &mut String| {
                    existing.push(',');
                    existing.push_str(v.trim());
                })
                .or_insert_with(|| v.trim().to_string());
        }
    }

    let received = buf[header_end + 4..].to_vec();
    let is_chunked = headers
        .get("transfer-encoding")
        .is_some_and(|v| v.to_ascii_lowercase().contains("chunked"));
    let body = if is_chunked {
        read_http_chunked_body(stream, received).await?
    } else {
        read_sized_body(stream, received, headers.get("content-length")).await?
    };
    let Some(body) = body else {
        return Ok(None);
    };

    Ok(Some(Request {
        method,
        path,
        query,
        headers,
        body,
    }))
}

/// A repeated `Content-Length` is only valid when every value agrees (RFC 9110).
/// Anything else is refused rather than read as an empty body, which would
/// overwrite an object with zero bytes.
fn parse_content_length(value: Option<&String>) -> Option<usize> {
    let Some(value) = value else {
        return Some(0);
    };
    let mut lengths = value.split(',').map(|v| v.trim().parse::<usize>().ok());
    let first = lengths.next()??;
    lengths.all(|v| v == Some(first)).then_some(first)
}

async fn read_sized_body<S: AsyncRead + Unpin>(
    stream: &mut S,
    mut body: Vec<u8>,
    content_length: Option<&String>,
) -> std::io::Result<Option<Vec<u8>>> {
    let Some(content_length) = parse_content_length(content_length) else {
        return Ok(None);
    };
    if content_length > MAX_BODY_BYTES {
        return Ok(None);
    }
    let mut tmp = [0u8; 4096];
    while body.len() < content_length {
        let n = stream.read(&mut tmp).await?;
        if n == 0 {
            break;
        }
        body.extend_from_slice(&tmp[..n]);
    }
    body.truncate(content_length);
    Ok(Some(body))
}

/// Reads more bytes into `raw`, returning false at end of stream.
async fn fill<S: AsyncRead + Unpin>(stream: &mut S, raw: &mut Vec<u8>) -> std::io::Result<bool> {
    let mut tmp = [0u8; 4096];
    let n = stream.read(&mut tmp).await?;
    raw.extend_from_slice(&tmp[..n]);
    Ok(n > 0)
}

/// Removes and returns the next CRLF-terminated line; `None` at end of stream or
/// when a line exceeds the header limit.
async fn read_chunk_line<S: AsyncRead + Unpin>(
    stream: &mut S,
    raw: &mut Vec<u8>,
) -> std::io::Result<Option<String>> {
    loop {
        if let Some(end) = find_subslice(raw, b"\r\n") {
            let line = String::from_utf8_lossy(&raw[..end]).into_owned();
            raw.drain(..end + 2);
            return Ok(Some(line));
        }
        if raw.len() > MAX_HEADER_BYTES || !fill(stream, raw).await? {
            return Ok(None);
        }
    }
}

/// Decodes `Transfer-Encoding: chunked`. An `aws-chunked` payload sent this way
/// still carries its own framing, which routing decodes afterwards.
async fn read_http_chunked_body<S: AsyncRead + Unpin>(
    stream: &mut S,
    mut raw: Vec<u8>,
) -> std::io::Result<Option<Vec<u8>>> {
    let mut body = Vec::new();
    loop {
        let Some(line) = read_chunk_line(stream, &mut raw).await? else {
            return Ok(None);
        };
        let size = line.split(';').next().unwrap_or("").trim();
        let Ok(size) = usize::from_str_radix(size, 16) else {
            return Ok(None);
        };
        if size == 0 {
            break;
        }
        if size > MAX_BODY_BYTES - body.len() {
            return Ok(None);
        }
        while raw.len() < size + 2 {
            if !fill(stream, &mut raw).await? {
                return Ok(None);
            }
        }
        if &raw[size..size + 2] != b"\r\n" {
            return Ok(None);
        }
        body.extend_from_slice(&raw[..size]);
        raw.drain(..size + 2);
    }
    loop {
        match read_chunk_line(stream, &mut raw).await? {
            Some(line) if line.is_empty() => return Ok(Some(body)),
            Some(_) => {}
            None => return Ok(None),
        }
    }
}

/// Routes a parsed request to the S3 store.
pub fn route(store: &FileBucketStore, req: &Request) -> Response {
    route_with_auth(store, req, &AuthConfig::default())
}

/// Routes with SigV4 verification when `auth.auth_mode == "strict"`.
pub fn route_with_auth(store: &FileBucketStore, req: &Request, auth: &AuthConfig) -> Response {
    route_with_cors(store, req, auth, None)
}

/// Routes with SigV4 verification and CORS handling. Browsers never attach
/// credentials to a preflight, so `OPTIONS` is answered before signature checks.
pub fn route_with_cors(
    store: &FileBucketStore,
    req: &Request,
    auth: &AuthConfig,
    default_cors: Option<&CorsRule>,
) -> Response {
    if crate::introspect::is_introspect_path(&req.path) {
        return route_verified(store, req, auth);
    }
    if req.method == "OPTIONS" {
        return cors_preflight(store, req, default_cors);
    }
    let origin = req.header("origin");
    if origin.is_empty() {
        return route_verified(store, req, auth);
    }
    // Read before routing: DeleteBucket removes the configuration it is answered under.
    let rules = cors_rules(store, &req.path, default_cors);
    let mut response = route_verified(store, req, auth);
    if !rules.is_empty() {
        response
            .headers
            .insert("Vary".to_string(), CORS_VARY.to_string());
    }
    if let Some(rule) = cors::find_rule(&rules, origin, &req.method, &[]) {
        response
            .headers
            .extend(cors::response_headers(rule, origin));
    }
    response
}

const CORS_VARY: &str = "Origin, Access-Control-Request-Headers, Access-Control-Request-Method";

fn cors_rules(
    store: &FileBucketStore,
    path: &str,
    default_cors: Option<&CorsRule>,
) -> Vec<CorsRule> {
    let bucket_config = parse_path_style(path)
        .and_then(|(bucket, _)| store.get_bucket_cors(&bucket).ok().flatten());
    match bucket_config {
        Some(config) => config.rules,
        None => default_cors.cloned().into_iter().collect(),
    }
}

fn cors_preflight(
    store: &FileBucketStore,
    req: &Request,
    default_cors: Option<&CorsRule>,
) -> Response {
    let origin = req.header("origin");
    if origin.is_empty() {
        return xml_error(
            400,
            "BadRequest",
            "Insufficient information. Origin request header needed.",
        );
    }
    let method = req.header("access-control-request-method");
    if method.is_empty() {
        return xml_error(
            400,
            "BadRequest",
            "Invalid Access-Control-Request-Method: null",
        );
    }
    let rules = cors_rules(store, &req.path, default_cors);
    if rules.is_empty() {
        let bucket_missing = parse_path_style(&req.path)
            .is_some_and(|(bucket, _)| !matches!(store.get_bucket(&bucket), Ok(Some(_))));
        return if bucket_missing {
            xml_error(404, "NoSuchBucket", "bucket does not exist")
        } else {
            xml_error(403, "CORSResponse", "CORS is not enabled for this bucket.")
        };
    }
    let requested_headers: Vec<&str> = req
        .header("access-control-request-headers")
        .split(',')
        .map(str::trim)
        .filter(|h| !h.is_empty())
        .collect();
    let Some(rule) = cors::find_rule(&rules, origin, method, &requested_headers) else {
        return xml_error(
            403,
            "CORSResponse",
            "This CORS request is not allowed. This is usually because the evaluation of \
             Origin, request method / Access-Control-Request-Method or \
             Access-Control-Request-Headers are not whitelisted by the resource's CORS spec.",
        );
    };
    let mut response = Response::empty(200);
    response
        .headers
        .extend(cors::response_headers(rule, origin));
    response
        .headers
        .insert("Vary".to_string(), CORS_VARY.to_string());
    if !requested_headers.is_empty() {
        response.headers.insert(
            "Access-Control-Allow-Headers".to_string(),
            requested_headers.join(", ").to_ascii_lowercase(),
        );
    }
    response
}

fn route_verified(store: &FileBucketStore, req: &Request, auth: &AuthConfig) -> Response {
    let chunk_signing = match verify_signature(req, auth) {
        Ok(chunk_signing) => chunk_signing,
        Err(err) => return xml_error(err.status, err.code, err.code),
    };
    // Decoded in every auth mode: otherwise the chunk framing would be stored as
    // part of the object.
    if req.header("x-amz-content-sha256").starts_with("STREAMING-") {
        return match decode_streaming_request(req, chunk_signing.as_ref()) {
            Ok(decoded) => dispatch(store, &decoded),
            Err(err) => xml_error(err.status, err.code, err.code),
        };
    }
    dispatch(store, req)
}

fn dispatch(store: &FileBucketStore, req: &Request) -> Response {
    if req.path == "/" {
        return handle_service(store, req);
    }
    // The read-only introspection API is namespaced under "/_introspect/" and
    // intercepted BEFORE provider-protocol dispatch, so its paths can never be
    // mistaken for path-style "/{bucket}/{key}" access.
    if crate::introspect::is_introspect_path(&req.path) {
        return crate::introspect::handle_introspect(store, req);
    }
    let Some((bucket, key)) = parse_path_style(&req.path) else {
        return xml_error(501, "NotImplemented", "operation is not implemented");
    };
    if !key.is_empty() {
        return handle_object(store, req, &bucket, &key);
    }
    handle_bucket(store, req, &bucket)
}

fn handle_service(store: &FileBucketStore, req: &Request) -> Response {
    if req.method != "GET" {
        return method_not_allowed("GET");
    }
    match store.list_buckets() {
        Ok(buckets) => {
            let values: Vec<(String, String)> = buckets
                .into_iter()
                .map(|b| (b.name, to_rfc3339_seconds(&b.created_at)))
                .collect();
            Response::xml(200, list_all_my_buckets(&values))
        }
        Err(_) => xml_error(500, "InternalError", "internal error"),
    }
}

fn handle_bucket(store: &FileBucketStore, req: &Request, bucket: &str) -> Response {
    match req.method.as_str() {
        "PUT" => {
            if req.query.contains_key("versioning") {
                return put_bucket_versioning(store, req, bucket);
            }
            if req.query.contains_key("acl") {
                return put_bucket_acl(store, req, bucket);
            }
            if req.query.contains_key("policy") {
                return put_bucket_policy(store, req, bucket);
            }
            if req.query.contains_key("cors") {
                return put_bucket_cors(store, req, bucket);
            }
            if req.query.contains_key("lifecycle") {
                return put_bucket_lifecycle(store, req, bucket);
            }
            if req.query.contains_key("object-lock") {
                return put_bucket_object_lock(store, req, bucket);
            }
            if req.query.contains_key("notification") {
                return put_bucket_notification(store, req, bucket);
            }
            if req.query.contains_key("inventory") {
                return put_bucket_inventory(store, req, bucket);
            }
            if req.query.contains_key("analytics") {
                return put_bucket_analytics(store, req, bucket);
            }
            if req.query.contains_key("replication") {
                return put_bucket_replication(store, req, bucket);
            }
            match store.create_bucket(bucket) {
                Ok((_, true)) => {
                    emit_dashboard_event(
                        "s3.bucket.created",
                        serde_json::json!({ "bucket": bucket }),
                    );
                    let mut r = Response::empty(200);
                    r.headers
                        .insert("Location".to_string(), format!("/{bucket}"));
                    r
                }
                Ok((_, false)) => {
                    xml_error(409, "BucketAlreadyOwnedByYou", "bucket already exists")
                }
                Err(StoreError::InvalidBucketName) => {
                    xml_error(400, "InvalidBucketName", "invalid bucket name")
                }
                Err(_) => xml_error(500, "InternalError", "internal error"),
            }
        }
        "HEAD" => match store.get_bucket(bucket) {
            Ok(Some(_)) => Response::empty(200),
            Ok(None) => Response::empty(404),
            Err(StoreError::InvalidBucketName) => Response::empty(400),
            Err(_) => Response::empty(500),
        },
        "GET" => {
            if req.query.contains_key("location") {
                return get_bucket_location(store, bucket);
            }
            if req.query.contains_key("versioning") {
                return get_bucket_versioning(store, bucket);
            }
            if req.query.contains_key("acl") {
                return get_bucket_acl(store, bucket);
            }
            if req.query.contains_key("policy") {
                return get_bucket_policy(store, bucket);
            }
            if req.query.contains_key("cors") {
                return get_bucket_cors(store, bucket);
            }
            if req.query.contains_key("lifecycle") {
                return get_bucket_lifecycle(store, bucket);
            }
            if req.query.contains_key("object-lock") {
                return get_bucket_object_lock(store, bucket);
            }
            if req.query.contains_key("versions") {
                return list_object_versions(store, req, bucket);
            }
            if req.query.contains_key("notification") {
                return get_bucket_notification(store, bucket);
            }
            if req.query.contains_key("inventory") {
                return get_bucket_inventory(store, req, bucket);
            }
            if req.query.contains_key("analytics") {
                return get_bucket_analytics(store, req, bucket);
            }
            if req.query.contains_key("replication") {
                return get_bucket_replication(store, bucket);
            }
            if req.query.contains_key("uploads") {
                return list_multipart_uploads(store, bucket);
            }
            list_objects(store, req, bucket)
        }
        "POST" => method_not_allowed("PUT, HEAD, GET, DELETE"),
        "DELETE" => {
            if req.query.contains_key("policy") {
                return delete_bucket_policy(store, bucket);
            }
            if req.query.contains_key("cors") {
                return delete_bucket_cors(store, bucket);
            }
            if req.query.contains_key("lifecycle") {
                return delete_bucket_lifecycle(store, bucket);
            }
            if req.query.contains_key("object-lock") {
                return delete_bucket_object_lock(store, bucket);
            }
            if req.query.contains_key("inventory") {
                return delete_bucket_inventory(store, req, bucket);
            }
            if req.query.contains_key("analytics") {
                return delete_bucket_analytics(store, req, bucket);
            }
            if req.query.contains_key("replication") {
                return delete_bucket_replication(store, bucket);
            }
            match store.delete_bucket(bucket) {
                Ok(true) => {
                    emit_dashboard_event(
                        "s3.bucket.deleted",
                        serde_json::json!({ "bucket": bucket }),
                    );
                    Response::empty(204)
                }
                Ok(false) => xml_error(404, "NoSuchBucket", "bucket does not exist"),
                Err(StoreError::BucketNotEmpty) => {
                    xml_error(409, "BucketNotEmpty", "bucket is not empty")
                }
                Err(StoreError::InvalidBucketName) => {
                    xml_error(400, "InvalidBucketName", "invalid bucket name")
                }
                Err(_) => xml_error(500, "InternalError", "internal error"),
            }
        }
        _ => method_not_allowed("PUT, HEAD, GET, DELETE"),
    }
}

fn handle_object(store: &FileBucketStore, req: &Request, bucket: &str, key: &str) -> Response {
    match req.method.as_str() {
        "POST" => {
            if req.query.contains_key("select") {
                return select_object_content(store, req, bucket, key);
            }
            if req.query.contains_key("uploads") {
                return create_multipart_upload(store, req, bucket, key);
            }
            if let Some(upload_id) = req.query.get("uploadId") {
                return complete_multipart_upload(store, req, bucket, key, upload_id);
            }
            method_not_allowed("PUT, HEAD, GET, DELETE, POST")
        }
        "PUT" => {
            if req.query.contains_key("acl") {
                return put_object_acl(store, req, bucket, key);
            }
            if req.query.contains_key("retention") {
                return put_object_retention(store, req, bucket, key);
            }
            if req.query.contains_key("legal-hold") {
                return put_object_legal_hold(store, req, bucket, key);
            }
            if let Some(upload_id) = req.query.get("uploadId") {
                return upload_part(store, req, bucket, key, upload_id);
            }
            if !req.header("x-amz-copy-source").is_empty() {
                return copy_object(store, req, bucket, key);
            }
            put_object(store, req, bucket, key)
        }
        "GET" => {
            if req.query.contains_key("acl") {
                return get_object_acl(store, req, bucket, key);
            }
            if req.query.contains_key("retention") {
                return get_object_retention(store, req, bucket, key);
            }
            if req.query.contains_key("legal-hold") {
                return get_object_legal_hold(store, req, bucket, key);
            }
            if let Some(upload_id) = req.query.get("uploadId") {
                return list_parts(store, req, bucket, key, upload_id);
            }
            get_object(store, req, bucket, key, false)
        }
        "HEAD" => get_object(store, req, bucket, key, true),
        "DELETE" => {
            if let Some(upload_id) = req.query.get("uploadId") {
                return abort_multipart_upload(store, bucket, key, upload_id);
            }
            let Some(bypass_governance) = bypass_governance_from_header(req) else {
                return xml_error(
                    400,
                    "InvalidArgument",
                    "bypass governance retention header is invalid",
                );
            };
            if let Some(version_id) = req.query.get("versionId").filter(|v| !v.is_empty()) {
                return delete_object_version(store, bucket, key, version_id, bypass_governance);
            }
            match store.delete_object_with_result(bucket, key, bypass_governance) {
                Ok((object, deleted)) => {
                    let mut r = Response::empty(204);
                    if !deleted {
                        return r;
                    }
                    let event_name = if object.delete_marker {
                        "s3:ObjectRemoved:DeleteMarkerCreated"
                    } else {
                        "s3:ObjectRemoved:Delete"
                    };
                    if record_object_event(store, bucket, key, event_name, &object).is_err() {
                        return xml_error(500, "InternalError", "internal error");
                    }
                    if object.delete_marker
                        && replicate_object_delete_marker(store, bucket, key).is_err()
                    {
                        return xml_error(500, "InternalError", "internal error");
                    }
                    if !object.version_id.is_empty() {
                        r.headers
                            .insert("x-amz-version-id".to_string(), object.version_id);
                    }
                    if object.delete_marker {
                        r.headers
                            .insert("x-amz-delete-marker".to_string(), "true".to_string());
                    }
                    emit_dashboard_event(
                        "s3.object.deleted",
                        serde_json::json!({ "bucket": bucket, "key": key }),
                    );
                    r
                }
                Err(StoreError::BucketNotExist) => {
                    xml_error(404, "NoSuchBucket", "bucket does not exist")
                }
                Err(StoreError::ObjectLocked) => {
                    xml_error(403, "AccessDenied", "object is protected by Object Lock")
                }
                Err(StoreError::InvalidVersionId) => {
                    xml_error(400, "InvalidArgument", "invalid version id")
                }
                Err(StoreError::InvalidBucketName) | Err(StoreError::InvalidObjectKey) => {
                    xml_error(400, "InvalidArgument", "invalid object key")
                }
                Err(_) => xml_error(500, "InternalError", "internal error"),
            }
        }
        _ => method_not_allowed("PUT, HEAD, GET, DELETE"),
    }
}

fn delete_object_version(
    store: &FileBucketStore,
    bucket: &str,
    key: &str,
    version_id: &str,
    bypass_governance: bool,
) -> Response {
    match store.delete_object_version(bucket, key, version_id, bypass_governance) {
        Ok((object, true)) => {
            let mut r = Response::empty(204);
            if !object.version_id.is_empty() {
                r.headers
                    .insert("x-amz-version-id".to_string(), object.version_id.clone());
            }
            if object.delete_marker {
                r.headers
                    .insert("x-amz-delete-marker".to_string(), "true".to_string());
            }
            if record_object_event(store, bucket, key, "s3:ObjectRemoved:Delete", &object).is_err()
            {
                return xml_error(500, "InternalError", "internal error");
            }
            emit_dashboard_event(
                "s3.object.deleted",
                serde_json::json!({ "bucket": bucket, "key": key }),
            );
            r
        }
        Ok((_, false)) => Response::empty(204),
        Err(StoreError::ObjectLocked) => {
            xml_error(403, "AccessDenied", "object is protected by Object Lock")
        }
        Err(StoreError::BucketNotExist) => xml_error(404, "NoSuchBucket", "bucket does not exist"),
        Err(StoreError::InvalidVersionId) => {
            xml_error(400, "InvalidArgument", "invalid version id")
        }
        Err(StoreError::InvalidBucketName) | Err(StoreError::InvalidObjectKey) => {
            xml_error(400, "InvalidArgument", "invalid object key")
        }
        Err(_) => xml_error(500, "InternalError", "internal error"),
    }
}

fn put_bucket_versioning(store: &FileBucketStore, req: &Request, bucket: &str) -> Response {
    let Some(status) = tag_text(&req.body, "Status") else {
        return xml_error(
            400,
            "MalformedXML",
            "versioning status must be Enabled or Suspended",
        );
    };
    if status != "Enabled" && status != "Suspended" {
        return xml_error(
            400,
            "MalformedXML",
            "versioning status must be Enabled or Suspended",
        );
    }
    match store.put_bucket_versioning(bucket, &status) {
        Ok(()) => Response::empty(200),
        Err(StoreError::BucketNotExist) => xml_error(404, "NoSuchBucket", "bucket does not exist"),
        Err(StoreError::InvalidBucketName) => {
            xml_error(400, "InvalidBucketName", "invalid bucket name")
        }
        Err(_) => xml_error(500, "InternalError", "internal error"),
    }
}

fn get_bucket_versioning(store: &FileBucketStore, bucket: &str) -> Response {
    match store.get_bucket_versioning(bucket) {
        Ok((status, true)) => Response::xml(200, versioning_configuration(&status)),
        Ok((_, false)) => xml_error(404, "NoSuchBucket", "bucket does not exist"),
        Err(StoreError::InvalidBucketName) => {
            xml_error(400, "InvalidBucketName", "invalid bucket name")
        }
        Err(_) => xml_error(500, "InternalError", "internal error"),
    }
}

fn get_bucket_location(store: &FileBucketStore, bucket: &str) -> Response {
    match store.get_bucket(bucket) {
        Ok(Some(_)) => Response::xml(200, location_constraint("")),
        Ok(None) => xml_error(404, "NoSuchBucket", "bucket does not exist"),
        Err(StoreError::InvalidBucketName) => {
            xml_error(400, "InvalidBucketName", "invalid bucket name")
        }
        Err(_) => xml_error(500, "InternalError", "internal error"),
    }
}

fn put_bucket_acl(store: &FileBucketStore, req: &Request, bucket: &str) -> Response {
    let Some(acl) = acl_from_request(req) else {
        return xml_error(400, "MalformedACLError", "bucket ACL is malformed");
    };
    match store.put_bucket_acl(bucket, &acl) {
        Ok(()) => Response::empty(200),
        Err(StoreError::BucketNotExist) => xml_error(404, "NoSuchBucket", "bucket does not exist"),
        Err(StoreError::InvalidBucketName) => {
            xml_error(400, "InvalidBucketName", "invalid bucket name")
        }
        Err(_) => xml_error(500, "InternalError", "internal error"),
    }
}

fn get_bucket_acl(store: &FileBucketStore, bucket: &str) -> Response {
    match store.get_bucket_acl(bucket) {
        Ok(Some(acl)) => Response::xml(200, access_control_policy(&acl)),
        Ok(None) => xml_error(404, "NoSuchBucket", "bucket does not exist"),
        Err(StoreError::InvalidBucketName) => {
            xml_error(400, "InvalidBucketName", "invalid bucket name")
        }
        Err(_) => xml_error(500, "InternalError", "internal error"),
    }
}

fn put_bucket_policy(store: &FileBucketStore, req: &Request, bucket: &str) -> Response {
    if req.body.len() > MAX_POLICY_BYTES
        || serde_json::from_slice::<serde_json::Value>(&req.body).is_err()
    {
        return xml_error(400, "MalformedPolicy", "bucket policy must be valid JSON");
    }
    match store.put_bucket_policy(bucket, &req.body) {
        Ok(()) => Response::empty(204),
        Err(StoreError::BucketNotExist) => xml_error(404, "NoSuchBucket", "bucket does not exist"),
        Err(StoreError::InvalidBucketName) => {
            xml_error(400, "InvalidBucketName", "invalid bucket name")
        }
        Err(_) => xml_error(500, "InternalError", "internal error"),
    }
}

fn get_bucket_policy(store: &FileBucketStore, bucket: &str) -> Response {
    match store.get_bucket_policy(bucket) {
        Ok(Some((policy, true))) => {
            let mut r = Response::new(200, policy);
            r.headers
                .insert("Content-Type".to_string(), "application/json".to_string());
            r
        }
        Ok(Some((_, false))) => {
            xml_error(404, "NoSuchBucketPolicy", "bucket policy does not exist")
        }
        Ok(None) => xml_error(404, "NoSuchBucket", "bucket does not exist"),
        Err(StoreError::InvalidBucketName) => {
            xml_error(400, "InvalidBucketName", "invalid bucket name")
        }
        Err(_) => xml_error(500, "InternalError", "internal error"),
    }
}

fn delete_bucket_policy(store: &FileBucketStore, bucket: &str) -> Response {
    match store.delete_bucket_policy(bucket) {
        Ok(_) => Response::empty(204),
        Err(StoreError::BucketNotExist) => xml_error(404, "NoSuchBucket", "bucket does not exist"),
        Err(StoreError::InvalidBucketName) => {
            xml_error(400, "InvalidBucketName", "invalid bucket name")
        }
        Err(_) => xml_error(500, "InternalError", "internal error"),
    }
}

fn put_bucket_cors(store: &FileBucketStore, req: &Request, bucket: &str) -> Response {
    if req.body.len() > MAX_POLICY_BYTES {
        return xml_error(400, "MalformedXML", "CORS configuration is too large");
    }
    let config = match cors::parse_configuration(&req.body) {
        Ok(config) => config,
        Err(message) => return xml_error(400, "MalformedXML", message),
    };
    match store.put_bucket_cors(bucket, &config) {
        Ok(()) => Response::empty(200),
        Err(StoreError::BucketNotExist) => xml_error(404, "NoSuchBucket", "bucket does not exist"),
        Err(StoreError::InvalidBucketName) => {
            xml_error(400, "InvalidBucketName", "invalid bucket name")
        }
        Err(_) => xml_error(500, "InternalError", "internal error"),
    }
}

fn get_bucket_cors(store: &FileBucketStore, bucket: &str) -> Response {
    match store.get_bucket(bucket) {
        Ok(Some(_)) => {}
        Ok(None) => return xml_error(404, "NoSuchBucket", "bucket does not exist"),
        Err(StoreError::InvalidBucketName) => {
            return xml_error(400, "InvalidBucketName", "invalid bucket name")
        }
        Err(_) => return xml_error(500, "InternalError", "internal error"),
    }
    match store.get_bucket_cors(bucket) {
        Ok(Some(config)) => Response::xml(200, cors::configuration_xml(&config)),
        Ok(None) => xml_error(
            404,
            "NoSuchCORSConfiguration",
            "the CORS configuration does not exist",
        ),
        Err(_) => xml_error(500, "InternalError", "internal error"),
    }
}

fn delete_bucket_cors(store: &FileBucketStore, bucket: &str) -> Response {
    match store.delete_bucket_cors(bucket) {
        Ok(()) => Response::empty(204),
        Err(StoreError::BucketNotExist) => xml_error(404, "NoSuchBucket", "bucket does not exist"),
        Err(StoreError::InvalidBucketName) => {
            xml_error(400, "InvalidBucketName", "invalid bucket name")
        }
        Err(_) => xml_error(500, "InternalError", "internal error"),
    }
}

fn put_bucket_lifecycle(store: &FileBucketStore, req: &Request, bucket: &str) -> Response {
    let lifecycle = match parse_lifecycle_configuration(&req.body) {
        Ok(config) => config,
        Err(LifecycleParseError::Unsupported) => {
            return xml_error(501, "NotImplemented", "unsupported lifecycle rule action");
        }
        Err(LifecycleParseError::Malformed) => {
            return xml_error(400, "MalformedXML", "lifecycle configuration is malformed");
        }
    };
    match store.put_bucket_lifecycle(bucket, &lifecycle) {
        Ok(()) => Response::empty(200),
        Err(StoreError::BucketNotExist) => xml_error(404, "NoSuchBucket", "bucket does not exist"),
        Err(StoreError::InvalidBucketName) => {
            xml_error(400, "InvalidBucketName", "invalid bucket name")
        }
        Err(_) => xml_error(500, "InternalError", "internal error"),
    }
}

fn get_bucket_lifecycle(store: &FileBucketStore, bucket: &str) -> Response {
    match store.get_bucket_lifecycle(bucket) {
        Ok(Some((config, true))) => Response::xml(200, lifecycle_configuration(&config)),
        Ok(Some((_, false))) => xml_error(
            404,
            "NoSuchLifecycleConfiguration",
            "bucket lifecycle does not exist",
        ),
        Ok(None) => xml_error(404, "NoSuchBucket", "bucket does not exist"),
        Err(StoreError::InvalidBucketName) => {
            xml_error(400, "InvalidBucketName", "invalid bucket name")
        }
        Err(_) => xml_error(500, "InternalError", "internal error"),
    }
}

fn delete_bucket_lifecycle(store: &FileBucketStore, bucket: &str) -> Response {
    match store.delete_bucket_lifecycle(bucket) {
        Ok(_) => Response::empty(204),
        Err(StoreError::BucketNotExist) => xml_error(404, "NoSuchBucket", "bucket does not exist"),
        Err(StoreError::InvalidBucketName) => {
            xml_error(400, "InvalidBucketName", "invalid bucket name")
        }
        Err(_) => xml_error(500, "InternalError", "internal error"),
    }
}

fn put_bucket_object_lock(store: &FileBucketStore, req: &Request, bucket: &str) -> Response {
    let config = match parse_object_lock_configuration(&req.body) {
        Some(config) if validate_object_lock_configuration(&config) => config,
        _ => {
            return xml_error(
                400,
                "InvalidArgument",
                "object lock configuration is invalid",
            );
        }
    };
    match store.put_bucket_object_lock_configuration(bucket, config) {
        Ok(()) => Response::empty(200),
        Err(StoreError::BucketNotExist) => xml_error(404, "NoSuchBucket", "bucket does not exist"),
        Err(StoreError::InvalidBucketName) => {
            xml_error(400, "InvalidBucketName", "invalid bucket name")
        }
        Err(_) => xml_error(500, "InternalError", "internal error"),
    }
}

fn get_bucket_object_lock(store: &FileBucketStore, bucket: &str) -> Response {
    match store.get_bucket_object_lock_configuration(bucket) {
        Ok(Some((config, true))) => Response::xml(200, object_lock_configuration(&config)),
        Ok(Some((_, false))) => xml_error(
            404,
            "ObjectLockConfigurationNotFoundError",
            "object lock configuration does not exist",
        ),
        Ok(None) => xml_error(404, "NoSuchBucket", "bucket does not exist"),
        Err(StoreError::InvalidBucketName) => {
            xml_error(400, "InvalidBucketName", "invalid bucket name")
        }
        Err(_) => xml_error(500, "InternalError", "internal error"),
    }
}

fn delete_bucket_object_lock(store: &FileBucketStore, bucket: &str) -> Response {
    match store.delete_bucket_object_lock_configuration(bucket) {
        Ok(_) => Response::empty(204),
        Err(StoreError::BucketNotExist) => xml_error(404, "NoSuchBucket", "bucket does not exist"),
        Err(StoreError::InvalidBucketName) => {
            xml_error(400, "InvalidBucketName", "invalid bucket name")
        }
        Err(_) => xml_error(500, "InternalError", "internal error"),
    }
}

fn put_bucket_notification(store: &FileBucketStore, req: &Request, bucket: &str) -> Response {
    let config = match parse_notification_configuration(&req.body) {
        Some(config) if validate_notification_configuration(&config) => config,
        _ => {
            return xml_error(
                400,
                "InvalidArgument",
                "notification configuration is invalid",
            );
        }
    };
    match store.put_bucket_notification(bucket, &config) {
        Ok(()) => Response::empty(200),
        Err(StoreError::BucketNotExist) => xml_error(404, "NoSuchBucket", "bucket does not exist"),
        Err(StoreError::InvalidBucketName) => {
            xml_error(400, "InvalidBucketName", "invalid bucket name")
        }
        Err(_) => xml_error(500, "InternalError", "internal error"),
    }
}

fn get_bucket_notification(store: &FileBucketStore, bucket: &str) -> Response {
    match store.get_bucket_notification(bucket) {
        Ok(Some(config)) => Response::xml(200, notification_configuration(&config)),
        Ok(None) => xml_error(404, "NoSuchBucket", "bucket does not exist"),
        Err(StoreError::InvalidBucketName) => {
            xml_error(400, "InvalidBucketName", "invalid bucket name")
        }
        Err(_) => xml_error(500, "InternalError", "internal error"),
    }
}

fn put_bucket_inventory(store: &FileBucketStore, req: &Request, bucket: &str) -> Response {
    let id = match configuration_id(req) {
        Some(id) => id,
        None => {
            return xml_error(
                400,
                "InvalidArgument",
                "inventory configuration id is invalid",
            );
        }
    };
    let config = match parse_inventory_configuration(&req.body) {
        Some(config) if validate_inventory_configuration(&id, &config) => config,
        _ => {
            return xml_error(400, "InvalidArgument", "inventory configuration is invalid");
        }
    };
    match store.put_bucket_inventory(bucket, &id, config) {
        Ok(()) => Response::empty(200),
        Err(StoreError::BucketNotExist) => xml_error(404, "NoSuchBucket", "bucket does not exist"),
        Err(StoreError::InvalidBucketName) => {
            xml_error(400, "InvalidBucketName", "invalid bucket name")
        }
        Err(_) => xml_error(500, "InternalError", "internal error"),
    }
}

fn get_bucket_inventory(store: &FileBucketStore, req: &Request, bucket: &str) -> Response {
    let id = req.query.get("id").map(|v| v.trim()).unwrap_or("");
    if id.is_empty() {
        return list_bucket_inventories(store, bucket);
    }
    if !valid_configuration_id(id) {
        return xml_error(
            400,
            "InvalidArgument",
            "inventory configuration id is invalid",
        );
    }
    match store.get_bucket_inventory(bucket, id) {
        Ok(Some((config, true))) => Response::xml(200, inventory_configuration(&config)),
        Ok(Some((_, false))) => xml_error(
            404,
            "NoSuchConfiguration",
            "inventory configuration does not exist",
        ),
        Ok(None) => xml_error(404, "NoSuchBucket", "bucket does not exist"),
        Err(StoreError::InvalidBucketName) => {
            xml_error(400, "InvalidBucketName", "invalid bucket name")
        }
        Err(_) => xml_error(500, "InternalError", "internal error"),
    }
}

fn list_bucket_inventories(store: &FileBucketStore, bucket: &str) -> Response {
    match store.list_bucket_inventories(bucket) {
        Ok(Some(configs)) => Response::xml(200, list_inventory_configurations_result(&configs)),
        Ok(None) => xml_error(404, "NoSuchBucket", "bucket does not exist"),
        Err(StoreError::InvalidBucketName) => {
            xml_error(400, "InvalidBucketName", "invalid bucket name")
        }
        Err(_) => xml_error(500, "InternalError", "internal error"),
    }
}

fn delete_bucket_inventory(store: &FileBucketStore, req: &Request, bucket: &str) -> Response {
    let id = match configuration_id(req) {
        Some(id) => id,
        None => {
            return xml_error(
                400,
                "InvalidArgument",
                "inventory configuration id is invalid",
            );
        }
    };
    match store.delete_bucket_inventory(bucket, &id) {
        Ok(_) => Response::empty(204),
        Err(StoreError::BucketNotExist) => xml_error(404, "NoSuchBucket", "bucket does not exist"),
        Err(StoreError::InvalidBucketName) => {
            xml_error(400, "InvalidBucketName", "invalid bucket name")
        }
        Err(_) => xml_error(500, "InternalError", "internal error"),
    }
}

fn put_bucket_analytics(store: &FileBucketStore, req: &Request, bucket: &str) -> Response {
    let id = match configuration_id(req) {
        Some(id) => id,
        None => {
            return xml_error(
                400,
                "InvalidArgument",
                "analytics configuration id is invalid",
            );
        }
    };
    let config = match parse_analytics_configuration(&req.body) {
        Some(config) if validate_analytics_configuration(&id, &config) => config,
        _ => {
            return xml_error(400, "InvalidArgument", "analytics configuration is invalid");
        }
    };
    match store.put_bucket_analytics(bucket, &id, config) {
        Ok(()) => Response::empty(200),
        Err(StoreError::BucketNotExist) => xml_error(404, "NoSuchBucket", "bucket does not exist"),
        Err(StoreError::InvalidBucketName) => {
            xml_error(400, "InvalidBucketName", "invalid bucket name")
        }
        Err(_) => xml_error(500, "InternalError", "internal error"),
    }
}

fn get_bucket_analytics(store: &FileBucketStore, req: &Request, bucket: &str) -> Response {
    let id = req.query.get("id").map(|v| v.trim()).unwrap_or("");
    if id.is_empty() {
        return list_bucket_analytics(store, bucket);
    }
    if !valid_configuration_id(id) {
        return xml_error(
            400,
            "InvalidArgument",
            "analytics configuration id is invalid",
        );
    }
    match store.get_bucket_analytics(bucket, id) {
        Ok(Some((config, true))) => Response::xml(200, analytics_configuration(&config)),
        Ok(Some((_, false))) => xml_error(
            404,
            "NoSuchConfiguration",
            "analytics configuration does not exist",
        ),
        Ok(None) => xml_error(404, "NoSuchBucket", "bucket does not exist"),
        Err(StoreError::InvalidBucketName) => {
            xml_error(400, "InvalidBucketName", "invalid bucket name")
        }
        Err(_) => xml_error(500, "InternalError", "internal error"),
    }
}

fn list_bucket_analytics(store: &FileBucketStore, bucket: &str) -> Response {
    match store.list_bucket_analytics(bucket) {
        Ok(Some(configs)) => Response::xml(200, list_analytics_configurations_result(&configs)),
        Ok(None) => xml_error(404, "NoSuchBucket", "bucket does not exist"),
        Err(StoreError::InvalidBucketName) => {
            xml_error(400, "InvalidBucketName", "invalid bucket name")
        }
        Err(_) => xml_error(500, "InternalError", "internal error"),
    }
}

fn delete_bucket_analytics(store: &FileBucketStore, req: &Request, bucket: &str) -> Response {
    let id = match configuration_id(req) {
        Some(id) => id,
        None => {
            return xml_error(
                400,
                "InvalidArgument",
                "analytics configuration id is invalid",
            );
        }
    };
    match store.delete_bucket_analytics(bucket, &id) {
        Ok(_) => Response::empty(204),
        Err(StoreError::BucketNotExist) => xml_error(404, "NoSuchBucket", "bucket does not exist"),
        Err(StoreError::InvalidBucketName) => {
            xml_error(400, "InvalidBucketName", "invalid bucket name")
        }
        Err(_) => xml_error(500, "InternalError", "internal error"),
    }
}

fn put_bucket_replication(store: &FileBucketStore, req: &Request, bucket: &str) -> Response {
    let config = match parse_replication_configuration(&req.body) {
        Some(config) if validate_replication_configuration(&config) => config,
        _ => {
            return xml_error(
                400,
                "InvalidArgument",
                "replication configuration is invalid",
            );
        }
    };
    match store.put_bucket_replication(bucket, &config) {
        Ok(()) => Response::empty(200),
        Err(StoreError::BucketNotExist) => xml_error(404, "NoSuchBucket", "bucket does not exist"),
        Err(StoreError::InvalidBucketName) => {
            xml_error(400, "InvalidBucketName", "invalid bucket name")
        }
        Err(_) => xml_error(500, "InternalError", "internal error"),
    }
}

fn get_bucket_replication(store: &FileBucketStore, bucket: &str) -> Response {
    match store.get_bucket_replication(bucket) {
        Ok(Some((config, true))) => Response::xml(200, replication_configuration(&config)),
        Ok(Some((_, false))) => xml_error(
            404,
            "ReplicationConfigurationNotFoundError",
            "replication configuration does not exist",
        ),
        Ok(None) => xml_error(404, "NoSuchBucket", "bucket does not exist"),
        Err(StoreError::InvalidBucketName) => {
            xml_error(400, "InvalidBucketName", "invalid bucket name")
        }
        Err(_) => xml_error(500, "InternalError", "internal error"),
    }
}

fn delete_bucket_replication(store: &FileBucketStore, bucket: &str) -> Response {
    match store.delete_bucket_replication(bucket) {
        Ok(_) => Response::empty(204),
        Err(StoreError::BucketNotExist) => xml_error(404, "NoSuchBucket", "bucket does not exist"),
        Err(StoreError::InvalidBucketName) => {
            xml_error(400, "InvalidBucketName", "invalid bucket name")
        }
        Err(_) => xml_error(500, "InternalError", "internal error"),
    }
}

fn put_object_acl(store: &FileBucketStore, req: &Request, bucket: &str, key: &str) -> Response {
    let Some(acl) = acl_from_request(req) else {
        return xml_error(400, "MalformedACLError", "object ACL is malformed");
    };
    let version_id = req.query.get("versionId").map(String::as_str).unwrap_or("");
    match store.put_object_acl(bucket, key, version_id, &acl) {
        Ok(true) => Response::empty(200),
        Ok(false) => xml_error(404, "NoSuchKey", "object does not exist"),
        Err(StoreError::BucketNotExist) => xml_error(404, "NoSuchBucket", "bucket does not exist"),
        Err(StoreError::InvalidVersionId) => {
            xml_error(400, "InvalidArgument", "invalid version id")
        }
        Err(StoreError::InvalidBucketName) | Err(StoreError::InvalidObjectKey) => {
            xml_error(400, "InvalidArgument", "invalid object key")
        }
        Err(_) => xml_error(500, "InternalError", "internal error"),
    }
}

fn get_object_acl(store: &FileBucketStore, req: &Request, bucket: &str, key: &str) -> Response {
    let version_id = req.query.get("versionId").map(String::as_str).unwrap_or("");
    match store.get_object_acl(bucket, key, version_id) {
        Ok(Some(acl)) => Response::xml(200, access_control_policy(&acl)),
        Ok(None) => xml_error(404, "NoSuchKey", "object does not exist"),
        Err(StoreError::BucketNotExist) => xml_error(404, "NoSuchBucket", "bucket does not exist"),
        Err(StoreError::InvalidVersionId) => {
            xml_error(400, "InvalidArgument", "invalid version id")
        }
        Err(StoreError::InvalidBucketName) | Err(StoreError::InvalidObjectKey) => {
            xml_error(400, "InvalidArgument", "invalid object key")
        }
        Err(_) => xml_error(500, "InternalError", "internal error"),
    }
}

fn put_object_retention(
    store: &FileBucketStore,
    req: &Request,
    bucket: &str,
    key: &str,
) -> Response {
    let Some(retention) = parse_object_retention(&req.body).filter(validate_object_retention)
    else {
        return xml_error(400, "InvalidArgument", "object retention is invalid");
    };
    let version_id = req.query.get("versionId").map(String::as_str).unwrap_or("");
    match store.put_object_retention(bucket, key, version_id, retention) {
        Ok(Some(_)) => Response::empty(200),
        Ok(None) => xml_error(404, "NoSuchKey", "object does not exist"),
        Err(StoreError::BucketNotExist) => xml_error(404, "NoSuchBucket", "bucket does not exist"),
        Err(StoreError::InvalidVersionId) => {
            xml_error(400, "InvalidArgument", "invalid version id")
        }
        Err(StoreError::InvalidBucketName) | Err(StoreError::InvalidObjectKey) => {
            xml_error(400, "InvalidArgument", "invalid object key")
        }
        Err(_) => xml_error(500, "InternalError", "internal error"),
    }
}

fn get_object_retention(
    store: &FileBucketStore,
    req: &Request,
    bucket: &str,
    key: &str,
) -> Response {
    let version_id = req.query.get("versionId").map(String::as_str).unwrap_or("");
    match store.get_object_retention(bucket, key, version_id) {
        Ok(Some(retention)) if !retention.mode.is_empty() => {
            Response::xml(200, object_retention(&retention))
        }
        Ok(Some(_)) => xml_error(
            404,
            "NoSuchObjectLockConfiguration",
            "object retention does not exist",
        ),
        Ok(None) => xml_error(404, "NoSuchKey", "object does not exist"),
        Err(StoreError::BucketNotExist) => xml_error(404, "NoSuchBucket", "bucket does not exist"),
        Err(StoreError::InvalidVersionId) => {
            xml_error(400, "InvalidArgument", "invalid version id")
        }
        Err(StoreError::InvalidBucketName) | Err(StoreError::InvalidObjectKey) => {
            xml_error(400, "InvalidArgument", "invalid object key")
        }
        Err(_) => xml_error(500, "InternalError", "internal error"),
    }
}

fn put_object_legal_hold(
    store: &FileBucketStore,
    req: &Request,
    bucket: &str,
    key: &str,
) -> Response {
    let Some(legal_hold) = parse_object_legal_hold(&req.body).filter(validate_object_legal_hold)
    else {
        return xml_error(400, "InvalidArgument", "object legal hold is invalid");
    };
    let version_id = req.query.get("versionId").map(String::as_str).unwrap_or("");
    match store.put_object_legal_hold(bucket, key, version_id, legal_hold) {
        Ok(Some(_)) => Response::empty(200),
        Ok(None) => xml_error(404, "NoSuchKey", "object does not exist"),
        Err(StoreError::BucketNotExist) => xml_error(404, "NoSuchBucket", "bucket does not exist"),
        Err(StoreError::InvalidVersionId) => {
            xml_error(400, "InvalidArgument", "invalid version id")
        }
        Err(StoreError::InvalidBucketName) | Err(StoreError::InvalidObjectKey) => {
            xml_error(400, "InvalidArgument", "invalid object key")
        }
        Err(_) => xml_error(500, "InternalError", "internal error"),
    }
}

fn get_object_legal_hold(
    store: &FileBucketStore,
    req: &Request,
    bucket: &str,
    key: &str,
) -> Response {
    let version_id = req.query.get("versionId").map(String::as_str).unwrap_or("");
    match store.get_object_legal_hold(bucket, key, version_id) {
        Ok(Some(legal_hold)) if !legal_hold.status.is_empty() => {
            Response::xml(200, object_legal_hold(&legal_hold))
        }
        Ok(Some(_)) => xml_error(
            404,
            "NoSuchObjectLockConfiguration",
            "object legal hold does not exist",
        ),
        Ok(None) => xml_error(404, "NoSuchKey", "object does not exist"),
        Err(StoreError::BucketNotExist) => xml_error(404, "NoSuchBucket", "bucket does not exist"),
        Err(StoreError::InvalidVersionId) => {
            xml_error(400, "InvalidArgument", "invalid version id")
        }
        Err(StoreError::InvalidBucketName) | Err(StoreError::InvalidObjectKey) => {
            xml_error(400, "InvalidArgument", "invalid object key")
        }
        Err(_) => xml_error(500, "InternalError", "internal error"),
    }
}

fn create_multipart_upload(
    store: &FileBucketStore,
    req: &Request,
    bucket: &str,
    key: &str,
) -> Response {
    let encryption = match server_side_encryption_from_headers(req) {
        Ok(encryption) => encryption,
        Err(err) => return server_side_encryption_error(err),
    };
    let input = CreateMultipartUploadInput {
        bucket: bucket.to_string(),
        key: key.to_string(),
        content_type: req.header("content-type").to_string(),
        content_encoding: req.header("content-encoding").to_string(),
        cache_control: req.header("cache-control").to_string(),
        content_disposition: req.header("content-disposition").to_string(),
        metadata: user_metadata(&req.headers),
        encryption,
        ..Default::default()
    };
    match store.create_multipart_upload(input) {
        Ok(upload) => Response::xml(
            200,
            initiate_multipart_upload_result(&upload.bucket, &upload.key, &upload.upload_id),
        ),
        Err(StoreError::BucketNotExist) => xml_error(404, "NoSuchBucket", "bucket does not exist"),
        Err(StoreError::InvalidBucketName) | Err(StoreError::InvalidObjectKey) => {
            xml_error(400, "InvalidArgument", "invalid object key")
        }
        Err(_) => xml_error(500, "InternalError", "internal error"),
    }
}

fn upload_part(
    store: &FileBucketStore,
    req: &Request,
    bucket: &str,
    key: &str,
    upload_id: &str,
) -> Response {
    let part_number = match req
        .query
        .get("partNumber")
        .and_then(|v| v.parse::<i64>().ok())
    {
        Some(n) if (1..=10000).contains(&n) => n,
        _ => return xml_error(400, "InvalidArgument", "invalid part number"),
    };
    match store.upload_part(
        bucket,
        key,
        upload_id,
        part_number,
        &req.body,
        req.header("content-md5"),
    ) {
        Ok(part) => {
            let mut r = Response::empty(200);
            r.headers.insert("ETag".to_string(), part.etag);
            r
        }
        Err(StoreError::InvalidContentMd5) => xml_error(
            400,
            "InvalidDigest",
            "the Content-MD5 you specified was invalid",
        ),
        Err(StoreError::ContentMd5Mismatch) => xml_error(
            400,
            "BadDigest",
            "the Content-MD5 you specified did not match what was received",
        ),
        Err(StoreError::InvalidPartNumber) => {
            xml_error(400, "InvalidArgument", "invalid part number")
        }
        Err(StoreError::BucketNotExist) => xml_error(404, "NoSuchBucket", "bucket does not exist"),
        Err(StoreError::MultipartUploadNotExist) | Err(StoreError::InvalidUploadId) => {
            xml_error(404, "NoSuchUpload", "multipart upload does not exist")
        }
        Err(_) => xml_error(500, "InternalError", "internal error"),
    }
}

fn list_parts(
    store: &FileBucketStore,
    req: &Request,
    bucket: &str,
    key: &str,
    upload_id: &str,
) -> Response {
    let (upload, parts) = match store.list_parts(bucket, key, upload_id) {
        Ok(Some(found)) => found,
        Ok(None) => return xml_error(404, "NoSuchUpload", "multipart upload does not exist"),
        Err(StoreError::BucketNotExist) => {
            return xml_error(404, "NoSuchBucket", "bucket does not exist")
        }
        Err(StoreError::InvalidUploadId) => {
            return xml_error(404, "NoSuchUpload", "multipart upload does not exist")
        }
        Err(_) => return xml_error(500, "InternalError", "internal error"),
    };
    let max_parts =
        match parse_max_parts(req.query.get("max-parts").map(String::as_str).unwrap_or("")) {
            Ok(v) => v,
            Err(_) => return xml_error(400, "InvalidArgument", "invalid max-parts"),
        };
    let part_number_marker = match parse_part_number_marker(
        req.query
            .get("part-number-marker")
            .map(String::as_str)
            .unwrap_or(""),
    ) {
        Ok(v) => v,
        Err(_) => return xml_error(400, "InvalidArgument", "invalid part-number-marker"),
    };
    let (page, truncated, next_marker) = paginate_parts(&parts, part_number_marker, max_parts);
    Response::xml(
        200,
        list_parts_result(
            &upload,
            &page,
            part_number_marker,
            max_parts,
            truncated,
            next_marker,
        ),
    )
}

fn complete_multipart_upload(
    store: &FileBucketStore,
    req: &Request,
    bucket: &str,
    key: &str,
    upload_id: &str,
) -> Response {
    let if_none_match = match if_none_match_write(req) {
        Ok(v) => v,
        Err(r) => return r,
    };
    let Some(part_numbers) = parse_complete_multipart_parts(&req.body) else {
        return xml_error(400, "MalformedXML", "request body is malformed");
    };
    if part_numbers.is_empty() {
        return xml_error(400, "MalformedXML", "request body is malformed");
    }
    let mut previous = 0;
    for part_number in &part_numbers {
        if *part_number <= 0 {
            return xml_error(400, "InvalidPart", "invalid multipart part");
        }
        if *part_number <= previous {
            return xml_error(
                400,
                "InvalidPartOrder",
                "multipart parts must be in ascending order",
            );
        }
        previous = *part_number;
    }
    let completed = if if_none_match {
        store.complete_multipart_upload_if_absent(bucket, key, upload_id, &part_numbers)
    } else {
        store.complete_multipart_upload(bucket, key, upload_id, &part_numbers)
    };
    match completed {
        Ok(Some(object)) => {
            let mut r = Response::xml(
                200,
                complete_multipart_upload_result(bucket, key, &object.etag),
            );
            if record_object_event(
                store,
                bucket,
                key,
                "s3:ObjectCreated:CompleteMultipartUpload",
                &object,
            )
            .is_err()
            {
                return xml_error(500, "InternalError", "internal error");
            }
            if replicate_object_write(store, bucket, key, &object).is_err() {
                return xml_error(500, "InternalError", "internal error");
            }
            emit_dashboard_event(
                "s3.object.put",
                serde_json::json!({
                    "bucket": bucket,
                    "key": key,
                    "etag": object.etag.clone(),
                    "contentLength": object.size,
                }),
            );
            r.headers.insert("ETag".to_string(), object.etag);
            if !object.version_id.is_empty() {
                r.headers
                    .insert("x-amz-version-id".to_string(), object.version_id);
            }
            r
        }
        Ok(None) => xml_error(404, "NoSuchUpload", "multipart upload does not exist"),
        Err(StoreError::PreconditionFailed) => precondition_failed(),
        Err(StoreError::InvalidPart(_)) => {
            xml_error(400, "InvalidPart", "multipart part is missing")
        }
        Err(StoreError::BucketNotExist) => xml_error(404, "NoSuchBucket", "bucket does not exist"),
        Err(StoreError::InvalidUploadId) => {
            xml_error(404, "NoSuchUpload", "multipart upload does not exist")
        }
        Err(_) => xml_error(500, "InternalError", "internal error"),
    }
}

fn abort_multipart_upload(
    store: &FileBucketStore,
    bucket: &str,
    key: &str,
    upload_id: &str,
) -> Response {
    match store.abort_multipart_upload(bucket, key, upload_id) {
        Ok(true) => Response::empty(204),
        Ok(false) => xml_error(404, "NoSuchUpload", "multipart upload does not exist"),
        Err(StoreError::BucketNotExist) => xml_error(404, "NoSuchBucket", "bucket does not exist"),
        Err(StoreError::InvalidUploadId) => {
            xml_error(404, "NoSuchUpload", "multipart upload does not exist")
        }
        Err(_) => xml_error(500, "InternalError", "internal error"),
    }
}

fn list_multipart_uploads(store: &FileBucketStore, bucket: &str) -> Response {
    match store.list_multipart_uploads(bucket) {
        Ok(Some(uploads)) => Response::xml(200, list_multipart_uploads_result(bucket, &uploads)),
        Ok(None) => xml_error(404, "NoSuchBucket", "bucket does not exist"),
        Err(StoreError::InvalidBucketName) => {
            xml_error(400, "InvalidBucketName", "invalid bucket name")
        }
        Err(_) => xml_error(500, "InternalError", "internal error"),
    }
}

fn put_object(store: &FileBucketStore, req: &Request, bucket: &str, key: &str) -> Response {
    let if_none_match = match if_none_match_write(req) {
        Ok(v) => v,
        Err(r) => return r,
    };
    let encryption = match server_side_encryption_from_headers(req) {
        Ok(encryption) => encryption,
        Err(err) => return server_side_encryption_error(err),
    };
    let Some((retention, legal_hold)) = object_lock_from_headers(req) else {
        return xml_error(400, "InvalidArgument", "object lock headers are invalid");
    };
    let input = PutObjectInput {
        bucket: bucket.to_string(),
        key: key.to_string(),
        body: req.body.clone(),
        content_md5: req.header("content-md5").to_string(),
        content_type: req.header("content-type").to_string(),
        content_encoding: req.header("content-encoding").to_string(),
        cache_control: req.header("cache-control").to_string(),
        content_disposition: req.header("content-disposition").to_string(),
        metadata: user_metadata(&req.headers),
        encryption,
        retention,
        legal_hold,
        ..Default::default()
    };
    let stored = if if_none_match {
        store.put_object_if_absent(input)
    } else {
        store.put_object(input)
    };
    match stored {
        Ok(object) => {
            let mut r = Response::empty(200);
            write_object_lock_headers(&mut r, &object);
            write_server_side_encryption_headers(&mut r, &object);
            if record_object_event(store, bucket, key, "s3:ObjectCreated:Put", &object).is_err() {
                return xml_error(500, "InternalError", "internal error");
            }
            if replicate_object_write(store, bucket, key, &object).is_err() {
                return xml_error(500, "InternalError", "internal error");
            }
            r.headers.insert("ETag".to_string(), object.etag);
            if !object.version_id.is_empty() {
                r.headers
                    .insert("x-amz-version-id".to_string(), object.version_id);
            }
            r
        }
        Err(StoreError::InvalidContentMd5) => xml_error(
            400,
            "InvalidDigest",
            "the Content-MD5 you specified was invalid",
        ),
        Err(StoreError::ContentMd5Mismatch) => xml_error(
            400,
            "BadDigest",
            "the Content-MD5 you specified did not match what was received",
        ),
        Err(StoreError::PreconditionFailed) => precondition_failed(),
        Err(StoreError::BucketNotExist) => xml_error(404, "NoSuchBucket", "bucket does not exist"),
        Err(StoreError::InvalidVersionId) => {
            xml_error(400, "InvalidArgument", "invalid version id")
        }
        Err(StoreError::InvalidBucketName) | Err(StoreError::InvalidObjectKey) => {
            xml_error(400, "InvalidArgument", "invalid object key")
        }
        Err(_) => xml_error(500, "InternalError", "internal error"),
    }
}

fn copy_object(store: &FileBucketStore, req: &Request, bucket: &str, key: &str) -> Response {
    let if_none_match = match if_none_match_write(req) {
        Ok(v) => v,
        Err(r) => return r,
    };
    let Some((source_bucket, source_key, source_version_id)) =
        parse_copy_source(req.header("x-amz-copy-source"))
    else {
        return xml_error(400, "InvalidArgument", "invalid copy source");
    };
    let (source_object, body) =
        match store.get_object_version(&source_bucket, &source_key, &source_version_id) {
            Ok(Some(found)) => found,
            Ok(None) => return xml_error(404, "NoSuchKey", "source object does not exist"),
            Err(StoreError::BucketNotExist) => {
                return xml_error(404, "NoSuchBucket", "source bucket does not exist");
            }
            Err(StoreError::InvalidVersionId) => {
                return xml_error(400, "InvalidArgument", "invalid version id");
            }
            Err(StoreError::InvalidBucketName) | Err(StoreError::InvalidObjectKey) => {
                return xml_error(400, "InvalidArgument", "invalid copy source");
            }
            Err(_) => return xml_error(500, "InternalError", "internal error"),
        };
    if etag_list_matches(
        req.header("x-amz-copy-source-if-none-match"),
        &source_object.etag,
    ) {
        return precondition_failed();
    }

    let mut input = PutObjectInput {
        bucket: bucket.to_string(),
        key: key.to_string(),
        body,
        content_type: source_object.content_type,
        content_encoding: source_object.content_encoding,
        cache_control: source_object.cache_control,
        content_disposition: source_object.content_disposition,
        metadata: source_object.metadata,
        encryption: source_object.encryption,
        retention: source_object.retention,
        legal_hold: source_object.legal_hold,
        ..Default::default()
    };
    if req
        .header("x-amz-metadata-directive")
        .eq_ignore_ascii_case("REPLACE")
    {
        input.content_type = req.header("content-type").to_string();
        input.content_encoding = req.header("content-encoding").to_string();
        input.cache_control = req.header("cache-control").to_string();
        input.content_disposition = req.header("content-disposition").to_string();
        input.metadata = user_metadata(&req.headers);
    }
    if has_server_side_encryption_headers(req) {
        input.encryption = match server_side_encryption_from_headers(req) {
            Ok(encryption) => encryption,
            Err(err) => return server_side_encryption_error(err),
        };
    }
    if has_object_lock_headers(req) {
        let Some((retention, legal_hold)) = object_lock_from_headers(req) else {
            return xml_error(400, "InvalidArgument", "object lock headers are invalid");
        };
        if !retention.mode.is_empty() {
            input.retention = retention;
        }
        if !legal_hold.status.is_empty() {
            input.legal_hold = legal_hold;
        }
    }

    let stored = if if_none_match {
        store.put_object_if_absent(input)
    } else {
        store.put_object(input)
    };
    match stored {
        Ok(object) => {
            let mut r = Response::xml(
                200,
                copy_object_result(&to_rfc3339_seconds(&object.last_modified), &object.etag),
            );
            r.headers.insert("ETag".to_string(), object.etag.clone());
            write_server_side_encryption_headers(&mut r, &object);
            write_object_lock_headers(&mut r, &object);
            if !object.version_id.is_empty() {
                r.headers
                    .insert("x-amz-version-id".to_string(), object.version_id.clone());
            }
            if record_object_event(store, bucket, key, "s3:ObjectCreated:Copy", &object).is_err() {
                return xml_error(500, "InternalError", "internal error");
            }
            if replicate_object_write(store, bucket, key, &object).is_err() {
                return xml_error(500, "InternalError", "internal error");
            }
            r
        }
        Err(StoreError::PreconditionFailed) => precondition_failed(),
        Err(StoreError::BucketNotExist) => xml_error(404, "NoSuchBucket", "bucket does not exist"),
        Err(StoreError::InvalidVersionId) => {
            xml_error(400, "InvalidArgument", "invalid version id")
        }
        Err(StoreError::InvalidBucketName) | Err(StoreError::InvalidObjectKey) => {
            xml_error(400, "InvalidArgument", "invalid object key")
        }
        Err(_) => xml_error(500, "InternalError", "internal error"),
    }
}

fn select_object_content(
    store: &FileBucketStore,
    req: &Request,
    bucket: &str,
    key: &str,
) -> Response {
    let Some(select) = parse_select_object_content_request(&req.body) else {
        return xml_error(400, "MalformedXML", "request body is malformed");
    };
    if select.expression_type.trim() != "SQL" {
        return xml_error(
            400,
            "InvalidExpressionType",
            "only SQL expressions are supported",
        );
    }
    if !supported_select_expression(&select.expression) {
        return xml_error(
            501,
            "NotImplemented",
            "only SELECT * FROM S3Object is supported",
        );
    }
    let version_id = req.query.get("versionId").map(String::as_str).unwrap_or("");
    let (_, body) = match store.get_object_version(bucket, key, version_id) {
        Ok(Some((object, _))) if object.delete_marker => {
            return xml_error(404, "NoSuchKey", "object does not exist");
        }
        Ok(Some(found)) => found,
        Ok(None) => return xml_error(404, "NoSuchKey", "object does not exist"),
        Err(StoreError::BucketNotExist) => {
            return xml_error(404, "NoSuchBucket", "bucket does not exist");
        }
        Err(StoreError::InvalidVersionId) => {
            return xml_error(400, "InvalidArgument", "invalid version id");
        }
        Err(StoreError::InvalidBucketName) | Err(StoreError::InvalidObjectKey) => {
            return xml_error(400, "InvalidArgument", "invalid object key");
        }
        Err(_) => return xml_error(500, "InternalError", "internal error"),
    };
    let output = match evaluate_select_object_content(&select, &body) {
        Ok(output) => output,
        Err(message) => return xml_error(501, "NotImplemented", &message),
    };
    let mut payload = encode_event_stream_message(
        &[
            (":message-type", "event"),
            (":event-type", "Records"),
            (":content-type", "application/octet-stream"),
        ],
        &output,
    );
    payload.extend(encode_event_stream_message(
        &[(":message-type", "event"), (":event-type", "End")],
        &[],
    ));
    let mut r = Response::new(200, payload);
    r.headers.insert(
        "Content-Type".to_string(),
        "application/vnd.amazon.eventstream".to_string(),
    );
    r.headers
        .insert("Content-Length".to_string(), r.body.len().to_string());
    r
}

#[derive(Debug, Default)]
struct SelectObjectContent {
    expression: String,
    expression_type: String,
    input_csv: Option<SelectCsvSerialization>,
    input_json: Option<SelectJsonSerialization>,
    output_csv: Option<SelectCsvSerialization>,
    output_json: Option<SelectJsonSerialization>,
}

#[derive(Debug, Default, Clone)]
struct SelectCsvSerialization {
    file_header_info: String,
    record_delimiter: String,
    field_delimiter: String,
}

#[derive(Debug, Default, Clone)]
struct SelectJsonSerialization {
    kind: String,
}

fn parse_select_object_content_request(body: &[u8]) -> Option<SelectObjectContent> {
    let xml = std::str::from_utf8(body).ok()?;
    if !xml.contains("<SelectObjectContentRequest") {
        return None;
    }
    let input_xml = tag_block(xml, "InputSerialization").unwrap_or_default();
    let output_xml = tag_block(xml, "OutputSerialization").unwrap_or_default();
    Some(SelectObjectContent {
        expression: tag_text_in(xml, "Expression")?,
        expression_type: tag_text_in(xml, "ExpressionType")?,
        input_csv: xml_child_present(&input_xml, "CSV").then(|| parse_select_csv(&input_xml)),
        input_json: xml_child_present(&input_xml, "JSON").then(|| parse_select_json(&input_xml)),
        output_csv: xml_child_present(&output_xml, "CSV").then(|| parse_select_csv(&output_xml)),
        output_json: xml_child_present(&output_xml, "JSON").then(|| parse_select_json(&output_xml)),
    })
}

fn parse_select_csv(xml: &str) -> SelectCsvSerialization {
    let csv_xml = tag_block(xml, "CSV").unwrap_or_default();
    SelectCsvSerialization {
        file_header_info: tag_text_in(&csv_xml, "FileHeaderInfo").unwrap_or_default(),
        record_delimiter: tag_text_in(&csv_xml, "RecordDelimiter").unwrap_or_default(),
        field_delimiter: tag_text_in(&csv_xml, "FieldDelimiter").unwrap_or_default(),
    }
}

fn parse_select_json(xml: &str) -> SelectJsonSerialization {
    let json_xml = tag_block(xml, "JSON").unwrap_or_default();
    SelectJsonSerialization {
        kind: tag_text_in(&json_xml, "Type").unwrap_or_default(),
    }
}

fn xml_child_present(xml: &str, tag: &str) -> bool {
    xml.contains(&format!("<{tag}>")) || xml.contains(&format!("<{tag} "))
}

fn supported_select_expression(expression: &str) -> bool {
    let normalized = expression.split_whitespace().collect::<Vec<_>>().join(" ");
    normalized.eq_ignore_ascii_case("SELECT * FROM S3Object")
        || normalized.eq_ignore_ascii_case("SELECT * FROM S3Object s")
}

fn evaluate_select_object_content(
    request: &SelectObjectContent,
    body: &[u8],
) -> Result<Vec<u8>, String> {
    if request.input_csv.is_some() {
        evaluate_csv_select_object_content(request, body)
    } else if request.input_json.is_some() {
        evaluate_json_select_object_content(request, body)
    } else {
        Err("input serialization is not supported".to_string())
    }
}

fn evaluate_csv_select_object_content(
    request: &SelectObjectContent,
    body: &[u8],
) -> Result<Vec<u8>, String> {
    let Some(output) = &request.output_csv else {
        return Err("only CSV output is supported for CSV input".to_string());
    };
    let input = request.input_csv.clone().unwrap_or_default();
    let input_delimiter = select_field_delimiter(&input);
    let output_delimiter = select_field_delimiter(output);
    if input_delimiter.len() != 1 || output_delimiter.len() != 1 {
        return Err("only single-byte CSV field delimiters are supported".to_string());
    }
    let input_delimiter = input_delimiter.as_bytes()[0] as char;
    let mut records: Vec<Vec<String>> = String::from_utf8_lossy(body)
        .lines()
        .filter(|line| !line.is_empty())
        .map(|line| {
            line.split(input_delimiter)
                .map(|field| field.to_string())
                .collect()
        })
        .collect();
    if input.file_header_info.eq_ignore_ascii_case("USE") && !records.is_empty() {
        records.remove(0);
    }
    let output_delimiter = output_delimiter.as_bytes()[0] as char;
    let mut out = String::new();
    for record in records {
        out.push_str(&record.join(&output_delimiter.to_string()));
        out.push('\n');
    }
    let record_delimiter = select_record_delimiter(output);
    if record_delimiter != "\n" {
        Ok(out.replace('\n', &record_delimiter).into_bytes())
    } else {
        Ok(out.into_bytes())
    }
}

fn evaluate_json_select_object_content(
    request: &SelectObjectContent,
    body: &[u8],
) -> Result<Vec<u8>, String> {
    let Some(input) = &request.input_json else {
        return Err("only JSON LINES input is supported".to_string());
    };
    if request.output_json.is_none() {
        return Err("only JSON output is supported for JSON input".to_string());
    }
    if !input.kind.is_empty() && input.kind != "LINES" {
        return Err("only JSON LINES input is supported".to_string());
    }
    let mut out = Vec::new();
    for line in String::from_utf8_lossy(body)
        .lines()
        .filter(|line| !line.is_empty())
    {
        let value: serde_json::Value =
            serde_json::from_str(line).map_err(|_| "JSON input is malformed".to_string())?;
        let encoded =
            serde_json::to_vec(&value).map_err(|_| "JSON input is malformed".to_string())?;
        out.extend(encoded);
        out.push(b'\n');
    }
    Ok(out)
}

fn select_field_delimiter(csv: &SelectCsvSerialization) -> String {
    if csv.field_delimiter.is_empty() {
        ",".to_string()
    } else {
        csv.field_delimiter.clone()
    }
}

fn select_record_delimiter(csv: &SelectCsvSerialization) -> String {
    if csv.record_delimiter.is_empty() {
        "\n".to_string()
    } else {
        csv.record_delimiter.clone()
    }
}

fn encode_event_stream_message(headers: &[(&str, &str)], payload: &[u8]) -> Vec<u8> {
    let mut header_bytes = Vec::new();
    for (name, value) in headers {
        header_bytes.push(name.len() as u8);
        header_bytes.extend(name.as_bytes());
        header_bytes.push(7);
        header_bytes.extend((value.len() as u16).to_be_bytes());
        header_bytes.extend(value.as_bytes());
    }
    let total_length = 16 + header_bytes.len() + payload.len();
    let headers_length = header_bytes.len();
    let mut message = Vec::with_capacity(total_length);
    message.extend((total_length as u32).to_be_bytes());
    message.extend((headers_length as u32).to_be_bytes());
    let prelude_crc = crc32_ieee(&message);
    message.extend(prelude_crc.to_be_bytes());
    message.extend(header_bytes);
    message.extend(payload);
    let message_crc = crc32_ieee(&message);
    message.extend(message_crc.to_be_bytes());
    message
}

fn crc32_ieee(data: &[u8]) -> u32 {
    let mut crc = 0xFFFF_FFFFu32;
    for byte in data {
        crc ^= *byte as u32;
        for _ in 0..8 {
            crc = if crc & 1 != 0 {
                (crc >> 1) ^ 0xEDB8_8320
            } else {
                crc >> 1
            };
        }
    }
    crc ^ 0xFFFF_FFFF
}

#[derive(Debug)]
struct SignatureError {
    code: &'static str,
    status: u16,
}

/// What `aws-chunked` verification needs from an already verified seed signature.
struct ChunkSigning {
    signing_key: Vec<u8>,
    amz_date: String,
    scope: String,
    seed_signature: String,
    has_trailer: bool,
}

fn verify_signature(
    req: &Request,
    auth: &AuthConfig,
) -> Result<Option<ChunkSigning>, SignatureError> {
    if !auth.auth_mode.eq_ignore_ascii_case("strict") {
        return Ok(None);
    }
    let has_presign = req
        .query
        .get("X-Amz-Algorithm")
        .map(|v| !v.is_empty())
        .unwrap_or(false);
    let has_header_auth = !req.header("authorization").is_empty();
    if !has_presign && !has_header_auth {
        return Err(SignatureError {
            code: "AccessDenied",
            status: 403,
        });
    }
    if has_presign {
        verify_presigned_url(req, auth).map(|()| None)
    } else {
        verify_authorization_header(req, auth)
    }
}

fn verify_presigned_url(req: &Request, auth: &AuthConfig) -> Result<(), SignatureError> {
    if req.query.get("X-Amz-Algorithm").map(String::as_str) != Some(SIGV4_ALGORITHM) {
        return Err(SignatureError {
            code: "InvalidArgument",
            status: 400,
        });
    }
    let (access_key, date_stamp, region, service) = parse_credential_scope(
        req.query
            .get("X-Amz-Credential")
            .map(String::as_str)
            .unwrap_or(""),
    )
    .ok_or(SignatureError {
        code: "AuthorizationHeaderMalformed",
        status: 400,
    })?;
    if !valid_credential(auth, access_key, region, service) {
        return Err(SignatureError {
            code: "InvalidAccessKeyId",
            status: 403,
        });
    }
    let amz_date = req
        .query
        .get("X-Amz-Date")
        .map(String::as_str)
        .unwrap_or("");
    let expires = req
        .query
        .get("X-Amz-Expires")
        .and_then(|v| v.parse::<i64>().ok())
        .filter(|v| (0..=604800).contains(v))
        .ok_or(SignatureError {
            code: "AccessDenied",
            status: 403,
        })?;
    let signed_at = parse_sigv4_time(amz_date).ok_or(SignatureError {
        code: "AccessDenied",
        status: 403,
    })?;
    let now = unix_now();
    if now > signed_at + expires || signed_at > now + SIGV4_MAX_CLOCK_SKEW_SECONDS {
        return Err(SignatureError {
            code: "AccessDenied",
            status: 403,
        });
    }
    verify_scope_date(amz_date, date_stamp)?;
    let signed_headers = req
        .query
        .get("X-Amz-SignedHeaders")
        .map(String::as_str)
        .unwrap_or("");
    if signed_headers.is_empty() {
        return Err(SignatureError {
            code: "AuthorizationHeaderMalformed",
            status: 400,
        });
    }
    verify_signed_header_coverage(req, signed_headers)?;
    let payload_hash = match req.header("x-amz-content-sha256") {
        "" => "UNSIGNED-PAYLOAD",
        streaming if streaming.starts_with("STREAMING-") => {
            return Err(SignatureError {
                code: "NotImplemented",
                status: 501,
            })
        }
        hash => {
            verify_payload_hash(req, hash)?;
            hash
        }
    };
    let expected = signature_for_request(
        req,
        auth,
        date_stamp,
        region,
        signed_headers,
        payload_hash,
        "X-Amz-Signature",
    );
    let signature = req
        .query
        .get("X-Amz-Signature")
        .map(String::as_str)
        .unwrap_or("");
    if !constant_time_eq(signature.as_bytes(), expected.as_bytes()) {
        return Err(SignatureError {
            code: "SignatureDoesNotMatch",
            status: 403,
        });
    }
    Ok(())
}

fn verify_authorization_header(
    req: &Request,
    auth: &AuthConfig,
) -> Result<Option<ChunkSigning>, SignatureError> {
    let authorization = req.header("authorization");
    let Some(rest) = authorization.strip_prefix(&format!("{SIGV4_ALGORITHM} ")) else {
        return Err(SignatureError {
            code: "AuthorizationHeaderMalformed",
            status: 400,
        });
    };
    let values = parse_auth_params(rest);
    let credential = values.get("Credential").map(String::as_str).unwrap_or("");
    let signed_headers = values
        .get("SignedHeaders")
        .map(String::as_str)
        .unwrap_or("");
    let signature = values.get("Signature").map(String::as_str).unwrap_or("");
    let (access_key, date_stamp, region, service) =
        parse_credential_scope(credential).ok_or(SignatureError {
            code: "AuthorizationHeaderMalformed",
            status: 400,
        })?;
    if signed_headers.is_empty() || signature.is_empty() {
        return Err(SignatureError {
            code: "AuthorizationHeaderMalformed",
            status: 400,
        });
    }
    if !valid_credential(auth, access_key, region, service) {
        return Err(SignatureError {
            code: "InvalidAccessKeyId",
            status: 403,
        });
    }
    let amz_date = req.header("x-amz-date");
    if amz_date.is_empty() {
        return Err(SignatureError {
            code: "AuthorizationHeaderMalformed",
            status: 400,
        });
    }
    let signed_at = parse_sigv4_time(amz_date).ok_or(SignatureError {
        code: "AccessDenied",
        status: 403,
    })?;
    if (unix_now() - signed_at).abs() > SIGV4_MAX_CLOCK_SKEW_SECONDS {
        return Err(SignatureError {
            code: "RequestTimeTooSkewed",
            status: 403,
        });
    }
    verify_scope_date(amz_date, date_stamp)?;
    verify_signed_header_coverage(req, signed_headers)?;
    let payload_hash = if req.header("x-amz-content-sha256").is_empty() {
        "UNSIGNED-PAYLOAD".to_string()
    } else {
        let payload_hash = req.header("x-amz-content-sha256").to_string();
        verify_payload_hash(req, &payload_hash)?;
        payload_hash
    };
    let expected = signature_for_request(
        req,
        auth,
        date_stamp,
        region,
        signed_headers,
        &payload_hash,
        "",
    );
    if !constant_time_eq(signature.as_bytes(), expected.as_bytes()) {
        return Err(SignatureError {
            code: "SignatureDoesNotMatch",
            status: 403,
        });
    }
    if payload_hash != STREAMING_SIGNED && payload_hash != STREAMING_SIGNED_TRAILER {
        return Ok(None);
    }
    Ok(Some(ChunkSigning {
        signing_key: derive_signing_key(&auth.secret_access_key, date_stamp, region),
        amz_date: amz_date.to_string(),
        scope: format!("{date_stamp}/{region}/{SIGV4_SERVICE}/aws4_request"),
        seed_signature: expected,
        has_trailer: payload_hash == STREAMING_SIGNED_TRAILER,
    }))
}

/// Rebuilds a request with its `aws-chunked` framing removed. With `signing`
/// present every chunk (and the trailer) must carry a valid signature chained
/// from the header's seed signature.
fn decode_streaming_request(
    req: &Request,
    signing: Option<&ChunkSigning>,
) -> Result<Request, SignatureError> {
    let incomplete = || SignatureError {
        code: "IncompleteBody",
        status: 400,
    };
    let parsed = aws_chunked::parse(&req.body).ok_or_else(incomplete)?;
    if let Some(signing) = signing {
        verify_chunk_signatures(&parsed, signing)?;
    }
    let body = parsed.payload();
    let decoded_length = req.header("x-amz-decoded-content-length");
    if !decoded_length.is_empty() && decoded_length.parse::<usize>().ok() != Some(body.len()) {
        return Err(incomplete());
    }
    let mut headers = req.headers.clone();
    let encodings: Vec<&str> = req
        .header("content-encoding")
        .split(',')
        .map(str::trim)
        .filter(|e| !e.is_empty() && !e.eq_ignore_ascii_case("aws-chunked"))
        .collect();
    if encodings.is_empty() {
        headers.remove("content-encoding");
    } else {
        headers.insert("content-encoding".to_string(), encodings.join(", "));
    }
    Ok(Request {
        method: req.method.clone(),
        path: req.path.clone(),
        query: req.query.clone(),
        headers,
        body,
    })
}

fn verify_chunk_signatures(
    parsed: &aws_chunked::ChunkedBody<'_>,
    signing: &ChunkSigning,
) -> Result<(), SignatureError> {
    let sign = |algorithm: &str, previous: &str, parts: &[&str]| {
        let mut fields = vec![algorithm, &signing.amz_date, &signing.scope, previous];
        fields.extend_from_slice(parts);
        hex::encode(hmac_sha256(
            &signing.signing_key,
            fields.join("\n").as_bytes(),
        ))
    };
    let mismatch = || SignatureError {
        code: "SignatureDoesNotMatch",
        status: 403,
    };
    let empty_hash = sha256_hex(b"");
    let mut previous = signing.seed_signature.clone();
    for chunk in &parsed.chunks {
        let expected = sign(
            SIGV4_CHUNK_ALGORITHM,
            &previous,
            &[&empty_hash, &sha256_hex(chunk.data)],
        );
        let actual = chunk.signature.ok_or_else(mismatch)?;
        if !constant_time_eq(actual.as_bytes(), expected.as_bytes()) {
            return Err(mismatch());
        }
        previous = expected;
    }
    if signing.has_trailer {
        let canonical_trailers: String = parsed
            .trailers
            .iter()
            .map(|(name, value)| format!("{}:{value}\n", name.to_ascii_lowercase()))
            .collect();
        let expected = sign(
            SIGV4_TRAILER_ALGORITHM,
            &previous,
            &[&sha256_hex(canonical_trailers.as_bytes())],
        );
        let actual = parsed.trailer_signature.ok_or_else(mismatch)?;
        if !constant_time_eq(actual.as_bytes(), expected.as_bytes()) {
            return Err(mismatch());
        }
    }
    Ok(())
}

fn unix_now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

fn verify_scope_date(amz_date: &str, date_stamp: &str) -> Result<(), SignatureError> {
    if amz_date.starts_with(date_stamp) && date_stamp.len() == 8 {
        return Ok(());
    }
    Err(SignatureError {
        code: "AuthorizationHeaderMalformed",
        status: 400,
    })
}

/// `host` and every `x-amz-*` header that changes behaviour must be signed, or a
/// captured signature could be replayed with different semantics (for example
/// an added `x-amz-copy-source`). `x-amz-user-agent` is exempt because browser
/// SDKs send it unsigned and it has no effect on the request;
/// `x-amz-content-sha256` because its value is signed as the payload hash anyway.
fn verify_signed_header_coverage(
    req: &Request,
    signed_headers: &str,
) -> Result<(), SignatureError> {
    let signed_headers = signed_headers.to_ascii_lowercase();
    let signed: Vec<&str> = signed_headers.split(';').map(str::trim).collect();
    let covered = signed.contains(&"host")
        && req
            .headers
            .keys()
            .filter(|name| {
                name.starts_with("x-amz-")
                    && !matches!(name.as_str(), "x-amz-user-agent" | "x-amz-content-sha256")
            })
            .all(|name| signed.contains(&name.as_str()));
    if covered {
        return Ok(());
    }
    Err(SignatureError {
        code: "AccessDenied",
        status: 403,
    })
}

fn valid_credential(auth: &AuthConfig, access_key: &str, region: &str, service: &str) -> bool {
    let configured_access_key = if auth.access_key_id.is_empty() {
        "dev"
    } else {
        &auth.access_key_id
    };
    let configured_region = if auth.region.is_empty() {
        "us-east-1"
    } else {
        &auth.region
    };
    access_key == configured_access_key && region == configured_region && service == SIGV4_SERVICE
}

fn verify_payload_hash(req: &Request, payload_hash: &str) -> Result<(), SignatureError> {
    if matches!(
        payload_hash,
        "UNSIGNED-PAYLOAD"
            | STREAMING_SIGNED
            | STREAMING_SIGNED_TRAILER
            | STREAMING_UNSIGNED_TRAILER
    ) {
        return Ok(());
    }
    if payload_hash.starts_with("STREAMING-") {
        return Err(SignatureError {
            code: "NotImplemented",
            status: 501,
        });
    }
    if !constant_time_eq(
        payload_hash.to_ascii_lowercase().as_bytes(),
        sha256_hex(&req.body).as_bytes(),
    ) {
        return Err(SignatureError {
            code: "XAmzContentSHA256Mismatch",
            status: 400,
        });
    }
    Ok(())
}

fn signature_for_request(
    req: &Request,
    auth: &AuthConfig,
    date_stamp: &str,
    region: &str,
    signed_headers: &str,
    payload_hash: &str,
    ignored_query_key: &str,
) -> String {
    let canonical_request = [
        req.method.clone(),
        canonical_uri(&req.path),
        canonical_query_string(&req.query, ignored_query_key),
        canonical_headers(req, signed_headers),
        signed_headers.to_ascii_lowercase(),
        payload_hash.to_string(),
    ]
    .join("\n");
    let amz_date = req
        .query
        .get("X-Amz-Date")
        .map(String::as_str)
        .filter(|v| !v.is_empty())
        .unwrap_or_else(|| req.header("x-amz-date"));
    let scope = format!("{date_stamp}/{region}/{SIGV4_SERVICE}/aws4_request");
    let string_to_sign = [
        SIGV4_ALGORITHM.to_string(),
        amz_date.to_string(),
        scope,
        sha256_hex(canonical_request.as_bytes()),
    ]
    .join("\n");
    hex::encode(hmac_sha256(
        &derive_signing_key(&auth.secret_access_key, date_stamp, region),
        string_to_sign.as_bytes(),
    ))
}

/// `req.path` is the raw request target, so each segment is normalized through
/// a decode/encode round trip rather than encoded a second time.
fn canonical_uri(raw_path: &str) -> String {
    raw_path
        .split('/')
        .map(|segment| aws_percent_encode(&percent_decode(segment), "~"))
        .collect::<Vec<_>>()
        .join("/")
}

fn parse_credential_scope(credential: &str) -> Option<(&str, &str, &str, &str)> {
    let parts: Vec<&str> = credential.split('/').collect();
    if parts.len() == 5 && parts[4] == "aws4_request" {
        Some((parts[0], parts[1], parts[2], parts[3]))
    } else {
        None
    }
}

fn parse_auth_params(value: &str) -> BTreeMap<String, String> {
    value
        .split(',')
        .filter_map(|part| {
            let (key, value) = part.trim().split_once('=')?;
            Some((key.to_string(), value.to_string()))
        })
        .collect()
}

fn canonical_query_string(values: &BTreeMap<String, String>, ignored_key: &str) -> String {
    values
        .iter()
        .filter(|(key, _)| key.as_str() != ignored_key)
        .map(|(key, value)| {
            format!(
                "{}={}",
                aws_percent_encode(key, "~-_"),
                aws_percent_encode(value, "~-_")
            )
        })
        .collect::<Vec<_>>()
        .join("&")
}

fn canonical_headers(req: &Request, signed_headers: &str) -> String {
    let mut out = String::new();
    for name in signed_headers.to_ascii_lowercase().split(';') {
        let name = name.trim();
        if name.is_empty() {
            continue;
        }
        let value = if name == "host" {
            req.header("host")
        } else {
            req.header(name)
        };
        out.push_str(name);
        out.push(':');
        out.push_str(&normalize_header_value(value));
        out.push('\n');
    }
    out
}

fn normalize_header_value(value: &str) -> String {
    value.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn derive_signing_key(secret: &str, date_stamp: &str, region: &str) -> Vec<u8> {
    let secret = if secret.is_empty() { "dev" } else { secret };
    let date_key = hmac_sha256(format!("AWS4{secret}").as_bytes(), date_stamp.as_bytes());
    let region_key = hmac_sha256(&date_key, region.as_bytes());
    let service_key = hmac_sha256(&region_key, SIGV4_SERVICE.as_bytes());
    hmac_sha256(&service_key, b"aws4_request")
}

fn hmac_sha256(key: &[u8], value: &[u8]) -> Vec<u8> {
    let mut mac = Hmac::<Sha256>::new_from_slice(key).expect("HMAC accepts any key length");
    mac.update(value);
    mac.finalize().into_bytes().to_vec()
}

fn sha256_hex(value: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(value);
    hex::encode(hasher.finalize())
}

pub(crate) fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.iter().zip(b).fold(0u8, |acc, (a, b)| acc | (a ^ b)) == 0
}

fn parse_sigv4_time(value: &str) -> Option<i64> {
    if value.len() != 16 || !value.is_ascii() || !value.ends_with('Z') {
        return None;
    }
    let year = value[0..4].parse::<i32>().ok()?;
    let month = value[4..6].parse::<i32>().ok()?;
    let day = value[6..8].parse::<i32>().ok()?;
    let hour = value[9..11].parse::<i32>().ok()?;
    let minute = value[11..13].parse::<i32>().ok()?;
    let second = value[13..15].parse::<i32>().ok()?;
    Some(datetime_to_unix(year, month, day, hour, minute, second))
}

fn datetime_to_unix(year: i32, month: i32, day: i32, hour: i32, minute: i32, second: i32) -> i64 {
    let y = year - (month <= 2) as i32;
    let era = (if y >= 0 { y } else { y - 399 }) / 400;
    let yoe = y - era * 400;
    let mp = month + if month > 2 { -3 } else { 9 };
    let doy = (153 * mp + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146097 + doe - 719468;
    (days as i64) * 86_400 + (hour as i64) * 3600 + (minute as i64) * 60 + second as i64
}

fn get_object(
    store: &FileBucketStore,
    req: &Request,
    bucket: &str,
    key: &str,
    head_only: bool,
) -> Response {
    if store.apply_bucket_lifecycle(bucket, &store.now()).is_err() {
        return xml_error(500, "InternalError", "internal error");
    }
    let found = if let Some(version_id) = req.query.get("versionId") {
        store.get_object_version(bucket, key, version_id)
    } else {
        store.get_object(bucket, key)
    };
    match found {
        Ok(Some((object, _body))) if object.delete_marker => {
            let mut r = xml_error(
                405,
                "MethodNotAllowed",
                "the specified version is a delete marker",
            );
            r.headers
                .insert("x-amz-delete-marker".to_string(), "true".to_string());
            if !object.version_id.is_empty() {
                r.headers
                    .insert("x-amz-version-id".to_string(), object.version_id);
            }
            r
        }
        Ok(Some((object, body))) => object_response(req, object, body, head_only),
        Ok(None) => xml_error(404, "NoSuchKey", "object does not exist"),
        Err(StoreError::BucketNotExist) => xml_error(404, "NoSuchBucket", "bucket does not exist"),
        Err(StoreError::InvalidVersionId) => {
            xml_error(400, "InvalidArgument", "invalid version id")
        }
        Err(StoreError::InvalidBucketName) | Err(StoreError::InvalidObjectKey) => {
            xml_error(400, "InvalidArgument", "invalid object key")
        }
        Err(_) => xml_error(500, "InternalError", "internal error"),
    }
}

fn object_response(req: &Request, object: Object, body: Vec<u8>, head_only: bool) -> Response {
    // Preconditions are evaluated before Range (RFC 7232 section 6).
    if etag_list_matches(req.header("if-none-match"), &object.etag) {
        return not_modified(object);
    }
    let (start, end, partial) = match parse_range(req.header("range"), body.len()) {
        Ok(v) => v,
        Err(_) => return xml_error(416, "InvalidRange", "requested range is not satisfiable"),
    };
    let payload = if body.is_empty() {
        Vec::new()
    } else {
        body[start..=end].to_vec()
    };
    let mut r = Response::new(
        if partial { 206 } else { 200 },
        if head_only { Vec::new() } else { payload },
    );
    write_object_lock_headers(&mut r, &object);
    write_server_side_encryption_headers(&mut r, &object);
    r.headers.insert("ETag".to_string(), object.etag);
    r.headers.insert(
        "Last-Modified".to_string(),
        http_date_from_rfc3339(&object.last_modified),
    );
    r.headers
        .insert("Content-Type".to_string(), object.content_type);
    r.headers
        .insert("Accept-Ranges".to_string(), "bytes".to_string());
    let content_length = if body.is_empty() {
        0
    } else {
        end.saturating_sub(start) + 1
    };
    r.headers
        .insert("Content-Length".to_string(), content_length.to_string());
    if partial {
        r.headers.insert(
            "Content-Range".to_string(),
            format!("bytes {start}-{end}/{}", body.len()),
        );
    }
    if !object.version_id.is_empty() {
        r.headers
            .insert("x-amz-version-id".to_string(), object.version_id);
    }
    if !object.content_encoding.is_empty() {
        r.headers
            .insert("Content-Encoding".to_string(), object.content_encoding);
    }
    if !object.cache_control.is_empty() {
        r.headers
            .insert("Cache-Control".to_string(), object.cache_control);
    }
    if !object.content_disposition.is_empty() {
        r.headers.insert(
            "Content-Disposition".to_string(),
            object.content_disposition,
        );
    }
    for (key, value) in object.metadata {
        r.headers.insert(format!("x-amz-meta-{key}"), value);
    }
    r
}

/// `304 Not Modified` for a GET/HEAD whose `If-None-Match` names the object's
/// ETag: validator headers only, no body.
fn not_modified(object: Object) -> Response {
    let mut r = Response::empty(304);
    r.headers.insert("ETag".to_string(), object.etag);
    r.headers.insert(
        "Last-Modified".to_string(),
        http_date_from_rfc3339(&object.last_modified),
    );
    if !object.version_id.is_empty() {
        r.headers
            .insert("x-amz-version-id".to_string(), object.version_id);
    }
    if !object.cache_control.is_empty() {
        r.headers
            .insert("Cache-Control".to_string(), object.cache_control);
    }
    r
}

/// Whether an `If-None-Match`-style header (`*`, or a comma-separated list of
/// quoted, unquoted, or weak `W/` ETags) names `etag`. An empty header names
/// nothing. Uses weak comparison, as RFC 7232 prescribes for `If-None-Match`.
fn etag_list_matches(header: &str, etag: &str) -> bool {
    let target = etag.trim_matches('"');
    header.split(',').map(str::trim).any(|candidate| {
        candidate == "*"
            || (!candidate.is_empty()
                && candidate
                    .strip_prefix("W/")
                    .unwrap_or(candidate)
                    .trim_matches('"')
                    == target)
    })
}

fn list_objects(store: &FileBucketStore, req: &Request, bucket: &str) -> Response {
    if store.apply_bucket_lifecycle(bucket, &store.now()).is_err() {
        return xml_error(500, "InternalError", "internal error");
    }
    let prefix = req.query.get("prefix").map(String::as_str).unwrap_or("");
    let objects = match store.list_objects(bucket, prefix) {
        Ok(Some(objects)) => objects,
        Ok(None) => return xml_error(404, "NoSuchBucket", "bucket does not exist"),
        Err(StoreError::InvalidBucketName) => {
            return xml_error(400, "InvalidBucketName", "invalid bucket name")
        }
        Err(_) => return xml_error(500, "InternalError", "internal error"),
    };
    let max_keys = match parse_max_keys(req.query.get("max-keys").map(String::as_str).unwrap_or(""))
    {
        Ok(v) => v,
        Err(_) => return xml_error(400, "InvalidArgument", "invalid max-keys"),
    };
    let list_type_v2 = req.query.get("list-type").map(String::as_str) == Some("2");
    let mut marker = req.query.get("marker").cloned().unwrap_or_default();
    if list_type_v2 {
        if let Some(token) = req.query.get("continuation-token") {
            let Some(decoded) = decode_continuation_token(token) else {
                return xml_error(400, "InvalidArgument", "invalid continuation-token");
            };
            marker = decoded;
        } else {
            marker = req.query.get("start-after").cloned().unwrap_or_default();
        }
    }
    let delimiter = req.query.get("delimiter").map(String::as_str).unwrap_or("");
    let encoding_type = req
        .query
        .get("encoding-type")
        .map(String::as_str)
        .unwrap_or("");
    let listing = build_object_listing(&objects, prefix, delimiter, &marker, max_keys);
    let mut result = ListBucketResult {
        name: bucket.to_string(),
        prefix: encode_list_value(prefix, encoding_type),
        delimiter: encode_list_value(delimiter, encoding_type),
        // Echoed so SDKs know to URL-decode keys; only "url" encodes anything.
        encoding_type: if encoding_type == "url" {
            encoding_type.to_string()
        } else {
            String::new()
        },
        marker: encode_list_value(
            req.query.get("marker").map(String::as_str).unwrap_or(""),
            encoding_type,
        ),
        next_marker: String::new(),
        continuation_token: req
            .query
            .get("continuation-token")
            .cloned()
            .unwrap_or_default(),
        next_continuation_token: listing.next_continuation_token,
        start_after: encode_list_value(
            req.query
                .get("start-after")
                .map(String::as_str)
                .unwrap_or(""),
            encoding_type,
        ),
        key_count: (listing.contents.len() + listing.common_prefixes.len()) as i64,
        max_keys,
        is_truncated: listing.truncated,
        list_type: if list_type_v2 { 2 } else { 0 },
        contents: Vec::new(),
        common_prefixes: listing
            .common_prefixes
            .into_iter()
            .map(|p| encode_list_value(&p, encoding_type))
            .collect(),
    };
    if !list_type_v2 && !listing.next_marker.is_empty() {
        result.next_marker = encode_list_value(&listing.next_marker, encoding_type);
    }
    result.contents = listing
        .contents
        .into_iter()
        .map(|object| ObjectElement {
            key: encode_list_value(&object.key, encoding_type),
            last_modified: to_rfc3339_seconds(&object.last_modified),
            etag: object.etag,
            size: object.size,
            storage_class: "STANDARD".to_string(),
        })
        .collect();
    Response::xml(200, result.to_xml())
}

fn list_object_versions(store: &FileBucketStore, req: &Request, bucket: &str) -> Response {
    if store.apply_bucket_lifecycle(bucket, &store.now()).is_err() {
        return xml_error(500, "InternalError", "internal error");
    }
    let prefix = req.query.get("prefix").map(String::as_str).unwrap_or("");
    let versions = match store.list_object_versions(bucket, prefix) {
        Ok(Some(versions)) => versions,
        Ok(None) => return xml_error(404, "NoSuchBucket", "bucket does not exist"),
        Err(StoreError::InvalidBucketName) => {
            return xml_error(400, "InvalidBucketName", "invalid bucket name");
        }
        Err(_) => return xml_error(500, "InternalError", "internal error"),
    };
    let max_keys = match parse_max_keys(req.query.get("max-keys").map(String::as_str).unwrap_or(""))
    {
        Ok(v) => v,
        Err(_) => return xml_error(400, "InvalidArgument", "invalid max-keys"),
    };
    let key_marker = req.query.get("key-marker").cloned().unwrap_or_default();
    let version_id_marker = req
        .query
        .get("version-id-marker")
        .cloned()
        .unwrap_or_default();
    let latest = latest_object_version_ids(&versions);
    let listing = build_version_listing(&versions, &key_marker, &version_id_marker, max_keys);
    let mut result = ListVersionsResult {
        name: bucket.to_string(),
        prefix: prefix.to_string(),
        key_marker,
        version_id_marker,
        next_key_marker: listing.next_key_marker,
        next_version_id_marker: listing.next_version_id_marker,
        max_keys,
        is_truncated: listing.truncated,
        versions: Vec::new(),
        delete_markers: Vec::new(),
    };
    for object in listing.versions {
        let version_id = object_version_id(&object);
        let is_latest = latest.get(&object.key) == Some(&version_id);
        if object.delete_marker {
            result.delete_markers.push(DeleteMarkerElement {
                key: object.key,
                version_id,
                is_latest,
                last_modified: to_rfc3339_seconds(&object.last_modified),
            });
        } else {
            result.versions.push(VersionElement {
                key: object.key,
                version_id,
                is_latest,
                last_modified: to_rfc3339_seconds(&object.last_modified),
                etag: object.etag,
                size: object.size,
                storage_class: "STANDARD".to_string(),
            });
        }
    }
    Response::xml(200, result.to_xml())
}

fn user_metadata(headers: &BTreeMap<String, String>) -> BTreeMap<String, String> {
    headers
        .iter()
        .filter_map(|(k, v)| {
            k.strip_prefix("x-amz-meta-")
                .map(|name| (name.to_string(), v.clone()))
        })
        .collect()
}

/// Reads `If-None-Match` on PutObject/CopyObject/CompleteMultipartUpload. S3
/// accepts only `*` there; `Ok(true)` means the write must not replace a
/// current object.
fn if_none_match_write(req: &Request) -> Result<bool, Response> {
    match req.header("if-none-match").trim() {
        "" => Ok(false),
        "*" => Ok(true),
        _ => Err(xml_error(
            501,
            "NotImplemented",
            "A header you provided implies functionality that is not implemented",
        )),
    }
}

fn precondition_failed() -> Response {
    xml_error(
        412,
        "PreconditionFailed",
        "At least one of the pre-conditions you specified did not hold",
    )
}

fn parse_copy_source(source: &str) -> Option<(String, String, String)> {
    let source = source.strip_prefix('/').unwrap_or(source);
    let (source_path, raw_query) = source.split_once('?').unwrap_or((source, ""));
    if source_path.is_empty() {
        return None;
    }
    let (bucket, key) = source_path.split_once('/')?;
    if bucket.is_empty() || key.is_empty() {
        return None;
    }
    let version_id = raw_query
        .split('&')
        .filter_map(|part| part.split_once('='))
        .find_map(|(k, v)| (percent_decode(k) == "versionId").then(|| percent_decode(v)))
        .unwrap_or_default();
    Some((percent_decode(bucket), percent_decode(key), version_id))
}

fn write_object_lock_headers(response: &mut Response, object: &Object) {
    if !object.retention.mode.is_empty() {
        response.headers.insert(
            "x-amz-object-lock-mode".to_string(),
            object.retention.mode.clone(),
        );
    }
    if !object.retention.retain_until_date.is_empty() {
        response.headers.insert(
            "x-amz-object-lock-retain-until-date".to_string(),
            object.retention.retain_until_date.clone(),
        );
    }
    if !object.legal_hold.status.is_empty() {
        response.headers.insert(
            "x-amz-object-lock-legal-hold".to_string(),
            object.legal_hold.status.clone(),
        );
    }
}

fn has_object_lock_headers(req: &Request) -> bool {
    !req.header("x-amz-object-lock-mode").is_empty()
        || !req.header("x-amz-object-lock-retain-until-date").is_empty()
        || !req.header("x-amz-object-lock-legal-hold").is_empty()
}

fn bypass_governance_from_header(req: &Request) -> Option<bool> {
    match req.header("x-amz-bypass-governance-retention").trim() {
        "" => Some(false),
        "true" | "TRUE" | "True" | "1" | "t" | "T" => Some(true),
        "false" | "FALSE" | "False" | "0" | "f" | "F" => Some(false),
        _ => None,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ServerSideEncryptionHeaderError {
    Invalid,
    Unsupported,
}

fn has_server_side_encryption_headers(req: &Request) -> bool {
    [
        "x-amz-server-side-encryption",
        "x-amz-server-side-encryption-aws-kms-key-id",
        "x-amz-server-side-encryption-bucket-key-enabled",
        "x-amz-server-side-encryption-customer-algorithm",
        "x-amz-server-side-encryption-customer-key",
        "x-amz-server-side-encryption-customer-key-md5",
    ]
    .iter()
    .any(|name| !req.header(name).is_empty())
}

fn server_side_encryption_from_headers(
    req: &Request,
) -> Result<ServerSideEncryption, ServerSideEncryptionHeaderError> {
    if !req
        .header("x-amz-server-side-encryption-customer-algorithm")
        .is_empty()
        || !req
            .header("x-amz-server-side-encryption-customer-key")
            .is_empty()
        || !req
            .header("x-amz-server-side-encryption-customer-key-md5")
            .is_empty()
    {
        return Err(ServerSideEncryptionHeaderError::Unsupported);
    }

    let algorithm = req.header("x-amz-server-side-encryption").trim();
    let kms_key_id = req
        .header("x-amz-server-side-encryption-aws-kms-key-id")
        .trim();
    let bucket_key_value = req
        .header("x-amz-server-side-encryption-bucket-key-enabled")
        .trim();
    if algorithm.is_empty() {
        if !kms_key_id.is_empty() || !bucket_key_value.is_empty() {
            return Err(ServerSideEncryptionHeaderError::Invalid);
        }
        return Ok(ServerSideEncryption::default());
    }

    match algorithm {
        "AES256" => {
            if !kms_key_id.is_empty() || !bucket_key_value.is_empty() {
                return Err(ServerSideEncryptionHeaderError::Invalid);
            }
            Ok(ServerSideEncryption {
                algorithm: algorithm.to_string(),
                ..Default::default()
            })
        }
        "aws:kms" => {
            let bucket_key_enabled = if bucket_key_value.is_empty() {
                None
            } else {
                match bucket_key_value {
                    "true" | "TRUE" | "True" | "1" | "t" | "T" => Some(true),
                    "false" | "FALSE" | "False" | "0" | "f" | "F" => Some(false),
                    _ => return Err(ServerSideEncryptionHeaderError::Invalid),
                }
            };
            Ok(ServerSideEncryption {
                algorithm: algorithm.to_string(),
                kms_key_id: kms_key_id.to_string(),
                bucket_key_enabled,
            })
        }
        _ => Err(ServerSideEncryptionHeaderError::Unsupported),
    }
}

fn server_side_encryption_error(err: ServerSideEncryptionHeaderError) -> Response {
    match err {
        ServerSideEncryptionHeaderError::Unsupported => xml_error(
            501,
            "NotImplemented",
            "server-side encryption mode is not supported",
        ),
        ServerSideEncryptionHeaderError::Invalid => xml_error(
            400,
            "InvalidArgument",
            "server-side encryption headers are invalid",
        ),
    }
}

fn write_server_side_encryption_headers(response: &mut Response, object: &Object) {
    if !object.encryption.algorithm.is_empty() {
        response.headers.insert(
            "x-amz-server-side-encryption".to_string(),
            object.encryption.algorithm.clone(),
        );
    }
    if !object.encryption.kms_key_id.is_empty() {
        response.headers.insert(
            "x-amz-server-side-encryption-aws-kms-key-id".to_string(),
            object.encryption.kms_key_id.clone(),
        );
    }
    if let Some(enabled) = object.encryption.bucket_key_enabled {
        response.headers.insert(
            "x-amz-server-side-encryption-bucket-key-enabled".to_string(),
            enabled.to_string(),
        );
    }
}

fn parse_complete_multipart_parts(body: &[u8]) -> Option<Vec<i64>> {
    let xml = std::str::from_utf8(body).ok()?;
    if !xml.contains("<CompleteMultipartUpload") {
        return None;
    }
    let mut out = Vec::new();
    let mut rest = xml;
    while let Some(start) = rest.find("<PartNumber>") {
        rest = &rest[start + "<PartNumber>".len()..];
        let end = rest.find("</PartNumber>")?;
        let n = rest[..end].trim().parse::<i64>().ok()?;
        out.push(n);
        rest = &rest[end + "</PartNumber>".len()..];
    }
    Some(out)
}

fn acl_from_request(req: &Request) -> Option<String> {
    let canned = req.header("x-amz-acl").trim();
    if !canned.is_empty() {
        return supported_acl(canned).then(|| canned.to_string());
    }
    if req.body.iter().all(|b| b.is_ascii_whitespace()) {
        return Some("private".to_string());
    }
    let acl = tag_text(&req.body, "CannedACL").unwrap_or_else(|| "custom".to_string());
    supported_acl(&acl).then_some(acl)
}

fn supported_acl(acl: &str) -> bool {
    matches!(
        acl,
        "private"
            | "public-read"
            | "public-read-write"
            | "authenticated-read"
            | "bucket-owner-read"
            | "bucket-owner-full-control"
            | "custom"
    )
}

fn tag_text(body: &[u8], tag: &str) -> Option<String> {
    let xml = std::str::from_utf8(body).ok()?;
    tag_text_in(xml, tag)
}

pub(crate) fn tag_text_in(xml: &str, tag: &str) -> Option<String> {
    let open = format!("<{tag}>");
    let close = format!("</{tag}>");
    let start = xml.find(&open)? + open.len();
    let rest = &xml[start..];
    let end = rest.find(&close)?;
    Some(rest[..end].trim().to_string())
}

fn object_lock_from_headers(req: &Request) -> Option<(ObjectRetention, ObjectLegalHold)> {
    let retention = ObjectRetention {
        mode: req.header("x-amz-object-lock-mode").trim().to_string(),
        retain_until_date: req
            .header("x-amz-object-lock-retain-until-date")
            .trim()
            .to_string(),
    };
    let legal_hold = ObjectLegalHold {
        status: req
            .header("x-amz-object-lock-legal-hold")
            .trim()
            .to_string(),
    };
    if (!retention.mode.is_empty() || !retention.retain_until_date.is_empty())
        && !validate_object_retention(&retention)
    {
        return None;
    }
    if !legal_hold.status.is_empty() && !validate_object_legal_hold(&legal_hold) {
        return None;
    }
    Some((retention, legal_hold))
}

fn parse_object_retention(body: &[u8]) -> Option<ObjectRetention> {
    Some(ObjectRetention {
        mode: tag_text(body, "Mode")?,
        retain_until_date: tag_text(body, "RetainUntilDate")?,
    })
}

fn validate_object_retention(retention: &ObjectRetention) -> bool {
    matches!(retention.mode.trim(), "GOVERNANCE" | "COMPLIANCE")
        && !retention.retain_until_date.trim().is_empty()
        && parse_rfc3339(retention.retain_until_date.trim()).is_some()
}

fn parse_object_legal_hold(body: &[u8]) -> Option<ObjectLegalHold> {
    Some(ObjectLegalHold {
        status: tag_text(body, "Status")?,
    })
}

fn validate_object_legal_hold(legal_hold: &ObjectLegalHold) -> bool {
    matches!(legal_hold.status.trim(), "ON" | "OFF")
}

fn parse_object_lock_configuration(body: &[u8]) -> Option<ObjectLockConfiguration> {
    let xml = std::str::from_utf8(body).ok()?;
    if !xml.contains("<ObjectLockConfiguration") {
        return None;
    }
    let default_retention_xml = tag_block(xml, "DefaultRetention").unwrap_or_default();
    let days = tag_text_in(&default_retention_xml, "Days")
        .and_then(|v| v.parse::<i64>().ok())
        .unwrap_or(0);
    let years = tag_text_in(&default_retention_xml, "Years")
        .and_then(|v| v.parse::<i64>().ok())
        .unwrap_or(0);
    Some(ObjectLockConfiguration {
        xmlns: "http://s3.amazonaws.com/doc/2006-03-01/".to_string(),
        object_lock_enabled: tag_text_in(xml, "ObjectLockEnabled").unwrap_or_default(),
        rule: ObjectLockRule {
            default_retention: DefaultRetention {
                mode: tag_text_in(&default_retention_xml, "Mode").unwrap_or_default(),
                days,
                years,
            },
        },
    })
}

fn validate_object_lock_configuration(config: &ObjectLockConfiguration) -> bool {
    if !config.object_lock_enabled.is_empty() && config.object_lock_enabled != "Enabled" {
        return false;
    }
    let retention = &config.rule.default_retention;
    if retention.mode.is_empty() && retention.days == 0 && retention.years == 0 {
        return true;
    }
    if !matches!(retention.mode.as_str(), "GOVERNANCE" | "COMPLIANCE") {
        return false;
    }
    if retention.days > 0 && retention.years > 0 {
        return false;
    }
    if retention.days < 0 || retention.years < 0 {
        return false;
    }
    retention.days != 0 || retention.years != 0
}

fn parse_notification_configuration(body: &[u8]) -> Option<NotificationConfiguration> {
    let xml = std::str::from_utf8(body).ok()?;
    if !xml.contains("<NotificationConfiguration") {
        return None;
    }
    let topic_configurations = tag_blocks(xml, "TopicConfiguration")
        .into_iter()
        .map(|block| {
            Some(NotificationTopicConfig {
                id: tag_text_in(&block, "Id").unwrap_or_default(),
                topic: tag_text_in(&block, "Topic")?,
                events: tag_texts_in(&block, "Event"),
                filter: parse_notification_filter(&block),
            })
        })
        .collect::<Option<Vec<_>>>()?;
    let queue_configurations = tag_blocks(xml, "QueueConfiguration")
        .into_iter()
        .map(|block| {
            Some(NotificationQueueConfig {
                id: tag_text_in(&block, "Id").unwrap_or_default(),
                queue: tag_text_in(&block, "Queue")?,
                events: tag_texts_in(&block, "Event"),
                filter: parse_notification_filter(&block),
            })
        })
        .collect::<Option<Vec<_>>>()?;
    let lambda_function_configurations = tag_blocks(xml, "CloudFunctionConfiguration")
        .into_iter()
        .map(|block| {
            Some(NotificationLambdaConfig {
                id: tag_text_in(&block, "Id").unwrap_or_default(),
                lambda_function: tag_text_in(&block, "CloudFunction")?,
                events: tag_texts_in(&block, "Event"),
                filter: parse_notification_filter(&block),
            })
        })
        .collect::<Option<Vec<_>>>()?;
    let event_bridge_configuration = xml
        .contains("<EventBridgeConfiguration")
        .then_some(crate::model::EventBridgeConfiguration {});
    Some(NotificationConfiguration {
        xmlns: "http://s3.amazonaws.com/doc/2006-03-01/".to_string(),
        topic_configurations,
        queue_configurations,
        lambda_function_configurations,
        event_bridge_configuration,
    })
}

fn parse_notification_filter(xml: &str) -> NotificationFilter {
    let rules = tag_blocks(xml, "FilterRule")
        .into_iter()
        .filter_map(|block| {
            Some(NotificationFilterRule {
                name: tag_text_in(&block, "Name")?,
                value: tag_text_in(&block, "Value")?,
            })
        })
        .collect();
    NotificationFilter {
        s3_key: NotificationS3KeyFilter { rules },
    }
}

fn validate_notification_configuration(config: &NotificationConfiguration) -> bool {
    config.topic_configurations.iter().all(|c| {
        !c.topic.trim().is_empty()
            && !c.events.is_empty()
            && validate_notification_events_and_filter(&c.events, &c.filter)
    }) && config.queue_configurations.iter().all(|c| {
        !c.queue.trim().is_empty()
            && !c.events.is_empty()
            && validate_notification_events_and_filter(&c.events, &c.filter)
    }) && config.lambda_function_configurations.iter().all(|c| {
        !c.lambda_function.trim().is_empty()
            && !c.events.is_empty()
            && validate_notification_events_and_filter(&c.events, &c.filter)
    })
}

fn validate_notification_events_and_filter(events: &[String], filter: &NotificationFilter) -> bool {
    events
        .iter()
        .all(|event| supported_notification_event(event))
        && filter
            .s3_key
            .rules
            .iter()
            .all(|rule| matches!(rule.name.as_str(), "prefix" | "suffix"))
}

fn supported_notification_event(event: &str) -> bool {
    matches!(
        event,
        "s3:ObjectCreated:*"
            | "s3:ObjectCreated:Put"
            | "s3:ObjectCreated:Post"
            | "s3:ObjectCreated:Copy"
            | "s3:ObjectCreated:CompleteMultipartUpload"
            | "s3:ObjectRemoved:*"
            | "s3:ObjectRemoved:Delete"
            | "s3:ObjectRemoved:DeleteMarkerCreated"
    )
}

fn record_object_event(
    store: &FileBucketStore,
    bucket: &str,
    key: &str,
    event_name: &str,
    object: &Object,
) -> Result<(), StoreError> {
    let Some(config) = store.get_bucket_notification(bucket)? else {
        return Ok(());
    };
    if !notification_matches(&config, event_name, key) {
        return Ok(());
    }
    store.append_notification_event(
        bucket,
        NotificationEventRecord {
            event_id: store.new_version_id(),
            event_name: event_name.to_string(),
            event_time: store.now(),
            bucket: bucket.to_string(),
            key: key.to_string(),
            etag: object.etag.clone(),
            size: object.size,
            version_id: object.version_id.clone(),
            delete_marker: object.delete_marker,
        },
    )?;
    Ok(())
}

fn notification_matches(config: &NotificationConfiguration, event_name: &str, key: &str) -> bool {
    config
        .topic_configurations
        .iter()
        .any(|c| notification_rule_matches(&c.events, &c.filter, event_name, key))
        || config
            .queue_configurations
            .iter()
            .any(|c| notification_rule_matches(&c.events, &c.filter, event_name, key))
        || config
            .lambda_function_configurations
            .iter()
            .any(|c| notification_rule_matches(&c.events, &c.filter, event_name, key))
}

fn notification_rule_matches(
    events: &[String],
    filter: &NotificationFilter,
    event_name: &str,
    key: &str,
) -> bool {
    let event_matches = events.iter().any(|event| {
        event == event_name
            || event.ends_with(":*") && event_name.starts_with(event.trim_end_matches('*'))
    });
    if !event_matches {
        return false;
    }
    filter
        .s3_key
        .rules
        .iter()
        .all(|rule| match rule.name.as_str() {
            "prefix" => key.starts_with(&rule.value),
            "suffix" => key.ends_with(&rule.value),
            _ => false,
        })
}

fn replicate_object_write(
    store: &FileBucketStore,
    bucket: &str,
    key: &str,
    object: &Object,
) -> Result<(), StoreError> {
    let Some((config, true)) = store.get_bucket_replication(bucket)? else {
        return Ok(());
    };
    if object.delete_marker {
        return Ok(());
    }
    let Some((_, body)) = store.get_object_version(bucket, key, &object.version_id)? else {
        return Ok(());
    };
    for rule in &config.rules {
        if rule.status != "Enabled" || !replication_rule_matches(rule, key) {
            continue;
        }
        let Ok(destination_bucket) = replication_destination_bucket(&rule.destination.bucket)
        else {
            continue;
        };
        if destination_bucket == bucket {
            continue;
        }
        if store.get_bucket(&destination_bucket)?.is_none() {
            continue;
        }
        store.put_object(PutObjectInput {
            bucket: destination_bucket,
            key: key.to_string(),
            body: body.clone(),
            content_type: object.content_type.clone(),
            content_encoding: object.content_encoding.clone(),
            cache_control: object.cache_control.clone(),
            content_disposition: object.content_disposition.clone(),
            metadata: object.metadata.clone(),
            encryption: object.encryption.clone(),
            retention: object.retention.clone(),
            legal_hold: object.legal_hold.clone(),
            ..Default::default()
        })?;
    }
    Ok(())
}

fn replicate_object_delete_marker(
    store: &FileBucketStore,
    bucket: &str,
    key: &str,
) -> Result<(), StoreError> {
    let Some((config, true)) = store.get_bucket_replication(bucket)? else {
        return Ok(());
    };
    for rule in &config.rules {
        if rule.status != "Enabled"
            || rule.delete_marker_replication.status != "Enabled"
            || !replication_rule_matches(rule, key)
        {
            continue;
        }
        let Ok(destination_bucket) = replication_destination_bucket(&rule.destination.bucket)
        else {
            continue;
        };
        if destination_bucket == bucket {
            continue;
        }
        if store.get_bucket(&destination_bucket)?.is_none() {
            continue;
        }
        store.delete_object_with_result(&destination_bucket, key, false)?;
    }
    Ok(())
}

fn replication_rule_matches(rule: &ReplicationRule, key: &str) -> bool {
    let prefix = if rule.filter.prefix.is_empty() {
        &rule.prefix
    } else {
        &rule.filter.prefix
    };
    prefix.is_empty() || key.starts_with(prefix)
}

fn configuration_id(req: &Request) -> Option<String> {
    let id = req.query.get("id")?.trim().to_string();
    valid_configuration_id(&id).then_some(id)
}

fn valid_configuration_id(id: &str) -> bool {
    !id.is_empty() && id.len() <= 64 && id.chars().all(|c| c >= '\u{20}' && c != '\u{7f}')
}

fn parse_replication_configuration(body: &[u8]) -> Option<ReplicationConfiguration> {
    let xml = std::str::from_utf8(body).ok()?;
    if !xml.contains("<ReplicationConfiguration") {
        return None;
    }
    let rules = tag_blocks(xml, "Rule")
        .into_iter()
        .map(|block| {
            let filter_xml = tag_block(&block, "Filter").unwrap_or_default();
            let destination_xml = tag_block(&block, "Destination").unwrap_or_default();
            let delete_marker_xml =
                tag_block(&block, "DeleteMarkerReplication").unwrap_or_default();
            let rule_xml = remove_tag_block(&block, "DeleteMarkerReplication");
            ReplicationRule {
                id: tag_text_in(&rule_xml, "ID").unwrap_or_default(),
                priority: tag_text_in(&rule_xml, "Priority")
                    .and_then(|v| v.parse::<i64>().ok())
                    .unwrap_or(0),
                prefix: tag_text_in(&remove_tag_block(&rule_xml, "Filter"), "Prefix")
                    .unwrap_or_default(),
                filter: ReplicationFilter {
                    prefix: tag_text_in(&filter_xml, "Prefix").unwrap_or_default(),
                },
                status: tag_text_in(&rule_xml, "Status").unwrap_or_default(),
                destination: ReplicationDestination {
                    bucket: tag_text_in(&destination_xml, "Bucket").unwrap_or_default(),
                    storage_class: tag_text_in(&destination_xml, "StorageClass")
                        .unwrap_or_default(),
                },
                delete_marker_replication: ReplicationDeleteMarkerSetting {
                    status: tag_text_in(&delete_marker_xml, "Status").unwrap_or_default(),
                },
            }
        })
        .collect();
    Some(ReplicationConfiguration {
        xmlns: "http://s3.amazonaws.com/doc/2006-03-01/".to_string(),
        role: tag_text_in(xml, "Role").unwrap_or_default(),
        rules,
    })
}

fn validate_replication_configuration(config: &ReplicationConfiguration) -> bool {
    !config.rules.is_empty()
        && config.rules.iter().all(|rule| {
            matches!(rule.status.as_str(), "Enabled" | "Disabled")
                && replication_destination_bucket(&rule.destination.bucket)
                    .map(|bucket| valid_bucket_name(&bucket))
                    .unwrap_or(false)
                && matches!(
                    rule.delete_marker_replication.status.as_str(),
                    "" | "Enabled" | "Disabled"
                )
                && (rule.destination.storage_class.is_empty()
                    || supported_replication_storage_class(&rule.destination.storage_class))
        })
}

fn replication_destination_bucket(value: &str) -> Result<String, ()> {
    let mut value = value.trim();
    if value.is_empty() {
        return Err(());
    }
    if let Some(stripped) = value.strip_prefix("arn:aws:s3:::") {
        value = stripped
            .split_once('/')
            .map(|(bucket, _)| bucket)
            .unwrap_or(stripped);
    }
    if value.is_empty() {
        Err(())
    } else {
        Ok(value.to_string())
    }
}

fn supported_replication_storage_class(value: &str) -> bool {
    matches!(
        value,
        "STANDARD"
            | "STANDARD_IA"
            | "ONEZONE_IA"
            | "INTELLIGENT_TIERING"
            | "GLACIER"
            | "DEEP_ARCHIVE"
            | "GLACIER_IR"
    )
}

fn parse_inventory_configuration(body: &[u8]) -> Option<InventoryConfiguration> {
    let xml = std::str::from_utf8(body).ok()?;
    if !xml.contains("<InventoryConfiguration") {
        return None;
    }
    let destination_xml = tag_block(xml, "S3BucketDestination").unwrap_or_default();
    let schedule_xml = tag_block(xml, "Schedule").unwrap_or_default();
    Some(InventoryConfiguration {
        xmlns: "http://s3.amazonaws.com/doc/2006-03-01/".to_string(),
        id: tag_text_in(xml, "Id").unwrap_or_default(),
        is_enabled: tag_text_in(xml, "IsEnabled")
            .map(|v| v == "true")
            .unwrap_or(false),
        included_object_versions: tag_text_in(xml, "IncludedObjectVersions").unwrap_or_default(),
        schedule: InventorySchedule {
            frequency: tag_text_in(&schedule_xml, "Frequency").unwrap_or_default(),
        },
        destination: InventoryDestination {
            s3_bucket_destination: InventoryS3BucketDestination {
                account_id: tag_text_in(&destination_xml, "AccountId").unwrap_or_default(),
                bucket: tag_text_in(&destination_xml, "Bucket").unwrap_or_default(),
                format: tag_text_in(&destination_xml, "Format").unwrap_or_default(),
                prefix: tag_text_in(&destination_xml, "Prefix").unwrap_or_default(),
            },
        },
        optional_fields: tag_texts_in(xml, "Field"),
    })
}

fn validate_inventory_configuration(id: &str, config: &InventoryConfiguration) -> bool {
    valid_configuration_id(id)
        && (config.id.is_empty() || config.id == id)
        && matches!(
            config.included_object_versions.trim(),
            "" | "All" | "Current"
        )
        && matches!(config.schedule.frequency.trim(), "" | "Daily" | "Weekly")
        && matches!(
            config.destination.s3_bucket_destination.format.trim(),
            "" | "CSV" | "ORC" | "Parquet"
        )
}

fn parse_analytics_configuration(body: &[u8]) -> Option<AnalyticsConfiguration> {
    let xml = std::str::from_utf8(body).ok()?;
    if !xml.contains("<AnalyticsConfiguration") {
        return None;
    }
    let filter_xml = tag_block(xml, "Filter").unwrap_or_default();
    let data_export_xml = tag_block(xml, "DataExport").unwrap_or_default();
    let destination_xml = tag_block(xml, "S3BucketDestination").unwrap_or_default();
    Some(AnalyticsConfiguration {
        xmlns: "http://s3.amazonaws.com/doc/2006-03-01/".to_string(),
        id: tag_text_in(xml, "Id").unwrap_or_default(),
        filter: AnalyticsFilter {
            prefix: tag_text_in(&filter_xml, "Prefix").unwrap_or_default(),
        },
        storage_class_analysis: StorageClassAnalysis {
            data_export: AnalyticsDataExport {
                output_schema_version: tag_text_in(&data_export_xml, "OutputSchemaVersion")
                    .unwrap_or_default(),
                destination: AnalyticsDestination {
                    s3_bucket_destination: AnalyticsS3BucketDestination {
                        format: tag_text_in(&destination_xml, "Format").unwrap_or_default(),
                        bucket: tag_text_in(&destination_xml, "Bucket").unwrap_or_default(),
                        prefix: tag_text_in(&destination_xml, "Prefix").unwrap_or_default(),
                    },
                },
            },
        },
    })
}

fn validate_analytics_configuration(id: &str, config: &AnalyticsConfiguration) -> bool {
    valid_configuration_id(id)
        && (config.id.is_empty() || config.id == id)
        && matches!(
            config
                .storage_class_analysis
                .data_export
                .output_schema_version
                .trim(),
            "" | "V_1"
        )
        && matches!(
            config
                .storage_class_analysis
                .data_export
                .destination
                .s3_bucket_destination
                .format
                .trim(),
            "" | "CSV"
        )
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LifecycleParseError {
    Malformed,
    Unsupported,
}

fn parse_lifecycle_configuration(
    body: &[u8],
) -> std::result::Result<LifecycleConfiguration, LifecycleParseError> {
    let xml = std::str::from_utf8(body).map_err(|_| LifecycleParseError::Malformed)?;
    if !xml.contains("<LifecycleConfiguration") {
        return Err(LifecycleParseError::Malformed);
    }
    if xml.contains("<Transition")
        || xml.contains("<NoncurrentVersionTransition")
        || xml.contains("<NoncurrentVersionExpiration")
        || xml.contains("<AbortIncompleteMultipartUpload")
    {
        return Err(LifecycleParseError::Unsupported);
    }

    let mut rules = Vec::new();
    let mut rest = xml;
    while let Some(start) = rest.find("<Rule>") {
        rest = &rest[start + "<Rule>".len()..];
        let end = rest.find("</Rule>").ok_or(LifecycleParseError::Malformed)?;
        let rule_xml = &rest[..end];
        rules.push(parse_lifecycle_rule(rule_xml)?);
        rest = &rest[end + "</Rule>".len()..];
    }
    if rules.is_empty() {
        return Err(LifecycleParseError::Malformed);
    }
    Ok(LifecycleConfiguration {
        xmlns: "http://s3.amazonaws.com/doc/2006-03-01/".to_string(),
        rules,
    })
}

fn parse_lifecycle_rule(xml: &str) -> std::result::Result<LifecycleRule, LifecycleParseError> {
    let status = tag_text_in(xml, "Status").ok_or(LifecycleParseError::Malformed)?;
    if status != "Enabled" && status != "Disabled" {
        return Err(LifecycleParseError::Malformed);
    }

    let id = tag_text_in(xml, "ID").unwrap_or_default();
    let filter_xml = tag_block(xml, "Filter").unwrap_or_default();
    let filter_prefix = tag_text_in(&filter_xml, "Prefix").unwrap_or_default();
    let top_level_xml = remove_tag_block(xml, "Filter");
    let prefix = tag_text_in(&top_level_xml, "Prefix").unwrap_or_default();

    let expiration_xml = tag_block(xml, "Expiration").ok_or(LifecycleParseError::Malformed)?;
    let days = match tag_text_in(&expiration_xml, "Days") {
        Some(value) => {
            let parsed = value
                .parse::<i64>()
                .map_err(|_| LifecycleParseError::Malformed)?;
            if parsed < 0 {
                return Err(LifecycleParseError::Malformed);
            }
            Some(parsed)
        }
        None => None,
    };
    let date = tag_text_in(&expiration_xml, "Date").unwrap_or_default();
    if days.is_none() && date.trim().is_empty() {
        return Err(LifecycleParseError::Malformed);
    }
    if !date.is_empty() && parse_lifecycle_date(&date).is_none() {
        return Err(LifecycleParseError::Malformed);
    }

    Ok(LifecycleRule {
        id,
        prefix,
        filter: LifecycleFilter {
            prefix: filter_prefix,
        },
        status,
        expiration: LifecycleExpiration { days, date },
    })
}

fn tag_block(xml: &str, tag: &str) -> Option<String> {
    let open = format!("<{tag}>");
    let close = format!("</{tag}>");
    let start = xml.find(&open)? + open.len();
    let rest = &xml[start..];
    let end = rest.find(&close)?;
    Some(rest[..end].to_string())
}

fn tag_blocks(xml: &str, tag: &str) -> Vec<String> {
    let open = format!("<{tag}>");
    let close = format!("</{tag}>");
    let mut out = Vec::new();
    let mut rest = xml;
    while let Some(start) = rest.find(&open) {
        rest = &rest[start + open.len()..];
        let Some(end) = rest.find(&close) else {
            break;
        };
        out.push(rest[..end].to_string());
        rest = &rest[end + close.len()..];
    }
    out
}

fn tag_texts_in(xml: &str, tag: &str) -> Vec<String> {
    let open = format!("<{tag}>");
    let close = format!("</{tag}>");
    let mut out = Vec::new();
    let mut rest = xml;
    while let Some(start) = rest.find(&open) {
        rest = &rest[start + open.len()..];
        let Some(end) = rest.find(&close) else {
            break;
        };
        out.push(rest[..end].trim().to_string());
        rest = &rest[end + close.len()..];
    }
    out
}

fn remove_tag_block(xml: &str, tag: &str) -> String {
    let Some(start) = xml.find(&format!("<{tag}>")) else {
        return xml.to_string();
    };
    let close = format!("</{tag}>");
    let Some(relative_end) = xml[start..].find(&close) else {
        return xml.to_string();
    };
    let end = start + relative_end + close.len();
    let mut out = String::with_capacity(xml.len().saturating_sub(end - start));
    out.push_str(&xml[..start]);
    out.push_str(&xml[end..]);
    out
}

fn parse_path_style(path: &str) -> Option<(String, String)> {
    let trimmed = path.strip_prefix('/').unwrap_or(path);
    if trimmed.is_empty() {
        return None;
    }
    let (bucket, key) = trimmed.split_once('/').unwrap_or((trimmed, ""));
    if bucket.is_empty() {
        None
    } else {
        Some((percent_decode(bucket), percent_decode(key)))
    }
}

fn parse_target(target: &str) -> (String, BTreeMap<String, String>) {
    let (path, query) = target.split_once('?').unwrap_or((target, ""));
    let mut values = BTreeMap::new();
    for part in query.split('&').filter(|p| !p.is_empty()) {
        let (k, v) = part.split_once('=').unwrap_or((part, ""));
        values.insert(percent_decode(k), percent_decode(v));
    }
    (path.to_string(), values)
}

pub(crate) fn percent_decode(value: &str) -> String {
    let bytes = value.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            if let (Some(a), Some(b)) = (hex_val(bytes[i + 1]), hex_val(bytes[i + 2])) {
                out.push(a * 16 + b);
                i += 3;
                continue;
            }
        }
        out.push(bytes[i]);
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
    let spec = &header["bytes=".len()..];
    let (left, right) = spec.split_once('-').ok_or(())?;
    if left.is_empty() {
        let suffix: usize = right.parse().map_err(|_| ())?;
        if suffix == 0 {
            return Err(());
        }
        let take = suffix.min(size);
        return Ok((size - take, size - 1, true));
    }
    let start: usize = left.parse().map_err(|_| ())?;
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

fn method_not_allowed(allow: &str) -> Response {
    let mut r = xml_error(405, "MethodNotAllowed", "method not allowed");
    r.headers.insert("Allow".to_string(), allow.to_string());
    r
}

fn emit_dashboard_event(event_type: &str, payload: serde_json::Value) {
    let event = serde_json::json!({
        "type": event_type,
        "service": "s3",
        "payload": payload,
    });
    let json = event.to_string();
    if let Some(tx) = crate::event_sink() {
        let _ = tx.send(json.clone());
    }
    println!("{DASHBOARD_EVENT_PREFIX}{json}");
}

fn xml_error(status: u16, code: &str, message: &str) -> Response {
    Response::xml(status, error_xml(code, message))
}

fn to_rfc3339_seconds(value: &str) -> String {
    parse_rfc3339(value)
        .map(|(secs, _)| rfc3339_seconds_from_unix(secs))
        .unwrap_or_else(|| value.to_string())
}

fn http_date_from_rfc3339(value: &str) -> String {
    let Some((secs, _)) = parse_rfc3339(value) else {
        return value.to_string();
    };
    http_date_from_unix(secs)
}

fn http_date_from_unix(secs: i64) -> String {
    const WEEKDAYS: [&str; 7] = ["Thu", "Fri", "Sat", "Sun", "Mon", "Tue", "Wed"];
    const MONTHS: [&str; 12] = [
        "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
    ];
    let days = secs.div_euclid(86_400);
    let sod = secs.rem_euclid(86_400);
    let weekday = WEEKDAYS[days.rem_euclid(7) as usize];
    let (year, month, day) = civil_from_days(days);
    format!(
        "{weekday}, {day:02} {} {year:04} {:02}:{:02}:{:02} GMT",
        MONTHS[(month - 1) as usize],
        sod / 3600,
        (sod % 3600) / 60,
        sod % 60
    )
}

fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    (if m <= 2 { y + 1 } else { y }, m, d)
}

pub(crate) async fn write_response(
    stream: &mut TcpStream,
    response: Response,
) -> std::io::Result<()> {
    let mut headers = response.headers;
    headers.insert("Server".to_string(), "AmazonS3".to_string());
    // A 304 carries no body, so it must not advertise a zero Content-Length
    // for the representation it stands in for.
    if response.status != 304 {
        headers
            .entry("Content-Length".to_string())
            .or_insert_with(|| response.body.len().to_string());
    }
    headers.insert("Connection".to_string(), "close".to_string());

    let mut head = format!(
        "HTTP/1.1 {} {}\r\n",
        response.status,
        reason_phrase(response.status)
    );
    for (k, v) in headers {
        head.push_str(&k);
        head.push_str(": ");
        head.push_str(&v);
        head.push_str("\r\n");
    }
    head.push_str("\r\n");
    stream.write_all(head.as_bytes()).await?;
    stream.write_all(&response.body).await?;
    stream.flush().await
}

fn reason_phrase(status: u16) -> &'static str {
    match status {
        200 => "OK",
        204 => "No Content",
        206 => "Partial Content",
        304 => "Not Modified",
        400 => "Bad Request",
        403 => "Forbidden",
        404 => "Not Found",
        405 => "Method Not Allowed",
        409 => "Conflict",
        412 => "Precondition Failed",
        416 => "Requested Range Not Satisfiable",
        500 => "Internal Server Error",
        501 => "Not Implemented",
        _ => "Status",
    }
}

fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    if needle.is_empty() || haystack.len() < needle.len() {
        return None;
    }
    haystack.windows(needle.len()).position(|w| w == needle)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Feeds `wire` through an in-memory stream in small pieces, so reads split
    /// across chunk boundaries are exercised.
    async fn parse_wire(wire: &[u8]) -> Option<Request> {
        let (mut client, mut server) = tokio::io::duplex(16);
        let wire = wire.to_vec();
        tokio::spawn(async move {
            let _ = client.write_all(&wire).await;
        });
        read_request(&mut server).await.unwrap()
    }

    fn wire(headers: &str, body: &str) -> Vec<u8> {
        format!("PUT /data/k HTTP/1.1\r\nHost: h\r\n{headers}\r\n{body}").into_bytes()
    }

    #[tokio::test]
    async fn repeated_content_length_must_agree() {
        let same = parse_wire(&wire("Content-Length: 5\r\nContent-Length: 5\r\n", "hello")).await;
        assert_eq!(same.unwrap().body, b"hello");
        for headers in [
            "Content-Length: 5\r\nContent-Length: 4\r\n",
            "Content-Length: abc\r\n",
            "Content-Length: 5,\r\n",
        ] {
            assert!(
                parse_wire(&wire(headers, "hello")).await.is_none(),
                "{headers}"
            );
        }
        assert_eq!(parse_wire(&wire("", "")).await.unwrap().body, b"");
    }

    #[tokio::test]
    async fn http_chunked_bodies_are_decoded() {
        let te = "Transfer-Encoding: chunked\r\n";
        let long = "x".repeat(70000);
        let body = format!(
            "5;ext=1\r\nhello\r\n{:x}\r\n{long}\r\n0\r\nTrailer: a\r\n\r\n",
            long.len()
        );
        let parsed = parse_wire(&wire(te, &body)).await.unwrap();
        assert_eq!(parsed.body, format!("hello{long}").into_bytes());
        for bad in [
            "5\r\nhello\r\n",
            "5\r\nhelloXX0\r\n\r\n",
            "zz\r\nx\r\n0\r\n\r\n",
            "ffffffffffffffff\r\nx",
            "0\r\n",
        ] {
            assert!(parse_wire(&wire(te, bad)).await.is_none(), "{bad}");
        }
    }

    #[tokio::test]
    async fn aws_chunked_payload_inside_http_chunked_reaches_the_object() {
        let framed = "b\r\nhello world\r\n0\r\nx-amz-checksum-crc32:AAAA\r\n\r\n";
        let body = format!("{:x}\r\n{framed}\r\n0\r\n\r\n", framed.len());
        let headers = "Transfer-Encoding: chunked\r\nContent-Encoding: aws-chunked\r\n\
                       x-amz-content-sha256: STREAMING-UNSIGNED-PAYLOAD-TRAILER\r\n";
        let request = parse_wire(&wire(headers, &body)).await.unwrap();

        let root = std::env::temp_dir().join(format!("devcloud-s3-te-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).unwrap();
        let store = FileBucketStore::new(&root);
        assert_eq!(
            route(&store, &Request::new("PUT", "/data", Vec::new())).status,
            200
        );
        assert_eq!(route(&store, &request).status, 200);
        let got = route(&store, &Request::new("GET", "/data/k", Vec::new()));
        assert_eq!(got.body, b"hello world");
    }

    /// The worked example in AWS's "Signature Calculations for the Authorization
    /// Header: Transferring Payload in Multiple Chunks" documentation.
    #[test]
    fn chunk_signatures_match_the_aws_documented_example() {
        let date_stamp = "20130524";
        let signing = ChunkSigning {
            signing_key: derive_signing_key(
                "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
                date_stamp,
                "us-east-1",
            ),
            amz_date: "20130524T000000Z".to_string(),
            scope: format!("{date_stamp}/us-east-1/s3/aws4_request"),
            seed_signature: "4f232c4386841ef735655705268965c44a0e4690baa4adea153f7db9fa80a0a9"
                .to_string(),
            has_trailer: false,
        };
        let mut body = Vec::new();
        for (size, signature) in [
            (
                65536,
                "ad80c730a21e5b8d04586a2213dd63b9a0e99e0e2307b0ade35a65485a288648",
            ),
            (
                1024,
                "0055627c9e194cb4542bae2aa5492e3c1575bbb81b612b7d234b86a503ef5497",
            ),
            (
                0,
                "b6c6ea8a5354eaf15b3cb7646744f4275b71ea724fed81ceb9323e279d449df9",
            ),
        ] {
            body.extend(format!("{size:x};chunk-signature={signature}\r\n").into_bytes());
            body.extend(vec![b'a'; size]);
            body.extend_from_slice(b"\r\n");
        }
        let parsed = aws_chunked::parse(&body).unwrap();
        assert_eq!(parsed.payload().len(), 66560);
        assert!(verify_chunk_signatures(&parsed, &signing).is_ok());
    }
}
