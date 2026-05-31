use std::collections::BTreeMap;
use std::fs;
use std::future::Future;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use devcloud_s3::base64;
use devcloud_s3::model::{Bucket, Object};
use devcloud_s3::objops::{PutObjectInput, UpdateObjectMetadataInput};
use devcloud_s3::store::{FileBucketStore, StoreError};
use devcloud_s3::time_fmt::parse_rfc3339;
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
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct ResumableSession {
    bucket: String,
    name: String,
    content_type: String,
    content_encoding: String,
    cache_control: String,
    content_disposition: String,
    metadata: BTreeMap<String, String>,
    if_generation_match: String,
    if_generation_not_match: String,
    if_metageneration_match: String,
    if_metageneration_not_match: String,
    received_bytes: i64,
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
    fn json<T: Serialize>(status: u16, value: &T) -> Self {
        let mut headers = BTreeMap::new();
        headers.insert(
            "Content-Type".to_string(),
            "application/json; charset=utf-8".to_string(),
        );
        Self {
            status,
            headers,
            body: serde_json::to_vec(value).unwrap_or_else(|_| b"{}".to_vec()),
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
    name: String,
    #[serde(default)]
    location: String,
    #[serde(rename = "storageClass", default)]
    storage_class: String,
}

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
    metadata: BTreeMap<String, String>,
}

#[derive(Debug, Default, Deserialize)]
struct CopyDestinationRequest {
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
    match req.path.as_str() {
        "/storage/v1/b" => handle_buckets(server, req),
        p if p.starts_with("/storage/v1/b/") => handle_bucket_or_object(server, req),
        p if p.starts_with("/upload/storage/v1/b/") => handle_upload(server, req),
        p if p.starts_with("/download/storage/v1/b/") => handle_download(server, req),
        _ => json_error(404, "notFound", "not found"),
    }
}

fn handle_buckets(server: &mut Server, req: &Request) -> Response {
    match req.method.as_str() {
        "GET" => {
            let prefix = req.query.get("prefix").map(String::as_str).unwrap_or("");
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
                Err(e) => json_error(400, "invalid", &store_error_message(&e)),
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
        if let Some((source, copy_suffix)) = object.split_once("/copyTo/b/") {
            return handle_copy_object(server, req, &bucket, source, copy_suffix);
        }
        if let Some((source, rewrite_suffix)) = object.split_once("/rewriteTo/b/") {
            return handle_rewrite_object(server, req, &bucket, source, rewrite_suffix);
        }
        if let Some(dest) = object.strip_suffix("/compose") {
            return handle_compose_object(server, req, &bucket, dest);
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
            Err(e) => json_error(400, "invalid", &store_error_message(&e)),
        },
        "DELETE" => match server.store.delete_bucket(name) {
            Ok(true) => {
                emit_dashboard_event("gcs.bucket.deleted", serde_json::json!({"bucket": name}));
                Response::empty(204)
            }
            Ok(false) => json_error(404, "notFound", "bucket not found"),
            Err(e) => json_error(409, "conflict", &store_error_message(&e)),
        },
        _ => method_not_allowed("GET, DELETE"),
    }
}

fn handle_objects(server: &mut Server, req: &Request, bucket: &str) -> Response {
    if req.method != "GET" {
        return method_not_allowed("GET");
    }
    let prefix = req.query.get("prefix").map(String::as_str).unwrap_or("");
    let delimiter = req.query.get("delimiter").map(String::as_str).unwrap_or("");
    let start_offset = req
        .query
        .get("startOffset")
        .map(String::as_str)
        .unwrap_or("");
    let end_offset = req.query.get("endOffset").map(String::as_str).unwrap_or("");
    let include_trailing = req
        .query
        .get("includeTrailingDelimiter")
        .map(|v| v.eq_ignore_ascii_case("true"))
        .unwrap_or(false);
    let objects = match server.store.list_objects(bucket, prefix) {
        Ok(Some(v)) => v,
        Ok(None) => return json_error(404, "notFound", "bucket not found"),
        Err(e) => return json_error(400, "invalid", &store_error_message(&e)),
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

fn handle_object(server: &mut Server, req: &Request, bucket: &str, key: &str) -> Response {
    match req.method.as_str() {
        "GET" | "HEAD" => {
            let Some((object, body)) = get_object_or_response(server, bucket, key) else {
                return last_error();
            };
            match generation_matches(req, &object) {
                Ok(true) => {}
                Ok(false) => return json_error(404, "notFound", "object not found"),
                Err(response) => return response,
            }
            if !check_preconditions(server, req, bucket, key) {
                return last_error();
            }
            if req.query.get("alt").map(String::as_str) == Some("media") {
                return media_response(req, &object, &body);
            }
            let mut r = if req.method == "HEAD" {
                Response::empty(200)
            } else {
                Response::json(200, &object_resource(&object))
            };
            set_object_headers(&mut r, &object);
            if req.method == "HEAD" {
                r.headers.insert(
                    "Content-Type".to_string(),
                    "application/json; charset=utf-8".to_string(),
                );
            }
            r
        }
        "PATCH" => {
            let Some((object, _)) = get_object_or_response(server, bucket, key) else {
                return last_error();
            };
            match generation_matches(req, &object) {
                Ok(true) => {}
                Ok(false) => return json_error(404, "notFound", "object not found"),
                Err(response) => return response,
            }
            if !check_preconditions(server, req, bucket, key) {
                return last_error();
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
                    metadata: Some(parsed.metadata),
                }) {
                Ok(Some(object)) => Response::json(200, &object_resource(&object)),
                Ok(None) => json_error(404, "notFound", "object not found"),
                Err(StoreError::BucketNotExist) => json_error(404, "notFound", "bucket not found"),
                Err(e) => json_error(400, "invalid", &store_error_message(&e)),
            }
        }
        "DELETE" => {
            let Some((object, _)) = get_object_or_response(server, bucket, key) else {
                return last_error();
            };
            match generation_matches(req, &object) {
                Ok(true) => {}
                Ok(false) => return json_error(404, "notFound", "object not found"),
                Err(response) => return response,
            }
            if !check_preconditions(server, req, bucket, key) {
                return last_error();
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
                Err(StoreError::BucketNotExist) => json_error(404, "notFound", "bucket not found"),
                Err(e) => json_error(400, "invalid", &store_error_message(&e)),
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
    if !check_preconditions(server, req, bucket, &dest_key) {
        return last_error();
    }

    let mut body = Vec::new();
    let mut first_source: Option<Object> = None;
    for source in &parsed.source_objects {
        if source.name.is_empty() {
            return json_error(400, "required", "source object name is required");
        }
        let Some((object, payload)) = get_object_or_response(server, bucket, &source.name) else {
            return last_error();
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
        key: dest_key.to_string(),
        body,
        content_type: first_non_empty(&parsed.destination.content_type, &first_source.content_type),
        content_encoding: parsed.destination.content_encoding,
        cache_control: parsed.destination.cache_control,
        content_disposition: parsed.destination.content_disposition,
        metadata: parsed.destination.metadata,
        ..Default::default()
    }) {
        Ok(object) => {
            emit_dashboard_event(
                "gcs.object.put",
                serde_json::json!({
                    "bucket": bucket,
                    "key": dest_key,
                    "etag": object.etag,
                    "contentLength": object.size,
                }),
            );
            Response::json(200, &object_resource(&object))
        }
        Err(StoreError::BucketNotExist) => json_error(404, "notFound", "bucket not found"),
        Err(e) => json_error(400, "invalid", &store_error_message(&e)),
    }
}

fn copy_object(
    server: &mut Server,
    req: &Request,
    source_bucket: &str,
    source_escaped: &str,
    dest_suffix: &str,
) -> Result<Object, Response> {
    let destination: CopyDestinationRequest = match serde_json::from_slice(&req.body) {
        Ok(v) => v,
        Err(_) if req.body.is_empty() => CopyDestinationRequest::default(),
        Err(_) => return Err(json_error(400, "invalid", "invalid json request")),
    };
    let Some((dest_bucket_escaped, dest_escaped)) = dest_suffix.split_once("/o/") else {
        return Err(json_error(404, "notFound", "not found"));
    };
    let source_key = url_decode(source_escaped);
    let dest_bucket = url_decode(dest_bucket_escaped);
    let dest_key = url_decode(dest_escaped);
    if source_key.is_empty() {
        return Err(json_error(400, "invalid", "invalid source object name"));
    }
    if dest_bucket.is_empty() {
        return Err(json_error(400, "invalid", "invalid destination bucket"));
    }
    if dest_key.is_empty() {
        return Err(json_error(
            400,
            "invalid",
            "invalid destination object name",
        ));
    }
    let Some((source, body)) = get_object_or_response(server, source_bucket, &source_key) else {
        return Err(last_error());
    };
    if !check_source_preconditions(req, &source) {
        return Err(last_error());
    }
    if !check_preconditions(server, req, &dest_bucket, &dest_key) {
        return Err(last_error());
    }

    let metadata = destination
        .metadata
        .unwrap_or_else(|| source.metadata.clone());
    match server.store.put_object(PutObjectInput {
        bucket: dest_bucket.clone(),
        key: dest_key.clone(),
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
        Ok(object) => {
            emit_dashboard_event(
                "gcs.object.put",
                serde_json::json!({
                    "bucket": dest_bucket,
                    "key": dest_key,
                    "etag": object.etag,
                    "contentLength": object.size,
                }),
            );
            Ok(object)
        }
        Err(StoreError::BucketNotExist) => Err(json_error(404, "notFound", "bucket not found")),
        Err(e) => Err(json_error(400, "invalid", &store_error_message(&e))),
    }
}

thread_local! {
    static LAST_ERROR: std::cell::RefCell<Response> = std::cell::RefCell::new(json_error(500, "backendError", "internal error"));
}

fn set_last_error(r: Response) {
    LAST_ERROR.with(|cell| *cell.borrow_mut() = r);
}

fn last_error() -> Response {
    LAST_ERROR.with(|cell| cell.borrow().clone())
}

fn get_object_or_response(server: &Server, bucket: &str, key: &str) -> Option<(Object, Vec<u8>)> {
    match server.store.get_object(bucket, key) {
        Ok(Some(found)) => Some(found),
        Ok(None) => {
            set_last_error(json_error(404, "notFound", "object not found"));
            None
        }
        Err(StoreError::BucketNotExist) => {
            set_last_error(json_error(404, "notFound", "bucket not found"));
            None
        }
        Err(e) => {
            set_last_error(json_error(400, "invalid", &store_error_message(&e)));
            None
        }
    }
}

fn handle_upload(server: &mut Server, req: &Request) -> Response {
    let Some((bucket, suffix)) = bucket_suffix(&req.path, "/upload/storage/v1/b/") else {
        return json_error(404, "notFound", "not found");
    };
    if suffix != "o" {
        return json_error(404, "notFound", "not found");
    }
    match req
        .query
        .get("uploadType")
        .map(String::as_str)
        .unwrap_or("")
    {
        "media" => handle_media_upload(server, req, &bucket),
        "multipart" => handle_json_multipart_upload(server, req, &bucket),
        "resumable" => handle_resumable_upload(server, req, &bucket),
        _ => json_error(400, "invalid", "unsupported uploadType"),
    }
}

fn handle_media_upload(server: &mut Server, req: &Request, bucket: &str) -> Response {
    if req.method != "POST" {
        return method_not_allowed("POST");
    }
    let name = req.query.get("name").cloned().unwrap_or_default();
    if name.is_empty() {
        return json_error(400, "required", "object name is required");
    }
    if !check_preconditions(server, req, bucket, &name) {
        return last_error();
    }
    put_object(
        server,
        req,
        bucket,
        &name,
        None,
        req.body.clone(),
        String::new(),
    )
}

fn handle_json_multipart_upload(server: &mut Server, req: &Request, bucket: &str) -> Response {
    if req.method != "POST" {
        return method_not_allowed("POST");
    }
    let (parsed, body, body_content_type) = if req
        .header("content-type")
        .to_ascii_lowercase()
        .starts_with("multipart/")
    {
        match parse_multipart_upload(req) {
            Ok(v) => v,
            Err(e) => return json_error(400, "invalid", &e),
        }
    } else {
        let parsed: ObjectMetadataRequest = match serde_json::from_slice(&req.body) {
            Ok(v) => v,
            Err(_) if req.body.is_empty() => ObjectMetadataRequest::default(),
            Err(_) => return json_error(400, "invalid", "invalid metadata json part"),
        };
        (parsed, Vec::new(), String::new())
    };
    let name = if parsed.name.is_empty() {
        req.query.get("name").cloned().unwrap_or_default()
    } else {
        parsed.name.clone()
    };
    if name.is_empty() {
        return json_error(400, "required", "object name is required");
    }
    if !check_preconditions(server, req, bucket, &name) {
        return last_error();
    }
    put_object(
        server,
        req,
        bucket,
        &name,
        Some(parsed),
        body,
        body_content_type,
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
    let name = first_non_empty(
        req.query.get("name").map(String::as_str).unwrap_or(""),
        &parsed.name,
    );
    if name.is_empty() {
        return json_error(400, "required", "object name is required");
    }
    if !check_preconditions(server, req, bucket, &name) {
        return last_error();
    }
    let id = format!("{:032x}", server.next_upload_id);
    server.next_upload_id += 1;
    let session = ResumableSession {
        bucket: bucket.to_string(),
        name: name.clone(),
        content_type: first_non_empty(req.header("x-upload-content-type"), &parsed.content_type),
        content_encoding: first_non_empty(req.header("content-encoding"), &parsed.content_encoding),
        cache_control: parsed.cache_control,
        content_disposition: parsed.content_disposition,
        metadata: merge_metadata(parsed.metadata, user_metadata(&req.headers)),
        if_generation_match: req
            .query
            .get("ifGenerationMatch")
            .cloned()
            .unwrap_or_default(),
        if_generation_not_match: req
            .query
            .get("ifGenerationNotMatch")
            .cloned()
            .unwrap_or_default(),
        if_metageneration_match: req
            .query
            .get("ifMetagenerationMatch")
            .cloned()
            .unwrap_or_default(),
        if_metageneration_not_match: req
            .query
            .get("ifMetagenerationNotMatch")
            .cloned()
            .unwrap_or_default(),
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
    let id = req.query.get("upload_id").cloned().unwrap_or_default();
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
    if !check_session_preconditions(server, &session) {
        return last_error();
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
        content_encoding: session.content_encoding,
        cache_control: session.cache_control,
        content_disposition: session.content_disposition,
        metadata: session.metadata,
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
        Err(StoreError::BucketNotExist) => json_error(404, "notFound", "bucket not found"),
        Err(e) => json_error(400, "invalid", &store_error_message(&e)),
    }
}

fn put_object(
    server: &mut Server,
    req: &Request,
    bucket: &str,
    name: &str,
    metadata: Option<ObjectMetadataRequest>,
    body: Vec<u8>,
    body_content_type: String,
) -> Response {
    let meta = metadata.unwrap_or_default();
    match server.store.put_object(PutObjectInput {
        bucket: bucket.to_string(),
        key: name.to_string(),
        body,
        content_type: first_non_empty(
            &meta.content_type,
            &first_non_empty(&body_content_type, req.header("content-type")),
        ),
        content_encoding: first_non_empty(&meta.content_encoding, req.header("content-encoding")),
        cache_control: first_non_empty(&meta.cache_control, req.header("cache-control")),
        content_disposition: first_non_empty(
            &meta.content_disposition,
            req.header("content-disposition"),
        ),
        metadata: merge_metadata(meta.metadata, user_metadata(&req.headers)),
        ..Default::default()
    }) {
        Ok(object) => {
            emit_dashboard_event(
                "gcs.object.put",
                serde_json::json!({
                    "bucket": bucket,
                    "key": name,
                    "etag": object.etag,
                    "contentLength": object.size,
                }),
            );
            Response::json(200, &object_resource(&object))
        }
        Err(StoreError::BucketNotExist) => json_error(404, "notFound", "bucket not found"),
        Err(e) => json_error(400, "invalid", &store_error_message(&e)),
    }
}

fn parse_multipart_upload(
    req: &Request,
) -> Result<(ObjectMetadataRequest, Vec<u8>, String), String> {
    let boundary =
        multipart_boundary(req.header("content-type")).ok_or("multipart boundary is required")?;
    let marker = format!("--{boundary}");
    let raw = String::from_utf8_lossy(&req.body);
    let mut parts = Vec::new();
    for section in raw.split(&marker).skip(1) {
        let trimmed = section.trim_start_matches("\r\n");
        if trimmed.starts_with("--") {
            break;
        }
        let trimmed = trimmed.trim_end_matches("\r\n");
        if trimmed.is_empty() {
            continue;
        }
        let Some((head, body)) = trimmed.split_once("\r\n\r\n") else {
            return Err("multipart part is malformed".to_string());
        };
        parts.push((head.to_string(), body.as_bytes().to_vec()));
    }
    if parts.len() < 2 {
        return Err("media part is required".to_string());
    }
    let metadata: ObjectMetadataRequest = serde_json::from_slice(&parts[0].1)
        .map_err(|_| "invalid metadata json part".to_string())?;
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

fn check_preconditions(server: &Server, req: &Request, bucket: &str, key: &str) -> bool {
    let found = match server.store.get_object(bucket, key) {
        Ok(Some((object, _))) => Some(object),
        Ok(None) => None,
        Err(StoreError::BucketNotExist) => None,
        Err(e) => {
            set_last_error(json_error(400, "invalid", &store_error_message(&e)));
            return false;
        }
    };
    let generation = found
        .as_ref()
        .map(generation)
        .unwrap_or_else(|| "0".to_string());
    let metageneration = found
        .as_ref()
        .map(|o| o.metageneration.max(1).to_string())
        .unwrap_or_else(|| "0".to_string());
    for (name, actual, invert) in [
        ("ifGenerationMatch", generation.as_str(), false),
        ("ifGenerationNotMatch", generation.as_str(), true),
        ("ifMetagenerationMatch", metageneration.as_str(), false),
        ("ifMetagenerationNotMatch", metageneration.as_str(), true),
    ] {
        let Some(expected) = req.query.get(name).filter(|v| !v.is_empty()) else {
            continue;
        };
        if expected.parse::<i64>().is_err() {
            set_last_error(json_error(400, "invalid", &format!("invalid {name}")));
            return false;
        }
        let matches = expected == actual;
        if (!invert && !matches) || (invert && matches) {
            set_last_error(json_error(412, "conditionNotMet", "precondition failed"));
            return false;
        }
    }
    true
}

fn check_source_preconditions(req: &Request, object: &Object) -> bool {
    let generation = generation(object);
    let metageneration = object.metageneration.max(1).to_string();
    for (name, actual, invert) in [
        ("ifSourceGenerationMatch", generation.as_str(), false),
        ("ifSourceGenerationNotMatch", generation.as_str(), true),
        (
            "ifSourceMetagenerationMatch",
            metageneration.as_str(),
            false,
        ),
        (
            "ifSourceMetagenerationNotMatch",
            metageneration.as_str(),
            true,
        ),
    ] {
        let Some(expected) = req.query.get(name).filter(|v| !v.is_empty()) else {
            continue;
        };
        if expected.parse::<i64>().is_err() {
            set_last_error(json_error(400, "invalid", &format!("invalid {name}")));
            return false;
        }
        let matches = expected == actual;
        if (!invert && !matches) || (invert && matches) {
            set_last_error(json_error(412, "conditionNotMet", "precondition failed"));
            return false;
        }
    }
    true
}

fn check_session_preconditions(server: &Server, session: &ResumableSession) -> bool {
    let found = match server.store.get_object(&session.bucket, &session.name) {
        Ok(Some((object, _))) => Some(object),
        Ok(None) | Err(StoreError::BucketNotExist) => None,
        Err(e) => {
            set_last_error(json_error(400, "invalid", &store_error_message(&e)));
            return false;
        }
    };
    let generation = found
        .as_ref()
        .map(generation)
        .unwrap_or_else(|| "0".to_string());
    let metageneration = found
        .as_ref()
        .map(|o| o.metageneration.max(1).to_string())
        .unwrap_or_else(|| "0".to_string());
    for (name, expected, actual, invert) in [
        (
            "ifGenerationMatch",
            session.if_generation_match.as_str(),
            generation.as_str(),
            false,
        ),
        (
            "ifGenerationNotMatch",
            session.if_generation_not_match.as_str(),
            generation.as_str(),
            true,
        ),
        (
            "ifMetagenerationMatch",
            session.if_metageneration_match.as_str(),
            metageneration.as_str(),
            false,
        ),
        (
            "ifMetagenerationNotMatch",
            session.if_metageneration_not_match.as_str(),
            metageneration.as_str(),
            true,
        ),
    ] {
        if expected.is_empty() {
            continue;
        }
        if expected.parse::<i64>().is_err() {
            set_last_error(json_error(400, "invalid", &format!("invalid {name}")));
            return false;
        }
        let matches = expected == actual;
        if (!invert && !matches) || (invert && matches) {
            set_last_error(json_error(412, "conditionNotMet", "precondition failed"));
            return false;
        }
    }
    true
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

    fn save_session(&self, id: &str, session: &ResumableSession) -> std::io::Result<()> {
        let Some(dir) = self.session_dir(id) else {
            return Ok(());
        };
        fs::create_dir_all(&dir)?;
        let mut data = serde_json::to_vec_pretty(session).unwrap_or_default();
        data.push(b'\n');
        fs::write(dir.join("session.json"), data)
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
    let Some((object, body)) = get_object_or_response(server, &bucket, &key) else {
        return last_error();
    };
    match generation_matches(req, &object) {
        Ok(true) => {}
        Ok(false) => return json_error(404, "notFound", "object not found"),
        Err(response) => return response,
    }
    if !check_preconditions(server, req, &bucket, &key) {
        return last_error();
    }
    media_response(req, &object, &body)
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
        r.headers
            .insert(format!("X-Goog-Meta-{key}"), value.clone());
    }
}

fn generation(object: &Object) -> String {
    parse_rfc3339(&object.last_modified)
        .map(|(secs, nanos)| secs.saturating_mul(1_000_000_000) + nanos as i64)
        .unwrap_or(0)
        .to_string()
}

fn generation_matches(req: &Request, object: &Object) -> Result<bool, Response> {
    match req.query.get("generation") {
        Some(v) if !v.is_empty() => {
            if v.parse::<i64>().is_err() {
                return Err(json_error(400, "invalid", "invalid generation"));
            }
            Ok(v == &generation(object))
        }
        _ => Ok(true),
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

fn merge_metadata(
    base: BTreeMap<String, String>,
    override_values: BTreeMap<String, String>,
) -> BTreeMap<String, String> {
    let mut out = base;
    for (k, v) in override_values {
        out.insert(k, v);
    }
    out
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
    let mut start = match query.get("pageToken").filter(|v| !v.trim().is_empty()) {
        Some(value) => value
            .trim()
            .parse::<usize>()
            .map_err(|_| "invalid pageToken".to_string())?,
        None => 0,
    };
    if start > total {
        start = total;
    }
    let mut limit = total - start;
    if let Some(value) = query.get("maxResults").filter(|v| !v.trim().is_empty()) {
        let parsed = value
            .trim()
            .parse::<usize>()
            .map_err(|_| "invalid maxResults".to_string())?;
        if parsed < limit {
            limit = parsed;
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

fn store_error_message(e: &StoreError) -> String {
    match e {
        StoreError::InvalidBucketName => "invalid bucket name".to_string(),
        StoreError::InvalidObjectKey => "invalid object key".to_string(),
        StoreError::BucketNotEmpty => "bucket is not empty".to_string(),
        StoreError::BucketNotExist => "bucket not found".to_string(),
        StoreError::ObjectLocked => "object is protected by Object Lock".to_string(),
        _ => "internal error".to_string(),
    }
}

fn emit_dashboard_event(event_type: &str, payload: serde_json::Value) {
    println!(
        "DEVCLOUD_EVENT {}",
        serde_json::json!({
            "type": event_type,
            "service": "gcs",
            "payload": payload,
        })
    );
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
