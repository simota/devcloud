//! REST HTTP server and request routing.
//!
//! Mirrors `internal/services/pubsub/{routes,...}.go`: a single handler routes by
//! path shape (`/v1/projects/<p>/<collection>/<id>[:action]`) and method to the
//! `Server` operations, applies bearer auth, and renders `RestResponse`/`ApiError`
//! with Go's `writeJSON` headers. A hand-rolled HTTP/1.1 reader/writer on plain
//! tokio keeps the dependency surface tiny (same pattern as the other crates).

use std::collections::BTreeMap;
use std::sync::Mutex;

use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use crate::errors::ApiError;
use crate::paths;
use crate::server::{RestResponse, Server};

const MAX_HEADER_BYTES: usize = 64 * 1024;
const MAX_BODY_BYTES: usize = 16 * 1024 * 1024;

/// A parsed HTTP request (the subset the router needs).
pub struct Request {
    pub method: String,
    pub path: String,
    pub query: BTreeMap<String, String>,
    pub headers: BTreeMap<String, String>,
    pub body: Vec<u8>,
}

impl Request {
    fn bearer_token(&self) -> String {
        let auth = self
            .headers
            .get("authorization")
            .map(String::as_str)
            .unwrap_or("")
            .trim();
        match auth.split_once(' ') {
            Some((scheme, token)) if scheme.eq_ignore_ascii_case("bearer") => {
                token.trim().to_string()
            }
            _ => String::new(),
        }
    }
}

fn is_iam_action(action: &str) -> bool {
    matches!(
        action,
        "getIamPolicy" | "setIamPolicy" | "testIamPermissions"
    )
}

/// Routes a parsed request to the matching `Server` operation. The path is the
/// raw (still percent-encoded) request path.
pub fn route(server: &mut Server, req: &Request) -> RestResponse {
    // Auth (applies to all paths, mirroring `authorize`).
    if !server.authorized(&req.bearer_token()) {
        let mut resp = render_error(&ApiError::new(
            401,
            "UNAUTHENTICATED",
            "invalid authentication credentials",
        ));
        resp.www_authenticate = true;
        return resp;
    }
    if !server.ready() {
        return render_error(&ApiError::internal("pubsub resource store unavailable"));
    }

    let path = req.path.as_str();
    let method = req.method.as_str();

    // Health.
    if path == "/healthz" || path == "/readyz" {
        if method != "GET" {
            return render_error(&ApiError::new(
                405,
                "METHOD_NOT_ALLOWED",
                "method not allowed",
            ));
        }
        return RestResponse {
            status: 200,
            body: crate::go_json::to_vec(&server.health_body()),
            allow: None,
            www_authenticate: false,
        };
    }

    let page_size = req
        .query
        .get("pageSize")
        .and_then(|v| v.parse::<i64>().ok())
        .unwrap_or(0);
    let page_token = req
        .query
        .get("pageToken")
        .and_then(|v| v.parse::<i64>().ok())
        .unwrap_or(0);
    let body_value = parse_body(&req.body);

    // Topic publish.
    if let Some((project, topic_id, action)) = paths::topic_action_parts(path) {
        if action == "publish" {
            if method != "POST" {
                return method_not_allowed("POST");
            }
            let messages = body_value
                .get("messages")
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default();
            // Decode error → invalid json (mirrors decode failure).
            if req.body_is_invalid_json() {
                return render_error(&ApiError::invalid_argument("invalid json request"));
            }
            return to_response(server.publish(&project, &topic_id, &messages));
        }
        if is_iam_action(&action) {
            if method != "POST" {
                return method_not_allowed("POST");
            }
            return to_response(server.topic_iam(&project, &topic_id, &action, &body_value));
        }
    }

    // Subscription actions.
    if let Some((project, sub_id, action)) = paths::subscription_action_parts(path) {
        if method != "POST" {
            return method_not_allowed("POST");
        }
        match action.as_str() {
            "pull" => {
                let max = body_value
                    .get("maxMessages")
                    .and_then(Value::as_i64)
                    .unwrap_or(0);
                return to_response(server.pull(&project, &sub_id, max));
            }
            "acknowledge" => {
                let ids = string_array(&body_value, "ackIds");
                return to_response(server.acknowledge(&project, &sub_id, &ids));
            }
            "modifyAckDeadline" => {
                let ids = string_array(&body_value, "ackIds");
                let deadline = body_value
                    .get("ackDeadlineSeconds")
                    .and_then(Value::as_i64)
                    .unwrap_or(0);
                return to_response(server.modify_ack_deadline(&project, &sub_id, &ids, deadline));
            }
            "modifyPushConfig" => {
                return to_response(server.modify_push_config(
                    &project,
                    &sub_id,
                    body_value.get("pushConfig"),
                ));
            }
            "detach" => return to_response(server.detach_subscription(&project, &sub_id)),
            "seek" => {
                if req.body_is_invalid_json() {
                    return render_error(&ApiError::invalid_argument("invalid json request"));
                }
                let snapshot = body_value
                    .get("snapshot")
                    .and_then(Value::as_str)
                    .unwrap_or("");
                let time = body_value.get("time").and_then(Value::as_str).unwrap_or("");
                return to_response(server.seek(&project, &sub_id, snapshot, time));
            }
            a if is_iam_action(a) => {
                return to_response(server.subscription_iam(&project, &sub_id, a, &body_value));
            }
            _ => return render_error(&ApiError::not_found("not found")),
        }
    }

    // Topic sub-collections.
    if let Some((project, topic_id)) = paths::topic_subscriptions_parts(path) {
        if method != "GET" {
            return method_not_allowed("GET");
        }
        return to_response(
            server.list_topic_subscriptions(&project, &topic_id, page_size, page_token),
        );
    }
    if let Some((project, topic_id)) = paths::topic_snapshots_parts(path) {
        if method != "GET" {
            return method_not_allowed("GET");
        }
        return to_response(
            server.list_topic_snapshots(&project, &topic_id, page_size, page_token),
        );
    }

    // Topics collection / leaf.
    if let Some(project) = paths::topics_collection(path) {
        if method != "GET" {
            return method_not_allowed("GET");
        }
        return to_response(server.list_topics(&project, page_size, page_token));
    }
    if let Some((project, topic_id)) = paths::topic_name_parts(path) {
        return match method {
            "PUT" => {
                if req.body_is_invalid_json() {
                    return render_error(&ApiError::invalid_argument("invalid json request"));
                }
                let topic = serde_json::from_value(body_value.clone()).unwrap_or_default();
                to_response(server.create_topic(&project, &topic_id, &topic))
            }
            "GET" => to_response(server.get_topic(&project, &topic_id)),
            "PATCH" => {
                let mask = req
                    .query
                    .get("updateMask")
                    .map(String::as_str)
                    .unwrap_or("");
                match crate::patch::decode_topic_patch(&req.body, mask) {
                    Some(p) => {
                        to_response(server.patch_topic(&project, &topic_id, &p.topic, &p.fields))
                    }
                    None => render_error(&ApiError::invalid_argument("invalid json request")),
                }
            }
            "DELETE" => to_response(server.delete_topic(&project, &topic_id)),
            _ => method_not_allowed("GET, PUT, PATCH, DELETE"),
        };
    }

    // Snapshots.
    if let Some(project) = paths::snapshots_collection(path) {
        if method != "GET" {
            return method_not_allowed("GET");
        }
        return to_response(server.list_snapshots(&project, page_size, page_token));
    }
    if let Some((project, snapshot_id)) = paths::snapshot_name_parts(path) {
        return match method {
            "PUT" => {
                if req.body_is_invalid_json() {
                    return render_error(&ApiError::invalid_argument("invalid json request"));
                }
                let sub = body_value
                    .get("subscription")
                    .and_then(Value::as_str)
                    .unwrap_or("");
                to_response(server.create_snapshot(&project, &snapshot_id, sub))
            }
            "GET" => to_response(server.get_snapshot(&project, &snapshot_id)),
            "DELETE" => to_response(server.delete_snapshot(&project, &snapshot_id)),
            _ => method_not_allowed("GET, PUT, DELETE"),
        };
    }

    // Schemas:validateMessage.
    if let Some(project) = paths::schemas_validate_message(path) {
        if method != "POST" {
            return method_not_allowed("POST");
        }
        if req.body_is_invalid_json() {
            return render_error(&ApiError::invalid_argument("invalid json request"));
        }
        return to_response(server.validate_message(&project, &body_value));
    }
    // Schemas collection (GET list / POST create).
    if let Some(project) = paths::schemas_collection(path) {
        if method == "POST" {
            if req.body_is_invalid_json() {
                return render_error(&ApiError::invalid_argument("invalid json request"));
            }
            let schema_id = req.query.get("schemaId").map(|s| s.trim()).unwrap_or("");
            let schema = serde_json::from_value(body_value.clone()).unwrap_or_default();
            return to_response(server.create_schema(&project, schema_id, &schema));
        }
        if method != "GET" {
            return method_not_allowed("GET, POST");
        }
        let view = req.query.get("view").map(String::as_str).unwrap_or("");
        if !matches!(view, "" | "FULL" | "BASIC") {
            return render_error(&ApiError::invalid_argument("invalid schema view"));
        }
        return to_response(server.list_schemas(&project, view, page_size, page_token));
    }
    if let Some((project, schema_id)) = paths::schema_name_parts(path) {
        return match method {
            "PUT" => {
                if req.body_is_invalid_json() {
                    return render_error(&ApiError::invalid_argument("invalid json request"));
                }
                let schema = serde_json::from_value(body_value.clone()).unwrap_or_default();
                to_response(server.create_schema(&project, &schema_id, &schema))
            }
            "GET" => {
                let view = req.query.get("view").map(String::as_str).unwrap_or("");
                if !matches!(view, "" | "FULL" | "BASIC") {
                    return render_error(&ApiError::invalid_argument("invalid schema view"));
                }
                to_response(server.get_schema(&project, &schema_id, view))
            }
            "DELETE" => to_response(server.delete_schema(&project, &schema_id)),
            _ => method_not_allowed("GET, PUT, DELETE"),
        };
    }

    // Subscriptions collection / leaf.
    if let Some(project) = paths::subscriptions_collection(path) {
        if method != "GET" {
            return method_not_allowed("GET");
        }
        return to_response(server.list_subscriptions(&project, page_size, page_token));
    }
    if let Some((project, sub_id)) = paths::subscription_name_parts(path) {
        return match method {
            "PUT" => {
                if req.body_is_invalid_json() {
                    return render_error(&ApiError::invalid_argument("invalid json request"));
                }
                let sub = serde_json::from_value(body_value.clone()).unwrap_or_default();
                to_response(server.create_subscription(&project, &sub_id, &sub))
            }
            "GET" => to_response(server.get_subscription(&project, &sub_id)),
            "PATCH" => {
                let mask = req
                    .query
                    .get("updateMask")
                    .map(String::as_str)
                    .unwrap_or("");
                match crate::patch::decode_subscription_patch(&req.body, mask) {
                    Some(p) => to_response(server.patch_subscription(
                        &project,
                        &sub_id,
                        &p.subscription,
                        &p.fields,
                    )),
                    None => render_error(&ApiError::invalid_argument("invalid json request")),
                }
            }
            "DELETE" => to_response(server.delete_subscription(&project, &sub_id)),
            _ => method_not_allowed("GET, PUT, PATCH, DELETE"),
        };
    }

    render_error(&ApiError::not_found("not found"))
}

impl Request {
    /// True when a non-empty body fails to parse as JSON (mirrors a decode
    /// failure). An empty body is treated as `{}` and is never invalid.
    fn body_is_invalid_json(&self) -> bool {
        !self.body.is_empty() && serde_json::from_slice::<Value>(&self.body).is_err()
    }
}

fn parse_body(body: &[u8]) -> Value {
    if body.is_empty() {
        return Value::Object(serde_json::Map::new());
    }
    serde_json::from_slice(body).unwrap_or(Value::Object(serde_json::Map::new()))
}

fn string_array(body: &Value, key: &str) -> Vec<String> {
    body.get(key)
        .and_then(Value::as_array)
        .map(|a| {
            a.iter()
                .filter_map(|v| v.as_str().map(str::to_string))
                .collect()
        })
        .unwrap_or_default()
}

fn method_not_allowed(allow: &str) -> RestResponse {
    let mut resp = render_error(&ApiError::new(
        405,
        "METHOD_NOT_ALLOWED",
        "method not allowed",
    ));
    resp.allow = Some(allow.to_string());
    resp
}

fn to_response(result: Result<RestResponse, ApiError>) -> RestResponse {
    match result {
        Ok(resp) => resp,
        Err(err) => render_error(&err),
    }
}

fn render_error(err: &ApiError) -> RestResponse {
    RestResponse {
        status: err.status,
        body: err.body_bytes(),
        allow: None,
        www_authenticate: false,
    }
}

// --- socket server ---------------------------------------------------------

/// Runs the accept loop until `shutdown` resolves.
pub async fn serve(
    listener: TcpListener,
    server: std::sync::Arc<Mutex<Server>>,
    shutdown: impl std::future::Future<Output = ()>,
) -> std::io::Result<()> {
    tokio::pin!(shutdown);
    loop {
        tokio::select! {
            _ = &mut shutdown => return Ok(()),
            accepted = listener.accept() => {
                let (stream, _) = accepted?;
                let server = std::sync::Arc::clone(&server);
                tokio::spawn(async move {
                    let _ = handle_conn(stream, server).await;
                });
            }
        }
    }
}

async fn handle_conn(
    mut stream: TcpStream,
    server: std::sync::Arc<Mutex<Server>>,
) -> std::io::Result<()> {
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
    let content_length: usize = headers
        .get("content-length")
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
    if content_length > MAX_BODY_BYTES {
        return Ok(None);
    }
    let mut body = buf[header_end + 4..].to_vec();
    while body.len() < content_length {
        let n = stream.read(&mut tmp).await?;
        if n == 0 {
            break;
        }
        body.extend_from_slice(&tmp[..n]);
    }
    body.truncate(content_length);

    Ok(Some(Request {
        method,
        path,
        query,
        headers,
        body,
    }))
}

/// Splits a request target into the (still percent-encoded) path and a decoded
/// query map.
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

async fn write_response(stream: &mut TcpStream, resp: RestResponse) -> std::io::Result<()> {
    let mut head = format!(
        "HTTP/1.1 {} {}\r\n",
        resp.status,
        reason_phrase(resp.status)
    );
    head.push_str("Server: devcloud-pubsub\r\n");
    if resp.status != 204 {
        head.push_str("Content-Type: application/json; charset=utf-8\r\n");
    }
    if let Some(allow) = &resp.allow {
        head.push_str(&format!("Allow: {allow}\r\n"));
    }
    if resp.www_authenticate {
        head.push_str("WWW-Authenticate: Bearer realm=\"devcloud-pubsub\"\r\n");
    }
    head.push_str(&format!("Content-Length: {}\r\n", resp.body.len()));
    head.push_str("Connection: close\r\n\r\n");
    stream.write_all(head.as_bytes()).await?;
    stream.write_all(&resp.body).await?;
    stream.flush().await
}

fn reason_phrase(status: u16) -> &'static str {
    match status {
        200 => "OK",
        204 => "No Content",
        400 => "Bad Request",
        401 => "Unauthorized",
        404 => "Not Found",
        405 => "Method Not Allowed",
        409 => "Conflict",
        500 => "Internal Server Error",
        _ => "Status",
    }
}

fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    if needle.is_empty() || haystack.len() < needle.len() {
        return None;
    }
    haystack.windows(needle.len()).position(|w| w == needle)
}
