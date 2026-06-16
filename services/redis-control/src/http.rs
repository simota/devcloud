//! HTTP control/introspection surface — port of
//! `internal/services/redis/http.rs`.
//!
//! Routes (intercepted before anything else, exactly as legacy ServeHTTP):
//!   GET    /_introspect/status                          -> Server.status
//!   GET    /_introspect/keys?cursor=&match=&count=      -> Server.keys
//!   GET    /_introspect/keys/{key}                      -> Server.key_detail
//!   POST   /_control/select-db        {db}              -> Server.set_current_db
//!   DELETE /_control/keys?confirm=FLUSHDB              -> Server.flush_db
//!   DELETE /_control/keys/{key}                         -> Server.delete_key
//!   POST   /_control/keys/{key}/expire {ttlSeconds}     -> Server.expire_key
//!   POST   /_control/exec             {command,args}    -> Server.exec
//!
//! Conventions reproduced from legacy:
//!   - `/_introspect/` is GET-only; non-GET → 405.
//!   - `/_control/` is POST/DELETE; GET on a control path → 405.
//!   - Unknown path / missing resource → 404; unsupported method → 405.
//!   - `verifyHTTP` runs before dispatch: no-op in relaxed mode, HTTP Basic
//!     password check (constant-time) in strict mode.
//!   - JSON response bodies match http.rs byte-for-byte in shape (the dashboard
//!     forwarding + redis page depend on them).
//!   - `audit_mutation` logs command + key ONLY — never values, args beyond the
//!     key, the password, or the Authorization header.
//!
//! A hand-rolled HTTP/1.1 reader/writer on plain tokio (same pattern as the sqs
//! and applicationautoscaling crates) keeps the dependency surface at
//! tokio/serde/serde_json.

use std::collections::HashMap;
use std::sync::Arc;

use serde::Deserialize;
use serde_json::json;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use crate::command_allowlist::{command_allowed, CommandClass};
use crate::server::{Server, Status, ERR_COMMAND_NOT_ALLOWED};

const INTROSPECT_PREFIX: &str = "/_introspect/";
const CONTROL_PREFIX: &str = "/_control/";
const MAX_HEADER_BYTES: usize = 64 * 1024;
const MAX_BODY_BYTES: usize = 1024 * 1024;

/// Auth configuration for the listener, mirroring legacy `HTTPConfig`
/// (`AuthMode`/`Password`); the listen addr lives on [`crate::Config`].
#[derive(Debug, Clone)]
pub struct HttpAuth {
    pub auth_mode: String,
    pub password: String,
}

impl HttpAuth {
    fn mode(&self) -> String {
        let mode = self.auth_mode.trim().to_ascii_lowercase();
        if mode.is_empty() {
            "relaxed".to_string()
        } else {
            mode
        }
    }

    fn is_strict(&self) -> bool {
        self.mode() == "strict"
    }
}

struct Request {
    method: String,
    /// The unescaped path component.
    path: String,
    /// The raw (still percent-encoded) path, used for key extraction so encoded
    /// separators survive, mirroring legacy `r.URL.EscapedPath()`.
    raw_path: String,
    query: HashMap<String, String>,
    headers: HashMap<String, String>,
    body: Vec<u8>,
}

impl Request {
    fn header(&self, name: &str) -> &str {
        self.headers.get(name).map(String::as_str).unwrap_or("")
    }
    fn query_param(&self, name: &str) -> &str {
        self.query.get(name).map(String::as_str).unwrap_or("")
    }
}

/// A rendered HTTP response.
#[derive(Debug)]
struct Response {
    status: u16,
    /// Content-Type; defaults to application/json for `json_body`.
    content_type: String,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}

impl Response {
    fn json(status: u16, value: serde_json::Value) -> Response {
        Response {
            status,
            content_type: "application/json".to_string(),
            headers: Vec::new(),
            body: serde_json::to_vec(&value).unwrap_or_default(),
        }
    }

    /// Mirrors legacy `http.Error`: plain text + trailing newline.
    fn text(status: u16, message: &str) -> Response {
        Response {
            status,
            content_type: "text/plain; charset=utf-8".to_string(),
            headers: vec![("X-Content-Type-Options".to_string(), "nosniff".to_string())],
            body: format!("{message}\n").into_bytes(),
        }
    }

    fn not_found() -> Response {
        Response::text(404, "404 page not found")
    }

    fn method_not_allowed(allow: &str) -> Response {
        let mut resp = Response::text(405, "method not allowed");
        resp.headers.push(("Allow".to_string(), allow.to_string()));
        resp
    }
}

/// Runs the accept loop until `shutdown` resolves.
pub async fn serve(
    listener: TcpListener,
    server: Arc<Server>,
    auth: HttpAuth,
    shutdown: impl std::future::Future<Output = ()>,
) -> std::io::Result<()> {
    tokio::pin!(shutdown);
    loop {
        tokio::select! {
            _ = &mut shutdown => return Ok(()),
            accepted = listener.accept() => {
                let (stream, _) = accepted?;
                let server = Arc::clone(&server);
                let auth = auth.clone();
                tokio::spawn(async move {
                    let _ = handle_conn(stream, server, auth).await;
                });
            }
        }
    }
}

async fn handle_conn(
    mut stream: TcpStream,
    server: Arc<Server>,
    auth: HttpAuth,
) -> std::io::Result<()> {
    let request = match read_request(&mut stream).await {
        Ok(Some(req)) => req,
        _ => return Ok(()),
    };
    let response = dispatch(&server, &auth, &request).await;
    write_response(&mut stream, response).await
}

/// Top-level routing, mirroring `ServeHTTP`: intercept the two prefixes before
/// anything else, run `verify_http` first, then 404 for everything else.
async fn dispatch(server: &Server, auth: &HttpAuth, req: &Request) -> Response {
    let path = if req.path.is_empty() { "/" } else { &req.path };
    if path.starts_with(INTROSPECT_PREFIX) {
        if let Some(resp) = verify_http(auth, req) {
            return resp;
        }
        handle_introspect(server, req).await
    } else if path.starts_with(CONTROL_PREFIX) {
        if let Some(resp) = verify_http(auth, req) {
            return resp;
        }
        handle_control(server, req).await
    } else {
        Response::not_found()
    }
}

/// Mirrors `handleIntrospect` (GET-only).
async fn handle_introspect(server: &Server, req: &Request) -> Response {
    if req.method != "GET" {
        return Response::method_not_allowed("GET");
    }
    let rest = req.path.trim_start_matches(INTROSPECT_PREFIX);
    if rest == "status" {
        match server.status().await {
            Ok(status) => Response::json(200, redis_status_response(&status)),
            Err(_) => Response::json(502, json!({"error": "redis status unavailable"})),
        }
    } else if rest == "keys" {
        let cursor = match parse_cursor(req.query_param("cursor")) {
            Ok(c) => c,
            Err(resp) => return resp,
        };
        let match_pattern = req.query_param("match");
        let count = parse_count(req.query_param("count"));
        match server.keys(cursor, match_pattern, count).await {
            Ok(snapshot) => Response::json(200, keys_response(&snapshot)),
            Err(_) => Response::json(502, json!({"error": "redis keys unavailable"})),
        }
    } else if rest.starts_with("keys/") {
        let Some(key) = key_from_introspect_path(&req.raw_path) else {
            return Response::not_found();
        };
        match server.key_detail(&key).await {
            Ok(detail) => Response::json(200, key_detail_response(&detail)),
            Err(_) => Response::json(502, json!({"error": "redis key unavailable"})),
        }
    } else {
        Response::not_found()
    }
}

/// Mirrors `handleControl`.
async fn handle_control(server: &Server, req: &Request) -> Response {
    let rest = req.path.trim_start_matches(CONTROL_PREFIX);
    if rest == "select-db" {
        handle_select_db(server, req).await
    } else if rest == "exec" {
        handle_exec(server, req).await
    } else if rest == "keys" {
        handle_flush_db(server, req).await
    } else if rest.starts_with("keys/") {
        handle_key_mutation(server, req).await
    } else {
        Response::not_found()
    }
}

#[derive(Deserialize)]
struct SelectDbRequest {
    #[serde(default)]
    db: i64,
}

async fn handle_select_db(server: &Server, req: &Request) -> Response {
    if req.method != "POST" {
        return Response::method_not_allowed("POST");
    }
    let request: SelectDbRequest = match serde_json::from_slice(&req.body) {
        Ok(r) => r,
        Err(_) => return Response::text(400, "invalid redis select-db request"),
    };
    if let Err(message) = server.set_current_db(request.db).await {
        return Response::json(400, json!({"error": message}));
    }
    audit_mutation("SELECT", &format!("db{}", request.db));
    Response::json(200, json!({"currentDB": request.db}))
}

async fn handle_flush_db(server: &Server, req: &Request) -> Response {
    if req.method != "DELETE" {
        return Response::method_not_allowed("DELETE");
    }
    if req.query_param("confirm") != "FLUSHDB" {
        return Response::text(400, "confirm=FLUSHDB is required");
    }
    match server.flush_db().await {
        Ok(result) => {
            audit_mutation("FLUSHDB", "");
            Response::json(200, json!({"result": result}))
        }
        Err(_) => Response::json(502, json!({"error": "redis flush failed"})),
    }
}

async fn handle_key_mutation(server: &Server, req: &Request) -> Response {
    let Some((key, action)) = key_and_action_from_control_path(&req.raw_path) else {
        return Response::not_found();
    };
    match (action.as_str(), req.method.as_str()) {
        ("", "DELETE") => match server.delete_key(&key).await {
            Ok(deleted) => {
                audit_mutation("DEL", &key);
                Response::json(200, json!({"deleted": deleted}))
            }
            Err(_) => Response::json(502, json!({"error": "redis delete failed"})),
        },
        ("expire", "POST") => {
            let ttl = match parse_ttl(req) {
                Ok(ttl) => ttl,
                Err(resp) => return resp,
            };
            match server.expire_key(&key, ttl).await {
                Ok(updated) => {
                    audit_mutation("EXPIRE", &key);
                    Response::json(200, json!({"updated": updated}))
                }
                Err(_) => Response::json(502, json!({"error": "redis expire failed"})),
            }
        }
        _ => Response::method_not_allowed("DELETE, POST"),
    }
}

#[derive(Deserialize)]
struct CommandRequest {
    #[serde(default)]
    command: String,
    #[serde(default)]
    args: Vec<String>,
}

/// Mirrors `handleExec`: allowlist gate (403), strict-mode mutation gate (via
/// `verify_http` already having run), audit, then run + format the reply.
async fn handle_exec(server: &Server, req: &Request) -> Response {
    if req.method != "POST" {
        return Response::method_not_allowed("POST");
    }
    let request: CommandRequest = match serde_json::from_slice(&req.body) {
        Ok(r) => r,
        Err(_) => return Response::text(400, "invalid redis command request"),
    };
    let command = request.command.trim().to_ascii_uppercase();
    let class = match command_allowed(&command) {
        Some(class) => class,
        None => return Response::json(403, json!({"error": "redis command is not allowlisted"})),
    };
    if class == CommandClass::Mutation {
        let key = request.args.first().map(String::as_str).unwrap_or("");
        audit_mutation(&command, key);
    }
    match server.exec(&command, &request.args).await {
        Ok(result) => Response::json(
            200,
            json!({
                "command": result.command,
                "class": result.class.as_str(),
                "rows": result.rows,
            }),
        ),
        Err(message) if message == ERR_COMMAND_NOT_ALLOWED => {
            Response::json(403, json!({"error": "redis command is not allowlisted"}))
        }
        Err(_) => Response::json(502, json!({"error": "redis command failed"})),
    }
}

/// Mirrors `auditMutation`: ONE structured line, command + key only. Never
/// values, args beyond the key, or credentials. Uses `eprintln!` so it lands on
/// stderr like the legacy `log` package default.
fn audit_mutation(command: &str, key: &str) {
    if key.is_empty() {
        eprintln!("redis /_control mutation: command={command}");
    } else {
        eprintln!("redis /_control mutation: command={command} key={key}");
    }
}

/// Mirrors `verifyHTTP`: open in relaxed mode; in strict mode require HTTP Basic
/// credentials whose password equals the configured one (constant-time compare).
/// Returns `Some(401)` when the request must be rejected, `None` to proceed.
fn verify_http(auth: &HttpAuth, req: &Request) -> Option<Response> {
    if !auth.is_strict() {
        return None;
    }
    if let Some((_, pass)) = basic_auth(req.header("authorization")) {
        if constant_time_eq(pass.as_bytes(), auth.password.as_bytes()) {
            return None;
        }
    }
    let mut resp = Response::text(401, "unauthorized");
    resp.headers.push((
        "WWW-Authenticate".to_string(),
        r#"Basic realm="devcloud-redis""#.to_string(),
    ));
    Some(resp)
}

/// Mirrors legacy `basicAuth`: parse `Authorization: Basic <base64(user:pass)>`.
/// Never logs the header or its contents.
fn basic_auth(header: &str) -> Option<(String, String)> {
    const PREFIX: &str = "Basic ";
    if header.len() < PREFIX.len() || !header[..PREFIX.len()].eq_ignore_ascii_case(PREFIX) {
        return None;
    }
    let decoded = base64_decode(&header[PREFIX.len()..])?;
    let decoded = String::from_utf8(decoded).ok()?;
    let (user, pass) = decoded.split_once(':')?;
    Some((user.to_string(), pass.to_string()))
}

/// Constant-time byte comparison (mirrors legacy `subtle.ConstantTimeCompare`).
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

// --- request parsing helpers (mirror http.rs) ---

/// Mirrors `cursorFromRequest`: empty → 0; non-numeric → 400.
fn parse_cursor(raw: &str) -> Result<u64, Response> {
    if raw.is_empty() {
        return Ok(0);
    }
    raw.parse::<u64>()
        .map_err(|_| Response::text(400, "cursor must be a non-negative integer"))
}

/// Mirrors `countFromRequest`: empty/invalid/≤0 → 100; clamp to 1000.
fn parse_count(raw: &str) -> i64 {
    if raw.is_empty() {
        return 100;
    }
    match raw.parse::<i64>() {
        Ok(count) if count > 0 => count.min(1000),
        _ => 100,
    }
}

#[derive(Deserialize)]
struct TtlBody {
    #[serde(rename = "ttlSeconds", default)]
    ttl_seconds: i64,
}

/// Mirrors `ttlFromRequest`: prefer the `ttlSeconds` query param, else the JSON
/// body; must parse to a positive integer.
fn parse_ttl(req: &Request) -> Result<i64, Response> {
    let raw = req.query_param("ttlSeconds");
    let ttl = if raw.is_empty() {
        match serde_json::from_slice::<TtlBody>(&req.body) {
            Ok(body) => body.ttl_seconds,
            Err(_) => return Err(Response::text(400, "ttlSeconds is required")),
        }
    } else {
        match raw.parse::<i64>() {
            Ok(n) => n,
            Err(_) => return Err(Response::text(400, "ttlSeconds must be a positive integer")),
        }
    };
    if ttl <= 0 {
        return Err(Response::text(400, "ttlSeconds must be a positive integer"));
    }
    Ok(ttl)
}

/// Mirrors `keyFromIntrospectPath`: extract + percent-decode the key from
/// `/_introspect/keys/{key}`.
fn key_from_introspect_path(escaped_path: &str) -> Option<String> {
    let prefix = format!("{INTROSPECT_PREFIX}keys/");
    let suffix = escaped_path.strip_prefix(&prefix)?;
    if suffix.is_empty() {
        return None;
    }
    let key = percent_decode(suffix.trim_end_matches('/'))?;
    if key.is_empty() {
        None
    } else {
        Some(key)
    }
}

/// Mirrors `keyAndActionFromControlPath`: extract key + optional `expire` action
/// from `/_control/keys/{key}` or `/_control/keys/{key}/expire`.
fn key_and_action_from_control_path(escaped_path: &str) -> Option<(String, String)> {
    let prefix = format!("{CONTROL_PREFIX}keys/");
    let suffix = escaped_path.strip_prefix(&prefix)?;
    if suffix.is_empty() {
        return None;
    }
    let (key_part, action) = if let Some(stripped) = suffix.strip_suffix("/expire") {
        (stripped, "expire".to_string())
    } else {
        (suffix.trim_end_matches('/'), String::new())
    };
    let decoded = percent_decode(key_part)?;
    if decoded.is_empty() {
        return None;
    }
    Some((decoded, action))
}

// --- response shapes (match http.rs exactly) ---

/// Mirrors `redisStatusResponse`.
fn redis_status_response(status: &Status) -> serde_json::Value {
    json!({
        "service": "redis",
        "status": "running",
        "running": true,
        "mode": status.mode,
        "address": status.address,
        "serverVersion": status.server_version,
        "connectedClients": status.connected_clients,
        "usedMemoryHuman": status.used_memory_human,
        "currentDB": status.current_db,
        "databaseCount": status.database_count,
        "currentDBKeys": status.current_db_keys,
    })
}

/// Mirrors `keysResponseFromSnapshot` (the `keys` array is always present, never
/// null, matching legacy `make([]..., 0, ...)`).
fn keys_response(snapshot: &crate::server::KeysSnapshot) -> serde_json::Value {
    let keys: Vec<serde_json::Value> = snapshot
        .keys
        .iter()
        .map(|k| {
            json!({
                "key": k.key,
                "type": k.key_type,
                "ttlSeconds": k.ttl_seconds,
            })
        })
        .collect();
    json!({
        "cursor": snapshot.cursor,
        "nextCursor": snapshot.next_cursor,
        "keys": keys,
    })
}

/// Mirrors `keyDetailResponseFromSnapshot` (the `preview` array is always
/// present, never null).
fn key_detail_response(detail: &crate::server::KeyDetail) -> serde_json::Value {
    json!({
        "key": detail.key,
        "type": detail.key_type,
        "ttlSeconds": detail.ttl_seconds,
        "preview": detail.preview,
    })
}

// --- HTTP/1.1 transport (hand-rolled, same pattern as sqs/http.rs) ---

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
    let mut parts = request_line.split(' ');
    let method = parts.next().unwrap_or("").to_string();
    let target = parts.next().unwrap_or("");
    let (raw_path, raw_query) = match target.split_once('?') {
        Some((p, q)) => (p.to_string(), q.to_string()),
        None => (target.to_string(), String::new()),
    };
    let path = percent_decode(&raw_path).unwrap_or_else(|| raw_path.clone());
    let query = parse_query(&raw_query);

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
        raw_path,
        query,
        headers,
        body,
    }))
}

async fn write_response(stream: &mut TcpStream, resp: Response) -> std::io::Result<()> {
    let mut head = format!(
        "HTTP/1.1 {} {}\r\n",
        resp.status,
        reason_phrase(resp.status)
    );
    head.push_str("Server: devcloud-redis\r\n");
    head.push_str(&format!("Content-Type: {}\r\n", resp.content_type));
    for (name, value) in &resp.headers {
        head.push_str(&format!("{name}: {value}\r\n"));
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
        400 => "Bad Request",
        401 => "Unauthorized",
        403 => "Forbidden",
        404 => "Not Found",
        405 => "Method Not Allowed",
        500 => "Internal Server Error",
        502 => "Bad Gateway",
        _ => "Status",
    }
}

fn parse_query(raw: &str) -> HashMap<String, String> {
    let mut map = HashMap::new();
    for pair in raw.split('&') {
        if pair.is_empty() {
            continue;
        }
        let (k, v) = match pair.split_once('=') {
            Some((k, v)) => (k, v),
            None => (pair, ""),
        };
        let key = percent_decode(k).unwrap_or_else(|| k.to_string());
        let value = percent_decode(v).unwrap_or_else(|| v.to_string());
        map.entry(key).or_insert(value);
    }
    map
}

fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    if needle.is_empty() || haystack.len() < needle.len() {
        return None;
    }
    haystack.windows(needle.len()).position(|w| w == needle)
}

/// Percent-decodes a path/query segment, treating `+` literally (path semantics,
/// matching legacy `url.PathUnescape`). Returns `None` on a malformed `%` escape.
fn percent_decode(input: &str) -> Option<String> {
    let bytes = input.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'%' => {
                if i + 2 >= bytes.len() {
                    return None;
                }
                let hi = hex_val(bytes[i + 1])?;
                let lo = hex_val(bytes[i + 2])?;
                out.push(hi << 4 | lo);
                i += 3;
            }
            other => {
                out.push(other);
                i += 1;
            }
        }
    }
    String::from_utf8(out).ok()
}

fn hex_val(c: u8) -> Option<u8> {
    match c {
        b'0'..=b'9' => Some(c - b'0'),
        b'a'..=b'f' => Some(c - b'a' + 10),
        b'A'..=b'F' => Some(c - b'A' + 10),
        _ => None,
    }
}

/// Standard base64 decode (for the Basic-auth credential). Hand-rolled to keep
/// the dependency surface at tokio/serde/serde_json.
fn base64_decode(input: &str) -> Option<Vec<u8>> {
    fn val(c: u8) -> Option<u8> {
        match c {
            b'A'..=b'Z' => Some(c - b'A'),
            b'a'..=b'z' => Some(c - b'a' + 26),
            b'0'..=b'9' => Some(c - b'0' + 52),
            b'+' => Some(62),
            b'/' => Some(63),
            _ => None,
        }
    }
    let cleaned: Vec<u8> = input.bytes().filter(|&b| b != b'=').collect();
    let mut out = Vec::with_capacity(cleaned.len() * 3 / 4);
    let mut buffer = 0u32;
    let mut bits = 0u32;
    for &c in &cleaned {
        let v = val(c)? as u32;
        buffer = buffer << 6 | v;
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            out.push((buffer >> bits) as u8);
        }
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn count_clamps_and_defaults() {
        assert_eq!(parse_count(""), 100);
        assert_eq!(parse_count("0"), 100);
        assert_eq!(parse_count("-5"), 100);
        assert_eq!(parse_count("abc"), 100);
        assert_eq!(parse_count("50"), 50);
        assert_eq!(parse_count("5000"), 1000);
    }

    #[test]
    fn cursor_parses_or_errors() {
        assert_eq!(parse_cursor("").unwrap(), 0);
        assert_eq!(parse_cursor("42").unwrap(), 42);
        assert_eq!(parse_cursor("x").unwrap_err().status, 400);
    }

    #[test]
    fn introspect_key_extraction_unescapes() {
        assert_eq!(
            key_from_introspect_path("/_introspect/keys/foo:bar").as_deref(),
            Some("foo:bar")
        );
        assert_eq!(
            key_from_introspect_path("/_introspect/keys/a%2Fb").as_deref(),
            Some("a/b")
        );
        assert_eq!(key_from_introspect_path("/_introspect/keys/"), None);
    }

    #[test]
    fn control_key_and_action_extraction() {
        assert_eq!(
            key_and_action_from_control_path("/_control/keys/foo"),
            Some(("foo".to_string(), String::new()))
        );
        assert_eq!(
            key_and_action_from_control_path("/_control/keys/foo/expire"),
            Some(("foo".to_string(), "expire".to_string()))
        );
        assert_eq!(key_and_action_from_control_path("/_control/keys/"), None);
    }

    #[test]
    fn base64_decode_round_trips_basic_auth() {
        // "user:secret" base64
        let decoded = base64_decode("dXNlcjpzZWNyZXQ=").unwrap();
        assert_eq!(String::from_utf8(decoded).unwrap(), "user:secret");
    }

    #[test]
    fn basic_auth_parses_password() {
        let parsed = basic_auth("Basic dXNlcjpzZWNyZXQ=").unwrap();
        assert_eq!(parsed.0, "user");
        assert_eq!(parsed.1, "secret");
        assert!(basic_auth("Bearer abc").is_none());
    }

    #[test]
    fn constant_time_eq_matches_subtle_semantics() {
        assert!(constant_time_eq(b"secret", b"secret"));
        assert!(!constant_time_eq(b"secret", b"secre"));
        assert!(!constant_time_eq(b"secret", b"wrong!"));
    }

    #[test]
    fn ttl_parse_prefers_query_then_body() {
        let mut req = test_request("POST", "/_control/keys/k/expire");
        req.query
            .insert("ttlSeconds".to_string(), "120".to_string());
        assert_eq!(parse_ttl(&req).unwrap(), 120);

        let mut body_req = test_request("POST", "/_control/keys/k/expire");
        body_req.body = br#"{"ttlSeconds":90}"#.to_vec();
        assert_eq!(parse_ttl(&body_req).unwrap(), 90);

        let mut zero_req = test_request("POST", "/_control/keys/k/expire");
        zero_req
            .query
            .insert("ttlSeconds".to_string(), "0".to_string());
        assert_eq!(parse_ttl(&zero_req).unwrap_err().status, 400);
    }

    fn test_request(method: &str, path: &str) -> Request {
        Request {
            method: method.to_string(),
            path: path.to_string(),
            raw_path: path.to_string(),
            query: HashMap::new(),
            headers: HashMap::new(),
            body: Vec::new(),
        }
    }
}
