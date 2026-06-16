//! Dashboard-facing HTTP introspect/control surface — port of
//! `internal/services/mail/http.rs`.
//!
//! Routes (intercepted before anything else, exactly as legacy `ServeHTTP`):
//!   GET    /_introspect/messages?limit=N      -> Service.list
//!   GET    /_introspect/messages/{id}         -> Service.get   (404 if absent)
//!   GET    /_introspect/messages/{id}/raw     -> Service.get_raw (message/rfc822)
//!   DELETE /_control/messages                 -> Service.delete_all (mail.cleared)
//!   DELETE /_control/messages/{id}            -> Service.delete     (mail.deleted)
//!
//! Conventions reproduced from legacy:
//!   - `/_introspect/` is GET-only; non-GET → 405 (Allow: GET).
//!   - `/_control/` is DELETE-only here; non-DELETE → 405 (Allow: DELETE).
//!   - Unknown path / missing resource → 404; mutations return 204 No Content.
//!   - `verify_http` runs before dispatch: a no-op in relaxed/off mode; in strict
//!     mode it requires HTTP Basic credentials whose user AND password equal the
//!     configured SMTP user/password (constant-time compare), matching http.rs.
//!   - Read bodies are the exact JSON of `ListMessagesResult` / `Message`; the
//!     raw endpoint streams the RFC822 blob with `Content-Type: message/rfc822`.
//!
//! A hand-rolled HTTP/1.1 reader/writer on plain tokio (same pattern as the
//! redis-control / sqs crates) keeps the dependency surface at
//! tokio/serde/serde_json. Never logs credentials, the Authorization header, or
//! message bodies.

use std::collections::HashMap;
use std::sync::Arc;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use crate::service::Service;
use crate::smtp::SMTP_AUTH_STRICT;

const INTROSPECT_PREFIX: &str = "/_introspect/";
const CONTROL_PREFIX: &str = "/_control/";
const MAX_HEADER_BYTES: usize = 64 * 1024;
const MAX_BODY_BYTES: usize = 1024 * 1024;
/// Mirrors http.rs's default list limit when `?limit=` is absent or unparsable.
const DEFAULT_LIST_LIMIT: i32 = 100;

/// Auth configuration for the listener, mirroring legacy `HTTPConfig` auth fields.
/// The listen addr is supplied to [`serve_http`] via the bound [`TcpListener`].
#[derive(Debug, Clone)]
pub struct HttpAuth {
    pub auth_mode: String,
    pub username: String,
    pub password: String,
}

impl HttpAuth {
    /// Mirrors `HTTPConfig.authMode`: empty → "off"; otherwise the lowered mode.
    fn mode(&self) -> String {
        let mode = self.auth_mode.trim().to_ascii_lowercase();
        if mode.is_empty() {
            crate::smtp::SMTP_AUTH_OFF.to_string()
        } else {
            mode
        }
    }

    fn is_strict(&self) -> bool {
        self.mode() == SMTP_AUTH_STRICT
    }
}

struct Request {
    method: String,
    /// The unescaped path component.
    path: String,
    query: HashMap<String, String>,
    headers: HashMap<String, String>,
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
struct Response {
    status: u16,
    content_type: String,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}

impl Response {
    fn json(status: u16, value: &serde_json::Value) -> Response {
        Response {
            status,
            content_type: "application/json".to_string(),
            headers: Vec::new(),
            body: serde_json::to_vec(value).unwrap_or_default(),
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

    fn no_content() -> Response {
        Response {
            status: 204,
            content_type: String::new(),
            headers: Vec::new(),
            body: Vec::new(),
        }
    }

    /// Mirrors legacy `http.NotFound`.
    fn not_found() -> Response {
        Response::text(404, "404 page not found")
    }

    fn method_not_allowed(allow: &str) -> Response {
        let mut resp = Response::text(405, "method not allowed");
        resp.headers.push(("Allow".to_string(), allow.to_string()));
        resp
    }
}

/// Serves the mail introspect/control surface on `listener` until `shutdown`
/// resolves. The orchestrator binds the listener on the mail HTTP addr and
/// reuses the same `Arc<Service>` it constructed for SMTP, so SMTP ingest and
/// dashboard reads/mutations share one store and one event sink.
pub async fn serve_http(
    listener: TcpListener,
    service: Arc<Service>,
    auth_mode: String,
    username: String,
    password: String,
    shutdown: impl std::future::Future<Output = ()>,
) -> Result<(), String> {
    let auth = HttpAuth {
        auth_mode,
        username,
        password,
    };
    tokio::pin!(shutdown);
    loop {
        tokio::select! {
            _ = &mut shutdown => return Ok(()),
            accepted = listener.accept() => {
                let (stream, _) = accepted.map_err(|e| format!("mail http accept: {e}"))?;
                let service = Arc::clone(&service);
                let auth = auth.clone();
                tokio::spawn(async move {
                    let _ = handle_conn(stream, service, auth).await;
                });
            }
        }
    }
}

async fn handle_conn(
    mut stream: TcpStream,
    service: Arc<Service>,
    auth: HttpAuth,
) -> std::io::Result<()> {
    let request = match read_request(&mut stream).await {
        Ok(Some(req)) => req,
        _ => return Ok(()),
    };
    let response = dispatch(&service, &auth, &request);
    write_response(&mut stream, response).await
}

/// Top-level routing, mirroring `ServeHTTP`: intercept the two prefixes before
/// anything else, run `verify_http` first, then 404 for everything else.
fn dispatch(service: &Service, auth: &HttpAuth, req: &Request) -> Response {
    let path = if req.path.is_empty() { "/" } else { &req.path };
    if path.starts_with(INTROSPECT_PREFIX) {
        if let Some(resp) = verify_http(auth, req) {
            return resp;
        }
        handle_introspect(service, req)
    } else if path.starts_with(CONTROL_PREFIX) {
        if let Some(resp) = verify_http(auth, req) {
            return resp;
        }
        handle_control(service, req)
    } else {
        Response::not_found()
    }
}

/// Mirrors `handleIntrospect` (GET-only).
fn handle_introspect(service: &Service, req: &Request) -> Response {
    if req.method != "GET" {
        return Response::method_not_allowed("GET");
    }
    let rest = req.path.trim_start_matches(INTROSPECT_PREFIX);
    if rest == "messages" {
        let limit = parse_limit(req.query_param("limit"));
        match service.list(crate::model::ListMessagesInput {
            limit,
            cursor: String::new(),
        }) {
            Ok(result) => Response::json(200, &serde_json::to_value(&result).unwrap_or_default()),
            Err(message) => Response::text(500, &message),
        }
    } else if let Some(id_part) = rest.strip_prefix("messages/") {
        let Some((id, raw)) = parse_message_rest(id_part) else {
            return Response::not_found();
        };
        if raw {
            match service.get_raw(&id) {
                Ok(Some(body)) => Response {
                    status: 200,
                    content_type: "message/rfc822".to_string(),
                    headers: Vec::new(),
                    body,
                },
                Ok(None) => Response::not_found(),
                Err(message) => Response::text(500, &message),
            }
        } else {
            match service.get(&id) {
                Ok(Some(message)) => {
                    Response::json(200, &serde_json::to_value(&message).unwrap_or_default())
                }
                Ok(None) => Response::not_found(),
                Err(message) => Response::text(500, &message),
            }
        }
    } else {
        Response::not_found()
    }
}

/// Mirrors `handleControl` (DELETE-only).
fn handle_control(service: &Service, req: &Request) -> Response {
    let rest = req.path.trim_start_matches(CONTROL_PREFIX);
    if rest == "messages" {
        if req.method != "DELETE" {
            return Response::method_not_allowed("DELETE");
        }
        match service.delete_all() {
            Ok(()) => Response::no_content(),
            Err(message) => Response::text(500, &message),
        }
    } else if let Some(id_part) = rest.strip_prefix("messages/") {
        // A `{id}/raw` path is not a valid control target (legacy: id=="" || raw → 404).
        let Some((id, raw)) = parse_message_rest(id_part) else {
            return Response::not_found();
        };
        if raw {
            return Response::not_found();
        }
        if req.method != "DELETE" {
            return Response::method_not_allowed("DELETE");
        }
        match service.delete(&id) {
            Ok(()) => Response::no_content(),
            Err(message) => Response::text(500, &message),
        }
    } else {
        Response::not_found()
    }
}

/// Mirrors http.rs's limit parsing: default 100; a parsable integer overrides it
/// (including non-positive values, which the store later clamps, matching legacy).
fn parse_limit(raw: &str) -> i32 {
    if raw.is_empty() {
        return DEFAULT_LIST_LIMIT;
    }
    raw.parse::<i32>().unwrap_or(DEFAULT_LIST_LIMIT)
}

/// Mirrors `parseMessageRest`: split the remainder after `messages/` into an id
/// and a raw flag. `{id}` → (id, false); `{id}/raw` → (id, true); anything else
/// (empty id, or a suffix other than `raw`) → None.
fn parse_message_rest(rest: &str) -> Option<(String, bool)> {
    let rest = rest.trim_matches('/');
    if rest.is_empty() {
        return None;
    }
    match rest.split_once('/') {
        None => Some((rest.to_string(), false)),
        Some((head, suffix)) => {
            if suffix != "raw" || head.is_empty() {
                None
            } else {
                Some((head.to_string(), true))
            }
        }
    }
}

/// Mirrors `verifyHTTP`: open in relaxed/off mode; in strict mode require HTTP
/// Basic credentials whose user AND password equal the configured ones
/// (constant-time compare). Returns `Some(401)` to reject, `None` to proceed.
fn verify_http(auth: &HttpAuth, req: &Request) -> Option<Response> {
    if !auth.is_strict() {
        return None;
    }
    if let Some((user, pass)) = basic_auth(req.header("authorization")) {
        if constant_time_eq(user.as_bytes(), auth.username.as_bytes())
            && constant_time_eq(pass.as_bytes(), auth.password.as_bytes())
        {
            return None;
        }
    }
    let mut resp = Response::text(401, "unauthorized");
    resp.headers.push((
        "WWW-Authenticate".to_string(),
        r#"Basic realm="devcloud-mail""#.to_string(),
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

// --- HTTP/1.1 transport (hand-rolled, same pattern as redis-control/http.rs) ---

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
    let path = percent_decode(&raw_path).unwrap_or(raw_path);
    let query = parse_query(&raw_query);

    let mut headers = HashMap::new();
    for line in lines {
        if let Some((k, v)) = line.split_once(':') {
            headers.insert(k.trim().to_ascii_lowercase(), v.trim().to_string());
        }
    }

    // Drain any request body (none of the routes consume it) so a client that
    // sent a body does not see a truncated read; bounded by MAX_BODY_BYTES.
    let content_length: usize = headers
        .get("content-length")
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
    if content_length > MAX_BODY_BYTES {
        return Ok(None);
    }
    let mut got = buf.len().saturating_sub(header_end + 4);
    while got < content_length {
        let n = stream.read(&mut tmp).await?;
        if n == 0 {
            break;
        }
        got += n;
    }

    Ok(Some(Request {
        method,
        path,
        query,
        headers,
    }))
}

async fn write_response(stream: &mut TcpStream, resp: Response) -> std::io::Result<()> {
    let mut head = format!(
        "HTTP/1.1 {} {}\r\n",
        resp.status,
        reason_phrase(resp.status)
    );
    head.push_str("Server: devcloud-mail\r\n");
    if !resp.content_type.is_empty() {
        head.push_str(&format!("Content-Type: {}\r\n", resp.content_type));
    }
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
        204 => "No Content",
        400 => "Bad Request",
        401 => "Unauthorized",
        404 => "Not Found",
        405 => "Method Not Allowed",
        500 => "Internal Server Error",
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

/// Percent-decodes a path/query segment (path semantics; `+` is literal,
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
    fn limit_defaults_then_parses() {
        assert_eq!(parse_limit(""), 100);
        assert_eq!(parse_limit("25"), 25);
        assert_eq!(parse_limit("abc"), 100);
        // Non-positive passes through to the store (which clamps), matching legacy.
        assert_eq!(parse_limit("0"), 0);
        assert_eq!(parse_limit("-3"), -3);
    }

    #[test]
    fn message_rest_splits_id_and_raw() {
        assert_eq!(
            parse_message_rest("msg_1"),
            Some(("msg_1".to_string(), false))
        );
        assert_eq!(
            parse_message_rest("msg_1/raw"),
            Some(("msg_1".to_string(), true))
        );
        assert_eq!(parse_message_rest(""), None);
        assert_eq!(parse_message_rest("/"), None);
        assert_eq!(parse_message_rest("msg_1/other"), None);
        // legacy trims surrounding slashes first, so "/raw" → id "raw" (no inner '/').
        assert_eq!(parse_message_rest("/raw"), Some(("raw".to_string(), false)));
    }

    #[test]
    fn basic_auth_parses_user_and_pass() {
        // "user:secret" base64
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
    fn relaxed_mode_skips_auth() {
        let auth = HttpAuth {
            auth_mode: "relaxed".to_string(),
            username: "u".to_string(),
            password: "p".to_string(),
        };
        let req = Request {
            method: "GET".to_string(),
            path: "/_introspect/messages".to_string(),
            query: HashMap::new(),
            headers: HashMap::new(),
        };
        assert!(verify_http(&auth, &req).is_none());
    }

    #[test]
    fn strict_mode_rejects_missing_credentials() {
        let auth = HttpAuth {
            auth_mode: "strict".to_string(),
            username: "u".to_string(),
            password: "p".to_string(),
        };
        let req = Request {
            method: "GET".to_string(),
            path: "/_introspect/messages".to_string(),
            query: HashMap::new(),
            headers: HashMap::new(),
        };
        assert_eq!(verify_http(&auth, &req).unwrap().status, 401);
    }
}
