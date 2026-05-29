//! Minimal HTTP/1.1 front-end mirroring `routes.go`'s single `/` POST handler.
//!
//! A hand-rolled HTTP/1.1 reader/writer on plain tokio (no framework) keeps the
//! dependency surface tiny. It replicates the Go request gate exactly: only
//! `POST /` is served; the `Server` header is always set; content-type must be
//! `application/x-amz-json-1.1` (when present); SigV4 is verified per auth mode;
//! then the `X-Amz-Target` header is dispatched. Error bodies carry
//! `X-Amzn-Errortype`.

use std::collections::HashMap;
use std::sync::Arc;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use crate::server::{error_outcome, Outcome, Server};
use crate::sigv4::{verify_signature, Credentials, SignedRequest};

const TARGET_PREFIX: &str = "AnyScaleFrontendService.";
const MAX_HEADER_BYTES: usize = 64 * 1024;
const MAX_BODY_BYTES: usize = 8 * 1024 * 1024;

/// A parsed HTTP request: method, path, query, and lowercase-keyed headers.
struct Request {
    method: String,
    path: String,
    query: String,
    headers: HashMap<String, String>,
    body: Vec<u8>,
}

impl Request {
    fn header(&self, name: &str) -> &str {
        self.headers.get(name).map(String::as_str).unwrap_or("")
    }
}

/// Runs the accept loop until `shutdown` resolves. Each connection is served on
/// its own task; connections are closed after one request (`Connection: close`).
pub async fn serve(
    listener: TcpListener,
    server: Arc<Server>,
    shutdown: impl std::future::Future<Output = ()>,
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

async fn handle_conn(mut stream: TcpStream, server: Arc<Server>) -> std::io::Result<()> {
    let request = match read_request(&mut stream).await {
        Ok(Some(req)) => req,
        Ok(None) => return Ok(()), // closed or malformed; drop quietly
        Err(_) => return Ok(()),
    };
    let outcome = process(&server, &request);
    write_response(&mut stream, outcome).await
}

/// Reads one HTTP/1.1 request: request line + headers (until CRLFCRLF) then a
/// `Content-Length`-bounded body. Returns `None` on clean EOF or malformed head.
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
    let target = parts.next().unwrap_or("/");
    let (path, query) = match target.split_once('?') {
        Some((p, q)) => (p.to_string(), q.to_string()),
        None => (target.to_string(), String::new()),
    };

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
        query,
        headers,
        body,
    }))
}

async fn write_response(stream: &mut TcpStream, outcome: Outcome) -> std::io::Result<()> {
    // Go's json.Encoder appends a trailing newline; match it byte-for-byte.
    let mut body = serde_json::to_vec(&outcome.body).unwrap_or_default();
    body.push(b'\n');

    let mut head = format!(
        "HTTP/1.1 {} {}\r\n",
        outcome.status,
        reason_phrase(outcome.status)
    );
    head.push_str("Server: devcloud-application-autoscaling\r\n");
    head.push_str("Content-Type: application/x-amz-json-1.1\r\n");
    if let Some(error_type) = &outcome.error_type {
        head.push_str(&format!("X-Amzn-Errortype: {error_type}\r\n"));
    }
    if outcome.status == 405 {
        head.push_str("Allow: POST\r\n");
    }
    head.push_str(&format!("Content-Length: {}\r\n", body.len()));
    head.push_str("Connection: close\r\n\r\n");

    stream.write_all(head.as_bytes()).await?;
    stream.write_all(&body).await?;
    stream.flush().await
}

fn process(server: &Server, req: &Request) -> Outcome {
    if server.load_err().is_some() {
        return error_outcome(
            500,
            "InternalServerError",
            "failed to load application-autoscaling state",
        );
    }
    if req.path != "/" {
        return error_outcome(404, "ResourceNotFoundException", "not found");
    }
    if req.method != "POST" {
        return error_outcome(405, "ValidationException", "method not allowed");
    }
    let content_type = req.header("content-type");
    if !content_type.is_empty() && !content_type.starts_with("application/x-amz-json-1.1") {
        return error_outcome(400, "ValidationException", "unsupported content type");
    }

    let header_fn = |name: &str| -> Option<String> { req.headers.get(name).cloned() };
    let signed = SignedRequest {
        method: &req.method,
        path: &req.path,
        query: &req.query,
        host: req.header("host"),
        authorization: req.header("authorization"),
        amz_date: req.header("x-amz-date"),
        content_sha256: req.header("x-amz-content-sha256"),
        header: &header_fn,
        body: &req.body,
    };
    let cfg = server.config();
    let creds = Credentials {
        auth_mode: &cfg.auth_mode,
        access_key_id: &cfg.access_key_id,
        secret_access_key: &cfg.secret_access_key,
        region: &cfg.region,
    };
    if let Err(e) = verify_signature(&signed, &creds) {
        return error_outcome(e.status, e.name, e.name);
    }

    let target = req.header("x-amz-target");
    if !target.starts_with(TARGET_PREFIX) {
        return error_outcome(400, "UnknownOperationException", "unknown operation");
    }
    server.dispatch(&target[TARGET_PREFIX.len()..], &req.body)
}

fn reason_phrase(status: u16) -> &'static str {
    match status {
        200 => "OK",
        400 => "Bad Request",
        403 => "Forbidden",
        404 => "Not Found",
        405 => "Method Not Allowed",
        500 => "Internal Server Error",
        501 => "Not Implemented",
        _ => "Status",
    }
}

fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    if needle.is_empty() || haystack.len() < needle.len() {
        return None;
    }
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

// --- In-process test harness (mirrors what a real HTTP request would produce) ---

/// A rendered response for in-process parity tests.
pub struct TestResponse {
    pub status: u16,
    pub error_type: Option<String>,
    pub allow: Option<String>,
    pub body: Vec<u8>,
}

/// Drives the full request pipeline (gate → SigV4 → dispatch) without a socket,
/// so parity tests exercise the same code path as a real HTTP request.
/// `headers` are `(name, value)` pairs; `path` may include a `?query`.
pub fn serve_for_test(
    server: &Server,
    method: &str,
    path: &str,
    headers: &[(&str, &str)],
    body: &[u8],
) -> TestResponse {
    let (p, q) = match path.split_once('?') {
        Some((p, q)) => (p.to_string(), q.to_string()),
        None => (path.to_string(), String::new()),
    };
    let header_map: HashMap<String, String> = headers
        .iter()
        .map(|(k, v)| (k.to_ascii_lowercase(), v.to_string()))
        .collect();
    let req = Request {
        method: method.to_string(),
        path: p,
        query: q,
        headers: header_map,
        body: body.to_vec(),
    };
    let outcome = process(server, &req);
    let status = outcome.status;
    let error_type = outcome.error_type.clone();
    let mut json = serde_json::to_vec(&outcome.body).unwrap_or_default();
    json.push(b'\n');
    TestResponse {
        status,
        error_type,
        allow: if status == 405 {
            Some("POST".to_string())
        } else {
            None
        },
        body: json,
    }
}
