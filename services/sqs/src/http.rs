//! Minimal HTTP/1.1 socket server for SQS, mirroring `routes.rs`. A hand-rolled
//! reader/writer on plain tokio (same pattern as the applicationautoscaling
//! crate) keeps the dependency surface tiny.
//!
//! Both AWS protocols are served, exactly as the legacy reference's
//! `detectOperation` decides: the modern JSON 1.0 protocol (dispatched via
//! `X-Amz-Target`, `Content-Type: application/x-amz-json-1.0`) and the legacy
//! Query/XML protocol (GET with an `Action` query param, or POST
//! `application/x-www-form-urlencoded`; form-encoded request → XML response).
//! JSON responses match legacy `writeJSON`; Query responses match legacy
//! `writeQueryXML` (`Content-Type: text/xml; charset=utf-8`).

use std::collections::HashMap;
use std::sync::Mutex;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use crate::http_json::JsonOutcome;
use crate::http_query::QueryOutcome;
use crate::introspect::IntrospectOutcome;
use crate::server::Server;

const MAX_HEADER_BYTES: usize = 64 * 1024;
const MAX_BODY_BYTES: usize = 16 * 1024 * 1024;

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

/// Runs the accept loop until `shutdown` resolves. The `Server` is shared behind
/// a `Mutex` (its operations are `&mut self`, mirroring the legacy `sync.Mutex`).
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
    match process(&server, &request) {
        Outcome::Json(outcome) => write_json_response(&mut stream, outcome).await,
        Outcome::Introspect(outcome) => write_introspect_response(&mut stream, outcome).await,
        Outcome::Query(outcome) => write_query_response(&mut stream, outcome).await,
    }
}

/// A rendered response in either protocol — JSON (modern), the read-only
/// introspection API (also JSON-shaped), or XML (Query).
enum Outcome {
    Json(JsonOutcome),
    Introspect(IntrospectOutcome),
    Query(QueryOutcome),
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
    let mut parts = request_line.split(' ');
    let method = parts.next().unwrap_or("").to_string();
    let target = parts.next().unwrap_or("");
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

fn process(server: &Mutex<Server>, req: &Request) -> Outcome {
    // Read-only introspection API, intercepted BEFORE the provider-protocol
    // dispatch (mirrors handleIntrospect being called at the top of routing).
    // GET-only; non-GET yields a 405 with `Allow: GET`. The path can never
    // collide with the AWS protocols, which target "/" with an Action param or
    // X-Amz-Target header.
    if crate::introspect::is_introspect_path(&req.path) {
        let guard = server.lock().unwrap();
        return Outcome::Introspect(crate::introspect::handle_introspect(
            &guard,
            &req.method,
            &req.path,
        ));
    }

    // Method gate, mirroring routes.rs: only GET/POST are accepted, and the
    // rejection is always rendered as a JSON error (matching legacy writeJSONError
    // for the method-not-allowed case).
    if req.method != "POST" && req.method != "GET" {
        return Outcome::Json(JsonOutcome::error(
            405,
            "InvalidAction",
            "method is not supported",
        ));
    }

    let target = req.header("x-amz-target");
    let content_type = req.header("content-type");

    // Protocol detection mirrors `detectOperation`. The JSON path is taken when
    // the X-Amz-Target carries the `AmazonSQS.` prefix; everything else is the
    // Query/XML path (GET with ?Action=, POST form, or the fallthrough errors).
    if target.starts_with("AmazonSQS.") {
        let mut guard = server.lock().unwrap();
        return Outcome::Json(guard.dispatch_json(target, &req.body));
    }
    if content_type.contains("application/x-amz-json-1.0") && req.method == "POST" {
        // JSON content-type without a target → JSON-shaped error, as legacy returns
        // for the `application/x-amz-json-1.0` branch of detectOperation.
        return Outcome::Json(JsonOutcome::error(
            400,
            "InvalidAction",
            "missing X-Amz-Target",
        ));
    }

    // Query/XML protocol. dispatch_query mirrors the rest of detectOperation
    // (path + Version validation) plus the per-action handlers.
    let form_body =
        if req.method == "POST" && content_type.contains("application/x-www-form-urlencoded") {
            String::from_utf8_lossy(&req.body).into_owned()
        } else {
            String::new()
        };
    let mut guard = server.lock().unwrap();
    Outcome::Query(guard.dispatch_query(&req.method, &req.path, &req.query, &form_body))
}

async fn write_json_response(stream: &mut TcpStream, outcome: JsonOutcome) -> std::io::Result<()> {
    let body = serde_json::to_vec(&outcome.body).unwrap_or_default();
    let mut head = response_head(outcome.status, "application/x-amz-json-1.0", body.len());
    head.push_str("Connection: close\r\n\r\n");
    stream.write_all(head.as_bytes()).await?;
    stream.write_all(&body).await?;
    stream.flush().await
}

/// Writes a read-only introspection response. The body is already serialized
/// (in struct field order); rendered with the same headers as `writeJSON`, plus
/// `Allow: GET` on the GET-only 405 (matching `handleIntrospect`).
async fn write_introspect_response(
    stream: &mut TcpStream,
    outcome: IntrospectOutcome,
) -> std::io::Result<()> {
    let mut head = response_head(
        outcome.status,
        "application/x-amz-json-1.0",
        outcome.body.len(),
    );
    if outcome.allow_get {
        head.push_str("Allow: GET\r\n");
    }
    head.push_str("Connection: close\r\n\r\n");
    stream.write_all(head.as_bytes()).await?;
    stream.write_all(&outcome.body).await?;
    stream.flush().await
}

async fn write_query_response(
    stream: &mut TcpStream,
    outcome: QueryOutcome,
) -> std::io::Result<()> {
    let body = outcome.body.into_bytes();
    let mut head = response_head(outcome.status, "text/xml; charset=utf-8", body.len());
    head.push_str("Connection: close\r\n\r\n");
    stream.write_all(head.as_bytes()).await?;
    stream.write_all(&body).await?;
    stream.flush().await
}

/// Common status line + headers shared by both protocols (`Server`,
/// `Content-Type`, `x-amzn-RequestId`, `Content-Length`), matching the headers
/// legacy sets in `writeJSON` / `writeQueryXML` plus the `Server` header from
/// `handle`.
fn response_head(status: u16, content_type: &str, content_length: usize) -> String {
    let mut head = format!("HTTP/1.1 {} {}\r\n", status, reason_phrase(status));
    head.push_str("Server: devcloud-sqs\r\n");
    head.push_str(&format!("Content-Type: {content_type}\r\n"));
    head.push_str("x-amzn-RequestId: devcloud-sqs\r\n");
    head.push_str(&format!("Content-Length: {content_length}\r\n"));
    head
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
    haystack.windows(needle.len()).position(|w| w == needle)
}
