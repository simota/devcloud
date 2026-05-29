//! Minimal HTTP/1.1 socket server for the SQS JSON protocol, mirroring the JSON
//! path of `routes.go`. A hand-rolled reader/writer on plain tokio (same pattern
//! as the applicationautoscaling crate) keeps the dependency surface tiny.
//!
//! Scope: the AWS JSON 1.0 protocol (modern SDK default), dispatched via
//! `X-Amz-Target`. The legacy Query/XML protocol is not served here — a request
//! without an `AmazonSQS.*` target gets a documented `501 NotImplemented`. The
//! response headers match Go's `writeJSON` (`Content-Type:
//! application/x-amz-json-1.0`, `x-amzn-RequestId: devcloud-sqs`, `Server:
//! devcloud-sqs`).

use std::collections::HashMap;
use std::sync::Mutex;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use crate::http_json::JsonOutcome;
use crate::server::Server;

const MAX_HEADER_BYTES: usize = 64 * 1024;
const MAX_BODY_BYTES: usize = 16 * 1024 * 1024;

struct Request {
    method: String,
    headers: HashMap<String, String>,
    body: Vec<u8>,
}

impl Request {
    fn header(&self, name: &str) -> &str {
        self.headers.get(name).map(String::as_str).unwrap_or("")
    }
}

/// Runs the accept loop until `shutdown` resolves. The `Server` is shared behind
/// a `Mutex` (its operations are `&mut self`, mirroring the Go `sync.Mutex`).
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
    let outcome = process(&server, &request);
    write_response(&mut stream, outcome).await
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
    let method = request_line.split(' ').next().unwrap_or("").to_string();

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
        headers,
        body,
    }))
}

fn process(server: &Mutex<Server>, req: &Request) -> JsonOutcome {
    if req.method != "POST" {
        return JsonOutcome::error(405, "InvalidAction", "method is not supported");
    }
    // The modern SDK speaks JSON 1.0 with an X-Amz-Target. The legacy Query/XML
    // protocol is not implemented in this Rust increment.
    let target = req.header("x-amz-target");
    if target.is_empty() {
        return JsonOutcome::error(
            501,
            "NotImplemented",
            "the Query/XML protocol is not supported; use the JSON protocol (X-Amz-Target)",
        );
    }
    let mut guard = server.lock().unwrap();
    guard.dispatch_json(target, &req.body)
}

async fn write_response(stream: &mut TcpStream, outcome: JsonOutcome) -> std::io::Result<()> {
    let body = serde_json::to_vec(&outcome.body).unwrap_or_default();
    let mut head = format!(
        "HTTP/1.1 {} {}\r\n",
        outcome.status,
        reason_phrase(outcome.status)
    );
    head.push_str("Server: devcloud-sqs\r\n");
    head.push_str("Content-Type: application/x-amz-json-1.0\r\n");
    head.push_str("x-amzn-RequestId: devcloud-sqs\r\n");
    head.push_str(&format!("Content-Length: {}\r\n", body.len()));
    head.push_str("Connection: close\r\n\r\n");
    stream.write_all(head.as_bytes()).await?;
    stream.write_all(&body).await?;
    stream.flush().await
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
