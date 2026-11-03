//! The Lambda Runtime API (`AWS_LAMBDA_RUNTIME_API`) for `provided*` runtimes.
//!
//! Each execution environment of a custom-runtime function gets its own
//! server on a loopback port; the function's `bootstrap` (for example a Go
//! binary built with `aws-lambda-go`) polls it for invocations and posts the
//! outcome back:
//!
//! - `GET  /2018-06-01/runtime/invocation/next` — long-polls for the next event
//! - `POST /2018-06-01/runtime/invocation/{id}/response`
//! - `POST /2018-06-01/runtime/invocation/{id}/error`
//! - `POST /2018-06-01/runtime/init/error`
//!
//! Extensions can register (`POST /2020-01-01/extension/register`) so that
//! runtime clients which register an internal extension for SIGTERM handling
//! (`aws-lambda-go`'s `WithEnableSIGTERM`) start cleanly, but no lifecycle
//! events are delivered: `GET /2020-01-01/extension/event/next` never
//! answers, and a stopped environment is killed without a SIGTERM.
//!
//! The server speaks the same protocol to [`crate::runtime`] as the embedded
//! Python/Node bootstraps: an outcome is written to the request's result file
//! and a `<marker> <tag>` line is written into the environment's output pipe.
//! The bootstrap's own output went into that pipe before it made the HTTP
//! call, so the marker lands after it and the log is split at the right place.

use std::collections::HashMap;
use std::io::Write;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use base64::Engine;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::mpsc;

const API_PREFIX: &str = "/2018-06-01/runtime/";
const EXTENSION_PREFIX: &str = "/2020-01-01/extension/";
const MAX_HEADER_BYTES: usize = 64 * 1024;
/// Lambda's 6 MB response limit, with room for an error document.
const MAX_BODY_BYTES: usize = 7 * 1024 * 1024;

/// One invocation handed to the bootstrap.
pub(crate) struct ApiInvocation {
    pub request_id: String,
    pub deadline_ms: u64,
    pub function_arn: String,
    pub event: Vec<u8>,
    /// Where the outcome is written, in the bootstrap result format.
    pub result_path: PathBuf,
}

struct State {
    marker_prefix: String,
    /// devcloud's own write end of the environment's output pipe.
    marker_out: Mutex<std::fs::File>,
    init_error_path: PathBuf,
    queue: tokio::sync::Mutex<mpsc::UnboundedReceiver<ApiInvocation>>,
    /// Request id → result file of invocations handed out and not yet answered.
    in_flight: Mutex<HashMap<String, PathBuf>>,
    ready_sent: AtomicBool,
    /// Body of an extension registration response.
    extension_info: serde_json::Value,
}

/// A running Runtime API server; dropping it stops the server.
pub(crate) struct RuntimeApi {
    pub addr: String,
    submit: mpsc::UnboundedSender<ApiInvocation>,
    task: tokio::task::AbortHandle,
}

impl Drop for RuntimeApi {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl RuntimeApi {
    /// Binds a loopback port and starts serving. `marker_out` is a write end
    /// of the pipe the bootstrap's stdout/stderr go to.
    pub async fn start(
        marker_prefix: String,
        marker_out: std::fs::File,
        init_error_path: PathBuf,
        extension_info: serde_json::Value,
    ) -> std::io::Result<RuntimeApi> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?.to_string();
        let (submit, queue) = mpsc::unbounded_channel();
        let state = Arc::new(State {
            marker_prefix,
            marker_out: Mutex::new(marker_out),
            init_error_path,
            queue: tokio::sync::Mutex::new(queue),
            in_flight: Mutex::new(HashMap::new()),
            ready_sent: AtomicBool::new(false),
            extension_info,
        });
        let task = tokio::spawn(async move {
            let mut connections = tokio::task::JoinSet::new();
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    continue;
                };
                while connections.try_join_next().is_some() {}
                let state = Arc::clone(&state);
                connections.spawn(async move {
                    let _ = serve_connection(stream, state).await;
                });
            }
        })
        .abort_handle();
        Ok(RuntimeApi { addr, submit, task })
    }

    /// Queues an invocation for the bootstrap's next `GET .../next`.
    pub fn submit(&self, invocation: ApiInvocation) {
        let _ = self.submit.send(invocation);
    }
}

impl State {
    /// Writes a marker line after everything already in the output pipe.
    async fn marker(self: &Arc<Self>, tag: &str) {
        let state = Arc::clone(self);
        let line = format!("{} {tag}\n", self.marker_prefix);
        let _ = tokio::task::spawn_blocking(move || {
            let mut out = state.marker_out.lock().unwrap();
            let _ = out.write_all(line.as_bytes());
        })
        .await;
    }
}

struct Request {
    method: String,
    path: String,
    headers: HashMap<String, String>,
    trailers: HashMap<String, String>,
    body: Vec<u8>,
    close: bool,
}

struct Response {
    status: u16,
    headers: Vec<(&'static str, String)>,
    body: Vec<u8>,
}

impl Response {
    fn json(status: u16, value: serde_json::Value) -> Self {
        Response {
            status,
            headers: Vec::new(),
            body: serde_json::to_vec(&value).unwrap_or_default(),
        }
    }

    fn accepted() -> Self {
        Response::json(202, serde_json::json!({ "status": "OK" }))
    }

    fn error(status: u16, error_type: &str, message: &str) -> Self {
        Response::json(
            status,
            serde_json::json!({ "errorType": error_type, "errorMessage": message }),
        )
    }
}

async fn serve_connection(
    mut stream: tokio::net::TcpStream,
    state: Arc<State>,
) -> std::io::Result<()> {
    let mut buf = Vec::new();
    loop {
        let Some(req) = read_request(&mut stream, &mut buf).await? else {
            return Ok(());
        };
        let close = req.close;
        let resp = handle(&state, req).await;
        write_response(&mut stream, resp).await?;
        if close {
            return Ok(());
        }
    }
}

async fn handle(state: &Arc<State>, req: Request) -> Response {
    if let Some(route) = req.path.strip_prefix(EXTENSION_PREFIX) {
        return match (req.method.as_str(), route) {
            ("POST", "register") => {
                let name = req
                    .headers
                    .get("lambda-extension-name")
                    .cloned()
                    .unwrap_or_default();
                let mut resp = Response::json(200, state.extension_info.clone());
                resp.headers
                    .push(("Lambda-Extension-Identifier", format!("devcloud-{name}")));
                resp
            }
            // No lifecycle events are emitted; the poll lasts as long as
            // the environment.
            ("GET", "event/next") => std::future::pending().await,
            _ => Response::error(404, "NotFound", "unknown path"),
        };
    }
    let Some(route) = req.path.strip_prefix(API_PREFIX) else {
        return Response::error(404, "NotFound", "unknown path");
    };
    match (req.method.as_str(), route) {
        ("GET", "invocation/next") => next_invocation(state).await,
        ("POST", "init/error") => {
            let doc = error_document(&req);
            let _ = write_json(&state.init_error_path, &serde_json::json!({ "error": doc }));
            state.marker("initerror").await;
            Response::accepted()
        }
        ("POST", rest) => {
            let Some(rest) = rest.strip_prefix("invocation/") else {
                return Response::error(404, "NotFound", "unknown path");
            };
            let (request_id, kind) = match rest.rsplit_once('/') {
                Some((id, kind @ ("response" | "error"))) => (id, kind),
                _ => return Response::error(404, "NotFound", "unknown path"),
            };
            let Some(result_path) = state.in_flight.lock().unwrap().remove(request_id) else {
                return Response::error(400, "InvalidRequestID", "Invalid request ID");
            };
            let outcome = if kind == "error"
                || req
                    .trailers
                    .contains_key("lambda-runtime-function-error-type")
            {
                serde_json::json!({ "error": error_document(&req) })
            } else {
                serde_json::json!({ "result": String::from_utf8_lossy(&req.body) })
            };
            let _ = write_json(&result_path, &outcome);
            state.marker("done").await;
            Response::accepted()
        }
        _ => Response::error(405, "MethodNotAllowed", "method not allowed"),
    }
}

async fn next_invocation(state: &Arc<State>) -> Response {
    // The first poll means init is over.
    if !state.ready_sent.swap(true, Ordering::SeqCst) {
        state.marker("ready").await;
    }
    let Some(inv) = state.queue.lock().await.recv().await else {
        return Response::error(500, "ServiceException", "runtime api closed");
    };
    state
        .in_flight
        .lock()
        .unwrap()
        .insert(inv.request_id.clone(), inv.result_path);
    Response {
        status: 200,
        headers: vec![
            ("Lambda-Runtime-Aws-Request-Id", inv.request_id.clone()),
            ("Lambda-Runtime-Deadline-Ms", inv.deadline_ms.to_string()),
            ("Lambda-Runtime-Invoked-Function-Arn", inv.function_arn),
            (
                "Lambda-Runtime-Trace-Id",
                format!(
                    "Root=1-00000000-{};Sampled=0",
                    trace_suffix(&inv.request_id)
                ),
            ),
        ],
        body: inv.event,
    }
}

/// 24 hex characters for the X-Ray root id, derived from the request id.
fn trace_suffix(request_id: &str) -> String {
    let mut hex: String = request_id.chars().filter(char::is_ascii_hexdigit).collect();
    hex.truncate(24);
    while hex.len() < 24 {
        hex.push('0');
    }
    hex
}

/// The error document a bootstrap reported: the JSON body of an `/error`
/// post, or the `Lambda-Runtime-Function-Error-Body` trailer of a streamed
/// response, with `errorType` falling back to the error-type header.
fn error_document(req: &Request) -> serde_json::Value {
    let header_type = req
        .trailers
        .get("lambda-runtime-function-error-type")
        .or_else(|| req.headers.get("lambda-runtime-function-error-type"))
        .cloned()
        .unwrap_or_default();
    let body = match req.trailers.get("lambda-runtime-function-error-body") {
        Some(b64) => base64::engine::general_purpose::STANDARD
            .decode(b64.trim())
            .unwrap_or_default(),
        None => req.body.clone(),
    };
    let mut doc = match serde_json::from_slice::<serde_json::Value>(&body) {
        Ok(v @ serde_json::Value::Object(_)) => v,
        _ => serde_json::json!({ "errorMessage": String::from_utf8_lossy(&body) }),
    };
    let missing_type = doc
        .get("errorType")
        .and_then(|t| t.as_str())
        .is_none_or(str::is_empty);
    if missing_type {
        doc["errorType"] = serde_json::json!(if header_type.is_empty() {
            "Unhandled".to_string()
        } else {
            header_type
        });
    }
    doc
}

fn write_json(path: &std::path::Path, value: &serde_json::Value) -> std::io::Result<()> {
    std::fs::write(path, serde_json::to_vec(value).unwrap_or_default())
}

/// Reads one request from a keep-alive connection; `buf` carries bytes read
/// past the previous request. `None` on a clean close or malformed input.
async fn read_request<S: AsyncRead + Unpin>(
    stream: &mut S,
    buf: &mut Vec<u8>,
) -> std::io::Result<Option<Request>> {
    let head_end = loop {
        if let Some(pos) = find(buf, b"\r\n\r\n") {
            break pos;
        }
        if buf.len() > MAX_HEADER_BYTES || !fill(stream, buf).await? {
            return Ok(None);
        }
    };
    let head = String::from_utf8_lossy(&buf[..head_end]).into_owned();
    buf.drain(..head_end + 4);

    let mut lines = head.split("\r\n");
    let mut request_line = lines.next().unwrap_or("").split(' ');
    let method = request_line.next().unwrap_or("").to_string();
    let target = request_line.next().unwrap_or("/");
    let path = target.split('?').next().unwrap_or("/").to_string();
    let version = request_line.next().unwrap_or("HTTP/1.1");
    let headers = parse_fields(lines);

    let chunked = headers
        .get("transfer-encoding")
        .is_some_and(|v| v.to_ascii_lowercase().contains("chunked"));
    let mut trailers = HashMap::new();
    let body = if chunked {
        match read_chunked(stream, buf, &mut trailers).await? {
            Some(b) => b,
            None => return Ok(None),
        }
    } else {
        let len: usize = match headers.get("content-length") {
            None => 0,
            Some(v) => match v.parse() {
                Ok(n) if n <= MAX_BODY_BYTES => n,
                _ => return Ok(None),
            },
        };
        while buf.len() < len {
            if !fill(stream, buf).await? {
                return Ok(None);
            }
        }
        buf.drain(..len).collect()
    };
    let connection = headers.get("connection").map(|v| v.to_ascii_lowercase());
    let close = match connection.as_deref() {
        Some("close") => true,
        Some("keep-alive") => false,
        _ => version == "HTTP/1.0",
    };
    Ok(Some(Request {
        method,
        path,
        headers,
        trailers,
        body,
        close,
    }))
}

/// Decodes a chunked body, collecting trailer fields into `trailers`.
async fn read_chunked<S: AsyncRead + Unpin>(
    stream: &mut S,
    buf: &mut Vec<u8>,
    trailers: &mut HashMap<String, String>,
) -> std::io::Result<Option<Vec<u8>>> {
    let mut body = Vec::new();
    loop {
        let Some(line) = read_line(stream, buf).await? else {
            return Ok(None);
        };
        let size_text = line.split(';').next().unwrap_or("").trim();
        let Ok(size) = usize::from_str_radix(size_text, 16) else {
            return Ok(None);
        };
        if size == 0 {
            // Trailer section: fields until an empty line.
            loop {
                let Some(line) = read_line(stream, buf).await? else {
                    return Ok(None);
                };
                if line.is_empty() {
                    return Ok(Some(body));
                }
                trailers.extend(parse_fields(std::iter::once(line.as_str())));
            }
        }
        if body.len() + size > MAX_BODY_BYTES {
            return Ok(None);
        }
        while buf.len() < size + 2 {
            if !fill(stream, buf).await? {
                return Ok(None);
            }
        }
        body.extend_from_slice(&buf[..size]);
        if &buf[size..size + 2] != b"\r\n" {
            return Ok(None);
        }
        buf.drain(..size + 2);
    }
}

async fn read_line<S: AsyncRead + Unpin>(
    stream: &mut S,
    buf: &mut Vec<u8>,
) -> std::io::Result<Option<String>> {
    loop {
        if let Some(pos) = find(buf, b"\r\n") {
            let line = String::from_utf8_lossy(&buf[..pos]).into_owned();
            buf.drain(..pos + 2);
            return Ok(Some(line));
        }
        if buf.len() > MAX_HEADER_BYTES || !fill(stream, buf).await? {
            return Ok(None);
        }
    }
}

fn parse_fields<'a>(lines: impl Iterator<Item = &'a str>) -> HashMap<String, String> {
    lines
        .filter_map(|l| l.split_once(':'))
        .map(|(k, v)| (k.trim().to_ascii_lowercase(), v.trim().to_string()))
        .collect()
}

/// Reads more bytes into `buf`; false at end of stream.
async fn fill<S: AsyncRead + Unpin>(stream: &mut S, buf: &mut Vec<u8>) -> std::io::Result<bool> {
    let mut chunk = [0u8; 8192];
    let n = stream.read(&mut chunk).await?;
    buf.extend_from_slice(&chunk[..n]);
    Ok(n > 0)
}

fn find(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack.windows(needle.len()).position(|w| w == needle)
}

async fn write_response<S: AsyncWrite + Unpin>(
    stream: &mut S,
    resp: Response,
) -> std::io::Result<()> {
    let reason = match resp.status {
        200 => "OK",
        202 => "Accepted",
        400 => "Bad Request",
        404 => "Not Found",
        405 => "Method Not Allowed",
        _ => "Internal Server Error",
    };
    let mut head = format!("HTTP/1.1 {} {reason}\r\n", resp.status);
    head.push_str("Content-Type: application/json\r\n");
    for (k, v) in &resp.headers {
        head.push_str(&format!("{k}: {v}\r\n"));
    }
    head.push_str(&format!("Content-Length: {}\r\n\r\n", resp.body.len()));
    stream.write_all(head.as_bytes()).await?;
    stream.write_all(&resp.body).await?;
    stream.flush().await
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn parse(wire: &[u8]) -> Option<Request> {
        let mut stream = wire;
        let mut buf = Vec::new();
        read_request(&mut stream, &mut buf).await.unwrap()
    }

    #[tokio::test]
    async fn content_length_body() {
        let req = parse(b"POST /2018-06-01/runtime/invocation/abc/response HTTP/1.1\r\nContent-Length: 4\r\n\r\n\"ok\"").await.unwrap();
        assert_eq!(req.method, "POST");
        assert_eq!(req.path, "/2018-06-01/runtime/invocation/abc/response");
        assert_eq!(req.body, b"\"ok\"");
        assert!(!req.close);
    }

    #[tokio::test]
    async fn chunked_body_with_trailers() {
        let req = parse(
            b"POST /x HTTP/1.1\r\nTransfer-Encoding: chunked\r\nTrailer: Lambda-Runtime-Function-Error-Type\r\n\r\n\
              3\r\nabc\r\n2;ext=1\r\nde\r\n0\r\nLambda-Runtime-Function-Error-Type: Runtime.TruncatedResponse\r\n\r\n",
        )
        .await
        .unwrap();
        assert_eq!(req.body, b"abcde");
        assert_eq!(
            req.trailers
                .get("lambda-runtime-function-error-type")
                .map(String::as_str),
            Some("Runtime.TruncatedResponse")
        );
    }

    #[tokio::test]
    async fn keep_alive_reads_consecutive_requests() {
        let wire: &[u8] = b"GET /a HTTP/1.1\r\n\r\nPOST /b HTTP/1.1\r\nContent-Length: 1\r\nConnection: close\r\n\r\nx";
        let mut stream = wire;
        let mut buf = Vec::new();
        let a = read_request(&mut stream, &mut buf).await.unwrap().unwrap();
        let b = read_request(&mut stream, &mut buf).await.unwrap().unwrap();
        assert_eq!((a.path.as_str(), b.path.as_str()), ("/a", "/b"));
        assert_eq!(b.body, b"x");
        assert!(b.close);
        assert!(read_request(&mut stream, &mut buf).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn malformed_chunk_is_rejected() {
        assert!(
            parse(b"POST /x HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\nzz\r\n")
                .await
                .is_none()
        );
        assert!(
            parse(b"POST /x HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\n3\r\nabcX\r\n")
                .await
                .is_none()
        );
    }

    #[test]
    fn error_document_sources() {
        let req = |body: &[u8], header: Option<&str>, trailer_body: Option<&str>| Request {
            method: "POST".into(),
            path: String::new(),
            headers: header
                .map(|h| {
                    HashMap::from([(
                        "lambda-runtime-function-error-type".to_string(),
                        h.to_string(),
                    )])
                })
                .unwrap_or_default(),
            trailers: trailer_body
                .map(|b| {
                    HashMap::from([(
                        "lambda-runtime-function-error-body".to_string(),
                        b.to_string(),
                    )])
                })
                .unwrap_or_default(),
            body: body.to_vec(),
            close: false,
        };
        let doc = error_document(&req(
            br#"{"errorMessage":"boom","errorType":"MyError"}"#,
            None,
            None,
        ));
        assert_eq!(doc["errorType"], "MyError");
        let doc = error_document(&req(
            br#"{"errorMessage":"boom"}"#,
            Some("errorString"),
            None,
        ));
        assert_eq!(doc["errorType"], "errorString");
        let doc = error_document(&req(b"not json", None, None));
        assert_eq!(doc["errorMessage"], "not json");
        assert_eq!(doc["errorType"], "Unhandled");
        let b64 = base64::engine::general_purpose::STANDARD
            .encode(br#"{"errorMessage":"cut","errorType":"Runtime.TruncatedResponse"}"#);
        let doc = error_document(&req(b"partial", None, Some(&b64)));
        assert_eq!(doc["errorMessage"], "cut");
    }

    #[test]
    fn trace_suffix_is_24_hex() {
        assert_eq!(
            trace_suffix("0a1b2c3d-4e5f-6789-abcd-ef0123456789").len(),
            24
        );
        assert_eq!(trace_suffix("x"), "0".repeat(24));
    }
}
