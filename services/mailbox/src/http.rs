use std::collections::HashMap;
use std::io;
use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::{broadcast, watch, Semaphore};

use crate::routes::{self, App};

pub const MAX_HEADER_BYTES: usize = 64 * 1024;
pub const MAX_BODY_BYTES: usize = 1024 * 1024;
pub const HEADER_TIMEOUT: Duration = Duration::from_secs(10);
pub const WRITE_TIMEOUT: Duration = Duration::from_secs(5);
pub const HEARTBEAT: Duration = Duration::from_secs(15);
pub const MAX_RESPONSE_BYTES: usize = 256 * 1024 * 1024;

/// Bound serialization as it writes, before an oversized response is allocated.
pub(crate) struct ResponseBuffer {
    bytes: Vec<u8>,
    limit: usize,
}

impl ResponseBuffer {
    pub(crate) fn new(limit: usize) -> Self {
        Self {
            bytes: Vec::new(),
            limit,
        }
    }

    pub(crate) fn into_inner(self) -> Vec<u8> {
        self.bytes
    }
}

impl io::Write for ResponseBuffer {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let length = self
            .bytes
            .len()
            .checked_add(bytes.len())
            .filter(|&length| length <= self.limit)
            .ok_or_else(|| io::Error::other("Response size limit exceeded"))?;
        if length > self.bytes.capacity() {
            let capacity = self
                .bytes
                .capacity()
                .saturating_mul(2)
                .max(length)
                .min(self.limit);
            self.bytes
                .try_reserve_exact(capacity - self.bytes.len())
                .map_err(|_| io::Error::other("Response allocation failed"))?;
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct Request {
    pub method: String,
    pub path: String,
    pub query: HashMap<String, String>,
    pub headers: HashMap<String, String>,
}

impl Request {
    pub fn new(method: &str, target: &str) -> Self {
        let (path, query) = target.split_once('?').unwrap_or((target, ""));
        let path = String::from_utf8_lossy(&crate::mime::percent_decode(path)).into_owned();
        let mut params = HashMap::new();
        for field in query.split('&').filter(|s| !s.is_empty()) {
            let (k, v) = field.split_once('=').unwrap_or((field, ""));
            let decode = |s: &str| {
                String::from_utf8_lossy(&crate::mime::percent_decode(&s.replace('+', " ")))
                    .into_owned()
            };
            params.entry(decode(k)).or_insert_with(|| decode(v));
        }
        Self {
            method: method.into(),
            path,
            query: params,
            headers: HashMap::new(),
        }
    }

    pub fn header(&self, name: &str) -> &str {
        self.headers.get(name).map_or("", String::as_str)
    }
    pub fn param(&self, name: &str) -> &str {
        self.query.get(name).map_or("", String::as_str)
    }
}

#[derive(Debug)]
pub struct Response {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
    pub sse: bool,
}

impl Response {
    pub fn new(status: u16, content_type: &str, body: Vec<u8>) -> Self {
        Self {
            status,
            headers: vec![("Content-Type".into(), content_type.into())],
            body,
            sse: false,
        }
    }

    pub fn text(status: u16, text: &str) -> Self {
        Self::new(
            status,
            "text/plain; charset=utf-8",
            text.as_bytes().to_vec(),
        )
    }

    pub fn json<T: serde::Serialize>(value: &T) -> Self {
        let mut buffer = ResponseBuffer::new(MAX_RESPONSE_BYTES);
        match serde_json::to_writer(&mut buffer, value) {
            Ok(()) => Self::new(200, "application/json", buffer.into_inner()),
            Err(_) => Self::text(500, "Response encoding failed"),
        }
    }

    pub fn with_header(mut self, name: &str, value: &str) -> Self {
        self.headers.retain(|(k, _)| !k.eq_ignore_ascii_case(name));
        self.headers.push((name.into(), value.into()));
        self
    }
}

fn token(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b"!#$%&'*+-.^_`|~".contains(&b)
}

pub fn safe_header(name: &str, value: &str) -> Option<(String, String)> {
    if name.is_empty()
        || !name.bytes().all(token)
        || name.to_ascii_lowercase().starts_with("access-control-")
    {
        return None;
    }
    let value: String = value
        .chars()
        .filter(|c| !c.is_control() || *c == '\t')
        .collect();
    Some((name.into(), value))
}

pub fn ascii_content_disposition(filename: &str) -> String {
    let filename: String = filename.chars().filter(|c| !c.is_control()).collect();
    let mut fallback = String::new();
    for c in filename.chars() {
        if !c.is_ascii() {
            fallback.push('_');
        } else if matches!(c, '"' | '\\') {
            fallback.push('\\');
            fallback.push(c);
        } else {
            fallback.push(c);
        }
    }
    format!("attachment; filename=\"{fallback}\"")
}

pub fn content_disposition(filename: &str) -> String {
    let encoded: String = filename
        .chars()
        .filter(|c| !c.is_control())
        .collect::<String>()
        .bytes()
        .map(|b| {
            if b.is_ascii_alphanumeric() || b"!#$&+-.^_`|~".contains(&b) {
                (b as char).to_string()
            } else {
                format!("%{b:02X}")
            }
        })
        .collect();
    format!(
        "{}; filename*=UTF-8''{encoded}",
        ascii_content_disposition(filename)
    )
}

pub fn safe_content_type(declared: &str) -> String {
    let (kind, _) = crate::mime::media_type(declared);
    match kind.as_str() {
        "image/png"
        | "image/jpeg"
        | "image/gif"
        | "image/webp"
        | "application/pdf"
        | "text/plain"
        | "application/zip"
        | "application/octet-stream"
        | "text/csv"
        | "application/json" => kind,
        _ => "application/octet-stream".into(),
    }
}

pub fn response_head(response: &Response, streaming: bool) -> Vec<u8> {
    let reason = match response.status {
        200 => "OK",
        400 => "Bad Request",
        401 => "Unauthorized",
        403 => "Forbidden",
        404 => "Not Found",
        405 => "Method Not Allowed",
        503 => "Service Unavailable",
        _ => "Internal Server Error",
    };
    let mut out = format!("HTTP/1.1 {} {reason}\r\n", response.status);
    for (name, value) in &response.headers {
        if name.eq_ignore_ascii_case("content-length")
            || name.eq_ignore_ascii_case("transfer-encoding")
            || name.eq_ignore_ascii_case("connection")
        {
            continue;
        }
        if let Some((name, value)) = safe_header(name, value) {
            out.push_str(&format!("{name}: {value}\r\n"));
        }
    }
    if streaming {
        out.push_str("Transfer-Encoding: chunked\r\nConnection: keep-alive\r\n\r\n");
    } else {
        out.push_str(&format!(
            "Content-Length: {}\r\nConnection: close\r\n\r\n",
            response.body.len()
        ));
    }
    out.into_bytes()
}

pub async fn read_request<S: AsyncRead + Unpin>(stream: &mut S) -> io::Result<Option<Request>> {
    let mut buf = Vec::new();
    let mut chunk = [0u8; 4096];
    let header_end = tokio::time::timeout(HEADER_TIMEOUT, async {
        loop {
            if let Some(end) = buf.windows(4).position(|s| s == b"\r\n\r\n") {
                if end + 4 > MAX_HEADER_BYTES {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "headers too large",
                    ));
                }
                return Ok(Some(end));
            }
            if buf.len() >= MAX_HEADER_BYTES {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "headers too large",
                ));
            }
            let n = stream.read(&mut chunk).await?;
            if n == 0 {
                return Ok(None);
            }
            buf.extend_from_slice(&chunk[..n]);
        }
    })
    .await
    .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "header timeout"))??;
    let Some(end) = header_end else {
        return Ok(None);
    };
    let head = std::str::from_utf8(&buf[..end])
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid headers"))?;
    let mut lines = head.split("\r\n");
    let mut words = lines.next().unwrap_or("").split_whitespace();
    let method = words.next().unwrap_or("");
    let target = words.next().unwrap_or("");
    let version = words.next().unwrap_or("");
    if !method.bytes().all(token)
        || method.is_empty()
        || !target.starts_with('/')
        || !matches!(version, "HTTP/1.1" | "HTTP/1.0")
        || words.next().is_some()
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid request line",
        ));
    }
    let mut req = Request::new(method, target);
    for line in lines {
        let Some((name, value)) = line.split_once(':') else {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "invalid header"));
        };
        if name.is_empty()
            || !name.bytes().all(token)
            || value.bytes().any(|b| (b < 32 && b != b'\t') || b == 127)
        {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "invalid header"));
        }
        let name = name.to_ascii_lowercase();
        if req.headers.insert(name, value.trim().to_string()).is_some() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "duplicate header",
            ));
        }
    }
    if req.headers.contains_key("transfer-encoding") {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "transfer encoding unsupported",
        ));
    }
    let length = if req.header("content-length").is_empty() {
        0
    } else {
        req.header("content-length")
            .parse::<usize>()
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid body length"))?
    };
    if length > MAX_BODY_BYTES {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "body too large"));
    }
    let buffered = buf.len().saturating_sub(end + 4).min(length);
    let mut remaining = length - buffered;
    tokio::time::timeout(HEADER_TIMEOUT, async {
        while remaining > 0 {
            let n = stream.read(&mut chunk[..remaining.min(4096)]).await?;
            if n == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "incomplete body",
                ));
            }
            remaining -= n;
        }
        Ok(())
    })
    .await
    .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "body timeout"))??;
    Ok(Some(req))
}

async fn write<S: AsyncWrite + Unpin>(stream: &mut S, bytes: &[u8]) -> io::Result<()> {
    tokio::time::timeout(WRITE_TIMEOUT, stream.write_all(bytes))
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "write timeout"))?
}

async fn chunk<S: AsyncWrite + Unpin>(stream: &mut S, bytes: &[u8]) -> io::Result<()> {
    let mut framed = format!("{:x}\r\n", bytes.len()).into_bytes();
    framed.extend_from_slice(bytes);
    framed.extend_from_slice(b"\r\n");
    write(stream, &framed).await
}

pub async fn stream_events<S: AsyncWrite + Unpin>(
    stream: &mut S,
    mut events: broadcast::Receiver<Arc<str>>,
    mut shutdown: watch::Receiver<bool>,
) -> io::Result<()> {
    let mut heartbeat = tokio::time::interval(HEARTBEAT);
    heartbeat.tick().await;
    loop {
        if *shutdown.borrow() {
            return Ok(());
        }
        tokio::select! {
            _ = shutdown.changed() => return Ok(()),
            _ = heartbeat.tick() => chunk(stream, b":\n\n").await?,
            event = events.recv() => match event {
                Ok(json) => chunk(stream, format!("data: {json}\n\n").as_bytes()).await?,
                Err(broadcast::error::RecvError::Lagged(_) | broadcast::error::RecvError::Closed) => return Ok(()),
            }
        }
    }
}

pub async fn handle_connection<S>(mut stream: S, app: Arc<App>, mut shutdown: watch::Receiver<bool>)
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let request = tokio::select! {
        _ = shutdown.changed() => return,
        request = read_request(&mut stream) => request,
    };
    let req = match request {
        Ok(Some(req)) => req,
        Ok(None) => return,
        Err(_) => {
            let response = routes::secure(Response::text(400, "Invalid HTTP request"), "/api/");
            let _ = write(&mut stream, &response_head(&response, false)).await;
            let _ = write(&mut stream, &response.body).await;
            return;
        }
    };
    let head_only = req.method == "HEAD";
    let path = req.path.clone();
    let app_for_route = app.clone();
    let mut response =
        match tokio::task::spawn_blocking(move || routes::dispatch(&app_for_route, &req)).await {
            Ok(response) => response,
            Err(_) => routes::secure(Response::text(500, "Request processing failed"), &path),
        };
    if response.sse && !head_only {
        let Ok(_permit) = app.sse_slots.clone().try_acquire_owned() else {
            response = routes::secure(Response::text(503, "SSE connection limit reached"), &path);
            let _ = write(&mut stream, &response_head(&response, false)).await;
            let _ = write(&mut stream, &response.body).await;
            return;
        };
        let events = app.events.subscribe();
        if write(&mut stream, &response_head(&response, true))
            .await
            .is_ok()
        {
            let (mut reader, mut writer) = tokio::io::split(&mut stream);
            let mut probe = [0u8; 1];
            let mut cancel = shutdown.clone();
            // Read-side EOF detects disconnection without waiting for the next
            // heartbeat; cancellation also interrupts a blocked SSE write.
            tokio::select! {
                _ = reader.read(&mut probe) => {},
                _ = cancel.changed() => {},
                _ = stream_events(&mut writer, events, shutdown) => {},
            }
        }
    } else if write(&mut stream, &response_head(&response, false))
        .await
        .is_ok()
        && !head_only
    {
        let _ = write(&mut stream, &response.body).await;
    }
}

pub async fn serve(
    listener: TcpListener,
    app: Arc<App>,
    mut shutdown: watch::Receiver<bool>,
) -> io::Result<()> {
    let slots = Arc::new(Semaphore::new(512));
    loop {
        tokio::select! {
            _ = shutdown.changed() => return Ok(()),
            accepted = listener.accept() => {
                let (stream, _) = match accepted {
                    Ok(connection) => connection,
                    Err(_) => {
                        eprintln!("devcloud-mailbox: HTTP accept failed; retrying");
                        tokio::select! {
                            _ = shutdown.changed() => return Ok(()),
                            _ = tokio::time::sleep(Duration::from_millis(75)) => {},
                        }
                        continue;
                    }
                };
                let Ok(permit) = slots.clone().try_acquire_owned() else { continue; };
                let app = app.clone();
                let shutdown = shutdown.clone();
                tokio::spawn(async move {
                    let _permit = permit;
                    handle_connection(stream, app, shutdown).await;
                });
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn response_buffer_caps_cumulative_writes_and_json_escape_expansion() {
        let mut buffer = ResponseBuffer::new(8);
        buffer.write_all(b"1234").unwrap();
        buffer.write_all(b"5678").unwrap();
        assert!(buffer.write_all(b"9").is_err());
        assert_eq!(buffer.bytes, b"12345678");
        assert!(buffer.bytes.capacity() <= 8);
        let mut buffer = ResponseBuffer::new(8);
        assert!(serde_json::to_writer(&mut buffer, "\u{1}\u{2}").is_err());
        assert!(buffer.bytes.len() <= 8);
        assert!(buffer.bytes.capacity() <= 8);
    }
}
