//! Hand-rolled HTTP/1.1 forwarding client — the core primitive every
//! `/api/<svc>/*` handler uses to reach a service's Phase-1 `/_introspect/` and
//! `/_control/` endpoints (and the providers' own protocols, where the legacy
//! dashboard forwards via `ServeHTTP`).
//!
//! It is deliberately minimal (plain tokio TCP, no client framework, matching
//! the crate's hand-rolled server) because the dashboard only ever talks to
//! loopback services it controls. It speaks `Connection: close` and reads the
//! full response by Content-Length (the service HTTP servers always set it; see
//! `services/sqs/src/http.rs` `response_head`).

use std::collections::HashMap;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// A forwarded request to a downstream service.
pub struct ForwardRequest<'a> {
    /// Base URL of the target service, e.g. `http://127.0.0.1:19324`.
    pub base: &'a str,
    pub method: &'a str,
    /// Request target path, e.g. `/_introspect/queues` or `/`.
    pub path: &'a str,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

/// The relayed response from a downstream service.
pub struct ForwardResponse {
    pub status: u16,
    pub headers: HashMap<String, String>,
    pub body: Vec<u8>,
}

impl ForwardResponse {
    pub fn header(&self, name: &str) -> &str {
        self.headers
            .get(&name.to_ascii_lowercase())
            .map(String::as_str)
            .unwrap_or("")
    }
}

/// Errors a forward can fail with. `Unreachable` covers connect/IO failures
/// (the service is down or the addr is wrong); the dashboard maps it to 502.
#[derive(Debug)]
pub enum ForwardError {
    /// The base URL was empty or could not be parsed into host:port.
    BadBase,
    /// The connection or read/write failed.
    Unreachable(std::io::Error),
    /// The response could not be parsed as HTTP/1.1.
    BadResponse,
}

const MAX_RESPONSE_BYTES: usize = 32 * 1024 * 1024;

/// Performs a single HTTP/1.1 request to `req.base + req.path` and returns the
/// parsed response. Connection is one-shot (`Connection: close`).
pub async fn forward(req: ForwardRequest<'_>) -> Result<ForwardResponse, ForwardError> {
    let (host_port, host_header) = parse_authority(req.base).ok_or(ForwardError::BadBase)?;

    let mut stream = TcpStream::connect(&host_port)
        .await
        .map_err(ForwardError::Unreachable)?;

    let mut head = format!("{} {} HTTP/1.1\r\n", req.method, req.path);
    head.push_str(&format!("Host: {host_header}\r\n"));
    head.push_str("Connection: close\r\n");
    // Content-Length / Host / Connection are managed here; callers cannot
    // override them.
    for (name, value) in &req.headers {
        if name.eq_ignore_ascii_case("content-length")
            || name.eq_ignore_ascii_case("host")
            || name.eq_ignore_ascii_case("connection")
        {
            continue;
        }
        head.push_str(&format!("{name}: {value}\r\n"));
    }
    head.push_str(&format!("Content-Length: {}\r\n\r\n", req.body.len()));

    stream
        .write_all(head.as_bytes())
        .await
        .map_err(ForwardError::Unreachable)?;
    if !req.body.is_empty() {
        stream
            .write_all(&req.body)
            .await
            .map_err(ForwardError::Unreachable)?;
    }
    stream.flush().await.map_err(ForwardError::Unreachable)?;

    read_response(&mut stream).await
}

async fn read_response(stream: &mut TcpStream) -> Result<ForwardResponse, ForwardError> {
    let mut buf = Vec::new();
    let mut tmp = [0u8; 4096];
    let header_end = loop {
        if let Some(pos) = find_subslice(&buf, b"\r\n\r\n") {
            break pos;
        }
        if buf.len() > 64 * 1024 {
            return Err(ForwardError::BadResponse);
        }
        let n = stream
            .read(&mut tmp)
            .await
            .map_err(ForwardError::Unreachable)?;
        if n == 0 {
            return Err(ForwardError::BadResponse);
        }
        buf.extend_from_slice(&tmp[..n]);
    };

    let head = String::from_utf8_lossy(&buf[..header_end]).into_owned();
    let mut lines = head.split("\r\n");
    let status_line = lines.next().unwrap_or("");
    // "HTTP/1.1 200 OK" -> 200
    let status = status_line
        .split(' ')
        .nth(1)
        .and_then(|s| s.parse::<u16>().ok())
        .ok_or(ForwardError::BadResponse)?;

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

    let mut body = buf[header_end + 4..].to_vec();
    if content_length > MAX_RESPONSE_BYTES {
        return Err(ForwardError::BadResponse);
    }
    if headers.contains_key("content-length") {
        while body.len() < content_length {
            let n = stream
                .read(&mut tmp)
                .await
                .map_err(ForwardError::Unreachable)?;
            if n == 0 {
                break;
            }
            body.extend_from_slice(&tmp[..n]);
        }
        body.truncate(content_length);
    } else {
        // No Content-Length: read until the peer closes (Connection: close).
        loop {
            let n = stream
                .read(&mut tmp)
                .await
                .map_err(ForwardError::Unreachable)?;
            if n == 0 {
                break;
            }
            if body.len() + n > MAX_RESPONSE_BYTES {
                return Err(ForwardError::BadResponse);
            }
            body.extend_from_slice(&tmp[..n]);
        }
    }

    Ok(ForwardResponse {
        status,
        headers,
        body,
    })
}

/// Splits a base URL like `http://127.0.0.1:19324` into a `connect` target
/// (`127.0.0.1:19324`) and a `Host` header value. Only `http://` is supported
/// (every devcloud service base is loopback HTTP).
fn parse_authority(base: &str) -> Option<(String, String)> {
    let trimmed = base.strip_prefix("http://")?;
    // Drop any path component; we only want the authority.
    let authority = trimmed.split('/').next().unwrap_or("");
    if authority.is_empty() {
        return None;
    }
    let connect = if authority.contains(':') {
        authority.to_string()
    } else {
        format!("{authority}:80")
    };
    Some((connect, authority.to_string()))
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

    #[test]
    fn parse_authority_with_port() {
        let (c, h) = parse_authority("http://127.0.0.1:19324").unwrap();
        assert_eq!(c, "127.0.0.1:19324");
        assert_eq!(h, "127.0.0.1:19324");
    }

    #[test]
    fn parse_authority_strips_path() {
        let (c, _) = parse_authority("http://127.0.0.1:19324/ignored").unwrap();
        assert_eq!(c, "127.0.0.1:19324");
    }

    #[test]
    fn parse_authority_rejects_non_http() {
        assert!(parse_authority("ws://127.0.0.1:18027").is_none());
        assert!(parse_authority("").is_none());
    }
}
