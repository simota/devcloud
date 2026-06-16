//! Hand-rolled tokio HTTP layer for the BigQuery JSON API — same idiom as
//! `services/gcs/src/http.rs` / `services/s3/src/http.rs`.
//!
//! Reads one request per connection, dispatches through [`crate::routes`],
//! and writes the legacy-shaped response (`Server: devcloud-bigquery`, JSON
//! content type, `WWW-Authenticate` on 401, `Allow` on 405, `Location` on
//! resource creation).

use std::future::Future;
use std::sync::Arc;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use crate::responses::ApiResponse;
use crate::routes::{self, Request};
use crate::server::Server;
use crate::validation::Query;

const MAX_HEADER_BYTES: usize = 64 * 1024;
const MAX_BODY_BYTES: usize = 128 * 1024 * 1024;

pub async fn serve(
    listener: TcpListener,
    server: Arc<Server>,
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

async fn handle_conn(mut stream: TcpStream, server: Arc<Server>) -> std::io::Result<()> {
    let request = match read_request(&mut stream).await {
        Ok(Some(req)) => req,
        _ => return Ok(()),
    };
    let response = routes::handle(&server, &request);
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
    let (path, raw_query) = match target.split_once('?') {
        Some((p, q)) => (p.to_string(), q.to_string()),
        None => (target.to_string(), String::new()),
    };

    let mut authorization = String::new();
    let mut content_type = String::new();
    let mut content_length: usize = 0;
    let mut chunked = false;
    for line in lines {
        if let Some((k, v)) = line.split_once(':') {
            let value = v.trim();
            match k.trim().to_ascii_lowercase().as_str() {
                "authorization" => authorization = value.to_string(),
                "content-type" => content_type = value.to_string(),
                "content-length" => content_length = value.parse().unwrap_or(0),
                "transfer-encoding" => {
                    chunked = value.to_ascii_lowercase().contains("chunked");
                }
                _ => {}
            }
        }
    }

    let mut body = buf[header_end + 4..].to_vec();
    if chunked {
        body = read_chunked_body(stream, body).await?;
    } else {
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
        query: Query::parse(&raw_query),
        authorization,
        content_type,
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
        let size_line = String::from_utf8_lossy(&buf[pos..line_end]).into_owned();
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

async fn write_response(stream: &mut TcpStream, resp: ApiResponse) -> std::io::Result<()> {
    let mut head = format!(
        "HTTP/1.1 {} {}\r\nServer: devcloud-bigquery\r\n",
        resp.status,
        reason_phrase(resp.status)
    );
    if resp.www_authenticate {
        head.push_str("WWW-Authenticate: Bearer realm=\"devcloud-bigquery\"\r\n");
    }
    if let Some(allow) = &resp.allow {
        head.push_str(&format!("Allow: {allow}\r\n"));
    }
    if let Some(location) = &resp.location {
        head.push_str(&format!("Location: {location}\r\n"));
    }
    if !resp.body.is_empty() {
        head.push_str("Content-Type: application/json; charset=utf-8\r\n");
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
    haystack.windows(needle.len()).position(|w| w == needle)
}
