//! Hand-rolled tokio HTTP layer for the Redshift control-plane + Data API.
//!
//! Same idiom as `services/bigquery/src/http.rs`: reads one request per
//! connection, dispatches through [`crate::http_api::Server::dispatch_http`]
//! (the `ServeHTTP` parity entry point), and writes a legacy-shaped response.
//! This is the HTTP half of legacy `Server.Run` (`runAPI`); the pgwire half
//! (`runSQL`) is started concurrently from `serve_sql` in `pgwire.rs`.

use std::collections::BTreeMap;
use std::future::Future;
use std::sync::Arc;

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use crate::server::Server;

const MAX_HEADER_BYTES: usize = 64 * 1024;
const MAX_BODY_BYTES: usize = 256 * 1024 * 1024;

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
                let (stream, _) = match accepted {
                    Ok(a) => a,
                    // Out of descriptors, or a peer gone before the accept:
                    // back off and keep serving instead of stopping (which
                    // would take every devcloud service down with it).
                    Err(e) => {
                        // Only transient errors; a broken listener still stops.
                        let transient = matches!(
                            e.kind(),
                            std::io::ErrorKind::ConnectionAborted
                                | std::io::ErrorKind::ConnectionReset
                                | std::io::ErrorKind::Interrupted
                                | std::io::ErrorKind::WouldBlock
                                | std::io::ErrorKind::TimedOut
                        ) || matches!(e.raw_os_error(), Some(12 | 23 | 24)); // ENOMEM, ENFILE, EMFILE
                        if !transient {
                            return Err(e);
                        }
                        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                        continue;
                    }
                };
                let server = Arc::clone(&server);
                tokio::spawn(async move {
                    let _ = handle_conn(stream, server).await;
                });
            }
        }
    }
}

async fn handle_conn(mut stream: TcpStream, server: Arc<Server>) -> std::io::Result<()> {
    let Some(request) = read_request(&mut stream).await? else {
        return Ok(());
    };
    let response = server.dispatch_http(
        &request.method,
        &request.path,
        &request.query,
        &request.headers,
        &request.body,
    );
    write_response(&mut stream, response).await
}

struct Request {
    method: String,
    path: String,
    query: String,
    headers: BTreeMap<String, String>,
    body: Vec<u8>,
}

async fn read_request(stream: &mut (impl AsyncRead + Unpin)) -> std::io::Result<Option<Request>> {
    let mut buf = Vec::new();
    let mut tmp = [0u8; 4096];
    let header_end = loop {
        if let Some(pos) = find_subslice(&buf, b"\r\n\r\n") {
            if pos + 4 > MAX_HEADER_BYTES {
                return Ok(None);
            }
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
    let (path, query) = match target.split_once('?') {
        Some((p, q)) => (p.to_string(), q.to_string()),
        None => (target.to_string(), String::new()),
    };

    let mut headers = BTreeMap::new();
    let mut content_length: usize = 0;
    let mut has_content_length = false;
    let mut chunked = false;
    for line in lines {
        if let Some((k, v)) = line.split_once(':') {
            let key = k.trim().to_ascii_lowercase();
            let value = v.trim().to_string();
            match key.as_str() {
                "content-length" => {
                    if has_content_length {
                        return Ok(None);
                    }
                    has_content_length = true;
                    if value.is_empty() || !value.bytes().all(|b| b.is_ascii_digit()) {
                        return Ok(None);
                    }
                    content_length = match value.parse() {
                        Ok(length) => length,
                        Err(_) => return Ok(None),
                    };
                }
                "transfer-encoding" => {
                    if chunked || !value.eq_ignore_ascii_case("chunked") {
                        return Ok(None);
                    }
                    chunked = true;
                }
                _ => {}
            }
            headers.insert(key, value);
        }
    }

    if chunked && has_content_length {
        return Ok(None);
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
                return Ok(None);
            }
            body.extend_from_slice(&tmp[..n]);
        }
        body.truncate(content_length);
    }

    Ok(Some(Request {
        method,
        path,
        query,
        headers,
        body,
    }))
}

async fn read_chunked_body(
    stream: &mut (impl AsyncRead + Unpin),
    mut buf: Vec<u8>,
) -> std::io::Result<Vec<u8>> {
    let mut out = Vec::new();
    let mut framing_bytes = 0usize;
    let mut tmp = [0u8; 4096];
    loop {
        let line = read_chunk_line(stream, &mut buf, MAX_HEADER_BYTES).await?;
        framing_bytes += line.len() + 2;
        // One-byte chunks use five framing bytes per data byte. Reserve that
        // legal overhead separately from the additional extension budget.
        if framing_bytes > 5 * out.len() + MAX_HEADER_BYTES {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "chunk framing too large",
            ));
        }
        let size_hex = line.split(|b| *b == b';').next().unwrap_or_default();
        if size_hex.is_empty() || !size_hex.iter().all(u8::is_ascii_hexdigit) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "invalid chunk size",
            ));
        }
        // ASCII validation above makes this conversion infallible. Reject a
        // huge declaration before reading its payload or adding byte offsets.
        let size =
            usize::from_str_radix(std::str::from_utf8(size_hex).unwrap(), 16).map_err(|_| {
                std::io::Error::new(std::io::ErrorKind::InvalidData, "invalid chunk size")
            })?;
        if size > MAX_BODY_BYTES - out.len() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "request body too large",
            ));
        }
        if size == 0 {
            // The zero chunk is followed by a complete, bounded trailer
            // section, ending in an empty line. EOF is never a terminator.
            let mut remaining = MAX_HEADER_BYTES;
            loop {
                let trailer = read_chunk_line(stream, &mut buf, remaining).await?;
                remaining -= trailer.len() + 2;
                framing_bytes += trailer.len() + 2;
                if framing_bytes > 5 * out.len() + MAX_HEADER_BYTES {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "chunk framing too large",
                    ));
                }
                if trailer.is_empty() {
                    return Ok(out);
                }
            }
        }
        // `size` is now bounded by MAX_BODY_BYTES, so the addition is safe.
        while buf.len() < size + 2 {
            let n = stream.read(&mut tmp).await?;
            if n == 0 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "incomplete chunk body",
                ));
            }
            buf.extend_from_slice(&tmp[..n]);
        }
        if &buf[size..size + 2] != b"\r\n" {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "invalid chunk terminator",
            ));
        }
        out.extend_from_slice(&buf[..size]);
        framing_bytes += 2;
        // Discard consumed framing as well as data, so many small chunks do
        // not retain an unbounded second copy of the request on the heap.
        buf.drain(..size + 2);
    }
}

async fn read_chunk_line(
    stream: &mut (impl AsyncRead + Unpin),
    buf: &mut Vec<u8>,
    limit: usize,
) -> std::io::Result<Vec<u8>> {
    let mut tmp = [0u8; 4096];
    loop {
        if let Some(end) = find_subslice(buf, b"\r\n") {
            if end + 2 > limit {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "chunk headers too large",
                ));
            }
            let line = buf[..end].to_vec();
            buf.drain(..end + 2);
            return Ok(line);
        }
        if buf.len() >= limit {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "chunk headers too large",
            ));
        }
        let n = stream.read(&mut tmp).await?;
        if n == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "incomplete chunk headers",
            ));
        }
        buf.extend_from_slice(&tmp[..n]);
    }
}

async fn write_response(
    stream: &mut TcpStream,
    resp: crate::http_api::HttpResponse,
) -> std::io::Result<()> {
    let body = resp.body.as_bytes();
    let mut head = format!(
        "HTTP/1.1 {} {}\r\nServer: devcloud-redshift\r\n",
        resp.status,
        reason_phrase(resp.status)
    );
    if !resp.content_type.is_empty() {
        head.push_str(&format!("Content-Type: {}\r\n", resp.content_type));
    }
    head.push_str(&format!("Content-Length: {}\r\n", body.len()));
    head.push_str("Connection: close\r\n\r\n");
    stream.write_all(head.as_bytes()).await?;
    stream.write_all(body).await?;
    stream.flush().await
}

fn reason_phrase(status: u16) -> &'static str {
    match status {
        200 => "OK",
        204 => "No Content",
        301 => "Moved Permanently",
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

#[cfg(test)]
mod request_framing_tests {
    use super::*;

    #[tokio::test]
    async fn incomplete_fixed_length_body_is_rejected() {
        // A complete JSON value is still an incomplete HTTP message. It must
        // never reach a mutating handler after the client disconnects early.
        let mut wire = &b"POST / HTTP/1.1\r\nContent-Length: 9\r\n\r\n{}"[..];
        assert!(read_request(&mut wire).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn invalid_content_length_is_rejected() {
        for value in ["bad", "-1", "+2", "", "18446744073709551616"] {
            let wire = format!("POST / HTTP/1.1\r\nContent-Length: {value}\r\n\r\n{{}}");
            assert!(
                read_request(&mut wire.as_bytes()).await.unwrap().is_none(),
                "{value:?}"
            );
        }
    }

    #[tokio::test]
    async fn ambiguous_or_unsupported_body_framing_is_rejected() {
        for headers in [
            "Transfer-Encoding: gzip\r\n",
            "Transfer-Encoding: xchunked\r\n",
            "Transfer-Encoding: chunked, gzip\r\n",
            "Content-Length: 9\r\nContent-Length: 2\r\n",
            "Content-Length: 2\r\nTransfer-Encoding: chunked\r\n",
            "Transfer-Encoding: chunked\r\nTransfer-Encoding: chunked\r\n",
        ] {
            let wire = format!("POST / HTTP/1.1\r\n{headers}\r\n{{}}");
            assert!(
                read_request(&mut wire.as_bytes()).await.unwrap().is_none(),
                "{headers:?}"
            );
        }
    }

    #[tokio::test]
    async fn complete_fixed_length_body_is_preserved() {
        let mut wire = &b"POST / HTTP/1.1\r\nContent-Length: 2\r\n\r\n{}"[..];
        let request = read_request(&mut wire).await.unwrap().unwrap();
        assert_eq!(request.body, b"{}");
    }

    #[tokio::test]
    async fn oversized_header_is_rejected_even_when_terminator_arrives() {
        let wire = format!(
            "POST / HTTP/1.1\r\nX-Padding: {}\r\n\r\n",
            "a".repeat(MAX_HEADER_BYTES)
        );
        assert!(read_request(&mut wire.as_bytes()).await.unwrap().is_none());
    }
}

#[cfg(test)]
mod chunk_framing_tests {
    use super::*;

    #[tokio::test]
    async fn malformed_or_incomplete_chunks_never_return_a_partial_body() {
        for wire in [
            &b"2\r\n{}\r\n"[..],
            &b"2\r\n{}\r\nxyz\r\n"[..],
            &b"5\r\n{}"[..],
            &b"2\r\n{}XX0\r\n\r\n"[..],
            &b"2\r\n{}\r\n0\r\n"[..],
            &b"2\r\n{}\r\n0\r\nX-Trailer: unfinished"[..],
            &b"+2\r\n{}\r\n0\r\n\r\n"[..],
            &b"\r\n"[..],
        ] {
            assert!(
                read_chunked_body(&mut &b""[..], wire.to_vec())
                    .await
                    .is_err(),
                "{wire:?}"
            );
        }
    }

    #[tokio::test]
    async fn oversized_chunk_is_rejected_before_reading_its_payload() {
        for size in [MAX_BODY_BYTES + 1, usize::MAX] {
            let wire = format!("{size:x}\r\n");
            assert!(read_chunked_body(&mut &b""[..], wire.into_bytes())
                .await
                .is_err());
        }
    }

    #[tokio::test]
    async fn chunk_lines_and_trailers_are_bounded() {
        for wire in [
            format!("1;{}", "x".repeat(MAX_HEADER_BYTES)),
            format!("0\r\nX-Trailer: {}", "x".repeat(MAX_HEADER_BYTES)),
        ] {
            assert!(read_chunked_body(&mut &b""[..], wire.into_bytes())
                .await
                .is_err());
        }
    }

    #[tokio::test]
    async fn cumulative_chunk_framing_is_bounded() {
        let chunk = format!("1;{}\r\nx\r\n", "x".repeat(MAX_HEADER_BYTES / 2));
        let wire = format!("{chunk}{chunk}{chunk}0\r\n\r\n");
        assert!(read_chunked_body(&mut wire.as_bytes(), Vec::new())
            .await
            .is_err());
    }

    #[tokio::test]
    async fn complete_chunked_request_is_preserved() {
        let mut wire =
            &b"POST / HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\n2\r\n{}\r\n0\r\n\r\n"[..];
        let request = read_request(&mut wire).await.unwrap().unwrap();
        assert_eq!(request.body, b"{}");
    }

    #[tokio::test]
    async fn many_small_chunks_are_accepted() {
        // The minimum legal data chunk carries five framing bytes per byte
        // of content. Fragmentation alone must not exhaust the extension cap.
        let wire = format!("{}0\r\n\r\n", "1\r\nx\r\n".repeat(20_000));
        let body = read_chunked_body(&mut wire.as_bytes(), Vec::new())
            .await
            .unwrap();
        assert_eq!(body, vec![b'x'; 20_000]);
    }

    #[tokio::test]
    async fn complete_chunks_extensions_and_trailers_are_decoded() {
        let wire = b"2;part=first\r\nab\r\n2\r\ncd\r\n0\r\nX-Trailer: ok\r\n\r\n";
        let body = read_chunked_body(&mut wire.as_slice(), Vec::new())
            .await
            .unwrap();
        assert_eq!(body, b"abcd");
    }
}
