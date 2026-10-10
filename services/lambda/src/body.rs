//! Request-body reader for the hand-rolled HTTP/1.1 front-end.
//!
//! Handles `Content-Length` and `Transfer-Encoding: chunked`. Anything it
//! cannot read faithfully — another transfer coding, a malformed chunk, a body
//! cut short — is an explicit [`BodyError`], never a silently shorter body
//! (an empty Invoke payload would run the handler with `{}`).

use std::collections::HashMap;

use tokio::io::{AsyncRead, AsyncReadExt};

#[derive(Debug, PartialEq, Eq)]
pub enum BodyError {
    TooLarge,
    Malformed(&'static str),
    UnsupportedEncoding(String),
    /// The peer closed the connection before the declared body arrived.
    Truncated,
}

/// Reads the body that follows a request head. `leftover` holds bytes that
/// arrived together with the head; `headers` keys are lowercase.
pub async fn read_body<R: AsyncRead + Unpin>(
    stream: &mut R,
    leftover: Vec<u8>,
    headers: &HashMap<String, String>,
    max: usize,
) -> std::io::Result<Result<Vec<u8>, BodyError>> {
    let mut input = Input {
        stream,
        buf: leftover,
        pos: 0,
    };
    if let Some(te) = headers.get("transfer-encoding") {
        // Transfer-Encoding wins over Content-Length (RFC 9112 §6.3).
        let codings: Vec<String> = te
            .split(',')
            .map(|c| c.trim().to_ascii_lowercase())
            .collect();
        if codings != ["chunked"] {
            return Ok(Err(BodyError::UnsupportedEncoding(te.clone())));
        }
        return input.chunked(max).await;
    }
    let length = match headers.get("content-length") {
        None => 0,
        Some(v) => match v.trim().parse::<usize>() {
            Ok(n) => n,
            Err(_) => return Ok(Err(BodyError::Malformed("invalid Content-Length"))),
        },
    };
    if length > max {
        return Ok(Err(BodyError::TooLarge));
    }
    input.exact(length).await
}

struct Input<'a, R> {
    stream: &'a mut R,
    buf: Vec<u8>,
    pos: usize,
}

impl<R: AsyncRead + Unpin> Input<'_, R> {
    /// Ensures at least `n` unread bytes are buffered; `false` on EOF.
    async fn fill(&mut self, n: usize) -> std::io::Result<bool> {
        let mut tmp = [0u8; 8192];
        if self.buf.len() - self.pos < n && self.pos > 0 {
            // Drop consumed bytes so the buffer never holds more than the
            // unread tail plus one read.
            self.buf.drain(..self.pos);
            self.pos = 0;
        }
        while self.buf.len() - self.pos < n {
            let read = self.stream.read(&mut tmp).await?;
            if read == 0 {
                return Ok(false);
            }
            self.buf.extend_from_slice(&tmp[..read]);
        }
        Ok(true)
    }

    async fn exact(&mut self, n: usize) -> std::io::Result<Result<Vec<u8>, BodyError>> {
        if !self.fill(n).await? {
            return Ok(Err(BodyError::Truncated));
        }
        let out = self.buf[self.pos..self.pos + n].to_vec();
        self.pos += n;
        Ok(Ok(out))
    }

    /// One CRLF-terminated line (without the CRLF), bounded in length.
    async fn line(&mut self) -> std::io::Result<Result<String, BodyError>> {
        const MAX_LINE: usize = 8 * 1024;
        loop {
            if let Some(i) = self.buf[self.pos..].windows(2).position(|w| w == b"\r\n") {
                let line = String::from_utf8_lossy(&self.buf[self.pos..self.pos + i]).into_owned();
                self.pos += i + 2;
                return Ok(Ok(line));
            }
            if self.buf.len() - self.pos > MAX_LINE {
                return Ok(Err(BodyError::Malformed("chunk line too long")));
            }
            let pending = self.buf.len() - self.pos + 1;
            if !self.fill(pending).await? {
                return Ok(Err(BodyError::Truncated));
            }
        }
    }

    /// [`Self::line`], charging its bytes to `framing` and failing once the
    /// framing exceeds the decoded body by more than `allowance`.
    async fn framing_line(
        &mut self,
        framing: &mut usize,
        body_len: usize,
        allowance: usize,
    ) -> std::io::Result<Result<String, BodyError>> {
        let line = self.line().await?;
        if let Ok(l) = &line {
            *framing = framing.saturating_add(l.len() + 2);
            if *framing > body_len.saturating_add(allowance) {
                return Ok(Err(BodyError::Malformed("chunk framing too large")));
            }
        }
        Ok(line)
    }

    async fn chunked(&mut self, max: usize) -> std::io::Result<Result<Vec<u8>, BodyError>> {
        /// Framing bytes (chunk-size lines, extensions, trailers) allowed on
        /// top of the decoded body size. Honest encoders stay far below the
        /// body itself; this stops a peer from streaming unbounded framing.
        const FRAMING_ALLOWANCE: usize = 64 * 1024;
        let mut body = Vec::new();
        let mut framing = 0usize;
        loop {
            let line = match self
                .framing_line(&mut framing, body.len(), FRAMING_ALLOWANCE)
                .await?
            {
                Ok(l) => l,
                Err(e) => return Ok(Err(e)),
            };
            let size_hex = line.split(';').next().unwrap_or("").trim();
            let Ok(size) = usize::from_str_radix(size_hex, 16) else {
                return Ok(Err(BodyError::Malformed("invalid chunk size")));
            };
            if size_hex.is_empty() || size_hex.starts_with('+') {
                return Ok(Err(BodyError::Malformed("invalid chunk size")));
            }
            if size == 0 {
                // Trailer section: header lines until an empty one.
                loop {
                    match self
                        .framing_line(&mut framing, body.len(), FRAMING_ALLOWANCE)
                        .await?
                    {
                        Ok(l) if l.is_empty() => return Ok(Ok(body)),
                        Ok(_) => {}
                        Err(e) => return Ok(Err(e)),
                    }
                }
            }
            if body.len().saturating_add(size) > max {
                return Ok(Err(BodyError::TooLarge));
            }
            match self.exact(size).await? {
                Ok(chunk) => body.extend_from_slice(&chunk),
                Err(e) => return Ok(Err(e)),
            }
            match self.exact(2).await? {
                Ok(crlf) if crlf == b"\r\n" => {}
                Ok(_) => return Ok(Err(BodyError::Malformed("chunk not terminated by CRLF"))),
                Err(e) => return Ok(Err(e)),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn headers(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    async fn read(
        leftover: &[u8],
        rest: &[u8],
        h: &[(&str, &str)],
        max: usize,
    ) -> Result<Vec<u8>, BodyError> {
        let mut stream = rest;
        read_body(&mut stream, leftover.to_vec(), &headers(h), max)
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn decodes_chunked_bodies_split_across_reads() {
        let te = [("transfer-encoding", "chunked")];
        let body = read(
            b"8\r\n{\"val",
            b"ue\"\r\n6;ext=1\r\n:1}   \r\n0\r\nX-Trailer: t\r\n\r\n",
            &te,
            1024,
        )
        .await;
        assert_eq!(body.unwrap(), b"{\"value\":1}   ");
    }

    #[tokio::test]
    async fn content_length_and_errors() {
        assert_eq!(
            read(b"ab", b"cd", &[("content-length", "4")], 10)
                .await
                .unwrap(),
            b"abcd"
        );
        assert_eq!(read(b"", b"", &[], 10).await.unwrap(), b"");
        assert_eq!(
            read(b"ab", b"", &[("content-length", "4")], 10).await,
            Err(BodyError::Truncated)
        );
        assert_eq!(
            read(b"", b"", &[("content-length", "11")], 10).await,
            Err(BodyError::TooLarge)
        );
        assert!(matches!(
            read(b"", b"", &[("content-length", "x")], 10).await,
            Err(BodyError::Malformed(_))
        ));
        let gzip = [("transfer-encoding", "gzip, chunked")];
        assert!(matches!(
            read(b"", b"", &gzip, 10).await,
            Err(BodyError::UnsupportedEncoding(_))
        ));
        let te = [("transfer-encoding", "chunked")];
        assert!(matches!(
            read(b"zz\r\n", b"", &te, 10).await,
            Err(BodyError::Malformed(_))
        ));
        assert_eq!(
            read(b"5\r\nab", b"", &te, 10).await,
            Err(BodyError::Truncated)
        );
        assert_eq!(read(b"b\r\n", b"", &te, 10).await, Err(BodyError::TooLarge));
        // Endless trailers (or chunk extensions) cannot grow without bound.
        let trailers = format!("0\r\n{}", "X: aaaaaaaa\r\n".repeat(10_000));
        assert_eq!(
            read(trailers.as_bytes(), b"", &te, 10).await,
            Err(BodyError::Malformed("chunk framing too large"))
        );
        // Transfer-Encoding wins over a conflicting Content-Length.
        let both = [("transfer-encoding", "chunked"), ("content-length", "1")];
        assert_eq!(
            read(b"2\r\nok\r\n0\r\n\r\n", b"", &both, 10).await.unwrap(),
            b"ok"
        );
    }
}
