//! Parser for the `aws-chunked` body framing used by `STREAMING-*` payloads:
//! `<hex-size>[;chunk-signature=<sig>]\r\n<data>\r\n`, a zero-size terminal chunk,
//! then optional `name:value\r\n` trailers.

pub struct Chunk<'a> {
    pub signature: Option<&'a str>,
    pub data: &'a [u8],
}

pub struct ChunkedBody<'a> {
    /// Every chunk in order, including the empty terminal chunk (it is signed too).
    pub chunks: Vec<Chunk<'a>>,
    pub trailers: Vec<(&'a str, &'a str)>,
    pub trailer_signature: Option<&'a str>,
}

impl ChunkedBody<'_> {
    pub fn payload(&self) -> Vec<u8> {
        self.chunks.iter().flat_map(|c| c.data).copied().collect()
    }
}

/// Returns `None` when the framing is malformed, the closing blank line is
/// missing, or bytes follow it: none of those are covered by a signature.
pub fn parse(body: &[u8]) -> Option<ChunkedBody<'_>> {
    let mut rest = body;
    let mut chunks = Vec::new();
    loop {
        let line = take_line(&mut rest)?;
        let (size, extension) = line.split_once(';').unwrap_or((line, ""));
        let size = usize::from_str_radix(size.trim(), 16).ok()?;
        let signature = extension.trim().strip_prefix("chunk-signature=");
        let data = rest.get(..size)?;
        rest = &rest[size..];
        chunks.push(Chunk { signature, data });
        if size == 0 {
            break;
        }
        rest = rest.strip_prefix(b"\r\n")?;
    }
    let mut trailers = Vec::new();
    let mut trailer_signature = None;
    loop {
        let line = take_line(&mut rest)?;
        if line.is_empty() {
            break;
        }
        let (name, value) = line.split_once(':')?;
        if name.trim().eq_ignore_ascii_case("x-amz-trailer-signature") {
            trailer_signature = Some(value.trim());
        } else {
            trailers.push((name.trim(), value.trim()));
        }
    }
    if !rest.is_empty() {
        return None;
    }
    Some(ChunkedBody {
        chunks,
        trailers,
        trailer_signature,
    })
}

fn take_line<'a>(rest: &mut &'a [u8]) -> Option<&'a str> {
    let end = rest.windows(2).position(|w| w == b"\r\n")?;
    let line = std::str::from_utf8(&rest[..end]).ok()?;
    *rest = &rest[end + 2..];
    Some(line)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_signed_chunks_and_trailers() {
        let body = b"5;chunk-signature=aa\r\nhello\r\n1;chunk-signature=bb\r\n!\r\n0;chunk-signature=cc\r\nx-amz-checksum-crc32:AAAA\r\nx-amz-trailer-signature:dd\r\n\r\n";
        let parsed = parse(body).unwrap();
        assert_eq!(parsed.payload(), b"hello!");
        assert_eq!(parsed.chunks.len(), 3);
        assert_eq!(parsed.chunks[2].signature, Some("cc"));
        assert_eq!(parsed.trailers, vec![("x-amz-checksum-crc32", "AAAA")]);
        assert_eq!(parsed.trailer_signature, Some("dd"));
    }

    #[test]
    fn parses_unsigned_body_without_trailers() {
        let parsed = parse(b"3\r\nabc\r\n0\r\n\r\n").unwrap();
        assert_eq!(parsed.payload(), b"abc");
        assert!(parsed.trailers.is_empty());
    }

    #[test]
    fn rejects_truncated_or_invalid_framing() {
        for body in [
            &b"5\r\nhel"[..],
            b"5\r\nhello\r\n",
            b"zz\r\nhello\r\n0\r\n\r\n",
            b"5\r\nhelloXX0\r\n\r\n",
            b"ffffffffffffffff\r\nx\r\n",
            b"plain body",
            b"3\r\nabc\r\n0\r\n",
            b"3\r\nabc\r\n0\r\nx-amz-checksum-crc32:AAAA\r\n",
            b"3\r\nabc\r\n0\r\n\r\ntrailing",
        ] {
            assert!(parse(body).is_none(), "{:?}", String::from_utf8_lossy(body));
        }
    }
}
