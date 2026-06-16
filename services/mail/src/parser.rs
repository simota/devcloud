//! Mirrors `internal/services/mail/parser.rs`.
//!
//! legacy uses `net/mail` + `mime` + `mime/multipart`. We hand-roll the minimal
//! equivalents so behavior matches byte-for-byte for the cases the service
//! relies on:
//!   * RFC 5322 header parsing with continuation folding; a non-continuation
//!     line without a colon is a parse error (mirrors `mail.ReadMessage`).
//!   * Canonical MIME header keys (mirrors `textproto.CanonicalMIMEHeaderKey`).
//!   * `mime.ParseMediaType` for `Content-Type`.
//!   * `multipart` body walking for `multipart/*`.

use crate::model::{Envelope, Message};

/// Mirrors `mail.ParseMessage(raw, envelope)`.
pub fn parse_message(raw: &[u8], envelope: &Envelope) -> Message {
    let mut msg = Message {
        from: envelope.from.clone(),
        to: envelope.to.clone(),
        ..Default::default()
    };

    let (headers, body) = match parse_headers(raw) {
        Ok(parsed) => parsed,
        Err(e) => {
            msg.parse_error = e;
            return msg;
        }
    };

    for (key, value) in &headers {
        msg.headers
            .entry(key.clone())
            .or_default()
            .push(value.clone());
    }

    msg.subject = header_get(&headers, "Subject");
    let from = header_get(&headers, "From");
    if !from.is_empty() {
        msg.from = from;
    }
    let to = header_get(&headers, "To");
    if !to.is_empty() && msg.to.is_empty() {
        msg.to = vec![to];
    }

    let content_type = header_get(&headers, "Content-Type");
    fill_message_body(&mut msg, &content_type, body);
    msg
}

/// Header list preserving order; keys are canonicalized. `body` is the offset of
/// the body within `raw`.
type Headers = Vec<(String, String)>;

fn parse_headers(raw: &[u8]) -> Result<(Headers, &[u8]), String> {
    let mut headers: Headers = Vec::new();
    let mut i = 0usize;
    loop {
        let start = i;
        while i < raw.len() && raw[i] != b'\n' {
            i += 1;
        }
        let mut content_end = i; // exclusive; points at '\n' or EOF
        if content_end > start && raw[content_end - 1] == b'\r' {
            content_end -= 1;
        }
        if i < raw.len() {
            i += 1; // consume '\n'
        }
        let line = &raw[start..content_end];

        if line.is_empty() {
            // Blank line terminates the header block; body is the remainder.
            return Ok((headers, &raw[i..]));
        }

        if line[0] == b' ' || line[0] == b'\t' {
            // Continuation of the previous header value (folding).
            match headers.last_mut() {
                Some(last) => {
                    let cont = String::from_utf8_lossy(line);
                    last.1.push(' ');
                    last.1.push_str(cont.trim_start());
                    continue;
                }
                None => return Err("malformed MIME header line".to_string()),
            }
        }

        match line.iter().position(|&b| b == b':') {
            None => return Err("malformed MIME header line".to_string()),
            Some(c) => {
                let key = String::from_utf8_lossy(&line[..c]).to_string();
                let value = String::from_utf8_lossy(&line[c + 1..])
                    .trim_start()
                    .to_string();
                headers.push((canonical_mime_header_key(&key), value));
            }
        }

        if start >= raw.len() {
            // Reached EOF without a blank line; treat the rest as no body.
            return Ok((headers, &raw[raw.len()..]));
        }
    }
}

/// First value for a canonical header key (mirrors `Header.Get`).
fn header_get(headers: &Headers, key: &str) -> String {
    let canonical = canonical_mime_header_key(key);
    headers
        .iter()
        .find(|(k, _)| *k == canonical)
        .map(|(_, v)| v.clone())
        .unwrap_or_default()
}

/// Mirrors `textproto.CanonicalMIMEHeaderKey`: upper-case the first letter and
/// any letter following '-', lower-case the rest.
fn canonical_mime_header_key(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut upper = true;
    for &b in s.as_bytes() {
        let mut c = b;
        if upper {
            if c.is_ascii_lowercase() {
                c -= 32;
            }
        } else if c.is_ascii_uppercase() {
            c += 32;
        }
        out.push(c as char);
        upper = c == b'-';
    }
    out
}

fn fill_message_body(msg: &mut Message, content_type: &str, body: &[u8]) {
    if let Some((media_type, params)) = parse_media_type(content_type) {
        if media_type.starts_with("multipart/") {
            if let Some(boundary) = params_get(&params, "boundary") {
                read_multipart_body(msg, &boundary, body);
            }
            return;
        }
        let data = String::from_utf8_lossy(body).to_string();
        match media_type.as_str() {
            "text/html" => msg.html_body = data,
            _ => msg.text_body = data,
        }
        return;
    }
    // Unparseable Content-Type → default to text body (mirrors legacy err==nil gate
    // falling through to the plain read with empty mediaType).
    msg.text_body = String::from_utf8_lossy(body).to_string();
}

fn fill_message_body_part(msg: &mut Message, part: &[u8]) {
    let (headers, body) = match parse_headers(part) {
        Ok(parsed) => parsed,
        Err(_) => return,
    };
    let content_type = header_get(&headers, "Content-Type");
    if let Some((media_type, params)) = parse_media_type(&content_type) {
        if media_type.starts_with("multipart/") {
            if let Some(boundary) = params_get(&params, "boundary") {
                read_multipart_body(msg, &boundary, body);
            }
            return;
        }
        let data = String::from_utf8_lossy(body).to_string();
        match media_type.as_str() {
            "text/html" => {
                if msg.html_body.is_empty() {
                    msg.html_body = data;
                }
            }
            "text/plain" | "" => {
                if msg.text_body.is_empty() {
                    msg.text_body = data;
                }
            }
            _ => {}
        }
    } else {
        // No/invalid Content-Type on the part → treated as text/plain ("").
        let data = String::from_utf8_lossy(body).to_string();
        if msg.text_body.is_empty() {
            msg.text_body = data;
        }
    }
}

fn read_multipart_body(msg: &mut Message, boundary: &str, body: &[u8]) {
    if boundary.is_empty() {
        return;
    }
    let dash = format!("--{}", boundary);
    let close = format!("--{}--", boundary);

    let mut in_part = false;
    let mut first_line = true;
    let mut part_buf: Vec<u8> = Vec::new();

    for line in split_lines(body) {
        let as_str = String::from_utf8_lossy(line);
        // RFC 2046 allows trailing whitespace after the boundary line.
        let trimmed = as_str.trim_end();
        if trimmed == dash || trimmed == close {
            if in_part {
                let part = std::mem::take(&mut part_buf);
                fill_message_body_part(msg, &part);
            }
            if trimmed == close {
                return;
            }
            in_part = true;
            first_line = true;
            part_buf = Vec::new();
            continue;
        }
        if in_part {
            // Join lines with CRLF so the trailing CRLF before the next
            // boundary is naturally excluded from the part body.
            if !first_line {
                part_buf.extend_from_slice(b"\r\n");
            }
            part_buf.extend_from_slice(line);
            first_line = false;
        }
    }
}

/// Splits `data` into line contents (terminators stripped), preserving inner
/// bytes. A trailing chunk without a newline is still returned as a line.
fn split_lines(data: &[u8]) -> Vec<&[u8]> {
    let mut lines = Vec::new();
    let mut start = 0usize;
    let mut i = 0usize;
    while i < data.len() {
        if data[i] == b'\n' {
            let mut end = i;
            if end > start && data[end - 1] == b'\r' {
                end -= 1;
            }
            lines.push(&data[start..end]);
            i += 1;
            start = i;
        } else {
            i += 1;
        }
    }
    if start < data.len() {
        lines.push(&data[start..]);
    }
    lines
}

/// Mirrors `mime.ParseMediaType` for the subset we use: returns the lower-cased
/// media type and its parameters, or `None` when the type is empty/invalid.
fn parse_media_type(value: &str) -> Option<(String, Vec<(String, String)>)> {
    let value = value.trim();
    if value.is_empty() {
        return None;
    }
    let segments = split_semicolons(value);
    let media_type = segments[0].trim().to_ascii_lowercase();
    if media_type.is_empty() || !media_type.contains('/') {
        return None;
    }
    let mut params = Vec::new();
    for raw in &segments[1..] {
        let raw = raw.trim();
        if raw.is_empty() {
            continue;
        }
        if let Some((k, v)) = raw.split_once('=') {
            let key = k.trim().to_ascii_lowercase();
            let val = unquote(v.trim());
            params.push((key, val));
        }
    }
    Some((media_type, params))
}

/// Splits on `;` that are not inside a double-quoted string, so a quoted
/// parameter such as `boundary="a;b"` survives as a single segment (mirrors
/// `mime.ParseMediaType`, which is quote-aware).
fn split_semicolons(s: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut cur = String::new();
    let mut in_quotes = false;
    let mut chars = s.chars();
    while let Some(c) = chars.next() {
        match c {
            '"' => {
                in_quotes = !in_quotes;
                cur.push(c);
            }
            '\\' if in_quotes => {
                cur.push(c);
                if let Some(n) = chars.next() {
                    cur.push(n);
                }
            }
            ';' if !in_quotes => out.push(std::mem::take(&mut cur)),
            _ => cur.push(c),
        }
    }
    out.push(cur);
    out
}

fn params_get(params: &[(String, String)], key: &str) -> Option<String> {
    params
        .iter()
        .find(|(k, _)| k == key)
        .map(|(_, v)| v.clone())
}

fn unquote(s: &str) -> String {
    if s.len() >= 2 && s.starts_with('"') && s.ends_with('"') {
        let inner = &s[1..s.len() - 1];
        let mut out = String::with_capacity(inner.len());
        let mut chars = inner.chars();
        while let Some(c) = chars.next() {
            if c == '\\' {
                if let Some(n) = chars.next() {
                    out.push(n);
                }
            } else {
                out.push(c);
            }
        }
        out
    } else {
        s.to_string()
    }
}
