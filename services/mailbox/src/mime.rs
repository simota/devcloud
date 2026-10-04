//! Bounded, forgiving MIME decoding for display. Compatibility parsing lives
//! separately in mailhog.rs because MailHog's part numbering is different.
use std::collections::BTreeMap;

use encoding_rs::{Encoding, REPLACEMENT};

pub const MAX_DEPTH: usize = 32;
pub const MAX_PARTS: usize = 1000;
pub const MAX_HEADER_BYTES: usize = 1024 * 1024;
pub const MAX_HEADER_FIELDS: usize = 10_000;
pub type Headers = Vec<(String, String)>;

#[derive(Debug, Default)]
pub struct ParsedMail {
    pub headers: Headers,
    pub text: String,
    pub html: Option<String>,
    pub attachments: Vec<Attachment>,
    pub warnings: Vec<String>,
}

#[derive(Debug)]
pub struct Attachment {
    pub filename: String,
    pub content_type: String,
    pub bytes: Vec<u8>,
    pub content_id: String,
}

pub fn warn(warnings: &mut Vec<String>, message: &str) {
    if !warnings.iter().any(|w| w == message) {
        warnings.push(message.to_string());
    }
}

/// Isolate a malformed message without putting parsing under any store lock.
pub fn parse(raw: &[u8]) -> ParsedMail {
    std::panic::catch_unwind(|| {
        let mut mail = ParsedMail::default();
        let mut count = 0;
        walk(raw, 0, &mut count, &mut mail);
        mail
    })
    .unwrap_or_else(|_| ParsedMail {
        warnings: vec!["MIME parsing failed; source is still available".into()],
        ..Default::default()
    })
}

pub fn header<'a>(headers: &'a Headers, name: &str) -> &'a str {
    headers
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case(name))
        .map(|(_, v)| v.as_str())
        .unwrap_or("")
}

/// Scan to the body even when the retained header block is truncated. Offsets
/// refer to the original bytes, so decoding never changes multipart framing.
pub fn headers<'a>(raw: &'a [u8], warnings: &mut Vec<String>) -> (Headers, &'a [u8]) {
    let mut out: Headers = Vec::new();
    let mut offset = 0;
    let mut fields = 0;
    while offset < raw.len() {
        let start = offset;
        let end = raw[offset..]
            .iter()
            .position(|&b| b == b'\n')
            .map_or(raw.len(), |p| offset + p);
        offset = (end + 1).min(raw.len());
        let line = raw[start..end]
            .strip_suffix(b"\r")
            .unwrap_or(&raw[start..end]);
        if line.is_empty() {
            return (out, &raw[offset..]);
        }
        if end > MAX_HEADER_BYTES || fields >= MAX_HEADER_FIELDS {
            warn(warnings, "MIME headers truncated at size/field limit");
            continue;
        }
        if line.starts_with(b" ") || line.starts_with(b"\t") {
            if let Some((_, value)) = out.last_mut() {
                value.push_str(&String::from_utf8_lossy(line));
            } else {
                warn(warnings, "Malformed folded MIME header");
            }
        } else if let Some(colon) = line.iter().position(|&b| b == b':') {
            fields += 1;
            out.push((
                String::from_utf8_lossy(&line[..colon]).into_owned(),
                String::from_utf8_lossy(&line[colon + 1..])
                    .trim()
                    .to_string(),
            ));
        } else {
            warn(warnings, "Malformed MIME header; treated as body");
            return (out, &raw[start..]);
        }
    }
    (out, &raw[raw.len()..])
}

fn walk(raw: &[u8], depth: usize, count: &mut usize, mail: &mut ParsedMail) {
    if *count >= MAX_PARTS {
        warn(&mut mail.warnings, "MIME part limit reached");
        return;
    }
    *count += 1;
    let (headers, body) = headers(raw, &mut mail.warnings);
    if depth == 0 {
        mail.headers = headers.clone();
    }
    let (mut kind, params) = media_type(header(&headers, "Content-Type"));
    let (disposition, disp_params) = media_type(header(&headers, "Content-Disposition"));
    let filename = parameter(&disp_params, "filename", &mut mail.warnings)
        .or_else(|| parameter(&params, "name", &mut mail.warnings));
    if kind.starts_with("multipart/") {
        if depth >= MAX_DEPTH {
            warn(
                &mut mail.warnings,
                "MIME nesting limit reached; opaque leaf",
            );
            kind = "application/octet-stream".into();
        } else if let Some(boundary) = params.get("boundary").filter(|b| !b.is_empty()) {
            let remaining = MAX_PARTS.saturating_sub(*count);
            let (parts, closed, capped) = multipart_parts(body, boundary, remaining);
            if !closed {
                warn(&mut mail.warnings, "Unterminated multipart body");
            }
            if capped {
                warn(&mut mail.warnings, "MIME part limit reached");
            }
            for part in parts {
                walk(part, depth + 1, count, mail);
            }
            return;
        } else {
            warn(
                &mut mail.warnings,
                "Multipart boundary missing; treated as text/plain",
            );
            kind = "text/plain".into();
        }
    }
    let bytes = transfer_decode(
        body,
        header(&headers, "Content-Transfer-Encoding"),
        &mut mail.warnings,
    );
    let cid = header(&headers, "Content-ID")
        .trim()
        .trim_matches(['<', '>'])
        .to_string();
    if disposition == "attachment"
        || filename.is_some()
        || !matches!(kind.as_str(), "text/plain" | "text/html")
    {
        mail.attachments.push(Attachment {
            filename: filename.unwrap_or_else(|| format!("attachment-{}", mail.attachments.len())),
            content_type: kind,
            bytes,
            content_id: cid,
        });
    } else {
        let label = params.get("charset").map_or("utf-8", String::as_str);
        let text = charset_decode(&bytes, label, &mut mail.warnings);
        if kind == "text/html" {
            if mail.html.is_none() {
                mail.html = Some(text);
            }
        } else if mail.text.is_empty() {
            mail.text = text;
        }
    }
}

/// Match delimiter lines (RFC 2046), excluding preamble, epilogue and the CRLF
/// owned by the following boundary. Keep an unterminated final part.
pub fn multipart_parts<'a>(
    body: &'a [u8],
    boundary: &str,
    cap: usize,
) -> (Vec<&'a [u8]>, bool, bool) {
    let marker = format!("--{boundary}");
    let mut parts = Vec::new();
    let mut offset = 0;
    let mut part_start = None;
    while offset < body.len() {
        let start = offset;
        let end = body[start..]
            .iter()
            .position(|&b| b == b'\n')
            .map_or(body.len(), |i| start + i);
        offset = (end + 1).min(body.len());
        let line = body[start..end]
            .strip_suffix(b"\r")
            .unwrap_or(&body[start..end]);
        let line = line.trim_ascii_end();
        let Some(suffix) = line.strip_prefix(marker.as_bytes()) else {
            continue;
        };
        if !suffix.is_empty() && suffix != b"--" {
            continue;
        }
        if let Some(begin) = part_start {
            if parts.len() >= cap {
                return (parts, false, true);
            }
            let mut finish = start;
            if finish > begin && body[finish - 1] == b'\n' {
                finish -= 1;
                if finish > begin && body[finish - 1] == b'\r' {
                    finish -= 1;
                }
            }
            parts.push(&body[begin..finish]);
        }
        if suffix == b"--" {
            return (parts, true, false);
        }
        if parts.len() >= cap {
            return (parts, false, true);
        }
        part_start = Some(offset);
    }
    if let Some(begin) = part_start {
        if parts.len() >= cap {
            return (parts, false, true);
        }
        parts.push(&body[begin..]);
    }
    (parts, false, false)
}

/// Quote-aware parameter parsing shared with the compatibility projection.
pub fn media_type(value: &str) -> (String, BTreeMap<String, String>) {
    let mut ranges = Vec::new();
    let mut quoted = false;
    let mut escaped = false;
    let mut start = 0;
    for (i, b) in value.bytes().enumerate() {
        if escaped {
            escaped = false;
            continue;
        }
        match b {
            b'\\' if quoted => escaped = true,
            b'"' => quoted = !quoted,
            b';' if !quoted => {
                ranges.push(&value[start..i]);
                start = i + 1;
            }
            _ => {}
        }
    }
    ranges.push(&value[start..]);
    let kind = ranges[0].trim().to_ascii_lowercase();
    let kind = if kind.is_empty() {
        "text/plain".into()
    } else {
        kind
    };
    let mut params = BTreeMap::new();
    for field in &ranges[1..] {
        if let Some((name, value)) = field.split_once('=') {
            let value = value.trim();
            let value =
                if let Some(inner) = value.strip_prefix('"').and_then(|v| v.strip_suffix('"')) {
                    let mut out = String::new();
                    let mut chars = inner.chars();
                    while let Some(c) = chars.next() {
                        if c == '\\' {
                            if let Some(c) = chars.next() {
                                out.push(c);
                            }
                        } else {
                            out.push(c);
                        }
                    }
                    out
                } else {
                    value.to_string()
                };
            params.insert(name.trim().to_ascii_lowercase(), value);
        }
    }
    (kind, params)
}

pub fn percent_decode(value: &str) -> Vec<u8> {
    let bytes = value.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            if let (Some(a), Some(b)) = (hex(bytes[i + 1]), hex(bytes[i + 2])) {
                out.push(a * 16 + b);
                i += 3;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    out
}

fn hex(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

pub fn parameter(
    params: &BTreeMap<String, String>,
    name: &str,
    warnings: &mut Vec<String>,
) -> Option<String> {
    if let Some(v) = params.get(&format!("{name}*")) {
        return Some(extended_parameter(v, warnings));
    }
    let mut segments = Vec::new();
    for i in 0..MAX_PARTS {
        if let Some(v) = params.get(&format!("{name}*{i}*")) {
            segments.push((true, v.as_str()));
        } else if let Some(v) = params.get(&format!("{name}*{i}")) {
            segments.push((false, v.as_str()));
        } else {
            break;
        }
    }
    if !segments.is_empty() {
        if !segments.iter().any(|(encoded, _)| *encoded) {
            let joined: String = segments.iter().map(|(_, value)| *value).collect();
            return Some(decode_words(&joined, warnings));
        }
        let mut charset = "utf-8";
        let mut bytes = Vec::new();
        for (index, (encoded, value)) in segments.into_iter().enumerate() {
            let payload = if index == 0 && encoded {
                let mut prefix = value.splitn(3, '\'');
                let label = prefix.next().unwrap_or("utf-8");
                let _language = prefix.next();
                match prefix.next() {
                    Some(payload) => {
                        charset = label;
                        payload
                    }
                    None => {
                        warn(warnings, "Malformed RFC2231 parameter");
                        value
                    }
                }
            } else {
                value
            };
            if encoded {
                bytes.extend_from_slice(&percent_decode(payload));
            } else {
                bytes.extend_from_slice(payload.as_bytes());
            }
        }
        return Some(charset_decode(&bytes, charset, warnings));
    }
    params.get(name).map(|v| decode_words(v, warnings))
}

fn extended_parameter(value: &str, warnings: &mut Vec<String>) -> String {
    let mut parts = value.splitn(3, '\'');
    let charset = parts.next().unwrap_or("utf-8");
    let _language = parts.next();
    match parts.next() {
        Some(encoded) => charset_decode(&percent_decode(encoded), charset, warnings),
        None => {
            warn(warnings, "Malformed RFC2231 parameter");
            String::from_utf8_lossy(&percent_decode(value)).into_owned()
        }
    }
}

pub fn charset_decode(bytes: &[u8], label: &str, warnings: &mut Vec<String>) -> String {
    let label = label.trim().to_ascii_lowercase();
    if matches!(
        label.as_str(),
        "iso-8859-1" | "iso_8859-1" | "latin1" | "latin-1" | "l1"
    ) {
        return bytes.iter().map(|&b| char::from(b)).collect();
    }
    if matches!(label.as_str(), "us-ascii" | "ascii") {
        if !bytes.is_ascii() {
            warn(warnings, "Invalid US-ASCII bytes");
        }
        return String::from_utf8_lossy(bytes).into_owned();
    }
    let encoding = Encoding::for_label(label.as_bytes()).filter(|&e| e != REPLACEMENT);
    match encoding {
        Some(enc) => {
            let (text, failed) = enc.decode_without_bom_handling(bytes);
            if failed {
                warn(warnings, "Invalid bytes for declared charset");
            }
            text.into_owned()
        }
        None => {
            warn(warnings, "Unknown charset; decoded as lossy UTF-8");
            String::from_utf8_lossy(bytes).into_owned()
        }
    }
}

pub fn transfer_decode(bytes: &[u8], encoding: &str, warnings: &mut Vec<String>) -> Vec<u8> {
    match encoding.trim().to_ascii_lowercase().as_str() {
        "base64" => decode_base64(bytes, warnings),
        "quoted-printable" => decode_qp(bytes, warnings),
        "" | "7bit" | "8bit" | "binary" => bytes.to_vec(),
        _ => {
            warn(warnings, "Unknown transfer encoding; kept original bytes");
            bytes.to_vec()
        }
    }
}

pub fn decode_base64(bytes: &[u8], warnings: &mut Vec<String>) -> Vec<u8> {
    let mut out = Vec::with_capacity(bytes.len() * 3 / 4);
    let mut accumulator = 0u32;
    let mut bits = 0;
    let mut padding = false;
    let mut digits = 0;
    let mut pads = 0;
    for &b in bytes {
        let value = match b {
            b'A'..=b'Z' => b - b'A',
            b'a'..=b'z' => b - b'a' + 26,
            b'0'..=b'9' => b - b'0' + 52,
            b'+' => 62,
            b'/' => 63,
            b'=' => {
                padding = true;
                pads += 1;
                continue;
            }
            b if b.is_ascii_whitespace() => continue,
            _ => {
                warn(warnings, "Invalid base64; junk skipped");
                continue;
            }
        };
        if padding {
            warn(warnings, "Invalid base64 padding");
        }
        digits += 1;
        accumulator = (accumulator << 6) | u32::from(value);
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            out.push((accumulator >> bits) as u8);
        }
    }
    if digits % 4 == 1 || pads > 2 || (pads > 0 && (digits + pads) % 4 != 0) {
        warn(warnings, "Invalid base64 length/padding");
    }
    out
}

pub fn decode_qp(bytes: &[u8], warnings: &mut Vec<String>) -> Vec<u8> {
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'=' {
            if bytes.get(i + 1) == Some(&b'\n') {
                i += 2;
                continue;
            }
            if bytes.get(i + 1..i + 3) == Some(b"\r\n") {
                i += 3;
                continue;
            }
            if i + 2 < bytes.len() {
                if let (Some(a), Some(b)) = (hex(bytes[i + 1]), hex(bytes[i + 2])) {
                    out.push(a * 16 + b);
                    i += 3;
                    continue;
                }
            }
            warn(warnings, "Invalid quoted-printable escape; kept literal");
        }
        out.push(bytes[i]);
        i += 1;
    }
    out
}

pub fn decode_words(value: &str, warnings: &mut Vec<String>) -> String {
    let mut out = String::new();
    let mut rest = value;
    let mut previous_encoded = false;
    while let Some(start) = rest.find("=?") {
        let prefix = &rest[..start];
        let word = &rest[start + 2..];
        let Some((label, word)) = word.split_once('?') else {
            break;
        };
        let Some((kind, word)) = word.split_once('?') else {
            break;
        };
        let Some((payload, tail)) = word.split_once("?=") else {
            break;
        };
        if !matches!(kind, "B" | "b" | "Q" | "q") {
            out.push_str(&rest[..start + 2]);
            rest = &rest[start + 2..];
            previous_encoded = false;
            continue;
        }
        if !previous_encoded || !prefix.chars().all(char::is_whitespace) {
            out.push_str(prefix);
        }
        let bytes = if kind.eq_ignore_ascii_case("B") {
            decode_base64(payload.as_bytes(), warnings)
        } else {
            decode_qp(payload.replace('_', " ").as_bytes(), warnings)
        };
        out.push_str(&charset_decode(&bytes, label, warnings));
        previous_encoded = true;
        rest = tail;
    }
    out.push_str(rest);
    out
}
