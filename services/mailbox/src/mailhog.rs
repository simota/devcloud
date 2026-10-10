//! Undecoded MailHog v1.0.1 wire shapes, including its naive multipart split.
use std::collections::BTreeMap;

use devcloud_mail::Message;
use serde::Serialize;

use crate::http::{ResponseBuffer, MAX_RESPONSE_BYTES};
use crate::mime::{self, Headers, MAX_PARTS};
use std::io::{self, Write};

const MAX_COMPAT_DEPTH: usize = 8;

#[derive(Debug, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct Path {
    pub relays: Option<Vec<String>>,
    pub mailbox: String,
    pub domain: String,
    pub params: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct Content {
    pub headers: BTreeMap<String, Vec<String>>,
    pub body: String,
    pub size: usize,
    #[serde(rename = "MIME")]
    pub mime: Option<MimeBody>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct MimeBody {
    pub parts: Option<Vec<Content>>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct Raw {
    pub from: String,
    pub to: Vec<String>,
    pub data: String,
    pub helo: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct MailhogMessage {
    #[serde(rename = "ID")]
    pub id: String,
    pub from: Path,
    pub to: Vec<Path>,
    pub content: Content,
    pub created: String,
    #[serde(rename = "MIME")]
    pub mime: Option<MimeBody>,
    pub raw: Raw,
}

pub fn raw_data(raw: &[u8]) -> &[u8] {
    raw.strip_suffix(b"\r\n").unwrap_or(raw)
}

pub fn address_only(value: &str) -> String {
    if let Some((_, rest)) = value.split_once('<') {
        if let Some((address, _)) = rest.split_once('>') {
            return address.to_string();
        }
    }
    value.trim().to_string()
}

pub fn path(value: &str) -> Path {
    let (relays, address) = if value.starts_with('@') {
        match value.split_once(':') {
            Some((route, addr)) => (
                Some(
                    route
                        .split(',')
                        .map(|s| s.trim_start_matches('@').to_string())
                        .collect(),
                ),
                addr,
            ),
            None => (None, value),
        }
    } else {
        (None, value)
    };
    let (mailbox, domain) = address.split_once('@').unwrap_or((address, ""));
    Path {
        relays,
        mailbox: mailbox.to_string(),
        domain: domain.to_string(),
        params: String::new(),
    }
}

pub fn project(message: &Message, raw: &[u8], hostname: &str) -> MailhogMessage {
    std::panic::catch_unwind(|| project_inner(message, raw, hostname))
        .unwrap_or_else(|_| project_inner(message, b"", hostname))
}

fn project_inner(message: &Message, raw: &[u8], hostname: &str) -> MailhogMessage {
    let data = raw_data(raw);
    let from = message
        .envelope_from
        .clone()
        .unwrap_or_else(|| address_only(&message.from));
    let to: Vec<String> = message.to.iter().map(|v| address_only(v)).collect();
    let mut count = 0;
    let mut remaining = MAX_RESPONSE_BYTES;
    let mut content = content(data, 0, &mut count, &mut remaining);
    let mime = content.mime.take();
    let created = message
        .received_at
        .clone()
        .unwrap_or_else(|| "1970-01-01T00:00:00Z".into());
    if !content
        .headers
        .keys()
        .any(|k| k.eq_ignore_ascii_case("Message-ID"))
    {
        content
            .headers
            .insert("Message-ID".into(), vec![message.id.clone()]);
    }
    append_header(
        &mut content.headers,
        "Received",
        format!(
            "from {} by {} (MailHog)\r\n          id {}; {}",
            message.helo,
            hostname,
            message.id,
            rfc1123z(&created)
        ),
    );
    append_header(&mut content.headers, "Return-Path", format!("<{from}>"));
    MailhogMessage {
        id: message.id.clone(),
        from: path(&from),
        to: to.iter().map(|v| path(v)).collect(),
        content,
        created,
        mime,
        raw: Raw {
            from,
            to,
            data: String::from_utf8_lossy(data).into_owned(),
            helo: message.helo.clone(),
        },
    }
}

fn append_header(headers: &mut BTreeMap<String, Vec<String>>, name: &str, value: String) {
    if let Some((_, values)) = headers
        .iter_mut()
        .find(|(key, _)| key.eq_ignore_ascii_case(name))
    {
        values.push(value);
    } else {
        headers.insert(name.into(), vec![value]);
    }
}

fn content(raw: &[u8], depth: usize, count: &mut usize, remaining: &mut usize) -> Content {
    *count += 1;
    *remaining = remaining.saturating_sub(raw.len());
    let mut warnings = Vec::new();
    let (headers, body) = compat_headers(raw, &mut warnings);
    let ctype = mime::header(&headers, "Content-Type");
    let (kind, params) = mime::media_type(ctype);
    let nested = if kind.starts_with("multipart/") {
        match params.get("boundary").filter(|b| !b.is_empty()) {
            Some(boundary) if depth < MAX_COMPAT_DEPTH && *count < MAX_PARTS => {
                let marker = format!("--{boundary}");
                let mut parts = Vec::new();
                // Match MailHog's substring split, deliberately retaining both
                // the preamble and the closing-delimiter/epilogue chunk.
                let mut begin = 0;
                for end in find_offsets(body, marker.as_bytes()) {
                    if *count >= MAX_PARTS || end - begin > *remaining {
                        break;
                    }
                    if end > begin {
                        parts.push(content(
                            trim_crlf(&body[begin..end]),
                            depth + 1,
                            count,
                            remaining,
                        ));
                    }
                    begin = end + marker.len();
                }
                if begin < body.len() && *count < MAX_PARTS && body.len() - begin <= *remaining {
                    parts.push(content(
                        trim_crlf(&body[begin..]),
                        depth + 1,
                        count,
                        remaining,
                    ));
                }
                Some(MimeBody { parts: Some(parts) })
            }
            _ => Some(MimeBody { parts: None }),
        }
    } else {
        None
    };
    let mut map = BTreeMap::new();
    // Repeated headers (Received, To, Cc) keep every value, as MailHog's
    // map[string][]string does.
    for (name, value) in headers {
        map.entry(name).or_insert_with(Vec::new).push(value);
    }
    Content {
        headers: map,
        body: String::from_utf8_lossy(body).into_owned(),
        size: raw.len(),
        mime: nested,
    }
}

fn trim_crlf(mut data: &[u8]) -> &[u8] {
    while data.first().is_some_and(|b| matches!(b, b'\r' | b'\n')) {
        data = &data[1..];
    }
    while data.last().is_some_and(|b| matches!(b, b'\r' | b'\n')) {
        data = &data[..data.len() - 1];
    }
    data
}

fn compat_headers<'a>(raw: &'a [u8], warnings: &mut Vec<String>) -> (Headers, &'a [u8]) {
    if !raw.windows(4).any(|s| s == b"\r\n\r\n") {
        return (Vec::new(), raw);
    }
    mime::headers(raw, warnings)
}

/// A forward-only search avoids materializing an unbounded vector of chunks.
fn find_offsets<'a>(body: &'a [u8], marker: &'a [u8]) -> impl Iterator<Item = usize> + 'a {
    // KMP keeps repeated-prefix hostile boundaries linear in input size.
    let mut prefix = vec![0; marker.len()];
    let mut matched = 0;
    for i in 1..marker.len() {
        while matched > 0 && marker[i] != marker[matched] {
            matched = prefix[matched - 1];
        }
        if marker[i] == marker[matched] {
            matched += 1;
        }
        prefix[i] = matched;
    }
    let mut offset = 0;
    let mut matched = 0;
    std::iter::from_fn(move || {
        while offset < body.len() {
            let b = body[offset];
            while matched > 0 && b != marker[matched] {
                matched = prefix[matched - 1];
            }
            if b == marker[matched] {
                matched += 1;
            }
            offset += 1;
            if matched == marker.len() {
                matched = 0;
                return Some(offset - marker.len());
            }
        }
        None
    })
}

pub fn matches(message: &MailhogMessage, kind: &str, query: &str) -> bool {
    let query = query.to_lowercase();
    let contains = |s: &str| s.to_lowercase().contains(&query);
    let header_matches = |name: Option<&str>| {
        message
            .content
            .headers
            .iter()
            .filter(|(k, _)| name.is_none_or(|name| k.eq_ignore_ascii_case(name)))
            .any(|(_, values)| values.iter().any(|v| contains(v)))
    };
    match kind {
        "from" => contains(&message.raw.from) || header_matches(Some("From")),
        "to" => message.raw.to.iter().any(|s| contains(s)) || header_matches(Some("To")),
        "containing" => contains(&message.content.body) || header_matches(None),
        _ => false,
    }
}

/// Go's Atoi accepts only signed decimal digits, with no surrounding whitespace.
pub fn positive(value: &str, default: usize) -> usize {
    let digits = value.strip_prefix(['+', '-']).unwrap_or(value);
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return default;
    }
    value
        .parse::<i64>()
        .ok()
        .filter(|&n| n > 0)
        .map_or(default, |n| n as usize)
}

pub fn paging(start: &str, limit: &str, cap: usize) -> (usize, usize) {
    (positive(start, 0), positive(limit, 50).min(cap))
}

pub fn header_value<'a>(content: &'a Content, name: &str) -> &'a str {
    content
        .headers
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case(name))
        .and_then(|(_, v)| v.first())
        .map_or("", String::as_str)
}

pub fn download(message: &MailhogMessage) -> io::Result<Vec<u8>> {
    let mut out = ResponseBuffer::new(MAX_RESPONSE_BYTES);
    for (name, values) in &message.content.headers {
        for value in values {
            write!(out, "{name}: {value}\r\n")?;
        }
    }
    out.write_all(b"\r\n")?;
    out.write_all(message.content.body.as_bytes())?;
    Ok(out.into_inner())
}

fn rfc1123z(created: &str) -> String {
    let (secs, _) = devcloud_mail::time_fmt::unix_from_rfc3339(created);
    let secs = if secs == i64::MIN { 0 } else { secs };
    let date = devcloud_mail::time_fmt::rfc3339_from_unix(secs, 0);
    let weekday = ["Thu", "Fri", "Sat", "Sun", "Mon", "Tue", "Wed"]
        [secs.div_euclid(86400).rem_euclid(7) as usize];
    let month: usize = date.get(5..7).and_then(|s| s.parse().ok()).unwrap_or(1);
    let months = [
        "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
    ];
    format!(
        "{weekday}, {} {} {} {} +0000",
        &date[8..10],
        months.get(month.saturating_sub(1)).unwrap_or(&"Jan"),
        &date[..4],
        &date[11..19]
    )
}
