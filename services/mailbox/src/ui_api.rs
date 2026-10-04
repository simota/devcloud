use devcloud_mail::Message;
use serde::Serialize;

use crate::{mailhog, mime};

#[derive(Debug, Serialize)]
pub struct Address {
    pub name: String,
    pub address: String,
}

pub fn addresses(value: &str) -> Vec<Address> {
    let mut quoted = false;
    let mut angle = false;
    let mut escaped = false;
    let mut start = 0;
    let mut out = Vec::new();
    for (i, c) in value.char_indices() {
        if escaped {
            escaped = false;
            continue;
        }
        match c {
            '\\' if quoted => escaped = true,
            '"' => quoted = !quoted,
            '<' if !quoted => angle = true,
            '>' if !quoted => angle = false,
            ',' if !quoted && !angle => {
                out.push(address(&value[start..i]));
                start = i + 1;
            }
            _ => {}
        }
    }
    if !value[start..].trim().is_empty() {
        out.push(address(&value[start..]));
    }
    out
}

fn address(value: &str) -> Address {
    let name = value
        .split_once('<')
        .map_or("", |(name, _)| name.trim().trim_matches('"'));
    Address {
        name: name.to_string(),
        address: mailhog::address_only(value),
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Summary {
    pub id: String,
    pub from: Address,
    pub to: Vec<Address>,
    pub subject: String,
    pub received_at: String,
    pub size: usize,
    pub attachment_count: usize,
    pub snippet: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AttachmentInfo {
    pub index: usize,
    pub filename: String,
    pub content_type: String,
    pub size: usize,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Detail {
    pub id: String,
    pub from: Address,
    pub to: Vec<Address>,
    pub cc: Vec<Address>,
    pub subject: String,
    pub date: String,
    pub received_at: String,
    pub size: usize,
    pub headers: mime::Headers,
    pub text: String,
    pub has_html: bool,
    pub attachments: Vec<AttachmentInfo>,
    pub warnings: Vec<String>,
}

pub fn detail(message: &Message, raw: &[u8], parsed: &mut mime::ParsedMail) -> Detail {
    let headers: mime::Headers = parsed
        .headers
        .iter()
        .map(|(k, v)| (k.clone(), mime::decode_words(v, &mut parsed.warnings)))
        .collect();
    let from = addresses(mime::header(&headers, "From"))
        .into_iter()
        .next()
        .unwrap_or_else(|| address(&message.from));
    let mut to = addresses(mime::header(&headers, "To"));
    if to.is_empty() {
        to = message.to.iter().map(|s| address(s)).collect();
    }
    Detail {
        id: message.id.clone(),
        from,
        to,
        cc: addresses(mime::header(&headers, "Cc")),
        subject: mime::header(&headers, "Subject").to_string(),
        date: mime::header(&headers, "Date").to_string(),
        received_at: message.received_at.clone().unwrap_or_default(),
        size: mailhog::raw_data(raw).len(),
        headers,
        text: parsed.text.clone(),
        has_html: parsed.html.is_some(),
        attachments: parsed
            .attachments
            .iter()
            .enumerate()
            .map(|(index, a)| AttachmentInfo {
                index,
                filename: a.filename.clone(),
                content_type: a.content_type.clone(),
                size: a.bytes.len(),
            })
            .collect(),
        warnings: parsed.warnings.clone(),
    }
}

pub fn summary(message: &Message, raw: &[u8], parsed: &mut mime::ParsedMail) -> Summary {
    let subject = mime::decode_words(
        mime::header(&parsed.headers, "Subject"),
        &mut parsed.warnings,
    );
    let from = mime::decode_words(mime::header(&parsed.headers, "From"), &mut parsed.warnings);
    let to = mime::decode_words(mime::header(&parsed.headers, "To"), &mut parsed.warnings);
    let from = addresses(&from)
        .into_iter()
        .next()
        .unwrap_or_else(|| address(&message.from));
    let mut to = addresses(&to);
    if to.is_empty() {
        to = message.to.iter().map(|s| address(s)).collect();
    }
    let mut snippet = String::new();
    for word in parsed.text.split_whitespace() {
        if !snippet.is_empty() {
            snippet.push(' ');
        }
        snippet.extend(
            word.chars()
                .take(160usize.saturating_sub(snippet.chars().count())),
        );
        if snippet.chars().count() >= 160 {
            break;
        }
    }
    Summary {
        id: message.id.clone(),
        from,
        to,
        subject,
        received_at: message.received_at.clone().unwrap_or_default(),
        size: mailhog::raw_data(raw).len(),
        attachment_count: parsed.attachments.len(),
        snippet,
    }
}

pub fn matches(summary: &Summary, text: &str, query: &str) -> bool {
    let query = query.to_lowercase();
    summary.subject.to_lowercase().contains(&query)
        || summary.from.name.to_lowercase().contains(&query)
        || summary.from.address.to_lowercase().contains(&query)
        || summary.to.iter().any(|a| {
            a.name.to_lowercase().contains(&query) || a.address.to_lowercase().contains(&query)
        })
        || text.to_lowercase().contains(&query)
}

pub fn html(parsed: &mime::ParsedMail) -> Option<String> {
    let mut html = parsed.html.clone()?;
    let lower = html.to_ascii_lowercase();
    let head_end = lower.match_indices("<head").find_map(|(start, _)| {
        let rest = &lower[start + 5..];
        if !rest.starts_with('>') && !rest.starts_with(char::is_whitespace) {
            return None;
        }
        // Attribute values may contain '>'; insert after the actual tag end.
        let mut quote = None;
        for (offset, c) in rest.char_indices() {
            match (quote, c) {
                (None, '\'' | '"') => quote = Some(c),
                (Some(q), c) if q == c => quote = None,
                (None, '>') => return Some(start + 5 + offset + 1),
                _ => {}
            }
        }
        None
    });
    let leading = lower.trim_start_matches(|c: char| c.is_whitespace() || c == '\u{feff}');
    let doctype_end = if leading.starts_with("<!doctype") {
        leading
            .find('>')
            .map(|end| lower.len() - leading.len() + end + 1)
    } else {
        None
    };
    html.insert_str(
        head_end.or(doctype_end).unwrap_or(0),
        "<base target=\"_blank\">",
    );
    Some(html)
}
