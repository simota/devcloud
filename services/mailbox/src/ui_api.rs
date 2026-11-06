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

/// Splits the raw header into addresses first, then decodes each display
/// name: an encoded `Doe, John` must not split into two recipients.
fn decoded_addresses(raw: &str, warnings: &mut Vec<String>) -> Vec<Address> {
    addresses(raw)
        .into_iter()
        .map(|mut a| {
            a.name = mime::decode_words(&a.name, warnings);
            a
        })
        .collect()
}

/// The body text to preview and search: the text part, or for HTML-only
/// mail a tag-stripped copy of the HTML.
pub fn body_text(parsed: &mime::ParsedMail) -> std::borrow::Cow<'_, str> {
    if !parsed.text.trim().is_empty() {
        return std::borrow::Cow::Borrowed(&parsed.text);
    }
    let Some(html) = parsed.html.as_deref() else {
        return std::borrow::Cow::Borrowed(&parsed.text);
    };
    std::borrow::Cow::Owned(strip_tags(html))
}

/// Crude HTML-to-text: drops tags, comments and script/style contents, and
/// decodes the common entities. Only for previews and search.
fn strip_tags(html: &str) -> String {
    let mut out = String::with_capacity(html.len() / 2);
    let lower = html.to_ascii_lowercase();
    let mut i = 0;
    while i < html.len() {
        let rest = &lower[i..];
        if rest.starts_with("<!--") {
            i += rest.find("-->").map_or(rest.len(), |e| e + 3);
            continue;
        }
        if rest.starts_with("<script") || rest.starts_with("<style") {
            let close = if rest.starts_with("<script") {
                "</script"
            } else {
                "</style"
            };
            i += rest.find(close).unwrap_or(rest.len());
            i += lower[i..].find('>').map_or(lower.len() - i, |e| e + 1);
            out.push(' ');
            continue;
        }
        if rest.starts_with('<') {
            i += rest.find('>').map_or(rest.len(), |e| e + 1);
            out.push(' ');
            continue;
        }
        let next = rest.find('<').unwrap_or(rest.len());
        out.push_str(&html[i..i + next]);
        i += next;
    }
    out.replace("&nbsp;", " ")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&#39;", "'")
        .replace("&amp;", "&")
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
    let raw_from = mime::header(&parsed.headers, "From").to_string();
    let raw_to = mime::header(&parsed.headers, "To").to_string();
    let raw_cc = mime::header(&parsed.headers, "Cc").to_string();
    let from = decoded_addresses(&raw_from, &mut parsed.warnings)
        .into_iter()
        .next()
        .unwrap_or_else(|| address(&message.from));
    let mut to = decoded_addresses(&raw_to, &mut parsed.warnings);
    if to.is_empty() {
        to = message.to.iter().map(|s| address(s)).collect();
    }
    let cc = decoded_addresses(&raw_cc, &mut parsed.warnings);
    Detail {
        id: message.id.clone(),
        from,
        to,
        cc,
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
    let raw_from = mime::header(&parsed.headers, "From").to_string();
    let raw_to = mime::header(&parsed.headers, "To").to_string();
    let from = decoded_addresses(&raw_from, &mut parsed.warnings)
        .into_iter()
        .next()
        .unwrap_or_else(|| address(&message.from));
    let mut to = decoded_addresses(&raw_to, &mut parsed.warnings);
    if to.is_empty() {
        to = message.to.iter().map(|s| address(s)).collect();
    }
    let mut snippet = String::new();
    for word in body_text(parsed).split_whitespace() {
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
