//! Bucket CORS configuration: XML parsing/serialization and AWS-style rule
//! matching. Routing and response assembly live in `http.rs`.

use serde::{Deserialize, Serialize};

use crate::xml::{encode, Element};

const XMLNS: &str = "http://s3.amazonaws.com/doc/2006-03-01/";
const MAX_RULES: usize = 100;
const ALLOWED_METHODS: [&str; 5] = ["GET", "PUT", "POST", "DELETE", "HEAD"];
const DEFAULT_RULE_MAX_AGE_SECONDS: u32 = 3600;
/// Response headers browser SDKs read, none of which are CORS-safelisted.
const DEFAULT_RULE_EXPOSE_HEADERS: [&str; 5] = [
    "ETag",
    "Content-Range",
    "Accept-Ranges",
    "x-amz-version-id",
    "x-amz-delete-marker",
];

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct CorsRule {
    pub id: String,
    pub allowed_origins: Vec<String>,
    pub allowed_methods: Vec<String>,
    pub allowed_headers: Vec<String>,
    pub expose_headers: Vec<String>,
    pub max_age_seconds: Option<u32>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct CorsConfiguration {
    pub rules: Vec<CorsRule>,
}

/// Builds the server-wide rule used by buckets without their own configuration.
/// `None` when no origin is configured, leaving only bucket rules in effect.
pub fn default_rule(origins: &[String]) -> Option<CorsRule> {
    if origins.is_empty() {
        return None;
    }
    Some(CorsRule {
        id: String::new(),
        allowed_origins: origins.to_vec(),
        allowed_methods: ALLOWED_METHODS.iter().map(|m| m.to_string()).collect(),
        allowed_headers: vec!["*".to_string()],
        expose_headers: DEFAULT_RULE_EXPOSE_HEADERS
            .iter()
            .map(|h| h.to_string())
            .collect(),
        max_age_seconds: Some(DEFAULT_RULE_MAX_AGE_SECONDS),
    })
}

/// Parses and validates a `<CORSConfiguration>` document.
pub fn parse_configuration(body: &[u8]) -> Result<CorsConfiguration, &'static str> {
    const MALFORMED: &str = "the XML is not well-formed or does not match the CORS schema";
    let xml = std::str::from_utf8(body).map_err(|_| MALFORMED)?;
    if !xml.contains("<CORSConfiguration") {
        return Err(MALFORMED);
    }
    let mut rules = Vec::new();
    for block in xml_blocks(xml, "CORSRule") {
        let max_age_seconds = match xml_blocks(block, "MaxAgeSeconds").first() {
            Some(value) => Some(value.trim().parse::<u32>().map_err(|_| MALFORMED)?),
            None => None,
        };
        rules.push(CorsRule {
            id: xml_values(block, "ID")
                .into_iter()
                .next()
                .unwrap_or_default(),
            allowed_origins: xml_values(block, "AllowedOrigin"),
            allowed_methods: xml_values(block, "AllowedMethod"),
            allowed_headers: xml_values(block, "AllowedHeader"),
            expose_headers: xml_values(block, "ExposeHeader"),
            max_age_seconds,
        });
    }
    validate(&rules)?;
    Ok(CorsConfiguration { rules })
}

fn validate(rules: &[CorsRule]) -> Result<(), &'static str> {
    if rules.is_empty() || rules.len() > MAX_RULES {
        return Err("a CORS configuration must have between 1 and 100 rules");
    }
    for rule in rules {
        if rule.allowed_origins.is_empty() || rule.allowed_methods.is_empty() {
            return Err("each CORSRule needs an AllowedOrigin and an AllowedMethod");
        }
        if !rule
            .allowed_methods
            .iter()
            .all(|m| ALLOWED_METHODS.contains(&m.as_str()))
        {
            return Err("AllowedMethod must be one of GET, PUT, POST, DELETE, HEAD");
        }
        if rule
            .allowed_origins
            .iter()
            .chain(&rule.allowed_headers)
            .any(|v| v.matches('*').count() > 1)
        {
            return Err("AllowedOrigin and AllowedHeader may contain one wildcard");
        }
        if rule.expose_headers.iter().any(|v| v.contains('*')) {
            return Err("ExposeHeader may not contain a wildcard");
        }
        // These values are written verbatim into response headers.
        if !rule.expose_headers.iter().all(|v| is_header_name(v)) {
            return Err("ExposeHeader must be a valid HTTP header name");
        }
    }
    Ok(())
}

fn is_header_name(value: &str) -> bool {
    !value.is_empty()
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"!#$%&'+-.^_`|~".contains(&b))
}

pub fn configuration_xml(config: &CorsConfiguration) -> Vec<u8> {
    let mut root = Element::new("CORSConfiguration").attr("xmlns", XMLNS);
    for rule in &config.rules {
        let mut element = Element::new("CORSRule");
        if !rule.id.is_empty() {
            element = element.text_child("ID", &rule.id);
        }
        for (tag, values) in [
            ("AllowedOrigin", &rule.allowed_origins),
            ("AllowedMethod", &rule.allowed_methods),
            ("AllowedHeader", &rule.allowed_headers),
            ("ExposeHeader", &rule.expose_headers),
        ] {
            for value in values {
                element = element.text_child(tag, value);
            }
        }
        if let Some(max_age) = rule.max_age_seconds {
            element = element.text_child("MaxAgeSeconds", &max_age.to_string());
        }
        root = root.child(element);
    }
    encode(&root)
}

/// Returns the first rule allowing this origin, method, and every requested
/// header. Header names compare case-insensitively; origins are case-sensitive.
pub fn find_rule<'a>(
    rules: &'a [CorsRule],
    origin: &str,
    method: &str,
    request_headers: &[&str],
) -> Option<&'a CorsRule> {
    rules.iter().find(|rule| {
        rule.allowed_methods.iter().any(|m| m == method)
            && rule
                .allowed_origins
                .iter()
                .any(|pattern| wildcard_match(pattern, origin))
            && request_headers.iter().all(|header| {
                let header = header.to_ascii_lowercase();
                rule.allowed_headers
                    .iter()
                    .any(|pattern| wildcard_match(&pattern.to_ascii_lowercase(), &header))
            })
    })
}

/// The `Access-Control-*` headers for an allowed request. Credentials are only
/// advertised for a specific origin: browsers reject them alongside `*`.
pub fn response_headers(rule: &CorsRule, origin: &str) -> Vec<(String, String)> {
    let any_origin = rule.allowed_origins.iter().any(|o| o == "*");
    let mut headers = vec![
        (
            "Access-Control-Allow-Origin".to_string(),
            if any_origin { "*" } else { origin }.to_string(),
        ),
        (
            "Access-Control-Allow-Methods".to_string(),
            rule.allowed_methods.join(", "),
        ),
    ];
    if !any_origin {
        headers.push((
            "Access-Control-Allow-Credentials".to_string(),
            "true".to_string(),
        ));
    }
    if !rule.expose_headers.is_empty() {
        headers.push((
            "Access-Control-Expose-Headers".to_string(),
            rule.expose_headers.join(", "),
        ));
    }
    if let Some(max_age) = rule.max_age_seconds {
        headers.push(("Access-Control-Max-Age".to_string(), max_age.to_string()));
    }
    headers
}

fn wildcard_match(pattern: &str, value: &str) -> bool {
    match pattern.split_once('*') {
        Some((prefix, suffix)) => {
            value.len() >= prefix.len() + suffix.len()
                && value.starts_with(prefix)
                && value.ends_with(suffix)
        }
        None => pattern == value,
    }
}

fn xml_blocks<'a>(xml: &'a str, tag: &str) -> Vec<&'a str> {
    let open = format!("<{tag}>");
    let close = format!("</{tag}>");
    let mut out = Vec::new();
    let mut rest = xml;
    while let Some(start) = rest.find(&open) {
        let inner = &rest[start + open.len()..];
        let Some(end) = inner.find(&close) else {
            break;
        };
        out.push(&inner[..end]);
        rest = &inner[end + close.len()..];
    }
    out
}

fn xml_values(xml: &str, tag: &str) -> Vec<String> {
    xml_blocks(xml, tag)
        .into_iter()
        .map(|value| xml_unescape(value.trim()))
        .collect()
}

fn xml_unescape(value: &str) -> String {
    value
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&apos;", "'")
        .replace("&amp;", "&")
}
