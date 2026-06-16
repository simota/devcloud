//! Mirrors `internal/services/applicationautoscaling/sigv4.rs`.
//!
//! AWS SigV4 verification with three modes (relaxed / signed-relaxed / strict),
//! matching the legacy server's reply names and HTTP statuses exactly.

use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};

type HmacSha256 = Hmac<Sha256>;

pub const SIGV4_ALGORITHM: &str = "AWS4-HMAC-SHA256";
pub const SIGV4_SERVICE: &str = "application-autoscaling";

/// A signature verification failure, carrying the AWS error name and HTTP
/// status the legacy server would return.
#[derive(Debug, Clone)]
pub struct SignatureError {
    pub name: &'static str,
    pub status: u16,
}

fn err(name: &'static str, status: u16) -> SignatureError {
    SignatureError { name, status }
}

/// Minimal request view the verifier needs (headers + method + path + query +
/// host + body), so it stays independent of the HTTP framework.
pub struct SignedRequest<'a> {
    pub method: &'a str,
    pub path: &'a str,
    pub query: &'a str,
    pub host: &'a str,
    pub authorization: &'a str,
    pub amz_date: &'a str,
    pub content_sha256: &'a str,
    /// Resolves a header value by lowercase name (for canonical headers).
    pub header: &'a dyn Fn(&str) -> Option<String>,
    pub body: &'a [u8],
}

pub struct Credentials<'a> {
    pub auth_mode: &'a str,
    pub access_key_id: &'a str,
    pub secret_access_key: &'a str,
    pub region: &'a str,
}

fn default_str<'a>(value: &'a str, fallback: &'a str) -> &'a str {
    if value.is_empty() {
        fallback
    } else {
        value
    }
}

/// Mirrors `Server.verifySignature`.
pub fn verify_signature(req: &SignedRequest, creds: &Credentials) -> Result<(), SignatureError> {
    if creds.auth_mode.is_empty() || creds.auth_mode.eq_ignore_ascii_case("relaxed") {
        return Ok(());
    }
    let auth = req.authorization.trim();
    if auth.is_empty() {
        return Err(err("AccessDeniedException", 403));
    }
    if creds.auth_mode.eq_ignore_ascii_case("signed-relaxed") {
        return verify_shape(req, auth);
    }
    verify_full(req, auth, creds)
}

fn verify_full(req: &SignedRequest, auth: &str, creds: &Credentials) -> Result<(), SignatureError> {
    let prefix = format!("{SIGV4_ALGORITHM} ");
    if !auth.starts_with(&prefix) {
        return Err(err("IncompleteSignatureException", 400));
    }
    let values = parse_auth_params(&auth[prefix.len()..]);
    let credential = values.get("Credential").cloned().unwrap_or_default();
    let signed_headers = values.get("SignedHeaders").cloned().unwrap_or_default();
    let signature = values.get("Signature").cloned().unwrap_or_default();
    let (access_key, date_stamp, region, service) = match parse_credential_scope(&credential) {
        Some(v) => v,
        None => return Err(err("IncompleteSignatureException", 400)),
    };
    if signed_headers.is_empty() || signature.is_empty() {
        return Err(err("IncompleteSignatureException", 400));
    }
    if !valid_credential(&access_key, &region, &service, creds) {
        return Err(err("UnrecognizedClientException", 403));
    }
    if req.amz_date.is_empty() {
        return Err(err("IncompleteSignatureException", 400));
    }
    let payload_hash = if req.content_sha256.is_empty() {
        "UNSIGNED-PAYLOAD".to_string()
    } else {
        verify_payload_hash(req, req.content_sha256)?;
        req.content_sha256.to_string()
    };
    let expected = signature_for_request(
        req,
        &date_stamp,
        &region,
        &signed_headers,
        &payload_hash,
        creds,
    );
    if !constant_time_eq(signature.as_bytes(), expected.as_bytes()) {
        return Err(err("InvalidSignatureException", 403));
    }
    Ok(())
}

fn verify_shape(req: &SignedRequest, auth: &str) -> Result<(), SignatureError> {
    let prefix = format!("{SIGV4_ALGORITHM} ");
    if !auth.starts_with(&prefix) {
        return Err(err("IncompleteSignatureException", 400));
    }
    let values = parse_auth_params(&auth[prefix.len()..]);
    let credential = values.get("Credential").cloned().unwrap_or_default();
    let signed_headers = values.get("SignedHeaders").cloned().unwrap_or_default();
    let signature = values.get("Signature").cloned().unwrap_or_default();
    let (_, _, _, service) = match parse_credential_scope(&credential) {
        Some(v) => v,
        None => return Err(err("IncompleteSignatureException", 400)),
    };
    if signed_headers.is_empty() || signature.is_empty() {
        return Err(err("IncompleteSignatureException", 400));
    }
    if service != SIGV4_SERVICE || !is_lower_hex(&signature, 64) {
        return Err(err("IncompleteSignatureException", 400));
    }
    if req.amz_date.is_empty() {
        return Err(err("IncompleteSignatureException", 400));
    }
    if !req.content_sha256.is_empty() {
        return verify_payload_hash(req, req.content_sha256);
    }
    Ok(())
}

fn is_lower_hex(value: &str, length: usize) -> bool {
    value.len() == length
        && value
            .bytes()
            .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c))
}

fn valid_credential(access_key: &str, region: &str, service: &str, creds: &Credentials) -> bool {
    let configured_access_key = default_str(creds.access_key_id, "dev");
    let configured_region = default_str(creds.region, "us-east-1");
    access_key == configured_access_key && region == configured_region && service == SIGV4_SERVICE
}

fn verify_payload_hash(req: &SignedRequest, payload_hash: &str) -> Result<(), SignatureError> {
    if payload_hash == "UNSIGNED-PAYLOAD" {
        return Ok(());
    }
    if payload_hash.starts_with("STREAMING-") {
        return Err(err("NotImplemented", 501));
    }
    let got = sha256_hex(req.body);
    if !constant_time_eq(payload_hash.to_ascii_lowercase().as_bytes(), got.as_bytes()) {
        return Err(err("InvalidSignatureException", 403));
    }
    Ok(())
}

fn signature_for_request(
    req: &SignedRequest,
    date_stamp: &str,
    region: &str,
    signed_headers: &str,
    payload_hash: &str,
    creds: &Credentials,
) -> String {
    let canonical_request = [
        req.method,
        &canonical_uri(req.path),
        &canonical_query_string(req.query),
        &canonical_headers(req, signed_headers),
        &signed_headers.to_ascii_lowercase(),
        payload_hash,
    ]
    .join("\n");
    let scope = [date_stamp, region, SIGV4_SERVICE, "aws4_request"].join("/");
    let string_to_sign = [
        SIGV4_ALGORITHM,
        req.amz_date,
        &scope,
        &sha256_hex(canonical_request.as_bytes()),
    ]
    .join("\n");
    let signing_key = derive_signing_key(creds.secret_access_key, date_stamp, region);
    hex::encode(hmac_sha256(&signing_key, string_to_sign.as_bytes()))
}

fn parse_credential_scope(credential: &str) -> Option<(String, String, String, String)> {
    let parts: Vec<&str> = credential.split('/').collect();
    if parts.len() != 5 || parts[4] != "aws4_request" {
        return None;
    }
    Some((
        parts[0].to_string(),
        parts[1].to_string(),
        parts[2].to_string(),
        parts[3].to_string(),
    ))
}

fn parse_auth_params(value: &str) -> std::collections::HashMap<String, String> {
    let mut result = std::collections::HashMap::new();
    for part in value.split(',') {
        if let Some((k, v)) = part.trim().split_once('=') {
            result.insert(k.to_string(), v.to_string());
        }
    }
    result
}

fn canonical_uri(path: &str) -> String {
    if path.is_empty() {
        return "/".to_string();
    }
    aws_percent_encode(path, "/~")
}

fn canonical_query_string(query: &str) -> String {
    if query.is_empty() {
        return String::new();
    }
    let mut pairs: Vec<(String, String)> = Vec::new();
    for item in query.split('&') {
        match item.split_once('=') {
            Some((k, v)) => pairs.push((url_decode(k), url_decode(v))),
            None => pairs.push((url_decode(item), String::new())),
        }
    }
    pairs.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));
    pairs
        .iter()
        .map(|(k, v)| {
            format!(
                "{}={}",
                aws_percent_encode(k, "~-_"),
                aws_percent_encode(v, "~-_")
            )
        })
        .collect::<Vec<_>>()
        .join("&")
}

fn canonical_headers(req: &SignedRequest, signed_headers: &str) -> String {
    let mut out = String::new();
    for name in signed_headers.to_ascii_lowercase().split(';') {
        let name = name.trim();
        if name.is_empty() {
            continue;
        }
        let value = if name == "host" {
            req.host.to_string()
        } else {
            (req.header)(name).unwrap_or_default()
        };
        out.push_str(name);
        out.push(':');
        out.push_str(&normalize_header_value(&value));
        out.push('\n');
    }
    out
}

fn normalize_header_value(value: &str) -> String {
    value.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn aws_percent_encode(value: &str, safe: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for &c in value.as_bytes() {
        let ch = c as char;
        if ch.is_ascii_alphanumeric()
            || ch == '-'
            || ch == '_'
            || ch == '.'
            || ch == '~'
            || safe.contains(ch)
        {
            out.push(ch);
        } else {
            out.push_str(&format!("%{:02X}", c));
        }
    }
    out
}

fn url_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'%' if i + 2 < bytes.len() => {
                if let Ok(b) = u8::from_str_radix(&s[i + 1..i + 3], 16) {
                    out.push(b);
                    i += 3;
                    continue;
                }
                out.push(bytes[i]);
                i += 1;
            }
            b'+' => {
                out.push(b' ');
                i += 1;
            }
            b => {
                out.push(b);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn derive_signing_key(secret: &str, date_stamp: &str, region: &str) -> Vec<u8> {
    let secret = default_str(secret, "dev");
    let date_key = hmac_sha256(format!("AWS4{secret}").as_bytes(), date_stamp.as_bytes());
    let region_key = hmac_sha256(&date_key, region.as_bytes());
    let service_key = hmac_sha256(&region_key, SIGV4_SERVICE.as_bytes());
    hmac_sha256(&service_key, b"aws4_request")
}

fn hmac_sha256(key: &[u8], value: &[u8]) -> Vec<u8> {
    let mut mac = HmacSha256::new_from_slice(key).expect("HMAC accepts any key length");
    mac.update(value);
    mac.finalize().into_bytes().to_vec()
}

fn sha256_hex(value: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(value);
    hex::encode(hasher.finalize())
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}
