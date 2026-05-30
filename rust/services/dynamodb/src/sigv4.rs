//! AWS SigV4 verification for the DynamoDB service.
//!
//! Mirrors `internal/services/dynamodb/sigv4.go` (`service = "dynamodb"`).
//! Relaxed mode (default, used by all tooling/tests) accepts everything; strict
//! mode performs a full canonical-request signature comparison; `signed-relaxed`
//! only validates the Authorization header shape + payload hash. The signer is
//! exposed so tests can sign and assert round-trip acceptance.

use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};

type HmacSha256 = Hmac<Sha256>;

pub const ALGORITHM: &str = "AWS4-HMAC-SHA256";
pub const SERVICE: &str = "dynamodb";

/// A signature failure carrying the AWS error code + HTTP status.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SignatureError {
    pub code: &'static str,
    pub status: u16,
}

fn err(code: &'static str, status: u16) -> SignatureError {
    SignatureError { code, status }
}

/// The request view the verifier needs. Headers are looked up case-insensitively
/// by the canonical lowercase name.
pub struct SignedRequest<'a> {
    pub method: &'a str,
    pub path: &'a str,
    pub query: &'a str,
    pub host: &'a str,
    /// Lowercase header name -> value.
    pub headers: &'a std::collections::BTreeMap<String, String>,
    pub body: &'a [u8],
}

impl SignedRequest<'_> {
    fn header(&self, name: &str) -> &str {
        self.headers.get(name).map(String::as_str).unwrap_or("")
    }
}

/// Server-side credential config.
pub struct Credentials<'a> {
    pub auth_mode: &'a str,
    pub region: &'a str,
    pub access_key_id: &'a str,
    pub secret_access_key: &'a str,
}

impl Credentials<'_> {
    fn access_key(&self) -> &str {
        if self.access_key_id.is_empty() {
            "dev"
        } else {
            self.access_key_id
        }
    }
    fn region(&self) -> &str {
        if self.region.is_empty() {
            "us-east-1"
        } else {
            self.region
        }
    }
    fn secret(&self) -> &str {
        if self.secret_access_key.is_empty() {
            "dev"
        } else {
            self.secret_access_key
        }
    }
}

/// Verifies the request signature per the configured auth mode. Mirrors
/// `verifySignature`.
pub fn verify_signature(creds: &Credentials, req: &SignedRequest) -> Result<(), SignatureError> {
    if creds.auth_mode.is_empty() || creds.auth_mode.eq_ignore_ascii_case("relaxed") {
        return Ok(());
    }
    let auth = req.header("authorization").trim().to_string();
    if auth.is_empty() {
        return Err(err("AccessDeniedException", 403));
    }
    if creds.auth_mode.eq_ignore_ascii_case("signed-relaxed") {
        return verify_header_shape(req, &auth);
    }
    verify_authorization_header(creds, req, &auth)
}

fn verify_authorization_header(
    creds: &Credentials,
    req: &SignedRequest,
    auth: &str,
) -> Result<(), SignatureError> {
    let prefix = format!("{ALGORITHM} ");
    let Some(rest) = auth.strip_prefix(&prefix) else {
        return Err(err("IncompleteSignatureException", 400));
    };
    let values = parse_auth_params(rest);
    let credential = values.get("Credential").map(String::as_str).unwrap_or("");
    let signed_headers = values
        .get("SignedHeaders")
        .map(String::as_str)
        .unwrap_or("");
    let signature = values.get("Signature").map(String::as_str).unwrap_or("");
    let Some((access_key, date_stamp, region, service)) = parse_credential_scope(credential) else {
        return Err(err("IncompleteSignatureException", 400));
    };
    if signed_headers.is_empty() || signature.is_empty() {
        return Err(err("IncompleteSignatureException", 400));
    }
    if access_key != creds.access_key() || region != creds.region() || service != SERVICE {
        return Err(err("UnrecognizedClientException", 403));
    }
    if req.header("x-amz-date").is_empty() {
        return Err(err("IncompleteSignatureException", 400));
    }
    let mut payload_hash = req.header("x-amz-content-sha256").to_string();
    if payload_hash.is_empty() {
        payload_hash = "UNSIGNED-PAYLOAD".to_string();
    } else {
        verify_payload_hash(req, &payload_hash)?;
    }
    let expected = signature_for_request(
        creds,
        req,
        date_stamp,
        region,
        signed_headers,
        &payload_hash,
    );
    if !constant_time_eq(signature.as_bytes(), expected.as_bytes()) {
        return Err(err("InvalidSignatureException", 403));
    }
    Ok(())
}

fn verify_header_shape(req: &SignedRequest, auth: &str) -> Result<(), SignatureError> {
    let prefix = format!("{ALGORITHM} ");
    let Some(rest) = auth.strip_prefix(&prefix) else {
        return Err(err("IncompleteSignatureException", 400));
    };
    let values = parse_auth_params(rest);
    let credential = values.get("Credential").map(String::as_str).unwrap_or("");
    let signed_headers = values
        .get("SignedHeaders")
        .map(String::as_str)
        .unwrap_or("");
    let signature = values.get("Signature").map(String::as_str).unwrap_or("");
    let Some((_, _, _, service)) = parse_credential_scope(credential) else {
        return Err(err("IncompleteSignatureException", 400));
    };
    if signed_headers.is_empty() || signature.is_empty() {
        return Err(err("IncompleteSignatureException", 400));
    }
    if service != SERVICE || !is_lower_hex(signature, 64) {
        return Err(err("IncompleteSignatureException", 400));
    }
    if req.header("x-amz-date").is_empty() {
        return Err(err("IncompleteSignatureException", 400));
    }
    let payload_hash = req.header("x-amz-content-sha256");
    if !payload_hash.is_empty() {
        return verify_payload_hash(req, payload_hash);
    }
    Ok(())
}

fn verify_payload_hash(req: &SignedRequest, payload_hash: &str) -> Result<(), SignatureError> {
    if payload_hash == "UNSIGNED-PAYLOAD" {
        return Ok(());
    }
    if payload_hash.starts_with("STREAMING-") {
        return Err(err("NotImplemented", 501));
    }
    let got = sha256_hex(req.body);
    if !constant_time_eq(payload_hash.to_lowercase().as_bytes(), got.as_bytes()) {
        return Err(err("InvalidSignatureException", 403));
    }
    Ok(())
}

/// Computes the expected request signature, mirroring `signatureForRequest`.
pub fn signature_for_request(
    creds: &Credentials,
    req: &SignedRequest,
    date_stamp: &str,
    region: &str,
    signed_headers: &str,
    payload_hash: &str,
) -> String {
    let canonical_request = [
        req.method.to_string(),
        canonical_uri(req.path),
        canonical_query_string(req.query),
        canonical_headers(req, signed_headers),
        signed_headers.to_lowercase(),
        payload_hash.to_string(),
    ]
    .join("\n");
    let amz_date = req.header("x-amz-date");
    let scope = [date_stamp, region, SERVICE, "aws4_request"].join("/");
    let string_to_sign = [
        ALGORITHM.to_string(),
        amz_date.to_string(),
        scope,
        sha256_hex(canonical_request.as_bytes()),
    ]
    .join("\n");
    let signing_key = derive_signing_key(creds.secret(), date_stamp, region);
    hex::encode(hmac_sha256(&signing_key, string_to_sign.as_bytes()))
}

fn parse_credential_scope(credential: &str) -> Option<(&str, &str, &str, &str)> {
    let parts: Vec<&str> = credential.split('/').collect();
    if parts.len() != 5 || parts[4] != "aws4_request" {
        return None;
    }
    Some((parts[0], parts[1], parts[2], parts[3]))
}

fn parse_auth_params(value: &str) -> std::collections::BTreeMap<String, String> {
    let mut result = std::collections::BTreeMap::new();
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
    for part in query.split('&') {
        if part.is_empty() {
            continue;
        }
        match part.split_once('=') {
            Some((k, v)) => pairs.push((url_decode(k), url_decode(v))),
            None => pairs.push((url_decode(part), String::new())),
        }
    }
    pairs.sort();
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
    for name in signed_headers.to_lowercase().split(';') {
        let name = name.trim();
        if name.is_empty() {
            continue;
        }
        let value = if name == "host" {
            req.host
        } else {
            req.header(name)
        };
        out.push_str(name);
        out.push(':');
        out.push_str(&normalize_header_value(value));
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
            out.push_str(&format!("%{c:02X}"));
        }
    }
    out
}

fn url_decode(value: &str) -> String {
    let bytes = value.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'%' if i + 2 < bytes.len() => {
                if let Ok(b) = u8::from_str_radix(&value[i + 1..i + 3], 16) {
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
    let date_key = hmac_sha256(format!("AWS4{secret}").as_bytes(), date_stamp.as_bytes());
    let region_key = hmac_sha256(&date_key, region.as_bytes());
    let service_key = hmac_sha256(&region_key, SERVICE.as_bytes());
    hmac_sha256(&service_key, b"aws4_request")
}

fn hmac_sha256(key: &[u8], value: &[u8]) -> Vec<u8> {
    let mut mac = HmacSha256::new_from_slice(key).expect("hmac key");
    mac.update(value);
    mac.finalize().into_bytes().to_vec()
}

fn sha256_hex(value: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(value);
    hex::encode(hasher.finalize())
}

fn is_lower_hex(value: &str, length: usize) -> bool {
    value.len() == length
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    fn headers(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_lowercase(), v.to_string()))
            .collect()
    }

    #[test]
    fn relaxed_accepts_everything() {
        let creds = Credentials {
            auth_mode: "relaxed",
            region: "us-east-1",
            access_key_id: "dev",
            secret_access_key: "dev",
        };
        let h = BTreeMap::new();
        let req = SignedRequest {
            method: "POST",
            path: "/",
            query: "",
            host: "localhost",
            headers: &h,
            body: b"{}",
        };
        assert!(verify_signature(&creds, &req).is_ok());
    }

    #[test]
    fn strict_round_trip_accepts_self_signed() {
        let creds = Credentials {
            auth_mode: "strict",
            region: "us-east-1",
            access_key_id: "dev",
            secret_access_key: "dev",
        };
        let body = b"{}";
        let payload_hash = sha256_hex(body);
        let date_stamp = "20260501";
        let amz_date = "20260501T000000Z";
        let signed_headers = "content-type;host;x-amz-content-sha256;x-amz-date;x-amz-target";
        let mut h = headers(&[
            ("content-type", "application/x-amz-json-1.0"),
            ("x-amz-content-sha256", &payload_hash),
            ("x-amz-date", amz_date),
            ("x-amz-target", "DynamoDB_20120810.ListTables"),
        ]);
        let req = SignedRequest {
            method: "POST",
            path: "/",
            query: "",
            host: "localhost",
            headers: &h,
            body,
        };
        let sig = signature_for_request(
            &creds,
            &req,
            date_stamp,
            "us-east-1",
            signed_headers,
            &payload_hash,
        );
        h.insert(
            "authorization".to_string(),
            format!(
                "{ALGORITHM} Credential=dev/{date_stamp}/us-east-1/dynamodb/aws4_request, \
                 SignedHeaders={signed_headers}, Signature={sig}"
            ),
        );
        let req = SignedRequest {
            method: "POST",
            path: "/",
            query: "",
            host: "localhost",
            headers: &h,
            body,
        };
        assert_eq!(verify_signature(&creds, &req), Ok(()));
    }

    #[test]
    fn strict_rejects_missing_auth() {
        let creds = Credentials {
            auth_mode: "strict",
            region: "us-east-1",
            access_key_id: "dev",
            secret_access_key: "dev",
        };
        let h = BTreeMap::new();
        let req = SignedRequest {
            method: "POST",
            path: "/",
            query: "",
            host: "localhost",
            headers: &h,
            body: b"{}",
        };
        assert_eq!(
            verify_signature(&creds, &req),
            Err(err("AccessDeniedException", 403))
        );
    }

    #[test]
    fn payload_hash_mismatch_rejected() {
        let creds = Credentials {
            auth_mode: "strict",
            region: "us-east-1",
            access_key_id: "dev",
            secret_access_key: "dev",
        };
        let h = headers(&[
            ("authorization", &format!("{ALGORITHM} Credential=dev/20260501/us-east-1/dynamodb/aws4_request, SignedHeaders=host, Signature=abc")),
            ("x-amz-date", "20260501T000000Z"),
            ("x-amz-content-sha256", &"0".repeat(64)),
        ]);
        let req = SignedRequest {
            method: "POST",
            path: "/",
            query: "",
            host: "localhost",
            headers: &h,
            body: b"{}",
        };
        assert_eq!(
            verify_signature(&creds, &req),
            Err(err("InvalidSignatureException", 403))
        );
    }
}
