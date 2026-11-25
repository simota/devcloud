//! GCS V4 (`GOOG4-HMAC-SHA256`, `GOOG4-RSA-SHA256`) and V2
//! (`GoogleAccessId`/`Expires`/`Signature`) signed URL validation, plus V4 HMAC,
//! V4 RSA, V2 RSA, and V2 HMAC-SHA1 signing helpers for deterministic local
//! testing.
//!
//! V2 URLs whose `GoogleAccessId` is a registered HMAC access ID are verified
//! as base64 HMAC-SHA1 over the V2 string-to-sign outside `relaxed` mode;
//! `relaxed` mode keeps its previous structural/expiry-only handling for them.
//!
//! RSA-signed URLs (V4 RSA and V2) are cryptographically verified against the
//! public key registered for the signing service account
//! (`Server::add_rsa_public_key_pem` / `add_service_account_key_json`). When no
//! key is registered, non-strict auth modes keep the structural/expiry-only
//! checks; `strict` mode rejects the URL.

use std::collections::BTreeMap;

use devcloud_s3::base64;
use devcloud_s3::hashes::{hmac_sha256, sha256_hex};
use devcloud_s3::time_fmt::parse_rfc3339;

use crate::http::{Request, Server};

pub const V4_ALGO_HMAC: &str = "GOOG4-HMAC-SHA256";
pub const V4_ALGO_RSA: &str = "GOOG4-RSA-SHA256";
const MAX_V4_EXPIRES_SECONDS: i64 = 604_800; // 7 days
const MAX_CLOCK_SKEW_SECONDS: i64 = 900; // 15 minutes

#[derive(Debug, PartialEq, Eq)]
pub enum SignedUrlCheck {
    NotSigned,
    Valid,
    Invalid {
        status: u16,
        code: &'static str,
        message: String,
    },
}

pub fn has_signed_url_params(req: &Request) -> bool {
    req.query.contains_key("X-Goog-Algorithm")
        || req.query.contains_key("X-Goog-Signature")
        || req.query.contains_key("X-Goog-Credential")
        || req.query.contains_key("GoogleAccessId")
        || (req.query.contains_key("Expires") && req.query.contains_key("Signature"))
}

pub fn verify_signed_url(server: &Server, req: &Request) -> SignedUrlCheck {
    if !has_signed_url_params(req) {
        return SignedUrlCheck::NotSigned;
    }
    if req.query.contains_key("X-Goog-Algorithm")
        || req.query.contains_key("X-Goog-Credential")
        || req.query.contains_key("X-Goog-Signature")
    {
        verify_v4(server, req)
    } else {
        verify_v2(server, req)
    }
}

fn verify_v4(server: &Server, req: &Request) -> SignedUrlCheck {
    let algo = req.query_value("X-Goog-Algorithm");
    if algo != V4_ALGO_HMAC && algo != V4_ALGO_RSA {
        return invalid(
            400,
            "InvalidArgument",
            "unsupported X-Goog-Algorithm; expected GOOG4-HMAC-SHA256 or GOOG4-RSA-SHA256",
        );
    }
    let credential = req.query_value("X-Goog-Credential");
    let Some((access_id, date_stamp, region, service, terminator)) =
        parse_v4_credential(credential)
    else {
        return invalid(400, "InvalidArgument", "malformed X-Goog-Credential scope");
    };
    if service != "storage" || terminator != "goog4_request" {
        return invalid(
            400,
            "InvalidArgument",
            "X-Goog-Credential must target storage/goog4_request",
        );
    }

    let goog_date = req.query_value("X-Goog-Date");
    let Some(signed_at_secs) = parse_goog4_date(goog_date) else {
        return invalid(
            400,
            "InvalidArgument",
            "X-Goog-Date must be formatted as YYYYMMDDTHHMMSSZ",
        );
    };
    if !goog_date.starts_with(date_stamp) {
        return invalid(
            400,
            "InvalidArgument",
            "X-Goog-Credential date does not match X-Goog-Date",
        );
    }

    let expires: i64 = match req.query_value("X-Goog-Expires").parse() {
        Ok(v) if (1..=MAX_V4_EXPIRES_SECONDS).contains(&v) => v,
        _ => {
            return invalid(
                400,
                "InvalidArgument",
                "X-Goog-Expires must be an integer between 1 and 604800",
            );
        }
    };

    let now_secs = parse_rfc3339(&server.now()).map(|(s, _)| s).unwrap_or(0);
    if now_secs > signed_at_secs.saturating_add(expires) {
        return invalid(400, "ExpiredToken", "signed URL has expired");
    }
    if signed_at_secs > now_secs.saturating_add(MAX_CLOCK_SKEW_SECONDS) {
        return invalid(
            400,
            "InvalidArgument",
            "X-Goog-Date is too far in the future",
        );
    }

    let signed_headers = req.query_value("X-Goog-SignedHeaders");
    let header_names: Vec<&str> = signed_headers
        .split(';')
        .filter(|h| !h.is_empty())
        .collect();
    if header_names.is_empty() || !header_names.contains(&"host") {
        return invalid(
            400,
            "InvalidArgument",
            "X-Goog-SignedHeaders must include host",
        );
    }
    for &name in &header_names {
        if name != name.to_ascii_lowercase() {
            return invalid(
                400,
                "InvalidArgument",
                "X-Goog-SignedHeaders must be lowercase",
            );
        }
        if req.header(name).is_empty() {
            return invalid(
                403,
                "SignatureDoesNotMatch",
                &format!("signed header {name:?} is missing from request"),
            );
        }
    }

    let signature = req.query_value("X-Goog-Signature");
    if signature.is_empty()
        || signature.len() % 2 != 0
        || !signature.bytes().all(|b| b.is_ascii_hexdigit())
    {
        return invalid(
            400,
            "InvalidArgument",
            "X-Goog-Signature must be a non-empty hex string",
        );
    }

    if algo == V4_ALGO_HMAC {
        if signature.len() != 64 {
            return invalid(
                403,
                "SignatureDoesNotMatch",
                "GOOG4-HMAC-SHA256 signature must be 64 hex characters",
            );
        }
        if let Some(secret) = server.hmac_secret(access_id) {
            let canonical_req = build_v4_canonical_request(
                &req.method,
                &req.path,
                &req.query_pairs(),
                &req.headers,
                signed_headers,
            );
            let expected =
                compute_v4_hmac_signature(secret, date_stamp, region, goog_date, &canonical_req);
            if !constant_time_eq(
                expected.as_bytes(),
                signature.to_ascii_lowercase().as_bytes(),
            ) {
                return invalid(
                    403,
                    "SignatureDoesNotMatch",
                    "request signature does not match calculated signature",
                );
            }
            return SignedUrlCheck::Valid;
        }
        if !server.is_relaxed_auth() {
            return invalid(
                403,
                "InvalidAccessKeyId",
                "HMAC access ID is not registered",
            );
        }
    }

    if algo == V4_ALGO_RSA {
        let Some(key) = server.rsa_public_key(access_id) else {
            return unregistered_rsa_key(server);
        };
        let Ok(sig_bytes) = hex::decode(signature) else {
            return invalid(
                400,
                "InvalidArgument",
                "X-Goog-Signature must be a non-empty hex string",
            );
        };
        let canonical_req = build_v4_canonical_request(
            &req.method,
            &req.path,
            &req.query_pairs(),
            &req.headers,
            signed_headers,
        );
        let string_to_sign =
            v4_string_to_sign(V4_ALGO_RSA, date_stamp, region, goog_date, &canonical_req);
        if !key.verify_sha256(string_to_sign.as_bytes(), &sig_bytes) {
            return invalid(
                403,
                "SignatureDoesNotMatch",
                "request signature does not match calculated signature",
            );
        }
    }

    // Unregistered HMAC IDs (relaxed mode) and unregistered RSA keys (non-strict
    // modes) have passed all structural, header-coverage, and expiration checks.
    SignedUrlCheck::Valid
}

fn unregistered_rsa_key(server: &Server) -> SignedUrlCheck {
    if server.is_strict_auth() {
        return invalid(
            403,
            "AccessDenied",
            "no RSA public key is registered for the signing service account",
        );
    }
    SignedUrlCheck::Valid
}

/// Identity proven (or, in relaxed modes, asserted) by a signed URL.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SignedUrlSigner {
    /// V4 `GOOG4-HMAC-SHA256` / V2 HMAC-SHA1 access ID.
    Hmac(String),
    /// V4 `GOOG4-RSA-SHA256` / V2 service-account email.
    ServiceAccount(String),
}

/// Returns the signer of a request that `verify_signed_url` accepted.
pub fn signed_url_signer(server: &Server, req: &Request) -> Option<SignedUrlSigner> {
    if !has_signed_url_params(req) {
        return None;
    }
    if req.query.contains_key("X-Goog-Algorithm")
        || req.query.contains_key("X-Goog-Credential")
        || req.query.contains_key("X-Goog-Signature")
    {
        let (access_id, ..) = parse_v4_credential(req.query_value("X-Goog-Credential"))?;
        if req.query_value("X-Goog-Algorithm") == V4_ALGO_HMAC {
            Some(SignedUrlSigner::Hmac(access_id.to_string()))
        } else {
            Some(SignedUrlSigner::ServiceAccount(access_id.to_string()))
        }
    } else {
        let id = req.query_value("GoogleAccessId").trim();
        if id.is_empty() {
            None
        } else if v2_hmac_secret(server, id).is_some() {
            Some(SignedUrlSigner::Hmac(id.to_string()))
        } else {
            Some(SignedUrlSigner::ServiceAccount(id.to_string()))
        }
    }
}

fn verify_v2(server: &Server, req: &Request) -> SignedUrlCheck {
    let access_id = req.query_value("GoogleAccessId").trim();
    if access_id.is_empty() {
        return invalid(400, "InvalidArgument", "GoogleAccessId is required");
    }
    let expires: i64 = match req.query_value("Expires").trim().parse() {
        Ok(v) if v > 0 => v,
        _ => return invalid(400, "InvalidArgument", "Expires must be a Unix timestamp"),
    };
    let now_secs = parse_rfc3339(&server.now()).map(|(s, _)| s).unwrap_or(0);
    if now_secs > expires {
        return invalid(400, "ExpiredToken", "signed URL has expired");
    }
    let sig = req.query_value("Signature").trim();
    let Some(sig_bytes) = decode_v2_signature(sig) else {
        return invalid(400, "InvalidArgument", "Signature must be valid base64");
    };
    let string_to_sign = build_v2_string_to_sign(
        &req.method,
        req.header("content-md5"),
        req.header("content-type"),
        expires,
        &req.headers,
        &v2_canonical_resource(req),
    );
    if let Some(secret) = v2_hmac_secret(server, access_id) {
        let expected = hmac_sha1(secret.as_bytes(), string_to_sign.as_bytes());
        if !constant_time_eq(&expected, &sig_bytes) {
            return invalid(
                403,
                "SignatureDoesNotMatch",
                "request signature does not match calculated signature",
            );
        }
        return SignedUrlCheck::Valid;
    }
    let Some(key) = server.rsa_public_key(access_id) else {
        return unregistered_rsa_key(server);
    };
    if !key.verify_sha256(string_to_sign.as_bytes(), &sig_bytes) {
        return invalid(
            403,
            "SignatureDoesNotMatch",
            "request signature does not match calculated signature",
        );
    }
    SignedUrlCheck::Valid
}

/// The HMAC secret used to verify a V2 URL signed by `access_id`. `relaxed`
/// mode never verifies V2 HMAC signatures.
fn v2_hmac_secret<'a>(server: &'a Server, access_id: &str) -> Option<&'a str> {
    if server.is_relaxed_auth() {
        return None;
    }
    server.hmac_secret(access_id)
}

fn hmac_sha1(key: &[u8], data: &[u8]) -> Vec<u8> {
    let key = ring::hmac::Key::new(ring::hmac::HMAC_SHA1_FOR_LEGACY_USE_ONLY, key);
    ring::hmac::sign(&key, data).as_ref().to_vec()
}

/// Decodes a V2 `Signature` value. A literal `+` that reached us unescaped is
/// decoded to a space by query parsing, so spaces are mapped back to `+`.
fn decode_v2_signature(s: &str) -> Option<Vec<u8>> {
    if s.is_empty() {
        return None;
    }
    let normalized = s.replace([' ', '-'], "+").replace('_', "/");
    base64::std_decode(&normalized)
}

/// The V2 canonicalized resource: `/{bucket}/{object}`. Virtual-host requests
/// carry only the object in the path, so the host bucket is prefixed.
fn v2_canonical_resource(req: &Request) -> String {
    match crate::xml_api::virtual_host_bucket(req.header("host")) {
        Some(bucket) => format!("/{bucket}{}", req.path),
        None => req.path.clone(),
    }
}

/// Builds the V2 string-to-sign:
/// `VERB\nContent-MD5\nContent-Type\nExpires\n{x-goog-* headers}{resource}`.
pub fn build_v2_string_to_sign(
    method: &str,
    content_md5: &str,
    content_type: &str,
    expires: i64,
    headers: &BTreeMap<String, String>,
    canonical_resource: &str,
) -> String {
    let mut extension_headers = String::new();
    for (name, value) in headers {
        let lower = name.to_ascii_lowercase();
        if lower.starts_with("x-goog-") {
            extension_headers.push_str(&lower);
            extension_headers.push(':');
            extension_headers.push_str(&normalize_header_value(value));
            extension_headers.push('\n');
        }
    }
    format!(
        "{}\n{}\n{}\n{}\n{}{}",
        method.to_ascii_uppercase(),
        content_md5.trim(),
        content_type.trim(),
        expires,
        extension_headers,
        canonical_resource
    )
}

fn invalid(status: u16, code: &'static str, message: &str) -> SignedUrlCheck {
    SignedUrlCheck::Invalid {
        status,
        code,
        message: message.to_string(),
    }
}

fn parse_v4_credential(value: &str) -> Option<(&str, &str, &str, &str, &str)> {
    let mut parts = value.split('/');
    let access_id = parts.next().filter(|s| !s.is_empty())?;
    let date_stamp = parts
        .next()
        .filter(|s| s.len() == 8 && s.bytes().all(|b| b.is_ascii_digit()))?;
    let region = parts.next().filter(|s| !s.is_empty())?;
    let service = parts.next()?;
    let terminator = parts.next()?;
    if parts.next().is_some() {
        return None;
    }
    Some((access_id, date_stamp, region, service, terminator))
}

fn parse_goog4_date(value: &str) -> Option<i64> {
    if value.len() != 16 || !value.ends_with('Z') || value.as_bytes()[8] != b'T' {
        return None;
    }
    let rfc3339 = format!(
        "{}-{}-{}T{}:{}:{}Z",
        &value[0..4],
        &value[4..6],
        &value[6..8],
        &value[9..11],
        &value[11..13],
        &value[13..15]
    );
    parse_rfc3339(&rfc3339).map(|(s, _)| s)
}

fn canonical_uri(path: &str) -> String {
    if path.is_empty() || path == "/" {
        return "/".to_string();
    }
    let encoded_segments: Vec<String> = path
        .split('/')
        .map(|seg| percent_encode_strict(&crate::http::url_decode_path(seg)))
        .collect();
    encoded_segments.join("/")
}

fn canonical_query_string(pairs: &[(String, String)]) -> String {
    let mut encoded: Vec<(String, String)> = pairs
        .iter()
        .filter(|(k, _)| k != "X-Goog-Signature")
        .map(|(k, v)| (percent_encode_strict(k), percent_encode_strict(v)))
        .collect();
    encoded.sort();
    encoded
        .into_iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join("&")
}

fn normalize_header_value(value: &str) -> String {
    let mut out = String::new();
    let mut prev_space = false;
    for ch in value.trim().chars() {
        if ch.is_ascii_whitespace() {
            if !prev_space {
                out.push(' ');
                prev_space = true;
            }
        } else {
            out.push(ch);
            prev_space = false;
        }
    }
    out
}

pub fn build_v4_canonical_request(
    method: &str,
    path: &str,
    query_pairs: &[(String, String)],
    headers: &BTreeMap<String, String>,
    signed_headers: &str,
) -> String {
    let mut names: Vec<&str> = signed_headers
        .split(';')
        .filter(|s| !s.is_empty())
        .collect();
    names.sort_unstable();
    let mut canonical_headers = String::new();
    for name in &names {
        let value = headers.get(*name).map(String::as_str).unwrap_or("");
        canonical_headers.push_str(name);
        canonical_headers.push(':');
        canonical_headers.push_str(&normalize_header_value(value));
        canonical_headers.push('\n');
    }
    let signed_headers_sorted = names.join(";");
    let payload_hash = headers
        .get("x-goog-content-sha256")
        .filter(|v| !v.is_empty())
        .map(String::as_str)
        .unwrap_or("UNSIGNED-PAYLOAD");

    format!(
        "{}\n{}\n{}\n{}\n{}\n{}",
        method.to_ascii_uppercase(),
        canonical_uri(path),
        canonical_query_string(query_pairs),
        canonical_headers,
        signed_headers_sorted,
        payload_hash
    )
}

pub fn compute_v4_hmac_signature(
    secret: &str,
    date_stamp: &str,
    region: &str,
    goog_date: &str,
    canonical_request: &str,
) -> String {
    let string_to_sign = v4_string_to_sign(
        V4_ALGO_HMAC,
        date_stamp,
        region,
        goog_date,
        canonical_request,
    );
    let key0 = format!("GOOG4{secret}");
    let k_date = hmac_sha256(key0.as_bytes(), date_stamp.as_bytes());
    let k_region = hmac_sha256(&k_date, region.as_bytes());
    let k_service = hmac_sha256(&k_region, b"storage");
    let k_signing = hmac_sha256(&k_service, b"goog4_request");
    hex::encode(hmac_sha256(&k_signing, string_to_sign.as_bytes()))
}

pub fn v4_string_to_sign(
    algo: &str,
    date_stamp: &str,
    region: &str,
    goog_date: &str,
    canonical_request: &str,
) -> String {
    let scope = format!("{date_stamp}/{region}/storage/goog4_request");
    format!(
        "{algo}\n{goog_date}\n{scope}\n{}",
        sha256_hex(canonical_request.as_bytes())
    )
}

/// Parameters for generating a deterministic GCS V4 HMAC-SHA256 signed URL
/// target (`/<bucket>/<object>?X-Goog-Algorithm=...`).
#[derive(Debug, Clone)]
pub struct SignV4HmacInput<'a> {
    pub method: &'a str,
    pub path: &'a str,
    pub host: &'a str,
    pub access_id: &'a str,
    pub secret: &'a str,
    pub region: &'a str,
    pub goog_date: &'a str,
    pub expires: i64,
    pub extra_query: Vec<(String, String)>,
    pub extra_headers: BTreeMap<String, String>,
}

pub fn sign_v4_hmac_target(input: &SignV4HmacInput<'_>) -> String {
    let prepared = prepare_v4_target(&V4TargetParts {
        algo: V4_ALGO_HMAC,
        method: input.method,
        path: input.path,
        host: input.host,
        access_id: input.access_id,
        region: input.region,
        goog_date: input.goog_date,
        expires: input.expires,
        extra_query: &input.extra_query,
        extra_headers: &input.extra_headers,
    });
    let signature = compute_v4_hmac_signature(
        input.secret,
        &input.goog_date[0..8],
        input.region,
        input.goog_date,
        &prepared.canonical_request,
    );
    prepared.finish(&signature)
}

/// Parameters for generating a GCS V4 `GOOG4-RSA-SHA256` signed URL target
/// signed with a PEM RSA private key (PKCS#8 or PKCS#1).
#[derive(Debug, Clone)]
pub struct SignV4RsaInput<'a> {
    pub method: &'a str,
    pub path: &'a str,
    pub host: &'a str,
    pub service_account: &'a str,
    pub private_key_pem: &'a str,
    pub region: &'a str,
    pub goog_date: &'a str,
    pub expires: i64,
    pub extra_query: Vec<(String, String)>,
    pub extra_headers: BTreeMap<String, String>,
}

pub fn sign_v4_rsa_target(input: &SignV4RsaInput<'_>) -> Result<String, String> {
    let prepared = prepare_v4_target(&V4TargetParts {
        algo: V4_ALGO_RSA,
        method: input.method,
        path: input.path,
        host: input.host,
        access_id: input.service_account,
        region: input.region,
        goog_date: input.goog_date,
        expires: input.expires,
        extra_query: &input.extra_query,
        extra_headers: &input.extra_headers,
    });
    let string_to_sign = v4_string_to_sign(
        V4_ALGO_RSA,
        &input.goog_date[0..8],
        input.region,
        input.goog_date,
        &prepared.canonical_request,
    );
    let signature =
        crate::rsa_keys::sign_sha256_pem(input.private_key_pem, string_to_sign.as_bytes())?;
    Ok(prepared.finish(&hex::encode(signature)))
}

/// Parameters for generating a V2 (`GoogleAccessId`/`Expires`/`Signature`)
/// signed URL target. `path` is the path-style `/{bucket}/{object}` resource.
#[derive(Debug, Clone)]
pub struct SignV2Input<'a> {
    pub method: &'a str,
    pub path: &'a str,
    pub service_account: &'a str,
    pub private_key_pem: &'a str,
    pub expires: i64,
    pub content_md5: &'a str,
    pub content_type: &'a str,
    pub extra_headers: BTreeMap<String, String>,
}

pub fn sign_v2_target(input: &SignV2Input<'_>) -> Result<String, String> {
    let string_to_sign = build_v2_string_to_sign(
        input.method,
        input.content_md5,
        input.content_type,
        input.expires,
        &input.extra_headers,
        input.path,
    );
    let signature =
        crate::rsa_keys::sign_sha256_pem(input.private_key_pem, string_to_sign.as_bytes())?;
    Ok(format!(
        "{}?GoogleAccessId={}&Expires={}&Signature={}",
        input.path,
        percent_encode_strict(input.service_account),
        input.expires,
        percent_encode_strict(&base64::std_encode(&signature))
    ))
}

/// Parameters for generating a V2 signed URL target signed with an HMAC key
/// (base64 HMAC-SHA1). `path` is the path-style `/{bucket}/{object}` resource.
#[derive(Debug, Clone)]
pub struct SignV2HmacInput<'a> {
    pub method: &'a str,
    pub path: &'a str,
    pub access_id: &'a str,
    pub secret: &'a str,
    pub expires: i64,
    pub content_md5: &'a str,
    pub content_type: &'a str,
    pub extra_headers: BTreeMap<String, String>,
}

pub fn sign_v2_hmac_target(input: &SignV2HmacInput<'_>) -> String {
    let string_to_sign = build_v2_string_to_sign(
        input.method,
        input.content_md5,
        input.content_type,
        input.expires,
        &input.extra_headers,
        input.path,
    );
    let signature = hmac_sha1(input.secret.as_bytes(), string_to_sign.as_bytes());
    format!(
        "{}?GoogleAccessId={}&Expires={}&Signature={}",
        input.path,
        percent_encode_strict(input.access_id),
        input.expires,
        percent_encode_strict(&base64::std_encode(&signature))
    )
}

struct V4TargetParts<'a> {
    algo: &'a str,
    method: &'a str,
    path: &'a str,
    host: &'a str,
    access_id: &'a str,
    region: &'a str,
    goog_date: &'a str,
    expires: i64,
    extra_query: &'a [(String, String)],
    extra_headers: &'a BTreeMap<String, String>,
}

struct PreparedV4Target {
    path: String,
    query_pairs: Vec<(String, String)>,
    canonical_request: String,
}

impl PreparedV4Target {
    fn finish(self, signature_hex: &str) -> String {
        let mut full_query = canonical_query_string(&self.query_pairs);
        full_query.push_str("&X-Goog-Signature=");
        full_query.push_str(signature_hex);
        format!("{}?{}", self.path, full_query)
    }
}

fn prepare_v4_target(input: &V4TargetParts<'_>) -> PreparedV4Target {
    let date_stamp = &input.goog_date[0..8];
    let credential = format!(
        "{}/{}/{}/storage/goog4_request",
        input.access_id, date_stamp, input.region
    );
    let mut headers: BTreeMap<String, String> = BTreeMap::new();
    headers.insert("host".to_string(), input.host.to_string());
    for (k, v) in input.extra_headers {
        headers.insert(k.to_ascii_lowercase(), v.clone());
    }
    let signed_headers = headers.keys().cloned().collect::<Vec<_>>().join(";");
    let mut query_pairs = input.extra_query.to_vec();
    query_pairs.push(("X-Goog-Algorithm".to_string(), input.algo.to_string()));
    query_pairs.push(("X-Goog-Credential".to_string(), credential));
    query_pairs.push(("X-Goog-Date".to_string(), input.goog_date.to_string()));
    query_pairs.push(("X-Goog-Expires".to_string(), input.expires.to_string()));
    query_pairs.push(("X-Goog-SignedHeaders".to_string(), signed_headers.clone()));

    let canonical_request = build_v4_canonical_request(
        input.method,
        input.path,
        &query_pairs,
        &headers,
        &signed_headers,
    );
    PreparedV4Target {
        path: input.path.to_string(),
        query_pairs,
        canonical_request,
    }
}

fn percent_encode_strict(value: &str) -> String {
    let mut out = String::new();
    for &b in value.as_bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b'~') {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (&x, &y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}
