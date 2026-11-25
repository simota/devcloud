//! AWS SigV4 verification for the DynamoDB service.
//!
//! Mirrors `internal/services/dynamodb/sigv4.rs` (`service = "dynamodb"`).
//! Relaxed mode (default, used by all tooling/tests) accepts everything; strict
//! mode performs a full canonical-request signature comparison; `signed-relaxed`
//! only validates the Authorization header shape + payload hash. The signer is
//! exposed so tests can sign and assert round-trip acceptance. SigV4a
//! (`AWS4-ECDSA-P256-SHA256`) headers and their `aws-chunked` chunk signatures
//! are verified too.

use devcloud_sigv4a::{ChunkChain, SigningKey};
use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};

type HmacSha256 = Hmac<Sha256>;

pub const ALGORITHM: &str = "AWS4-HMAC-SHA256";
pub const SERVICE: &str = "dynamodb";
pub const CHUNK_ALGORITHM: &str = "AWS4-HMAC-SHA256-PAYLOAD";
pub const TRAILER_ALGORITHM: &str = "AWS4-HMAC-SHA256-TRAILER";
pub const STREAMING_SIGNED: &str = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD";
pub const STREAMING_SIGNED_TRAILER: &str = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER";
pub const STREAMING_UNSIGNED_TRAILER: &str = "STREAMING-UNSIGNED-PAYLOAD-TRAILER";

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
    pub(crate) fn header(&self, name: &str) -> &str {
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
    verify_signature_for_service(creds, req, SERVICE)
}

/// Verifies the request signature against an explicit AWS SigV4 service name
/// (e.g. `"dynamodb"` or `"dax"`).
pub fn verify_signature_for_service(
    creds: &Credentials,
    req: &SignedRequest,
    expected_service: &str,
) -> Result<(), SignatureError> {
    if creds.auth_mode.is_empty() || creds.auth_mode.eq_ignore_ascii_case("relaxed") {
        return Ok(());
    }
    let auth = req.header("authorization").trim().to_string();
    if auth.is_empty() {
        return Err(err("AccessDeniedException", 403));
    }
    let sigv4a = devcloud_sigv4a::is_authorization(&auth);
    if creds.auth_mode.eq_ignore_ascii_case("signed-relaxed") {
        return if sigv4a {
            verify_sigv4a_shape(req, &auth, expected_service)
        } else {
            verify_header_shape(req, &auth, expected_service)
        };
    }
    if sigv4a {
        return verify_sigv4a_header(creds, req, &auth, expected_service);
    }
    verify_authorization_header(creds, req, &auth, expected_service)
}

fn verify_authorization_header(
    creds: &Credentials,
    req: &SignedRequest,
    auth: &str,
    expected_service: &str,
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
    if access_key != creds.access_key() || region != creds.region() || service != expected_service {
        return Err(err("UnrecognizedClientException", 403));
    }
    // SigV4a chunk signatures need a SigV4a seed signature.
    if req.header("x-amz-date").is_empty()
        || devcloud_sigv4a::is_streaming(req.header("x-amz-content-sha256"))
    {
        return Err(err("IncompleteSignatureException", 400));
    }
    let mut payload_hash = req.header("x-amz-content-sha256").to_string();
    if payload_hash.is_empty() {
        payload_hash = "UNSIGNED-PAYLOAD".to_string();
    } else {
        verify_payload_hash(req, &payload_hash)?;
    }
    let expected = signature_for_request_with_service(
        creds,
        req,
        date_stamp,
        region,
        expected_service,
        signed_headers,
        &payload_hash,
    );
    if !constant_time_eq(signature.as_bytes(), expected.as_bytes()) {
        return Err(err("InvalidSignatureException", 403));
    }
    if matches!(
        payload_hash.as_str(),
        STREAMING_SIGNED | STREAMING_SIGNED_TRAILER
    ) {
        verify_streaming_chunks(
            creds,
            req,
            date_stamp,
            region,
            expected_service,
            &expected,
            &payload_hash,
        )?;
    }
    Ok(())
}

fn verify_header_shape(
    req: &SignedRequest,
    auth: &str,
    expected_service: &str,
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
    let Some((_, _, _, service)) = parse_credential_scope(credential) else {
        return Err(err("IncompleteSignatureException", 400));
    };
    if signed_headers.is_empty() || signature.is_empty() {
        return Err(err("IncompleteSignatureException", 400));
    }
    if service != expected_service || !is_lower_hex(signature, 64) {
        return Err(err("IncompleteSignatureException", 400));
    }
    // SigV4a chunk signatures need a SigV4a seed signature.
    if req.header("x-amz-date").is_empty()
        || devcloud_sigv4a::is_streaming(req.header("x-amz-content-sha256"))
    {
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
    if matches!(
        payload_hash,
        STREAMING_SIGNED
            | STREAMING_SIGNED_TRAILER
            | STREAMING_UNSIGNED_TRAILER
            | devcloud_sigv4a::STREAMING_PAYLOAD
            | devcloud_sigv4a::STREAMING_PAYLOAD_TRAILER
    ) {
        verify_chunked_framing(req, payload_hash)?;
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

struct Chunk<'a> {
    signature: Option<&'a str>,
    data: &'a [u8],
}

struct ChunkedBody<'a> {
    chunks: Vec<Chunk<'a>>,
    trailers: Vec<(&'a str, &'a str)>,
    trailer_signature: Option<&'a str>,
}

impl ChunkedBody<'_> {
    fn payload_len(&self) -> usize {
        self.chunks.iter().map(|c| c.data.len()).sum()
    }

    fn payload(&self) -> Vec<u8> {
        self.chunks.iter().flat_map(|c| c.data).copied().collect()
    }
}

pub fn is_aws_chunked(content_sha256: &str, content_encoding: &str) -> bool {
    content_sha256.starts_with("STREAMING-")
        || content_encoding
            .split(',')
            .any(|e| e.trim().eq_ignore_ascii_case("aws-chunked"))
}

pub fn decode_aws_chunked(
    body: &[u8],
    decoded_content_length: &str,
) -> Result<Vec<u8>, SignatureError> {
    let parsed =
        parse_chunked_body(body).ok_or_else(|| err("IncompleteSignatureException", 400))?;
    let decoded = parsed.payload();
    let decoded_length = decoded_content_length.trim();
    if !decoded_length.is_empty() && decoded_length.parse::<usize>().ok() != Some(decoded.len()) {
        return Err(err("IncompleteSignatureException", 400));
    }
    Ok(decoded)
}

fn parse_chunked_body(body: &[u8]) -> Option<ChunkedBody<'_>> {
    let mut rest = body;
    let mut chunks = Vec::new();
    loop {
        let line = take_chunk_line(&mut rest)?;
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
        let line = take_chunk_line(&mut rest)?;
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

fn take_chunk_line<'a>(rest: &mut &'a [u8]) -> Option<&'a str> {
    let end = rest.windows(2).position(|w| w == b"\r\n")?;
    let line = std::str::from_utf8(&rest[..end]).ok()?;
    *rest = &rest[end + 2..];
    Some(line)
}

fn verify_chunked_framing(req: &SignedRequest, payload_hash: &str) -> Result<(), SignatureError> {
    let parsed =
        parse_chunked_body(req.body).ok_or_else(|| err("IncompleteSignatureException", 400))?;
    let decoded_length = req.header("x-amz-decoded-content-length").trim();
    if !decoded_length.is_empty()
        && decoded_length.parse::<usize>().ok() != Some(parsed.payload_len())
    {
        return Err(err("IncompleteSignatureException", 400));
    }
    if matches!(payload_hash, STREAMING_SIGNED | STREAMING_SIGNED_TRAILER) {
        if !parsed
            .chunks
            .iter()
            .all(|c| c.signature.is_some_and(|s| is_lower_hex(s, 64)))
        {
            return Err(err("InvalidSignatureException", 403));
        }
    }
    if payload_hash == STREAMING_SIGNED_TRAILER
        && !parsed
            .trailer_signature
            .is_some_and(|s| is_lower_hex(s, 64))
    {
        return Err(err("InvalidSignatureException", 403));
    }
    if devcloud_sigv4a::is_streaming(payload_hash) {
        let shaped = |s: Option<&str>| s.is_some_and(devcloud_sigv4a::is_signature_shape);
        if !parsed.chunks.iter().all(|c| shaped(c.signature))
            || (payload_hash == devcloud_sigv4a::STREAMING_PAYLOAD_TRAILER
                && !shaped(parsed.trailer_signature))
        {
            return Err(err("InvalidSignatureException", 403));
        }
    }
    Ok(())
}

/// SigV4a (`AWS4-ECDSA-P256-SHA256`) counterpart of
/// `verify_authorization_header`: the scope has no region (`X-Amz-Region-Set`
/// must cover the configured one) and the signature is ECDSA P-256 over the
/// same canonical request.
fn verify_sigv4a_header(
    creds: &Credentials,
    req: &SignedRequest,
    auth: &str,
    expected_service: &str,
) -> Result<(), SignatureError> {
    let parsed = devcloud_sigv4a::parse_authorization(auth)
        .ok_or_else(|| err("IncompleteSignatureException", 400))?;
    if parsed.access_key != creds.access_key() || parsed.service != expected_service {
        return Err(err("UnrecognizedClientException", 403));
    }
    let payload_hash = sigv4a_payload_hash(req)?;
    if !devcloud_sigv4a::region_set_matches(req.header("x-amz-region-set"), creds.region()) {
        return Err(err("IncompleteSignatureException", 400));
    }
    let amz_date = req.header("x-amz-date");
    let canonical = canonical_request(req, parsed.signed_headers, payload_hash);
    let scope = devcloud_sigv4a::credential_scope(parsed.date_stamp, expected_service);
    let string_to_sign = devcloud_sigv4a::string_to_sign(amz_date, &scope, &canonical);
    let mismatch = || err("InvalidSignatureException", 403);
    let key = SigningKey::derive(parsed.access_key, creds.secret()).ok_or_else(mismatch)?;
    if !key.verify(string_to_sign.as_bytes(), parsed.signature) {
        return Err(mismatch());
    }
    if devcloud_sigv4a::is_streaming(payload_hash) {
        let body =
            parse_chunked_body(req.body).ok_or_else(|| err("IncompleteSignatureException", 400))?;
        let chain = ChunkChain::new(key, amz_date, &scope, parsed.signature);
        let trailer = (payload_hash == devcloud_sigv4a::STREAMING_PAYLOAD_TRAILER)
            .then_some((body.trailer_signature, body.trailers.as_slice()));
        let chunks = body.chunks.iter().map(|c| (c.signature, c.data));
        if !devcloud_sigv4a::verify_chunk_chain(chain, chunks, trailer) {
            return Err(mismatch());
        }
    }
    Ok(())
}

/// SigV4a counterpart of `verify_header_shape` for `signed-relaxed`.
fn verify_sigv4a_shape(
    req: &SignedRequest,
    auth: &str,
    expected_service: &str,
) -> Result<(), SignatureError> {
    let parsed = devcloud_sigv4a::parse_authorization(auth)
        .ok_or_else(|| err("IncompleteSignatureException", 400))?;
    if parsed.service != expected_service || !devcloud_sigv4a::is_signature_shape(parsed.signature)
    {
        return Err(err("IncompleteSignatureException", 400));
    }
    sigv4a_payload_hash(req).map(|_| ())
}

/// Checks the headers every SigV4a request carries and returns the payload
/// hash it signs (`UNSIGNED-PAYLOAD` when `x-amz-content-sha256` is absent).
fn sigv4a_payload_hash<'a>(req: &SignedRequest<'a>) -> Result<&'a str, SignatureError> {
    let payload_hash = req
        .headers
        .get("x-amz-content-sha256")
        .map_or("", String::as_str);
    // An ECDSA seed signature cannot start an HMAC chunk chain.
    if req.header("x-amz-date").is_empty()
        || req.header("x-amz-region-set").trim().is_empty()
        || matches!(payload_hash, STREAMING_SIGNED | STREAMING_SIGNED_TRAILER)
    {
        return Err(err("IncompleteSignatureException", 400));
    }
    if payload_hash.is_empty() {
        return Ok("UNSIGNED-PAYLOAD");
    }
    verify_payload_hash(req, payload_hash)?;
    Ok(payload_hash)
}

fn verify_streaming_chunks(
    creds: &Credentials,
    req: &SignedRequest,
    date_stamp: &str,
    region: &str,
    service: &str,
    seed_signature: &str,
    payload_hash: &str,
) -> Result<(), SignatureError> {
    let parsed =
        parse_chunked_body(req.body).ok_or_else(|| err("IncompleteSignatureException", 400))?;
    let signing_key = derive_signing_key(creds.secret(), date_stamp, region, service);
    let amz_date = req.header("x-amz-date");
    let scope = [date_stamp, region, service, "aws4_request"].join("/");
    let sign = |algorithm: &str, previous: &str, parts: &[&str]| {
        let mut fields = vec![algorithm, amz_date, &scope, previous];
        fields.extend_from_slice(parts);
        hex::encode(hmac_sha256(&signing_key, fields.join("\n").as_bytes()))
    };
    let mismatch = || err("InvalidSignatureException", 403);
    let empty_hash = sha256_hex(b"");
    let mut previous = seed_signature.to_string();
    for chunk in &parsed.chunks {
        let expected = sign(
            CHUNK_ALGORITHM,
            &previous,
            &[&empty_hash, &sha256_hex(chunk.data)],
        );
        let actual = chunk.signature.ok_or_else(mismatch)?;
        if !constant_time_eq(actual.as_bytes(), expected.as_bytes()) {
            return Err(mismatch());
        }
        previous = expected;
    }
    if payload_hash == STREAMING_SIGNED_TRAILER {
        let canonical_trailers: String = parsed
            .trailers
            .iter()
            .map(|(name, value)| format!("{}:{value}\n", name.to_ascii_lowercase()))
            .collect();
        let expected = sign(
            TRAILER_ALGORITHM,
            &previous,
            &[&sha256_hex(canonical_trailers.as_bytes())],
        );
        let actual = parsed.trailer_signature.ok_or_else(mismatch)?;
        if !constant_time_eq(actual.as_bytes(), expected.as_bytes()) {
            return Err(mismatch());
        }
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
    signature_for_request_with_service(
        creds,
        req,
        date_stamp,
        region,
        SERVICE,
        signed_headers,
        payload_hash,
    )
}

/// Computes the expected request signature for an explicit AWS service name.
pub fn signature_for_request_with_service(
    creds: &Credentials,
    req: &SignedRequest,
    date_stamp: &str,
    region: &str,
    service: &str,
    signed_headers: &str,
    payload_hash: &str,
) -> String {
    let canonical_request = canonical_request(req, signed_headers, payload_hash);
    let amz_date = req.header("x-amz-date");
    let scope = [date_stamp, region, service, "aws4_request"].join("/");
    let string_to_sign = [
        ALGORITHM.to_string(),
        amz_date.to_string(),
        scope,
        sha256_hex(canonical_request.as_bytes()),
    ]
    .join("\n");
    let signing_key = derive_signing_key(creds.secret(), date_stamp, region, service);
    hex::encode(hmac_sha256(&signing_key, string_to_sign.as_bytes()))
}

/// The SigV4 canonical request (shared by SigV4 and SigV4a).
fn canonical_request(req: &SignedRequest, signed_headers: &str, payload_hash: &str) -> String {
    [
        req.method.to_string(),
        canonical_uri(req.path),
        canonical_query_string(req.query),
        canonical_headers(req, signed_headers),
        signed_headers.to_lowercase(),
        payload_hash.to_string(),
    ]
    .join("\n")
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
                if let (Some(hi), Some(lo)) = (
                    (bytes[i + 1] as char).to_digit(16),
                    (bytes[i + 2] as char).to_digit(16),
                ) {
                    out.push((hi * 16 + lo) as u8);
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

fn derive_signing_key(secret: &str, date_stamp: &str, region: &str, service: &str) -> Vec<u8> {
    let date_key = hmac_sha256(format!("AWS4{secret}").as_bytes(), date_stamp.as_bytes());
    let region_key = hmac_sha256(&date_key, region.as_bytes());
    let service_key = hmac_sha256(&region_key, service.as_bytes());
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

    fn sign_streaming_chunks(
        chunks: &[&[u8]],
        seed: &str,
        amz_date: &str,
        trailer: Option<&str>,
    ) -> Vec<u8> {
        let date_stamp = &amz_date[..8];
        let key = derive_signing_key("dev", date_stamp, "us-east-1", SERVICE);
        let scope = format!("{date_stamp}/us-east-1/{SERVICE}/aws4_request");
        let empty = sha256_hex(b"");
        let mut previous = seed.to_string();
        let mut out = Vec::new();
        for chunk in chunks.iter().copied().chain([&b""[..]]) {
            let to_sign = format!(
                "{CHUNK_ALGORITHM}\n{amz_date}\n{scope}\n{previous}\n{empty}\n{}",
                sha256_hex(chunk)
            );
            previous = hex::encode(hmac_sha256(&key, to_sign.as_bytes()));
            out.extend(format!("{:x};chunk-signature={previous}\r\n", chunk.len()).into_bytes());
            out.extend_from_slice(chunk);
            out.extend_from_slice(b"\r\n");
        }
        if let Some(trailer) = trailer {
            out.truncate(out.len() - 2);
            let to_sign = format!(
                "{TRAILER_ALGORITHM}\n{amz_date}\n{scope}\n{previous}\n{}",
                sha256_hex(format!("{trailer}\n").as_bytes())
            );
            let sig = hex::encode(hmac_sha256(&key, to_sign.as_bytes()));
            out.extend(format!("{trailer}\r\nx-amz-trailer-signature:{sig}\r\n\r\n").into_bytes());
        }
        out
    }

    #[test]
    fn streaming_payloads_verify_in_strict_and_signed_relaxed_modes() {
        let strict = Credentials {
            auth_mode: "strict",
            region: "us-east-1",
            access_key_id: "dev",
            secret_access_key: "dev",
        };
        let signed_relaxed = Credentials {
            auth_mode: "signed-relaxed",
            ..strict
        };
        let date_stamp = "20260501";
        let amz_date = "20260501T000000Z";
        let signed_headers = "host;x-amz-content-sha256;x-amz-date";

        let build_req = |payload_hash: &str,
                         chunks: &[&[u8]],
                         trailer: Option<&str>,
                         decoded_len: Option<&str>,
                         override_body: Option<Vec<u8>>| {
            let mut h = headers(&[
                ("x-amz-content-sha256", payload_hash),
                ("x-amz-date", amz_date),
            ]);
            if let Some(len) = decoded_len {
                h.insert("x-amz-decoded-content-length".to_string(), len.to_string());
            }
            let tmp = SignedRequest {
                method: "POST",
                path: "/",
                query: "",
                host: "localhost",
                headers: &h,
                body: b"",
            };
            let seed = signature_for_request(
                &strict,
                &tmp,
                date_stamp,
                "us-east-1",
                signed_headers,
                payload_hash,
            );
            h.insert(
                "authorization".to_string(),
                format!(
                    "{ALGORITHM} Credential=dev/{date_stamp}/us-east-1/dynamodb/aws4_request, \
                     SignedHeaders={signed_headers}, Signature={seed}"
                ),
            );
            let body = override_body
                .unwrap_or_else(|| sign_streaming_chunks(chunks, &seed, amz_date, trailer));
            (h, body)
        };

        // STREAMING_SIGNED round-trip.
        let (h, body) = build_req(STREAMING_SIGNED, &[b"{", b"}"], None, Some("2"), None);
        let req = SignedRequest {
            method: "POST",
            path: "/",
            query: "",
            host: "localhost",
            headers: &h,
            body: &body,
        };
        assert_eq!(verify_signature(&strict, &req), Ok(()));
        assert_eq!(verify_signature(&signed_relaxed, &req), Ok(()));

        // Tampered chunk fails in strict mode.
        let mut tampered = body.clone();
        let pos = tampered.iter().position(|&b| b == b'{').unwrap();
        tampered[pos] = b'[';
        let req_bad = SignedRequest {
            body: &tampered,
            ..req
        };
        assert_eq!(
            verify_signature(&strict, &req_bad),
            Err(err("InvalidSignatureException", 403))
        );

        // STREAMING_SIGNED_TRAILER round-trip & tamper.
        let (h, body) = build_req(
            STREAMING_SIGNED_TRAILER,
            &[b"{}"],
            Some("x-amz-checksum-crc32:AAAA"),
            Some("2"),
            None,
        );
        let req = SignedRequest {
            method: "POST",
            path: "/",
            query: "",
            host: "localhost",
            headers: &h,
            body: &body,
        };
        assert_eq!(verify_signature(&strict, &req), Ok(()));
        assert_eq!(verify_signature(&signed_relaxed, &req), Ok(()));
        let mut tampered_trailer = body.clone();
        let tpos = tampered_trailer
            .windows(4)
            .position(|w| w == b"AAAA")
            .unwrap();
        tampered_trailer[tpos] = b'B';
        let req_bad_trailer = SignedRequest {
            body: &tampered_trailer,
            ..req
        };
        assert_eq!(
            verify_signature(&strict, &req_bad_trailer),
            Err(err("InvalidSignatureException", 403))
        );

        // STREAMING_UNSIGNED_TRAILER.
        let (h, body) = build_req(
            STREAMING_UNSIGNED_TRAILER,
            &[],
            None,
            Some("2"),
            Some(b"2\r\n{}\r\n0\r\nx-amz-checksum-crc32:AAAA\r\n\r\n".to_vec()),
        );
        let req = SignedRequest {
            method: "POST",
            path: "/",
            query: "",
            host: "localhost",
            headers: &h,
            body: &body,
        };
        assert_eq!(verify_signature(&strict, &req), Ok(()));
        assert_eq!(verify_signature(&signed_relaxed, &req), Ok(()));

        // Malformed chunked body.
        let (h, body) = build_req(
            STREAMING_UNSIGNED_TRAILER,
            &[],
            None,
            None,
            Some(b"plain".to_vec()),
        );
        let req = SignedRequest {
            method: "POST",
            path: "/",
            query: "",
            host: "localhost",
            headers: &h,
            body: &body,
        };
        assert_eq!(
            verify_signature(&strict, &req),
            Err(err("IncompleteSignatureException", 400))
        );

        // SigV4a chunk signatures cannot be chained from an HMAC seed.
        let (h, body) = build_req(
            devcloud_sigv4a::STREAMING_PAYLOAD,
            &[],
            None,
            None,
            Some(b"0\r\n\r\n".to_vec()),
        );
        let req = SignedRequest {
            method: "POST",
            path: "/",
            query: "",
            host: "localhost",
            headers: &h,
            body: &body,
        };
        assert_eq!(
            verify_signature(&strict, &req),
            Err(err("IncompleteSignatureException", 400))
        );
    }

    /// Signs a SigV4a request as `dev`/`dev` for `service`, frames `chunks`
    /// when the payload is streaming, applies `tamper` to the body, and
    /// verifies it under `creds`.
    fn sigv4a_check(
        creds: &Credentials,
        service: &str,
        payload_hash: &str,
        chunks: &[&[u8]],
        trailers: Option<&[(&str, &str)]>,
        region_set: &str,
        tamper: fn(&mut Vec<u8>),
    ) -> Result<(), SignatureError> {
        let amz_date = "20261125T000000Z";
        let scope = devcloud_sigv4a::credential_scope("20261125", service);
        let mut h = headers(&[("x-amz-date", amz_date), ("x-amz-region-set", region_set)]);
        let mut signed_headers = "host;x-amz-date;x-amz-region-set";
        if !payload_hash.is_empty() {
            h.insert("x-amz-content-sha256".to_string(), payload_hash.to_string());
            signed_headers = "host;x-amz-content-sha256;x-amz-date;x-amz-region-set";
        }
        let signed_hash = if payload_hash.is_empty() {
            "UNSIGNED-PAYLOAD"
        } else {
            payload_hash
        };
        let tmp = SignedRequest {
            method: "POST",
            path: "/",
            query: "",
            host: "localhost",
            headers: &h,
            body: b"",
        };
        let key = SigningKey::derive("dev", "dev").unwrap();
        let canonical = canonical_request(&tmp, signed_headers, signed_hash);
        let seed =
            key.sign(devcloud_sigv4a::string_to_sign(amz_date, &scope, &canonical).as_bytes());
        h.insert(
            "authorization".to_string(),
            format!(
                "{} Credential=dev/{scope}, SignedHeaders={signed_headers}, Signature={seed}",
                devcloud_sigv4a::ALGORITHM
            ),
        );
        let mut body = if devcloud_sigv4a::is_streaming(payload_hash) {
            let mut chain = ChunkChain::new(key, amz_date, &scope, &seed);
            devcloud_sigv4a::encode_signed_chunks(&mut chain, chunks, trailers, true)
        } else {
            chunks.concat()
        };
        tamper(&mut body);
        let req = SignedRequest {
            method: "POST",
            path: "/",
            query: "",
            host: "localhost",
            headers: &h,
            body: &body,
        };
        verify_signature_for_service(creds, &req, service)
    }

    #[test]
    fn sigv4a_header_and_streaming_payloads_verify() {
        let strict = Credentials {
            auth_mode: "strict",
            region: "us-east-1",
            access_key_id: "dev",
            secret_access_key: "dev",
        };
        let signed_relaxed = Credentials {
            auth_mode: "signed-relaxed",
            ..strict
        };
        let wrong_secret = Credentials {
            secret_access_key: "other",
            ..strict
        };
        let relaxed = Credentials {
            auth_mode: "relaxed",
            ..wrong_secret
        };
        let keep: fn(&mut Vec<u8>) = |_| {};
        let flip_data: fn(&mut Vec<u8>) = |body| {
            let at = body.iter().position(|&b| b == b'{').unwrap();
            body[at] = b'[';
        };
        let trailer = [("x-amz-checksum-crc32", "AAAA")];
        let streaming = devcloud_sigv4a::STREAMING_PAYLOAD;
        let streaming_trailer = devcloud_sigv4a::STREAMING_PAYLOAD_TRAILER;
        let body_hash = sha256_hex(b"{}");
        let mismatch = Err(err("InvalidSignatureException", 403));
        let malformed = Err(err("IncompleteSignatureException", 400));

        for (name, creds, service, hash, trailers, region_set, tamper, expected) in [
            (
                "signed body",
                &strict,
                SERVICE,
                body_hash.as_str(),
                None,
                "*",
                keep,
                Ok(()),
            ),
            (
                "unsigned payload",
                &strict,
                SERVICE,
                "",
                None,
                "us-east-1",
                keep,
                Ok(()),
            ),
            (
                "streaming",
                &strict,
                SERVICE,
                streaming,
                None,
                "eu-west-1,us-east-1",
                keep,
                Ok(()),
            ),
            ("dax", &strict, "dax", streaming, None, "*", keep, Ok(())),
            (
                "streaming trailer",
                &strict,
                SERVICE,
                streaming_trailer,
                Some(&trailer[..]),
                "*",
                keep,
                Ok(()),
            ),
            (
                "shape only",
                &signed_relaxed,
                SERVICE,
                streaming,
                None,
                "*",
                keep,
                Ok(()),
            ),
            (
                "relaxed",
                &relaxed,
                SERVICE,
                streaming,
                None,
                "*",
                flip_data,
                Ok(()),
            ),
            (
                "tampered chunk",
                &strict,
                SERVICE,
                streaming,
                None,
                "*",
                flip_data,
                mismatch.clone(),
            ),
            (
                "tampered body",
                &strict,
                SERVICE,
                body_hash.as_str(),
                None,
                "*",
                flip_data,
                Err(err("InvalidSignatureException", 403)),
            ),
            (
                "wrong secret",
                &wrong_secret,
                SERVICE,
                streaming,
                None,
                "*",
                keep,
                mismatch.clone(),
            ),
            (
                "region set",
                &strict,
                SERVICE,
                streaming,
                None,
                "eu-west-1",
                keep,
                malformed.clone(),
            ),
            (
                "hmac chain with ecdsa seed",
                &strict,
                SERVICE,
                STREAMING_SIGNED,
                None,
                "*",
                keep,
                malformed.clone(),
            ),
        ] {
            assert_eq!(
                sigv4a_check(
                    creds,
                    service,
                    hash,
                    &[b"{", b"}"],
                    trailers,
                    region_set,
                    tamper
                ),
                expected,
                "{name}"
            );
        }
    }
}

#[cfg(test)]
mod percent_decode_tests {
    use super::*;

    #[test]
    fn malformed_percent_escapes_next_to_unicode_do_not_panic() {
        for value in ["%あ", "%aあ", "猫%猫", "%é", "%💾", "%", "%2", "%zz"] {
            assert_eq!(url_decode(value), value);
        }
    }

    #[test]
    fn valid_percent_escapes_and_plus_are_preserved() {
        assert_eq!(url_decode("%E7%8C%AB+%2B%2F"), "猫 +/");
    }
}
