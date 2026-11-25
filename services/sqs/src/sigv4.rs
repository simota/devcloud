//! Mirrors `internal/services/sqs/sigv4.rs`.
//!
//! AWS SigV4 verification for the SQS service (`service = "sqs"`), with the same
//! error codes + HTTP statuses the legacy server returns. Relaxed mode (default)
//! accepts everything; strict mode performs a full canonical-request signature
//! comparison. The signer is exposed (`signature_for_request`) so tests can sign
//! a request and assert round-trip acceptance. SigV4a (`AWS4-ECDSA-P256-SHA256`)
//! headers and their `aws-chunked` chunk signatures are verified too.

use devcloud_sigv4a::{ChunkChain, SigningKey};
use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};

type HmacSha256 = Hmac<Sha256>;

pub const SIGV4_ALGORITHM: &str = "AWS4-HMAC-SHA256";
pub const SIGV4_SERVICE: &str = "sqs";
pub const SIGV4_CHUNK_ALGORITHM: &str = "AWS4-HMAC-SHA256-PAYLOAD";
pub const SIGV4_TRAILER_ALGORITHM: &str = "AWS4-HMAC-SHA256-TRAILER";
pub const STREAMING_SIGNED: &str = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD";
pub const STREAMING_SIGNED_TRAILER: &str = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER";
pub const STREAMING_UNSIGNED_TRAILER: &str = "STREAMING-UNSIGNED-PAYLOAD-TRAILER";

/// A signature failure carrying the AWS error code + HTTP status from the legacy
/// server.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SignatureError {
    pub code: &'static str,
    pub status: u16,
}

fn err(code: &'static str, status: u16) -> SignatureError {
    SignatureError { code, status }
}

/// Minimal request view the verifier needs.
pub struct SignedRequest<'a> {
    pub method: &'a str,
    pub path: &'a str,
    /// Raw query string (`a=1&b=2`), as received.
    pub query: &'a str,
    pub host: &'a str,
    pub authorization: &'a str,
    pub amz_date: &'a str,
    pub content_sha256: &'a str,
    /// Resolves a header value by lowercase name.
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
        return Err(err("AccessDenied", 403));
    }
    if devcloud_sigv4a::is_authorization(auth) {
        return verify_sigv4a(req, auth, creds);
    }
    let prefix = format!("{SIGV4_ALGORITHM} ");
    if !auth.starts_with(&prefix) {
        return Err(err("AuthorizationHeaderMalformed", 400));
    }
    let values = parse_auth_params(&auth[prefix.len()..]);
    let credential = values.get("Credential").cloned().unwrap_or_default();
    let signed_headers = values.get("SignedHeaders").cloned().unwrap_or_default();
    let signature = values.get("Signature").cloned().unwrap_or_default();
    let (access_key, date_stamp, region, service) = match parse_credential_scope(&credential) {
        Some(v) => v,
        None => return Err(err("AuthorizationHeaderMalformed", 400)),
    };
    if signed_headers.is_empty() || signature.is_empty() {
        return Err(err("AuthorizationHeaderMalformed", 400));
    }
    if !valid_credential(&access_key, &region, &service, creds) {
        return Err(err("InvalidAccessKeyId", 403));
    }
    // SigV4a chunk signatures need a SigV4a seed signature.
    if req.amz_date.is_empty() || devcloud_sigv4a::is_streaming(req.content_sha256) {
        return Err(err("AuthorizationHeaderMalformed", 400));
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
        return Err(err("SignatureDoesNotMatch", 403));
    }
    if matches!(
        payload_hash.as_str(),
        STREAMING_SIGNED | STREAMING_SIGNED_TRAILER
    ) {
        verify_streaming_chunks(req, &date_stamp, &region, &expected, creds, &payload_hash)?;
    }
    Ok(())
}

/// SigV4a (`AWS4-ECDSA-P256-SHA256`) verification: the scope has no region
/// (`X-Amz-Region-Set` must cover the configured one) and the signature is
/// ECDSA P-256 over the same canonical request.
fn verify_sigv4a(
    req: &SignedRequest,
    auth: &str,
    creds: &Credentials,
) -> Result<(), SignatureError> {
    let malformed = || err("AuthorizationHeaderMalformed", 400);
    let parsed = devcloud_sigv4a::parse_authorization(auth).ok_or_else(malformed)?;
    if parsed.access_key != default_str(creds.access_key_id, "dev")
        || parsed.service != SIGV4_SERVICE
    {
        return Err(err("InvalidAccessKeyId", 403));
    }
    let region_set = (req.header)("x-amz-region-set").unwrap_or_default();
    // An ECDSA seed signature cannot start an HMAC chunk chain.
    if req.amz_date.is_empty()
        || !devcloud_sigv4a::region_set_matches(&region_set, default_str(creds.region, "us-east-1"))
        || matches!(
            req.content_sha256,
            STREAMING_SIGNED | STREAMING_SIGNED_TRAILER
        )
    {
        return Err(malformed());
    }
    let payload_hash = if req.content_sha256.is_empty() {
        "UNSIGNED-PAYLOAD"
    } else {
        verify_payload_hash(req, req.content_sha256)?;
        req.content_sha256
    };
    let canonical = canonical_request(req, parsed.signed_headers, payload_hash);
    let scope = devcloud_sigv4a::credential_scope(parsed.date_stamp, SIGV4_SERVICE);
    let string_to_sign = devcloud_sigv4a::string_to_sign(req.amz_date, &scope, &canonical);
    let mismatch = || err("SignatureDoesNotMatch", 403);
    let key = SigningKey::derive(
        parsed.access_key,
        default_str(creds.secret_access_key, "dev"),
    )
    .ok_or_else(mismatch)?;
    if !key.verify(string_to_sign.as_bytes(), parsed.signature) {
        return Err(mismatch());
    }
    if devcloud_sigv4a::is_streaming(payload_hash) {
        let body = parse_chunked_body(req.body).ok_or_else(malformed)?;
        let chain = ChunkChain::new(key, req.amz_date, &scope, parsed.signature);
        let trailer = (payload_hash == devcloud_sigv4a::STREAMING_PAYLOAD_TRAILER)
            .then_some((body.trailer_signature, body.trailers.as_slice()));
        let chunks = body.chunks.iter().map(|c| (c.signature, c.data));
        if !devcloud_sigv4a::verify_chunk_chain(chain, chunks, trailer) {
            return Err(mismatch());
        }
    }
    Ok(())
}

fn valid_credential(access_key: &str, region: &str, service: &str, creds: &Credentials) -> bool {
    access_key == default_str(creds.access_key_id, "dev")
        && region == default_str(creds.region, "us-east-1")
        && service == SIGV4_SERVICE
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
        verify_chunked_framing(req)?;
        return Ok(());
    }
    if payload_hash.starts_with("STREAMING-") {
        return Err(err("NotImplemented", 501));
    }
    let got = sha256_hex(req.body);
    if !constant_time_eq(payload_hash.to_ascii_lowercase().as_bytes(), got.as_bytes()) {
        return Err(err("SignatureDoesNotMatch", 403));
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
        parse_chunked_body(body).ok_or_else(|| err("AuthorizationHeaderMalformed", 400))?;
    let decoded = parsed.payload();
    let decoded_length = decoded_content_length.trim();
    if !decoded_length.is_empty() && decoded_length.parse::<usize>().ok() != Some(decoded.len()) {
        return Err(err("AuthorizationHeaderMalformed", 400));
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

fn verify_chunked_framing(req: &SignedRequest) -> Result<(), SignatureError> {
    let parsed =
        parse_chunked_body(req.body).ok_or_else(|| err("AuthorizationHeaderMalformed", 400))?;
    if let Some(decoded_length) = (req.header)("x-amz-decoded-content-length") {
        let decoded_length = decoded_length.trim();
        if !decoded_length.is_empty()
            && decoded_length.parse::<usize>().ok() != Some(parsed.payload_len())
        {
            return Err(err("AuthorizationHeaderMalformed", 400));
        }
    }
    Ok(())
}

fn verify_streaming_chunks(
    req: &SignedRequest,
    date_stamp: &str,
    region: &str,
    seed_signature: &str,
    creds: &Credentials,
    payload_hash: &str,
) -> Result<(), SignatureError> {
    let parsed =
        parse_chunked_body(req.body).ok_or_else(|| err("AuthorizationHeaderMalformed", 400))?;
    let signing_key = derive_signing_key(creds.secret_access_key, date_stamp, region);
    let scope = [date_stamp, region, SIGV4_SERVICE, "aws4_request"].join("/");
    let sign = |algorithm: &str, previous: &str, parts: &[&str]| {
        let mut fields = vec![algorithm, req.amz_date, &scope, previous];
        fields.extend_from_slice(parts);
        hex_lower(&hmac_sha256(&signing_key, fields.join("\n").as_bytes()))
    };
    let mismatch = || err("SignatureDoesNotMatch", 403);
    let empty_hash = sha256_hex(b"");
    let mut previous = seed_signature.to_string();
    for chunk in &parsed.chunks {
        let expected = sign(
            SIGV4_CHUNK_ALGORITHM,
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
            SIGV4_TRAILER_ALGORITHM,
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

/// Computes the SigV4 hex signature for a request. Public so tests can sign.
pub fn signature_for_request(
    req: &SignedRequest,
    date_stamp: &str,
    region: &str,
    signed_headers: &str,
    payload_hash: &str,
    creds: &Credentials,
) -> String {
    let canonical_request = canonical_request(req, signed_headers, payload_hash);
    let scope = [date_stamp, region, SIGV4_SERVICE, "aws4_request"].join("/");
    let string_to_sign = [
        SIGV4_ALGORITHM,
        req.amz_date,
        &scope,
        &sha256_hex(canonical_request.as_bytes()),
    ]
    .join("\n");
    let key = derive_signing_key(creds.secret_access_key, date_stamp, region);
    hex_lower(&hmac_sha256(&key, string_to_sign.as_bytes()))
}

/// The SigV4 canonical request (shared by SigV4 and SigV4a). Public so tests can sign.
pub fn canonical_request(req: &SignedRequest, signed_headers: &str, payload_hash: &str) -> String {
    [
        req.method,
        &canonical_uri(req.path),
        &canonical_query_string(req.query),
        &canonical_headers(req, signed_headers),
        &signed_headers.to_ascii_lowercase(),
        payload_hash,
    ]
    .join("\n")
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

/// Mirrors `canonicalQueryString`: decode each `k=v`, sort by (key, value),
/// re-encode. Operates on the raw query string.
pub fn canonical_query_string(query: &str) -> String {
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

/// Mirrors `normalizeHeaderValue`: collapse runs of whitespace to single spaces.
pub fn normalize_header_value(value: &str) -> String {
    value.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// Mirrors `awsPercentEncode`.
pub fn aws_percent_encode(value: &str, safe: &str) -> String {
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
            out.push('%');
            out.push_str(&format!("{:02X}", c));
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

/// Lowercase hex SHA-256. Public for the canonical-payload tests.
pub fn sha256_hex(value: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(value);
    hex_lower(&hasher.finalize())
}

fn hex_lower(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push_str(&format!("{:02x}", b));
    }
    s
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
