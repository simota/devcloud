//! AWS SigV4 verification (relaxed / signed-relaxed / strict), shared shape
//! with `services/applicationautoscaling/src/sigv4.rs`.

use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};

type HmacSha256 = Hmac<Sha256>;

pub const SIGV4_ALGORITHM: &str = "AWS4-HMAC-SHA256";
pub const SIGV4_SERVICE: &str = "ssm";
pub const SIGV4_CHUNK_ALGORITHM: &str = "AWS4-HMAC-SHA256-PAYLOAD";
pub const SIGV4_TRAILER_ALGORITHM: &str = "AWS4-HMAC-SHA256-TRAILER";
pub const STREAMING_SIGNED: &str = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD";
pub const STREAMING_SIGNED_TRAILER: &str = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER";
pub const STREAMING_UNSIGNED_TRAILER: &str = "STREAMING-UNSIGNED-PAYLOAD-TRAILER";

/// A signature verification failure, carrying the AWS error name and HTTP
/// status the legacy server would return.
#[derive(Debug, Clone, PartialEq, Eq)]
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
    // SDKs sign the body's SHA-256 for this service without sending the
    // header (only S3 uses the header-driven `UNSIGNED-PAYLOAD` convention).
    let payload_hash = if req.content_sha256.is_empty() {
        sha256_hex(req.body)
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
    if matches!(
        payload_hash.as_str(),
        STREAMING_SIGNED | STREAMING_SIGNED_TRAILER
    ) {
        verify_streaming_chunks(req, &date_stamp, &region, &expected, creds, &payload_hash)?;
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
    if matches!(
        payload_hash,
        STREAMING_SIGNED | STREAMING_SIGNED_TRAILER | STREAMING_UNSIGNED_TRAILER
    ) {
        verify_chunked_framing(req, payload_hash)?;
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
    if let Some(decoded_length) = (req.header)("x-amz-decoded-content-length") {
        let decoded_length = decoded_length.trim();
        if !decoded_length.is_empty()
            && decoded_length.parse::<usize>().ok() != Some(parsed.payload_len())
        {
            return Err(err("IncompleteSignatureException", 400));
        }
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
        parse_chunked_body(req.body).ok_or_else(|| err("IncompleteSignatureException", 400))?;
    let signing_key = derive_signing_key(creds.secret_access_key, date_stamp, region);
    let scope = [date_stamp, region, SIGV4_SERVICE, "aws4_request"].join("/");
    let sign = |algorithm: &str, previous: &str, parts: &[&str]| {
        let mut fields = vec![algorithm, req.amz_date, &scope, previous];
        fields.extend_from_slice(parts);
        hex::encode(hmac_sha256(&signing_key, fields.join("\n").as_bytes()))
    };
    let mismatch = || err("InvalidSignatureException", 403);
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

    fn sign_streaming_chunks(
        chunks: &[&[u8]],
        seed: &str,
        amz_date: &str,
        trailer: Option<&str>,
    ) -> Vec<u8> {
        let date_stamp = &amz_date[..8];
        let key = derive_signing_key("dev", date_stamp, "us-east-1");
        let scope = format!("{date_stamp}/us-east-1/{SIGV4_SERVICE}/aws4_request");
        let empty = sha256_hex(b"");
        let mut previous = seed.to_string();
        let mut out = Vec::new();
        for chunk in chunks.iter().copied().chain([&b""[..]]) {
            let to_sign = format!(
                "{SIGV4_CHUNK_ALGORITHM}\n{amz_date}\n{scope}\n{previous}\n{empty}\n{}",
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
                "{SIGV4_TRAILER_ALGORITHM}\n{amz_date}\n{scope}\n{previous}\n{}",
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
            access_key_id: "dev",
            secret_access_key: "dev",
            region: "us-east-1",
        };
        let signed_relaxed = Credentials {
            auth_mode: "signed-relaxed",
            ..strict
        };
        let date_stamp = "20261005";
        let amz_date = "20261005T000000Z";
        let signed_headers = "host;x-amz-content-sha256;x-amz-date";

        let check = |creds: &Credentials,
                     payload_hash: &str,
                     chunks: &[&[u8]],
                     trailer: Option<&str>,
                     decoded_len: Option<&str>,
                     override_body: Option<&[u8]>| {
            let mut hmap = std::collections::HashMap::new();
            hmap.insert("x-amz-date".to_string(), amz_date.to_string());
            hmap.insert("x-amz-content-sha256".to_string(), payload_hash.to_string());
            if let Some(len) = decoded_len {
                hmap.insert("x-amz-decoded-content-length".to_string(), len.to_string());
            }
            let h1 = hmap.clone();
            let header_fn1 = move |name: &str| h1.get(name).cloned();
            let tmp = SignedRequest {
                method: "POST",
                path: "/",
                query: "",
                host: "localhost",
                authorization: "",
                amz_date,
                content_sha256: payload_hash,
                header: &header_fn1,
                body: b"",
            };
            let seed = signature_for_request(
                &tmp,
                date_stamp,
                "us-east-1",
                signed_headers,
                payload_hash,
                &strict,
            );
            let auth = format!(
                "{SIGV4_ALGORITHM} Credential=dev/{date_stamp}/us-east-1/{SIGV4_SERVICE}/aws4_request, \
                 SignedHeaders={signed_headers}, Signature={seed}"
            );
            let framed = sign_streaming_chunks(chunks, &seed, amz_date, trailer);
            let body = override_body.unwrap_or(&framed);
            let header_fn2 = move |name: &str| hmap.get(name).cloned();
            let req = SignedRequest {
                method: "POST",
                path: "/",
                query: "",
                host: "localhost",
                authorization: &auth,
                amz_date,
                content_sha256: payload_hash,
                header: &header_fn2,
                body,
            };
            verify_signature(&req, creds)
        };

        assert_eq!(
            check(
                &strict,
                STREAMING_SIGNED,
                &[b"{", b"}"],
                None,
                Some("2"),
                None
            ),
            Ok(())
        );
        assert_eq!(
            check(
                &signed_relaxed,
                STREAMING_SIGNED,
                &[b"{", b"}"],
                None,
                Some("2"),
                None
            ),
            Ok(())
        );
        assert_eq!(
            check(
                &strict,
                STREAMING_SIGNED,
                &[b"{}"],
                None,
                None,
                Some(b"2\r\n{}\r\n0\r\n\r\n")
            ),
            Err(err("InvalidSignatureException", 403))
        );
        assert_eq!(
            check(
                &strict,
                STREAMING_SIGNED_TRAILER,
                &[b"{}"],
                Some("x-amz-checksum-crc32:AAAA"),
                Some("2"),
                None
            ),
            Ok(())
        );
        assert_eq!(
            check(
                &strict,
                STREAMING_UNSIGNED_TRAILER,
                &[],
                None,
                Some("2"),
                Some(b"2\r\n{}\r\n0\r\nx-amz-checksum-crc32:AAAA\r\n\r\n")
            ),
            Ok(())
        );
        assert_eq!(
            check(
                &strict,
                STREAMING_UNSIGNED_TRAILER,
                &[],
                None,
                Some("5"),
                Some(b"2\r\n{}\r\n0\r\n\r\n")
            ),
            Err(err("IncompleteSignatureException", 400))
        );
        assert_eq!(
            check(
                &strict,
                "STREAMING-AWS4-ECDSA-P256-SHA256-PAYLOAD",
                &[],
                None,
                None,
                Some(b"0\r\n\r\n")
            ),
            Err(err("NotImplemented", 501))
        );
    }
}
