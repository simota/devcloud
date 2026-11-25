//! SigV2 (HMAC-SHA1) and SigV4a (ECDSA-P256-SHA256) signature verification.
//!
//! Pure-Rust SHA-1 and HMAC-SHA1; the P-256 key derivation, ECDSA, and the
//! SigV4a chunk-signature chain live in the workspace `devcloud-sigv4a` crate.

use devcloud_sigv4a::{ChunkChain, SigningKey};

use crate::base64;
use crate::http::{
    canonical_headers, canonical_query_string, canonical_uri, constant_time_eq, parse_auth_params,
    parse_sigv4_time, unix_now, verify_payload_hash, verify_scope_date,
    verify_signed_header_coverage, AuthConfig, Request, SignatureError,
};

pub(crate) const SIGV4A_ALGORITHM: &str = devcloud_sigv4a::ALGORITHM;
const SIGV4A_SERVICE: &str = "s3";
const MAX_CLOCK_SKEW_SECONDS: i64 = 900;

/// S3 subresources included in a SigV2 `CanonicalizedResource`.
const SIGV2_SUBRESOURCES: &[&str] = &[
    "acl",
    "analytics",
    "cors",
    "delete",
    "inventory",
    "legal-hold",
    "lifecycle",
    "location",
    "logging",
    "notification",
    "object-lock",
    "partNumber",
    "policy",
    "replication",
    "requestPayment",
    "response-cache-control",
    "response-content-disposition",
    "response-content-encoding",
    "response-content-language",
    "response-content-type",
    "response-expires",
    "restore",
    "retention",
    "select",
    "select-type",
    "tagging",
    "torrent",
    "uploadId",
    "uploads",
    "versionId",
    "versioning",
    "versions",
    "website",
];

// --- SigV2 ------------------------------------------------------------------

pub(crate) fn is_sigv2_header(req: &Request) -> bool {
    req.header("authorization").starts_with("AWS ")
}

pub(crate) fn is_sigv2_presign(req: &Request) -> bool {
    req.query
        .get("AWSAccessKeyId")
        .is_some_and(|v| !v.is_empty())
        && req.query.get("Signature").is_some_and(|v| !v.is_empty())
        && req.query.get("Expires").is_some_and(|v| !v.is_empty())
}

pub(crate) fn verify_sigv2_header(req: &Request, auth: &AuthConfig) -> Result<(), SignatureError> {
    let authorization = req.header("authorization");
    let Some(rest) = authorization.strip_prefix("AWS ") else {
        return Err(SignatureError {
            code: "AuthorizationHeaderMalformed",
            status: 400,
        });
    };
    let Some((access_key, signature)) = rest.split_once(':') else {
        return Err(SignatureError {
            code: "AuthorizationHeaderMalformed",
            status: 400,
        });
    };
    if access_key.is_empty() || signature.is_empty() {
        return Err(SignatureError {
            code: "AuthorizationHeaderMalformed",
            status: 400,
        });
    }
    let configured_access_key = configured_access_key(auth);
    if !constant_time_eq(access_key.as_bytes(), configured_access_key.as_bytes()) {
        return Err(SignatureError {
            code: "InvalidAccessKeyId",
            status: 403,
        });
    }

    let amz_date = req.header("x-amz-date");
    let date_header = req.header("date");
    let effective_date = if !amz_date.is_empty() {
        amz_date
    } else {
        date_header
    };
    if effective_date.is_empty() {
        return Err(SignatureError {
            code: "AccessDenied",
            status: 403,
        });
    }
    let signed_at = parse_sigv2_date(effective_date).ok_or(SignatureError {
        code: "AccessDenied",
        status: 403,
    })?;
    if (unix_now() - signed_at).abs() > MAX_CLOCK_SKEW_SECONDS {
        return Err(SignatureError {
            code: "RequestTimeTooSkewed",
            status: 403,
        });
    }

    // Per S3 SigV2, when `x-amz-date` is present the `Date` line is empty
    // because `x-amz-date` is signed in CanonicalizedAmzHeaders.
    let date_for_signing = if !amz_date.is_empty() {
        ""
    } else {
        date_header
    };
    let expected = sigv2_signature(req, auth, date_for_signing);
    if !constant_time_eq(signature.as_bytes(), expected.as_bytes()) {
        return Err(SignatureError {
            code: "SignatureDoesNotMatch",
            status: 403,
        });
    }
    Ok(())
}

pub(crate) fn verify_sigv2_presign(req: &Request, auth: &AuthConfig) -> Result<(), SignatureError> {
    let access_key = req
        .query
        .get("AWSAccessKeyId")
        .map(String::as_str)
        .unwrap_or("");
    let expires_str = req.query.get("Expires").map(String::as_str).unwrap_or("");
    let signature = req.query.get("Signature").map(String::as_str).unwrap_or("");

    if !constant_time_eq(
        access_key.as_bytes(),
        configured_access_key(auth).as_bytes(),
    ) {
        return Err(SignatureError {
            code: "InvalidAccessKeyId",
            status: 403,
        });
    }
    let expires = expires_str.parse::<i64>().map_err(|_| SignatureError {
        code: "AccessDenied",
        status: 403,
    })?;
    if unix_now() > expires {
        return Err(SignatureError {
            code: "AccessDenied",
            status: 403,
        });
    }

    let date_for_signing = if !req.header("x-amz-date").is_empty() {
        ""
    } else {
        expires_str
    };
    let expected = sigv2_signature(req, auth, date_for_signing);
    if !constant_time_eq(signature.as_bytes(), expected.as_bytes()) {
        return Err(SignatureError {
            code: "SignatureDoesNotMatch",
            status: 403,
        });
    }
    Ok(())
}

pub(crate) fn sigv2_string_to_sign(req: &Request, date_line: &str) -> String {
    let content_md5 = req.header("content-md5");
    let content_type = req.header("content-type");
    let amz_headers = sigv2_canonicalized_amz_headers(req);
    let resource = sigv2_canonicalized_resource(req);
    format!(
        "{}\n{}\n{}\n{}\n{}{}",
        req.method, content_md5, content_type, date_line, amz_headers, resource
    )
}

fn sigv2_signature(req: &Request, auth: &AuthConfig, date_line: &str) -> String {
    let secret = configured_secret(auth);
    let sts = sigv2_string_to_sign(req, date_line);
    let mac = hmac_sha1(secret.as_bytes(), sts.as_bytes());
    base64::std_encode(&mac)
}

fn sigv2_canonicalized_amz_headers(req: &Request) -> String {
    let mut out = String::new();
    for (name, value) in &req.headers {
        if !name.starts_with("x-amz-") {
            continue;
        }
        let normalized = value.split_whitespace().collect::<Vec<_>>().join(" ");
        out.push_str(name);
        out.push(':');
        out.push_str(&normalized);
        out.push('\n');
    }
    out
}

fn sigv2_canonicalized_resource(req: &Request) -> String {
    let mut out = req.path.clone();
    let mut subresources = Vec::new();
    for (key, value) in &req.query {
        if SIGV2_SUBRESOURCES.contains(&key.as_str()) {
            if value.is_empty() {
                subresources.push(key.clone());
            } else {
                subresources.push(format!("{key}={value}"));
            }
        }
    }
    if !subresources.is_empty() {
        out.push('?');
        out.push_str(&subresources.join("&"));
    }
    out
}

/// Parses HTTP dates (`Tue, 27 Mar 2007 19:36:42 +0000` / `GMT`), ISO-8601 basic
/// `YYYYMMDDTHHMMSSZ`, or RFC3339 timestamps into UNIX seconds.
fn parse_sigv2_date(value: &str) -> Option<i64> {
    let trimmed = value.trim();
    if let Some(ts) = parse_sigv4_time(trimmed) {
        return Some(ts);
    }
    if let Some((secs, _)) = crate::time_fmt::parse_rfc3339(trimmed) {
        return Some(secs);
    }
    parse_rfc2822_utc(trimmed)
}

fn parse_rfc2822_utc(value: &str) -> Option<i64> {
    // e.g. "Tue, 27 Mar 2007 19:36:42 GMT" or "Tue, 27 Mar 2007 19:36:42 +0000"
    let after_comma = match value.split_once(',') {
        Some((_, rest)) => rest.trim(),
        None => value.trim(),
    };
    let parts: Vec<&str> = after_comma.split_whitespace().collect();
    if parts.len() != 5 {
        return None;
    }
    let day: i32 = parts[0].parse().ok()?;
    let month: i32 = match parts[1] {
        "Jan" => 1,
        "Feb" => 2,
        "Mar" => 3,
        "Apr" => 4,
        "May" => 5,
        "Jun" => 6,
        "Jul" => 7,
        "Aug" => 8,
        "Sep" => 9,
        "Oct" => 10,
        "Nov" => 11,
        "Dec" => 12,
        _ => return None,
    };
    let year: i32 = parts[2].parse().ok()?;
    let mut hms = parts[3].split(':');
    let hour: i32 = hms.next()?.parse().ok()?;
    let minute: i32 = hms.next()?.parse().ok()?;
    let second: i32 = hms.next()?.parse().ok()?;
    if hms.next().is_some() {
        return None;
    }
    let tz_offset_secs: i64 = match parts[4] {
        "GMT" | "UTC" | "UT" | "Z" => 0,
        tz if tz.len() == 5 && (tz.starts_with('+') || tz.starts_with('-')) => {
            let sign: i64 = if tz.starts_with('+') { 1 } else { -1 };
            let hh: i64 = tz[1..3].parse().ok()?;
            let mm: i64 = tz[3..5].parse().ok()?;
            sign * (hh * 3600 + mm * 60)
        }
        _ => return None,
    };
    Some(datetime_to_unix(year, month, day, hour, minute, second) - tz_offset_secs)
}

fn datetime_to_unix(year: i32, month: i32, day: i32, hour: i32, minute: i32, second: i32) -> i64 {
    let y = year - (month <= 2) as i32;
    let era = (if y >= 0 { y } else { y - 399 }) / 400;
    let yoe = y - era * 400;
    let mp = month + if month > 2 { -3 } else { 9 };
    let doy = (153 * mp + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146097 + doe - 719468;
    (days as i64) * 86_400 + (hour as i64) * 3600 + (minute as i64) * 60 + second as i64
}

// --- Pure-Rust SHA-1 & HMAC-SHA1 (RFC 3174 / RFC 2104) ----------------------

pub(crate) fn sha1(data: &[u8]) -> [u8; 20] {
    let mut h: [u32; 5] = [
        0x6745_2301,
        0xEFCD_AB89,
        0x98BA_DCFE,
        0x1032_5476,
        0xC3D2_E1F0,
    ];
    let bit_len = (data.len() as u64).wrapping_mul(8);
    let mut padded = Vec::with_capacity(data.len() + 72);
    padded.extend_from_slice(data);
    padded.push(0x80);
    while padded.len() % 64 != 56 {
        padded.push(0);
    }
    padded.extend_from_slice(&bit_len.to_be_bytes());

    let mut w = [0u32; 80];
    for chunk in padded.chunks_exact(64) {
        for (i, word) in w[..16].iter_mut().enumerate() {
            *word = u32::from_be_bytes(chunk[i * 4..i * 4 + 4].try_into().unwrap());
        }
        for i in 16..80 {
            w[i] = (w[i - 3] ^ w[i - 8] ^ w[i - 14] ^ w[i - 16]).rotate_left(1);
        }
        let [mut a, mut b, mut c, mut d, mut e] = h;
        for (i, &wi) in w.iter().enumerate() {
            let (f, k) = match i {
                0..=19 => ((b & c) | ((!b) & d), 0x5A82_7999u32),
                20..=39 => (b ^ c ^ d, 0x6ED9_EBA1u32),
                40..=59 => ((b & c) | (b & d) | (c & d), 0x8F1B_BCDCu32),
                _ => (b ^ c ^ d, 0xCA62_C1D6u32),
            };
            let temp = a
                .rotate_left(5)
                .wrapping_add(f)
                .wrapping_add(e)
                .wrapping_add(k)
                .wrapping_add(wi);
            e = d;
            d = c;
            c = b.rotate_left(30);
            b = a;
            a = temp;
        }
        h[0] = h[0].wrapping_add(a);
        h[1] = h[1].wrapping_add(b);
        h[2] = h[2].wrapping_add(c);
        h[3] = h[3].wrapping_add(d);
        h[4] = h[4].wrapping_add(e);
    }

    let mut out = [0u8; 20];
    for (i, word) in h.iter().enumerate() {
        out[i * 4..i * 4 + 4].copy_from_slice(&word.to_be_bytes());
    }
    out
}

pub(crate) fn hmac_sha1(key: &[u8], data: &[u8]) -> [u8; 20] {
    let mut k = [0u8; 64];
    if key.len() > 64 {
        k[..20].copy_from_slice(&sha1(key));
    } else {
        k[..key.len()].copy_from_slice(key);
    }
    let mut inner = Vec::with_capacity(64 + data.len());
    for b in k {
        inner.push(b ^ 0x36);
    }
    inner.extend_from_slice(data);
    let inner_hash = sha1(&inner);

    let mut outer = [0u8; 84];
    for (i, b) in k.iter().enumerate() {
        outer[i] = b ^ 0x5c;
    }
    outer[64..].copy_from_slice(&inner_hash);
    sha1(&outer)
}

// --- SigV4a (AWS4-ECDSA-P256-SHA256) ----------------------------------------

pub(crate) fn is_sigv4a_header(req: &Request) -> bool {
    req.header("authorization")
        .starts_with(&format!("{SIGV4A_ALGORITHM} "))
}

pub(crate) fn is_sigv4a_presign(req: &Request) -> bool {
    req.query.get("X-Amz-Algorithm").map(String::as_str) == Some(SIGV4A_ALGORITHM)
}

/// What verifying a `STREAMING-AWS4-ECDSA-P256-SHA256-PAYLOAD[-TRAILER]` body
/// needs from an already verified SigV4a seed signature.
#[derive(Debug, Clone)]
pub(crate) struct EcdsaChunkSigning {
    pub(crate) chain: ChunkChain,
    pub(crate) has_trailer: bool,
}

/// Verifies a SigV4a `Authorization` header. For SigV4a streaming payloads it
/// returns the chunk chain seeded by this signature.
pub(crate) fn verify_sigv4a_header(
    req: &Request,
    auth: &AuthConfig,
) -> Result<Option<EcdsaChunkSigning>, SignatureError> {
    let authorization = req.header("authorization");
    let Some(rest) = authorization.strip_prefix(&format!("{SIGV4A_ALGORITHM} ")) else {
        return Err(SignatureError {
            code: "AuthorizationHeaderMalformed",
            status: 400,
        });
    };
    let values = parse_auth_params(rest);
    let credential = values.get("Credential").map(String::as_str).unwrap_or("");
    let signed_headers = values
        .get("SignedHeaders")
        .map(String::as_str)
        .unwrap_or("");
    let signature_hex = values.get("Signature").map(String::as_str).unwrap_or("");
    let (access_key, date_stamp, service) =
        parse_sigv4a_credential_scope(credential).ok_or(SignatureError {
            code: "AuthorizationHeaderMalformed",
            status: 400,
        })?;
    if signed_headers.is_empty() || signature_hex.is_empty() {
        return Err(SignatureError {
            code: "AuthorizationHeaderMalformed",
            status: 400,
        });
    }
    if !constant_time_eq(
        access_key.as_bytes(),
        configured_access_key(auth).as_bytes(),
    ) || service != SIGV4A_SERVICE
    {
        return Err(SignatureError {
            code: "InvalidAccessKeyId",
            status: 403,
        });
    }

    let region_set = req.header("x-amz-region-set");
    if !region_set_matches(region_set, configured_region(auth)) {
        return Err(SignatureError {
            code: "AuthorizationHeaderMalformed",
            status: 400,
        });
    }

    let amz_date = req.header("x-amz-date");
    if amz_date.is_empty() {
        return Err(SignatureError {
            code: "AuthorizationHeaderMalformed",
            status: 400,
        });
    }
    let signed_at = parse_sigv4_time(amz_date).ok_or(SignatureError {
        code: "AccessDenied",
        status: 403,
    })?;
    if (unix_now() - signed_at).abs() > MAX_CLOCK_SKEW_SECONDS {
        return Err(SignatureError {
            code: "RequestTimeTooSkewed",
            status: 403,
        });
    }
    verify_scope_date(amz_date, date_stamp)?;
    verify_signed_header_coverage(req, signed_headers)?;

    let payload_hash = match req.header("x-amz-content-sha256") {
        "" => "UNSIGNED-PAYLOAD",
        // Chunk signatures are checked against the chain once the body is decoded.
        streaming if devcloud_sigv4a::is_streaming(streaming) => streaming,
        // An HMAC chunk chain cannot be seeded by an ECDSA signature.
        streaming if streaming.starts_with("STREAMING-AWS4-HMAC-SHA256-") => {
            return Err(SignatureError {
                code: "InvalidArgument",
                status: 400,
            });
        }
        hash => {
            verify_payload_hash(req, hash)?;
            hash
        }
    };

    let string_to_sign =
        sigv4a_string_to_sign(req, date_stamp, amz_date, signed_headers, payload_hash, "");
    let key = verify_sigv4a_ecdsa(auth, access_key, string_to_sign.as_bytes(), signature_hex)?;
    if !devcloud_sigv4a::is_streaming(payload_hash) {
        return Ok(None);
    }
    Ok(Some(EcdsaChunkSigning {
        chain: ChunkChain::new(
            key,
            amz_date,
            &devcloud_sigv4a::credential_scope(date_stamp, SIGV4A_SERVICE),
            signature_hex,
        ),
        has_trailer: payload_hash == devcloud_sigv4a::STREAMING_PAYLOAD_TRAILER,
    }))
}

pub(crate) fn verify_sigv4a_presign(
    req: &Request,
    auth: &AuthConfig,
) -> Result<(), SignatureError> {
    if req.query.get("X-Amz-Algorithm").map(String::as_str) != Some(SIGV4A_ALGORITHM) {
        return Err(SignatureError {
            code: "InvalidArgument",
            status: 400,
        });
    }
    let (access_key, date_stamp, service) = parse_sigv4a_credential_scope(
        req.query
            .get("X-Amz-Credential")
            .map(String::as_str)
            .unwrap_or(""),
    )
    .ok_or(SignatureError {
        code: "AuthorizationHeaderMalformed",
        status: 400,
    })?;
    if !constant_time_eq(
        access_key.as_bytes(),
        configured_access_key(auth).as_bytes(),
    ) || service != SIGV4A_SERVICE
    {
        return Err(SignatureError {
            code: "InvalidAccessKeyId",
            status: 403,
        });
    }
    let region_set = req
        .query
        .get("X-Amz-Region-Set")
        .map(String::as_str)
        .unwrap_or("");
    if !region_set_matches(region_set, configured_region(auth)) {
        return Err(SignatureError {
            code: "AuthorizationHeaderMalformed",
            status: 400,
        });
    }
    let amz_date = req
        .query
        .get("X-Amz-Date")
        .map(String::as_str)
        .unwrap_or("");
    let expires = req
        .query
        .get("X-Amz-Expires")
        .and_then(|v| v.parse::<i64>().ok())
        .filter(|v| (0..=604800).contains(v))
        .ok_or(SignatureError {
            code: "AccessDenied",
            status: 403,
        })?;
    let signed_at = parse_sigv4_time(amz_date).ok_or(SignatureError {
        code: "AccessDenied",
        status: 403,
    })?;
    let now = unix_now();
    if now > signed_at + expires || signed_at > now + MAX_CLOCK_SKEW_SECONDS {
        return Err(SignatureError {
            code: "AccessDenied",
            status: 403,
        });
    }
    verify_scope_date(amz_date, date_stamp)?;
    let signed_headers = req
        .query
        .get("X-Amz-SignedHeaders")
        .map(String::as_str)
        .unwrap_or("");
    if signed_headers.is_empty() {
        return Err(SignatureError {
            code: "AuthorizationHeaderMalformed",
            status: 400,
        });
    }
    verify_signed_header_coverage(req, signed_headers)?;
    let payload_hash = match req.header("x-amz-content-sha256") {
        "" => "UNSIGNED-PAYLOAD",
        // Decoded (and its trailer checksums validated) once the URL verifies.
        unsigned @ "STREAMING-UNSIGNED-PAYLOAD-TRAILER" => unsigned,
        // A chunk-signature chain is seeded by an `Authorization` header signature.
        streaming if streaming.starts_with("STREAMING-") => {
            return Err(SignatureError {
                code: "NotImplemented",
                status: 501,
            });
        }
        hash => {
            verify_payload_hash(req, hash)?;
            hash
        }
    };
    let signature_hex = req
        .query
        .get("X-Amz-Signature")
        .map(String::as_str)
        .unwrap_or("");
    let string_to_sign = sigv4a_string_to_sign(
        req,
        date_stamp,
        amz_date,
        signed_headers,
        payload_hash,
        "X-Amz-Signature",
    );
    verify_sigv4a_ecdsa(auth, access_key, string_to_sign.as_bytes(), signature_hex).map(|_| ())
}

fn parse_sigv4a_credential_scope(credential: &str) -> Option<(&str, &str, &str)> {
    let parts: Vec<&str> = credential.split('/').collect();
    if parts.len() == 4 && parts[3] == "aws4_request" {
        Some((parts[0], parts[1], parts[2]))
    } else {
        None
    }
}

fn region_set_matches(region_set: &str, configured: &str) -> bool {
    devcloud_sigv4a::region_set_matches(region_set, configured)
}

fn sigv4a_string_to_sign(
    req: &Request,
    date_stamp: &str,
    amz_date: &str,
    signed_headers: &str,
    payload_hash: &str,
    ignored_query_key: &str,
) -> String {
    let canonical_request = [
        req.method.clone(),
        canonical_uri(&req.path),
        canonical_query_string(&req.query, ignored_query_key),
        canonical_headers(req, signed_headers),
        signed_headers.to_ascii_lowercase(),
        payload_hash.to_string(),
    ]
    .join("\n");
    let scope = devcloud_sigv4a::credential_scope(date_stamp, SIGV4A_SERVICE);
    devcloud_sigv4a::string_to_sign(amz_date, &scope, &canonical_request)
}

/// Verifies the ECDSA signature and returns the key for any chunk chain.
fn verify_sigv4a_ecdsa(
    auth: &AuthConfig,
    access_key: &str,
    string_to_sign: &[u8],
    signature_hex: &str,
) -> Result<SigningKey, SignatureError> {
    let mismatch = SignatureError {
        code: "SignatureDoesNotMatch",
        status: 403,
    };
    let Some(key) = SigningKey::derive(access_key, configured_secret(auth)) else {
        return Err(mismatch);
    };
    if key.verify(string_to_sign, signature_hex) {
        Ok(key)
    } else {
        Err(mismatch)
    }
}

pub(crate) fn configured_access_key(auth: &AuthConfig) -> &str {
    if auth.access_key_id.is_empty() {
        "dev"
    } else {
        &auth.access_key_id
    }
}

pub(crate) fn configured_secret(auth: &AuthConfig) -> &str {
    if auth.secret_access_key.is_empty() {
        "dev"
    } else {
        &auth.secret_access_key
    }
}

pub(crate) fn configured_region(auth: &AuthConfig) -> &str {
    if auth.region.is_empty() {
        "us-east-1"
    } else {
        &auth.region
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sha1_and_hmac_sha1_match_rfc_vectors() {
        assert_eq!(
            hex::encode(sha1(b"abc")),
            "a9993e364706816aba3e25717850c26c9cd0d89d"
        );
        assert_eq!(
            hex::encode(sha1(b"")),
            "da39a3ee5e6b4b0d3255bfef95601890afd80709"
        );
        // RFC 2202 test case 2: key = "Jefe", data = "what do ya want for nothing?"
        assert_eq!(
            hex::encode(hmac_sha1(b"Jefe", b"what do ya want for nothing?")),
            "effcdf6ae5eb2fa2d27416d5f184df9c259a7c79"
        );
        // FIPS 180 two-block message and one million 'a'.
        assert_eq!(
            hex::encode(sha1(
                b"abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq"
            )),
            "84983e441c3bd26ebaae4aa1f95129e5e54670f1"
        );
        assert_eq!(
            hex::encode(sha1(&vec![b'a'; 1_000_000])),
            "34aa973cd4c4daa4f61eeb2bdbad27316534016f"
        );
        // Padding boundaries, checked against Python hashlib.
        for (len, expected) in [
            (55, "c1c8bbdc22796e28c0e15163d20899b65621d65a"),
            (56, "c2db330f6083854c99d4b5bfb6e8f29f201be699"),
            (64, "0098ba824b5c16427bd7a1122a5a442a25ec644d"),
            (119, "ee971065aaa017e0632a8ca6c77bb3bf8b1dfc56"),
        ] {
            assert_eq!(hex::encode(sha1(&vec![b'a'; len])), expected, "{len}");
        }
        // RFC 2202 test cases 1 and 6 (key longer than the block size).
        assert_eq!(
            hex::encode(hmac_sha1(&[0x0b; 20], b"Hi There")),
            "b617318655057264e28bc0b6fb378c8ef146be00"
        );
        assert_eq!(
            hex::encode(hmac_sha1(
                &[0xaa; 80],
                b"Test Using Larger Than Block-Size Key - Hash Key First"
            )),
            "aa4ae5e15272d00e95705637ce8a3b55ed402112"
        );
        // A key of exactly the block size, checked against Python hmac.
        assert_eq!(
            hex::encode(hmac_sha1(&[b'k'; 64], b"exact block key")),
            "2a19d0bbd00565ec3cb7e72e1c7349537f8a9c74"
        );
    }

    #[test]
    fn sigv2_matches_aws_documented_example() {
        let mut req = Request::new("GET", "/johnsmith/photos/puppy.jpg", Vec::new());
        req.headers.insert(
            "date".to_string(),
            "Tue, 27 Mar 2007 19:36:42 +0000".to_string(),
        );
        let auth = AuthConfig {
            auth_mode: "strict".to_string(),
            access_key_id: "AKIAIOSFODNN7EXAMPLE".to_string(),
            secret_access_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".to_string(),
            region: "us-east-1".to_string(),
        };
        assert_eq!(
            sigv2_signature(&req, &auth, "Tue, 27 Mar 2007 19:36:42 +0000"),
            "bWq2s1WEIj+Ydj0vQ697zp+IXMU="
        );
    }
}
