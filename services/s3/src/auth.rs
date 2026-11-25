//! SigV2 (HMAC-SHA1) and SigV4a (ECDSA-P256-SHA256) signature verification.
//!
//! Pure-Rust SHA-1, HMAC-SHA1, and NIST P-256 (secp256r1) verification so
//! `devcloud-s3` adds zero external crypto dependencies.

use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};

use crate::base64;
use crate::http::{
    canonical_headers, canonical_query_string, canonical_uri, constant_time_eq, parse_auth_params,
    parse_sigv4_time, unix_now, verify_payload_hash, verify_scope_date,
    verify_signed_header_coverage, AuthConfig, Request, SignatureError,
};

pub(crate) const SIGV4A_ALGORITHM: &str = "AWS4-ECDSA-P256-SHA256";
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

pub(crate) fn verify_sigv4a_header(req: &Request, auth: &AuthConfig) -> Result<(), SignatureError> {
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

    let payload_hash = if req.header("x-amz-content-sha256").is_empty() {
        "UNSIGNED-PAYLOAD".to_string()
    } else {
        let payload_hash = req.header("x-amz-content-sha256").to_string();
        verify_payload_hash(req, &payload_hash)?;
        payload_hash
    };

    let string_to_sign =
        sigv4a_string_to_sign(req, date_stamp, amz_date, signed_headers, &payload_hash, "");
    verify_sigv4a_ecdsa(auth, access_key, string_to_sign.as_bytes(), signature_hex)
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
    verify_sigv4a_ecdsa(auth, access_key, string_to_sign.as_bytes(), signature_hex)
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
    let region_set = region_set.trim();
    if region_set.is_empty() {
        return false;
    }
    region_set
        .split(',')
        .map(str::trim)
        .any(|r| r == "*" || r == configured)
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
    let scope = format!("{date_stamp}/{SIGV4A_SERVICE}/aws4_request");
    let hash = hex::encode(Sha256::digest(canonical_request.as_bytes()));
    format!("{SIGV4A_ALGORITHM}\n{amz_date}\n{scope}\n{hash}")
}

fn verify_sigv4a_ecdsa(
    auth: &AuthConfig,
    access_key: &str,
    string_to_sign: &[u8],
    signature_hex: &str,
) -> Result<(), SignatureError> {
    let mismatch = || SignatureError {
        code: "SignatureDoesNotMatch",
        status: 403,
    };
    let sig_der = hex::decode(signature_hex).map_err(|_| mismatch())?;
    let (r, s) = parse_der_ecdsa_sig(&sig_der).ok_or_else(mismatch)?;
    let d = derive_sigv4a_private_key(access_key, configured_secret(auth)).ok_or_else(mismatch)?;
    let pub_key = p256::scalar_mul_g(&d);
    let digest: [u8; 32] = Sha256::digest(string_to_sign).into();
    let z = p256::U256::from_be_bytes(&digest);
    if p256::ecdsa_verify(&pub_key, &z, &r, &s) {
        Ok(())
    } else {
        Err(mismatch())
    }
}

/// Derives the SigV4a ECDSA P-256 private scalar `d` from `(access_key, secret)`
/// using the AWS NIST SP 800-108 Counter Mode KDF specification.
pub fn derive_sigv4a_private_key(access_key: &str, secret: &str) -> Option<p256::U256> {
    let key = format!("AWS4A{secret}");
    let n_minus_2 = p256::N_MINUS_2;
    for counter in 1u8..=254u8 {
        let mut input = Vec::with_capacity(4 + SIGV4A_ALGORITHM.len() + 1 + access_key.len() + 5);
        input.extend_from_slice(&1u32.to_be_bytes());
        input.extend_from_slice(SIGV4A_ALGORITHM.as_bytes());
        input.push(0x00);
        input.extend_from_slice(access_key.as_bytes());
        input.push(counter);
        input.extend_from_slice(&256u32.to_be_bytes());

        let mut mac = Hmac::<Sha256>::new_from_slice(key.as_bytes()).ok()?;
        mac.update(&input);
        let bytes: [u8; 32] = mac.finalize().into_bytes().into();
        let c = p256::U256::from_be_bytes(&bytes);
        if c <= n_minus_2 {
            return Some(c.add_one());
        }
    }
    None
}

/// Parses an ASN.1 DER `SEQUENCE { INTEGER r, INTEGER s }` signature.
fn parse_der_ecdsa_sig(der: &[u8]) -> Option<(p256::U256, p256::U256)> {
    if der.len() < 8 || der[0] != 0x30 {
        return None;
    }
    let seq_len = der[1] as usize;
    if seq_len + 2 != der.len() {
        return None;
    }
    let (r, rest) = parse_der_uint(&der[2..])?;
    let (s, rest) = parse_der_uint(rest)?;
    if !rest.is_empty() {
        return None;
    }
    Some((r, s))
}

fn parse_der_uint(input: &[u8]) -> Option<(p256::U256, &[u8])> {
    if input.len() < 3 || input[0] != 0x02 {
        return None;
    }
    let len = input[1] as usize;
    if len == 0 || input.len() < 2 + len {
        return None;
    }
    let raw = &input[2..2 + len];
    // Reject negative integers or non-minimal DER padding.
    if raw[0] & 0x80 != 0 {
        return None;
    }
    let trimmed = if raw.len() > 1 && raw[0] == 0x00 {
        if raw[1] & 0x80 == 0 {
            return None;
        }
        &raw[1..]
    } else {
        raw
    };
    if trimmed.len() > 32 {
        return None;
    }
    let mut buf = [0u8; 32];
    buf[32 - trimmed.len()..].copy_from_slice(trimmed);
    Some((p256::U256::from_be_bytes(&buf), &input[2 + len..]))
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

/// Minimal NIST P-256 (secp256r1) elliptic curve arithmetic for SigV4a ECDSA.
pub mod p256 {
    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
    pub struct U256(pub [u64; 4]); // big-endian limb order for natural Ord

    /// Field prime p = 2^256 - 2^224 + 2^192 + 2^96 - 1.
    pub const P: U256 = U256([
        0xFFFF_FFFF_0000_0001,
        0x0000_0000_0000_0000,
        0x0000_0000_FFFF_FFFF,
        0xFFFF_FFFF_FFFF_FFFF,
    ]);

    /// Curve order n.
    pub const N: U256 = U256([
        0xFFFF_FFFF_0000_0000,
        0xFFFF_FFFF_FFFF_FFFF,
        0xBCE6_FAAD_A717_9E84,
        0xF3B9_CAC2_FC63_2551,
    ]);

    pub const N_MINUS_2: U256 = U256([
        0xFFFF_FFFF_0000_0000,
        0xFFFF_FFFF_FFFF_FFFF,
        0xBCE6_FAAD_A717_9E84,
        0xF3B9_CAC2_FC63_254F,
    ]);

    const P_MINUS_2: U256 = U256([
        0xFFFF_FFFF_0000_0001,
        0x0000_0000_0000_0000,
        0x0000_0000_FFFF_FFFF,
        0xFFFF_FFFF_FFFF_FFFD,
    ]);

    const GX: U256 = U256([
        0x6B17_D1F2_E12C_4247,
        0xF8BC_E6E5_63A4_40F2,
        0x7703_7D81_2DEB_33A0,
        0xF4A1_3945_D898_C296,
    ]);

    const GY: U256 = U256([
        0x4FE3_42E2_FE1A_7F9B,
        0x8EE7_EB4A_7C0F_9E16,
        0x2BCE_3357_6B31_5ECE,
        0xCBB6_4068_37BF_51F5,
    ]);

    pub const ZERO: U256 = U256([0, 0, 0, 0]);
    pub const ONE: U256 = U256([0, 0, 0, 1]);

    impl U256 {
        pub fn from_be_bytes(bytes: &[u8; 32]) -> Self {
            U256([
                u64::from_be_bytes(bytes[0..8].try_into().unwrap()),
                u64::from_be_bytes(bytes[8..16].try_into().unwrap()),
                u64::from_be_bytes(bytes[16..24].try_into().unwrap()),
                u64::from_be_bytes(bytes[24..32].try_into().unwrap()),
            ])
        }

        pub fn to_be_bytes(self) -> [u8; 32] {
            let mut out = [0u8; 32];
            out[0..8].copy_from_slice(&self.0[0].to_be_bytes());
            out[8..16].copy_from_slice(&self.0[1].to_be_bytes());
            out[16..24].copy_from_slice(&self.0[2].to_be_bytes());
            out[24..32].copy_from_slice(&self.0[3].to_be_bytes());
            out
        }

        pub fn is_zero(self) -> bool {
            self == ZERO
        }

        pub fn add_one(self) -> Self {
            let (out, _) = add_raw(self, ONE);
            out
        }

        fn bit(self, index: usize) -> bool {
            let limb = 3 - (index / 64);
            ((self.0[limb] >> (index % 64)) & 1) != 0
        }
    }

    fn add_raw(a: U256, b: U256) -> (U256, bool) {
        let mut out = [0u64; 4];
        let mut carry = 0u128;
        for i in (0..4).rev() {
            let sum = a.0[i] as u128 + b.0[i] as u128 + carry;
            out[i] = sum as u64;
            carry = sum >> 64;
        }
        (U256(out), carry != 0)
    }

    fn sub_raw(a: U256, b: U256) -> (U256, bool) {
        let mut out = [0u64; 4];
        let mut borrow = 0i128;
        for i in (0..4).rev() {
            let diff = a.0[i] as i128 - b.0[i] as i128 - borrow;
            if diff < 0 {
                out[i] = (diff + (1i128 << 64)) as u64;
                borrow = 1;
            } else {
                out[i] = diff as u64;
                borrow = 0;
            }
        }
        (U256(out), borrow != 0)
    }

    pub fn add_mod(a: U256, b: U256, m: U256) -> U256 {
        let (sum, carry) = add_raw(a, b);
        if carry || sum >= m {
            sub_raw(sum, m).0
        } else {
            sum
        }
    }

    pub fn sub_mod(a: U256, b: U256, m: U256) -> U256 {
        if a >= b {
            sub_raw(a, b).0
        } else {
            let (diff, _) = sub_raw(b, a);
            sub_raw(m, diff).0
        }
    }

    pub fn reduce_mod(mut a: U256, m: U256) -> U256 {
        while a >= m {
            a = sub_raw(a, m).0;
        }
        a
    }

    pub fn mul_mod(a: U256, b: U256, m: U256) -> U256 {
        let a = reduce_mod(a, m);
        let mut acc = ZERO;
        for i in (0..256).rev() {
            acc = add_mod(acc, acc, m);
            if b.bit(i) {
                acc = add_mod(acc, a, m);
            }
        }
        acc
    }

    pub fn pow_mod(mut base: U256, exp: U256, m: U256) -> U256 {
        base = reduce_mod(base, m);
        let mut result = ONE;
        for i in (0..256).rev() {
            result = mul_mod(result, result, m);
            if exp.bit(i) {
                result = mul_mod(result, base, m);
            }
        }
        result
    }

    fn inv_p(a: U256) -> U256 {
        pow_mod(a, P_MINUS_2, P)
    }

    pub fn inv_n(a: U256) -> U256 {
        pow_mod(a, N_MINUS_2, N)
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct AffinePoint {
        pub x: U256,
        pub y: U256,
        pub infinity: bool,
    }

    impl AffinePoint {
        pub const INFINITY: Self = AffinePoint {
            x: ZERO,
            y: ZERO,
            infinity: true,
        };
    }

    #[derive(Debug, Clone, Copy)]
    struct JacobianPoint {
        x: U256,
        y: U256,
        z: U256,
    }

    impl JacobianPoint {
        const INFINITY: Self = JacobianPoint {
            x: ONE,
            y: ONE,
            z: ZERO,
        };

        fn from_affine(p: AffinePoint) -> Self {
            if p.infinity {
                Self::INFINITY
            } else {
                JacobianPoint {
                    x: p.x,
                    y: p.y,
                    z: ONE,
                }
            }
        }

        fn to_affine(self) -> AffinePoint {
            if self.z.is_zero() {
                return AffinePoint::INFINITY;
            }
            let z_inv = inv_p(self.z);
            let z_inv2 = mul_mod(z_inv, z_inv, P);
            let z_inv3 = mul_mod(z_inv2, z_inv, P);
            AffinePoint {
                x: mul_mod(self.x, z_inv2, P),
                y: mul_mod(self.y, z_inv3, P),
                infinity: false,
            }
        }

        fn double(self) -> Self {
            if self.z.is_zero() || self.y.is_zero() {
                return Self::INFINITY;
            }
            // a = -3 mod p => M = 3*(X - Z^2)*(X + Z^2)
            let z2 = mul_mod(self.z, self.z, P);
            let x_minus_z2 = sub_mod(self.x, z2, P);
            let x_plus_z2 = add_mod(self.x, z2, P);
            let t = mul_mod(x_minus_z2, x_plus_z2, P);
            let m = add_mod(add_mod(t, t, P), t, P);

            let y2 = mul_mod(self.y, self.y, P);
            let xy2 = mul_mod(self.x, y2, P);
            let s = add_mod(add_mod(xy2, xy2, P), add_mod(xy2, xy2, P), P);

            let m2 = mul_mod(m, m, P);
            let two_s = add_mod(s, s, P);
            let x3 = sub_mod(m2, two_s, P);

            let y4 = mul_mod(y2, y2, P);
            let two_y4 = add_mod(y4, y4, P);
            let four_y4 = add_mod(two_y4, two_y4, P);
            let eight_y4 = add_mod(four_y4, four_y4, P);
            let y3 = sub_mod(mul_mod(m, sub_mod(s, x3, P), P), eight_y4, P);

            let yz = mul_mod(self.y, self.z, P);
            let z3 = add_mod(yz, yz, P);
            JacobianPoint {
                x: x3,
                y: y3,
                z: z3,
            }
        }

        fn add(self, other: Self) -> Self {
            if self.z.is_zero() {
                return other;
            }
            if other.z.is_zero() {
                return self;
            }
            let z1z1 = mul_mod(self.z, self.z, P);
            let z2z2 = mul_mod(other.z, other.z, P);
            let u1 = mul_mod(self.x, z2z2, P);
            let u2 = mul_mod(other.x, z1z1, P);
            let s1 = mul_mod(self.y, mul_mod(other.z, z2z2, P), P);
            let s2 = mul_mod(other.y, mul_mod(self.z, z1z1, P), P);
            if u1 == u2 {
                if s1 == s2 {
                    return self.double();
                } else {
                    return Self::INFINITY;
                }
            }
            let h = sub_mod(u2, u1, P);
            let r = sub_mod(s2, s1, P);
            let h2 = mul_mod(h, h, P);
            let h3 = mul_mod(h2, h, P);
            let u1h2 = mul_mod(u1, h2, P);
            let x3 = sub_mod(sub_mod(mul_mod(r, r, P), h3, P), add_mod(u1h2, u1h2, P), P);
            let y3 = sub_mod(mul_mod(r, sub_mod(u1h2, x3, P), P), mul_mod(s1, h3, P), P);
            let z3 = mul_mod(h, mul_mod(self.z, other.z, P), P);
            JacobianPoint {
                x: x3,
                y: y3,
                z: z3,
            }
        }
    }

    pub fn scalar_mul(point: AffinePoint, scalar: &U256) -> AffinePoint {
        if point.infinity || scalar.is_zero() {
            return AffinePoint::INFINITY;
        }
        let base = JacobianPoint::from_affine(point);
        let mut acc = JacobianPoint::INFINITY;
        for i in (0..256).rev() {
            acc = acc.double();
            if scalar.bit(i) {
                acc = acc.add(base);
            }
        }
        acc.to_affine()
    }

    pub fn scalar_mul_g(scalar: &U256) -> AffinePoint {
        scalar_mul(
            AffinePoint {
                x: GX,
                y: GY,
                infinity: false,
            },
            scalar,
        )
    }

    pub fn ecdsa_verify(pub_key: &AffinePoint, z: &U256, r: &U256, s: &U256) -> bool {
        if pub_key.infinity || r.is_zero() || *r >= N || s.is_zero() || *s >= N {
            return false;
        }
        let w = inv_n(*s);
        let u1 = mul_mod(*z, w, N);
        let u2 = mul_mod(*r, w, N);
        let p1 = JacobianPoint::from_affine(scalar_mul_g(&u1));
        let p2 = JacobianPoint::from_affine(scalar_mul(*pub_key, &u2));
        let point = p1.add(p2).to_affine();
        if point.infinity {
            return false;
        }
        reduce_mod(point.x, N) == *r
    }

    /// Deterministic ECDSA P-256 signing helper (used by tests) producing ASN.1 DER.
    pub fn ecdsa_sign_der_with_k(private_key: &U256, z: &U256, k: &U256) -> Vec<u8> {
        let r_point = scalar_mul_g(k);
        let r = reduce_mod(r_point.x, N);
        let k_inv = inv_n(*k);
        let rd = mul_mod(r, *private_key, N);
        let s = mul_mod(k_inv, add_mod(reduce_mod(*z, N), rd, N), N);
        encode_der_sig(&r, &s)
    }

    fn encode_der_sig(r: &U256, s: &U256) -> Vec<u8> {
        let r_enc = encode_der_uint(r);
        let s_enc = encode_der_uint(s);
        let mut out = Vec::with_capacity(2 + r_enc.len() + s_enc.len());
        out.push(0x30);
        out.push((r_enc.len() + s_enc.len()) as u8);
        out.extend_from_slice(&r_enc);
        out.extend_from_slice(&s_enc);
        out
    }

    fn encode_der_uint(v: &U256) -> Vec<u8> {
        let bytes = v.to_be_bytes();
        let first_nz = bytes.iter().position(|&b| b != 0).unwrap_or(31);
        let slice = &bytes[first_nz..];
        let needs_pad = slice[0] & 0x80 != 0;
        let mut out = Vec::with_capacity(2 + slice.len() + usize::from(needs_pad));
        out.push(0x02);
        out.push((slice.len() + usize::from(needs_pad)) as u8);
        if needs_pad {
            out.push(0x00);
        }
        out.extend_from_slice(slice);
        out
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

    #[test]
    fn p256_sigv4a_key_derivation_and_ecdsa_roundtrip() {
        let d = derive_sigv4a_private_key(
            "AKIAIOSFODNN7EXAMPLE",
            "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        )
        .unwrap();
        let pub_key = p256::scalar_mul_g(&d);
        assert!(!pub_key.infinity);
        let digest: [u8; 32] = Sha256::digest(b"hello sigv4a").into();
        let z = p256::U256::from_be_bytes(&digest);
        let k = p256::U256([0x1234, 0x5678, 0x9abc, 0xdef1]);
        let der = p256::ecdsa_sign_der_with_k(&d, &z, &k);
        let (r, s) = parse_der_ecdsa_sig(&der).unwrap();
        assert!(p256::ecdsa_verify(&pub_key, &z, &r, &s));

        let wrong_digest: [u8; 32] = Sha256::digest(b"tampered").into();
        let z2 = p256::U256::from_be_bytes(&wrong_digest);
        assert!(!p256::ecdsa_verify(&pub_key, &z2, &r, &s));
    }
}
