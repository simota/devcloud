//! Parity tests for the SQS SigV4 verifier. The canonical-encoding and SHA-256
//! values are captured from the legacy implementation; the accept/reject paths are
//! exercised by signing a request with the crate's own signer (round-trip) and
//! by tampering with each field.

use std::collections::HashMap;

use devcloud_sqs::sigv4::{
    aws_percent_encode, canonical_query_string, canonical_request, normalize_header_value,
    sha256_hex, signature_for_request, verify_signature, Credentials, SignatureError,
    SignedRequest, SIGV4_ALGORITHM,
};

// --- canonicalization oracle (values captured from legacy) ---

#[test]
fn sha256_matches_legacy() {
    assert_eq!(
        sha256_hex(b""),
        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
    );
    assert_eq!(
        sha256_hex(br#"{"QueueName":"Orders"}"#),
        "326d1d3022eb9865274d547205e305c01ea7910c916d8f47eb8eade5ceee839a"
    );
}

#[test]
fn percent_encoding_matches_legacy() {
    assert_eq!(aws_percent_encode("a b+c/d=e", "~-_"), "a%20b%2Bc%2Fd%3De");
    // canonical_uri keeps `/` and `~` safe.
    assert_eq!(
        aws_percent_encode("/000000000000/My Queue", "/~"),
        "/000000000000/My%20Queue"
    );
}

#[test]
fn normalize_header_value_matches_legacy() {
    assert_eq!(normalize_header_value("  a   b  c "), "a b c");
}

#[test]
fn canonical_query_sorts_and_encodes() {
    // Out-of-order keys sort; values encoded.
    assert_eq!(canonical_query_string("B=2&A=1&A=0"), "A=0&A=1&B=2");
    assert_eq!(canonical_query_string(""), "");
    assert_eq!(canonical_query_string("k=a b"), "k=a%20b");
}

// --- verification: relaxed mode ---

fn header_none(_: &str) -> Option<String> {
    None
}

#[test]
fn relaxed_mode_accepts_unsigned() {
    let req = SignedRequest {
        method: "POST",
        path: "/",
        query: "",
        host: "127.0.0.1:9324",
        authorization: "",
        amz_date: "",
        content_sha256: "",
        header: &header_none,
        body: b"{}",
    };
    let creds = Credentials {
        auth_mode: "relaxed",
        access_key_id: "dev",
        secret_access_key: "dev",
        region: "us-east-1",
    };
    assert!(verify_signature(&req, &creds).is_ok());
    // Empty auth_mode is also relaxed.
    let creds2 = Credentials {
        auth_mode: "",
        ..creds
    };
    assert!(verify_signature(&req, &creds2).is_ok());
}

// --- verification: strict mode, round-trip ---

fn strict_creds() -> Credentials<'static> {
    Credentials {
        auth_mode: "strict",
        access_key_id: "dev",
        secret_access_key: "dev",
        region: "us-east-1",
    }
}

/// Builds a strict-mode request signed with the crate's own signer, returning
/// the Authorization header value and the content hash.
fn signed_request(body: &'static [u8], headers: HashMap<String, String>) -> (String, String) {
    let creds = strict_creds();
    let date_stamp = "20260430";
    let amz_date = "20260430T100000Z";
    let signed_headers = "host;x-amz-date";
    let content_sha = sha256_hex(body);
    let host = "127.0.0.1:9324".to_string();

    let hmap = headers.clone();
    let header_fn = move |name: &str| hmap.get(name).cloned();
    let req = SignedRequest {
        method: "POST",
        path: "/",
        query: "",
        host: &host,
        authorization: "",
        amz_date,
        content_sha256: &content_sha,
        header: &header_fn,
        body,
    };
    let sig = signature_for_request(
        &req,
        date_stamp,
        "us-east-1",
        signed_headers,
        &content_sha,
        &creds,
    );
    let auth = format!(
        "{SIGV4_ALGORITHM} Credential=dev/{date_stamp}/us-east-1/sqs/aws4_request, \
         SignedHeaders={signed_headers}, Signature={sig}"
    );
    (auth, content_sha)
}

#[test]
fn strict_mode_accepts_valid_signature() {
    let body: &[u8] = b"{}";
    let mut headers = HashMap::new();
    headers.insert("x-amz-date".to_string(), "20260430T100000Z".to_string());
    let (auth, content_sha) = signed_request(body, headers.clone());

    let header_fn = move |name: &str| headers.get(name).cloned();
    let req = SignedRequest {
        method: "POST",
        path: "/",
        query: "",
        host: "127.0.0.1:9324",
        authorization: &auth,
        amz_date: "20260430T100000Z",
        content_sha256: &content_sha,
        header: &header_fn,
        body,
    };
    assert!(verify_signature(&req, &strict_creds()).is_ok());
}

#[test]
fn strict_mode_rejects_tampered_signature() {
    let body: &[u8] = b"{}";
    let mut headers = HashMap::new();
    headers.insert("x-amz-date".to_string(), "20260430T100000Z".to_string());
    let (auth, content_sha) = signed_request(body, headers.clone());
    // Flip the last hex char of the signature.
    let mut bad = auth.clone();
    let last = bad.pop().unwrap();
    bad.push(if last == '0' { '1' } else { '0' });

    let header_fn = move |name: &str| headers.get(name).cloned();
    let req = SignedRequest {
        method: "POST",
        path: "/",
        query: "",
        host: "127.0.0.1:9324",
        authorization: &bad,
        amz_date: "20260430T100000Z",
        content_sha256: &content_sha,
        header: &header_fn,
        body,
    };
    assert_eq!(
        verify_signature(&req, &strict_creds()),
        Err(SignatureError {
            code: "SignatureDoesNotMatch",
            status: 403
        })
    );
}

#[test]
fn strict_mode_rejects_body_tamper_via_payload_hash() {
    let body: &[u8] = b"{}";
    let mut headers = HashMap::new();
    headers.insert("x-amz-date".to_string(), "20260430T100000Z".to_string());
    let (auth, content_sha) = signed_request(body, headers.clone());

    // Same signed headers + content hash, but the actual body differs → the
    // payload-hash check fails first.
    let header_fn = move |name: &str| headers.get(name).cloned();
    let req = SignedRequest {
        method: "POST",
        path: "/",
        query: "",
        host: "127.0.0.1:9324",
        authorization: &auth,
        amz_date: "20260430T100000Z",
        content_sha256: &content_sha,
        header: &header_fn,
        body: b"tampered",
    };
    assert_eq!(
        verify_signature(&req, &strict_creds()),
        Err(SignatureError {
            code: "SignatureDoesNotMatch",
            status: 403
        })
    );
}

#[test]
fn strict_mode_error_codes() {
    let creds = strict_creds();
    let base = |auth: &'static str, amz: &'static str| {
        verify_signature(
            &SignedRequest {
                method: "POST",
                path: "/",
                query: "",
                host: "127.0.0.1:9324",
                authorization: auth,
                amz_date: amz,
                content_sha256: "",
                header: &header_none,
                body: b"{}",
            },
            &creds,
        )
    };
    // Missing auth → AccessDenied/403.
    assert_eq!(base("", "x").unwrap_err().code, "AccessDenied");
    // Wrong algorithm prefix → AuthorizationHeaderMalformed/400.
    assert_eq!(
        base("Basic abc", "x").unwrap_err().code,
        "AuthorizationHeaderMalformed"
    );
    // Malformed credential scope → AuthorizationHeaderMalformed/400.
    assert_eq!(
        base(
            "AWS4-HMAC-SHA256 Credential=dev/bad, SignedHeaders=host, Signature=abc",
            "x"
        )
        .unwrap_err()
        .code,
        "AuthorizationHeaderMalformed"
    );
    // Unknown access key → InvalidAccessKeyId/403.
    assert_eq!(
        base(
            "AWS4-HMAC-SHA256 Credential=WRONG/20260430/us-east-1/sqs/aws4_request, SignedHeaders=host;x-amz-date, Signature=abc",
            "20260430T100000Z"
        )
        .unwrap_err()
        .code,
        "InvalidAccessKeyId"
    );
    // Valid credential but missing x-amz-date → AuthorizationHeaderMalformed/400.
    assert_eq!(
        base(
            "AWS4-HMAC-SHA256 Credential=dev/20260430/us-east-1/sqs/aws4_request, SignedHeaders=host;x-amz-date, Signature=abc",
            ""
        )
        .unwrap_err()
        .code,
        "AuthorizationHeaderMalformed"
    );
}

// --- streaming (aws-chunked) payload verification ---

fn sign_streaming_chunks(
    chunks: &[&[u8]],
    seed: &str,
    amz_date: &str,
    trailer: Option<&str>,
) -> Vec<u8> {
    use hmac::{Hmac, Mac};
    use sha2::Sha256;
    fn hmac(key: &[u8], val: &[u8]) -> String {
        let mut mac = Hmac::<Sha256>::new_from_slice(key).unwrap();
        mac.update(val);
        mac.finalize()
            .into_bytes()
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect()
    }
    fn hmac_bytes(key: &[u8], val: &[u8]) -> Vec<u8> {
        let mut mac = Hmac::<Sha256>::new_from_slice(key).unwrap();
        mac.update(val);
        mac.finalize().into_bytes().to_vec()
    }
    let date_stamp = &amz_date[..8];
    let date_key = hmac_bytes(b"AWS4dev", date_stamp.as_bytes());
    let region_key = hmac_bytes(&date_key, b"us-east-1");
    let service_key = hmac_bytes(&region_key, b"sqs");
    let key = hmac_bytes(&service_key, b"aws4_request");
    let scope = format!("{date_stamp}/us-east-1/sqs/aws4_request");
    let empty = sha256_hex(b"");
    let mut previous = seed.to_string();
    let mut out = Vec::new();
    for chunk in chunks.iter().copied().chain([&b""[..]]) {
        let to_sign = format!(
            "AWS4-HMAC-SHA256-PAYLOAD\n{amz_date}\n{scope}\n{previous}\n{empty}\n{}",
            sha256_hex(chunk)
        );
        previous = hmac(&key, to_sign.as_bytes());
        out.extend(format!("{:x};chunk-signature={previous}\r\n", chunk.len()).into_bytes());
        out.extend_from_slice(chunk);
        out.extend_from_slice(b"\r\n");
    }
    if let Some(trailer) = trailer {
        out.truncate(out.len() - 2);
        let to_sign = format!(
            "AWS4-HMAC-SHA256-TRAILER\n{amz_date}\n{scope}\n{previous}\n{}",
            sha256_hex(format!("{trailer}\n").as_bytes())
        );
        let sig = hmac(&key, to_sign.as_bytes());
        out.extend(format!("{trailer}\r\nx-amz-trailer-signature:{sig}\r\n\r\n").into_bytes());
    }
    out
}

#[test]
fn strict_mode_verifies_streaming_payloads() {
    use devcloud_sqs::sigv4::{
        STREAMING_SIGNED, STREAMING_SIGNED_TRAILER, STREAMING_UNSIGNED_TRAILER,
    };

    let creds = strict_creds();
    let date_stamp = "20260430";
    let amz_date = "20260430T100000Z";
    let signed_headers = "host;x-amz-content-sha256;x-amz-date";
    let host = "127.0.0.1:9324";

    let run = |payload_hash: &str,
               chunks: &[&[u8]],
               trailer: Option<&str>,
               decoded_len: Option<&str>,
               override_body: Option<&[u8]>| {
        let mut hmap = HashMap::new();
        hmap.insert("x-amz-date".to_string(), amz_date.to_string());
        hmap.insert("x-amz-content-sha256".to_string(), payload_hash.to_string());
        if let Some(len) = decoded_len {
            hmap.insert("x-amz-decoded-content-length".to_string(), len.to_string());
        }
        let h1 = hmap.clone();
        let header_fn1 = move |name: &str| h1.get(name).cloned();
        let req_for_sig = SignedRequest {
            method: "POST",
            path: "/",
            query: "",
            host,
            authorization: "",
            amz_date,
            content_sha256: payload_hash,
            header: &header_fn1,
            body: b"",
        };
        let seed = signature_for_request(
            &req_for_sig,
            date_stamp,
            "us-east-1",
            signed_headers,
            payload_hash,
            &creds,
        );
        let auth = format!(
            "{SIGV4_ALGORITHM} Credential=dev/{date_stamp}/us-east-1/sqs/aws4_request, \
             SignedHeaders={signed_headers}, Signature={seed}"
        );
        let framed = sign_streaming_chunks(chunks, &seed, amz_date, trailer);
        let body = override_body.unwrap_or(&framed);
        let header_fn2 = move |name: &str| hmap.get(name).cloned();
        let req = SignedRequest {
            method: "POST",
            path: "/",
            query: "",
            host,
            authorization: &auth,
            amz_date,
            content_sha256: payload_hash,
            header: &header_fn2,
            body,
        };
        verify_signature(&req, &creds)
    };

    // Valid signed chunks.
    assert_eq!(
        run(
            STREAMING_SIGNED,
            &[b"hello ", b"world"],
            None,
            Some("11"),
            None
        ),
        Ok(())
    );
    // Tampered chunk byte fails with SignatureDoesNotMatch.
    let mut hmap = HashMap::new();
    hmap.insert("x-amz-date".to_string(), amz_date.to_string());
    hmap.insert(
        "x-amz-content-sha256".to_string(),
        STREAMING_SIGNED.to_string(),
    );
    let h1 = hmap.clone();
    let header_fn1 = move |name: &str| h1.get(name).cloned();
    let seed = signature_for_request(
        &SignedRequest {
            method: "POST",
            path: "/",
            query: "",
            host,
            authorization: "",
            amz_date,
            content_sha256: STREAMING_SIGNED,
            header: &header_fn1,
            body: b"",
        },
        date_stamp,
        "us-east-1",
        signed_headers,
        STREAMING_SIGNED,
        &creds,
    );
    let mut bad_body = sign_streaming_chunks(&[b"hello ", b"world"], &seed, amz_date, None);
    let pos = bad_body.windows(5).position(|w| w == b"hello").unwrap();
    bad_body[pos] = b'j';
    assert_eq!(
        run(
            STREAMING_SIGNED,
            &[b"hello ", b"world"],
            None,
            None,
            Some(&bad_body)
        ),
        Err(SignatureError {
            code: "SignatureDoesNotMatch",
            status: 403,
        })
    );

    // Valid signed trailer and tampered trailer.
    assert_eq!(
        run(
            STREAMING_SIGNED_TRAILER,
            &[b"hello world"],
            Some("x-amz-checksum-crc32:DQpIDQ=="),
            Some("11"),
            None,
        ),
        Ok(())
    );
    let mut bad_trailer = sign_streaming_chunks(
        &[b"hello world"],
        &signature_for_request(
            &SignedRequest {
                method: "POST",
                path: "/",
                query: "",
                host,
                authorization: "",
                amz_date,
                content_sha256: STREAMING_SIGNED_TRAILER,
                header: &move |name: &str| {
                    if name == "x-amz-date" {
                        Some(amz_date.to_string())
                    } else if name == "x-amz-content-sha256" {
                        Some(STREAMING_SIGNED_TRAILER.to_string())
                    } else {
                        None
                    }
                },
                body: b"",
            },
            date_stamp,
            "us-east-1",
            signed_headers,
            STREAMING_SIGNED_TRAILER,
            &creds,
        ),
        amz_date,
        Some("x-amz-checksum-crc32:DQpIDQ=="),
    );
    let tpos = bad_trailer
        .windows(8)
        .position(|w| w == b"DQpIDQ==")
        .unwrap();
    bad_trailer[tpos] = b'A';
    assert_eq!(
        run(
            STREAMING_SIGNED_TRAILER,
            &[b"hello world"],
            Some("x-amz-checksum-crc32:DQpIDQ=="),
            None,
            Some(&bad_trailer),
        ),
        Err(SignatureError {
            code: "SignatureDoesNotMatch",
            status: 403,
        })
    );

    // Unsigned trailer framing.
    assert_eq!(
        run(
            STREAMING_UNSIGNED_TRAILER,
            &[],
            None,
            Some("11"),
            Some(b"b\r\nhello world\r\n0\r\nx-amz-checksum-crc32:DQpIDQ==\r\n\r\n"),
        ),
        Ok(())
    );

    // Malformed framing or mismatched decoded length.
    assert_eq!(
        run(
            STREAMING_UNSIGNED_TRAILER,
            &[],
            None,
            None,
            Some(b"not chunked"),
        ),
        Err(SignatureError {
            code: "AuthorizationHeaderMalformed",
            status: 400,
        })
    );
    assert_eq!(
        run(
            STREAMING_UNSIGNED_TRAILER,
            &[],
            None,
            Some("99"),
            Some(b"b\r\nhello world\r\n0\r\n\r\n"),
        ),
        Err(SignatureError {
            code: "AuthorizationHeaderMalformed",
            status: 400,
        })
    );

    // SigV4a chunk signatures cannot be chained from an HMAC seed signature.
    assert_eq!(
        run(
            devcloud_sigv4a::STREAMING_PAYLOAD,
            &[],
            None,
            None,
            Some(b"0\r\n\r\n"),
        ),
        Err(SignatureError {
            code: "AuthorizationHeaderMalformed",
            status: 400,
        })
    );
}

// --- SigV4a (AWS4-ECDSA-P256-SHA256) ---

/// Signs a SigV4a request as `dev`/`dev`, frames `chunks` when the payload is
/// streaming, applies `tamper` to the body, and verifies it under `creds`.
fn sigv4a_check(
    creds: &Credentials,
    payload_hash: &str,
    chunks: &[&[u8]],
    trailers: Option<&[(&str, &str)]>,
    region_set: &str,
    tamper: fn(&mut Vec<u8>),
) -> Result<(), SignatureError> {
    let amz_date = "20261125T000000Z";
    let scope = devcloud_sigv4a::credential_scope("20261125", "sqs");
    let signed_headers = "host;x-amz-content-sha256;x-amz-date;x-amz-region-set";
    let mut hmap = HashMap::new();
    hmap.insert("x-amz-date".to_string(), amz_date.to_string());
    hmap.insert("x-amz-region-set".to_string(), region_set.to_string());
    hmap.insert("x-amz-content-sha256".to_string(), payload_hash.to_string());
    let header_fn = move |name: &str| hmap.get(name).cloned();
    let mut req = SignedRequest {
        method: "POST",
        path: "/",
        query: "",
        host: "127.0.0.1:9324",
        authorization: "",
        amz_date,
        content_sha256: payload_hash,
        header: &header_fn,
        body: b"",
    };
    let key = devcloud_sigv4a::SigningKey::derive("dev", "dev").unwrap();
    let canonical = canonical_request(&req, signed_headers, payload_hash);
    let seed = key.sign(devcloud_sigv4a::string_to_sign(amz_date, &scope, &canonical).as_bytes());
    let auth = format!(
        "AWS4-ECDSA-P256-SHA256 Credential=dev/{scope}, SignedHeaders={signed_headers}, Signature={seed}"
    );
    let mut body = if devcloud_sigv4a::is_streaming(payload_hash) {
        let mut chain = devcloud_sigv4a::ChunkChain::new(key, amz_date, &scope, &seed);
        devcloud_sigv4a::encode_signed_chunks(&mut chain, chunks, trailers, true)
    } else {
        chunks.concat()
    };
    tamper(&mut body);
    req.authorization = &auth;
    req.body = &body;
    verify_signature(&req, creds)
}

#[test]
fn sigv4a_header_and_streaming_payloads_verify() {
    use devcloud_sqs::sigv4::STREAMING_SIGNED;

    let strict = strict_creds();
    let wrong_secret = Credentials {
        secret_access_key: "other",
        ..strict_creds()
    };
    let relaxed = Credentials {
        auth_mode: "relaxed",
        ..strict_creds()
    };
    let keep: fn(&mut Vec<u8>) = |_| {};
    let flip_data: fn(&mut Vec<u8>) = |body| {
        let at = body.windows(5).position(|w| w == b"hello").unwrap();
        body[at] = b'j';
    };
    let trailer = [("x-amz-checksum-crc32", "AAAA")];
    let streaming = devcloud_sigv4a::STREAMING_PAYLOAD;
    let streaming_trailer = devcloud_sigv4a::STREAMING_PAYLOAD_TRAILER;
    let body_hash = sha256_hex(b"hello world");
    let malformed = Err(SignatureError {
        code: "AuthorizationHeaderMalformed",
        status: 400,
    });
    let mismatch = Err(SignatureError {
        code: "SignatureDoesNotMatch",
        status: 403,
    });

    for (name, creds, hash, trailers, region_set, tamper, expected) in [
        (
            "signed body",
            &strict,
            body_hash.as_str(),
            None,
            "*",
            keep,
            Ok(()),
        ),
        (
            "unsigned payload",
            &strict,
            "UNSIGNED-PAYLOAD",
            None,
            "us-east-1",
            keep,
            Ok(()),
        ),
        (
            "streaming",
            &strict,
            streaming,
            None,
            "eu-west-1,us-east-1",
            keep,
            Ok(()),
        ),
        (
            "streaming trailer",
            &strict,
            streaming_trailer,
            Some(&trailer[..]),
            "*",
            keep,
            Ok(()),
        ),
        ("relaxed", &relaxed, streaming, None, "*", flip_data, Ok(())),
        (
            "tampered chunk",
            &strict,
            streaming,
            None,
            "*",
            flip_data,
            mismatch.clone(),
        ),
        (
            "tampered body",
            &strict,
            body_hash.as_str(),
            None,
            "*",
            flip_data,
            mismatch.clone(),
        ),
        (
            "wrong secret",
            &wrong_secret,
            streaming,
            None,
            "*",
            keep,
            mismatch.clone(),
        ),
        (
            "region set",
            &strict,
            streaming,
            None,
            "eu-west-1",
            keep,
            malformed.clone(),
        ),
        (
            "hmac chain with ecdsa seed",
            &strict,
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
                hash,
                &[b"hello ", b"world"],
                trailers,
                region_set,
                tamper
            ),
            expected,
            "{name}"
        );
    }
}
