//! Parity tests for the SQS SigV4 verifier. The canonical-encoding and SHA-256
//! values are captured from the Go implementation; the accept/reject paths are
//! exercised by signing a request with the crate's own signer (round-trip) and
//! by tampering with each field.

use std::collections::HashMap;

use devcloud_sqs::sigv4::{
    aws_percent_encode, canonical_query_string, normalize_header_value, sha256_hex,
    signature_for_request, verify_signature, Credentials, SignatureError, SignedRequest,
    SIGV4_ALGORITHM,
};

// --- canonicalization oracle (values captured from Go) ---

#[test]
fn sha256_matches_go() {
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
fn percent_encoding_matches_go() {
    assert_eq!(aws_percent_encode("a b+c/d=e", "~-_"), "a%20b%2Bc%2Fd%3De");
    // canonical_uri keeps `/` and `~` safe.
    assert_eq!(
        aws_percent_encode("/000000000000/My Queue", "/~"),
        "/000000000000/My%20Queue"
    );
}

#[test]
fn normalize_header_value_matches_go() {
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
