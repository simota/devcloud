//! STS behavior through the full request pipeline.

use devcloud_sts::http::serve_for_test;
use devcloud_sts::{Config, Server};

const FORM: (&str, &str) = (
    "Content-Type",
    "application/x-www-form-urlencoded; charset=utf-8",
);

fn server() -> Server {
    Server::new(Config {
        account_id: "123456789012".to_string(),
        auth_mode: "relaxed".to_string(),
        ..Default::default()
    })
}

fn post(s: &Server, body: &str) -> (u16, String) {
    let r = serve_for_test(s, "POST", "/", &[FORM], body.as_bytes());
    (r.status, r.body)
}

fn tag<'a>(xml: &'a str, name: &str) -> &'a str {
    let open = format!("<{name}>");
    let close = format!("</{name}>");
    let start = xml
        .find(&open)
        .unwrap_or_else(|| panic!("no <{name}> in {xml}"))
        + open.len();
    let end = start + xml[start..].find(&close).unwrap();
    &xml[start..end]
}

#[test]
fn assume_role_with_inline_policy_returns_credentials() {
    let s = server();
    // What aws-sdk-go-v2 sends: form-encoded, policy JSON percent-encoded.
    let body = "Action=AssumeRole&Version=2011-06-15\
        &RoleArn=arn%3Aaws%3Aiam%3A%3A111122223333%3Arole%2Fcsa%2Fsite-runner\
        &RoleSessionName=site-a%40runner\
        &DurationSeconds=900\
        &Policy=%7B%22Version%22%3A%222012-10-17%22%2C%22Statement%22%3A%5B%5D%7D";
    let (status, xml) = post(&s, body);
    assert_eq!(status, 200, "{xml}");
    assert!(
        xml.starts_with("<AssumeRoleResponse xmlns=\"https://sts.amazonaws.com/doc/2011-06-15/\">")
    );
    assert!(tag(&xml, "AccessKeyId").starts_with("ASIA"));
    assert_eq!(tag(&xml, "AccessKeyId").len(), 20);
    assert!(tag(&xml, "SecretAccessKey").len() >= 40);
    assert!(!tag(&xml, "SessionToken").is_empty());
    assert_eq!(
        tag(&xml, "Arn"),
        "arn:aws:sts::111122223333:assumed-role/site-runner/site-a@runner"
    );
    assert!(tag(&xml, "AssumedRoleId").ends_with(":site-a@runner"));
    assert!(tag(&xml, "PackedPolicySize").parse::<u32>().unwrap() >= 1);
    let expiration = tag(&xml, "Expiration");
    assert_eq!(expiration.len(), "2026-11-03T10:00:00Z".len());
    assert!(expiration.ends_with('Z'));
}

#[test]
fn each_call_issues_distinct_credentials() {
    let s = server();
    let body = "Action=AssumeRole&RoleArn=arn:aws:iam::1:role/r&RoleSessionName=s1";
    let (_, a) = post(&s, body);
    let (_, b) = post(&s, body);
    assert_ne!(tag(&a, "AccessKeyId"), tag(&b, "AccessKeyId"));
    assert_ne!(tag(&a, "SessionToken"), tag(&b, "SessionToken"));
}

#[test]
fn assume_role_validation_errors() {
    let s = server();
    let cases = [
        ("Action=AssumeRole&RoleSessionName=s1", "ValidationError"),
        (
            "Action=AssumeRole&RoleArn=arn:aws:iam::1:role/r",
            "ValidationError",
        ),
        (
            "Action=AssumeRole&RoleArn=arn:aws:iam::1:role/r&RoleSessionName=x",
            "ValidationError",
        ),
        (
            "Action=AssumeRole&RoleArn=arn:aws:iam::1:role/r&RoleSessionName=bad%20name",
            "ValidationError",
        ),
        (
            "Action=AssumeRole&RoleArn=arn:aws:iam::1:user/u&RoleSessionName=s1",
            "ValidationError",
        ),
        (
            "Action=AssumeRole&RoleArn=arn:aws:iam::1:role/r&RoleSessionName=s1&DurationSeconds=60",
            "ValidationError",
        ),
        (
            "Action=AssumeRole&RoleArn=arn:aws:iam::1:role/r&RoleSessionName=s1&Policy=not-json",
            "MalformedPolicyDocument",
        ),
    ];
    for (body, code) in cases {
        let (status, xml) = post(&s, body);
        assert_eq!(status, 400, "{body}");
        assert_eq!(tag(&xml, "Code"), code, "{body}");
        assert_eq!(tag(&xml, "Type"), "Sender");
    }
}

#[test]
fn get_caller_identity_via_get_and_post() {
    let s = server();
    let (status, xml) = post(&s, "Action=GetCallerIdentity&Version=2011-06-15");
    assert_eq!(status, 200);
    assert_eq!(tag(&xml, "Account"), "123456789012");
    assert_eq!(tag(&xml, "Arn"), "arn:aws:iam::123456789012:root");

    let r = serve_for_test(
        &s,
        "GET",
        "/?Action=GetCallerIdentity&Version=2011-06-15",
        &[],
        b"",
    );
    assert_eq!(r.status, 200);
}

#[test]
fn unknown_and_missing_action() {
    let s = server();
    let (status, xml) = post(&s, "Action=GetSessionToken");
    assert_eq!((status, tag(&xml, "Code")), (400, "InvalidAction"));
    let (status, xml) = post(&s, "Version=2011-06-15");
    assert_eq!((status, tag(&xml, "Code")), (400, "MissingAction"));
}

#[test]
fn strict_mode_requires_signature() {
    let s = Server::new(Config {
        auth_mode: "strict".to_string(),
        ..Default::default()
    });
    let (status, _) = post(&s, "Action=GetCallerIdentity");
    assert_eq!(status, 403);
}

fn sign_sts_streaming(
    payload_hash: &str,
    chunks: &[&[u8]],
    trailer: Option<&str>,
    tamper_chunk_sig: bool,
    override_body: Option<&[u8]>,
) -> (Vec<(String, String)>, Vec<u8>) {
    use hmac::{Hmac, Mac};
    use sha2::{Digest, Sha256};

    fn sha(b: &[u8]) -> String {
        hex::encode(Sha256::digest(b))
    }
    fn hm(key: &[u8], data: &[u8]) -> Vec<u8> {
        let mut mac = Hmac::<Sha256>::new_from_slice(key).unwrap();
        mac.update(data);
        mac.finalize().into_bytes().to_vec()
    }

    let host = "127.0.0.1:18032";
    let date_stamp = "20260430";
    let amz_date = "20260430T100000Z";
    let region = "us-east-1";
    let scope = format!("{date_stamp}/{region}/sts/aws4_request");
    let signed_headers = "host;x-amz-content-sha256;x-amz-date";
    let canonical_headers =
        format!("host:{host}\nx-amz-content-sha256:{payload_hash}\nx-amz-date:{amz_date}\n");
    let canonical_request =
        format!("POST\n/\n\n{canonical_headers}\n{signed_headers}\n{payload_hash}");
    let string_to_sign = format!(
        "AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{}",
        sha(canonical_request.as_bytes())
    );
    let k_date = hm(b"AWS4dev", date_stamp.as_bytes());
    let k_region = hm(&k_date, region.as_bytes());
    let k_service = hm(&k_region, b"sts");
    let signing_key = hm(&k_service, b"aws4_request");
    let seed = hex::encode(hm(&signing_key, string_to_sign.as_bytes()));

    let decoded_len: usize = chunks.iter().map(|c| c.len()).sum();
    let body = match override_body {
        Some(raw) => raw.to_vec(),
        None => {
            let empty_hash = sha(b"");
            let mut prev = seed.clone();
            let mut out = Vec::new();
            for (idx, chunk) in chunks.iter().copied().chain([&b""[..]]).enumerate() {
                let to_sign = format!(
                    "AWS4-HMAC-SHA256-PAYLOAD\n{amz_date}\n{scope}\n{prev}\n{empty_hash}\n{}",
                    sha(chunk)
                );
                let mut sig = hex::encode(hm(&signing_key, to_sign.as_bytes()));
                prev = sig.clone();
                if tamper_chunk_sig && idx == 0 {
                    let last = sig.pop().unwrap();
                    sig.push(if last == '0' { '1' } else { '0' });
                }
                out.extend(format!("{:x};chunk-signature={sig}\r\n", chunk.len()).into_bytes());
                out.extend_from_slice(chunk);
                out.extend_from_slice(b"\r\n");
            }
            if let Some(trailer) = trailer {
                out.truncate(out.len() - 2);
                let to_sign = format!(
                    "AWS4-HMAC-SHA256-TRAILER\n{amz_date}\n{scope}\n{prev}\n{}",
                    sha(format!("{trailer}\n").as_bytes())
                );
                let sig = hex::encode(hm(&signing_key, to_sign.as_bytes()));
                out.extend(
                    format!("{trailer}\r\nx-amz-trailer-signature:{sig}\r\n\r\n").into_bytes(),
                );
            }
            out
        }
    };
    let auth = format!(
        "AWS4-HMAC-SHA256 Credential=dev/{scope}, SignedHeaders={signed_headers}, Signature={seed}"
    );
    let headers = vec![
        ("Host".to_string(), host.to_string()),
        (FORM.0.to_string(), FORM.1.to_string()),
        ("Content-Encoding".to_string(), "aws-chunked".to_string()),
        ("X-Amz-Date".to_string(), amz_date.to_string()),
        ("X-Amz-Content-Sha256".to_string(), payload_hash.to_string()),
        (
            "X-Amz-Decoded-Content-Length".to_string(),
            decoded_len.to_string(),
        ),
        ("Authorization".to_string(), auth),
    ];
    (headers, body)
}

#[test]
fn streaming_payloads_execute_get_caller_identity_and_assume_role_end_to_end() {
    use devcloud_sts::sigv4::{
        STREAMING_SIGNED, STREAMING_SIGNED_TRAILER, STREAMING_UNSIGNED_TRAILER,
    };

    let s = Server::new(Config {
        region: "us-east-1".to_string(),
        account_id: "123456789012".to_string(),
        auth_mode: "strict".to_string(),
        access_key_id: "dev".to_string(),
        secret_access_key: "dev".to_string(),
        ..Default::default()
    });

    // 1. Strict mode multi-chunk STREAMING-AWS4-HMAC-SHA256-PAYLOAD GetCallerIdentity.
    let gci_body = b"Action=GetCallerIdentity&Version=2011-06-15";
    let (headers, body) = sign_sts_streaming(
        STREAMING_SIGNED,
        &[&gci_body[..20], &gci_body[20..]],
        None,
        false,
        None,
    );
    let refs: Vec<(&str, &str)> = headers
        .iter()
        .map(|(k, v)| (k.as_str(), v.as_str()))
        .collect();
    let resp = serve_for_test(&s, "POST", "/", &refs, &body);
    assert_eq!(resp.status, 200, "{}", resp.body);
    assert_eq!(tag(&resp.body, "Account"), "123456789012");

    // 2. Strict mode STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER AssumeRole.
    let assume_body =
        b"Action=AssumeRole&Version=2011-06-15&RoleArn=arn:aws:iam::123456789012:role/r&RoleSessionName=s1";
    let (headers, body) = sign_sts_streaming(
        STREAMING_SIGNED_TRAILER,
        &[assume_body],
        Some("x-amz-checksum-crc32:AAAA"),
        false,
        None,
    );
    let refs: Vec<(&str, &str)> = headers
        .iter()
        .map(|(k, v)| (k.as_str(), v.as_str()))
        .collect();
    let resp = serve_for_test(&s, "POST", "/", &refs, &body);
    assert_eq!(resp.status, 200, "{}", resp.body);
    assert!(tag(&resp.body, "AccessKeyId").starts_with("ASIA"));

    // 3. Tampered chunk signature rejected with 403.
    let (headers, body) = sign_sts_streaming(STREAMING_SIGNED, &[gci_body], None, true, None);
    let refs: Vec<(&str, &str)> = headers
        .iter()
        .map(|(k, v)| (k.as_str(), v.as_str()))
        .collect();
    let resp = serve_for_test(&s, "POST", "/", &refs, &body);
    assert_eq!(resp.status, 403, "{}", resp.body);

    // 4. Malformed framing rejected with 400.
    let (headers, body) = sign_sts_streaming(
        STREAMING_SIGNED,
        &[gci_body],
        None,
        false,
        Some(b"not-chunked"),
    );
    let refs: Vec<(&str, &str)> = headers
        .iter()
        .map(|(k, v)| (k.as_str(), v.as_str()))
        .collect();
    let resp = serve_for_test(&s, "POST", "/", &refs, &body);
    assert_eq!(resp.status, 400, "{}", resp.body);

    // 5. Relaxed & signed-relaxed modes decode aws-chunked body.
    for mode in ["relaxed", "signed-relaxed"] {
        let ms = Server::new(Config {
            account_id: "123456789012".to_string(),
            auth_mode: mode.to_string(),
            ..Default::default()
        });
        let chunked = format!(
            "{:x}\r\nAction=GetCallerIdentity&Version=2011-06-15\r\n0\r\nx-amz-checksum-crc32:AAAA\r\n\r\n",
            gci_body.len()
        );
        let sig = "a".repeat(64);
        let auth = format!(
            "AWS4-HMAC-SHA256 Credential=any/20260430/us-east-1/sts/aws4_request, SignedHeaders=host;x-amz-date, Signature={sig}"
        );
        let mut hdrs = vec![
            FORM,
            ("Content-Encoding", "aws-chunked"),
            ("X-Amz-Date", "20260430T100000Z"),
            ("X-Amz-Content-Sha256", STREAMING_UNSIGNED_TRAILER),
        ];
        if mode == "signed-relaxed" {
            hdrs.push(("Authorization", &auth));
        }
        let resp = serve_for_test(&ms, "POST", "/", &hdrs, chunked.as_bytes());
        assert_eq!(resp.status, 200, "{mode}: {}", resp.body);
        assert_eq!(tag(&resp.body, "Account"), "123456789012");
    }
}
