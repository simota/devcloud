//! SSM Parameter Store behavior through the full request pipeline.

use devcloud_ssm::http::serve_for_test;
use devcloud_ssm::{Config, Server};
use serde_json::{json, Value};

fn server(storage: &str) -> Server {
    Server::new(Config {
        region: "ap-northeast-1".to_string(),
        account_id: "123456789012".to_string(),
        auth_mode: "relaxed".to_string(),
        storage_path: storage.to_string(),
        ..Default::default()
    })
}

fn call(s: &Server, op: &str, body: Value) -> (u16, Option<String>, Value) {
    let target = format!("AmazonSSM.{op}");
    let resp = serve_for_test(
        s,
        "POST",
        "/",
        &[
            ("Content-Type", "application/x-amz-json-1.1"),
            ("X-Amz-Target", &target),
        ],
        body.to_string().as_bytes(),
    );
    (
        resp.status,
        resp.error_type,
        serde_json::from_slice(&resp.body).unwrap(),
    )
}

fn temp_dir(name: &str) -> String {
    let dir = std::env::temp_dir().join(format!(
        "devcloud-ssm-{name}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    dir.to_string_lossy().into_owned()
}

#[test]
fn put_then_get_string() {
    let s = server("");
    let (status, _, body) = call(
        &s,
        "PutParameter",
        json!({"Name": "/csa/dwh/user", "Value": "loader", "Type": "String"}),
    );
    assert_eq!(status, 200);
    assert_eq!(body, json!({"Version": 1, "Tier": "Standard"}));

    let (status, _, body) = call(&s, "GetParameter", json!({"Name": "/csa/dwh/user"}));
    assert_eq!(status, 200);
    let p = &body["Parameter"];
    assert_eq!(p["Name"], "/csa/dwh/user");
    assert_eq!(p["Type"], "String");
    assert_eq!(p["Value"], "loader");
    assert_eq!(p["Version"], 1);
    assert_eq!(p["DataType"], "text");
    assert_eq!(
        p["ARN"],
        "arn:aws:ssm:ap-northeast-1:123456789012:parameter/csa/dwh/user"
    );
    assert!(p["LastModifiedDate"].as_f64().unwrap() > 1_700_000_000.0);
}

#[test]
fn secure_string_requires_with_decryption() {
    let s = server("");
    call(
        &s,
        "PutParameter",
        json!({"Name": "/csa/dwh/password", "Value": "s3cret", "Type": "SecureString"}),
    );

    let (_, _, plain) = call(
        &s,
        "GetParameter",
        json!({"Name": "/csa/dwh/password", "WithDecryption": true}),
    );
    assert_eq!(plain["Parameter"]["Value"], "s3cret");
    assert_eq!(plain["Parameter"]["Type"], "SecureString");

    let (_, _, opaque) = call(&s, "GetParameter", json!({"Name": "/csa/dwh/password"}));
    let value = opaque["Parameter"]["Value"].as_str().unwrap();
    assert_ne!(value, "s3cret");
    assert!(!value.contains("s3cret"));
}

#[test]
fn overwrite_rules_and_versioning() {
    let s = server("");
    call(
        &s,
        "PutParameter",
        json!({"Name": "role", "Value": "a", "Type": "String"}),
    );

    let (status, err, _) = call(
        &s,
        "PutParameter",
        json!({"Name": "role", "Value": "b", "Type": "String"}),
    );
    assert_eq!(
        (status, err.as_deref()),
        (400, Some("ParameterAlreadyExists"))
    );

    // Type may be omitted on overwrite: the existing type is kept.
    let (status, _, body) = call(
        &s,
        "PutParameter",
        json!({"Name": "role", "Value": "b", "Overwrite": true}),
    );
    assert_eq!(status, 200);
    assert_eq!(body["Version"], 2);

    let (_, _, body) = call(&s, "GetParameter", json!({"Name": "role"}));
    assert_eq!(body["Parameter"]["Value"], "b");
    assert_eq!(body["Parameter"]["Type"], "String");

    let (status, _, body) = call(&s, "GetParameter", json!({"Name": "role:2"}));
    assert_eq!(status, 200);
    assert_eq!(body["Parameter"]["Selector"], ":2");
    let (status, err, _) = call(&s, "GetParameter", json!({"Name": "role:1"}));
    assert_eq!(
        (status, err.as_deref()),
        (400, Some("ParameterVersionNotFound"))
    );
}

#[test]
fn validation_errors() {
    let s = server("");
    let cases = [
        json!({"Name": "", "Value": "v", "Type": "String"}),
        json!({"Name": "a/b", "Value": "v", "Type": "String"}),
        json!({"Name": "/aws/x", "Value": "v", "Type": "String"}),
        json!({"Name": "SSM-x", "Value": "v", "Type": "String"}),
        json!({"Name": "bad name", "Value": "v", "Type": "String"}),
        json!({"Name": "x", "Value": "v", "Type": "Number"}),
        json!({"Name": "x", "Value": "", "Type": "String"}),
        json!({"Name": "x", "Type": "String"}),
        json!({"Name": "x", "Value": "v"}),
        json!({"Name": "x", "Value": "v".repeat(4097), "Type": "String"}),
    ];
    for body in cases {
        let (status, err, _) = call(&s, "PutParameter", body.clone());
        assert_eq!(
            (status, err.as_deref()),
            (400, Some("ValidationException")),
            "{body}"
        );
    }
    let (status, _, _) = call(
        &s,
        "PutParameter",
        json!({"Name": "x", "Value": "v".repeat(4097), "Type": "String", "Tier": "Advanced"}),
    );
    assert_eq!(status, 200);
}

#[test]
fn missing_parameter_and_unknown_operation() {
    let s = server("");
    let (status, err, body) = call(&s, "GetParameter", json!({"Name": "/nope"}));
    assert_eq!((status, err.as_deref()), (400, Some("ParameterNotFound")));
    assert_eq!(body["__type"], "ParameterNotFound");
    let (status, err, _) = call(&s, "DeleteParameter", json!({"Name": "/nope"}));
    assert_eq!((status, err.as_deref()), (400, Some("ParameterNotFound")));
    let (status, err, _) = call(&s, "GetParametersByPath", json!({"Path": "/"}));
    assert_eq!(
        (status, err.as_deref()),
        (400, Some("UnknownOperationException"))
    );
}

#[test]
fn delete_removes_parameter() {
    let s = server("");
    call(
        &s,
        "PutParameter",
        json!({"Name": "gone", "Value": "v", "Type": "String"}),
    );
    let (status, _, body) = call(&s, "DeleteParameter", json!({"Name": "gone"}));
    assert_eq!((status, body), (200, json!({})));
    let (status, _, _) = call(&s, "GetParameter", json!({"Name": "gone"}));
    assert_eq!(status, 400);
}

#[test]
fn describe_lists_metadata_without_values_and_paginates() {
    let s = server("");
    for i in 0..3 {
        call(
            &s,
            "PutParameter",
            json!({"Name": format!("/p/{i}"), "Value": format!("secret-{i}"), "Type": "SecureString", "Description": "d"}),
        );
    }
    let (status, _, page1) = call(&s, "DescribeParameters", json!({"MaxResults": 2}));
    assert_eq!(status, 200);
    let names: Vec<_> = page1["Parameters"]
        .as_array()
        .unwrap()
        .iter()
        .map(|p| p["Name"].as_str().unwrap().to_string())
        .collect();
    assert_eq!(names, ["/p/0", "/p/1"]);
    assert!(!page1.to_string().contains("secret-"));
    assert_eq!(page1["Parameters"][0]["KeyId"], "alias/aws/ssm");
    assert_eq!(page1["Parameters"][0]["Description"], "d");

    let token = page1["NextToken"].as_str().unwrap();
    let (_, _, page2) = call(
        &s,
        "DescribeParameters",
        json!({"MaxResults": 2, "NextToken": token}),
    );
    assert_eq!(page2["Parameters"].as_array().unwrap().len(), 1);
    assert!(page2.get("NextToken").is_none());

    let (status, err, _) = call(
        &s,
        "DescribeParameters",
        json!({"ParameterFilters": [{"Key": "Name", "Values": ["/p/0"]}]}),
    );
    assert_eq!((status, err.as_deref()), (400, Some("ValidationException")));
}

#[test]
fn state_persists_across_restart() {
    let dir = temp_dir("persist");
    {
        let s = server(&dir);
        call(
            &s,
            "PutParameter",
            json!({"Name": "/k", "Value": "v1", "Type": "SecureString"}),
        );
    }
    let s = server(&dir);
    assert!(s.load_err().is_none());
    let (status, _, body) = call(
        &s,
        "GetParameter",
        json!({"Name": "/k", "WithDecryption": true}),
    );
    assert_eq!(status, 200);
    assert_eq!(body["Parameter"]["Value"], "v1");
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn rejects_wrong_method_path_and_content_type() {
    let s = server("");
    let r = serve_for_test(&s, "GET", "/", &[], b"");
    assert_eq!(r.status, 405);
    let r = serve_for_test(&s, "POST", "/x", &[], b"");
    assert_eq!(r.status, 404);
    let r = serve_for_test(
        &s,
        "POST",
        "/",
        &[
            ("Content-Type", "text/plain"),
            ("X-Amz-Target", "AmazonSSM.GetParameter"),
        ],
        b"{}",
    );
    assert_eq!(r.status, 400);
}

#[test]
fn strict_mode_requires_signature() {
    let s = Server::new(Config {
        auth_mode: "strict".to_string(),
        ..Default::default()
    });
    let r = serve_for_test(
        &s,
        "POST",
        "/",
        &[("X-Amz-Target", "AmazonSSM.GetParameter")],
        br#"{"Name":"x"}"#,
    );
    assert_eq!(r.status, 403);
}

fn sign_ssm_streaming(
    target: &str,
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

    let host = "127.0.0.1:18031";
    let date_stamp = "20260430";
    let amz_date = "20260430T100000Z";
    let region = "us-east-1";
    let scope = format!("{date_stamp}/{region}/ssm/aws4_request");
    let signed_headers = "host;x-amz-content-sha256;x-amz-date;x-amz-target";
    let canonical_headers = format!(
        "host:{host}\nx-amz-content-sha256:{payload_hash}\nx-amz-date:{amz_date}\nx-amz-target:{target}\n"
    );
    let canonical_request =
        format!("POST\n/\n\n{canonical_headers}\n{signed_headers}\n{payload_hash}");
    let string_to_sign = format!(
        "AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{}",
        sha(canonical_request.as_bytes())
    );
    let k_date = hm(b"AWS4dev", date_stamp.as_bytes());
    let k_region = hm(&k_date, region.as_bytes());
    let k_service = hm(&k_region, b"ssm");
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
        (
            "Content-Type".to_string(),
            "application/x-amz-json-1.1".to_string(),
        ),
        ("Content-Encoding".to_string(), "aws-chunked".to_string()),
        ("X-Amz-Target".to_string(), target.to_string()),
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
fn streaming_payloads_execute_put_and_get_parameter_end_to_end() {
    use devcloud_ssm::sigv4::{
        STREAMING_SIGNED, STREAMING_SIGNED_TRAILER, STREAMING_UNSIGNED_TRAILER,
    };

    let s = Server::new(Config {
        region: "us-east-1".to_string(),
        account_id: "000000000000".to_string(),
        auth_mode: "strict".to_string(),
        access_key_id: "dev".to_string(),
        secret_access_key: "dev".to_string(),
        ..Default::default()
    });

    // 1. Strict mode multi-chunk STREAMING-AWS4-HMAC-SHA256-PAYLOAD PutParameter.
    let put_body = br#"{"Name":"/stream/key","Value":"decoded-value","Type":"String"}"#;
    let (headers, body) = sign_ssm_streaming(
        "AmazonSSM.PutParameter",
        STREAMING_SIGNED,
        &[&put_body[..25], &put_body[25..]],
        None,
        false,
        None,
    );
    let refs: Vec<(&str, &str)> = headers
        .iter()
        .map(|(k, v)| (k.as_str(), v.as_str()))
        .collect();
    let put = serve_for_test(&s, "POST", "/", &refs, &body);
    assert_eq!(put.status, 200, "{}", String::from_utf8_lossy(&put.body));

    // 2. Strict mode STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER GetParameter reads back value.
    let get_body = br#"{"Name":"/stream/key"}"#;
    let (headers, body) = sign_ssm_streaming(
        "AmazonSSM.GetParameter",
        STREAMING_SIGNED_TRAILER,
        &[get_body],
        Some("x-amz-checksum-crc32:AAAA"),
        false,
        None,
    );
    let refs: Vec<(&str, &str)> = headers
        .iter()
        .map(|(k, v)| (k.as_str(), v.as_str()))
        .collect();
    let get = serve_for_test(&s, "POST", "/", &refs, &body);
    assert_eq!(get.status, 200, "{}", String::from_utf8_lossy(&get.body));
    let parsed: Value = serde_json::from_slice(&get.body).unwrap();
    assert_eq!(parsed["Parameter"]["Value"], "decoded-value");

    // 3. Tampered chunk signature rejected with 403.
    let (headers, body) = sign_ssm_streaming(
        "AmazonSSM.GetParameter",
        STREAMING_SIGNED,
        &[get_body],
        None,
        true,
        None,
    );
    let refs: Vec<(&str, &str)> = headers
        .iter()
        .map(|(k, v)| (k.as_str(), v.as_str()))
        .collect();
    let tampered = serve_for_test(&s, "POST", "/", &refs, &body);
    assert_eq!(tampered.status, 403);

    // 4. Malformed framing rejected with 400.
    let (headers, body) = sign_ssm_streaming(
        "AmazonSSM.GetParameter",
        STREAMING_SIGNED,
        &[get_body],
        None,
        false,
        Some(b"not-chunked"),
    );
    let refs: Vec<(&str, &str)> = headers
        .iter()
        .map(|(k, v)| (k.as_str(), v.as_str()))
        .collect();
    let malformed = serve_for_test(&s, "POST", "/", &refs, &body);
    assert_eq!(malformed.status, 400);

    // 5. Relaxed & signed-relaxed modes decode aws-chunked payload.
    for mode in ["relaxed", "signed-relaxed"] {
        let ms = Server::new(Config {
            region: "us-east-1".to_string(),
            auth_mode: mode.to_string(),
            ..Default::default()
        });
        let payload = br#"{"Name":"/relaxed/key","Value":"v","Type":"String"}"#;
        let chunked = format!(
            "{:x}\r\n{}\r\n0\r\nx-amz-checksum-crc32:AAAA\r\n\r\n",
            payload.len(),
            std::str::from_utf8(payload).unwrap()
        );
        let sig = "a".repeat(64);
        let auth = format!(
            "AWS4-HMAC-SHA256 Credential=any/20260430/us-east-1/ssm/aws4_request, SignedHeaders=host;x-amz-date, Signature={sig}"
        );
        let mut hdrs = vec![
            ("Content-Type", "application/x-amz-json-1.1"),
            ("Content-Encoding", "aws-chunked"),
            ("X-Amz-Target", "AmazonSSM.PutParameter"),
            ("X-Amz-Date", "20260430T100000Z"),
            ("X-Amz-Content-Sha256", STREAMING_UNSIGNED_TRAILER),
        ];
        if mode == "signed-relaxed" {
            hdrs.push(("Authorization", &auth));
        }
        let put = serve_for_test(&ms, "POST", "/", &hdrs, chunked.as_bytes());
        assert_eq!(
            put.status,
            200,
            "{mode}: {}",
            String::from_utf8_lossy(&put.body)
        );
    }
}
