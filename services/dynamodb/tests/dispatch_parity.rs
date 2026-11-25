//! Parity tests for tags / resource policy and the `dispatch` HTTP routing
//! layer, against golden oracles captured from the legacy service.

use devcloud_dynamodb::model::{AttributeDefinition, KeySchemaElement, Tag};
use devcloud_dynamodb::requests::{
    CreateTableRequest, ListTagsOfResourceRequest, PutResourcePolicyRequest, ResourceArnRequest,
    TagResourceRequest, UntagResourceRequest,
};
use devcloud_dynamodb::server::{Config, Server};

const ARN: &str = "arn:aws:dynamodb:us-east-1:000000000000:table/T";

fn server(dir: &std::path::Path) -> Server {
    let mut s = Server::new(Config {
        region: "us-east-1".to_string(),
        auth_mode: "relaxed".to_string(),
        storage_path: dir.to_string_lossy().to_string(),
        ..Default::default()
    });
    s.set_fixed_now(1_780_099_289);
    s.create_table(&CreateTableRequest {
        table_name: "T".to_string(),
        attribute_definitions: vec![AttributeDefinition {
            attribute_name: "pk".to_string(),
            attribute_type: "S".to_string(),
        }],
        key_schema: vec![KeySchemaElement {
            attribute_name: "pk".to_string(),
            key_type: "HASH".to_string(),
        }],
        billing_mode: "PAY_PER_REQUEST".to_string(),
        ..Default::default()
    })
    .expect("create");
    s
}

fn matches(got: &[u8], fixture: &[u8], label: &str) {
    assert_eq!(
        String::from_utf8_lossy(got),
        String::from_utf8_lossy(fixture),
        "{label}"
    );
}

fn tag(k: &str, v: &str) -> Tag {
    Tag {
        key: k.to_string(),
        value: v.to_string(),
    }
}

#[test]
fn tags_lifecycle_matches_oracle() {
    let dir = tempdir();
    let mut s = server(&dir);
    matches(
        &s.tag_resource(&TagResourceRequest {
            resource_arn: ARN.to_string(),
            tags: vec![tag("env", "prod"), tag("team", "core")],
        })
        .expect("tag"),
        include_bytes!("fixtures/tag.json"),
        "tag",
    );
    matches(
        &s.list_tags_of_resource(&ListTagsOfResourceRequest {
            resource_arn: ARN.to_string(),
            ..Default::default()
        })
        .expect("list"),
        include_bytes!("fixtures/tags_list.json"),
        "tags_list",
    );
    matches(
        &s.untag_resource(&UntagResourceRequest {
            resource_arn: ARN.to_string(),
            tag_keys: vec!["team".to_string()],
        })
        .expect("untag"),
        include_bytes!("fixtures/untag.json"),
        "untag",
    );
    matches(
        &s.list_tags_of_resource(&ListTagsOfResourceRequest {
            resource_arn: ARN.to_string(),
            ..Default::default()
        })
        .expect("list2"),
        include_bytes!("fixtures/tags_list2.json"),
        "tags_list2",
    );
}

#[test]
fn resource_policy_lifecycle_matches_oracle() {
    let dir = tempdir();
    let mut s = server(&dir);
    matches(
        &s.put_resource_policy(&PutResourcePolicyRequest {
            resource_arn: ARN.to_string(),
            policy: "{\"Version\":\"2012-10-17\",\"Statement\":[]}".to_string(),
        })
        .expect("put"),
        include_bytes!("fixtures/putpol.json"),
        "putpol",
    );
    matches(
        &s.get_resource_policy(&ResourceArnRequest {
            resource_arn: ARN.to_string(),
        })
        .expect("get"),
        include_bytes!("fixtures/getpol.json"),
        "getpol",
    );
    matches(
        &s.delete_resource_policy(&ResourceArnRequest {
            resource_arn: ARN.to_string(),
        })
        .expect("del"),
        include_bytes!("fixtures/delpol.json"),
        "delpol",
    );
    assert_eq!(
        s.get_resource_policy(&ResourceArnRequest {
            resource_arn: ARN.to_string(),
        })
        .expect_err("gone")
        .name,
        "PolicyNotFoundException"
    );
}

#[test]
fn full_lifecycle_state_matches_oracle() {
    let dir = tempdir();
    let mut s = server(&dir);
    s.tag_resource(&TagResourceRequest {
        resource_arn: ARN.to_string(),
        tags: vec![tag("env", "prod"), tag("team", "core")],
    })
    .expect("tag");
    s.untag_resource(&UntagResourceRequest {
        resource_arn: ARN.to_string(),
        tag_keys: vec!["team".to_string()],
    })
    .expect("untag");
    s.put_resource_policy(&PutResourcePolicyRequest {
        resource_arn: ARN.to_string(),
        policy: "{\"Version\":\"2012-10-17\",\"Statement\":[]}".to_string(),
    })
    .expect("put");
    s.delete_resource_policy(&ResourceArnRequest {
        resource_arn: ARN.to_string(),
    })
    .expect("del");
    let on_disk = std::fs::read(dir.join("state.json")).expect("read");
    matches(
        &on_disk,
        include_bytes!("fixtures/tagspol_state.json"),
        "tagspol_state",
    );
}

#[test]
fn dispatch_routes_operations() {
    let dir = tempdir();
    let mut s = server(&dir);
    // PutItem via dispatch.
    let put = s.dispatch(
        "DynamoDB_20120810.PutItem",
        br#"{"TableName":"T","Item":{"pk":{"S":"x"}}}"#,
        "relaxed",
        None,
        1_780_000_000,
    );
    assert_eq!(put.status, 200);
    assert_eq!(String::from_utf8_lossy(&put.body), "{}\n");

    // GetItem via dispatch returns the item.
    let get = s.dispatch(
        "DynamoDB_20120810.GetItem",
        br#"{"TableName":"T","Key":{"pk":{"S":"x"}}}"#,
        "relaxed",
        None,
        1_780_000_000,
    );
    assert_eq!(get.status, 200);
    assert_eq!(
        String::from_utf8_lossy(&get.body),
        "{\"Item\":{\"pk\":{\"S\":\"x\"}}}\n"
    );

    // Unknown operation.
    let unknown = s.dispatch(
        "DynamoDB_20120810.Nope",
        b"{}",
        "relaxed",
        None,
        1_780_000_000,
    );
    assert_eq!(unknown.status, 400);
    assert_eq!(
        unknown.error_type.as_deref(),
        Some("UnknownOperationException")
    );

    // Bad target prefix.
    let bad = s.dispatch("Bogus.Op", b"{}", "relaxed", None, 1_780_000_000);
    assert_eq!(bad.status, 400);
    assert_eq!(bad.error_type.as_deref(), Some("UnknownOperationException"));

    // Malformed JSON.
    let malformed = s.dispatch(
        "DynamoDB_20120810.PutItem",
        b"{not json",
        "relaxed",
        None,
        1_780_000_000,
    );
    assert_eq!(malformed.status, 400);
    assert_eq!(
        malformed.error_type.as_deref(),
        Some("SerializationException")
    );
}

#[test]
fn dispatch_expires_ttl_before_serving() {
    let dir = tempdir();
    let mut s = server(&dir);
    s.dispatch(
        "DynamoDB_20120810.UpdateTimeToLive",
        br#"{"TableName":"T","TimeToLiveSpecification":{"AttributeName":"exp","Enabled":true}}"#,
        "relaxed",
        None,
        1_780_000_000,
    );
    s.dispatch(
        "DynamoDB_20120810.PutItem",
        br#"{"TableName":"T","Item":{"pk":{"S":"old"},"exp":{"N":"1"}}}"#,
        "relaxed",
        None,
        1_780_000_000,
    );
    // A later dispatch with a now past the expiry removes the item first.
    let scan = s.dispatch(
        "DynamoDB_20120810.Scan",
        br#"{"TableName":"T"}"#,
        "relaxed",
        None,
        1_780_000_000,
    );
    assert_eq!(
        String::from_utf8_lossy(&scan.body),
        "{\"Count\":0,\"Items\":[],\"ScannedCount\":0}\n"
    );
}

#[test]
fn list_tags_of_unknown_resource_errors() {
    let dir = tempdir();
    let s = server(&dir);
    assert_eq!(
        s.list_tags_of_resource(&ListTagsOfResourceRequest {
            resource_arn: "arn:aws:dynamodb:us-east-1:000000000000:table/Nope".to_string(),
            ..Default::default()
        })
        .expect_err("nope")
        .name,
        "ResourceNotFoundException"
    );
}

// --- minimal tempdir -------------------------------------------------------

fn tempdir() -> std::path::PathBuf {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let mut dir = std::env::temp_dir();
    dir.push(format!(
        "devcloud-ddb-dispatch-{}-{}",
        std::process::id(),
        n
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create tempdir");
    dir
}

fn ddb_sha256_hex(data: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    hex::encode(Sha256::digest(data))
}

fn ddb_hmac(key: &[u8], data: &[u8]) -> Vec<u8> {
    use hmac::{Hmac, Mac};
    use sha2::Sha256;
    let mut mac = Hmac::<Sha256>::new_from_slice(key).unwrap();
    mac.update(data);
    mac.finalize().into_bytes().to_vec()
}

fn build_ddb_streaming_http(
    host: &str,
    target: &str,
    payload_hash: &str,
    chunks: &[&[u8]],
    trailer: Option<&str>,
    tamper_chunk_sig: bool,
    override_body: Option<&[u8]>,
) -> Vec<u8> {
    let date_stamp = "20260430";
    let amz_date = "20260430T100000Z";
    let region = "us-east-1";
    let secret = "dev";
    let scope = format!("{date_stamp}/{region}/dynamodb/aws4_request");
    let signed_headers = "host;x-amz-content-sha256;x-amz-date;x-amz-target";
    let canonical_headers = format!(
        "host:{host}\nx-amz-content-sha256:{payload_hash}\nx-amz-date:{amz_date}\nx-amz-target:{target}\n"
    );
    let canonical_request =
        format!("POST\n/\n\n{canonical_headers}\n{signed_headers}\n{payload_hash}");
    let string_to_sign = format!(
        "AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{}",
        ddb_sha256_hex(canonical_request.as_bytes())
    );
    let k_date = ddb_hmac(format!("AWS4{secret}").as_bytes(), date_stamp.as_bytes());
    let k_region = ddb_hmac(&k_date, region.as_bytes());
    let k_service = ddb_hmac(&k_region, b"dynamodb");
    let signing_key = ddb_hmac(&k_service, b"aws4_request");
    let seed = hex::encode(ddb_hmac(&signing_key, string_to_sign.as_bytes()));

    let decoded_len: usize = chunks.iter().map(|c| c.len()).sum();
    let body = match override_body {
        Some(raw) => raw.to_vec(),
        None => {
            let empty_hash = ddb_sha256_hex(b"");
            let mut prev = seed.clone();
            let mut out = Vec::new();
            for (idx, chunk) in chunks.iter().copied().chain([&b""[..]]).enumerate() {
                let to_sign = format!(
                    "AWS4-HMAC-SHA256-PAYLOAD\n{amz_date}\n{scope}\n{prev}\n{empty_hash}\n{}",
                    ddb_sha256_hex(chunk)
                );
                let mut sig = hex::encode(ddb_hmac(&signing_key, to_sign.as_bytes()));
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
                    ddb_sha256_hex(format!("{trailer}\n").as_bytes())
                );
                let sig = hex::encode(ddb_hmac(&signing_key, to_sign.as_bytes()));
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
    let mut wire = format!(
        "POST / HTTP/1.1\r\n\
         Host: {host}\r\n\
         Content-Type: application/x-amz-json-1.0\r\n\
         Content-Encoding: aws-chunked\r\n\
         X-Amz-Target: {target}\r\n\
         X-Amz-Date: {amz_date}\r\n\
         X-Amz-Content-Sha256: {payload_hash}\r\n\
         X-Amz-Decoded-Content-Length: {decoded_len}\r\n\
         Authorization: {auth}\r\n\
         Content-Length: {}\r\n\r\n",
        body.len()
    )
    .into_bytes();
    wire.extend_from_slice(&body);
    wire
}

async fn send_raw_http(addr: std::net::SocketAddr, wire: &[u8]) -> (u16, Vec<u8>) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
    stream.write_all(wire).await.unwrap();
    let mut buf = Vec::new();
    stream.read_to_end(&mut buf).await.unwrap();
    let pos = buf.windows(4).position(|w| w == b"\r\n\r\n").unwrap();
    let head = String::from_utf8_lossy(&buf[..pos]);
    let status = head
        .lines()
        .next()
        .unwrap()
        .split(' ')
        .nth(1)
        .unwrap()
        .parse()
        .unwrap();
    (status, buf[pos + 4..].to_vec())
}

#[tokio::test]
async fn streaming_payloads_execute_put_and_get_item_end_to_end() {
    use devcloud_dynamodb::sigv4::{
        STREAMING_SIGNED, STREAMING_SIGNED_TRAILER, STREAMING_UNSIGNED_TRAILER,
    };
    use std::sync::{Arc, Mutex};

    let dir = tempdir();
    let s = Arc::new(Mutex::new(server(&dir)));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let host = addr.to_string();
    tokio::spawn(devcloud_dynamodb::http::serve(
        listener,
        Arc::clone(&s),
        "strict".to_string(),
        std::future::pending(),
    ));

    // 1. Strict mode multi-chunk STREAMING-AWS4-HMAC-SHA256-PAYLOAD PutItem.
    let put_body = br#"{"TableName":"T","Item":{"pk":{"S":"stream-1"},"val":{"S":"ok"}}}"#;
    let wire = build_ddb_streaming_http(
        &host,
        "DynamoDB_20120810.PutItem",
        STREAMING_SIGNED,
        &[&put_body[..24], &put_body[24..]],
        None,
        false,
        None,
    );
    let (status, resp) = send_raw_http(addr, &wire).await;
    assert_eq!(status, 200, "{}", String::from_utf8_lossy(&resp));

    // 2. Strict mode STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER GetItem reads back the item.
    let get_body = br#"{"TableName":"T","Key":{"pk":{"S":"stream-1"}}}"#;
    let wire = build_ddb_streaming_http(
        &host,
        "DynamoDB_20120810.GetItem",
        STREAMING_SIGNED_TRAILER,
        &[get_body],
        Some("x-amz-checksum-crc32:AAAA"),
        false,
        None,
    );
    let (status, resp) = send_raw_http(addr, &wire).await;
    assert_eq!(status, 200, "{}", String::from_utf8_lossy(&resp));
    let got: serde_json::Value = serde_json::from_slice(&resp).unwrap();
    assert_eq!(got["Item"]["val"]["S"], "ok");

    // 3. Strict mode rejects tampered chunk signature with 403.
    let wire = build_ddb_streaming_http(
        &host,
        "DynamoDB_20120810.PutItem",
        STREAMING_SIGNED,
        &[put_body],
        None,
        true,
        None,
    );
    let (status, resp) = send_raw_http(addr, &wire).await;
    assert_eq!(status, 403, "{}", String::from_utf8_lossy(&resp));

    // 4. Strict mode rejects malformed framing with 400.
    let wire = build_ddb_streaming_http(
        &host,
        "DynamoDB_20120810.PutItem",
        STREAMING_SIGNED,
        &[put_body],
        None,
        false,
        Some(b"bad-chunked-framing"),
    );
    let (status, resp) = send_raw_http(addr, &wire).await;
    assert_eq!(status, 400, "{}", String::from_utf8_lossy(&resp));

    // 5. Relaxed & signed-relaxed modes decode aws-chunked body and execute PutItem/GetItem.
    for mode in ["relaxed", "signed-relaxed"] {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let maddr = listener.local_addr().unwrap();
        let mhost = maddr.to_string();
        tokio::spawn(devcloud_dynamodb::http::serve(
            listener,
            Arc::clone(&s),
            mode.to_string(),
            std::future::pending(),
        ));
        let item_json = format!(r#"{{"TableName":"T","Item":{{"pk":{{"S":"{mode}-pk"}}}}}}"#);
        let chunked = format!(
            "{:x}\r\n{item_json}\r\n0\r\nx-amz-checksum-crc32:AAAA\r\n\r\n",
            item_json.len()
        );
        let auth_header = if mode == "signed-relaxed" {
            format!(
                "Authorization: AWS4-HMAC-SHA256 Credential=any/20260430/us-east-1/dynamodb/aws4_request, SignedHeaders=host;x-amz-date, Signature={}\r\n",
                "a".repeat(64)
            )
        } else {
            String::new()
        };
        let wire = format!(
            "POST / HTTP/1.1\r\n\
             Host: {mhost}\r\n\
             Content-Type: application/x-amz-json-1.0\r\n\
             Content-Encoding: aws-chunked\r\n\
             X-Amz-Target: DynamoDB_20120810.PutItem\r\n\
             X-Amz-Date: 20260430T100000Z\r\n\
             X-Amz-Content-Sha256: {STREAMING_UNSIGNED_TRAILER}\r\n\
             X-Amz-Decoded-Content-Length: {}\r\n\
             {auth_header}\
             Content-Length: {}\r\n\r\n{chunked}",
            item_json.len(),
            chunked.len()
        );
        let (status, resp) = send_raw_http(maddr, wire.as_bytes()).await;
        assert_eq!(status, 200, "{mode}: {}", String::from_utf8_lossy(&resp));
    }
}
