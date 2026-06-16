use devcloud_s3::http::{route, route_with_auth, AuthConfig, Request};
use devcloud_s3::store::FileBucketStore;
use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};

fn tempdir() -> std::path::PathBuf {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let mut dir = std::env::temp_dir();
    dir.push(format!("devcloud-s3-http-{}-{}", std::process::id(), n));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn req(method: &str, target: &str) -> Request {
    Request::new(method, target, Vec::new())
}

fn event_stream_records(mut data: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    while data.len() >= 16 {
        let total = u32::from_be_bytes(data[0..4].try_into().unwrap()) as usize;
        let headers_len = u32::from_be_bytes(data[4..8].try_into().unwrap()) as usize;
        if total > data.len() || total < 16 + headers_len {
            break;
        }
        let payload_start = 12 + headers_len;
        let payload_end = total - 4;
        let headers = &data[12..payload_start];
        if event_type(headers) == Some("Records".to_string()) {
            out.extend(&data[payload_start..payload_end]);
        }
        data = &data[total..];
    }
    out
}

fn event_type(mut headers: &[u8]) -> Option<String> {
    while !headers.is_empty() {
        let name_len = *headers.first()? as usize;
        headers = headers.get(1..)?;
        let name = std::str::from_utf8(headers.get(..name_len)?).ok()?;
        headers = headers.get(name_len..)?;
        let value_type = *headers.first()?;
        headers = headers.get(1..)?;
        if value_type != 7 {
            return None;
        }
        let value_len = u16::from_be_bytes(headers.get(..2)?.try_into().ok()?) as usize;
        headers = headers.get(2..)?;
        let value = std::str::from_utf8(headers.get(..value_len)?).ok()?;
        headers = headers.get(value_len..)?;
        if name == ":event-type" {
            return Some(value.to_string());
        }
    }
    None
}

fn strict_auth() -> AuthConfig {
    AuthConfig {
        auth_mode: "strict".to_string(),
        access_key_id: "dev".to_string(),
        secret_access_key: "dev".to_string(),
        region: "us-east-1".to_string(),
    }
}

fn signed_req(method: &str, target: &str, body: &[u8]) -> Request {
    let mut req = Request::new(method, target, body.to_vec());
    req.headers
        .insert("host".to_string(), "example.com".to_string());
    req.headers
        .insert("x-amz-date".to_string(), "20260430T120000Z".to_string());
    let body_hash = sha256_hex(body);
    req.headers
        .insert("x-amz-content-sha256".to_string(), body_hash.clone());
    let signed_headers = "host;x-amz-content-sha256;x-amz-date";
    let canonical_request = [
        method.to_string(),
        target.to_string(),
        String::new(),
        format!(
            "host:example.com\nx-amz-content-sha256:{body_hash}\nx-amz-date:20260430T120000Z\n"
        ),
        signed_headers.to_string(),
        body_hash,
    ]
    .join("\n");
    let scope = "20260430/us-east-1/s3/aws4_request";
    let string_to_sign = [
        "AWS4-HMAC-SHA256".to_string(),
        "20260430T120000Z".to_string(),
        scope.to_string(),
        sha256_hex(canonical_request.as_bytes()),
    ]
    .join("\n");
    let signature = hex::encode(hmac_sha256(
        &test_signing_key("dev", "20260430", "us-east-1"),
        string_to_sign.as_bytes(),
    ));
    req.headers.insert(
        "authorization".to_string(),
        format!(
            "AWS4-HMAC-SHA256 Credential=dev/{scope}, SignedHeaders={signed_headers}, Signature={signature}"
        ),
    );
    req
}

fn test_signing_key(secret: &str, date_stamp: &str, region: &str) -> Vec<u8> {
    let date_key = hmac_sha256(format!("AWS4{secret}").as_bytes(), date_stamp.as_bytes());
    let region_key = hmac_sha256(&date_key, region.as_bytes());
    let service_key = hmac_sha256(&region_key, b"s3");
    hmac_sha256(&service_key, b"aws4_request")
}

fn hmac_sha256(key: &[u8], value: &[u8]) -> Vec<u8> {
    let mut mac = Hmac::<Sha256>::new_from_slice(key).unwrap();
    mac.update(value);
    mac.finalize().into_bytes().to_vec()
}

fn sha256_hex(value: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(value);
    hex::encode(hasher.finalize())
}

#[test]
fn bucket_lifecycle_and_service_listing() {
    let root = tempdir();
    let mut store = FileBucketStore::new(&root);
    store.set_fixed_now("2026-05-30T12:00:00Z");

    let created = route(&store, &req("PUT", "/alpha"));
    assert_eq!(created.status, 200);
    assert_eq!(created.headers.get("Location").unwrap(), "/alpha");

    let duplicate = route(&store, &req("PUT", "/alpha"));
    assert_eq!(duplicate.status, 409);
    assert!(String::from_utf8(duplicate.body)
        .unwrap()
        .contains("BucketAlreadyOwnedByYou"));

    let head = route(&store, &req("HEAD", "/alpha"));
    assert_eq!(head.status, 200);

    let list = route(&store, &req("GET", "/"));
    assert_eq!(list.status, 200);
    let list_body = String::from_utf8(list.body).unwrap();
    assert!(list_body.contains("<Name>alpha</Name>"));
    assert!(list_body.contains("<CreationDate>2026-05-30T12:00:00Z</CreationDate>"));

    let deleted = route(&store, &req("DELETE", "/alpha"));
    assert_eq!(deleted.status, 204);
}

#[test]
fn sigv4_strict_header_auth_and_payload_hash_validation() {
    let root = tempdir();
    let store = FileBucketStore::new(&root);
    let auth = strict_auth();

    let unsigned = route_with_auth(&store, &req("GET", "/"), &auth);
    assert_eq!(unsigned.status, 403);
    assert!(String::from_utf8(unsigned.body)
        .unwrap()
        .contains("<Code>AccessDenied</Code>"));

    let create = signed_req("PUT", "/demo-bucket", b"");
    assert_eq!(route_with_auth(&store, &create, &auth).status, 200);

    let put = signed_req("PUT", "/demo-bucket/docs/readme.txt", b"signed body\n");
    assert_eq!(route_with_auth(&store, &put, &auth).status, 200);

    let get = signed_req("GET", "/demo-bucket/docs/readme.txt", b"");
    let got = route_with_auth(&store, &get, &auth);
    assert_eq!(got.status, 200);
    assert_eq!(got.body, b"signed body\n");

    let mut tampered = signed_req("PUT", "/demo-bucket/docs/tampered.txt", b"original body");
    tampered.body = b"tampered body".to_vec();
    let rejected = route_with_auth(&store, &tampered, &auth);
    assert_eq!(rejected.status, 400);
    assert!(String::from_utf8(rejected.body)
        .unwrap()
        .contains("<Code>XAmzContentSHA256Mismatch</Code>"));
}

#[test]
fn object_put_get_head_delete_and_metadata() {
    let root = tempdir();
    let mut store = FileBucketStore::new(&root);
    store.set_fixed_now("2026-05-30T12:00:00Z");
    assert_eq!(route(&store, &req("PUT", "/data")).status, 200);

    let mut put = Request::new("PUT", "/data/logs/a.txt", b"hello".to_vec());
    put.headers
        .insert("content-type".to_string(), "text/plain".to_string());
    put.headers
        .insert("x-amz-meta-source".to_string(), "unit-test".to_string());
    let put_resp = route(&store, &put);
    assert_eq!(put_resp.status, 200);
    assert_eq!(
        put_resp.headers.get("ETag").unwrap(),
        "\"5d41402abc4b2a76b9719d911017c592\""
    );

    let get = route(&store, &req("GET", "/data/logs/a.txt"));
    assert_eq!(get.status, 200);
    assert_eq!(get.body, b"hello");
    assert_eq!(get.headers.get("Content-Type").unwrap(), "text/plain");
    assert_eq!(get.headers.get("x-amz-meta-source").unwrap(), "unit-test");

    let mut range = req("GET", "/data/logs/a.txt");
    range
        .headers
        .insert("range".to_string(), "bytes=1-3".to_string());
    let range_resp = route(&store, &range);
    assert_eq!(range_resp.status, 206);
    assert_eq!(range_resp.body, b"ell");
    assert_eq!(
        range_resp.headers.get("Content-Range").unwrap(),
        "bytes 1-3/5"
    );

    let head = route(&store, &req("HEAD", "/data/logs/a.txt"));
    assert_eq!(head.status, 200);
    assert!(head.body.is_empty());
    assert_eq!(head.headers.get("Content-Length").unwrap(), "5");

    let delete = route(&store, &req("DELETE", "/data/logs/a.txt"));
    assert_eq!(delete.status, 204);
    let missing = route(&store, &req("GET", "/data/logs/a.txt"));
    assert_eq!(missing.status, 404);

    let empty_put = Request::new("PUT", "/data/empty.txt", Vec::new());
    assert_eq!(route(&store, &empty_put).status, 200);
    let empty_get = route(&store, &req("GET", "/data/empty.txt"));
    assert_eq!(empty_get.status, 200);
    assert_eq!(empty_get.headers.get("Content-Length").unwrap(), "0");
    assert!(empty_get.body.is_empty());
}

#[test]
fn list_objects_v1_and_v2() {
    let root = tempdir();
    let mut store = FileBucketStore::new(&root);
    store.set_fixed_now("2026-05-30T12:00:00Z");
    assert_eq!(route(&store, &req("PUT", "/data")).status, 200);
    for key in ["logs/2026/a.txt", "logs/2026/b.txt", "logs/2027/c.txt"] {
        let put = Request::new("PUT", &format!("/data/{key}"), b"x".to_vec());
        assert_eq!(route(&store, &put).status, 200);
    }

    let v1 = route(&store, &req("GET", "/data?prefix=logs/&delimiter=/"));
    assert_eq!(v1.status, 200);
    let body = String::from_utf8(v1.body).unwrap();
    assert!(body.contains("<CommonPrefixes><Prefix>logs/2026/</Prefix></CommonPrefixes>"));
    assert!(body.contains("<CommonPrefixes><Prefix>logs/2027/</Prefix></CommonPrefixes>"));

    let v2 = route(
        &store,
        &req("GET", "/data?list-type=2&prefix=logs/2026/&max-keys=1"),
    );
    assert_eq!(v2.status, 200);
    let body = String::from_utf8(v2.body).unwrap();
    assert!(body.contains("<ListType>2</ListType>"));
    assert!(body.contains("<IsTruncated>true</IsTruncated>"));
    assert!(body.contains("<NextContinuationToken>"));
}

#[test]
fn list_object_versions_paginates_versions_and_delete_markers() {
    let root = tempdir();
    let store = FileBucketStore::new(&root);
    store.push_version_ids(&[
        "11111111111111111111111111111111",
        "22222222222222222222222222222222",
        "33333333333333333333333333333333",
    ]);
    assert_eq!(route(&store, &req("PUT", "/demo-bucket")).status, 200);
    assert_eq!(
        route(
            &store,
            &Request::new(
                "PUT",
                "/demo-bucket?versioning",
                br#"<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>"#
                    .to_vec(),
            ),
        )
        .status,
        200
    );
    let first = route(
        &store,
        &Request::new("PUT", "/demo-bucket/docs/a.txt", b"one".to_vec()),
    );
    let first_version = first.headers.get("x-amz-version-id").unwrap().clone();
    let second = route(
        &store,
        &Request::new("PUT", "/demo-bucket/docs/a.txt", b"two".to_vec()),
    );
    let second_version = second.headers.get("x-amz-version-id").unwrap().clone();
    assert_eq!(
        route(&store, &req("DELETE", "/demo-bucket/docs/a.txt")).status,
        204
    );

    let listed = route(&store, &req("GET", "/demo-bucket?versions&prefix=docs/"));
    assert_eq!(listed.status, 200);
    let body = String::from_utf8(listed.body).unwrap();
    assert!(body.contains("<ListVersionsResult"));
    assert!(body.contains(&format!("<VersionId>{first_version}</VersionId>")));
    assert!(body.contains(&format!("<VersionId>{second_version}</VersionId>")));
    assert!(body.contains("<DeleteMarker>"));

    let page = route(
        &store,
        &req("GET", "/demo-bucket?versions&prefix=docs/&max-keys=1"),
    );
    assert_eq!(page.status, 200);
    let body = String::from_utf8(page.body).unwrap();
    assert!(body.contains("<IsTruncated>true</IsTruncated>"));
    assert!(body.contains("<NextKeyMarker>docs/a.txt</NextKeyMarker>"));
    assert!(body.contains("<NextVersionIdMarker>"));
}

#[test]
fn bucket_location_versioning_and_acl_subresources() {
    let root = tempdir();
    let store = FileBucketStore::new(&root);
    assert_eq!(route(&store, &req("PUT", "/data")).status, 200);

    let location = route(&store, &req("GET", "/data?location"));
    assert_eq!(location.status, 200);
    assert_eq!(
        String::from_utf8(location.body).unwrap(),
        r#"<LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/"></LocationConstraint>"#
    );

    let empty_versioning = route(&store, &req("GET", "/data?versioning"));
    assert_eq!(empty_versioning.status, 200);
    assert!(String::from_utf8(empty_versioning.body)
        .unwrap()
        .contains("<VersioningConfiguration"));

    let versioning = Request::new(
        "PUT",
        "/data?versioning",
        br#"<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>"#.to_vec(),
    );
    assert_eq!(route(&store, &versioning).status, 200);
    let get_versioning = route(&store, &req("GET", "/data?versioning"));
    assert_eq!(get_versioning.status, 200);
    assert!(String::from_utf8(get_versioning.body)
        .unwrap()
        .contains("<Status>Enabled</Status>"));

    let mut put_acl = req("PUT", "/data?acl");
    put_acl
        .headers
        .insert("x-amz-acl".to_string(), "public-read".to_string());
    assert_eq!(route(&store, &put_acl).status, 200);
    let get_acl = route(&store, &req("GET", "/data?acl"));
    assert_eq!(get_acl.status, 200);
    let acl_body = String::from_utf8(get_acl.body).unwrap();
    assert!(acl_body.contains("<CannedACL>public-read</CannedACL>"));
    assert!(acl_body.contains("<Permission>READ</Permission>"));

    assert_eq!(route(&store, &req("GET", "/missing?location")).status, 404);
    assert_eq!(route(&store, &req("GET", "/missing?acl")).status, 404);
}

#[test]
fn bucket_policy_subresource_persists_and_deletes_raw_json() {
    let root = tempdir();
    let store = FileBucketStore::new(&root);
    assert_eq!(route(&store, &req("PUT", "/data")).status, 200);

    let policy = br#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::data/*"}]}"#;
    let put = Request::new("PUT", "/data?policy", policy.to_vec());
    assert_eq!(route(&store, &put).status, 204);

    let get = route(&store, &req("GET", "/data?policy"));
    assert_eq!(get.status, 200);
    assert_eq!(get.headers.get("Content-Type").unwrap(), "application/json");
    assert_eq!(get.body, policy);

    assert_eq!(route(&store, &req("DELETE", "/data?policy")).status, 204);
    let missing = route(&store, &req("GET", "/data?policy"));
    assert_eq!(missing.status, 404);
    assert!(String::from_utf8(missing.body)
        .unwrap()
        .contains("<Code>NoSuchBucketPolicy</Code>"));

    let malformed = Request::new("PUT", "/data?policy", br#"{"Version":"#.to_vec());
    let malformed_resp = route(&store, &malformed);
    assert_eq!(malformed_resp.status, 400);
    assert!(String::from_utf8(malformed_resp.body)
        .unwrap()
        .contains("<Code>MalformedPolicy</Code>"));
}

#[test]
fn bucket_lifecycle_subresource_persists_deletes_and_rejects_transitions() {
    let root = tempdir();
    let store = FileBucketStore::new(&root);
    assert_eq!(route(&store, &req("PUT", "/data")).status, 200);

    let config = br#"<LifecycleConfiguration><Rule><ID>expire-logs</ID><Filter><Prefix>logs/</Prefix></Filter><Status>Enabled</Status><Expiration><Days>30</Days></Expiration></Rule></LifecycleConfiguration>"#;
    let put = Request::new("PUT", "/data?lifecycle", config.to_vec());
    assert_eq!(route(&store, &put).status, 200);

    let get = route(&store, &req("GET", "/data?lifecycle"));
    assert_eq!(get.status, 200);
    let body = String::from_utf8(get.body).unwrap();
    assert!(body.contains("<LifecycleConfiguration"));
    assert!(body.contains("<ID>expire-logs</ID>"));
    assert!(body.contains("<Filter><Prefix>logs/</Prefix></Filter>"));
    assert!(body.contains("<Days>30</Days>"));

    assert_eq!(route(&store, &req("DELETE", "/data?lifecycle")).status, 204);
    let missing = route(&store, &req("GET", "/data?lifecycle"));
    assert_eq!(missing.status, 404);
    assert!(String::from_utf8(missing.body)
        .unwrap()
        .contains("<Code>NoSuchLifecycleConfiguration</Code>"));

    let transition = br#"<LifecycleConfiguration><Rule><ID>transition</ID><Status>Enabled</Status><Expiration><Days>30</Days></Expiration><Transition><Days>1</Days><StorageClass>GLACIER</StorageClass></Transition></Rule></LifecycleConfiguration>"#;
    let unsupported = Request::new("PUT", "/data?lifecycle", transition.to_vec());
    let unsupported_resp = route(&store, &unsupported);
    assert_eq!(unsupported_resp.status, 501);
    assert!(String::from_utf8(unsupported_resp.body)
        .unwrap()
        .contains("<Code>NotImplemented</Code>"));
}

#[test]
fn object_lock_configuration_retention_and_legal_hold_subresources() {
    let root = tempdir();
    let mut store = FileBucketStore::new(&root);
    store.set_fixed_now("2026-05-30T12:00:00Z");
    assert_eq!(route(&store, &req("PUT", "/data")).status, 200);

    let config = br#"<ObjectLockConfiguration><ObjectLockEnabled>Enabled</ObjectLockEnabled><Rule><DefaultRetention><Mode>GOVERNANCE</Mode><Days>7</Days></DefaultRetention></Rule></ObjectLockConfiguration>"#;
    assert_eq!(
        route(
            &store,
            &Request::new("PUT", "/data?object-lock", config.to_vec())
        )
        .status,
        200
    );
    let get_config = route(&store, &req("GET", "/data?object-lock"));
    assert_eq!(get_config.status, 200);
    let body = String::from_utf8(get_config.body).unwrap();
    assert!(body.contains("<ObjectLockEnabled>Enabled</ObjectLockEnabled>"));
    assert!(body.contains("<Mode>GOVERNANCE</Mode>"));
    assert!(body.contains("<Days>7</Days>"));

    let default_put = Request::new("PUT", "/data/default.txt", b"default".to_vec());
    let default_put_resp = route(&store, &default_put);
    assert_eq!(default_put_resp.status, 200);
    assert_eq!(
        default_put_resp
            .headers
            .get("x-amz-object-lock-mode")
            .unwrap(),
        "GOVERNANCE"
    );
    assert_eq!(
        default_put_resp
            .headers
            .get("x-amz-object-lock-retain-until-date")
            .unwrap(),
        "2026-06-06T12:00:00Z"
    );

    let mut locked_put = Request::new("PUT", "/data/locked.txt", b"locked".to_vec());
    locked_put.headers.insert(
        "x-amz-object-lock-mode".to_string(),
        "COMPLIANCE".to_string(),
    );
    locked_put.headers.insert(
        "x-amz-object-lock-retain-until-date".to_string(),
        "2031-01-01T00:00:00Z".to_string(),
    );
    locked_put
        .headers
        .insert("x-amz-object-lock-legal-hold".to_string(), "ON".to_string());
    let locked_put_resp = route(&store, &locked_put);
    assert_eq!(locked_put_resp.status, 200);
    assert_eq!(
        locked_put_resp
            .headers
            .get("x-amz-object-lock-legal-hold")
            .unwrap(),
        "ON"
    );

    let retention = route(&store, &req("GET", "/data/locked.txt?retention"));
    assert_eq!(retention.status, 200);
    let body = String::from_utf8(retention.body).unwrap();
    assert!(body.contains("<Mode>COMPLIANCE</Mode>"));
    assert!(body.contains("<RetainUntilDate>2031-01-01T00:00:00Z</RetainUntilDate>"));

    let turn_off = Request::new(
        "PUT",
        "/data/locked.txt?legal-hold",
        br#"<LegalHold><Status>OFF</Status></LegalHold>"#.to_vec(),
    );
    assert_eq!(route(&store, &turn_off).status, 200);
    let legal_hold = route(&store, &req("GET", "/data/locked.txt?legal-hold"));
    assert_eq!(legal_hold.status, 200);
    assert!(String::from_utf8(legal_hold.body)
        .unwrap()
        .contains("<Status>OFF</Status>"));

    assert_eq!(
        route(&store, &req("DELETE", "/data?object-lock")).status,
        204
    );
    let missing_config = route(&store, &req("GET", "/data?object-lock"));
    assert_eq!(missing_config.status, 404);
    assert!(String::from_utf8(missing_config.body)
        .unwrap()
        .contains("<Code>ObjectLockConfigurationNotFoundError</Code>"));
}

#[test]
fn object_retention_blocks_delete_until_expired() {
    let root = tempdir();
    let mut store = FileBucketStore::new(&root);
    store.set_fixed_now("2026-05-30T12:00:00Z");
    assert_eq!(route(&store, &req("PUT", "/data")).status, 200);
    assert_eq!(
        route(
            &store,
            &Request::new("PUT", "/data/locked.txt", b"locked".to_vec())
        )
        .status,
        200
    );

    let retention = Request::new(
        "PUT",
        "/data/locked.txt?retention",
        br#"<Retention><Mode>GOVERNANCE</Mode><RetainUntilDate>2031-01-01T00:00:00Z</RetainUntilDate></Retention>"#.to_vec(),
    );
    assert_eq!(route(&store, &retention).status, 200);
    let delete_locked = route(&store, &req("DELETE", "/data/locked.txt"));
    assert_eq!(delete_locked.status, 403);

    let expired = Request::new(
        "PUT",
        "/data/locked.txt?retention",
        br#"<Retention><Mode>GOVERNANCE</Mode><RetainUntilDate>2000-01-01T00:00:00Z</RetainUntilDate></Retention>"#.to_vec(),
    );
    assert_eq!(route(&store, &expired).status, 200);
    assert_eq!(
        route(&store, &req("DELETE", "/data/locked.txt")).status,
        204
    );
}

#[test]
fn bucket_notification_subresource_persists_and_records_matching_events() {
    let root = tempdir();
    let mut store = FileBucketStore::new(&root);
    store.set_fixed_now("2026-05-30T12:00:00Z");
    store.push_version_ids(&[
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
    ]);
    assert_eq!(route(&store, &req("PUT", "/data")).status, 200);

    let config = br#"<NotificationConfiguration><TopicConfiguration><Topic>arn:aws:sns:us-east-1:000000000000:local</Topic><Event>s3:ObjectCreated:Put</Event><Event>s3:ObjectRemoved:*</Event><Filter><S3Key><FilterRule><Name>prefix</Name><Value>docs/</Value></FilterRule><FilterRule><Name>suffix</Name><Value>.txt</Value></FilterRule></S3Key></Filter></TopicConfiguration><QueueConfiguration><Id>docs-created</Id><Queue>arn:aws:sqs:us-east-1:000000000000:local</Queue><Event>s3:ObjectCreated:*</Event><Filter><S3Key><FilterRule><Name>prefix</Name><Value>docs/</Value></FilterRule><FilterRule><Name>suffix</Name><Value>.txt</Value></FilterRule></S3Key></Filter></QueueConfiguration></NotificationConfiguration>"#;
    assert_eq!(
        route(
            &store,
            &Request::new("PUT", "/data?notification", config.to_vec())
        )
        .status,
        200
    );

    let get = route(&store, &req("GET", "/data?notification"));
    assert_eq!(get.status, 200);
    let body = String::from_utf8(get.body).unwrap();
    assert!(body.contains("<Topic>arn:aws:sns:us-east-1:000000000000:local</Topic>"));
    assert!(body.contains("<Id>docs-created</Id>"));
    assert!(body.contains("<Name>prefix</Name><Value>docs/</Value>"));
    assert!(body.contains("<Name>suffix</Name><Value>.txt</Value>"));

    assert_eq!(
        route(
            &store,
            &Request::new("PUT", "/data/docs/readme.txt", b"body".to_vec())
        )
        .status,
        200
    );
    assert_eq!(
        route(
            &store,
            &Request::new("PUT", "/data/docs/readme.bin", b"body".to_vec())
        )
        .status,
        200
    );
    assert_eq!(
        route(&store, &req("DELETE", "/data/docs/readme.txt")).status,
        204
    );

    let events = store.list_notification_events("data").unwrap().unwrap();
    assert_eq!(events.len(), 2);
    assert_eq!(events[0].event_id, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
    assert_eq!(events[0].event_name, "s3:ObjectCreated:Put");
    assert_eq!(events[0].event_time, "2026-05-30T12:00:00Z");
    assert_eq!(events[0].key, "docs/readme.txt");
    assert_eq!(events[1].event_id, "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");
    assert_eq!(events[1].event_name, "s3:ObjectRemoved:Delete");
}

#[test]
fn bucket_notification_event_bridge_and_validation() {
    let root = tempdir();
    let store = FileBucketStore::new(&root);
    assert_eq!(route(&store, &req("PUT", "/data")).status, 200);

    let event_bridge = Request::new(
        "PUT",
        "/data?notification",
        br#"<NotificationConfiguration><EventBridgeConfiguration /></NotificationConfiguration>"#
            .to_vec(),
    );
    assert_eq!(route(&store, &event_bridge).status, 200);
    let get = route(&store, &req("GET", "/data?notification"));
    assert_eq!(get.status, 200);
    assert!(String::from_utf8(get.body)
        .unwrap()
        .contains("<EventBridgeConfiguration>"));

    let unsupported = Request::new(
        "PUT",
        "/data?notification",
        br#"<NotificationConfiguration><QueueConfiguration><Queue>arn:aws:sqs:us-east-1:000000000000:local</Queue><Event>s3:ReducedRedundancyLostObject</Event></QueueConfiguration></NotificationConfiguration>"#.to_vec(),
    );
    let resp = route(&store, &unsupported);
    assert_eq!(resp.status, 400);
    assert!(String::from_utf8(resp.body)
        .unwrap()
        .contains("<Code>InvalidArgument</Code>"));
}

#[test]
fn bucket_inventory_subresource_crud_list_and_report_generation() {
    let root = tempdir();
    let store = FileBucketStore::new(&root);
    store.push_version_ids(&[
        "11111111111111111111111111111111",
        "22222222222222222222222222222222",
        "33333333333333333333333333333333",
    ]);
    assert_eq!(route(&store, &req("PUT", "/demo-bucket")).status, 200);
    assert_eq!(
        route(
            &store,
            &Request::new(
                "PUT",
                "/demo-bucket?versioning",
                br#"<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>"#
                    .to_vec(),
            ),
        )
        .status,
        200
    );
    assert_eq!(
        route(
            &store,
            &Request::new("PUT", "/demo-bucket/docs/readme.txt", b"first".to_vec()),
        )
        .status,
        200
    );
    assert_eq!(
        route(
            &store,
            &Request::new("PUT", "/demo-bucket/docs/readme.txt", b"second".to_vec()),
        )
        .status,
        200
    );
    let mut sse = Request::new("PUT", "/demo-bucket/logs/audit.txt", b"audit".to_vec());
    sse.headers.insert(
        "x-amz-server-side-encryption".to_string(),
        "AES256".to_string(),
    );
    assert_eq!(route(&store, &sse).status, 200);

    let config = br#"<InventoryConfiguration><Id>all-versions</Id><IsEnabled>true</IsEnabled><Destination><S3BucketDestination><Bucket>arn:aws:s3:::reports-bucket</Bucket><Format>CSV</Format><Prefix>inventory/</Prefix></S3BucketDestination></Destination><Schedule><Frequency>Daily</Frequency></Schedule><IncludedObjectVersions>All</IncludedObjectVersions><OptionalFields><Field>EncryptionStatus</Field></OptionalFields></InventoryConfiguration>"#;
    assert_eq!(
        route(
            &store,
            &Request::new(
                "PUT",
                "/demo-bucket?inventory&id=all-versions",
                config.to_vec(),
            ),
        )
        .status,
        200
    );

    let get = route(
        &store,
        &req("GET", "/demo-bucket?inventory&id=all-versions"),
    );
    assert_eq!(get.status, 200);
    let body = String::from_utf8(get.body).unwrap();
    assert!(body.contains("<Id>all-versions</Id>"));
    assert!(body.contains("<IsEnabled>true</IsEnabled>"));
    assert!(body.contains("<Frequency>Daily</Frequency>"));
    assert!(body.contains("<Field>EncryptionStatus</Field>"));

    let list = route(&store, &req("GET", "/demo-bucket?inventory"));
    assert_eq!(list.status, 200);
    let body = String::from_utf8(list.body).unwrap();
    assert!(body.contains("<ListInventoryConfigurationsResult"));
    assert!(body.contains("<IsTruncated>false</IsTruncated>"));
    assert!(body.contains("<InventoryConfiguration><Id>all-versions</Id>"));

    let manifest_data = std::fs::read_to_string(
        store.inventory_report_manifest_path("demo-bucket", "all-versions"),
    )
    .unwrap();
    let manifest: serde_json::Value = serde_json::from_str(&manifest_data).unwrap();
    assert_eq!(manifest["configurationId"], "all-versions");
    assert_eq!(manifest["sourceBucket"], "demo-bucket");
    assert_eq!(manifest["includedObjectVersions"], "All");
    assert_eq!(manifest["objectCount"], 3);
    let fields = manifest["fields"].as_array().unwrap();
    assert!(fields.iter().any(|v| v == "VersionId"));
    assert!(fields.iter().any(|v| v == "IsLatest"));
    assert!(fields.iter().any(|v| v == "EncryptionStatus"));

    let report =
        std::fs::read_to_string(store.inventory_report_csv_path("demo-bucket", "all-versions"))
            .unwrap();
    let lines: Vec<&str> = report.trim_end().split('\n').collect();
    assert_eq!(lines.len(), 4);
    assert!(lines[0].contains("VersionId"));
    assert!(lines[0].contains("IsLatest"));
    assert!(lines[0].contains("EncryptionStatus"));
    assert!(report.contains("logs/audit.txt"));
    assert!(report.contains("AES256"));

    assert_eq!(
        route(
            &store,
            &Request::new(
                "PUT",
                "/demo-bucket?inventory&id=all-versions",
                br#"<InventoryConfiguration><Id>all-versions</Id><IsEnabled>false</IsEnabled></InventoryConfiguration>"#.to_vec(),
            ),
        )
        .status,
        200
    );
    assert!(!store
        .inventory_report_csv_path("demo-bucket", "all-versions")
        .exists());

    assert_eq!(
        route(
            &store,
            &req("DELETE", "/demo-bucket?inventory&id=all-versions")
        )
        .status,
        204
    );
    let missing = route(
        &store,
        &req("GET", "/demo-bucket?inventory&id=all-versions"),
    );
    assert_eq!(missing.status, 404);
    assert!(String::from_utf8(missing.body)
        .unwrap()
        .contains("<Code>NoSuchConfiguration</Code>"));
}

#[test]
fn bucket_analytics_subresource_crud_list_and_validation() {
    let root = tempdir();
    let store = FileBucketStore::new(&root);
    assert_eq!(route(&store, &req("PUT", "/demo-bucket")).status, 200);

    let config = br#"<AnalyticsConfiguration><Id>storage-class</Id><Filter><Prefix>logs/</Prefix></Filter><StorageClassAnalysis><DataExport><OutputSchemaVersion>V_1</OutputSchemaVersion><Destination><S3BucketDestination><Format>CSV</Format><Bucket>arn:aws:s3:::reports-bucket</Bucket><Prefix>analytics/</Prefix></S3BucketDestination></Destination></DataExport></StorageClassAnalysis></AnalyticsConfiguration>"#;
    assert_eq!(
        route(
            &store,
            &Request::new(
                "PUT",
                "/demo-bucket?analytics&id=storage-class",
                config.to_vec(),
            ),
        )
        .status,
        200
    );

    let get = route(
        &store,
        &req("GET", "/demo-bucket?analytics&id=storage-class"),
    );
    assert_eq!(get.status, 200);
    let body = String::from_utf8(get.body).unwrap();
    assert!(body.contains("<Id>storage-class</Id>"));
    assert!(body.contains("<Filter><Prefix>logs/</Prefix></Filter>"));
    assert!(body.contains("<OutputSchemaVersion>V_1</OutputSchemaVersion>"));
    assert!(body.contains("<Format>CSV</Format>"));

    let list = route(&store, &req("GET", "/demo-bucket?analytics"));
    assert_eq!(list.status, 200);
    let body = String::from_utf8(list.body).unwrap();
    assert!(body.contains("<ListAnalyticsConfigurationsResult"));
    assert!(body.contains("<IsTruncated>false</IsTruncated>"));
    assert!(body.contains("<AnalyticsConfiguration><Id>storage-class</Id>"));

    let invalid_id = route(
        &store,
        &Request::new(
            "PUT",
            "/demo-bucket?analytics&id=query-id",
            br#"<AnalyticsConfiguration><Id>body-id</Id></AnalyticsConfiguration>"#.to_vec(),
        ),
    );
    assert_eq!(invalid_id.status, 400);
    assert!(String::from_utf8(invalid_id.body)
        .unwrap()
        .contains("<Code>InvalidArgument</Code>"));

    let invalid_format = route(
        &store,
        &Request::new(
            "PUT",
            "/demo-bucket?analytics&id=bad-format",
            br#"<AnalyticsConfiguration><StorageClassAnalysis><DataExport><Destination><S3BucketDestination><Format>ORC</Format></S3BucketDestination></Destination></DataExport></StorageClassAnalysis></AnalyticsConfiguration>"#.to_vec(),
        ),
    );
    assert_eq!(invalid_format.status, 400);

    let missing_inventory_id = route(
        &store,
        &Request::new(
            "PUT",
            "/demo-bucket?inventory",
            br#"<InventoryConfiguration><Id>daily</Id></InventoryConfiguration>"#.to_vec(),
        ),
    );
    assert_eq!(missing_inventory_id.status, 400);

    assert_eq!(
        route(
            &store,
            &req("DELETE", "/demo-bucket?analytics&id=storage-class")
        )
        .status,
        204
    );
    assert_eq!(
        route(
            &store,
            &req("GET", "/demo-bucket?analytics&id=storage-class")
        )
        .status,
        404
    );
}

#[test]
fn bucket_replication_subresource_replicates_matching_writes() {
    let root = tempdir();
    let store = FileBucketStore::new(&root);
    assert_eq!(route(&store, &req("PUT", "/source-bucket")).status, 200);
    assert_eq!(route(&store, &req("PUT", "/replica-bucket")).status, 200);

    let config = br#"<ReplicationConfiguration><Role>arn:aws:iam::000000000000:role/devcloud</Role><Rule><ID>docs-only</ID><Status>Enabled</Status><Filter><Prefix>docs/</Prefix></Filter><Destination><Bucket>arn:aws:s3:::replica-bucket</Bucket><StorageClass>STANDARD</StorageClass></Destination></Rule></ReplicationConfiguration>"#;
    assert_eq!(
        route(
            &store,
            &Request::new("PUT", "/source-bucket?replication", config.to_vec()),
        )
        .status,
        200
    );

    let get = route(&store, &req("GET", "/source-bucket?replication"));
    assert_eq!(get.status, 200);
    let body = String::from_utf8(get.body).unwrap();
    assert!(body.contains("<Role>arn:aws:iam::000000000000:role/devcloud</Role>"));
    assert!(body.contains("<ID>docs-only</ID>"));
    assert!(body.contains("<Filter><Prefix>docs/</Prefix></Filter>"));
    assert!(body.contains("<Bucket>arn:aws:s3:::replica-bucket</Bucket>"));

    assert_eq!(
        route(
            &store,
            &Request::new(
                "PUT",
                "/source-bucket/docs/readme.txt",
                b"replicated body".to_vec(),
            ),
        )
        .status,
        200
    );
    let replica = route(&store, &req("GET", "/replica-bucket/docs/readme.txt"));
    assert_eq!(replica.status, 200);
    assert_eq!(replica.body, b"replicated body");

    let mut copy = req("PUT", "/source-bucket/docs/copy.txt");
    copy.headers.insert(
        "x-amz-copy-source".to_string(),
        "/source-bucket/docs/readme.txt".to_string(),
    );
    let copied = route(&store, &copy);
    assert_eq!(copied.status, 200);
    assert!(String::from_utf8(copied.body)
        .unwrap()
        .contains("<CopyObjectResult>"));
    let replica_copy = route(&store, &req("GET", "/replica-bucket/docs/copy.txt"));
    assert_eq!(replica_copy.status, 200);
    assert_eq!(replica_copy.body, b"replicated body");

    assert_eq!(
        route(
            &store,
            &Request::new(
                "PUT",
                "/source-bucket/logs/readme.txt",
                b"ignored body".to_vec(),
            ),
        )
        .status,
        200
    );
    assert_eq!(
        route(&store, &req("GET", "/replica-bucket/logs/readme.txt")).status,
        404
    );

    assert_eq!(
        route(&store, &req("DELETE", "/source-bucket?replication")).status,
        204
    );
    let missing = route(&store, &req("GET", "/source-bucket?replication"));
    assert_eq!(missing.status, 404);
    assert!(String::from_utf8(missing.body)
        .unwrap()
        .contains("<Code>ReplicationConfigurationNotFoundError</Code>"));
}

#[test]
fn copy_object_uses_escaped_source_query_version_and_replace_metadata() {
    let root = tempdir();
    let store = FileBucketStore::new(&root);
    store.push_version_ids(&[
        "11111111111111111111111111111111",
        "22222222222222222222222222222222",
        "33333333333333333333333333333333",
    ]);
    assert_eq!(route(&store, &req("PUT", "/demo-bucket")).status, 200);
    assert_eq!(
        route(
            &store,
            &Request::new(
                "PUT",
                "/demo-bucket?versioning",
                br#"<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>"#
                    .to_vec(),
            ),
        )
        .status,
        200
    );

    let mut first = Request::new(
        "PUT",
        "/demo-bucket/docs/source%20file.txt",
        b"first".to_vec(),
    );
    first
        .headers
        .insert("content-type".to_string(), "text/plain".to_string());
    first
        .headers
        .insert("x-amz-meta-origin".to_string(), "source".to_string());
    let first_resp = route(&store, &first);
    assert_eq!(first_resp.status, 200);
    let first_version = first_resp.headers.get("x-amz-version-id").unwrap().clone();
    assert_eq!(
        route(
            &store,
            &Request::new(
                "PUT",
                "/demo-bucket/docs/source%20file.txt",
                b"second".to_vec(),
            ),
        )
        .status,
        200
    );

    let mut copy = req("PUT", "/demo-bucket/docs/copied.txt");
    copy.headers.insert(
        "x-amz-copy-source".to_string(),
        format!("/demo-bucket/docs/source%20file.txt?versionId={first_version}"),
    );
    copy.headers.insert(
        "x-amz-metadata-directive".to_string(),
        "REPLACE".to_string(),
    );
    copy.headers
        .insert("content-type".to_string(), "application/json".to_string());
    copy.headers
        .insert("x-amz-meta-origin".to_string(), "copy".to_string());
    copy.headers.insert(
        "x-amz-server-side-encryption".to_string(),
        "AES256".to_string(),
    );
    let copied = route(&store, &copy);
    assert_eq!(copied.status, 200);
    assert_eq!(
        copied.headers.get("x-amz-server-side-encryption").unwrap(),
        "AES256"
    );
    assert!(String::from_utf8(copied.body)
        .unwrap()
        .contains("<CopyObjectResult>"));

    let get = route(&store, &req("GET", "/demo-bucket/docs/copied.txt"));
    assert_eq!(get.status, 200);
    assert_eq!(get.body, b"first");
    assert_eq!(get.headers.get("Content-Type").unwrap(), "application/json");
    assert_eq!(get.headers.get("x-amz-meta-origin").unwrap(), "copy");
    assert_eq!(
        get.headers.get("x-amz-server-side-encryption").unwrap(),
        "AES256"
    );

    let latest = route(&store, &req("GET", "/demo-bucket/docs/source%20file.txt"));
    assert_eq!(latest.status, 200);
    assert_eq!(latest.body, b"second");
}

#[test]
fn select_object_content_supports_csv_json_and_rejects_unsupported_sql() {
    let root = tempdir();
    let store = FileBucketStore::new(&root);
    assert_eq!(route(&store, &req("PUT", "/demo-bucket")).status, 200);
    assert_eq!(
        route(
            &store,
            &Request::new(
                "PUT",
                "/demo-bucket/reports/users.csv",
                b"name,age\nalice,31\nbob,28\n".to_vec(),
            ),
        )
        .status,
        200
    );

    let csv_request = br#"<SelectObjectContentRequest><Expression>SELECT * FROM S3Object</Expression><ExpressionType>SQL</ExpressionType><InputSerialization><CSV><FileHeaderInfo>USE</FileHeaderInfo></CSV></InputSerialization><OutputSerialization><CSV /></OutputSerialization></SelectObjectContentRequest>"#;
    let csv_select = route(
        &store,
        &Request::new(
            "POST",
            "/demo-bucket/reports/users.csv?select&select-type=2",
            csv_request.to_vec(),
        ),
    );
    assert_eq!(csv_select.status, 200);
    assert_eq!(
        csv_select.headers.get("Content-Type").unwrap(),
        "application/vnd.amazon.eventstream"
    );
    assert_eq!(
        String::from_utf8(event_stream_records(&csv_select.body)).unwrap(),
        "alice,31\nbob,28\n"
    );

    assert_eq!(
        route(
            &store,
            &Request::new(
                "PUT",
                "/demo-bucket/reports/users.jsonl",
                (r#"{"name":"alice","age":31}"#.to_string()
                    + "\n"
                    + r#"{"name":"bob","age":28}"#
                    + "\n")
                    .into_bytes(),
            ),
        )
        .status,
        200
    );
    let json_request = br#"<SelectObjectContentRequest><Expression>SELECT * FROM S3Object s</Expression><ExpressionType>SQL</ExpressionType><InputSerialization><JSON><Type>LINES</Type></JSON></InputSerialization><OutputSerialization><JSON /></OutputSerialization></SelectObjectContentRequest>"#;
    let json_select = route(
        &store,
        &Request::new(
            "POST",
            "/demo-bucket/reports/users.jsonl?select&select-type=2",
            json_request.to_vec(),
        ),
    );
    assert_eq!(json_select.status, 200);
    assert_eq!(
        String::from_utf8(event_stream_records(&json_select.body)).unwrap(),
        "{\"age\":31,\"name\":\"alice\"}\n{\"age\":28,\"name\":\"bob\"}\n"
    );

    let unsupported = route(
        &store,
        &Request::new(
            "POST",
            "/demo-bucket/reports/users.csv?select&select-type=2",
            br#"<SelectObjectContentRequest><Expression>SELECT name FROM S3Object WHERE age &gt; 30</Expression><ExpressionType>SQL</ExpressionType><InputSerialization><CSV /></InputSerialization><OutputSerialization><CSV /></OutputSerialization></SelectObjectContentRequest>"#.to_vec(),
        ),
    );
    assert_eq!(unsupported.status, 501);
    assert!(String::from_utf8(unsupported.body)
        .unwrap()
        .contains("<Code>NotImplemented</Code>"));
}

#[test]
fn bucket_replication_validation_and_delete_marker_replication() {
    let root = tempdir();
    let store = FileBucketStore::new(&root);
    store.push_version_ids(&[
        "11111111111111111111111111111111",
        "22222222222222222222222222222222",
        "33333333333333333333333333333333",
        "44444444444444444444444444444444",
    ]);
    for bucket in ["source-bucket", "replica-bucket"] {
        assert_eq!(
            route(&store, &req("PUT", &format!("/{bucket}"))).status,
            200
        );
        assert_eq!(
            route(
                &store,
                &Request::new(
                    "PUT",
                    &format!("/{bucket}?versioning"),
                    br#"<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>"#
                        .to_vec(),
                ),
            )
            .status,
            200
        );
    }

    let invalid = route(
        &store,
        &Request::new(
            "PUT",
            "/source-bucket?replication",
            br#"<ReplicationConfiguration><Rule><Status>Enabled</Status><Destination><Bucket>arn:aws:s3:::Invalid_Bucket</Bucket></Destination></Rule></ReplicationConfiguration>"#.to_vec(),
        ),
    );
    assert_eq!(invalid.status, 400);
    assert!(String::from_utf8(invalid.body)
        .unwrap()
        .contains("<Code>InvalidArgument</Code>"));

    let config = br#"<ReplicationConfiguration><Role>arn:aws:iam::000000000000:role/devcloud</Role><Rule><ID>docs-delete-markers</ID><Status>Enabled</Status><Filter><Prefix>docs/</Prefix></Filter><DeleteMarkerReplication><Status>Enabled</Status></DeleteMarkerReplication><Destination><Bucket>arn:aws:s3:::replica-bucket</Bucket></Destination></Rule></ReplicationConfiguration>"#;
    assert_eq!(
        route(
            &store,
            &Request::new("PUT", "/source-bucket?replication", config.to_vec()),
        )
        .status,
        200
    );

    assert_eq!(
        route(
            &store,
            &Request::new(
                "PUT",
                "/source-bucket/docs/readme.txt",
                b"replicated body".to_vec(),
            ),
        )
        .status,
        200
    );
    let replica = route(&store, &req("GET", "/replica-bucket/docs/readme.txt"));
    assert_eq!(replica.status, 200);
    let replica_version = replica.headers.get("x-amz-version-id").unwrap().clone();

    let delete = route(&store, &req("DELETE", "/source-bucket/docs/readme.txt"));
    assert_eq!(delete.status, 204);
    assert_eq!(delete.headers.get("x-amz-delete-marker").unwrap(), "true");
    assert_eq!(
        route(&store, &req("GET", "/replica-bucket/docs/readme.txt")).status,
        404
    );
    let original = route(
        &store,
        &req(
            "GET",
            &format!("/replica-bucket/docs/readme.txt?versionId={replica_version}"),
        ),
    );
    assert_eq!(original.status, 200);
    assert_eq!(original.body, b"replicated body");
}

#[test]
fn object_acl_subresource_supports_latest_and_version_id() {
    let root = tempdir();
    let store = FileBucketStore::new(&root);
    store.create_bucket("data").unwrap();
    store.push_version_ids(&[
        "11111111111111111111111111111111",
        "22222222222222222222222222222222",
    ]);

    let versioning = Request::new(
        "PUT",
        "/data?versioning",
        br#"<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>"#.to_vec(),
    );
    assert_eq!(route(&store, &versioning).status, 200);

    let first = route(
        &store,
        &Request::new("PUT", "/data/docs/versioned.txt", b"first".to_vec()),
    );
    assert_eq!(first.status, 200);
    let first_version = first.headers.get("x-amz-version-id").unwrap().clone();
    let second = route(
        &store,
        &Request::new("PUT", "/data/docs/versioned.txt", b"second".to_vec()),
    );
    assert_eq!(second.status, 200);

    let mut put_first_acl = req(
        "PUT",
        &format!("/data/docs/versioned.txt?acl&versionId={first_version}"),
    );
    put_first_acl
        .headers
        .insert("x-amz-acl".to_string(), "public-read".to_string());
    assert_eq!(route(&store, &put_first_acl).status, 200);

    let first_acl = route(
        &store,
        &req(
            "GET",
            &format!("/data/docs/versioned.txt?acl&versionId={first_version}"),
        ),
    );
    assert_eq!(first_acl.status, 200);
    assert!(String::from_utf8(first_acl.body)
        .unwrap()
        .contains("<CannedACL>public-read</CannedACL>"));

    let latest_acl = route(&store, &req("GET", "/data/docs/versioned.txt?acl"));
    assert_eq!(latest_acl.status, 200);
    assert!(String::from_utf8(latest_acl.body)
        .unwrap()
        .contains("<CannedACL>private</CannedACL>"));

    let mut put_latest_acl = req("PUT", "/data/docs/versioned.txt?acl");
    put_latest_acl.headers.insert(
        "x-amz-acl".to_string(),
        "bucket-owner-full-control".to_string(),
    );
    assert_eq!(route(&store, &put_latest_acl).status, 200);
    let latest_acl = route(&store, &req("GET", "/data/docs/versioned.txt?acl"));
    assert!(String::from_utf8(latest_acl.body)
        .unwrap()
        .contains("<CannedACL>bucket-owner-full-control</CannedACL>"));

    assert_eq!(
        route(&store, &req("GET", "/data/missing.txt?acl")).status,
        404
    );
}

#[test]
fn multipart_upload_flow() {
    let root = tempdir();
    let store = FileBucketStore::new(&root);
    store.create_bucket("data").unwrap();
    store.push_version_ids(&["0123456789abcdef0123456789abcdef"]);

    let mut initiate = req("POST", "/data/big.bin?uploads");
    initiate.headers.insert(
        "content-type".to_string(),
        "application/octet-stream".to_string(),
    );
    let initiated = route(&store, &initiate);
    assert_eq!(initiated.status, 200);
    let body = String::from_utf8(initiated.body).unwrap();
    assert!(body.contains("<UploadId>0123456789abcdef0123456789abcdef</UploadId>"));

    let upload_id = "0123456789abcdef0123456789abcdef";
    let part2 = Request::new(
        "PUT",
        &format!("/data/big.bin?uploadId={upload_id}&partNumber=2"),
        b"WORLD".to_vec(),
    );
    let part2_resp = route(&store, &part2);
    assert_eq!(part2_resp.status, 200);
    assert_eq!(
        part2_resp.headers.get("ETag").unwrap(),
        "\"5289492cf082446ca4a6eec9f72f1ec3\""
    );

    let part1 = Request::new(
        "PUT",
        &format!("/data/big.bin?uploadId={upload_id}&partNumber=1"),
        b"hello".to_vec(),
    );
    assert_eq!(route(&store, &part1).status, 200);

    let listed = route(
        &store,
        &req(
            "GET",
            &format!("/data/big.bin?uploadId={upload_id}&max-parts=1"),
        ),
    );
    assert_eq!(listed.status, 200);
    let body = String::from_utf8(listed.body).unwrap();
    assert!(body.contains("<PartNumber>1</PartNumber>"));
    assert!(body.contains("<IsTruncated>true</IsTruncated>"));
    assert!(body.contains("<NextPartNumberMarker>1</NextPartNumberMarker>"));

    let uploads = route(&store, &req("GET", "/data?uploads"));
    assert_eq!(uploads.status, 200);
    let body = String::from_utf8(uploads.body).unwrap();
    assert!(body.contains("<Key>big.bin</Key>"));
    assert!(body.contains("<UploadId>0123456789abcdef0123456789abcdef</UploadId>"));

    let complete_body = br#"<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>"x"</ETag></Part><Part><PartNumber>2</PartNumber><ETag>"y"</ETag></Part></CompleteMultipartUpload>"#;
    let complete = Request::new(
        "POST",
        &format!("/data/big.bin?uploadId={upload_id}"),
        complete_body.to_vec(),
    );
    let completed = route(&store, &complete);
    assert_eq!(completed.status, 200);
    assert_eq!(
        completed.headers.get("ETag").unwrap(),
        "\"0dc92aaf05380431066db92c24d4870c-2\""
    );

    let get = route(&store, &req("GET", "/data/big.bin"));
    assert_eq!(get.status, 200);
    assert_eq!(get.body, b"helloWORLD");
}

#[test]
fn multipart_abort_removes_upload() {
    let root = tempdir();
    let store = FileBucketStore::new(&root);
    store.create_bucket("data").unwrap();
    store.push_version_ids(&["aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"]);
    assert_eq!(
        route(&store, &req("POST", "/data/tmp.bin?uploads")).status,
        200
    );

    let aborted = route(
        &store,
        &req(
            "DELETE",
            "/data/tmp.bin?uploadId=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        ),
    );
    assert_eq!(aborted.status, 204);
    let missing = route(
        &store,
        &req(
            "GET",
            "/data/tmp.bin?uploadId=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        ),
    );
    assert_eq!(missing.status, 404);
}
