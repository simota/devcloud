//! GCS Pub/Sub notifications (`notificationConfigs` CRUD, event recording,
//! loopback Pub/Sub REST forwarding) and Signed URLs (V4 HMAC, V4 RSA, V2).

use std::collections::BTreeMap;
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use devcloud_gcs::http::{
    route, Config, Request, Response, Server, DEFAULT_HMAC_ACCESS_ID, DEFAULT_HMAC_SECRET,
};
use devcloud_gcs::notifications::build_pubsub_publish_request;
use devcloud_gcs::signed_urls::{
    sign_v2_target, sign_v4_hmac_target, sign_v4_rsa_target, SignV2Input, SignV4HmacInput,
    SignV4RsaInput,
};
use devcloud_s3::base64;
use devcloud_s3::store::FileBucketStore;

#[path = "support/test_rsa_keys.rs"]
mod test_rsa_keys;
use test_rsa_keys::{
    test_service_account_json, OTHER_PRIVATE_KEY_PEM, TEST_SA_EMAIL, TEST_SA_PRIVATE_KEY_PEM,
    TEST_SA_PUBLIC_KEY_PEM,
};

static NEXT_ID: AtomicU64 = AtomicU64::new(0);

struct Fixture {
    root: PathBuf,
    server: Server,
}

impl Fixture {
    fn new(mut config: Config, now: &str) -> Self {
        let root = std::env::temp_dir().join(format!(
            "devcloud-gcs-notif-sig-{}-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
            NEXT_ID.fetch_add(1, Ordering::Relaxed),
        ));
        if config.upload_session_path.is_empty() {
            config.upload_session_path = root.join("sessions").to_string_lossy().into_owned();
        }
        let mut store = FileBucketStore::new(root.join("buckets"));
        store.set_fixed_now(now);
        let server = Server::new(config, store);
        Self { root, server }
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.root);
    }
}

fn call(
    server: &mut Server,
    method: &str,
    target: &str,
    body: &[u8],
    headers: &[(&str, &str)],
) -> Response {
    let mut req = Request::new(method, target, body.to_vec());
    for (k, v) in headers {
        req.headers.insert(k.to_ascii_lowercase(), (*v).to_string());
    }
    route(server, &req)
}

fn json(r: &Response) -> serde_json::Value {
    serde_json::from_slice(&r.body).unwrap()
}

#[test]
fn notification_configs_crud_filtering_and_event_recording() {
    let mut f = Fixture::new(Config::default(), "2026-06-01T12:00:00Z");

    assert_eq!(
        call(
            &mut f.server,
            "POST",
            "/storage/v1/b",
            br#"{"name":"events-bucket"}"#,
            &[("Content-Type", "application/json")],
        )
        .status,
        200
    );

    // Invalid topic and payload_format rejected.
    assert_eq!(
        call(
            &mut f.server,
            "POST",
            "/storage/v1/b/events-bucket/notificationConfigs",
            br#"{"topic":"bad-topic","payload_format":"JSON_API_V1"}"#,
            &[],
        )
        .status,
        400
    );
    assert_eq!(
        call(
            &mut f.server,
            "POST",
            "/storage/v1/b/events-bucket/notificationConfigs",
            br#"{"topic":"projects/devcloud/topics/uploads","payload_format":"XML"}"#,
            &[],
        )
        .status,
        400
    );

    // Create two notification configs: prefix-filtered finalize/delete (JSON_API_V1)
    // and metadata-update only (NONE).
    let cfg1 = call(
        &mut f.server,
        "POST",
        "/storage/v1/b/events-bucket/notificationConfigs",
        br#"{"topic":"projects/devcloud/topics/uploads","payload_format":"JSON_API_V1","event_types":["OBJECT_FINALIZE","OBJECT_DELETE"],"object_name_prefix":"incoming/","custom_attributes":{"env":"local"}}"#,
        &[],
    );
    assert_eq!(cfg1.status, 200);
    let c1 = json(&cfg1);
    assert_eq!(c1["kind"], "storage#notification");
    assert_eq!(c1["id"], "1");
    assert_eq!(
        c1["topic"],
        "//pubsub.googleapis.com/projects/devcloud/topics/uploads"
    );

    let cfg2 = call(
        &mut f.server,
        "POST",
        "/storage/v1/b/events-bucket/notificationConfigs",
        br#"{"topic":"//pubsub.googleapis.com/projects/devcloud/topics/meta","payload_format":"NONE","event_types":["OBJECT_METADATA_UPDATE"]}"#,
        &[],
    );
    assert_eq!(cfg2.status, 200);
    assert_eq!(json(&cfg2)["id"], "2");

    // List and get by ID.
    let list = call(
        &mut f.server,
        "GET",
        "/storage/v1/b/events-bucket/notificationConfigs",
        b"",
        &[],
    );
    assert_eq!(list.status, 200);
    assert_eq!(json(&list)["items"].as_array().unwrap().len(), 2);

    let get1 = call(
        &mut f.server,
        "GET",
        "/storage/v1/b/events-bucket/notificationConfigs/1",
        b"",
        &[],
    );
    assert_eq!(get1.status, 200);
    assert_eq!(json(&get1)["object_name_prefix"], "incoming/");

    // Upload outside prefix -> no finalize event for cfg1.
    assert_eq!(
        call(
            &mut f.server,
            "POST",
            "/upload/storage/v1/b/events-bucket/o?uploadType=media&name=other/ignore.txt",
            b"ignored",
            &[("Content-Type", "text/plain")],
        )
        .status,
        200
    );
    assert!(f
        .server
        .list_notification_events("events-bucket")
        .is_empty());

    // Upload inside prefix -> triggers cfg1 OBJECT_FINALIZE with JSON_API_V1 payload.
    assert_eq!(
        call(
            &mut f.server,
            "PUT",
            "/events-bucket/incoming/order.json",
            br#"{"id":1}"#,
            &[("Content-Type", "application/json")],
        )
        .status,
        200
    );
    // Patch metadata -> triggers cfg2 OBJECT_METADATA_UPDATE with payload=None.
    assert_eq!(
        call(
            &mut f.server,
            "PATCH",
            "/storage/v1/b/events-bucket/o/incoming%2Forder.json",
            br#"{"cacheControl":"no-store"}"#,
            &[("Content-Type", "application/json")],
        )
        .status,
        200
    );
    // Delete -> triggers cfg1 OBJECT_DELETE.
    assert_eq!(
        call(
            &mut f.server,
            "DELETE",
            "/storage/v1/b/events-bucket/o/incoming%2Forder.json",
            b"",
            &[],
        )
        .status,
        204
    );

    let events = f.server.list_notification_events("events-bucket");
    assert_eq!(events.len(), 3);
    assert_eq!(events[0].event_type, "OBJECT_FINALIZE");
    assert_eq!(events[0].object_id, "incoming/order.json");
    assert_eq!(
        events[0].attributes.get("env").map(String::as_str),
        Some("local")
    );
    assert!(events[0].payload.is_some());

    assert_eq!(events[1].event_type, "OBJECT_METADATA_UPDATE");
    assert_eq!(events[1].payload_format, "NONE");
    assert!(events[1].payload.is_none());

    assert_eq!(events[2].event_type, "OBJECT_DELETE");
    assert_eq!(events[2].notification_id, "1");

    // Introspect endpoint surfaces recorded events.
    let introspect = call(
        &mut f.server,
        "GET",
        "/_introspect/buckets/events-bucket/notification-events",
        b"",
        &[],
    );
    assert_eq!(introspect.status, 200);
    assert_eq!(json(&introspect)["events"].as_array().unwrap().len(), 3);

    // Delete notification config.
    assert_eq!(
        call(
            &mut f.server,
            "DELETE",
            "/storage/v1/b/events-bucket/notificationConfigs/1",
            b"",
            &[],
        )
        .status,
        204
    );
    assert_eq!(
        call(
            &mut f.server,
            "GET",
            "/storage/v1/b/events-bucket/notificationConfigs/1",
            b"",
            &[],
        )
        .status,
        404
    );
}

#[test]
fn notification_pubsub_publish_wire_format_and_resumable_dedupe() {
    let mut f = Fixture::new(Config::default(), "2026-06-01T12:00:00Z");
    assert_eq!(
        call(
            &mut f.server,
            "POST",
            "/storage/v1/b",
            br#"{"name":"push-bucket"}"#,
            &[],
        )
        .status,
        200
    );
    assert_eq!(
        call(
            &mut f.server,
            "POST",
            "/storage/v1/b/push-bucket/notificationConfigs",
            br#"{"topic":"projects/devcloud/topics/obj-events","payload_format":"JSON_API_V1"}"#,
            &[],
        )
        .status,
        200
    );

    // Start and commit a resumable upload, then replay the final PUT:
    // OBJECT_FINALIZE notification must be recorded exactly once.
    let init = call(
        &mut f.server,
        "POST",
        "/upload/storage/v1/b/push-bucket/o?uploadType=resumable&name=hello.txt",
        br#"{"contentType":"text/plain"}"#,
        &[("Content-Type", "application/json")],
    );
    assert_eq!(init.status, 200);
    let upload_target = init.headers["Location"].clone();
    let commit = call(
        &mut f.server,
        "PUT",
        &upload_target,
        b"world",
        &[("Content-Range", "bytes 0-4/5")],
    );
    assert_eq!(commit.status, 200);
    let replay = call(
        &mut f.server,
        "PUT",
        &upload_target,
        b"world",
        &[("Content-Range", "bytes 0-4/5")],
    );
    assert_eq!(replay.status, 200);

    let events = f.server.list_notification_events("push-bucket");
    assert_eq!(events.len(), 1);
    let ev = &events[0];
    let wire = build_pubsub_publish_request(
        "127.0.0.1:18086",
        &ev.topic,
        &ev.attributes,
        ev.payload.as_ref(),
    );
    assert!(wire.starts_with("POST /v1/projects/devcloud/topics/obj-events:publish HTTP/1.1\r\n"));
    let (_, body_str) = wire.split_once("\r\n\r\n").unwrap();
    let body_json: serde_json::Value = serde_json::from_str(body_str).unwrap();
    let msg = &body_json["messages"][0];
    assert_eq!(msg["attributes"]["eventType"], "OBJECT_FINALIZE");
    assert_eq!(msg["attributes"]["objectId"], "hello.txt");
    let decoded = base64::std_decode(msg["data"].as_str().unwrap()).unwrap();
    let payload: serde_json::Value = serde_json::from_slice(&decoded).unwrap();
    assert_eq!(payload["name"], "hello.txt");
    assert_eq!(payload["bucket"], "push-bucket");
}

#[test]
fn signed_urls_v4_hmac_v4_rsa_and_v2_validation() {
    let mut f = Fixture::new(
        Config {
            project: "devcloud".to_string(),
            auth_mode: "bearer-dev".to_string(),
            bearer_token: "secret-bearer".to_string(),
            ..Default::default()
        },
        "2026-06-01T12:00:00Z",
    );
    f.server
        .add_hmac_key(DEFAULT_HMAC_ACCESS_ID, DEFAULT_HMAC_SECRET);

    // Create bucket and upload object using bearer auth.
    assert_eq!(
        call(
            &mut f.server,
            "POST",
            "/storage/v1/b",
            br#"{"name":"signed-bucket"}"#,
            &[("Authorization", "Bearer secret-bearer")],
        )
        .status,
        200
    );

    // Anonymous XML PUT is rejected under bearer-dev.
    assert_eq!(
        call(
            &mut f.server,
            "PUT",
            "/signed-bucket/report.txt",
            b"secret",
            &[]
        )
        .status,
        401
    );

    // V4 HMAC signed PUT succeeds WITHOUT Bearer token!
    let put_target = sign_v4_hmac_target(&SignV4HmacInput {
        method: "PUT",
        path: "/signed-bucket/report.txt",
        host: "127.0.0.1:14443",
        access_id: DEFAULT_HMAC_ACCESS_ID,
        secret: DEFAULT_HMAC_SECRET,
        region: "auto",
        goog_date: "20260601T120000Z",
        expires: 300,
        extra_query: vec![],
        extra_headers: BTreeMap::from([("content-type".to_string(), "text/plain".to_string())]),
    });
    let signed_put = call(
        &mut f.server,
        "PUT",
        &put_target,
        b"signed payload",
        &[("Host", "127.0.0.1:14443"), ("Content-Type", "text/plain")],
    );
    assert_eq!(
        signed_put.status,
        200,
        "{}",
        String::from_utf8_lossy(&signed_put.body)
    );

    // V4 HMAC signed GET with response-content-disposition override succeeds without Bearer token.
    let get_target = sign_v4_hmac_target(&SignV4HmacInput {
        method: "GET",
        path: "/signed-bucket/report.txt",
        host: "127.0.0.1:14443",
        access_id: DEFAULT_HMAC_ACCESS_ID,
        secret: DEFAULT_HMAC_SECRET,
        region: "auto",
        goog_date: "20260601T120000Z",
        expires: 300,
        extra_query: vec![(
            "response-content-disposition".to_string(),
            "attachment; filename=\"r.txt\"".to_string(),
        )],
        extra_headers: BTreeMap::new(),
    });
    let signed_get = call(
        &mut f.server,
        "GET",
        &get_target,
        b"",
        &[("Host", "127.0.0.1:14443")],
    );
    assert_eq!(signed_get.status, 200);
    assert_eq!(signed_get.body, b"signed payload");
    assert_eq!(
        signed_get
            .headers
            .get("Content-Disposition")
            .map(String::as_str),
        Some("attachment; filename=\"r.txt\"")
    );

    // Tampered signature is rejected with 403 SignatureDoesNotMatch.
    let mut tampered = get_target.clone();
    let last_char = tampered.pop().unwrap();
    tampered.push(if last_char == '0' { '1' } else { '0' });
    let bad_sig = call(
        &mut f.server,
        "GET",
        &tampered,
        b"",
        &[("Host", "127.0.0.1:14443")],
    );
    assert_eq!(bad_sig.status, 403);
    assert!(String::from_utf8_lossy(&bad_sig.body).contains("SignatureDoesNotMatch"));

    // Missing signed header (Host omitted) is rejected with 403.
    let missing_host = call(&mut f.server, "GET", &get_target, b"", &[]);
    assert_eq!(missing_host.status, 403);

    // Expired V4 URL is rejected with 400 ExpiredToken.
    f.server.set_fixed_now("2026-06-01T12:10:00Z");
    let expired = call(
        &mut f.server,
        "GET",
        &get_target,
        b"",
        &[("Host", "127.0.0.1:14443")],
    );
    assert_eq!(expired.status, 400);
    assert!(String::from_utf8_lossy(&expired.body).contains("ExpiredToken"));

    // Reset clock and test V4 GOOG4-RSA-SHA256 structural + expiry validation.
    f.server.set_fixed_now("2026-06-01T12:00:00Z");
    let rsa_target = "/signed-bucket/report.txt?X-Goog-Algorithm=GOOG4-RSA-SHA256&X-Goog-Credential=sa%40devcloud.iam.gserviceaccount.com%2F20260601%2Fauto%2Fstorage%2Fgoog4_request&X-Goog-Date=20260601T120000Z&X-Goog-Expires=60&X-Goog-SignedHeaders=host&X-Goog-Signature=deadbeef";
    let rsa_ok = call(
        &mut f.server,
        "GET",
        rsa_target,
        b"",
        &[("Host", "127.0.0.1:14443")],
    );
    assert_eq!(rsa_ok.status, 200);
    assert_eq!(rsa_ok.body, b"signed payload");

    // V2 Signed URL: valid passes without Bearer token; expired fails with 400 ExpiredToken.
    // 2026-06-01T12:00:00Z = 1780315200
    let v2_ok = call(
        &mut f.server,
        "GET",
        "/signed-bucket/report.txt?GoogleAccessId=sa@devcloud.iam.gserviceaccount.com&Expires=1780315800&Signature=YWJjZA==",
        b"",
        &[],
    );
    assert_eq!(v2_ok.status, 200);
    assert_eq!(v2_ok.body, b"signed payload");

    let v2_expired = call(
        &mut f.server,
        "GET",
        "/signed-bucket/report.txt?GoogleAccessId=sa@devcloud.iam.gserviceaccount.com&Expires=1780315000&Signature=YWJjZA==",
        b"",
        &[],
    );
    assert_eq!(v2_expired.status, 400);
    assert!(String::from_utf8_lossy(&v2_expired.body).contains("ExpiredToken"));
}

fn rsa_v4_target(method: &str, path: &str, key_pem: &str, account: &str) -> String {
    sign_v4_rsa_target(&SignV4RsaInput {
        method,
        path,
        host: "127.0.0.1:14443",
        service_account: account,
        private_key_pem: key_pem,
        region: "auto",
        goog_date: "20260601T120000Z",
        expires: 300,
        extra_query: vec![],
        extra_headers: BTreeMap::new(),
    })
    .unwrap()
}

fn v2_target(method: &str, path: &str, key_pem: &str, content_type: &str) -> String {
    sign_v2_target(&SignV2Input {
        method,
        path,
        service_account: TEST_SA_EMAIL,
        private_key_pem: key_pem,
        // 2026-06-01T12:00:00Z + 600s
        expires: 1_780_315_800,
        content_md5: "",
        content_type,
        extra_headers: BTreeMap::new(),
    })
    .unwrap()
}

fn seed_rsa_bucket(f: &mut Fixture) {
    let auth = [("Authorization", "Bearer secret-bearer")];
    assert_eq!(
        call(
            &mut f.server,
            "POST",
            "/storage/v1/b",
            br#"{"name":"rsa-bucket"}"#,
            &auth
        )
        .status,
        200
    );
    assert_eq!(
        call(
            &mut f.server,
            "PUT",
            "/rsa-bucket/doc.txt",
            b"rsa payload",
            &auth
        )
        .status,
        200
    );
}

#[test]
fn rsa_signed_urls_v4_and_v2_are_cryptographically_verified() {
    let mut f = Fixture::new(
        Config {
            auth_mode: "bearer-dev".to_string(),
            bearer_token: "secret-bearer".to_string(),
            ..Default::default()
        },
        "2026-06-01T12:00:00Z",
    );
    f.server
        .add_rsa_public_key_pem(TEST_SA_EMAIL, TEST_SA_PUBLIC_KEY_PEM)
        .unwrap();
    seed_rsa_bucket(&mut f);
    let host = [("Host", "127.0.0.1:14443")];

    // V4 GOOG4-RSA-SHA256: a correct signature authorizes the request.
    let target = rsa_v4_target(
        "GET",
        "/rsa-bucket/doc.txt",
        TEST_SA_PRIVATE_KEY_PEM,
        TEST_SA_EMAIL,
    );
    let ok = call(&mut f.server, "GET", &target, b"", &host);
    assert_eq!(ok.status, 200, "{}", String::from_utf8_lossy(&ok.body));
    assert_eq!(ok.body, b"rsa payload");

    // Signature from an unrelated key, a flipped signature byte, a different
    // method, and a different path are all rejected.
    let wrong_key = rsa_v4_target(
        "GET",
        "/rsa-bucket/doc.txt",
        OTHER_PRIVATE_KEY_PEM,
        TEST_SA_EMAIL,
    );
    let mut flipped = target.clone();
    let last = flipped.pop().unwrap();
    flipped.push(if last == '0' { '1' } else { '0' });
    for bad in [&wrong_key, &flipped] {
        let r = call(&mut f.server, "GET", bad, b"", &host);
        assert_eq!(r.status, 403);
        assert!(String::from_utf8_lossy(&r.body).contains("SignatureDoesNotMatch"));
    }
    assert_eq!(call(&mut f.server, "HEAD", &target, b"", &host).status, 403);
    let other_path = target.replacen("/rsa-bucket/doc.txt", "/rsa-bucket/other.txt", 1);
    assert_eq!(
        call(&mut f.server, "GET", &other_path, b"", &host).status,
        403
    );

    // V4 RSA signed PUT works without a bearer token.
    let put = rsa_v4_target(
        "PUT",
        "/rsa-bucket/up.txt",
        TEST_SA_PRIVATE_KEY_PEM,
        TEST_SA_EMAIL,
    );
    assert_eq!(
        call(&mut f.server, "PUT", &put, b"uploaded", &host).status,
        200
    );

    // V2: verified over VERB/Content-MD5/Content-Type/Expires/resource.
    let v2 = v2_target("GET", "/rsa-bucket/doc.txt", TEST_SA_PRIVATE_KEY_PEM, "");
    let r = call(&mut f.server, "GET", &v2, b"", &[]);
    assert_eq!(r.status, 200, "{}", String::from_utf8_lossy(&r.body));
    assert_eq!(r.body, b"rsa payload");
    let v2_wrong = v2_target("GET", "/rsa-bucket/doc.txt", OTHER_PRIVATE_KEY_PEM, "");
    let r = call(&mut f.server, "GET", &v2_wrong, b"", &[]);
    assert_eq!(r.status, 403);
    assert!(String::from_utf8_lossy(&r.body).contains("SignatureDoesNotMatch"));
    let v2_put = v2_target(
        "PUT",
        "/rsa-bucket/v2.txt",
        TEST_SA_PRIVATE_KEY_PEM,
        "text/plain",
    );
    assert_eq!(
        call(
            &mut f.server,
            "PUT",
            &v2_put,
            b"v2",
            &[("Content-Type", "text/plain")]
        )
        .status,
        200
    );
    // Content-Type is part of the V2 string-to-sign.
    assert_eq!(
        call(
            &mut f.server,
            "PUT",
            &v2_put,
            b"v2",
            &[("Content-Type", "text/html")]
        )
        .status,
        403
    );
    // V2 against a virtual-host bucket signs `/{bucket}/{object}`.
    let vhost = v2.replacen("/rsa-bucket/doc.txt", "/doc.txt", 1);
    let r = call(
        &mut f.server,
        "GET",
        &vhost,
        b"",
        &[("Host", "rsa-bucket.localhost:14443")],
    );
    assert_eq!(r.status, 200, "{}", String::from_utf8_lossy(&r.body));
    // Expiry still applies to valid signatures.
    f.server.set_fixed_now("2026-06-01T12:20:00Z");
    let r = call(&mut f.server, "GET", &v2, b"", &[]);
    assert_eq!(r.status, 400);
    assert!(String::from_utf8_lossy(&r.body).contains("ExpiredToken"));
}

#[test]
fn rsa_keys_register_from_service_account_json_and_strict_mode_requires_a_key() {
    // Service-account JSON registration (private key -> public key).
    let mut f = Fixture::new(
        Config {
            auth_mode: "strict".to_string(),
            bearer_token: "secret-bearer".to_string(),
            ..Default::default()
        },
        "2026-06-01T12:00:00Z",
    );
    seed_rsa_bucket(&mut f);
    let host = [("Host", "127.0.0.1:14443")];
    let target = rsa_v4_target(
        "GET",
        "/rsa-bucket/doc.txt",
        TEST_SA_PRIVATE_KEY_PEM,
        TEST_SA_EMAIL,
    );

    // strict: no key registered for the signer -> rejected.
    let r = call(&mut f.server, "GET", &target, b"", &host);
    assert_eq!(r.status, 403);
    assert!(String::from_utf8_lossy(&r.body).contains("no RSA public key is registered"));

    f.server
        .add_service_account_key_json(&test_service_account_json())
        .unwrap();
    // Key registered but the service account has no IAM binding yet.
    let r = call(&mut f.server, "GET", &target, b"", &host);
    assert_eq!(r.status, 403);
    assert!(String::from_utf8_lossy(&r.body).contains("storage.objects.get"));

    let policy = format!(
        r#"{{"bindings":[{{"role":"roles/storage.objectViewer","members":["serviceAccount:{TEST_SA_EMAIL}"]}}]}}"#
    );
    assert_eq!(
        call(
            &mut f.server,
            "PUT",
            "/storage/v1/b/rsa-bucket/iam",
            policy.as_bytes(),
            &[("Authorization", "Bearer secret-bearer")],
        )
        .status,
        200
    );
    let r = call(&mut f.server, "GET", &target, b"", &host);
    assert_eq!(r.status, 200, "{}", String::from_utf8_lossy(&r.body));
    assert_eq!(r.body, b"rsa payload");
    // Viewer cannot write through a signed PUT.
    let put = rsa_v4_target(
        "PUT",
        "/rsa-bucket/new.txt",
        TEST_SA_PRIVATE_KEY_PEM,
        TEST_SA_EMAIL,
    );
    assert_eq!(call(&mut f.server, "PUT", &put, b"x", &host).status, 403);

    // Malformed key material is rejected at registration time.
    assert!(f
        .server
        .add_rsa_public_key_pem(
            "x@y",
            "-----BEGIN PUBLIC KEY-----\nAAAA\n-----END PUBLIC KEY-----\n"
        )
        .is_err());
    assert!(f.server.add_service_account_key_json("{}").is_err());

    // Non-strict modes keep structural-only checks for unregistered signers.
    let mut relaxed = Fixture::new(Config::default(), "2026-06-01T12:00:00Z");
    assert_eq!(
        call(
            &mut relaxed.server,
            "POST",
            "/storage/v1/b",
            br#"{"name":"rsa-bucket"}"#,
            &[]
        )
        .status,
        200
    );
    assert_eq!(
        call(&mut relaxed.server, "PUT", "/rsa-bucket/doc.txt", b"r", &[]).status,
        200
    );
    let unregistered = rsa_v4_target(
        "GET",
        "/rsa-bucket/doc.txt",
        OTHER_PRIVATE_KEY_PEM,
        "nobody@x.iam.gserviceaccount.com",
    );
    assert_eq!(
        call(&mut relaxed.server, "GET", &unregistered, b"", &host).status,
        200
    );
}
