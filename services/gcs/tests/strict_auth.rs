//! `strict` auth mode: owner/principal/anonymous callers and bucket IAM
//! enforcement. Relaxed behavior is covered by the other test files.

use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use devcloud_gcs::http::{route, Config, Request, Response, Server};
use devcloud_gcs::strict_auth::parse_principals;
use devcloud_s3::store::FileBucketStore;

static NEXT_ID: AtomicU64 = AtomicU64::new(0);

const OWNER: &str = "owner-token";
const ALICE: &str = "alice-token";
const BOB: &str = "bob-token";

struct Fixture {
    root: PathBuf,
    server: Server,
}

impl Fixture {
    fn new(mode: &str) -> Self {
        let root = std::env::temp_dir().join(format!(
            "devcloud-gcs-strict-{}-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
            NEXT_ID.fetch_add(1, Ordering::Relaxed),
        ));
        let mut server = Server::new(
            Config {
                project: "devcloud".to_string(),
                auth_mode: mode.to_string(),
                bearer_token: OWNER.to_string(),
                ..Default::default()
            },
            FileBucketStore::new(root.join("buckets")),
        );
        for (token, member) in parse_principals(&format!(
            "{ALICE}=user:alice@example.com, {BOB}=serviceAccount:bob@other.test,bad"
        )) {
            server.add_principal(token, member);
        }
        Self { root, server }
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.root);
    }
}

fn call(server: &mut Server, method: &str, target: &str, body: &str, token: &str) -> Response {
    let mut req = Request::new(method, target, body.as_bytes().to_vec());
    if !body.is_empty() {
        req.headers
            .insert("content-type".to_string(), "application/json".to_string());
    }
    if !token.is_empty() {
        req.headers
            .insert("authorization".to_string(), format!("Bearer {token}"));
    }
    route(server, &req)
}

fn status(server: &mut Server, method: &str, target: &str, body: &str, token: &str) -> u16 {
    call(server, method, target, body, token).status
}

fn setup_bucket(f: &mut Fixture, bucket: &str, bindings: &str) {
    let s = &mut f.server;
    let create = format!(r#"{{"name":"{bucket}"}}"#);
    assert_eq!(status(s, "POST", "/storage/v1/b", &create, OWNER), 200);
    let upload = format!("/upload/storage/v1/b/{bucket}/o?uploadType=media&name=a.txt");
    assert_eq!(status(s, "POST", &upload, "hello", OWNER), 200);
    let policy = format!(r#"{{"bindings":{bindings}}}"#);
    let iam = format!("/storage/v1/b/{bucket}/iam");
    assert_eq!(status(s, "PUT", &iam, &policy, OWNER), 200);
}

#[test]
fn strict_requires_a_known_bearer_token() {
    let mut f = Fixture::new("strict");
    let s = &mut f.server;
    let anon = call(s, "GET", "/storage/v1/b", "", "");
    assert_eq!(anon.status, 401);
    assert_eq!(
        anon.headers.get("WWW-Authenticate").map(String::as_str),
        Some("Bearer realm=\"devcloud-gcs\"")
    );
    assert_eq!(status(s, "GET", "/storage/v1/b", "", "nope"), 401);
    assert_eq!(status(s, "GET", "/storage/v1/b", "", OWNER), 200);
    assert_eq!(status(s, "GET", "/_introspect/buckets", "", ""), 401);
    assert_eq!(status(s, "GET", "/_introspect/buckets", "", OWNER), 200);
    // XML API too.
    assert_eq!(status(s, "GET", "/", "", ""), 401);
}

#[test]
fn strict_principals_are_bound_by_bucket_iam() {
    let mut f = Fixture::new("strict");
    setup_bucket(
        &mut f,
        "data",
        r#"[{"role":"roles/storage.objectViewer","members":["user:alice@example.com"]}]"#,
    );
    let s = &mut f.server;

    // Alice can read objects through JSON, media download, and XML.
    assert_eq!(status(s, "GET", "/storage/v1/b/data/o", "", ALICE), 200);
    assert_eq!(
        status(s, "GET", "/storage/v1/b/data/o/a.txt", "", ALICE),
        200
    );
    let media = call(
        s,
        "GET",
        "/download/storage/v1/b/data/o/a.txt?alt=media",
        "",
        ALICE,
    );
    assert_eq!(media.status, 200);
    assert_eq!(media.body, b"hello");
    assert_eq!(status(s, "GET", "/data/a.txt", "", ALICE), 200);

    // ...but not write, delete, read bucket metadata, or change IAM.
    let up = "/upload/storage/v1/b/data/o?uploadType=media&name=b.txt";
    let denied = call(s, "POST", up, "x", ALICE);
    assert_eq!(denied.status, 403);
    assert!(String::from_utf8_lossy(&denied.body)
        .contains("user:alice@example.com does not have storage.objects.create access"));
    assert_eq!(status(s, "PUT", "/data/b.txt", "x", ALICE), 403);
    assert_eq!(
        status(s, "DELETE", "/storage/v1/b/data/o/a.txt", "", ALICE),
        403
    );
    assert_eq!(status(s, "GET", "/storage/v1/b/data", "", ALICE), 403);
    assert_eq!(
        status(
            s,
            "PUT",
            "/storage/v1/b/data/iam",
            r#"{"bindings":[]}"#,
            ALICE
        ),
        403
    );

    // Project-level calls are owner-only.
    assert_eq!(status(s, "GET", "/storage/v1/b", "", ALICE), 403);
    assert_eq!(
        status(s, "POST", "/storage/v1/b", r#"{"name":"mine"}"#, ALICE),
        403
    );

    // Bob has no binding at all.
    assert_eq!(status(s, "GET", "/storage/v1/b/data/o/a.txt", "", BOB), 403);
    // Anonymous callers get 401, not 403.
    assert_eq!(status(s, "GET", "/storage/v1/b/data/o/a.txt", "", ""), 401);
}

#[test]
fn strict_honors_all_users_domain_and_ignores_conditional_bindings() {
    let mut f = Fixture::new("strict");
    setup_bucket(
        &mut f,
        "public",
        r#"[{"role":"roles/storage.objectViewer","members":["allUsers"]},
            {"role":"roles/storage.objectAdmin","members":["domain:other.test"]},
            {"role":"roles/storage.admin","members":["user:alice@example.com"],
             "condition":{"title":"never","expression":"false"}}]"#,
    );
    let s = &mut f.server;
    assert_eq!(status(s, "GET", "/public/a.txt", "", ""), 200);
    assert_eq!(status(s, "PUT", "/public/b.txt", "x", ""), 401);
    // domain: grants Bob (serviceAccount:bob@other.test) objectAdmin.
    assert_eq!(status(s, "PUT", "/public/b.txt", "x", BOB), 200);
    // Alice only has a conditional admin binding, so she can read (allUsers)
    // but not write.
    assert_eq!(status(s, "GET", "/public/a.txt", "", ALICE), 200);
    assert_eq!(status(s, "PUT", "/public/c.txt", "x", ALICE), 403);
}

#[test]
fn strict_copy_needs_read_on_source_and_create_on_destination() {
    let mut f = Fixture::new("strict");
    setup_bucket(
        &mut f,
        "src",
        r#"[{"role":"roles/storage.objectViewer","members":["user:alice@example.com"]}]"#,
    );
    setup_bucket(&mut f, "dst", r#"[]"#);
    let copy = "/storage/v1/b/src/o/a.txt/copyTo/b/dst/o/copy.txt";
    assert_eq!(status(&mut f.server, "POST", copy, "", ALICE), 403);
    let grant = r#"{"bindings":[{"role":"roles/storage.objectCreator","members":["user:alice@example.com"]}]}"#;
    assert_eq!(
        status(&mut f.server, "PUT", "/storage/v1/b/dst/iam", grant, OWNER),
        200
    );
    assert_eq!(status(&mut f.server, "POST", copy, "", ALICE), 200);
}

#[test]
fn strict_rejects_signed_urls_it_cannot_verify() {
    let mut f = Fixture::new("strict");
    setup_bucket(&mut f, "signed", "[]");
    f.server.set_fixed_now("2026-06-01T12:00:00Z");
    let rsa = "/signed/a.txt?X-Goog-Algorithm=GOOG4-RSA-SHA256&X-Goog-Credential=sa%40devcloud.iam.gserviceaccount.com%2F20260601%2Fauto%2Fstorage%2Fgoog4_request&X-Goog-Date=20260601T120000Z&X-Goog-Expires=60&X-Goog-SignedHeaders=host&X-Goog-Signature=deadbeef";
    assert_eq!(status(&mut f.server, "GET", rsa, "", ""), 403);
    let v2 = "/signed/a.txt?GoogleAccessId=sa@devcloud.iam.gserviceaccount.com&Expires=1780315800&Signature=YWJjZA==";
    assert_eq!(status(&mut f.server, "GET", v2, "", ""), 403);
}

#[test]
fn non_strict_modes_ignore_principals_and_bucket_iam() {
    let mut f = Fixture::new("relaxed");
    setup_bucket(&mut f, "open", "[]");
    let s = &mut f.server;
    assert_eq!(status(s, "GET", "/storage/v1/b/open/o/a.txt", "", ""), 200);
    assert_eq!(status(s, "PUT", "/open/b.txt", "x", ALICE), 200);

    let mut f = Fixture::new("bearer-dev");
    setup_bucket(&mut f, "dev", "[]");
    let s = &mut f.server;
    assert_eq!(
        status(s, "GET", "/storage/v1/b/dev/o/a.txt", "", OWNER),
        200
    );
    // bearer-dev only knows the configured token.
    assert_eq!(
        status(s, "GET", "/storage/v1/b/dev/o/a.txt", "", ALICE),
        401
    );
}
