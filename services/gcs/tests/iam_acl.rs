//! GCS IAM policy, testIamPermissions, bucket/defaultObject/object ACLs,
//! XML ACL interop, and Uniform Bucket-Level Access tests.

use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use devcloud_gcs::http::{route, Config, Request, Response, Server};
use devcloud_s3::store::FileBucketStore;

static NEXT_ID: AtomicU64 = AtomicU64::new(0);

struct Fixture {
    root: PathBuf,
    server: Server,
}

impl Fixture {
    fn new() -> Self {
        let root = std::env::temp_dir().join(format!(
            "devcloud-gcs-iam-acl-{}-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
            NEXT_ID.fetch_add(1, Ordering::Relaxed),
        ));
        let server = Server::new(
            Config {
                project: "devcloud".to_string(),
                location: "US".to_string(),
                ..Default::default()
            },
            FileBucketStore::new(root.join("buckets")),
        );
        Self { root, server }
    }

    fn restart(&self) -> Server {
        Server::new(
            Config {
                project: "devcloud".to_string(),
                location: "US".to_string(),
                ..Default::default()
            },
            FileBucketStore::new(self.root.join("buckets")),
        )
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
    body: &str,
    headers: &[(&str, &str)],
) -> Response {
    let mut req = Request::new(method, target, body.as_bytes().to_vec());
    if !body.is_empty() {
        req.headers
            .insert("content-type".to_string(), "application/json".to_string());
    }
    for (k, v) in headers {
        req.headers.insert(k.to_ascii_lowercase(), (*v).to_string());
    }
    route(server, &req)
}

fn json(r: &Response) -> serde_json::Value {
    serde_json::from_slice(&r.body).unwrap()
}

#[test]
fn bucket_iam_policy_roundtrip_etag_and_test_permissions() {
    let mut f = Fixture::new();
    assert_eq!(
        call(
            &mut f.server,
            "POST",
            "/storage/v1/b",
            r#"{"name":"iam-bucket"}"#,
            &[]
        )
        .status,
        200
    );

    let initial = call(
        &mut f.server,
        "GET",
        "/storage/v1/b/iam-bucket/iam",
        "",
        &[],
    );
    assert_eq!(initial.status, 200);
    let initial_json = json(&initial);
    assert_eq!(initial_json["kind"], "storage#policy");
    assert_eq!(initial_json["resourceId"], "projects/_/buckets/iam-bucket");
    let etag1 = initial_json["etag"].as_str().unwrap().to_string();

    // Update policy with matching etag.
    let updated = call(
        &mut f.server,
        "PUT",
        "/storage/v1/b/iam-bucket/iam",
        &format!(
            r#"{{"version":3,"etag":"{etag1}","bindings":[{{"role":"roles/storage.objectViewer","members":["user:alice@example.com"]}},{{"role":"roles/storage.admin","members":["user:bob@example.com"]}}]}}"#
        ),
        &[],
    );
    assert_eq!(updated.status, 200);
    let updated_json = json(&updated);
    assert_eq!(updated_json["version"], 3);
    assert_ne!(updated_json["etag"], etag1);

    // Stale etag fails with 412.
    let stale = call(
        &mut f.server,
        "PUT",
        "/storage/v1/b/iam-bucket/iam",
        &format!(r#"{{"etag":"{etag1}","bindings":[]}}"#),
        &[],
    );
    assert_eq!(stale.status, 412);

    // Persists across restart.
    let mut restarted = f.restart();
    let fetched = call(
        &mut restarted,
        "GET",
        "/storage/v1/b/iam-bucket/iam",
        "",
        &[],
    );
    assert_eq!(fetched.status, 200);
    assert_eq!(json(&fetched)["bindings"].as_array().unwrap().len(), 2);

    // testPermissions without principal (relaxed mode grants all requested).
    let relaxed_test = call(
        &mut restarted,
        "GET",
        "/storage/v1/b/iam-bucket/iam/testPermissions?permissions=storage.objects.get&permissions=storage.objects.delete",
        "",
        &[],
    );
    assert_eq!(relaxed_test.status, 200);
    assert_eq!(
        json(&relaxed_test)["permissions"],
        serde_json::json!(["storage.objects.get", "storage.objects.delete"])
    );

    // testPermissions with principal evaluates stored IAM bindings.
    let alice_test = call(
        &mut restarted,
        "GET",
        "/storage/v1/b/iam-bucket/iam/testPermissions?permissions=storage.objects.get&permissions=storage.objects.delete",
        "",
        &[("x-goog-principal", "user:alice@example.com")],
    );
    assert_eq!(alice_test.status, 200);
    assert_eq!(
        json(&alice_test)["permissions"],
        serde_json::json!(["storage.objects.get"])
    );

    let bob_test = call(
        &mut restarted,
        "GET",
        "/storage/v1/b/iam-bucket/iam/testPermissions?permissions=storage.objects.get,storage.objects.delete",
        "",
        &[("x-goog-principal", "user:bob@example.com")],
    );
    assert_eq!(bob_test.status, 200);
    assert_eq!(
        json(&bob_test)["permissions"],
        serde_json::json!(["storage.objects.get", "storage.objects.delete"])
    );
}

#[test]
fn bucket_default_object_and_object_acls_json_and_xml_interop() {
    let mut f = Fixture::new();
    assert_eq!(
        call(
            &mut f.server,
            "POST",
            "/storage/v1/b?predefinedAcl=publicRead&predefinedDefaultObjectAcl=authenticatedRead",
            r#"{"name":"acl-bucket"}"#,
            &[],
        )
        .status,
        200
    );

    // Bucket ACL reflects predefinedAcl=publicRead.
    let list = call(
        &mut f.server,
        "GET",
        "/storage/v1/b/acl-bucket/acl",
        "",
        &[],
    );
    assert_eq!(list.status, 200);
    let items = json(&list)["items"].as_array().unwrap().clone();
    assert!(items
        .iter()
        .any(|e| e["entity"] == "allUsers" && e["role"] == "READER"));

    // Insert, update, get, delete bucket ACL entry.
    let ins = call(
        &mut f.server,
        "POST",
        "/storage/v1/b/acl-bucket/acl",
        r#"{"entity":"user-dev@example.com","role":"WRITER"}"#,
        &[],
    );
    assert_eq!(ins.status, 200);
    assert_eq!(json(&ins)["email"], "dev@example.com");

    let patch = call(
        &mut f.server,
        "PATCH",
        "/storage/v1/b/acl-bucket/acl/user-dev%40example.com",
        r#"{"role":"OWNER"}"#,
        &[],
    );
    assert_eq!(patch.status, 200);
    assert_eq!(json(&patch)["role"], "OWNER");

    // XML ?acl reflects the same entries.
    let xml_acl = call(&mut f.server, "GET", "/acl-bucket?acl", "", &[]);
    assert_eq!(xml_acl.status, 200);
    let xml_body = String::from_utf8_lossy(&xml_acl.body);
    assert!(xml_body.contains("dev@example.com"));
    assert!(xml_body.contains("<Permission>FULL_CONTROL</Permission>"));

    // Delete bucket ACL entry.
    assert_eq!(
        call(
            &mut f.server,
            "DELETE",
            "/storage/v1/b/acl-bucket/acl/user-dev%40example.com",
            "",
            &[],
        )
        .status,
        204
    );
    assert_eq!(
        call(
            &mut f.server,
            "GET",
            "/storage/v1/b/acl-bucket/acl/user-dev%40example.com",
            "",
            &[],
        )
        .status,
        404
    );

    // Default object ACL inherited by new object.
    let def_list = call(
        &mut f.server,
        "GET",
        "/storage/v1/b/acl-bucket/defaultObjectAcl",
        "",
        &[],
    );
    assert_eq!(def_list.status, 200);
    assert!(json(&def_list)["items"]
        .as_array()
        .unwrap()
        .iter()
        .any(|e| e["entity"] == "allAuthenticatedUsers" && e["role"] == "READER"));

    assert_eq!(
        call(
            &mut f.server,
            "POST",
            "/upload/storage/v1/b/acl-bucket/o?uploadType=media&name=hello.txt",
            "hello",
            &[("Content-Type", "text/plain")],
        )
        .status,
        200
    );

    let obj_acl = call(
        &mut f.server,
        "GET",
        "/storage/v1/b/acl-bucket/o/hello.txt/acl",
        "",
        &[],
    );
    assert_eq!(obj_acl.status, 200);
    assert!(json(&obj_acl)["items"]
        .as_array()
        .unwrap()
        .iter()
        .any(|e| e["entity"] == "allAuthenticatedUsers" && e["role"] == "READER"));

    // Update object ACL via XML PUT ?acl and verify via JSON GET.
    let put_xml_acl = call(
        &mut f.server,
        "PUT",
        "/acl-bucket/hello.txt?acl",
        r#"<AccessControlList><Entries><Entry><Scope type="AllUsers"/><Permission>READ</Permission></Entry></Entries></AccessControlList>"#,
        &[("Content-Type", "application/xml")],
    );
    assert_eq!(put_xml_acl.status, 200);

    let obj_acl_after = call(
        &mut f.server,
        "GET",
        "/storage/v1/b/acl-bucket/o/hello.txt/acl/allUsers",
        "",
        &[],
    );
    assert_eq!(obj_acl_after.status, 200);
    assert_eq!(json(&obj_acl_after)["role"], "READER");
}

#[test]
fn uniform_bucket_level_access_rejects_legacy_acl_endpoints() {
    let mut f = Fixture::new();
    let created = call(
        &mut f.server,
        "POST",
        "/storage/v1/b",
        r#"{"name":"ubla-bucket","iamConfiguration":{"uniformBucketLevelAccess":{"enabled":true}}}"#,
        &[],
    );
    assert_eq!(created.status, 200);
    assert_eq!(
        json(&created)["iamConfiguration"]["uniformBucketLevelAccess"]["enabled"],
        true
    );

    assert_eq!(
        call(
            &mut f.server,
            "GET",
            "/storage/v1/b/ubla-bucket/acl",
            "",
            &[]
        )
        .status,
        400
    );
    assert_eq!(
        call(
            &mut f.server,
            "GET",
            "/storage/v1/b/ubla-bucket/defaultObjectAcl",
            "",
            &[],
        )
        .status,
        400
    );
    assert_eq!(
        call(&mut f.server, "GET", "/ubla-bucket?acl", "", &[]).status,
        400
    );
    assert_eq!(
        call(
            &mut f.server,
            "POST",
            "/upload/storage/v1/b/ubla-bucket/o?uploadType=media&name=a.txt&predefinedAcl=publicRead",
            "x",
            &[("Content-Type", "text/plain")],
        )
        .status,
        400
    );
}

fn strict_fixture() -> Fixture {
    let root = std::env::temp_dir().join(format!(
        "devcloud-gcs-iam-strict-{}-{}-{}",
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
            auth_mode: "strict".to_string(),
            bearer_token: "owner-token".to_string(),
            ..Default::default()
        },
        {
            let mut store = FileBucketStore::new(root.join("buckets"));
            store.set_fixed_now("2026-06-01T12:00:00Z");
            store
        },
    );
    server.add_access_token("alice-token", "user:alice@example.com");
    server.add_access_token("bob-token", "user:bob@example.com");
    Fixture { root, server }
}

const OWNER: (&str, &str) = ("Authorization", "Bearer owner-token");
const ALICE: (&str, &str) = ("Authorization", "Bearer alice-token");
const BOB: (&str, &str) = ("Authorization", "Bearer bob-token");

fn set_policy(server: &mut Server, bucket: &str, bindings: &str) {
    let r = call(
        server,
        "PUT",
        &format!("/storage/v1/b/{bucket}/iam"),
        &format!(r#"{{"bindings":{bindings}}}"#),
        &[OWNER],
    );
    assert_eq!(r.status, 200, "{}", String::from_utf8_lossy(&r.body));
}

#[test]
fn strict_mode_enforces_bucket_iam_per_request() {
    let mut f = strict_fixture();
    let s = &mut f.server;

    // Unknown tokens are rejected; project-level ops are owner-only.
    assert_eq!(
        call(
            s,
            "GET",
            "/storage/v1/b",
            "",
            &[("Authorization", "Bearer nope")]
        )
        .status,
        401
    );
    let r = call(s, "POST", "/storage/v1/b", r#"{"name":"iam-b"}"#, &[ALICE]);
    assert_eq!(r.status, 403);
    assert_eq!(json(&r)["error"]["errors"][0]["reason"], "forbidden");
    assert_eq!(call(s, "GET", "/storage/v1/b", "", &[ALICE]).status, 403);
    assert_eq!(
        call(s, "GET", "/_introspect/buckets", "", &[ALICE]).status,
        403
    );
    assert_eq!(
        call(s, "POST", "/storage/v1/b", r#"{"name":"iam-b"}"#, &[OWNER]).status,
        200
    );
    assert_eq!(
        call(s, "PUT", "/iam-b/a.txt", "hello", &[OWNER]).status,
        200
    );

    // No binding: alice cannot read, list, write, or read the policy.
    let r = call(s, "GET", "/iam-b/a.txt", "", &[ALICE]);
    assert_eq!(r.status, 403);
    assert!(String::from_utf8_lossy(&r.body).contains("AccessDenied"));
    assert_eq!(
        call(s, "GET", "/storage/v1/b/iam-b/o", "", &[ALICE]).status,
        403
    );
    assert_eq!(
        call(s, "GET", "/storage/v1/b/iam-b/iam", "", &[ALICE]).status,
        403
    );

    // objectViewer grants get/list only.
    set_policy(
        s,
        "iam-b",
        r#"[{"role":"roles/storage.objectViewer","members":["user:alice@example.com"]}]"#,
    );
    let r = call(s, "GET", "/iam-b/a.txt", "", &[ALICE]);
    assert_eq!(r.status, 200);
    assert_eq!(r.body, b"hello");
    assert_eq!(
        call(s, "GET", "/storage/v1/b/iam-b/o", "", &[ALICE]).status,
        200
    );
    assert_eq!(
        call(
            s,
            "GET",
            "/download/storage/v1/b/iam-b/o/a.txt?alt=media",
            "",
            &[ALICE]
        )
        .status,
        200
    );
    assert_eq!(call(s, "PUT", "/iam-b/b.txt", "x", &[ALICE]).status, 403);
    assert_eq!(
        call(
            s,
            "POST",
            "/upload/storage/v1/b/iam-b/o?uploadType=media&name=b.txt",
            "x",
            &[ALICE]
        )
        .status,
        403
    );
    assert_eq!(
        call(s, "DELETE", "/storage/v1/b/iam-b/o/a.txt", "", &[ALICE]).status,
        403
    );
    assert_eq!(
        call(s, "DELETE", "/storage/v1/b/iam-b", "", &[ALICE]).status,
        403
    );
    assert_eq!(
        call(s, "PUT", "/storage/v1/b/iam-b/iam", "{}", &[ALICE]).status,
        403
    );
    // Bob is not bound.
    assert_eq!(call(s, "GET", "/iam-b/a.txt", "", &[BOB]).status, 403);
    // testIamPermissions stays callable and reports alice's grants.
    let r = call(
        s,
        "GET",
        "/storage/v1/b/iam-b/iam/testPermissions?permissions=storage.objects.get,storage.objects.create",
        "",
        &[ALICE, ("x-goog-principal", "user:alice@example.com")],
    );
    assert_eq!(r.status, 200);
    assert_eq!(
        json(&r)["permissions"],
        serde_json::json!(["storage.objects.get"])
    );

    // objectAdmin grants writes and deletes; copy needs create on the target.
    set_policy(
        s,
        "iam-b",
        r#"[{"role":"roles/storage.objectAdmin","members":["user:alice@example.com"]}]"#,
    );
    assert_eq!(call(s, "PUT", "/iam-b/b.txt", "x", &[ALICE]).status, 200);
    assert_eq!(
        call(s, "DELETE", "/storage/v1/b/iam-b/o/b.txt", "", &[ALICE]).status,
        204
    );
    assert_eq!(
        call(
            s,
            "POST",
            "/storage/v1/b",
            r#"{"name":"iam-dest"}"#,
            &[OWNER]
        )
        .status,
        200
    );
    let copy = "/storage/v1/b/iam-b/o/a.txt/copyTo/b/iam-dest/o/a.txt";
    assert_eq!(call(s, "POST", copy, "", &[ALICE]).status, 403);
    let r = call(
        s,
        "PUT",
        "/iam-dest/a.txt",
        "",
        &[ALICE, ("x-goog-copy-source", "/iam-b/a.txt")],
    );
    assert_eq!(r.status, 403);
    set_policy(
        s,
        "iam-dest",
        r#"[{"role":"roles/storage.objectCreator","members":["user:alice@example.com"]}]"#,
    );
    assert_eq!(call(s, "POST", copy, "", &[ALICE]).status, 200);
    // objectCreator cannot read back.
    assert_eq!(call(s, "GET", "/iam-dest/a.txt", "", &[ALICE]).status, 403);
    // Bucket admin operations still need bucket-level roles.
    assert_eq!(
        call(s, "GET", "/storage/v1/b/iam-b", "", &[ALICE]).status,
        403
    );
    set_policy(
        s,
        "iam-b",
        r#"[{"role":"roles/storage.admin","members":["user:alice@example.com"]}]"#,
    );
    assert_eq!(
        call(s, "GET", "/storage/v1/b/iam-b", "", &[ALICE]).status,
        200
    );
    assert_eq!(
        call(s, "GET", "/storage/v1/b/iam-b/iam", "", &[ALICE]).status,
        200
    );
}

#[test]
fn strict_mode_anonymous_conditions_and_hmac_service_accounts() {
    let mut f = strict_fixture();
    let s = &mut f.server;
    assert_eq!(
        call(s, "POST", "/storage/v1/b", r#"{"name":"pub-b"}"#, &[OWNER]).status,
        200
    );
    assert_eq!(
        call(s, "PUT", "/pub-b/a.txt", "public", &[OWNER]).status,
        200
    );

    // Anonymous callers get 401 until allUsers is bound.
    let r = call(s, "GET", "/pub-b/a.txt", "", &[]);
    assert_eq!(r.status, 401);
    assert!(r.headers.contains_key("WWW-Authenticate"));
    assert_eq!(
        call(s, "GET", "/storage/v1/b/pub-b/o/a.txt", "", &[]).status,
        401
    );

    // allAuthenticatedUsers does not cover anonymous callers.
    set_policy(
        s,
        "pub-b",
        r#"[{"role":"roles/storage.objectViewer","members":["allAuthenticatedUsers"]}]"#,
    );
    assert_eq!(call(s, "GET", "/pub-b/a.txt", "", &[]).status, 401);
    assert_eq!(call(s, "GET", "/pub-b/a.txt", "", &[BOB]).status, 200);

    set_policy(
        s,
        "pub-b",
        r#"[{"role":"roles/storage.objectViewer","members":["allUsers"]}]"#,
    );
    let r = call(s, "GET", "/pub-b/a.txt", "", &[]);
    assert_eq!(r.status, 200);
    assert_eq!(r.body, b"public");
    assert_eq!(call(s, "PUT", "/pub-b/a.txt", "x", &[]).status, 401);
    assert_eq!(call(s, "GET", "/", "", &[]).status, 401);

    // Conditional bindings are not evaluated and never grant access.
    set_policy(
        s,
        "pub-b",
        r#"[{"role":"roles/storage.objectViewer","members":["user:alice@example.com"],"condition":{"title":"t","expression":"request.time < timestamp('2030-01-01T00:00:00Z')"}}]"#,
    );
    assert_eq!(call(s, "GET", "/pub-b/a.txt", "", &[ALICE]).status, 403);

    // HMAC keys bound to a service account act as that account.
    s.add_hmac_key_for_service_account(
        "GOOG1SA",
        "sa-secret",
        "hmac-sa@devcloud.iam.gserviceaccount.com",
    );
    let target = devcloud_gcs::signed_urls::sign_v4_hmac_target(
        &devcloud_gcs::signed_urls::SignV4HmacInput {
            method: "GET",
            path: "/pub-b/a.txt",
            host: "127.0.0.1:14443",
            access_id: "GOOG1SA",
            secret: "sa-secret",
            region: "auto",
            goog_date: "20260601T120000Z",
            expires: 600,
            extra_query: vec![],
            extra_headers: Default::default(),
        },
    );
    let host = [("Host", "127.0.0.1:14443")];
    assert_eq!(call(s, "GET", &target, "", &host).status, 403);
    set_policy(
        s,
        "pub-b",
        r#"[{"role":"roles/storage.objectViewer","members":["serviceAccount:hmac-sa@devcloud.iam.gserviceaccount.com"]}]"#,
    );
    assert_eq!(call(s, "GET", &target, "", &host).status, 200);

    // Relaxed mode is unaffected by bucket IAM.
    let mut relaxed = Fixture::new();
    let r = &mut relaxed.server;
    assert_eq!(
        call(r, "POST", "/storage/v1/b", r#"{"name":"open-b"}"#, &[]).status,
        200
    );
    set_policy_relaxed(r);
    assert_eq!(call(r, "PUT", "/open-b/a.txt", "x", &[]).status, 200);
    assert_eq!(call(r, "GET", "/open-b/a.txt", "", &[]).status, 200);
}

fn set_policy_relaxed(server: &mut Server) {
    let r = call(
        server,
        "PUT",
        "/storage/v1/b/open-b/iam",
        r#"{"bindings":[{"role":"roles/storage.objectViewer","members":["user:nobody@example.com"]}]}"#,
        &[],
    );
    assert_eq!(r.status, 200);
}
