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
