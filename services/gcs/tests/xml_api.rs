//! GCS XML API tests: service listing, bucket/object CRUD, v1/v2 listings,
//! copy, preconditions, virtual-host routing, and JSON/XML interoperability.

use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use devcloud_gcs::http::{route, Config, Request, Response, Server};
use devcloud_s3::base64;
use devcloud_s3::hashes::md5_etag;
use devcloud_s3::store::FileBucketStore;

static NEXT_ID: AtomicU64 = AtomicU64::new(0);

struct Fixture {
    root: PathBuf,
    server: Server,
}

impl Fixture {
    fn new() -> Self {
        let root = std::env::temp_dir().join(format!(
            "devcloud-gcs-xml-{}-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
            NEXT_ID.fetch_add(1, Ordering::Relaxed),
        ));
        let mut store = FileBucketStore::new(root.join("buckets"));
        store.set_fixed_now("2026-10-12T02:00:00Z");
        let server = Server::new(
            Config {
                project: "devcloud".to_string(),
                location: "US".to_string(),
                ..Default::default()
            },
            store,
        );
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

fn body_text(r: &Response) -> String {
    String::from_utf8_lossy(&r.body).into_owned()
}

#[test]
fn xml_service_and_bucket_crud_with_location_and_storage_class() {
    let mut f = Fixture::new();

    let create = call(
        &mut f.server,
        "PUT",
        "/xml-bucket",
        b"<CreateBucketConfiguration><LocationConstraint>EUROPE-WEST1</LocationConstraint><StorageClass>NEARLINE</StorageClass></CreateBucketConfiguration>",
        &[],
    );
    assert_eq!(create.status, 200, "{}", body_text(&create));

    let dup = call(&mut f.server, "PUT", "/xml-bucket", b"", &[]);
    assert_eq!(dup.status, 409);
    assert!(body_text(&dup).contains("BucketAlreadyOwnedByYou"));

    let head = call(&mut f.server, "HEAD", "/xml-bucket", b"", &[]);
    assert_eq!(head.status, 200);
    assert_eq!(
        head.headers
            .get("x-goog-metageneration")
            .map(String::as_str),
        Some("1")
    );

    let list_all = call(&mut f.server, "GET", "/", b"", &[]);
    assert_eq!(list_all.status, 200);
    assert!(body_text(&list_all).contains("<ListAllMyBucketsResult"));
    assert!(body_text(&list_all).contains("<Name>xml-bucket</Name>"));

    let loc = call(&mut f.server, "GET", "/xml-bucket?location", b"", &[]);
    assert_eq!(loc.status, 200);
    assert!(body_text(&loc).contains("<LocationConstraint>EUROPE-WEST1</LocationConstraint>"));

    let sc = call(&mut f.server, "GET", "/xml-bucket?storageClass", b"", &[]);
    assert_eq!(sc.status, 200);
    assert!(body_text(&sc).contains("<StorageClass>NEARLINE</StorageClass>"));

    // Bucket created via XML API is visible via JSON API with the same metadata.
    let json_get = call(&mut f.server, "GET", "/storage/v1/b/xml-bucket", b"", &[]);
    assert_eq!(json_get.status, 200);
    let v: serde_json::Value = serde_json::from_slice(&json_get.body).unwrap();
    assert_eq!(v["location"], "EUROPE-WEST1");
    assert_eq!(v["storageClass"], "NEARLINE");

    let del = call(&mut f.server, "DELETE", "/xml-bucket", b"", &[]);
    assert_eq!(del.status, 204);
    let missing = call(&mut f.server, "HEAD", "/xml-bucket", b"", &[]);
    assert_eq!(missing.status, 404);
}

#[test]
fn xml_object_put_get_head_range_preconditions_copy_and_delete() {
    let mut f = Fixture::new();
    assert_eq!(
        call(&mut f.server, "PUT", "/data-bucket", b"", &[]).status,
        200
    );

    // Content-MD5 validation.
    let bad_md5 = call(
        &mut f.server,
        "PUT",
        "/data-bucket/a+b/file.txt",
        b"hello xml",
        &[("Content-MD5", "AAAAAAAAAAAAAAAAAAAAAA==")],
    );
    assert_eq!(bad_md5.status, 400);
    assert!(body_text(&bad_md5).contains("BadDigest"));

    let valid_md5 = {
        let hex_digest = md5_etag(b"hello xml");
        let raw = hex::decode(hex_digest.trim_matches('"')).unwrap();
        base64::std_encode(&raw)
    };
    let put = call(
        &mut f.server,
        "PUT",
        "/data-bucket/a+b/file.txt",
        b"hello xml",
        &[
            ("Content-Type", "text/plain"),
            ("Content-MD5", &valid_md5),
            ("x-goog-meta-origin", "xml"),
            ("x-goog-if-generation-match", "0"),
        ],
    );
    assert_eq!(put.status, 200, "{}", body_text(&put));
    let gen = put.headers["X-Goog-Generation"].clone();
    let etag = put.headers["ETag"].clone();

    // If-Generation-Match=0 now fails on overwrite.
    let conflict = call(
        &mut f.server,
        "PUT",
        "/data-bucket/a+b/file.txt",
        b"overwrite",
        &[("x-goog-if-generation-match", "0")],
    );
    assert_eq!(conflict.status, 412);

    // GET + Range + If-None-Match.
    let get = call(&mut f.server, "GET", "/data-bucket/a+b/file.txt", b"", &[]);
    assert_eq!(get.status, 200);
    assert_eq!(get.body, b"hello xml");
    assert_eq!(
        get.headers.get("X-Goog-Meta-Origin").map(String::as_str),
        Some("xml")
    );

    let range = call(
        &mut f.server,
        "GET",
        "/data-bucket/a+b/file.txt",
        b"",
        &[("Range", "bytes=6-8")],
    );
    assert_eq!(range.status, 206);
    assert_eq!(range.body, b"xml");

    let not_mod = call(
        &mut f.server,
        "GET",
        "/data-bucket/a+b/file.txt",
        b"",
        &[("If-None-Match", &etag)],
    );
    assert_eq!(not_mod.status, 304);

    let head = call(&mut f.server, "HEAD", "/data-bucket/a+b/file.txt", b"", &[]);
    assert_eq!(head.status, 200);
    assert!(head.body.is_empty());
    assert_eq!(
        head.headers.get("Content-Length").map(String::as_str),
        Some("9")
    );

    // Interop: JSON API sees the object written by XML API (preserving literal `+`).
    let json_obj = call(
        &mut f.server,
        "GET",
        "/storage/v1/b/data-bucket/o/a%2Bb%2Ffile.txt",
        b"",
        &[],
    );
    assert_eq!(json_obj.status, 200, "{}", body_text(&json_obj));
    let meta: serde_json::Value = serde_json::from_slice(&json_obj.body).unwrap();
    assert_eq!(meta["name"], "a+b/file.txt");
    assert_eq!(meta["generation"], gen);

    // XML Copy with REPLACE metadata directive.
    let copy = call(
        &mut f.server,
        "PUT",
        "/data-bucket/copied.txt",
        b"",
        &[
            ("x-goog-copy-source", "/data-bucket/a+b/file.txt"),
            ("x-goog-copy-source-if-generation-match", &gen),
            ("x-goog-metadata-directive", "REPLACE"),
            ("Content-Type", "text/markdown"),
            ("x-goog-meta-copied", "yes"),
        ],
    );
    assert_eq!(copy.status, 200, "{}", body_text(&copy));
    assert!(body_text(&copy).contains("<CopyObjectResult>"));

    let copied_get = call(&mut f.server, "GET", "/data-bucket/copied.txt", b"", &[]);
    assert_eq!(copied_get.status, 200);
    assert_eq!(copied_get.body, b"hello xml");
    assert_eq!(
        copied_get.headers.get("Content-Type").map(String::as_str),
        Some("text/markdown")
    );
    assert_eq!(
        copied_get
            .headers
            .get("X-Goog-Meta-Copied")
            .map(String::as_str),
        Some("yes")
    );

    // Non-empty bucket delete returns 409 BucketNotEmpty.
    let non_empty = call(&mut f.server, "DELETE", "/data-bucket", b"", &[]);
    assert_eq!(non_empty.status, 409);
    assert!(body_text(&non_empty).contains("BucketNotEmpty"));

    // Delete objects and bucket.
    assert_eq!(
        call(
            &mut f.server,
            "DELETE",
            "/data-bucket/a+b/file.txt",
            b"",
            &[("x-goog-if-generation-match", &gen)],
        )
        .status,
        204
    );
    assert_eq!(
        call(&mut f.server, "DELETE", "/data-bucket/copied.txt", b"", &[]).status,
        204
    );
    assert_eq!(
        call(&mut f.server, "DELETE", "/data-bucket", b"", &[]).status,
        204
    );
}

#[test]
fn xml_list_objects_v1_v2_and_virtual_host_routing() {
    let mut f = Fixture::new();
    assert_eq!(
        call(&mut f.server, "PUT", "/vhost-bucket", b"", &[]).status,
        200
    );

    // Virtual-host style PUT & GET.
    let put = call(
        &mut f.server,
        "PUT",
        "/docs/alpha.txt",
        b"alpha",
        &[("Host", "vhost-bucket.storage.googleapis.com")],
    );
    assert_eq!(put.status, 200);
    let get = call(
        &mut f.server,
        "GET",
        "/docs/alpha.txt",
        b"",
        &[("Host", "vhost-bucket.localhost:14443")],
    );
    assert_eq!(get.status, 200);
    assert_eq!(get.body, b"alpha");

    for (key, body) in [
        ("docs/beta.txt", b"beta".as_slice()),
        ("images/cat.png", b"cat"),
        ("readme.md", b"hi"),
    ] {
        assert_eq!(
            call(
                &mut f.server,
                "PUT",
                &format!("/vhost-bucket/{key}"),
                body,
                &[]
            )
            .status,
            200
        );
    }

    // List v1 with delimiter.
    let list_v1 = call(
        &mut f.server,
        "GET",
        "/vhost-bucket?delimiter=/&max-keys=2",
        b"",
        &[],
    );
    assert_eq!(list_v1.status, 200);
    let xml1 = body_text(&list_v1);
    assert!(xml1.contains("<IsTruncated>true</IsTruncated>"));
    assert!(xml1.contains("<Prefix>docs/</Prefix>"));
    assert!(xml1.contains("<Prefix>images/</Prefix>"));
    assert!(xml1.contains("<NextMarker>images/</NextMarker>"));

    // Page 2 via marker.
    let page2 = call(
        &mut f.server,
        "GET",
        "/vhost-bucket?delimiter=/&marker=images/&max-keys=2",
        b"",
        &[],
    );
    assert_eq!(page2.status, 200);
    let xml2 = body_text(&page2);
    assert!(xml2.contains("<IsTruncated>false</IsTruncated>"));
    assert!(xml2.contains("<Key>readme.md</Key>"));

    // List v2 with prefix and pagination.
    let list_v2 = call(
        &mut f.server,
        "GET",
        "/vhost-bucket?list-type=2&prefix=docs/&max-keys=1",
        b"",
        &[],
    );
    assert_eq!(list_v2.status, 200);
    let v2 = body_text(&list_v2);
    assert!(v2.contains("<KeyCount>1</KeyCount>"));
    assert!(v2.contains("<Key>docs/alpha.txt</Key>"));
    assert!(v2.contains("<NextContinuationToken>docs/alpha.txt</NextContinuationToken>"));

    let list_v2_next = call(
        &mut f.server,
        "GET",
        "/vhost-bucket?list-type=2&prefix=docs/&continuation-token=docs/alpha.txt",
        b"",
        &[],
    );
    assert_eq!(list_v2_next.status, 200);
    let v2n = body_text(&list_v2_next);
    assert!(v2n.contains("<Key>docs/beta.txt</Key>"));
    assert!(v2n.contains("<IsTruncated>false</IsTruncated>"));
}
