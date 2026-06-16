//! Parity tests for the read-only `/_introspect/` API, ported from the routing
//! contract in `internal/services/s3/introspect.rs`. The introspection API is
//! intercepted before provider-protocol dispatch, is GET-only, and returns the
//! dashboard summary JSON shapes.

use devcloud_s3::http::{route, Request, Response};
use devcloud_s3::objops::CreateMultipartUploadInput;
use devcloud_s3::store::FileBucketStore;
use serde_json::Value;

fn tempdir() -> std::path::PathBuf {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let mut dir = std::env::temp_dir();
    dir.push(format!(
        "devcloud-s3-introspect-{}-{}",
        std::process::id(),
        n
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn store() -> FileBucketStore {
    FileBucketStore::new(tempdir())
}

fn req(method: &str, target: &str) -> Request {
    Request::new(method, target, Vec::new())
}

fn put(store: &FileBucketStore, method: &str, target: &str, body: &[u8]) -> Response {
    route(store, &Request::new(method, target, body.to_vec()))
}

fn json(resp: &Response) -> Value {
    serde_json::from_slice(&resp.body).expect("introspect body is JSON")
}

#[test]
fn buckets_listing_reports_object_counts() {
    let store = store();
    put(&store, "PUT", "/alpha", b"");
    put(&store, "PUT", "/beta", b"");
    put(&store, "PUT", "/alpha/one.txt", b"hello");
    put(&store, "PUT", "/alpha/two.txt", b"world");

    let resp = route(&store, &req("GET", "/_introspect/buckets"));
    assert_eq!(resp.status, 200);
    assert_eq!(
        resp.headers.get("Content-Type").map(String::as_str),
        Some("application/json")
    );
    let body = json(&resp);
    let buckets = body["buckets"].as_array().unwrap();
    assert_eq!(buckets.len(), 2);
    // Buckets are returned in store order (alphabetical from list_buckets).
    let alpha = buckets.iter().find(|b| b["name"] == "alpha").unwrap();
    assert_eq!(alpha["objectCount"], 2);
    assert!(alpha["creationDate"].as_str().unwrap().contains('T'));
    let beta = buckets.iter().find(|b| b["name"] == "beta").unwrap();
    assert_eq!(beta["objectCount"], 0);
}

#[test]
fn bucket_detail_returns_summary_and_404_when_absent() {
    let store = store();
    put(&store, "PUT", "/data", b"");
    put(&store, "PUT", "/data/file", b"abc");

    let resp = route(&store, &req("GET", "/_introspect/buckets/data"));
    assert_eq!(resp.status, 200);
    let body = json(&resp);
    assert_eq!(body["name"], "data");
    assert_eq!(body["objectCount"], 1);

    let missing = route(&store, &req("GET", "/_introspect/buckets/nope"));
    assert_eq!(missing.status, 404);
    assert_eq!(json(&missing)["error"], "bucket does not exist");
}

#[test]
fn objects_listing_honors_prefix_and_shapes_summary() {
    let store = store();
    put(&store, "PUT", "/box", b"");
    put(&store, "PUT", "/box/docs/a.txt", b"aaa");
    put(&store, "PUT", "/box/docs/b.txt", b"bb");
    put(&store, "PUT", "/box/img/c.png", b"c");

    let resp = route(
        &store,
        &req("GET", "/_introspect/buckets/box/objects?prefix=docs/"),
    );
    assert_eq!(resp.status, 200);
    let body = json(&resp);
    assert_eq!(body["bucket"], "box");
    assert_eq!(body["prefix"], "docs/");
    let objects = body["objects"].as_array().unwrap();
    assert_eq!(objects.len(), 2);
    let first = &objects[0];
    assert_eq!(first["key"], "docs/a.txt");
    assert_eq!(first["size"], 3);
    assert_eq!(first["s3Uri"], "s3://box/docs/a.txt");
    assert_eq!(
        first["downloadUrl"],
        "/api/s3/buckets/box/objects/docs%2Fa.txt/download"
    );
    assert!(first["etag"].as_str().unwrap().len() > 0);

    let missing = route(&store, &req("GET", "/_introspect/buckets/ghost/objects"));
    assert_eq!(missing.status, 404);
    assert_eq!(json(&missing)["error"], "bucket does not exist");
}

#[test]
fn object_detail_returns_summary_and_404s() {
    let store = store();
    put(&store, "PUT", "/repo", b"");
    put(&store, "PUT", "/repo/path/to/key.bin", b"payload");

    let resp = route(
        &store,
        &req("GET", "/_introspect/buckets/repo/objects/path/to/key.bin"),
    );
    assert_eq!(resp.status, 200);
    let body = json(&resp);
    assert_eq!(body["key"], "path/to/key.bin");
    assert_eq!(body["size"], 7);
    assert_eq!(body["s3Uri"], "s3://repo/path/to/key.bin");
    // Body content is never leaked: only metadata fields are present.
    assert!(body.get("body").is_none());

    let missing_key = route(
        &store,
        &req("GET", "/_introspect/buckets/repo/objects/absent"),
    );
    assert_eq!(missing_key.status, 404);
    assert_eq!(json(&missing_key)["error"], "object does not exist");

    // legacy parity: object-detail on a missing bucket surfaces the store error as
    // a 500 ("bucket does not exist"), not a 404 — the error branch is checked
    // before the not-found branch in the legacy handler.
    let missing_bucket = route(&store, &req("GET", "/_introspect/buckets/ghost/objects/x"));
    assert_eq!(missing_bucket.status, 500);
    assert_eq!(json(&missing_bucket)["error"], "bucket does not exist");
}

#[test]
fn multipart_listing_reports_uploads() {
    let store = store();
    put(&store, "PUT", "/mpbucket", b"");
    store
        .create_multipart_upload(CreateMultipartUploadInput {
            bucket: "mpbucket".to_string(),
            key: "big.bin".to_string(),
            ..Default::default()
        })
        .unwrap();

    let resp = route(
        &store,
        &req("GET", "/_introspect/buckets/mpbucket/multipart"),
    );
    assert_eq!(resp.status, 200);
    let body = json(&resp);
    assert_eq!(body["bucket"], "mpbucket");
    let uploads = body["uploads"].as_array().unwrap();
    assert_eq!(uploads.len(), 1);
    assert_eq!(uploads[0]["key"], "big.bin");
    assert!(uploads[0]["uploadId"].as_str().unwrap().len() > 0);

    let missing = route(&store, &req("GET", "/_introspect/buckets/ghost/multipart"));
    assert_eq!(missing.status, 404);
}

#[test]
fn non_get_methods_are_rejected() {
    let store = store();
    let resp = route(&store, &req("POST", "/_introspect/buckets"));
    assert_eq!(resp.status, 405);
    assert_eq!(resp.headers.get("Allow").map(String::as_str), Some("GET"));
    assert_eq!(
        json(&resp)["error"],
        "introspection endpoints are read-only"
    );
}

#[test]
fn unknown_introspect_paths_return_404() {
    let store = store();
    let resp = route(&store, &req("GET", "/_introspect/widgets"));
    assert_eq!(resp.status, 404);
    assert_eq!(json(&resp)["error"], "introspection endpoint not found");
}

#[test]
fn provider_protocol_paths_are_unaffected() {
    let store = store();
    put(&store, "PUT", "/regular", b"");
    // A normal bucket HEAD still routes through the provider dispatch.
    let resp = route(&store, &req("HEAD", "/regular"));
    assert_eq!(resp.status, 200);
    // A bucket literally named like the prefix segment is NOT intercepted,
    // because the prefix requires a trailing slash ("/_introspect/").
    let resp = route(&store, &req("HEAD", "/_introspect"));
    // "_introspect" is an invalid bucket name -> provider dispatch handles it.
    assert!(resp.status == 400 || resp.status == 404);
}
