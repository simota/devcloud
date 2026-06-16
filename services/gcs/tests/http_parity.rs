//! 1:1 port of the legacy GCS test suite (`internal/services/gcs/*_test.rs`).
//! Each test function mirrors one legacy test function — same scenario, same
//! expected statuses/headers/JSON. The legacy implementation is the oracle.

use std::fs;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use devcloud_gcs::http::{route, Config, Request, Response, Server};
use devcloud_s3::store::FileBucketStore;

// Per-process monotonic counter so two parallel tests can never land on the same
// temp dir even if `SystemTime::now()` returns the same tick (the nanos+pid combo
// alone can collide under high test parallelism, causing shared on-disk store
// state and flaky failures like bucket_delete_requires_empty_bucket).
static TEMP_SEQ: AtomicU64 = AtomicU64::new(0);

fn temp_root(tag: &str) -> std::path::PathBuf {
    let root = std::env::temp_dir().join(format!(
        "devcloud-gcs-test-{tag}-{}-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
        TEMP_SEQ.fetch_add(1, Ordering::Relaxed)
    ));
    let _ = fs::remove_dir_all(&root);
    root
}

fn server() -> Server {
    Server::new(Config::default(), FileBucketStore::new(temp_root("store")))
}

/// Mirrors the legacy `performRequestWithHeaders` helper: a JSON content type is
/// assumed for non-empty bodies, custom headers override it.
fn perform_h(
    s: &mut Server,
    method: &str,
    target: &str,
    body: &str,
    headers: &[(&str, &str)],
) -> Response {
    let mut request = Request::new(method, target, body.as_bytes().to_vec());
    if !body.is_empty() {
        request
            .headers
            .insert("content-type".to_string(), "application/json".to_string());
    }
    for (key, value) in headers {
        request
            .headers
            .insert(key.to_ascii_lowercase(), value.to_string());
    }
    route(s, &request)
}

fn perform(s: &mut Server, method: &str, target: &str, body: &str) -> Response {
    perform_h(s, method, target, body, &[])
}

fn json_body(resp: &Response) -> serde_json::Value {
    serde_json::from_slice(&resp.body)
        .unwrap_or_else(|e| panic!("decode body {e}: {}", String::from_utf8_lossy(&resp.body)))
}

fn body_str(resp: &Response) -> String {
    String::from_utf8_lossy(&resp.body).into_owned()
}

fn header<'a>(resp: &'a Response, name: &str) -> &'a str {
    resp.headers.get(name).map(String::as_str).unwrap_or("")
}

fn create_bucket(s: &mut Server, name: &str) {
    let resp = perform(
        s,
        "POST",
        "/storage/v1/b?project=devcloud",
        &format!(r#"{{"name":"{name}"}}"#),
    );
    assert_eq!(
        resp.status,
        200,
        "create bucket {name}: {}",
        body_str(&resp)
    );
}

fn upload_with(
    s: &mut Server,
    bucket: &str,
    name: &str,
    body: &str,
    headers: &[(&str, &str)],
) -> serde_json::Value {
    let target = format!("/upload/storage/v1/b/{bucket}/o?uploadType=media&name={name}");
    let resp = perform_h(s, "POST", &target, body, headers);
    assert_eq!(resp.status, 200, "upload {name}: {}", body_str(&resp));
    json_body(&resp)
}

fn upload_text(s: &mut Server, bucket: &str, name: &str, body: &str) -> serde_json::Value {
    upload_with(s, bucket, name, body, &[("Content-Type", "text/plain")])
}

// ---------------------------------------------------------------------------
// bucket_test.rs
// ---------------------------------------------------------------------------

/// legacy: TestBucketLifecycleAndListBuckets
#[test]
fn bucket_lifecycle_and_list_buckets() {
    let mut s = Server::new(
        Config {
            project: "devcloud".to_string(),
            location: "US".to_string(),
            ..Default::default()
        },
        FileBucketStore::new(temp_root("bucket-lifecycle")),
    );

    let create = perform(
        &mut s,
        "POST",
        "/storage/v1/b?project=devcloud",
        r#"{"name":"demo-bucket","location":"US","storageClass":"STANDARD"}"#,
    );
    assert_eq!(create.status, 200, "{}", body_str(&create));
    let created = json_body(&create);
    assert_eq!(created["name"], "demo-bucket");
    assert_eq!(created["kind"], "storage#bucket");
    assert_eq!(created["storageClass"], "STANDARD");
    created["projectNumber"]
        .as_str()
        .unwrap()
        .parse::<u64>()
        .expect("projectNumber must be a uint64 string");

    let get = perform(&mut s, "GET", "/storage/v1/b/demo-bucket", "");
    assert_eq!(get.status, 200, "{}", body_str(&get));
    assert_eq!(json_body(&get)["name"], "demo-bucket");

    let list = perform(&mut s, "GET", "/storage/v1/b?project=devcloud", "");
    assert_eq!(list.status, 200, "{}", body_str(&list));
    let listed = json_body(&list);
    let items = listed["items"].as_array().unwrap();
    assert_eq!(items.len(), 1);
    assert_eq!(items[0]["name"], "demo-bucket");

    let delete = perform(&mut s, "DELETE", "/storage/v1/b/demo-bucket", "");
    assert_eq!(delete.status, 204, "{}", body_str(&delete));

    let missing = perform(&mut s, "GET", "/storage/v1/b/demo-bucket", "");
    assert_eq!(missing.status, 404);
}

/// legacy: TestBucketInsertRejectsDuplicateAndInvalidJSON
#[test]
fn bucket_insert_rejects_duplicate_and_invalid_json() {
    let mut s = server();

    let create = perform(
        &mut s,
        "POST",
        "/storage/v1/b?project=devcloud",
        r#"{"name":"demo-bucket"}"#,
    );
    assert_eq!(create.status, 200, "{}", body_str(&create));

    let duplicate = perform(
        &mut s,
        "POST",
        "/storage/v1/b?project=devcloud",
        r#"{"name":"demo-bucket"}"#,
    );
    assert_eq!(duplicate.status, 409, "{}", body_str(&duplicate));

    let invalid = perform(
        &mut s,
        "POST",
        "/storage/v1/b?project=devcloud",
        r#"{"name":"#,
    );
    assert_eq!(invalid.status, 400, "{}", body_str(&invalid));
}

/// legacy: TestBucketsListSupportsPagination
#[test]
fn buckets_list_supports_pagination() {
    let mut s = server();
    for name in ["alpha-bucket", "beta-bucket", "gamma-bucket"] {
        create_bucket(&mut s, name);
    }

    let first_page = perform(
        &mut s,
        "GET",
        "/storage/v1/b?project=devcloud&maxResults=2",
        "",
    );
    assert_eq!(first_page.status, 200, "{}", body_str(&first_page));
    let first = json_body(&first_page);
    let items = first["items"].as_array().unwrap();
    assert_eq!(items.len(), 2);
    assert_eq!(items[0]["name"], "alpha-bucket");
    assert_eq!(items[1]["name"], "beta-bucket");
    let token = first["nextPageToken"].as_str().unwrap();
    assert!(!token.is_empty());

    let second_page = perform(
        &mut s,
        "GET",
        &format!("/storage/v1/b?project=devcloud&pageToken={token}"),
        "",
    );
    assert_eq!(second_page.status, 200, "{}", body_str(&second_page));
    let second = json_body(&second_page);
    let items = second["items"].as_array().unwrap();
    assert_eq!(items.len(), 1);
    assert_eq!(items[0]["name"], "gamma-bucket");
    assert!(second.get("nextPageToken").is_none());

    let invalid = perform(
        &mut s,
        "GET",
        "/storage/v1/b?project=devcloud&pageToken=bad",
        "",
    );
    assert_eq!(invalid.status, 400, "{}", body_str(&invalid));
}

/// legacy: TestBucketsListSupportsPrefixFilter
#[test]
fn buckets_list_supports_prefix_filter() {
    let mut s = server();
    for name in ["app-assets", "app-logs", "backup-archive"] {
        create_bucket(&mut s, name);
    }

    let list = perform(
        &mut s,
        "GET",
        "/storage/v1/b?project=devcloud&prefix=app-",
        "",
    );
    assert_eq!(list.status, 200, "{}", body_str(&list));
    let listed = json_body(&list);
    let items = listed["items"].as_array().unwrap();
    assert_eq!(items.len(), 2);
    assert_eq!(items[0]["name"], "app-assets");
    assert_eq!(items[1]["name"], "app-logs");
}

/// legacy: TestBucketDeleteRequiresEmptyBucket
#[test]
fn bucket_delete_requires_empty_bucket() {
    let mut s = server();
    create_bucket(&mut s, "demo-bucket");
    upload_text(&mut s, "demo-bucket", "object.txt", "body");

    let delete = perform(&mut s, "DELETE", "/storage/v1/b/demo-bucket", "");
    assert_eq!(delete.status, 409, "{}", body_str(&delete));
}

// ---------------------------------------------------------------------------
// copy_test.rs
// ---------------------------------------------------------------------------

/// legacy: TestObjectCopyEnforcesSourcePreconditions
#[test]
fn object_copy_enforces_source_preconditions() {
    let mut s = server();
    create_bucket(&mut s, "demo-bucket");
    let uploaded = upload_text(&mut s, "demo-bucket", "docs/source.txt", "copy source");
    let generation = uploaded["generation"].as_str().unwrap().to_string();

    let mismatch = perform(
        &mut s,
        "POST",
        "/storage/v1/b/demo-bucket/o/docs%2Fsource.txt/copyTo/b/demo-bucket/o/docs%2Fcopy.txt?ifSourceGenerationMatch=999999",
        "{}",
    );
    assert_eq!(mismatch.status, 412, "{}", body_str(&mismatch));

    let copy = perform(
        &mut s,
        "POST",
        &format!("/storage/v1/b/demo-bucket/o/docs%2Fsource.txt/copyTo/b/demo-bucket/o/docs%2Fcopy.txt?ifSourceGenerationMatch={generation}&ifSourceMetagenerationMatch=1"),
        "{}",
    );
    assert_eq!(copy.status, 200, "{}", body_str(&copy));
    assert_eq!(json_body(&copy)["name"], "docs/copy.txt");
}

/// legacy: TestObjectCopyUsesDestinationMetadataWhenProvided
#[test]
fn object_copy_uses_destination_metadata_when_provided() {
    let mut s = server();
    create_bucket(&mut s, "demo-bucket");
    upload_with(
        &mut s,
        "demo-bucket",
        "docs/source.txt",
        "copy source",
        &[
            ("Content-Type", "text/plain"),
            ("x-goog-meta-source", "original"),
        ],
    );

    let copy = perform(
        &mut s,
        "POST",
        "/storage/v1/b/demo-bucket/o/docs%2Fsource.txt/copyTo/b/demo-bucket/o/docs%2Fcopy.txt",
        r#"{"contentType":"text/markdown","cacheControl":"no-cache","contentDisposition":"inline","metadata":{"source":"copy-request","owner":"gcs"}}"#,
    );
    assert_eq!(copy.status, 200, "{}", body_str(&copy));
    let copied = json_body(&copy);
    assert_eq!(copied["contentType"], "text/markdown");
    assert_eq!(copied["cacheControl"], "no-cache");
    assert_eq!(copied["contentDisposition"], "inline");
    assert_eq!(copied["metadata"]["source"], "copy-request");
    assert_eq!(copied["metadata"]["owner"], "gcs");

    let download = perform(
        &mut s,
        "GET",
        "/download/storage/v1/b/demo-bucket/o/docs%2Fcopy.txt?alt=media",
        "",
    );
    assert_eq!(download.status, 200, "{}", body_str(&download));
    assert_eq!(download.body, b"copy source");
    assert_eq!(header(&download, "Content-Type"), "text/markdown");
}

/// legacy: TestObjectRewriteCopiesExistingObject
#[test]
fn object_rewrite_copies_existing_object() {
    let mut s = server();
    create_bucket(&mut s, "demo-bucket");
    let uploaded = upload_with(
        &mut s,
        "demo-bucket",
        "docs/source.txt",
        "rewrite source",
        &[
            ("Content-Type", "text/plain"),
            ("x-goog-meta-source", "rewrite-test"),
        ],
    );
    let generation = uploaded["generation"].as_str().unwrap().to_string();

    let rewrite = perform(
        &mut s,
        "POST",
        &format!("/storage/v1/b/demo-bucket/o/docs%2Fsource.txt/rewriteTo/b/demo-bucket/o/docs%2Frewrite.txt?ifSourceGenerationMatch={generation}"),
        "{}",
    );
    assert_eq!(rewrite.status, 200, "{}", body_str(&rewrite));
    let response = json_body(&rewrite);
    assert_eq!(response["kind"], "storage#rewriteResponse");
    assert_eq!(response["done"], true);
    assert_eq!(response["resource"]["name"], "docs/rewrite.txt");
    assert_eq!(response["totalBytesRewritten"], "14");
    assert_eq!(response["objectSize"], "14");
    assert_eq!(response["resource"]["contentType"], "text/plain");
    assert_eq!(response["resource"]["metadata"]["source"], "rewrite-test");

    let download = perform(
        &mut s,
        "GET",
        "/download/storage/v1/b/demo-bucket/o/docs%2Frewrite.txt?alt=media",
        "",
    );
    assert_eq!(download.status, 200, "{}", body_str(&download));
    assert_eq!(download.body, b"rewrite source");
}

/// legacy: TestObjectComposeConcatenatesSources
#[test]
fn object_compose_concatenates_sources() {
    let mut s = server();
    create_bucket(&mut s, "demo-bucket");
    let first = upload_text(&mut s, "demo-bucket", "parts/one.txt", "hello ");
    let generation = first["generation"].as_str().unwrap().to_string();
    upload_text(&mut s, "demo-bucket", "parts/two.txt", "compose");

    let compose = perform(
        &mut s,
        "POST",
        "/storage/v1/b/demo-bucket/o/parts%2Fjoined.txt/compose",
        &format!(
            r#"{{"sourceObjects":[{{"name":"parts/one.txt","generation":"{generation}"}},{{"name":"parts/two.txt"}}],"destination":{{"contentType":"text/plain","metadata":{{"source":"compose-test"}}}}}}"#
        ),
    );
    assert_eq!(compose.status, 200, "{}", body_str(&compose));
    let composed = json_body(&compose);
    assert_eq!(composed["name"], "parts/joined.txt");
    assert_eq!(composed["contentType"], "text/plain");
    assert_eq!(composed["metadata"]["source"], "compose-test");

    let download = perform(
        &mut s,
        "GET",
        "/download/storage/v1/b/demo-bucket/o/parts%2Fjoined.txt?alt=media",
        "",
    );
    assert_eq!(download.status, 200, "{}", body_str(&download));
    assert_eq!(download.body, b"hello compose");
}

/// legacy: TestObjectComposeRejectsSourceGenerationMismatch
#[test]
fn object_compose_rejects_source_generation_mismatch() {
    let mut s = server();
    create_bucket(&mut s, "demo-bucket");
    upload_text(&mut s, "demo-bucket", "parts/source.txt", "body");

    let compose = perform(
        &mut s,
        "POST",
        "/storage/v1/b/demo-bucket/o/parts%2Fjoined.txt/compose",
        r#"{"sourceObjects":[{"name":"parts/source.txt","objectPreconditions":{"ifGenerationMatch":"999999"}}]}"#,
    );
    assert_eq!(compose.status, 412, "{}", body_str(&compose));

    let missing = perform(
        &mut s,
        "GET",
        "/storage/v1/b/demo-bucket/o/parts%2Fjoined.txt",
        "",
    );
    assert_eq!(missing.status, 404, "{}", body_str(&missing));
}

// ---------------------------------------------------------------------------
// object_test.rs
// ---------------------------------------------------------------------------

/// legacy: TestObjectMediaLifecycleListRangeAndCopy
#[test]
fn object_media_lifecycle_list_range_and_copy() {
    let mut s = server();
    create_bucket(&mut s, "demo-bucket");

    let uploaded = upload_with(
        &mut s,
        "demo-bucket",
        "docs/readme.txt",
        "hello from devcloud gcs\n",
        &[
            ("Content-Type", "text/plain"),
            ("x-goog-meta-source", "unit-test"),
        ],
    );
    assert_eq!(uploaded["name"], "docs/readme.txt");
    assert!(!uploaded["generation"].as_str().unwrap().is_empty());
    assert_eq!(uploaded["metageneration"], "1");
    assert_eq!(uploaded["metadata"]["source"], "unit-test");

    let metadata = perform(
        &mut s,
        "GET",
        "/storage/v1/b/demo-bucket/o/docs%2Freadme.txt",
        "",
    );
    assert_eq!(metadata.status, 200, "{}", body_str(&metadata));
    let got = json_body(&metadata);
    assert_eq!(got["name"], "docs/readme.txt");
    assert!(!got["md5Hash"].as_str().unwrap().is_empty());
    assert!(!got["crc32c"].as_str().unwrap().is_empty());

    let download = perform(
        &mut s,
        "GET",
        "/download/storage/v1/b/demo-bucket/o/docs%2Freadme.txt?alt=media",
        "",
    );
    assert_eq!(download.status, 200, "{}", body_str(&download));
    assert_eq!(download.body, b"hello from devcloud gcs\n");

    let json_alt = perform(
        &mut s,
        "GET",
        "/storage/v1/b/demo-bucket/o/docs%2Freadme.txt?alt=media",
        "",
    );
    assert_eq!(json_alt.status, 200, "{}", body_str(&json_alt));
    assert_eq!(json_alt.body, b"hello from devcloud gcs\n");
    assert_eq!(header(&json_alt, "Content-Type"), "text/plain");

    let range = perform_h(
        &mut s,
        "GET",
        "/download/storage/v1/b/demo-bucket/o/docs%2Freadme.txt?alt=media",
        "",
        &[("Range", "bytes=0-4")],
    );
    assert_eq!(range.status, 206, "{}", body_str(&range));
    assert_eq!(range.body, b"hello");

    let unsatisfiable = perform_h(
        &mut s,
        "GET",
        "/download/storage/v1/b/demo-bucket/o/docs%2Freadme.txt?alt=media",
        "",
        &[("Range", "bytes=999-1000")],
    );
    assert_eq!(unsatisfiable.status, 416, "{}", body_str(&unsatisfiable));
    assert_eq!(header(&unsatisfiable, "Content-Range"), "bytes */24");

    let list = perform(
        &mut s,
        "GET",
        "/storage/v1/b/demo-bucket/o?prefix=docs/",
        "",
    );
    assert_eq!(list.status, 200, "{}", body_str(&list));
    let listed = json_body(&list);
    let items = listed["items"].as_array().unwrap();
    assert_eq!(items.len(), 1);
    assert_eq!(items[0]["name"], "docs/readme.txt");

    let copy = perform(
        &mut s,
        "POST",
        "/storage/v1/b/demo-bucket/o/docs%2Freadme.txt/copyTo/b/demo-bucket/o/docs%2Fcopy.txt",
        "{}",
    );
    assert_eq!(copy.status, 200, "{}", body_str(&copy));
    assert_eq!(json_body(&copy)["name"], "docs/copy.txt");

    let delete = perform(
        &mut s,
        "DELETE",
        "/storage/v1/b/demo-bucket/o/docs%2Fcopy.txt",
        "",
    );
    assert_eq!(delete.status, 204, "{}", body_str(&delete));
}

/// legacy: TestObjectHeadReturnsGCSMetadataHeadersWithoutBody
#[test]
fn object_head_returns_gcs_metadata_headers_without_body() {
    let mut s = server();
    create_bucket(&mut s, "demo-bucket");
    upload_with(
        &mut s,
        "demo-bucket",
        "docs/head.txt",
        "hello metadata",
        &[
            ("Content-Type", "text/plain"),
            ("Cache-Control", "no-cache"),
            ("Content-Disposition", "inline"),
            ("x-goog-meta-source", "head-test"),
        ],
    );

    let metadata = perform(
        &mut s,
        "HEAD",
        "/storage/v1/b/demo-bucket/o/docs%2Fhead.txt",
        "",
    );
    assert_eq!(metadata.status, 200, "{}", body_str(&metadata));
    assert!(metadata.body.is_empty(), "HEAD body must be empty");
    for (key, want) in [
        ("X-Goog-Metageneration", "1"),
        ("X-Goog-Stored-Content-Length", "14"),
        ("X-Goog-Meta-Source", "head-test"),
        ("Cache-Control", "no-cache"),
        ("Content-Disposition", "inline"),
    ] {
        assert_eq!(header(&metadata, key), want, "header {key}");
    }
    assert!(!header(&metadata, "X-Goog-Generation").is_empty());
    assert!(!header(&metadata, "ETag").is_empty());
    let hash = header(&metadata, "X-Goog-Hash");
    assert!(
        hash.contains("crc32c=") && hash.contains("md5="),
        "X-Goog-Hash = {hash}"
    );

    let download = perform(
        &mut s,
        "HEAD",
        "/download/storage/v1/b/demo-bucket/o/docs%2Fhead.txt?alt=media",
        "",
    );
    assert_eq!(download.status, 200, "{}", body_str(&download));
    assert!(download.body.is_empty());
    assert_eq!(header(&download, "Content-Length"), "14");
    assert_eq!(header(&download, "Content-Type"), "text/plain");

    let range = perform_h(
        &mut s,
        "HEAD",
        "/download/storage/v1/b/demo-bucket/o/docs%2Fhead.txt?alt=media",
        "",
        &[("Range", "bytes=0-4")],
    );
    assert_eq!(range.status, 206, "{}", body_str(&range));
    assert_eq!(header(&range, "Content-Range"), "bytes 0-4/14");
    assert_eq!(header(&range, "Content-Length"), "5");
}

/// legacy: TestObjectsListSupportsDelimiterPrefixes
#[test]
fn objects_list_supports_delimiter_prefixes() {
    let mut s = server();
    create_bucket(&mut s, "demo-bucket");
    for (name, body) in [
        ("docs/readme.txt", "readme"),
        ("docs/guides/setup.txt", "setup"),
        ("docs/guides/run.txt", "run"),
        ("docs/archive.zip", "archive"),
    ] {
        upload_text(&mut s, "demo-bucket", name, body);
    }

    let list = perform(
        &mut s,
        "GET",
        "/storage/v1/b/demo-bucket/o?prefix=docs/&delimiter=/",
        "",
    );
    assert_eq!(list.status, 200, "{}", body_str(&list));
    let listed = json_body(&list);
    let items = listed["items"].as_array().unwrap();
    assert_eq!(items.len(), 2, "items = {items:?}");
    let names: Vec<&str> = items.iter().map(|i| i["name"].as_str().unwrap()).collect();
    assert!(names.contains(&"docs/archive.zip") && names.contains(&"docs/readme.txt"));
    let prefixes = listed["prefixes"].as_array().unwrap();
    assert_eq!(prefixes.len(), 1);
    assert_eq!(prefixes[0], "docs/guides/");
}

/// legacy: TestObjectsListSupportsIncludeTrailingDelimiter
#[test]
fn objects_list_supports_include_trailing_delimiter() {
    let mut s = server();
    create_bucket(&mut s, "demo-bucket");
    for (name, body) in [
        ("docs/folder/", ""),
        ("docs/folder/readme.md", "readme"),
        ("docs/readme.txt", "readme"),
    ] {
        upload_text(&mut s, "demo-bucket", name, body);
    }

    let without_marker = perform(
        &mut s,
        "GET",
        "/storage/v1/b/demo-bucket/o?prefix=docs/&delimiter=/",
        "",
    );
    assert_eq!(without_marker.status, 200, "{}", body_str(&without_marker));
    let default_list = json_body(&without_marker);
    let items = default_list["items"].as_array().unwrap();
    assert_eq!(items.len(), 1, "items = {items:?}");
    assert_eq!(items[0]["name"], "docs/readme.txt");
    let prefixes = default_list["prefixes"].as_array().unwrap();
    assert_eq!(prefixes.len(), 1);
    assert_eq!(prefixes[0], "docs/folder/");

    let with_marker = perform(
        &mut s,
        "GET",
        "/storage/v1/b/demo-bucket/o?prefix=docs/&delimiter=/&includeTrailingDelimiter=true",
        "",
    );
    assert_eq!(with_marker.status, 200, "{}", body_str(&with_marker));
    let listed = json_body(&with_marker);
    let items = listed["items"].as_array().unwrap();
    assert_eq!(items.len(), 2, "items = {items:?}");
    assert_eq!(items[0]["name"], "docs/folder/");
    assert_eq!(items[1]["name"], "docs/readme.txt");
    let prefixes = listed["prefixes"].as_array().unwrap();
    assert_eq!(prefixes.len(), 1);
    assert_eq!(prefixes[0], "docs/folder/");
}

/// legacy: TestObjectsListSupportsPaginationAcrossItemsAndPrefixes
#[test]
fn objects_list_supports_pagination_across_items_and_prefixes() {
    let mut s = server();
    create_bucket(&mut s, "demo-bucket");
    for (name, body) in [
        ("docs/a.txt", "a"),
        ("docs/b/file.txt", "b"),
        ("docs/c/file.txt", "c"),
        ("docs/d.txt", "d"),
    ] {
        upload_text(&mut s, "demo-bucket", name, body);
    }

    let first_page = perform(
        &mut s,
        "GET",
        "/storage/v1/b/demo-bucket/o?prefix=docs/&delimiter=/&maxResults=2",
        "",
    );
    assert_eq!(first_page.status, 200, "{}", body_str(&first_page));
    let first = json_body(&first_page);
    assert_eq!(first["items"].as_array().unwrap().len(), 1);
    assert_eq!(first["items"][0]["name"], "docs/a.txt");
    assert_eq!(first["prefixes"].as_array().unwrap().len(), 1);
    assert_eq!(first["prefixes"][0], "docs/b/");
    let token = first["nextPageToken"].as_str().unwrap();
    assert!(!token.is_empty());

    let second_page = perform(
        &mut s,
        "GET",
        &format!("/storage/v1/b/demo-bucket/o?prefix=docs/&delimiter=/&pageToken={token}"),
        "",
    );
    assert_eq!(second_page.status, 200, "{}", body_str(&second_page));
    let second = json_body(&second_page);
    assert_eq!(second["items"].as_array().unwrap().len(), 1);
    assert_eq!(second["items"][0]["name"], "docs/d.txt");
    assert_eq!(second["prefixes"].as_array().unwrap().len(), 1);
    assert_eq!(second["prefixes"][0], "docs/c/");
    assert!(second.get("nextPageToken").is_none());
}

/// legacy: TestObjectsListSupportsStartAndEndOffset
#[test]
fn objects_list_supports_start_and_end_offset() {
    let mut s = server();
    create_bucket(&mut s, "demo-bucket");
    for name in ["docs/a.txt", "docs/b.txt", "docs/c/file.txt", "docs/d.txt"] {
        upload_text(&mut s, "demo-bucket", name, "body");
    }

    let list = perform(
        &mut s,
        "GET",
        "/storage/v1/b/demo-bucket/o?prefix=docs/&startOffset=docs/b.txt&endOffset=docs/d.txt",
        "",
    );
    assert_eq!(list.status, 200, "{}", body_str(&list));
    let listed = json_body(&list);
    let items = listed["items"].as_array().unwrap();
    assert_eq!(items.len(), 2, "items = {items:?}");
    assert_eq!(items[0]["name"], "docs/b.txt");
    assert_eq!(items[1]["name"], "docs/c/file.txt");

    let delimited = perform(
        &mut s,
        "GET",
        "/storage/v1/b/demo-bucket/o?prefix=docs/&delimiter=/&startOffset=docs/b.txt&endOffset=docs/d.txt",
        "",
    );
    assert_eq!(delimited.status, 200, "{}", body_str(&delimited));
    let delimited_list = json_body(&delimited);
    let items = delimited_list["items"].as_array().unwrap();
    assert_eq!(items.len(), 1, "items = {items:?}");
    assert_eq!(items[0]["name"], "docs/b.txt");
    let prefixes = delimited_list["prefixes"].as_array().unwrap();
    assert_eq!(prefixes.len(), 1);
    assert_eq!(prefixes[0], "docs/c/");
}

/// legacy: TestObjectPatchUpdatesMetadataAndMetageneration
#[test]
fn object_patch_updates_metadata_and_metageneration() {
    let mut s = server();
    create_bucket(&mut s, "demo-bucket");
    let uploaded = upload_with(
        &mut s,
        "demo-bucket",
        "docs/metadata.txt",
        "body",
        &[
            ("Content-Type", "text/plain"),
            ("x-goog-meta-source", "initial"),
        ],
    );

    let patch = perform(
        &mut s,
        "PATCH",
        "/storage/v1/b/demo-bucket/o/docs%2Fmetadata.txt?ifMetagenerationMatch=1",
        r#"{"contentType":"text/markdown","cacheControl":"no-cache","metadata":{"source":"patched","owner":"gcs"}}"#,
    );
    assert_eq!(patch.status, 200, "{}", body_str(&patch));
    let patched = json_body(&patch);
    assert_eq!(patched["generation"], uploaded["generation"]);
    assert_eq!(patched["metageneration"], "2");
    assert_eq!(patched["contentType"], "text/markdown");
    assert_eq!(patched["cacheControl"], "no-cache");
    assert_eq!(patched["metadata"]["source"], "patched");
    assert_eq!(patched["metadata"]["owner"], "gcs");

    let mismatch = perform(
        &mut s,
        "PATCH",
        "/storage/v1/b/demo-bucket/o/docs%2Fmetadata.txt?ifMetagenerationMatch=1",
        r#"{"metadata":{"source":"stale"}}"#,
    );
    assert_eq!(mismatch.status, 412, "{}", body_str(&mismatch));

    let download = perform(
        &mut s,
        "GET",
        "/download/storage/v1/b/demo-bucket/o/docs%2Fmetadata.txt?alt=media",
        "",
    );
    assert_eq!(download.status, 200, "{}", body_str(&download));
    assert_eq!(download.body, b"body");
    assert_eq!(header(&download, "Content-Type"), "text/markdown");
}

// ---------------------------------------------------------------------------
// preconditions_test.rs
// ---------------------------------------------------------------------------

/// legacy: TestObjectInsertRejectsGenerationPreconditionMismatch
#[test]
fn object_insert_rejects_generation_precondition_mismatch() {
    let mut s = server();
    create_bucket(&mut s, "demo-bucket");

    let mismatch = perform_h(
        &mut s,
        "POST",
        "/upload/storage/v1/b/demo-bucket/o?uploadType=media&name=docs/precondition.txt&ifGenerationMatch=999999",
        "mismatch\n",
        &[("Content-Type", "text/plain")],
    );
    assert_eq!(mismatch.status, 412, "{}", body_str(&mismatch));

    let create_only = perform_h(
        &mut s,
        "POST",
        "/upload/storage/v1/b/demo-bucket/o?uploadType=media&name=docs/precondition.txt&ifGenerationMatch=0",
        "created\n",
        &[("Content-Type", "text/plain")],
    );
    assert_eq!(create_only.status, 200, "{}", body_str(&create_only));
}

/// legacy: TestObjectReadDownloadAndDeleteRejectGenerationPreconditionMismatch
#[test]
fn object_read_download_and_delete_reject_generation_precondition_mismatch() {
    let mut s = server();
    create_bucket(&mut s, "demo-bucket");
    let uploaded = upload_text(&mut s, "demo-bucket", "docs/precondition-read.txt", "body");
    let generation = uploaded["generation"].as_str().unwrap().to_string();

    for (name, method, target) in [
        (
            "metadata",
            "GET",
            "/storage/v1/b/demo-bucket/o/docs%2Fprecondition-read.txt?ifGenerationMatch=999999",
        ),
        (
            "download",
            "GET",
            "/download/storage/v1/b/demo-bucket/o/docs%2Fprecondition-read.txt?alt=media&ifGenerationMatch=999999",
        ),
        (
            "delete",
            "DELETE",
            "/storage/v1/b/demo-bucket/o/docs%2Fprecondition-read.txt?ifGenerationMatch=999999",
        ),
    ] {
        let rec = perform(&mut s, method, target, "");
        assert_eq!(rec.status, 412, "{name}: {}", body_str(&rec));
    }

    let metadata = perform(
        &mut s,
        "GET",
        &format!("/storage/v1/b/demo-bucket/o/docs%2Fprecondition-read.txt?ifGenerationMatch={generation}"),
        "",
    );
    assert_eq!(metadata.status, 200, "{}", body_str(&metadata));

    let delete = perform(
        &mut s,
        "DELETE",
        &format!("/storage/v1/b/demo-bucket/o/docs%2Fprecondition-read.txt?ifGenerationMatch={generation}"),
        "",
    );
    assert_eq!(delete.status, 204, "{}", body_str(&delete));
}

/// legacy: TestObjectOperationsRejectInvalidPreconditionValues
#[test]
fn object_operations_reject_invalid_precondition_values() {
    let mut s = server();
    create_bucket(&mut s, "demo-bucket");
    upload_text(&mut s, "demo-bucket", "docs/source.txt", "body");

    for (name, method, target, body) in [
        (
            "media upload invalid generation match",
            "POST",
            "/upload/storage/v1/b/demo-bucket/o?uploadType=media&name=docs/new.txt&ifGenerationMatch=bad",
            "new body",
        ),
        (
            "metadata invalid metageneration match",
            "GET",
            "/storage/v1/b/demo-bucket/o/docs%2Fsource.txt?ifMetagenerationMatch=bad",
            "",
        ),
        (
            "download invalid generation not match",
            "GET",
            "/download/storage/v1/b/demo-bucket/o/docs%2Fsource.txt?alt=media&ifGenerationNotMatch=bad",
            "",
        ),
        (
            "patch invalid metageneration not match",
            "PATCH",
            "/storage/v1/b/demo-bucket/o/docs%2Fsource.txt?ifMetagenerationNotMatch=bad",
            r#"{"metadata":{"source":"invalid"}}"#,
        ),
        (
            "delete invalid generation match",
            "DELETE",
            "/storage/v1/b/demo-bucket/o/docs%2Fsource.txt?ifGenerationMatch=bad",
            "",
        ),
        (
            "copy invalid source metageneration",
            "POST",
            "/storage/v1/b/demo-bucket/o/docs%2Fsource.txt/copyTo/b/demo-bucket/o/docs%2Fcopy.txt?ifSourceMetagenerationMatch=bad",
            "{}",
        ),
        (
            "compose invalid destination generation",
            "POST",
            "/storage/v1/b/demo-bucket/o/docs%2Fjoined.txt/compose?ifGenerationMatch=bad",
            r#"{"sourceObjects":[{"name":"docs/source.txt"}]}"#,
        ),
    ] {
        let rec = perform(&mut s, method, target, body);
        assert_eq!(rec.status, 400, "{name}: {}", body_str(&rec));
    }
}

/// legacy: TestObjectOperationsHonorGenerationQuery
#[test]
fn object_operations_honor_generation_query() {
    let mut s = server();
    create_bucket(&mut s, "demo-bucket");
    let uploaded = upload_text(&mut s, "demo-bucket", "docs/versioned.txt", "body");
    let generation = uploaded["generation"].as_str().unwrap().to_string();

    for (name, method, target) in [
        (
            "metadata",
            "GET",
            format!("/storage/v1/b/demo-bucket/o/docs%2Fversioned.txt?generation={generation}"),
        ),
        (
            "download",
            "GET",
            format!("/download/storage/v1/b/demo-bucket/o/docs%2Fversioned.txt?alt=media&generation={generation}"),
        ),
        (
            "patch",
            "PATCH",
            format!("/storage/v1/b/demo-bucket/o/docs%2Fversioned.txt?generation={generation}"),
        ),
    ] {
        let body = if method == "PATCH" {
            r#"{"metadata":{"source":"generation-query"}}"#
        } else {
            ""
        };
        let rec = perform(&mut s, method, &target, body);
        assert_eq!(rec.status, 200, "{name}: {}", body_str(&rec));
    }

    for (name, method, target) in [
        (
            "metadata",
            "GET",
            "/storage/v1/b/demo-bucket/o/docs%2Fversioned.txt?generation=1",
        ),
        (
            "download",
            "GET",
            "/download/storage/v1/b/demo-bucket/o/docs%2Fversioned.txt?alt=media&generation=1",
        ),
        (
            "patch",
            "PATCH",
            "/storage/v1/b/demo-bucket/o/docs%2Fversioned.txt?generation=1",
        ),
        (
            "delete",
            "DELETE",
            "/storage/v1/b/demo-bucket/o/docs%2Fversioned.txt?generation=1",
        ),
    ] {
        let rec = perform(&mut s, method, target, r#"{"metadata":{"source":"stale"}}"#);
        assert_eq!(
            rec.status,
            404,
            "{name} stale generation: {}",
            body_str(&rec)
        );
    }

    let invalid = perform(
        &mut s,
        "GET",
        "/storage/v1/b/demo-bucket/o/docs%2Fversioned.txt?generation=bad",
        "",
    );
    assert_eq!(invalid.status, 400, "{}", body_str(&invalid));

    let delete = perform(
        &mut s,
        "DELETE",
        &format!("/storage/v1/b/demo-bucket/o/docs%2Fversioned.txt?generation={generation}"),
        "",
    );
    assert_eq!(delete.status, 204, "{}", body_str(&delete));
}

// ---------------------------------------------------------------------------
// resumable_test.rs
// ---------------------------------------------------------------------------

/// Extracts the session target (path + query) from a resumable `Location`.
fn session_target(resp: &Response) -> String {
    let location = header(resp, "Location");
    assert!(!location.is_empty(), "init Location is empty");
    location
        .strip_prefix("http://example.com")
        .unwrap_or(location)
        .to_string()
}

/// legacy: TestResumableUploadCreatesSessionAndCommitsObject
#[test]
fn resumable_upload_creates_session_and_commits_object() {
    let mut s = server();
    create_bucket(&mut s, "demo-bucket");

    let init = perform_h(
        &mut s,
        "POST",
        "/upload/storage/v1/b/demo-bucket/o?uploadType=resumable&name=docs/resumable.txt",
        r#"{"name":"docs/resumable.txt","contentType":"text/plain"}"#,
        &[
            ("Host", "example.com"),
            ("Content-Type", "application/json"),
            ("X-Upload-Content-Type", "text/plain"),
            ("x-goog-meta-source", "resumable-test"),
        ],
    );
    assert_eq!(init.status, 200, "{}", body_str(&init));
    let upload_id = header(&init, "X-GUploader-UploadID").to_string();
    assert!(!upload_id.is_empty(), "init X-GUploader-UploadID is empty");
    let target = session_target(&init);
    assert!(
        target.contains(&format!("upload_id={upload_id}")),
        "session target {target} must carry upload_id {upload_id}"
    );

    let commit = perform_h(
        &mut s,
        "PUT",
        &target,
        "resumable body",
        &[
            ("Content-Type", "text/plain"),
            ("Content-Range", "bytes 0-13/14"),
        ],
    );
    assert_eq!(commit.status, 200, "{}", body_str(&commit));
    let committed = json_body(&commit);
    assert_eq!(committed["name"], "docs/resumable.txt");
    assert_eq!(committed["metadata"]["source"], "resumable-test");

    let download = perform(
        &mut s,
        "GET",
        "/download/storage/v1/b/demo-bucket/o/docs%2Fresumable.txt?alt=media",
        "",
    );
    assert_eq!(download.status, 200, "{}", body_str(&download));
    assert_eq!(download.body, b"resumable body");
}

/// legacy: TestResumableUploadUsesJSONMetadataFromInitiationRequest
#[test]
fn resumable_upload_uses_json_metadata_from_initiation_request() {
    let mut s = server();
    create_bucket(&mut s, "demo-bucket");

    let init = perform_h(
        &mut s,
        "POST",
        "/upload/storage/v1/b/demo-bucket/o?uploadType=resumable",
        r#"{"name":"docs/resumable-metadata.txt","contentType":"text/plain","contentEncoding":"gzip","cacheControl":"no-cache","contentDisposition":"inline","metadata":{"source":"json-init","override":"json"}}"#,
        &[
            ("Host", "example.com"),
            ("Content-Type", "application/json"),
            ("x-goog-meta-override", "header"),
        ],
    );
    assert_eq!(init.status, 200, "{}", body_str(&init));
    let target = session_target(&init);

    let commit = perform_h(
        &mut s,
        "PUT",
        &target,
        "metadata body",
        &[
            ("Content-Type", "text/plain"),
            ("Content-Range", "bytes 0-12/13"),
        ],
    );
    assert_eq!(commit.status, 200, "{}", body_str(&commit));
    let committed = json_body(&commit);
    assert_eq!(committed["name"], "docs/resumable-metadata.txt");
    assert_eq!(committed["contentType"], "text/plain");
    assert_eq!(committed["contentEncoding"], "gzip");
    assert_eq!(committed["cacheControl"], "no-cache");
    assert_eq!(committed["contentDisposition"], "inline");
    assert_eq!(committed["metadata"]["source"], "json-init");
    assert_eq!(committed["metadata"]["override"], "header");
}

/// legacy: TestResumableUploadReadsJSONMetadataWithQueryNameAndUploadContentType
#[test]
fn resumable_upload_reads_json_metadata_with_query_name_and_upload_content_type() {
    let mut s = server();
    create_bucket(&mut s, "demo-bucket");

    let init = perform_h(
        &mut s,
        "POST",
        "/upload/storage/v1/b/demo-bucket/o?uploadType=resumable&name=docs/query-name.txt",
        r#"{"metadata":{"source":"json-init"},"cacheControl":"no-cache","contentDisposition":"inline"}"#,
        &[
            ("Host", "example.com"),
            ("Content-Type", "application/json"),
            ("X-Upload-Content-Type", "text/plain"),
        ],
    );
    assert_eq!(init.status, 200, "{}", body_str(&init));
    let target = session_target(&init);

    let commit = perform_h(
        &mut s,
        "PUT",
        &target,
        "query metadata",
        &[
            ("Content-Type", "text/plain"),
            ("Content-Range", "bytes 0-13/14"),
        ],
    );
    assert_eq!(commit.status, 200, "{}", body_str(&commit));
    let committed = json_body(&commit);
    assert_eq!(committed["name"], "docs/query-name.txt");
    assert_eq!(committed["contentType"], "text/plain");
    assert_eq!(committed["metadata"]["source"], "json-init");
    assert_eq!(committed["cacheControl"], "no-cache");
    assert_eq!(committed["contentDisposition"], "inline");
}

/// legacy: TestResumableUploadRechecksStoredPreconditionsAtCommit
#[test]
fn resumable_upload_rechecks_stored_preconditions_at_commit() {
    let mut s = server();
    create_bucket(&mut s, "demo-bucket");

    let init = perform_h(
        &mut s,
        "POST",
        "/upload/storage/v1/b/demo-bucket/o?uploadType=resumable&name=docs/race.txt&ifGenerationMatch=0",
        r#"{"name":"docs/race.txt","contentType":"text/plain"}"#,
        &[
            ("Host", "example.com"),
            ("Content-Type", "application/json"),
            ("X-Upload-Content-Type", "text/plain"),
        ],
    );
    assert_eq!(init.status, 200, "{}", body_str(&init));
    let target = session_target(&init);

    upload_text(&mut s, "demo-bucket", "docs/race.txt", "existing body");

    let commit = perform_h(
        &mut s,
        "PUT",
        &target,
        "new body",
        &[
            ("Content-Type", "text/plain"),
            ("Content-Range", "bytes 0-7/8"),
        ],
    );
    assert_eq!(commit.status, 412, "{}", body_str(&commit));

    let download = perform(
        &mut s,
        "GET",
        "/download/storage/v1/b/demo-bucket/o/docs%2Frace.txt?alt=media",
        "",
    );
    assert_eq!(download.status, 200, "{}", body_str(&download));
    assert_eq!(download.body, b"existing body");
}

/// legacy: TestResumableUploadPersistsChunkStatusAcrossServerRestart
#[test]
fn resumable_upload_persists_chunk_status_across_server_restart() {
    let root = temp_root("resumable-restart");
    let sessions = root.join("sessions");
    let buckets = root.join("buckets");
    let mut s = Server::new(
        Config {
            upload_session_path: sessions.to_string_lossy().to_string(),
            ..Default::default()
        },
        FileBucketStore::new(&buckets),
    );
    create_bucket(&mut s, "demo-bucket");

    let init = perform_h(
        &mut s,
        "POST",
        "/upload/storage/v1/b/demo-bucket/o?uploadType=resumable&name=docs/chunked.txt",
        r#"{"name":"docs/chunked.txt","contentType":"text/plain"}"#,
        &[
            ("Host", "example.com"),
            ("Content-Type", "application/json"),
            ("X-Upload-Content-Type", "text/plain"),
        ],
    );
    assert_eq!(init.status, 200, "{}", body_str(&init));
    let target = session_target(&init);

    let initial_status = perform_h(
        &mut s,
        "PUT",
        &target,
        "",
        &[("Content-Range", "bytes */14")],
    );
    assert_eq!(initial_status.status, 308, "{}", body_str(&initial_status));
    assert_eq!(header(&initial_status, "Range"), "");

    let first_chunk = perform_h(
        &mut s,
        "PUT",
        &target,
        "resumable ",
        &[
            ("Content-Type", "text/plain"),
            ("Content-Range", "bytes 0-9/14"),
        ],
    );
    assert_eq!(first_chunk.status, 308, "{}", body_str(&first_chunk));
    assert_eq!(header(&first_chunk, "Range"), "bytes=0-9");

    let mut restarted = Server::new(
        Config {
            upload_session_path: sessions.to_string_lossy().to_string(),
            ..Default::default()
        },
        FileBucketStore::new(&buckets),
    );
    let status = perform_h(
        &mut restarted,
        "PUT",
        &target,
        "",
        &[("Content-Range", "bytes */14")],
    );
    assert_eq!(status.status, 308, "{}", body_str(&status));
    assert_eq!(header(&status, "Range"), "bytes=0-9");

    let commit = perform_h(
        &mut restarted,
        "PUT",
        &target,
        "body",
        &[
            ("Content-Type", "text/plain"),
            ("Content-Range", "bytes 10-13/14"),
        ],
    );
    assert_eq!(commit.status, 200, "{}", body_str(&commit));

    let download = perform(
        &mut restarted,
        "GET",
        "/download/storage/v1/b/demo-bucket/o/docs%2Fchunked.txt?alt=media",
        "",
    );
    assert_eq!(download.status, 200, "{}", body_str(&download));
    assert_eq!(download.body, b"resumable body");
}

/// legacy: TestResumableUploadRejectsMalformedCommitRequests
#[test]
fn resumable_upload_rejects_malformed_commit_requests() {
    let mut s = server();
    create_bucket(&mut s, "demo-bucket");

    let missing_id = perform_h(
        &mut s,
        "PUT",
        "/upload/storage/v1/b/demo-bucket/o?uploadType=resumable",
        "body",
        &[("Content-Range", "bytes 0-3/4")],
    );
    assert_eq!(missing_id.status, 400, "{}", body_str(&missing_id));

    let unknown_id = perform_h(
        &mut s,
        "PUT",
        "/upload/storage/v1/b/demo-bucket/o?uploadType=resumable&upload_id=missing",
        "body",
        &[("Content-Range", "bytes 0-3/4")],
    );
    assert_eq!(unknown_id.status, 404, "{}", body_str(&unknown_id));

    let init = perform_h(
        &mut s,
        "POST",
        "/upload/storage/v1/b/demo-bucket/o?uploadType=resumable&name=docs/resumable.txt",
        r#"{"contentType":"text/plain"}"#,
        &[
            ("Host", "example.com"),
            ("Content-Type", "application/json"),
        ],
    );
    assert_eq!(init.status, 200, "{}", body_str(&init));
    let target = session_target(&init);

    for (name, content_range, body) in [
        ("wrong unit", "items 0-3/4", "body"),
        ("payload size mismatch", "bytes 0-9/10", "body"),
        ("wrong committed offset", "bytes 1-4/5", "body"),
    ] {
        let rec = perform_h(
            &mut s,
            "PUT",
            &target,
            body,
            &[
                ("Content-Type", "text/plain"),
                ("Content-Range", content_range),
            ],
        );
        assert_eq!(rec.status, 400, "{name}: {}", body_str(&rec));
    }
}

// ---------------------------------------------------------------------------
// server_test.rs
// ---------------------------------------------------------------------------

/// legacy: TestAuthModes
#[test]
fn auth_modes() {
    let root = temp_root("auth-modes");

    let mut relaxed = Server::new(
        Config {
            auth_mode: "relaxed".to_string(),
            ..Default::default()
        },
        FileBucketStore::new(&root),
    );
    let rec = perform(&mut relaxed, "GET", "/storage/v1/b?project=devcloud", "");
    assert_eq!(rec.status, 200, "{}", body_str(&rec));

    let mut oauth_relaxed = Server::new(
        Config {
            auth_mode: "oauth-relaxed".to_string(),
            ..Default::default()
        },
        FileBucketStore::new(&root),
    );
    let rec = perform(
        &mut oauth_relaxed,
        "GET",
        "/storage/v1/b?project=devcloud",
        "",
    );
    assert_eq!(rec.status, 401, "{}", body_str(&rec));
    let rec = perform_h(
        &mut oauth_relaxed,
        "GET",
        "/storage/v1/b?project=devcloud",
        "",
        &[("Authorization", "Bearer local-token")],
    );
    assert_eq!(rec.status, 200, "{}", body_str(&rec));

    let mut bearer_dev = Server::new(
        Config {
            auth_mode: "bearer-dev".to_string(),
            bearer_token: "expected-token".to_string(),
            ..Default::default()
        },
        FileBucketStore::new(&root),
    );
    let rec = perform_h(
        &mut bearer_dev,
        "GET",
        "/storage/v1/b?project=devcloud",
        "",
        &[("Authorization", "Bearer wrong-token")],
    );
    assert_eq!(rec.status, 401, "{}", body_str(&rec));
    let rec = perform_h(
        &mut bearer_dev,
        "GET",
        "/storage/v1/b?project=devcloud",
        "",
        &[("Authorization", "Bearer expected-token")],
    );
    assert_eq!(rec.status, 200, "{}", body_str(&rec));
}

// ---------------------------------------------------------------------------
// upload_test.rs
// ---------------------------------------------------------------------------

/// legacy: TestObjectMultipartUploadUsesMetadataAndMediaParts
#[test]
fn object_multipart_upload_uses_metadata_and_media_parts() {
    let mut s = server();
    create_bucket(&mut s, "demo-bucket");

    let body = [
        "--devcloud-boundary",
        "Content-Type: application/json; charset=UTF-8",
        "",
        r#"{"name":"docs/multipart.txt","contentType":"text/plain","cacheControl":"no-cache","contentDisposition":"inline","metadata":{"source":"multipart-test"}}"#,
        "--devcloud-boundary",
        "Content-Type: text/plain",
        "",
        "hello multipart",
        "--devcloud-boundary--",
        "",
    ]
    .join("\r\n");
    let upload = perform_h(
        &mut s,
        "POST",
        "/upload/storage/v1/b/demo-bucket/o?uploadType=multipart",
        &body,
        &[(
            "Content-Type",
            r#"multipart/related; boundary="devcloud-boundary""#,
        )],
    );
    assert_eq!(upload.status, 200, "{}", body_str(&upload));
    let uploaded = json_body(&upload);
    assert_eq!(uploaded["name"], "docs/multipart.txt");
    assert_eq!(uploaded["contentType"], "text/plain");
    assert_eq!(uploaded["metadata"]["source"], "multipart-test");
    assert_eq!(uploaded["cacheControl"], "no-cache");
    assert_eq!(uploaded["contentDisposition"], "inline");

    let download = perform(
        &mut s,
        "GET",
        "/download/storage/v1/b/demo-bucket/o/docs%2Fmultipart.txt?alt=media",
        "",
    );
    assert_eq!(download.status, 200, "{}", body_str(&download));
    assert_eq!(download.body, b"hello multipart");
}
