use std::fs;
use std::time::{SystemTime, UNIX_EPOCH};

use devcloud_gcs::http::{route, Config, Request, Server};
use devcloud_s3::store::FileBucketStore;

fn server() -> Server {
    let root = std::env::temp_dir().join(format!(
        "devcloud-gcs-test-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = fs::remove_dir_all(&root);
    Server::new(
        Config {
            project: "devcloud".to_string(),
            location: "US".to_string(),
            auth_mode: "relaxed".to_string(),
            bearer_token: String::new(),
            ..Default::default()
        },
        FileBucketStore::new(root),
    )
}

fn req(method: &str, target: &str, body: impl Into<Vec<u8>>) -> Request {
    Request::new(method, target, body.into())
}

fn create_bucket(s: &mut Server, name: &str) {
    let response = route(
        s,
        &req(
            "POST",
            "/storage/v1/b?project=devcloud",
            format!(r#"{{"name":"{name}"}}"#).into_bytes(),
        ),
    );
    assert_eq!(
        response.status,
        200,
        "{}",
        String::from_utf8_lossy(&response.body)
    );
}

fn upload_text(s: &mut Server, bucket: &str, name: &str, body: &[u8]) -> serde_json::Value {
    let mut request = req(
        "POST",
        format!("/upload/storage/v1/b/{bucket}/o?uploadType=media&name={name}").as_str(),
        body.to_vec(),
    );
    request
        .headers
        .insert("content-type".to_string(), "text/plain".to_string());
    let response = route(s, &request);
    assert_eq!(
        response.status,
        200,
        "{}",
        String::from_utf8_lossy(&response.body)
    );
    serde_json::from_slice(&response.body).unwrap()
}

#[test]
fn bucket_and_media_object_lifecycle() {
    let mut s = server();

    let create = route(
        &mut s,
        &req(
            "POST",
            "/storage/v1/b?project=devcloud",
            br#"{"name":"demo","location":"US","storageClass":"STANDARD"}"#.to_vec(),
        ),
    );
    assert_eq!(
        create.status,
        200,
        "{}",
        String::from_utf8_lossy(&create.body)
    );
    let body = String::from_utf8(create.body).unwrap();
    assert!(body.contains(r#""kind":"storage#bucket""#));
    assert!(body.contains(r#""name":"demo""#));

    let put = route(
        &mut s,
        &req(
            "POST",
            "/upload/storage/v1/b/demo/o?uploadType=media&name=docs/readme.txt",
            b"hello from gcs".to_vec(),
        ),
    );
    assert_eq!(put.status, 200, "{}", String::from_utf8_lossy(&put.body));
    let put_body = String::from_utf8(put.body).unwrap();
    assert!(put_body.contains(r#""kind":"storage#object""#));
    assert!(put_body.contains(r#""name":"docs/readme.txt""#));
    assert!(put_body.contains(r#""size":"14""#));

    let list = route(
        &mut s,
        &req("GET", "/storage/v1/b/demo/o?prefix=docs/", Vec::new()),
    );
    assert_eq!(list.status, 200, "{}", String::from_utf8_lossy(&list.body));
    assert!(String::from_utf8(list.body)
        .unwrap()
        .contains(r#""name":"docs/readme.txt""#));

    let get_meta = route(
        &mut s,
        &req("GET", "/storage/v1/b/demo/o/docs%2Freadme.txt", Vec::new()),
    );
    assert_eq!(
        get_meta.status,
        200,
        "{}",
        String::from_utf8_lossy(&get_meta.body)
    );

    let mut download = req(
        "GET",
        "/download/storage/v1/b/demo/o/docs%2Freadme.txt?alt=media",
        Vec::new(),
    );
    download
        .headers
        .insert("range".to_string(), "bytes=0-4".to_string());
    let downloaded = route(&mut s, &download);
    assert_eq!(downloaded.status, 206);
    assert_eq!(downloaded.body, b"hello");

    let copy = route(
        &mut s,
        &req(
            "POST",
            "/storage/v1/b/demo/o/docs%2Freadme.txt/copyTo/b/demo/o/docs%2Fcopy.txt",
            b"{}".to_vec(),
        ),
    );
    assert_eq!(copy.status, 200, "{}", String::from_utf8_lossy(&copy.body));
    assert!(String::from_utf8(copy.body)
        .unwrap()
        .contains(r#""name":"docs/copy.txt""#));

    let mismatch = route(
        &mut s,
        &req(
            "POST",
            "/upload/storage/v1/b/demo/o?uploadType=media&name=docs/precondition.txt&ifGenerationMatch=999999",
            b"blocked".to_vec(),
        ),
    );
    assert_eq!(mismatch.status, 412);

    let mut init = req(
        "POST",
        "/upload/storage/v1/b/demo/o?uploadType=resumable&name=docs/resumable.txt",
        br#"{"name":"docs/resumable.txt","contentType":"text/plain"}"#.to_vec(),
    );
    init.headers
        .insert("host".to_string(), "127.0.0.1:4443".to_string());
    init.headers.insert(
        "x-upload-content-type".to_string(),
        "text/plain".to_string(),
    );
    let init_resp = route(&mut s, &init);
    assert_eq!(init_resp.status, 200);
    let location = init_resp.headers.get("Location").unwrap().clone();
    let upload_target = location.trim_start_matches("http://127.0.0.1:4443");
    let mut finalize = req("PUT", upload_target, b"resumable body".to_vec());
    finalize
        .headers
        .insert("content-type".to_string(), "text/plain".to_string());
    finalize
        .headers
        .insert("content-range".to_string(), "bytes 0-13/14".to_string());
    let finalized = route(&mut s, &finalize);
    assert_eq!(
        finalized.status,
        200,
        "{}",
        String::from_utf8_lossy(&finalized.body)
    );

    let delete = route(
        &mut s,
        &req(
            "DELETE",
            "/storage/v1/b/demo/o/docs%2Freadme.txt",
            Vec::new(),
        ),
    );
    assert_eq!(delete.status, 204);
}

#[test]
fn bucket_pagination_prefix_and_non_empty_delete_match_go_parity() {
    let mut s = server();
    for name in ["alpha-bucket", "beta-bucket", "gamma-bucket"] {
        create_bucket(&mut s, name);
    }

    let first = route(
        &mut s,
        &req(
            "GET",
            "/storage/v1/b?project=devcloud&maxResults=2",
            Vec::new(),
        ),
    );
    assert_eq!(
        first.status,
        200,
        "{}",
        String::from_utf8_lossy(&first.body)
    );
    let first_json: serde_json::Value = serde_json::from_slice(&first.body).unwrap();
    assert_eq!(first_json["items"].as_array().unwrap().len(), 2);
    assert_eq!(first_json["items"][0]["name"], "alpha-bucket");
    assert_eq!(first_json["items"][1]["name"], "beta-bucket");
    let token = first_json["nextPageToken"].as_str().unwrap();
    assert!(!token.is_empty());

    let second = route(
        &mut s,
        &req(
            "GET",
            format!("/storage/v1/b?project=devcloud&pageToken={token}").as_str(),
            Vec::new(),
        ),
    );
    assert_eq!(
        second.status,
        200,
        "{}",
        String::from_utf8_lossy(&second.body)
    );
    let second_json: serde_json::Value = serde_json::from_slice(&second.body).unwrap();
    assert_eq!(second_json["items"].as_array().unwrap().len(), 1);
    assert_eq!(second_json["items"][0]["name"], "gamma-bucket");
    assert!(second_json.get("nextPageToken").is_none());

    let invalid = route(
        &mut s,
        &req(
            "GET",
            "/storage/v1/b?project=devcloud&pageToken=bad",
            Vec::new(),
        ),
    );
    assert_eq!(invalid.status, 400);

    create_bucket(&mut s, "app-assets");
    create_bucket(&mut s, "app-logs");
    create_bucket(&mut s, "backup-archive");
    let prefixed = route(
        &mut s,
        &req(
            "GET",
            "/storage/v1/b?project=devcloud&prefix=app-",
            Vec::new(),
        ),
    );
    assert_eq!(
        prefixed.status,
        200,
        "{}",
        String::from_utf8_lossy(&prefixed.body)
    );
    let prefixed_json: serde_json::Value = serde_json::from_slice(&prefixed.body).unwrap();
    let names: Vec<_> = prefixed_json["items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|item| item["name"].as_str().unwrap())
        .collect();
    assert_eq!(names, vec!["app-assets", "app-logs"]);

    create_bucket(&mut s, "non-empty");
    upload_text(&mut s, "non-empty", "object.txt", b"body");
    let delete = route(
        &mut s,
        &req("DELETE", "/storage/v1/b/non-empty", Vec::new()),
    );
    assert_eq!(delete.status, 409);
}

#[test]
fn head_media_and_delimited_listing_match_go_parity() {
    let mut s = server();
    create_bucket(&mut s, "demo");

    let mut upload = req(
        "POST",
        "/upload/storage/v1/b/demo/o?uploadType=media&name=docs/head.txt",
        b"hello metadata".to_vec(),
    );
    upload
        .headers
        .insert("content-type".to_string(), "text/plain".to_string());
    upload
        .headers
        .insert("cache-control".to_string(), "no-cache".to_string());
    upload
        .headers
        .insert("content-disposition".to_string(), "inline".to_string());
    upload
        .headers
        .insert("x-goog-meta-source".to_string(), "head-test".to_string());
    assert_eq!(route(&mut s, &upload).status, 200);

    let metadata = route(
        &mut s,
        &req("HEAD", "/storage/v1/b/demo/o/docs%2Fhead.txt", Vec::new()),
    );
    assert_eq!(metadata.status, 200);
    assert!(metadata.body.is_empty());
    assert_eq!(
        metadata
            .headers
            .get("X-Goog-Stored-Content-Length")
            .map(String::as_str),
        Some("14")
    );
    assert_eq!(
        metadata
            .headers
            .get("X-Goog-Meta-source")
            .map(String::as_str),
        Some("head-test")
    );
    assert_eq!(
        metadata.headers.get("Cache-Control").map(String::as_str),
        Some("no-cache")
    );
    assert_eq!(
        metadata
            .headers
            .get("Content-Disposition")
            .map(String::as_str),
        Some("inline")
    );

    let download = route(
        &mut s,
        &req(
            "HEAD",
            "/download/storage/v1/b/demo/o/docs%2Fhead.txt?alt=media",
            Vec::new(),
        ),
    );
    assert_eq!(download.status, 200);
    assert!(download.body.is_empty());
    assert_eq!(
        download.headers.get("Content-Length").map(String::as_str),
        Some("14")
    );

    let mut range = req(
        "HEAD",
        "/download/storage/v1/b/demo/o/docs%2Fhead.txt?alt=media",
        Vec::new(),
    );
    range
        .headers
        .insert("range".to_string(), "bytes=0-4".to_string());
    let range_download = route(&mut s, &range);
    assert_eq!(range_download.status, 206);
    assert_eq!(
        range_download
            .headers
            .get("Content-Range")
            .map(String::as_str),
        Some("bytes 0-4/14")
    );
    assert_eq!(
        range_download
            .headers
            .get("Content-Length")
            .map(String::as_str),
        Some("5")
    );
}

#[test]
fn object_listing_offsets_trailing_delimiter_patch_and_multipart_parity() {
    let mut s = server();
    create_bucket(&mut s, "demo");

    for (name, body) in [
        ("docs/folder/", b"".as_slice()),
        ("docs/folder/readme.md", b"readme".as_slice()),
        ("docs/a.txt", b"a".as_slice()),
        ("docs/b.txt", b"b".as_slice()),
        ("docs/c/file.txt", b"c".as_slice()),
        ("docs/d.txt", b"d".as_slice()),
    ] {
        upload_text(&mut s, "demo", name, body);
    }

    let without_marker = route(
        &mut s,
        &req(
            "GET",
            "/storage/v1/b/demo/o?prefix=docs/&delimiter=/",
            Vec::new(),
        ),
    );
    assert_eq!(
        without_marker.status,
        200,
        "{}",
        String::from_utf8_lossy(&without_marker.body)
    );
    let default_list: serde_json::Value = serde_json::from_slice(&without_marker.body).unwrap();
    assert!(default_list["items"]
        .as_array()
        .unwrap()
        .iter()
        .all(|item| item["name"] != "docs/folder/"));
    assert!(default_list["prefixes"]
        .as_array()
        .unwrap()
        .iter()
        .any(|prefix| prefix == "docs/folder/"));

    let with_marker = route(
        &mut s,
        &req(
            "GET",
            "/storage/v1/b/demo/o?prefix=docs/&delimiter=/&includeTrailingDelimiter=true",
            Vec::new(),
        ),
    );
    assert_eq!(
        with_marker.status,
        200,
        "{}",
        String::from_utf8_lossy(&with_marker.body)
    );
    let marker_list: serde_json::Value = serde_json::from_slice(&with_marker.body).unwrap();
    assert!(marker_list["items"]
        .as_array()
        .unwrap()
        .iter()
        .any(|item| item["name"] == "docs/folder/"));

    let offset = route(
        &mut s,
        &req(
            "GET",
            "/storage/v1/b/demo/o?prefix=docs/&startOffset=docs/b.txt&endOffset=docs/d.txt",
            Vec::new(),
        ),
    );
    assert_eq!(
        offset.status,
        200,
        "{}",
        String::from_utf8_lossy(&offset.body)
    );
    let offset_json: serde_json::Value = serde_json::from_slice(&offset.body).unwrap();
    let offset_names: Vec<_> = offset_json["items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|item| item["name"].as_str().unwrap())
        .collect();
    assert_eq!(offset_names, vec!["docs/b.txt", "docs/c/file.txt"]);

    let uploaded = upload_text(&mut s, "demo", "docs/metadata.txt", b"body");
    let generation = uploaded["generation"].as_str().unwrap();
    let patch = route(
        &mut s,
        &req(
            "PATCH",
            "/storage/v1/b/demo/o/docs%2Fmetadata.txt?ifMetagenerationMatch=1",
            br#"{"contentType":"text/markdown","cacheControl":"no-cache","metadata":{"source":"patched","owner":"gcs"}}"#.to_vec(),
        ),
    );
    assert_eq!(
        patch.status,
        200,
        "{}",
        String::from_utf8_lossy(&patch.body)
    );
    let patched: serde_json::Value = serde_json::from_slice(&patch.body).unwrap();
    assert_eq!(patched["generation"], generation);
    assert_eq!(patched["metageneration"], "2");
    assert_eq!(patched["contentType"], "text/markdown");
    assert_eq!(patched["cacheControl"], "no-cache");
    assert_eq!(patched["metadata"]["source"], "patched");
    let stale_patch = route(
        &mut s,
        &req(
            "PATCH",
            "/storage/v1/b/demo/o/docs%2Fmetadata.txt?ifMetagenerationMatch=1",
            br#"{"metadata":{"source":"stale"}}"#.to_vec(),
        ),
    );
    assert_eq!(stale_patch.status, 412);

    let multipart_body = [
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
    let mut multipart = req(
        "POST",
        "/upload/storage/v1/b/demo/o?uploadType=multipart",
        multipart_body.into_bytes(),
    );
    multipart.headers.insert(
        "content-type".to_string(),
        r#"multipart/related; boundary="devcloud-boundary""#.to_string(),
    );
    let uploaded_multipart = route(&mut s, &multipart);
    assert_eq!(
        uploaded_multipart.status,
        200,
        "{}",
        String::from_utf8_lossy(&uploaded_multipart.body)
    );
    let multipart_json: serde_json::Value =
        serde_json::from_slice(&uploaded_multipart.body).unwrap();
    assert_eq!(multipart_json["name"], "docs/multipart.txt");
    assert_eq!(multipart_json["contentType"], "text/plain");
    assert_eq!(multipart_json["cacheControl"], "no-cache");
    assert_eq!(multipart_json["contentDisposition"], "inline");
    assert_eq!(multipart_json["metadata"]["source"], "multipart-test");
    let downloaded = route(
        &mut s,
        &req(
            "GET",
            "/download/storage/v1/b/demo/o/docs%2Fmultipart.txt?alt=media",
            Vec::new(),
        ),
    );
    assert_eq!(downloaded.body, b"hello multipart");
}

#[test]
fn delimited_object_listing_paginates_items_and_prefixes_together() {
    let mut s = server();
    create_bucket(&mut s, "demo");
    for (name, body) in [
        ("docs/a.txt", b"a".as_slice()),
        ("docs/b/file.txt", b"b".as_slice()),
        ("docs/c/file.txt", b"c".as_slice()),
        ("docs/d.txt", b"d".as_slice()),
    ] {
        upload_text(&mut s, "demo", name, body);
    }

    let first = route(
        &mut s,
        &req(
            "GET",
            "/storage/v1/b/demo/o?prefix=docs/&delimiter=/&maxResults=2",
            Vec::new(),
        ),
    );
    assert_eq!(
        first.status,
        200,
        "{}",
        String::from_utf8_lossy(&first.body)
    );
    let first_json: serde_json::Value = serde_json::from_slice(&first.body).unwrap();
    assert_eq!(first_json["items"].as_array().unwrap().len(), 1);
    assert_eq!(first_json["items"][0]["name"], "docs/a.txt");
    assert_eq!(first_json["prefixes"][0], "docs/b/");
    let token = first_json["nextPageToken"].as_str().unwrap();
    assert!(!token.is_empty());

    let second = route(
        &mut s,
        &req(
            "GET",
            format!("/storage/v1/b/demo/o?prefix=docs/&delimiter=/&pageToken={token}").as_str(),
            Vec::new(),
        ),
    );
    assert_eq!(
        second.status,
        200,
        "{}",
        String::from_utf8_lossy(&second.body)
    );
    let second_json: serde_json::Value = serde_json::from_slice(&second.body).unwrap();
    assert_eq!(second_json["items"].as_array().unwrap().len(), 1);
    assert_eq!(second_json["items"][0]["name"], "docs/d.txt");
    assert_eq!(second_json["prefixes"][0], "docs/c/");
    assert!(second_json.get("nextPageToken").is_none());

    let invalid = route(
        &mut s,
        &req(
            "GET",
            "/storage/v1/b/demo/o?prefix=docs/&delimiter=/&maxResults=bad",
            Vec::new(),
        ),
    );
    assert_eq!(invalid.status, 400);
}

#[test]
fn copy_enforces_source_preconditions_and_destination_metadata() {
    let mut s = server();
    create_bucket(&mut s, "demo");
    let uploaded = upload_text(&mut s, "demo", "docs/source.txt", b"copy source");
    let generation = uploaded["generation"].as_str().unwrap();

    let mismatch = route(
        &mut s,
        &req(
            "POST",
            "/storage/v1/b/demo/o/docs%2Fsource.txt/copyTo/b/demo/o/docs%2Fblocked.txt?ifSourceGenerationMatch=999999",
            b"{}".to_vec(),
        ),
    );
    assert_eq!(mismatch.status, 412);

    let copy = route(
        &mut s,
        &req(
            "POST",
            format!(
                "/storage/v1/b/demo/o/docs%2Fsource.txt/copyTo/b/demo/o/docs%2Fcopy.txt?ifSourceGenerationMatch={generation}&ifSourceMetagenerationMatch=1"
            )
            .as_str(),
            br#"{"contentType":"text/markdown","cacheControl":"no-cache","contentDisposition":"inline","metadata":{"source":"copy-request","owner":"gcs"}}"#.to_vec(),
        ),
    );
    assert_eq!(copy.status, 200, "{}", String::from_utf8_lossy(&copy.body));
    let copied: serde_json::Value = serde_json::from_slice(&copy.body).unwrap();
    assert_eq!(copied["name"], "docs/copy.txt");
    assert_eq!(copied["contentType"], "text/markdown");
    assert_eq!(copied["cacheControl"], "no-cache");
    assert_eq!(copied["contentDisposition"], "inline");
    assert_eq!(copied["metadata"]["source"], "copy-request");
    assert_eq!(copied["metadata"]["owner"], "gcs");

    let downloaded = route(
        &mut s,
        &req(
            "GET",
            "/download/storage/v1/b/demo/o/docs%2Fcopy.txt?alt=media",
            Vec::new(),
        ),
    );
    assert_eq!(downloaded.status, 200);
    assert_eq!(downloaded.body, b"copy source");
    assert_eq!(
        downloaded.headers.get("Content-Type").map(String::as_str),
        Some("text/markdown")
    );
}

#[test]
fn rewrite_copies_existing_object() {
    let mut s = server();
    create_bucket(&mut s, "demo");
    let uploaded = upload_text(&mut s, "demo", "docs/source.txt", b"rewrite source");
    let generation = uploaded["generation"].as_str().unwrap();

    let rewrite = route(
        &mut s,
        &req(
            "POST",
            format!(
                "/storage/v1/b/demo/o/docs%2Fsource.txt/rewriteTo/b/demo/o/docs%2Frewrite.txt?ifSourceGenerationMatch={generation}"
            )
            .as_str(),
            b"{}".to_vec(),
        ),
    );
    assert_eq!(
        rewrite.status,
        200,
        "{}",
        String::from_utf8_lossy(&rewrite.body)
    );
    let response: serde_json::Value = serde_json::from_slice(&rewrite.body).unwrap();
    assert_eq!(response["kind"], "storage#rewriteResponse");
    assert_eq!(response["done"], true);
    assert_eq!(response["totalBytesRewritten"], "14");
    assert_eq!(response["objectSize"], "14");
    assert_eq!(response["resource"]["name"], "docs/rewrite.txt");
    assert_eq!(response["resource"]["contentType"], "text/plain");

    let downloaded = route(
        &mut s,
        &req(
            "GET",
            "/download/storage/v1/b/demo/o/docs%2Frewrite.txt?alt=media",
            Vec::new(),
        ),
    );
    assert_eq!(downloaded.status, 200);
    assert_eq!(downloaded.body, b"rewrite source");
}

#[test]
fn compose_concatenates_sources_and_rejects_generation_mismatch() {
    let mut s = server();
    create_bucket(&mut s, "demo");
    let first = upload_text(&mut s, "demo", "parts/one.txt", b"hello ");
    let generation = first["generation"].as_str().unwrap();
    upload_text(&mut s, "demo", "parts/two.txt", b"compose");

    let compose = route(
        &mut s,
        &req(
            "POST",
            "/storage/v1/b/demo/o/parts%2Fjoined.txt/compose",
            format!(
                r#"{{"sourceObjects":[{{"name":"parts/one.txt","generation":"{generation}"}},{{"name":"parts/two.txt"}}],"destination":{{"contentType":"text/plain","metadata":{{"source":"compose-test"}}}}}}"#
            )
            .into_bytes(),
        ),
    );
    assert_eq!(
        compose.status,
        200,
        "{}",
        String::from_utf8_lossy(&compose.body)
    );
    let composed: serde_json::Value = serde_json::from_slice(&compose.body).unwrap();
    assert_eq!(composed["name"], "parts/joined.txt");
    assert_eq!(composed["contentType"], "text/plain");
    assert_eq!(composed["metadata"]["source"], "compose-test");

    let downloaded = route(
        &mut s,
        &req(
            "GET",
            "/download/storage/v1/b/demo/o/parts%2Fjoined.txt?alt=media",
            Vec::new(),
        ),
    );
    assert_eq!(downloaded.status, 200);
    assert_eq!(downloaded.body, b"hello compose");

    let mismatch = route(
        &mut s,
        &req(
            "POST",
            "/storage/v1/b/demo/o/parts%2Fblocked.txt/compose",
            br#"{"sourceObjects":[{"name":"parts/one.txt","objectPreconditions":{"ifGenerationMatch":"999999"}}]}"#.to_vec(),
        ),
    );
    assert_eq!(mismatch.status, 412);
    let missing = route(
        &mut s,
        &req(
            "GET",
            "/storage/v1/b/demo/o/parts%2Fblocked.txt",
            Vec::new(),
        ),
    );
    assert_eq!(missing.status, 404);
}

#[test]
fn read_download_patch_and_delete_enforce_object_preconditions() {
    let mut s = server();
    create_bucket(&mut s, "demo");
    let uploaded = upload_text(&mut s, "demo", "docs/precondition.txt", b"body");
    let generation = uploaded["generation"].as_str().unwrap();

    for (method, target, body) in [
        (
            "GET",
            "/storage/v1/b/demo/o/docs%2Fprecondition.txt?ifGenerationMatch=999999",
            Vec::new(),
        ),
        (
            "GET",
            "/download/storage/v1/b/demo/o/docs%2Fprecondition.txt?alt=media&ifGenerationMatch=999999",
            Vec::new(),
        ),
        (
            "PATCH",
            "/storage/v1/b/demo/o/docs%2Fprecondition.txt?ifMetagenerationMatch=999999",
            br#"{"metadata":{"source":"stale"}}"#.to_vec(),
        ),
        (
            "DELETE",
            "/storage/v1/b/demo/o/docs%2Fprecondition.txt?ifGenerationMatch=999999",
            Vec::new(),
        ),
    ] {
        let response = route(&mut s, &req(method, target, body));
        assert_eq!(
            response.status,
            412,
            "{method} {target}: {}",
            String::from_utf8_lossy(&response.body)
        );
    }

    let metadata = route(
        &mut s,
        &req(
            "GET",
            format!("/storage/v1/b/demo/o/docs%2Fprecondition.txt?ifGenerationMatch={generation}")
                .as_str(),
            Vec::new(),
        ),
    );
    assert_eq!(
        metadata.status,
        200,
        "{}",
        String::from_utf8_lossy(&metadata.body)
    );

    let patched = route(
        &mut s,
        &req(
            "PATCH",
            format!("/storage/v1/b/demo/o/docs%2Fprecondition.txt?generation={generation}")
                .as_str(),
            br#"{"metadata":{"source":"generation-query"}}"#.to_vec(),
        ),
    );
    assert_eq!(
        patched.status,
        200,
        "{}",
        String::from_utf8_lossy(&patched.body)
    );

    for (method, target, body) in [
        (
            "GET",
            "/storage/v1/b/demo/o/docs%2Fprecondition.txt?generation=bad",
            Vec::new(),
        ),
        (
            "GET",
            "/download/storage/v1/b/demo/o/docs%2Fprecondition.txt?alt=media&ifGenerationNotMatch=bad",
            Vec::new(),
        ),
        (
            "PATCH",
            "/storage/v1/b/demo/o/docs%2Fprecondition.txt?ifMetagenerationNotMatch=bad",
            br#"{"metadata":{"source":"invalid"}}"#.to_vec(),
        ),
    ] {
        let response = route(&mut s, &req(method, target, body));
        assert_eq!(
            response.status,
            400,
            "{method} {target}: {}",
            String::from_utf8_lossy(&response.body)
        );
    }

    let stale_delete = route(
        &mut s,
        &req(
            "DELETE",
            "/storage/v1/b/demo/o/docs%2Fprecondition.txt?generation=1",
            Vec::new(),
        ),
    );
    assert_eq!(stale_delete.status, 404);

    let delete = route(
        &mut s,
        &req(
            "DELETE",
            format!("/storage/v1/b/demo/o/docs%2Fprecondition.txt?generation={generation}")
                .as_str(),
            Vec::new(),
        ),
    );
    assert_eq!(delete.status, 204);
}

#[test]
fn resumable_upload_metadata_preconditions_and_malformed_commits() {
    let mut s = server();
    create_bucket(&mut s, "demo");

    let mut init = req(
        "POST",
        "/upload/storage/v1/b/demo/o?uploadType=resumable",
        br#"{"name":"docs/resumable-metadata.txt","contentType":"text/plain","contentEncoding":"gzip","cacheControl":"no-cache","contentDisposition":"inline","metadata":{"source":"json-init","override":"json"}}"#.to_vec(),
    );
    init.headers
        .insert("host".to_string(), "127.0.0.1:4443".to_string());
    init.headers
        .insert("content-type".to_string(), "application/json".to_string());
    init.headers
        .insert("x-goog-meta-override".to_string(), "header".to_string());
    let init_resp = route(&mut s, &init);
    assert_eq!(
        init_resp.status,
        200,
        "{}",
        String::from_utf8_lossy(&init_resp.body)
    );
    let location = init_resp.headers.get("Location").unwrap().clone();
    let upload_id = init_resp.headers.get("X-GUploader-UploadID").unwrap();
    assert!(location.contains(&format!("upload_id={upload_id}")));
    let target = location.trim_start_matches("http://127.0.0.1:4443");

    let mut commit = req("PUT", target, b"metadata body".to_vec());
    commit
        .headers
        .insert("content-type".to_string(), "text/plain".to_string());
    commit
        .headers
        .insert("content-range".to_string(), "bytes 0-12/13".to_string());
    let committed = route(&mut s, &commit);
    assert_eq!(
        committed.status,
        200,
        "{}",
        String::from_utf8_lossy(&committed.body)
    );
    let committed_json: serde_json::Value = serde_json::from_slice(&committed.body).unwrap();
    assert_eq!(committed_json["name"], "docs/resumable-metadata.txt");
    assert_eq!(committed_json["contentType"], "text/plain");
    assert_eq!(committed_json["contentEncoding"], "gzip");
    assert_eq!(committed_json["cacheControl"], "no-cache");
    assert_eq!(committed_json["contentDisposition"], "inline");
    assert_eq!(committed_json["metadata"]["source"], "json-init");
    assert_eq!(committed_json["metadata"]["override"], "header");

    let mut guarded = req(
        "POST",
        "/upload/storage/v1/b/demo/o?uploadType=resumable&name=docs/race.txt&ifGenerationMatch=0",
        br#"{"name":"docs/race.txt","contentType":"text/plain"}"#.to_vec(),
    );
    guarded
        .headers
        .insert("host".to_string(), "127.0.0.1:4443".to_string());
    guarded
        .headers
        .insert("content-type".to_string(), "application/json".to_string());
    let guarded_resp = route(&mut s, &guarded);
    assert_eq!(guarded_resp.status, 200);
    let guarded_location = guarded_resp.headers.get("Location").unwrap().clone();
    let guarded_target = guarded_location.trim_start_matches("http://127.0.0.1:4443");
    upload_text(&mut s, "demo", "docs/race.txt", b"existing body");
    let mut racing_commit = req("PUT", guarded_target, b"new body".to_vec());
    racing_commit
        .headers
        .insert("content-type".to_string(), "text/plain".to_string());
    racing_commit
        .headers
        .insert("content-range".to_string(), "bytes 0-7/8".to_string());
    let racing_resp = route(&mut s, &racing_commit);
    assert_eq!(racing_resp.status, 412);
    let raced_download = route(
        &mut s,
        &req(
            "GET",
            "/download/storage/v1/b/demo/o/docs%2Frace.txt?alt=media",
            Vec::new(),
        ),
    );
    assert_eq!(raced_download.body, b"existing body");

    let missing_id = {
        let mut r = req(
            "PUT",
            "/upload/storage/v1/b/demo/o?uploadType=resumable",
            b"body".to_vec(),
        );
        r.headers
            .insert("content-range".to_string(), "bytes 0-3/4".to_string());
        route(&mut s, &r)
    };
    assert_eq!(missing_id.status, 400);

    let unknown_id = {
        let mut r = req(
            "PUT",
            "/upload/storage/v1/b/demo/o?uploadType=resumable&upload_id=missing",
            b"body".to_vec(),
        );
        r.headers
            .insert("content-range".to_string(), "bytes 0-3/4".to_string());
        route(&mut s, &r)
    };
    assert_eq!(unknown_id.status, 404);

    let mut malformed_init = req(
        "POST",
        "/upload/storage/v1/b/demo/o?uploadType=resumable&name=docs/malformed.txt",
        br#"{"contentType":"text/plain"}"#.to_vec(),
    );
    malformed_init
        .headers
        .insert("host".to_string(), "127.0.0.1:4443".to_string());
    malformed_init
        .headers
        .insert("content-type".to_string(), "application/json".to_string());
    let malformed_init_resp = route(&mut s, &malformed_init);
    assert_eq!(malformed_init_resp.status, 200);
    let malformed_location = malformed_init_resp.headers.get("Location").unwrap().clone();
    let malformed_target = malformed_location.trim_start_matches("http://127.0.0.1:4443");
    for (content_range, body) in [
        ("items 0-3/4", b"body".as_slice()),
        ("bytes 0-9/10", b"body".as_slice()),
        ("bytes 1-4/5", b"body".as_slice()),
    ] {
        let mut r = req("PUT", malformed_target, body.to_vec());
        r.headers
            .insert("content-type".to_string(), "text/plain".to_string());
        r.headers
            .insert("content-range".to_string(), content_range.to_string());
        let response = route(&mut s, &r);
        assert_eq!(
            response.status,
            400,
            "{content_range}: {}",
            String::from_utf8_lossy(&response.body)
        );
    }
}

#[test]
fn bearer_dev_auth_rejects_missing_token() {
    let mut s = Server::new(
        Config {
            auth_mode: "bearer-dev".to_string(),
            bearer_token: "dev-token".to_string(),
            ..Default::default()
        },
        FileBucketStore::new(std::env::temp_dir().join("devcloud-gcs-auth-test")),
    );

    let denied = route(&mut s, &req("GET", "/storage/v1/b", Vec::new()));
    assert_eq!(denied.status, 401);

    let mut allowed = req("GET", "/storage/v1/b", Vec::new());
    allowed
        .headers
        .insert("authorization".to_string(), "Bearer dev-token".to_string());
    assert_eq!(route(&mut s, &allowed).status, 200);
}

#[test]
fn resumable_upload_persists_chunk_status_across_restart() {
    let root = std::env::temp_dir().join(format!(
        "devcloud-gcs-resumable-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let sessions = root.join("sessions");
    let buckets = root.join("buckets");
    let mut s = Server::new(
        Config {
            upload_session_path: sessions.to_string_lossy().to_string(),
            ..Default::default()
        },
        FileBucketStore::new(&buckets),
    );
    assert_eq!(
        route(
            &mut s,
            &req(
                "POST",
                "/storage/v1/b?project=devcloud",
                br#"{"name":"demo"}"#.to_vec()
            ),
        )
        .status,
        200
    );
    let mut init = req(
        "POST",
        "/upload/storage/v1/b/demo/o?uploadType=resumable&name=docs/chunked.txt",
        br#"{"name":"docs/chunked.txt","contentType":"text/plain"}"#.to_vec(),
    );
    init.headers
        .insert("host".to_string(), "127.0.0.1:4443".to_string());
    init.headers.insert(
        "x-upload-content-type".to_string(),
        "text/plain".to_string(),
    );
    let init_resp = route(&mut s, &init);
    assert_eq!(init_resp.status, 200);
    let location = init_resp.headers.get("Location").unwrap().clone();
    let target = location.trim_start_matches("http://127.0.0.1:4443");

    let mut first = req("PUT", target, b"resumable ".to_vec());
    first
        .headers
        .insert("content-range".to_string(), "bytes 0-9/14".to_string());
    first
        .headers
        .insert("content-type".to_string(), "text/plain".to_string());
    let first_resp = route(&mut s, &first);
    assert_eq!(first_resp.status, 308);
    assert_eq!(
        first_resp.headers.get("Range").map(String::as_str),
        Some("bytes=0-9")
    );

    let mut restarted = Server::new(
        Config {
            upload_session_path: sessions.to_string_lossy().to_string(),
            ..Default::default()
        },
        FileBucketStore::new(&buckets),
    );
    let mut status = req("PUT", target, Vec::new());
    status
        .headers
        .insert("content-range".to_string(), "bytes */14".to_string());
    let status_resp = route(&mut restarted, &status);
    assert_eq!(status_resp.status, 308);
    assert_eq!(
        status_resp.headers.get("Range").map(String::as_str),
        Some("bytes=0-9")
    );

    let mut final_chunk = req("PUT", target, b"body".to_vec());
    final_chunk
        .headers
        .insert("content-range".to_string(), "bytes 10-13/14".to_string());
    final_chunk
        .headers
        .insert("content-type".to_string(), "text/plain".to_string());
    let committed = route(&mut restarted, &final_chunk);
    assert_eq!(
        committed.status,
        200,
        "{}",
        String::from_utf8_lossy(&committed.body)
    );

    let downloaded = route(
        &mut restarted,
        &req(
            "GET",
            "/download/storage/v1/b/demo/o/docs%2Fchunked.txt?alt=media",
            Vec::new(),
        ),
    );
    assert_eq!(downloaded.status, 200);
    assert_eq!(downloaded.body, b"resumable body");
}
