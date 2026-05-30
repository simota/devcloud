//! Part 4 parity: the multipart plane (create/upload-part/list/complete/abort)
//! reproduces the Go store's `upload.json`/`part.json` byte format, part-path
//! encoding, completion ETag/body, and listing order, against the oracle in
//! `/tmp/s3_oracle_multipart.txt`.

use devcloud_s3::go_json::to_vec_indent;
use devcloud_s3::model::{MultipartPart, MultipartUpload, ServerSideEncryption};
use devcloud_s3::objops::CreateMultipartUploadInput;
use devcloud_s3::store::FileBucketStore;
use std::collections::BTreeMap;

fn tempdir() -> std::path::PathBuf {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let mut dir = std::env::temp_dir();
    dir.push(format!("devcloud-s3-mp-{}-{}", std::process::id(), n));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

const UID: &str = "0123456789abcdef0123456789abcdef";

#[test]
fn upload_and_part_json_match_oracle() {
    let mut metadata = BTreeMap::new();
    metadata.insert("x-amz-meta-a".to_string(), "1".to_string());
    metadata.insert("x-amz-meta-b".to_string(), "2".to_string());
    let full = MultipartUpload {
        bucket: "data".to_string(),
        key: "big.bin".to_string(),
        upload_id: UID.to_string(),
        created_at: "2026-05-30T12:00:00Z".to_string(),
        content_type: "text/plain".to_string(),
        content_encoding: "gzip".to_string(),
        cache_control: "max-age=60".to_string(),
        content_disposition: "attachment".to_string(),
        metadata,
        encryption: ServerSideEncryption {
            algorithm: "AES256".to_string(),
            ..Default::default()
        },
    };
    assert_eq!(
        String::from_utf8(to_vec_indent(&full)).unwrap(),
        include_str!("fixtures/upload_full.json")
    );

    let min = MultipartUpload {
        bucket: "data".to_string(),
        key: "big.bin".to_string(),
        upload_id: UID.to_string(),
        created_at: "2026-05-30T12:00:00Z".to_string(),
        content_type: "application/octet-stream".to_string(),
        ..Default::default()
    };
    assert_eq!(
        String::from_utf8(to_vec_indent(&min)).unwrap(),
        include_str!("fixtures/upload_min.json")
    );

    let part = MultipartPart {
        part_number: 1,
        etag: "\"5d41402abc4b2a76b9719d911017c592\"".to_string(),
        size: 5,
        last_modified: "2026-05-30T12:00:00Z".to_string(),
    };
    assert_eq!(
        String::from_utf8(to_vec_indent(&part)).unwrap(),
        include_str!("fixtures/part.json")
    );
}

#[test]
fn multipart_flow_matches_oracle() {
    let root = tempdir();
    let store = FileBucketStore::new(&root);
    store.create_bucket("data").unwrap();
    store.push_version_ids(&[UID]);

    let upload = store
        .create_multipart_upload(CreateMultipartUploadInput {
            bucket: "data".to_string(),
            key: "big.bin".to_string(),
            ..Default::default()
        })
        .unwrap();
    assert_eq!(upload.upload_id.len(), 32);

    // Upload out of order.
    store
        .upload_part("data", "big.bin", UID, 2, b"WORLD", "")
        .unwrap();
    store
        .upload_part("data", "big.bin", UID, 1, b"hello", "")
        .unwrap();

    let (_, parts) = store.list_parts("data", "big.bin", UID).unwrap().unwrap();
    let nums: Vec<i64> = parts.iter().map(|p| p.part_number).collect();
    assert_eq!(nums, vec![1, 2], "parts sorted by number");

    // Part-path zero-padding.
    let rel = store
        .multipart_part_path("data", UID, 42)
        .strip_prefix(&root)
        .unwrap()
        .to_string_lossy()
        .into_owned();
    assert_eq!(rel, format!("data/multipart/{UID}/parts/00042"));

    let object = store
        .complete_multipart_upload("data", "big.bin", UID, &[1, 2])
        .unwrap()
        .unwrap();
    assert_eq!(object.etag, "\"0dc92aaf05380431066db92c24d4870c-2\"");
    assert_eq!(object.size, 10);

    let (_, body) = store.get_object("data", "big.bin").unwrap().unwrap();
    assert_eq!(body, b"helloWORLD");

    // Multipart dir removed after completion.
    assert!(!store.multipart_path("data").exists());
}

#[test]
fn list_uploads_and_abort_match_oracle() {
    let store = FileBucketStore::new(tempdir());
    store.create_bucket("data").unwrap();
    store.push_version_ids(&[
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
    ]);
    let u1 = store
        .create_multipart_upload(CreateMultipartUploadInput {
            bucket: "data".to_string(),
            key: "zeta".to_string(),
            ..Default::default()
        })
        .unwrap();
    store
        .create_multipart_upload(CreateMultipartUploadInput {
            bucket: "data".to_string(),
            key: "alpha".to_string(),
            ..Default::default()
        })
        .unwrap();

    let uploads = store.list_multipart_uploads("data").unwrap().unwrap();
    let keys: Vec<String> = uploads.iter().map(|u| u.key.clone()).collect();
    assert_eq!(keys, vec!["alpha", "zeta"], "sorted by key");

    assert!(store
        .abort_multipart_upload("data", "zeta", &u1.upload_id)
        .unwrap());
    assert_eq!(
        store.list_multipart_uploads("data").unwrap().unwrap().len(),
        1
    );
}
