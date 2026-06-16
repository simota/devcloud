//! Part 3 parity: the object data plane (put/get/delete/list) and versioning
//! (delete markers, version listings, current-object rebuild) reproduce the legacy
//! store behavior captured in `/tmp/s3_oracle_objects.txt`, and the delete-marker
//! `object.json` matches the legacy zero-time render byte-for-byte.

use devcloud_s3::model::Object;
use devcloud_s3::objops::PutObjectInput;
use devcloud_s3::store::FileBucketStore;
use devcloud_s3::time_fmt::GO_ZERO_TIME;
use devcloud_s3::wire_json::to_vec_indent;

fn tempdir() -> std::path::PathBuf {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let mut dir = std::env::temp_dir();
    dir.push(format!("devcloud-s3-obj-{}-{}", std::process::id(), n));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn put(store: &FileBucketStore, bucket: &str, key: &str, body: &str) {
    store
        .put_object(PutObjectInput {
            bucket: bucket.to_string(),
            key: key.to_string(),
            body: body.as_bytes().to_vec(),
            ..Default::default()
        })
        .unwrap();
}

fn keys(objs: &[Object]) -> Vec<String> {
    objs.iter().map(|o| o.key.clone()).collect()
}

#[test]
fn delete_marker_json_matches_oracle() {
    // A delete marker leaves createdAt at the legacy zero time and always emits the
    // empty etag / struct fields.
    let marker = Object {
        bucket: "my-bucket".to_string(),
        key: "k".to_string(),
        created_at: GO_ZERO_TIME.to_string(),
        last_modified: "2026-05-30T12:00:00Z".to_string(),
        updated_at: "2026-05-30T12:00:00Z".to_string(),
        version_id: "v1".to_string(),
        delete_marker: true,
        ..Default::default()
    };
    assert_eq!(
        String::from_utf8(to_vec_indent(&marker)).unwrap(),
        include_str!("fixtures/delete_marker.json"),
    );
}

#[test]
fn non_versioned_data_plane_matches_oracle() {
    let store = FileBucketStore::new(tempdir());
    store.create_bucket("data").unwrap();
    put(&store, "data", "beta.txt", "B");
    put(&store, "data", "alpha.txt", "AAA");
    put(&store, "data", "logs/2026.txt", "L");

    let all = store.list_objects("data", "").unwrap().unwrap();
    assert_eq!(keys(&all), vec!["alpha.txt", "beta.txt", "logs/2026.txt"]);

    let logs = store.list_objects("data", "logs/").unwrap().unwrap();
    assert_eq!(keys(&logs), vec!["logs/2026.txt"]);

    let (_, body) = store.get_object("data", "alpha.txt").unwrap().unwrap();
    assert_eq!(body, b"AAA");
    assert!(store.get_object("data", "missing").unwrap().is_none());

    // PutObject defaults: content-type, metageneration, etag, crc32c.
    let (alpha, _) = store.get_object("data", "alpha.txt").unwrap().unwrap();
    assert_eq!(alpha.content_type, "application/octet-stream");
    assert_eq!(alpha.metageneration, 1);
    assert_eq!(alpha.size, 3);
    assert!(alpha.etag.starts_with('"'));

    assert!(store.delete_object("data", "beta.txt").unwrap());
    let after = store.list_objects("data", "").unwrap().unwrap();
    assert_eq!(keys(&after), vec!["alpha.txt", "logs/2026.txt"]);
}

#[test]
fn versioning_marker_and_rebuild_match_oracle() {
    let store = FileBucketStore::new(tempdir());
    store.create_bucket("vers").unwrap();
    store.put_bucket_versioning("vers", "Enabled").unwrap();
    store.push_version_ids(&["v1", "v2", "v3"]);

    put(&store, "vers", "v.txt", "one");
    put(&store, "vers", "v.txt", "two");
    let versions = store.list_object_versions("vers", "").unwrap().unwrap();
    assert_eq!(versions.len(), 2, "two puts -> two versions");

    let (marker, deleted) = store
        .delete_object_with_result("vers", "v.txt", false)
        .unwrap();
    assert!(deleted);
    assert!(marker.delete_marker);

    // GetObject hides the current delete marker.
    assert!(store.get_object("vers", "v.txt").unwrap().is_none());

    let versions2 = store.list_object_versions("vers", "").unwrap().unwrap();
    assert_eq!(versions2.len(), 3, "two data versions + one delete marker");
    assert_eq!(
        versions2.iter().filter(|v| v.delete_marker).count(),
        1,
        "exactly one delete marker"
    );

    // Deleting the marker (v3) rebuilds the current object to the latest data
    // version (v2 = "two").
    let (_, removed) = store
        .delete_object_version("vers", "v.txt", "v3", false)
        .unwrap();
    assert!(removed);
    let (current, body) = store.get_object("vers", "v.txt").unwrap().unwrap();
    assert!(!current.delete_marker);
    assert_eq!(body, b"two");
}

#[test]
fn suspended_versioning_uses_null_version_id() {
    let store = FileBucketStore::new(tempdir());
    store.create_bucket("sus").unwrap();
    store.put_bucket_versioning("sus", "Suspended").unwrap();
    put(&store, "sus", "k", "data");
    let (obj, _) = store.get_object("sus", "k").unwrap().unwrap();
    assert_eq!(obj.version_id, "null");
}

#[test]
fn update_object_metadata_bumps_metageneration() {
    let store = FileBucketStore::new(tempdir());
    store.create_bucket("data").unwrap();
    put(&store, "data", "k", "x");
    let updated = store
        .update_object_metadata(devcloud_s3::objops::UpdateObjectMetadataInput {
            bucket: "data".to_string(),
            key: "k".to_string(),
            content_type: "text/plain".to_string(),
            ..Default::default()
        })
        .unwrap()
        .unwrap();
    assert_eq!(updated.content_type, "text/plain");
    assert_eq!(updated.metageneration, 2);
}
