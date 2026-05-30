//! Part 2 parity: the `FileBucketStore` bucket-CRUD plane reproduces the Go
//! store's on-disk layout, object-key path encoding, list ordering, and
//! delete-emptiness rules. The exact byte format of `bucket.json` is pinned in
//! `foundation_parity.rs`; here we verify structure and behavior against the Go
//! oracle (`/tmp/s3_oracle_store.txt`).

use devcloud_s3::store::{FileBucketStore, StoreError};
use std::collections::BTreeSet;
use std::path::Path;

fn tempdir() -> std::path::PathBuf {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let mut dir = std::env::temp_dir();
    dir.push(format!("devcloud-s3-store-{}-{}", std::process::id(), n));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// Sorted set of paths under `root`, dirs suffixed with `/` (matches the Go
/// oracle's `relTree`).
fn rel_tree(root: &Path) -> Vec<String> {
    fn walk(root: &Path, dir: &Path, out: &mut BTreeSet<String>) {
        for entry in std::fs::read_dir(dir).unwrap() {
            let entry = entry.unwrap();
            let path = entry.path();
            let rel = path
                .strip_prefix(root)
                .unwrap()
                .to_string_lossy()
                .into_owned();
            if entry.file_type().unwrap().is_dir() {
                out.insert(format!("{rel}/"));
                walk(root, &path, out);
            } else {
                out.insert(rel);
            }
        }
    }
    let mut out = BTreeSet::new();
    walk(root, root, &mut out);
    out.into_iter().collect()
}

#[test]
fn bucket_crud_matches_oracle() {
    let root = tempdir();
    let store = FileBucketStore::new(&root);

    let (_, c1) = store.create_bucket("bravo").unwrap();
    let (_, c2) = store.create_bucket("alpha").unwrap();
    let (_, c3) = store.create_bucket("alpha").unwrap();
    assert!(c1, "create bravo -> created");
    assert!(c2, "create alpha -> created");
    assert!(!c3, "duplicate alpha -> not created");

    let names: Vec<String> = store
        .list_buckets()
        .unwrap()
        .into_iter()
        .map(|b| b.name)
        .collect();
    assert_eq!(names, vec!["alpha", "bravo"], "list sorted by name");

    assert!(store.get_bucket("alpha").unwrap().is_some());
    assert!(store.get_bucket("missing").unwrap().is_none());

    assert_eq!(
        rel_tree(&root),
        vec!["alpha/", "alpha/bucket.json", "bravo/", "bravo/bucket.json",]
    );

    // Object-path encodings (relative to root), pinned to the Go oracle.
    let cases = [
        ("simple.txt", "alpha/objects/c2ltcGxlLnR4dA"),
        (
            "path/to/object.txt",
            "alpha/objects/cGF0aC90by9vYmplY3QudHh0",
        ),
        ("a b+c", "alpha/objects/YSBiK2M"),
        ("日本語.txt", "alpha/objects/5pel5pys6KqeLnR4dA"),
    ];
    for (key, want) in cases {
        let rel = store
            .object_path("alpha", key)
            .strip_prefix(&root)
            .unwrap()
            .to_string_lossy()
            .into_owned();
        assert_eq!(rel, want, "object_path({key})");
    }

    assert!(store.delete_bucket("alpha").unwrap(), "delete alpha");
    assert!(!store.delete_bucket("missing").unwrap(), "delete missing");
    assert_eq!(rel_tree(&root), vec!["bravo/", "bravo/bucket.json"]);
}

#[test]
fn delete_rejects_non_empty_bucket() {
    let root = tempdir();
    let store = FileBucketStore::new(&root);
    store.create_bucket("data").unwrap();
    // An object directory makes the bucket non-empty.
    std::fs::create_dir_all(store.objects_path("data").join("xyz")).unwrap();
    match store.delete_bucket("data") {
        Err(StoreError::BucketNotEmpty) => {}
        other => panic!("expected BucketNotEmpty, got {other:?}"),
    }
}

#[test]
fn metadata_is_deterministic_with_fixed_clock() {
    let root = tempdir();
    let mut store = FileBucketStore::new(&root);
    store.set_fixed_now("2026-05-30T12:00:00Z");
    let (bucket, _) = store.create_bucket("my-bucket").unwrap();
    assert_eq!(bucket.created_at, "2026-05-30T12:00:00Z");
    let on_disk = std::fs::read(store.bucket_path("my-bucket").join("bucket.json")).unwrap();
    assert_eq!(
        String::from_utf8(on_disk).unwrap(),
        include_str!("fixtures/bucket_min.json"),
    );
}

#[test]
fn invalid_bucket_name_rejected() {
    let store = FileBucketStore::new(tempdir());
    assert!(matches!(
        store.create_bucket("ab"),
        Err(StoreError::InvalidBucketName)
    ));
}
