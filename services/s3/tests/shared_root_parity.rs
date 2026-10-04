//! Conditional writes stay atomic when several `FileBucketStore` instances share
//! one storage root, as S3, GCS, BigQuery, and Redshift do under the
//! orchestrator.

use devcloud_s3::objops::{CreateMultipartUploadInput, PutObjectInput};
use devcloud_s3::store::{FileBucketStore, StoreError};

fn tempdir() -> std::path::PathBuf {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let mut dir = std::env::temp_dir();
    dir.push(format!("devcloud-s3-shared-{}-{}", std::process::id(), n));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn put_input(key: &str, body: &[u8]) -> PutObjectInput {
    PutObjectInput {
        bucket: "data".to_string(),
        key: key.to_string(),
        body: body.to_vec(),
        ..Default::default()
    }
}

#[test]
fn conditional_puts_across_instances_admit_one_writer() {
    let root = tempdir();
    let first = FileBucketStore::new(&root);
    // A differently spelled path to the same root must share the lock.
    let second = FileBucketStore::new(root.join("."));
    first.create_bucket("data").unwrap();

    let results: Vec<bool> = std::thread::scope(|scope| {
        let handles: Vec<_> = (0..16)
            .map(|i| {
                let store = if i % 2 == 0 { &first } else { &second };
                scope.spawn(move || {
                    match store.put_object_if_absent(put_input("lock", format!("w{i}").as_bytes()))
                    {
                        Ok(_) => true,
                        Err(StoreError::PreconditionFailed) => false,
                        Err(e) => panic!("unexpected error: {e:?}"),
                    }
                })
            })
            .collect();
        handles.into_iter().map(|h| h.join().unwrap()).collect()
    });
    assert_eq!(results.iter().filter(|ok| **ok).count(), 1);
}

/// A plain PUT from another instance races a conditional multipart completion.
/// Whichever order they serialize in, the plain PUT's body must end up current:
/// either it lands first (completion gets 412) or last (it overwrites). The
/// completion must never pass its existence check and then clobber it.
#[test]
fn conditional_complete_never_overwrites_concurrent_plain_put() {
    let root = tempdir();
    let s3 = FileBucketStore::new(&root);
    let gcs = FileBucketStore::new(&root);
    s3.create_bucket("data").unwrap();
    let part = vec![b'm'; 4 * 1024 * 1024];

    for round in 0..20 {
        let key = format!("obj-{round}");
        let upload = s3
            .create_multipart_upload(CreateMultipartUploadInput {
                bucket: "data".to_string(),
                key: key.clone(),
                ..Default::default()
            })
            .unwrap();
        for part_number in 1..=2 {
            s3.upload_part("data", &key, &upload.upload_id, part_number, &part, "")
                .unwrap();
        }

        let completed = std::thread::scope(|scope| {
            let complete = scope.spawn(|| {
                s3.complete_multipart_upload_if_absent("data", &key, &upload.upload_id, &[1, 2])
            });
            gcs.put_object(put_input(&key, b"plain")).unwrap();
            complete.join().unwrap()
        });
        match completed {
            Ok(Some(_)) | Err(StoreError::PreconditionFailed) => {}
            other => panic!("unexpected completion result: {other:?}"),
        }
        let (_, body) = s3.get_object("data", &key).unwrap().unwrap();
        assert_eq!(
            body, b"plain",
            "round {round}: conditional completion clobbered"
        );
    }
}
