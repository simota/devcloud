//! Conditional writes stay atomic when several `FileBucketStore` instances share
//! one storage root, as S3, GCS, BigQuery, and Redshift do under the
//! orchestrator.

use devcloud_s3::model::Object;
use devcloud_s3::objops::{CreateMultipartUploadInput, PutObjectInput, UpdateObjectMetadataInput};
use devcloud_s3::store::{ConditionalError, FileBucketStore, StoreError};

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
fn provider_conditions_and_mutations_exclude_a_competing_writer() {
    use std::sync::mpsc;
    use std::time::Duration;

    for operation in ["put", "resumable", "metadata", "delete"] {
        let root = tempdir();
        let provider = FileBucketStore::new(&root);
        let s3 = FileBucketStore::new(root.join("."));
        provider.create_bucket("data").unwrap();
        let creates = operation == "put" || operation == "resumable";
        let original = if creates {
            None
        } else {
            Some(provider.put_object(put_input("key", b"original")).unwrap())
        };
        let (checked_tx, checked_rx) = mpsc::channel();
        let (continue_tx, continue_rx) = mpsc::channel();
        let (started_tx, started_rx) = mpsc::channel();
        let (done_tx, done_rx) = mpsc::channel();

        std::thread::scope(|scope| {
            let predicate = move |current: Option<&Object>| -> Result<(), &'static str> {
                assert_eq!(current, original.as_ref());
                checked_tx.send(()).unwrap();
                continue_rx.recv_timeout(Duration::from_secs(5)).unwrap();
                Ok(())
            };
            let mutation = scope.spawn(|| match operation {
                "put" => provider
                    .put_object_if(put_input("key", b"provider"), predicate)
                    .map(|_| ()),
                "resumable" => provider
                    .put_resumable_object_if(
                        put_input("key", b"provider"),
                        "0123456789abcdef0123456789abcdef",
                        predicate,
                    )
                    .map(|_| ()),
                "metadata" => provider
                    .update_object_metadata_if(
                        UpdateObjectMetadataInput {
                            bucket: "data".to_string(),
                            key: "key".to_string(),
                            content_type: "text/plain".to_string(),
                            ..Default::default()
                        },
                        predicate,
                    )
                    .map(|object| assert!(object.is_some())),
                "delete" => provider
                    .delete_object_if("data", "key", predicate)
                    .map(|deleted| assert!(deleted)),
                _ => unreachable!(),
            });
            checked_rx.recv_timeout(Duration::from_secs(5)).unwrap();
            let competitor = scope.spawn(|| {
                started_tx.send(()).unwrap();
                let result = if creates {
                    s3.put_object_if_absent(put_input("key", b"competitor"))
                } else {
                    s3.put_object(put_input("key", b"competitor"))
                };
                done_tx.send(result).unwrap();
            });
            started_rx.recv_timeout(Duration::from_secs(5)).unwrap();
            // The competing S3 write cannot complete after the provider has
            // inspected the current object but before its mutation commits.
            assert!(matches!(
                done_rx.recv_timeout(Duration::from_millis(100)),
                Err(mpsc::RecvTimeoutError::Timeout)
            ));
            continue_tx.send(()).unwrap();
            mutation.join().unwrap().unwrap();
            let competing_result = done_rx.recv_timeout(Duration::from_secs(5)).unwrap();
            competitor.join().unwrap();
            if creates {
                assert!(matches!(
                    competing_result,
                    Err(StoreError::PreconditionFailed)
                ));
            } else {
                competing_result.unwrap();
            }
        });
        let (object, body) = s3.get_object_snapshot("data", "key").unwrap().unwrap();
        if creates {
            assert_eq!(body, b"provider", "{operation}");
        } else {
            assert_eq!(body, b"competitor", "{operation}");
            assert_eq!(object.metageneration, 1, "{operation}");
            assert_eq!(object.content_type, "application/octet-stream");
        }
        std::fs::remove_dir_all(root).unwrap();
    }
}

#[test]
fn rejected_provider_conditions_leave_data_and_versions_unchanged() {
    let root = tempdir();
    let store = FileBucketStore::new(&root);
    store.create_bucket("data").unwrap();
    store.put_bucket_versioning("data", "Enabled").unwrap();
    let original = store.put_object(put_input("key", b"original")).unwrap();
    for operation in ["put", "resumable", "metadata", "delete"] {
        let predicate = |current: Option<&Object>| {
            assert_eq!(current, Some(&original));
            Err::<(), _>("provider condition failed")
        };
        let result = match operation {
            "put" => store
                .put_object_if(put_input("key", b"replacement"), predicate)
                .map(|_| ()),
            "resumable" => store
                .put_resumable_object_if(
                    put_input("key", b"replacement"),
                    "0123456789abcdef0123456789abcdef",
                    predicate,
                )
                .map(|_| ()),
            "metadata" => store
                .update_object_metadata_if(
                    UpdateObjectMetadataInput {
                        bucket: "data".to_string(),
                        key: "key".to_string(),
                        content_type: "text/plain".to_string(),
                        ..Default::default()
                    },
                    predicate,
                )
                .map(|_| ()),
            "delete" => store.delete_object_if("data", "key", predicate).map(|_| ()),
            _ => unreachable!(),
        };
        assert!(matches!(
            result,
            Err(ConditionalError::Condition("provider condition failed"))
        ));
        assert_eq!(
            store.get_object_snapshot("data", "key").unwrap().unwrap(),
            (original.clone(), b"original".to_vec()),
            "{operation}"
        );
        assert_eq!(
            store.list_object_versions("data", "key").unwrap().unwrap(),
            vec![original.clone()],
            "{operation}"
        );
    }
    assert!(!root
        .join(".gcs-upload-0123456789abcdef0123456789abcdef.json")
        .exists());
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn conditional_resumable_replay_returns_the_commit_before_checking_conditions() {
    for later_state in ["receipt", "marker", "overwrite", "delete", "delete-bucket"] {
        let root = tempdir();
        let store = FileBucketStore::new(&root);
        store.create_bucket("data").unwrap();
        let id = "0123456789abcdef0123456789abcdef";
        let original = store
            .put_resumable_object_if(put_input("key", b"original"), id, |current| {
                assert!(current.is_none());
                Ok::<(), &'static str>(())
            })
            .unwrap();
        match later_state {
            "receipt" => {}
            "marker" => std::fs::remove_file(root.join(format!(".gcs-upload-{id}.json"))).unwrap(),
            "overwrite" => {
                store.put_object(put_input("key", b"newer")).unwrap();
            }
            "delete" | "delete-bucket" => {
                store.delete_object("data", "key").unwrap();
                if later_state == "delete-bucket" {
                    store.delete_bucket("data").unwrap();
                }
            }
            _ => unreachable!(),
        }
        let replayed = store
            .put_resumable_object_if(
                put_input("key", b"must not replace"),
                id,
                |_| -> Result<(), &'static str> {
                    panic!("an existing commit must precede provider conditions")
                },
            )
            .unwrap();
        assert_eq!(replayed, original, "{later_state}");
        if later_state == "delete-bucket" {
            assert!(store.get_bucket("data").unwrap().is_none());
        } else {
            let current = store.get_object_snapshot("data", "key").unwrap();
            match later_state {
                "delete" => assert!(current.is_none()),
                "overwrite" => assert_eq!(current.unwrap().1, b"newer"),
                _ => assert_eq!(current.unwrap(), (original, b"original".to_vec())),
            }
        }
        std::fs::remove_dir_all(root).unwrap();
    }
}

#[test]
fn provider_condition_treats_delete_markers_as_absent_and_keeps_store_errors() {
    let root = tempdir();
    let store = FileBucketStore::new(&root);
    store.create_bucket("data").unwrap();
    store.put_bucket_versioning("data", "Enabled").unwrap();
    store.put_object(put_input("key", b"original")).unwrap();
    store.delete_object("data", "key").unwrap();
    store
        .put_object_if(put_input("key", b"recreated"), |current| {
            assert!(current.is_none());
            Ok::<(), &'static str>(())
        })
        .unwrap();
    let error = store.delete_object_if("missing", "key", |_| -> Result<(), &'static str> {
        panic!("a missing bucket is a store error")
    });
    assert!(matches!(
        error,
        Err(ConditionalError::Store(StoreError::BucketNotExist))
    ));
    assert_eq!(
        store.get_object_snapshot("data", "key").unwrap().unwrap().1,
        b"recreated"
    );
    std::fs::remove_dir_all(root).unwrap();
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
