//! Part 5 parity: bucket sub-resources (object-lock, lifecycle, policy, ACL) and
//! object-level lock ops (ACL/retention/legal-hold) reproduce the legacy store's
//! sidecar byte formats and behavior, against `/tmp/s3_oracle_config.txt`.

use devcloud_s3::model::{
    DefaultRetention, LifecycleConfiguration, LifecycleExpiration, LifecycleFilter, LifecycleRule,
    ObjectLegalHold, ObjectLockConfiguration, ObjectLockRule, ObjectRetention,
};
use devcloud_s3::objops::PutObjectInput;
use devcloud_s3::store::FileBucketStore;
use devcloud_s3::wire_json::to_vec_indent;

fn tempdir() -> std::path::PathBuf {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let mut dir = std::env::temp_dir();
    dir.push(format!("devcloud-s3-cfg-{}-{}", std::process::id(), n));
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

#[test]
fn object_lock_json_matches_oracle() {
    let olc = ObjectLockConfiguration {
        xmlns: "http://s3.amazonaws.com/doc/2006-03-01/".to_string(),
        object_lock_enabled: "Enabled".to_string(),
        rule: ObjectLockRule {
            default_retention: DefaultRetention {
                mode: "COMPLIANCE".to_string(),
                years: 2,
                days: 0,
            },
        },
    };
    assert_eq!(
        String::from_utf8(to_vec_indent(&olc)).unwrap(),
        include_str!("fixtures/objectlock.json")
    );
}

#[test]
fn lifecycle_json_matches_oracle() {
    let lc = LifecycleConfiguration {
        xmlns: "http://s3.amazonaws.com/doc/2006-03-01/".to_string(),
        rules: vec![
            LifecycleRule {
                id: "rule1".to_string(),
                prefix: "logs/".to_string(),
                filter: LifecycleFilter {
                    prefix: "logs/2026/".to_string(),
                },
                status: "Enabled".to_string(),
                expiration: LifecycleExpiration {
                    days: Some(30),
                    date: String::new(),
                },
            },
            LifecycleRule {
                status: "Disabled".to_string(),
                expiration: LifecycleExpiration {
                    days: None,
                    date: "2027-01-01T00:00:00Z".to_string(),
                },
                ..Default::default()
            },
        ],
    };
    assert_eq!(
        String::from_utf8(to_vec_indent(&lc)).unwrap(),
        include_str!("fixtures/lifecycle.json")
    );
}

#[test]
fn lifecycle_apply_expires_matching_objects() {
    let store = FileBucketStore::new(tempdir());
    store.create_bucket("data").unwrap();
    put(&store, "data", "logs/old.txt", "x");
    put(&store, "data", "keep.txt", "y");
    store
        .put_bucket_lifecycle(
            "data",
            &LifecycleConfiguration {
                rules: vec![LifecycleRule {
                    prefix: "logs/".to_string(),
                    status: "Enabled".to_string(),
                    expiration: LifecycleExpiration {
                        days: Some(1),
                        date: String::new(),
                    },
                    ..Default::default()
                }],
                ..Default::default()
            },
        )
        .unwrap();
    let (expired, exists) = store
        .apply_bucket_lifecycle("data", "2030-01-01T00:00:00Z")
        .unwrap();
    assert!(exists);
    assert_eq!(expired, 1);
    let remaining: Vec<String> = store
        .list_objects("data", "")
        .unwrap()
        .unwrap()
        .into_iter()
        .map(|o| o.key)
        .collect();
    assert_eq!(remaining, vec!["keep.txt"]);
}

#[test]
fn object_lock_ops_round_trip() {
    let store = FileBucketStore::new(tempdir());
    store.create_bucket("data").unwrap();
    put(&store, "data", "locked", "z");
    store
        .put_object_retention(
            "data",
            "locked",
            "",
            ObjectRetention {
                mode: "GOVERNANCE".to_string(),
                retain_until_date: "2031-01-01T00:00:00Z".to_string(),
            },
        )
        .unwrap();
    store
        .put_object_legal_hold(
            "data",
            "locked",
            "",
            ObjectLegalHold {
                status: "ON".to_string(),
            },
        )
        .unwrap();
    assert!(store
        .put_object_acl("data", "locked", "", "public-read")
        .unwrap());

    let ret = store
        .get_object_retention("data", "locked", "")
        .unwrap()
        .unwrap();
    assert_eq!(ret.mode, "GOVERNANCE");
    assert_eq!(ret.retain_until_date, "2031-01-01T00:00:00Z");
    let lh = store
        .get_object_legal_hold("data", "locked", "")
        .unwrap()
        .unwrap();
    assert_eq!(lh.status, "ON");
    assert_eq!(
        store.get_object_acl("data", "locked", "").unwrap().unwrap(),
        "public-read"
    );

    // Defaults.
    put(&store, "data", "plain", "p");
    assert_eq!(
        store.get_object_acl("data", "plain", "").unwrap().unwrap(),
        "private"
    );
    assert_eq!(store.get_bucket_acl("data").unwrap().unwrap(), "private");
}

#[test]
fn object_lock_blocks_object_deletion() {
    let store = FileBucketStore::new(tempdir());
    store.create_bucket("data").unwrap();
    put(&store, "data", "locked", "z");
    store
        .put_object_legal_hold(
            "data",
            "locked",
            "",
            ObjectLegalHold {
                status: "ON".to_string(),
            },
        )
        .unwrap();
    // A legal hold blocks deletion (non-versioned -> ObjectLocked error).
    assert!(matches!(
        store.delete_object_with_result("data", "locked", false),
        Err(devcloud_s3::store::StoreError::ObjectLocked)
    ));
}

#[test]
fn bucket_object_lock_and_policy_round_trip() {
    let store = FileBucketStore::new(tempdir());
    store.create_bucket("data").unwrap();

    let olc = ObjectLockConfiguration {
        object_lock_enabled: "Enabled".to_string(),
        ..Default::default()
    };
    store
        .put_bucket_object_lock_configuration("data", olc)
        .unwrap();
    let (got, configured) = store
        .get_bucket_object_lock_configuration("data")
        .unwrap()
        .unwrap();
    assert!(configured);
    assert_eq!(got.object_lock_enabled, "Enabled");

    store
        .put_bucket_policy("data", br#"{"Version":"2012"}"#)
        .unwrap();
    let (policy, present) = store.get_bucket_policy("data").unwrap().unwrap();
    assert!(present);
    assert_eq!(policy, br#"{"Version":"2012"}"#);
    assert!(store.delete_bucket_policy("data").unwrap());
    let (_, present2) = store.get_bucket_policy("data").unwrap().unwrap();
    assert!(!present2);
}
