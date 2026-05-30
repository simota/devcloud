//! Part 1 parity: the bucket/object metadata JSON and the helper outputs must
//! match golden oracles captured from the Go S3 store (`writeJSONFile`,
//! `crc32cBase64`, `multipartETag`).

use devcloud_s3::go_json::to_vec_indent;
use devcloud_s3::hashes::{crc32c_base64, multipart_etag};
use devcloud_s3::model::{
    Bucket, DefaultRetention, Object, ObjectLegalHold, ObjectLockConfiguration, ObjectLockRule,
    ObjectRetention, ServerSideEncryption,
};
use std::collections::BTreeMap;

fn matches(got: Vec<u8>, fixture: &[u8], label: &str) {
    assert_eq!(
        String::from_utf8(got).unwrap(),
        String::from_utf8_lossy(fixture),
        "{label}"
    );
}

#[test]
fn bucket_min_matches_oracle() {
    let bucket = Bucket {
        name: "my-bucket".to_string(),
        created_at: "2026-05-30T12:00:00Z".to_string(),
        ..Default::default()
    };
    matches(
        to_vec_indent(&bucket),
        include_bytes!("fixtures/bucket_min.json"),
        "bucket_min",
    );
}

#[test]
fn bucket_full_matches_oracle() {
    let bucket = Bucket {
        name: "my-bucket".to_string(),
        created_at: "2026-05-30T12:00:00.123456789Z".to_string(),
        versioning: "Enabled".to_string(),
        acl: "public-read".to_string(),
        object_lock_config: ObjectLockConfiguration {
            xmlns: "http://s3.amazonaws.com/doc/2006-03-01/".to_string(),
            object_lock_enabled: "Enabled".to_string(),
            rule: ObjectLockRule {
                default_retention: DefaultRetention {
                    mode: "GOVERNANCE".to_string(),
                    days: 30,
                    years: 0,
                },
            },
        },
    };
    matches(
        to_vec_indent(&bucket),
        include_bytes!("fixtures/bucket_full.json"),
        "bucket_full",
    );
}

#[test]
fn object_basic_matches_oracle() {
    let mut metadata = BTreeMap::new();
    metadata.insert("x-amz-meta-foo".to_string(), "bar".to_string());
    metadata.insert("x-amz-meta-baz".to_string(), "qux".to_string());
    let object = Object {
        bucket: "my-bucket".to_string(),
        key: "path/to/object.txt".to_string(),
        etag: "\"9a0364b9e99bb480dd25e1f0284c8555\"".to_string(),
        size: 7,
        created_at: "2026-05-30T12:00:00.123456789Z".to_string(),
        last_modified: "2026-05-30T12:00:00.123456789Z".to_string(),
        updated_at: "2026-05-30T12:00:00.123456789Z".to_string(),
        metageneration: 1,
        content_type: "application/octet-stream".to_string(),
        crc32c: crc32c_base64(b"content"),
        metadata,
        encryption: ServerSideEncryption {
            algorithm: "AES256".to_string(),
            bucket_key_enabled: Some(true),
            ..Default::default()
        },
        retention: ObjectRetention::default(),
        legal_hold: ObjectLegalHold::default(),
        ..Default::default()
    };
    matches(
        to_vec_indent(&object),
        include_bytes!("fixtures/object_basic.json"),
        "object_basic",
    );
}

#[test]
fn object_full_matches_oracle() {
    let object = Object {
        bucket: "my-bucket".to_string(),
        key: "k".to_string(),
        etag: "\"d41d8cd98f00b204e9800998ecf8427e\"".to_string(),
        size: 0,
        created_at: "2026-05-30T12:00:00.123456789Z".to_string(),
        last_modified: "2026-05-30T12:00:00.123456789Z".to_string(),
        updated_at: "2026-05-30T12:00:00.123456789Z".to_string(),
        metageneration: 1,
        content_type: "text/plain".to_string(),
        content_encoding: "gzip".to_string(),
        crc32c: crc32c_base64(b""),
        cache_control: "max-age=3600".to_string(),
        content_disposition: "attachment".to_string(),
        version_id: "abc123".to_string(),
        delete_marker: true,
        acl: "private".to_string(),
        encryption: ServerSideEncryption {
            algorithm: "aws:kms".to_string(),
            kms_key_id: "key-1".to_string(),
            bucket_key_enabled: None,
        },
        retention: ObjectRetention {
            mode: "GOVERNANCE".to_string(),
            retain_until_date: "2026-06-30T12:00:00Z".to_string(),
        },
        legal_hold: ObjectLegalHold {
            status: "ON".to_string(),
        },
        ..Default::default()
    };
    matches(
        to_vec_indent(&object),
        include_bytes!("fixtures/object_full.json"),
        "object_full",
    );
}

#[test]
fn metadata_json_round_trips() {
    // Decoding back is tolerant and lossless for a fully-populated object.
    let json = include_bytes!("fixtures/object_full.json");
    let object: Object = serde_json::from_slice(json).unwrap();
    assert_eq!(object.bucket, "my-bucket");
    assert_eq!(object.version_id, "abc123");
    assert!(object.delete_marker);
    assert_eq!(to_vec_indent(&object), json);
}

#[test]
fn helper_vectors() {
    assert_eq!(crc32c_base64(b"content"), "Ya91Mw==");
    assert_eq!(crc32c_base64(b""), "AAAAAA==");
    assert_eq!(crc32c_base64(b"hello world"), "yZRlqg==");
    assert_eq!(
        multipart_etag(&[
            "\"9a0364b9e99bb480dd25e1f0284c8555\"".to_string(),
            "\"5eb63bbbe01eeed093cb22bb8f5acdc3\"".to_string(),
        ]),
        "\"5812295871323bf83b6f30d96ac98cf0-2\""
    );
    assert_eq!(
        multipart_etag(&["\"9a0364b9e99bb480dd25e1f0284c8555\"".to_string()]),
        "\"73ad9750e8d5fcf7936433620b4baa21-1\""
    );
    assert_eq!(multipart_etag(&["not-hex".to_string()]), "\"1\"");
}
