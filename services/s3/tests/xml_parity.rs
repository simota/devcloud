//! Part 7 parity: the XML response builders reproduce legacy
//! `xml.NewEncoder(w).Encode` output byte-for-byte, against oracles captured in
//! `/tmp/s3_oracle_xml_*`.

use devcloud_s3::responses::*;

fn matches(got: Vec<u8>, fixture: &[u8], label: &str) {
    assert_eq!(
        String::from_utf8(got).unwrap(),
        String::from_utf8_lossy(fixture),
        "{label}"
    );
}

#[test]
fn error_matches_oracle() {
    matches(
        error_xml("NoSuchBucket", "bucket does not exist"),
        include_bytes!("fixtures/xml_error.xml"),
        "error",
    );
}

#[test]
fn buckets_match_oracle() {
    matches(
        list_all_my_buckets(&[
            ("alpha".to_string(), "2026-05-30T12:00:00Z".to_string()),
            ("bravo".to_string(), "2026-05-30T13:00:00Z".to_string()),
        ]),
        include_bytes!("fixtures/xml_buckets.xml"),
        "buckets",
    );
    matches(
        list_all_my_buckets(&[]),
        include_bytes!("fixtures/xml_buckets_empty.xml"),
        "buckets_empty",
    );
}

#[test]
fn acl_matches_oracle() {
    matches(
        access_control_policy("public-read"),
        include_bytes!("fixtures/xml_acl.xml"),
        "acl",
    );
}

#[test]
fn location_and_versioning_match_oracle() {
    matches(
        location_constraint("us-east-1"),
        include_bytes!("fixtures/xml_location.xml"),
        "location",
    );
    matches(
        versioning_configuration("Enabled"),
        include_bytes!("fixtures/xml_versioning.xml"),
        "versioning",
    );
    matches(
        versioning_configuration(""),
        include_bytes!("fixtures/xml_versioning_empty.xml"),
        "versioning_empty",
    );
}

#[test]
fn copy_result_matches_oracle() {
    matches(
        copy_object_result("2026-05-30T12:00:00Z", "\"9a0364b9\""),
        include_bytes!("fixtures/xml_copy.xml"),
        "copy",
    );
}

#[test]
fn list_bucket_result_matches_oracle() {
    let result = ListBucketResult {
        name: "data".to_string(),
        prefix: "logs/".to_string(),
        delimiter: "/".to_string(),
        key_count: 2,
        max_keys: 1000,
        list_type: 2,
        contents: vec![ObjectElement {
            key: "logs/a&b.txt".to_string(),
            last_modified: "2026-05-30T12:00:00Z".to_string(),
            etag: "\"abc\"".to_string(),
            size: 3,
            storage_class: "STANDARD".to_string(),
        }],
        common_prefixes: vec!["logs/sub/".to_string()],
        ..Default::default()
    };
    matches(
        result.to_xml(),
        include_bytes!("fixtures/xml_listbucket.xml"),
        "listbucket",
    );

    let empty = ListBucketResult {
        name: "data".to_string(),
        max_keys: 1000,
        ..Default::default()
    };
    matches(
        empty.to_xml(),
        include_bytes!("fixtures/xml_listbucket_empty.xml"),
        "listbucket_empty",
    );
}

#[test]
fn list_versions_result_matches_oracle() {
    let result = ListVersionsResult {
        name: "data".to_string(),
        max_keys: 1000,
        versions: vec![VersionElement {
            key: "k".to_string(),
            version_id: "v1".to_string(),
            is_latest: true,
            last_modified: "2026-05-30T12:00:00Z".to_string(),
            etag: "\"abc\"".to_string(),
            size: 3,
            storage_class: "STANDARD".to_string(),
        }],
        delete_markers: vec![DeleteMarkerElement {
            key: "k".to_string(),
            version_id: "v2".to_string(),
            is_latest: false,
            last_modified: "2026-05-30T13:00:00Z".to_string(),
        }],
        ..Default::default()
    };
    matches(
        result.to_xml(),
        include_bytes!("fixtures/xml_listversions.xml"),
        "listversions",
    );
}

#[test]
fn continuation_token_round_trips() {
    let token = encode_continuation_token("logs/x.txt");
    assert_eq!(token, "eyJsYXN0S2V5IjoibG9ncy94LnR4dCJ9");
    assert_eq!(decode_continuation_token(&token).unwrap(), "logs/x.txt");
}

#[test]
fn encode_list_value_url() {
    assert_eq!(encode_list_value("a b/c", ""), "a b/c");
    assert_eq!(encode_list_value("a b/c~d", "url"), "a%20b%2Fc~d");
}

#[test]
fn object_listing_delimiter_groups_common_prefixes() {
    use devcloud_s3::model::Object;
    let objects: Vec<Object> = [
        "logs/2026/a.txt",
        "logs/2026/b.txt",
        "logs/2027/c.txt",
        "top.txt",
    ]
    .iter()
    .map(|k| Object {
        key: k.to_string(),
        ..Default::default()
    })
    .collect();
    let listing = build_object_listing(&objects, "logs/", "/", "", 1000);
    assert_eq!(listing.common_prefixes, vec!["logs/2026/", "logs/2027/"]);
    // "top.txt" has no "logs/" prefix; with delimiter it stays a content object.
    assert_eq!(listing.contents.len(), 1);
    assert_eq!(listing.contents[0].key, "top.txt");
}
