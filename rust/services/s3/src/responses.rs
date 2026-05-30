//! S3 XML response builders and listing logic, ported from the Go `responses.go`
//! and `listing.go`. Each builder produces an [`Element`] tree serialized to
//! byte-exact XML via [`crate::xml::encode`]; `omitempty` is applied by
//! conditionally adding child elements.

use crate::model::{Object, NULL_VERSION_ID};
use crate::percent::aws_percent_encode;
use crate::xml::{encode, Element};
use serde::{Deserialize, Serialize};

const XMLNS: &str = "http://s3.amazonaws.com/doc/2006-03-01/";

fn bool_str(b: bool) -> String {
    if b {
        "true".to_string()
    } else {
        "false".to_string()
    }
}

/// `<Error>` response body.
pub fn error_xml(code: &str, message: &str) -> Vec<u8> {
    encode(
        &Element::new("Error")
            .text_child("Code", code)
            .text_child("Message", message),
    )
}

/// `<ListAllMyBucketsResult>` for `GET /`.
pub fn list_all_my_buckets(buckets: &[(String, String)]) -> Vec<u8> {
    let mut bucket_list = Element::new("Buckets");
    for (name, creation_date) in buckets {
        bucket_list = bucket_list.child(
            Element::new("Bucket")
                .text_child("Name", name)
                .text_child("CreationDate", creation_date),
        );
    }
    encode(
        &Element::new("ListAllMyBucketsResult")
            .attr("xmlns", XMLNS)
            .child(owner_element())
            .child(bucket_list),
    )
}

fn owner_element() -> Element {
    Element::new("Owner")
        .text_child("ID", "devcloud")
        .text_child("DisplayName", "devcloud")
}

/// `<AccessControlPolicy>` for an ACL GET; `acl` empty defaults to `private`.
pub fn access_control_policy(acl: &str) -> Vec<u8> {
    let acl = if acl.is_empty() { "private" } else { acl };
    let permission = if matches!(
        acl,
        "public-read" | "authenticated-read" | "bucket-owner-read"
    ) {
        "READ"
    } else {
        "FULL_CONTROL"
    };
    let grantee = Element::new("Grantee")
        .attr("xmlns:xsi", "http://www.w3.org/2001/XMLSchema-instance")
        .attr("xsi:type", "CanonicalUser")
        .text_child("ID", "devcloud")
        .text_child("DisplayName", "devcloud");
    let grant = Element::new("Grant")
        .child(grantee)
        .text_child("Permission", permission);
    encode(
        &Element::new("AccessControlPolicy")
            .attr("xmlns", XMLNS)
            .child(owner_element())
            .child(Element::new("AccessControlList").child(grant))
            .text_child("CannedACL", acl),
    )
}

/// `<LocationConstraint>` for `GET /<bucket>?location`.
pub fn location_constraint(value: &str) -> Vec<u8> {
    encode(
        &Element::new("LocationConstraint")
            .attr("xmlns", XMLNS)
            .text(value),
    )
}

/// `<VersioningConfiguration>` for `GET /<bucket>?versioning`.
pub fn versioning_configuration(status: &str) -> Vec<u8> {
    let mut el = Element::new("VersioningConfiguration").attr("xmlns", XMLNS);
    if !status.is_empty() {
        el = el.text_child("Status", status);
    }
    encode(&el)
}

/// `<CopyObjectResult>`.
pub fn copy_object_result(last_modified: &str, etag: &str) -> Vec<u8> {
    encode(
        &Element::new("CopyObjectResult")
            .text_child("LastModified", last_modified)
            .text_child("ETag", etag),
    )
}

// --- object listing --------------------------------------------------------

/// The result of paginating a flat object list (the Go `objectListing`).
#[derive(Debug, Default, Clone)]
pub struct ObjectListing {
    pub contents: Vec<Object>,
    pub common_prefixes: Vec<String>,
    pub truncated: bool,
    pub next_marker: String,
    pub next_continuation_token: String,
}

/// The result of paginating a version list (the Go `versionListing`).
#[derive(Debug, Default, Clone)]
pub struct VersionListing {
    pub versions: Vec<Object>,
    pub truncated: bool,
    pub next_key_marker: String,
    pub next_version_id_marker: String,
}

/// The effective version ID of an object (empty -> `null`).
pub fn object_version_id(object: &Object) -> String {
    if object.version_id.is_empty() {
        NULL_VERSION_ID.to_string()
    } else {
        object.version_id.clone()
    }
}

/// Maps each key to its first-seen (latest) version ID. Mirrors the Go
/// `latestObjectVersionIDs`.
pub fn latest_object_version_ids(versions: &[Object]) -> std::collections::HashMap<String, String> {
    let mut latest = std::collections::HashMap::new();
    for object in versions {
        latest
            .entry(object.key.clone())
            .or_insert_with(|| object_version_id(object));
    }
    latest
}

/// Error from [`parse_max_keys`] when the `max-keys` value is malformed.
#[derive(Debug, PartialEq, Eq)]
pub struct InvalidMaxKeys;

/// Parses the `max-keys` query value: empty -> 1000, clamped to 1000, negative
/// is an error. Mirrors the Go `parseMaxKeys`.
pub fn parse_max_keys(value: &str) -> Result<i64, InvalidMaxKeys> {
    if value.is_empty() {
        return Ok(1000);
    }
    match value.parse::<i64>() {
        Ok(n) if n < 0 => Err(InvalidMaxKeys),
        Ok(n) if n > 1000 => Ok(1000),
        Ok(n) => Ok(n),
        Err(_) => Err(InvalidMaxKeys),
    }
}

#[derive(Serialize, Deserialize)]
struct ContinuationToken {
    #[serde(rename = "lastKey")]
    last_key: String,
}

/// Base64-RawURL of `{"lastKey":...}`. Mirrors the Go `encodeContinuationToken`.
pub fn encode_continuation_token(last_key: &str) -> String {
    let bytes = crate::go_json::to_vec_compact(&ContinuationToken {
        last_key: last_key.to_string(),
    });
    crate::base64::raw_url_encode(&bytes)
}

/// Decodes a continuation token to its `lastKey`. Mirrors the Go
/// `decodeContinuationToken`; `None` on malformed input.
pub fn decode_continuation_token(value: &str) -> Option<String> {
    let data = crate::base64::raw_url_decode(value)?;
    let token: ContinuationToken = serde_json::from_slice(&data).ok()?;
    Some(token.last_key)
}

/// URL-encodes a listing value when `encoding_type == "url"`. Mirrors the Go
/// `encodeListValue`.
pub fn encode_list_value(value: &str, encoding_type: &str) -> String {
    if encoding_type != "url" || value.is_empty() {
        value.to_string()
    } else {
        aws_percent_encode(value, "~-_.")
    }
}

/// Paginates `objects` into an [`ObjectListing`] honoring prefix/delimiter/
/// marker/max-keys. Mirrors the Go `buildObjectListing`.
pub fn build_object_listing(
    objects: &[Object],
    prefix: &str,
    delimiter: &str,
    marker: &str,
    max_keys: i64,
) -> ObjectListing {
    let mut listing = ObjectListing::default();
    if max_keys == 0 {
        return listing;
    }
    let mut marker = marker.to_string();
    let mut common_prefixes_seen = std::collections::HashSet::new();
    let mut count: i64 = 0;
    let mut i = 0;
    while i < objects.len() {
        let object = &objects[i];
        if !marker.is_empty() && object.key <= marker {
            i += 1;
            continue;
        }
        let mut item_key = object.key.clone();
        let mut item_is_object = true;
        let mut last_key_for_item = object.key.clone();
        if !delimiter.is_empty() {
            let remainder = object.key.strip_prefix(prefix).unwrap_or(&object.key);
            if let Some(index) = remainder.find(delimiter) {
                item_key = format!("{}{}", prefix, &remainder[..index + delimiter.len()]);
                item_is_object = false;
                while i + 1 < objects.len() && objects[i + 1].key.starts_with(&item_key) {
                    i += 1;
                    last_key_for_item = objects[i].key.clone();
                }
                if common_prefixes_seen.contains(&item_key) {
                    i += 1;
                    continue;
                }
            }
        }
        if count >= max_keys {
            listing.truncated = true;
            listing.next_marker = marker.clone();
            listing.next_continuation_token = encode_continuation_token(&marker);
            if listing.next_marker.is_empty() {
                listing.next_marker = object.key.clone();
                listing.next_continuation_token = encode_continuation_token(&object.key);
            }
            return listing;
        }
        if item_is_object {
            listing.contents.push(object.clone());
        } else {
            common_prefixes_seen.insert(item_key.clone());
            listing.common_prefixes.push(item_key);
        }
        count += 1;
        marker = last_key_for_item;
        i += 1;
    }
    listing
}

/// Paginates `versions` into a [`VersionListing`] honoring the key/version
/// markers and max-keys. Mirrors the Go `buildVersionListing`.
pub fn build_version_listing(
    versions: &[Object],
    key_marker: &str,
    version_id_marker: &str,
    max_keys: i64,
) -> VersionListing {
    let mut listing = VersionListing::default();
    if max_keys == 0 {
        return listing;
    }
    let mut started = key_marker.is_empty();
    for object in versions {
        let version_id = object_version_id(object);
        if !started {
            if object.key.as_str() < key_marker {
                continue;
            } else if object.key.as_str() > key_marker {
                started = true;
            } else if version_id_marker.is_empty() {
                continue;
            } else if version_id == version_id_marker {
                started = true;
                continue;
            } else {
                continue;
            }
        }
        if listing.versions.len() as i64 >= max_keys {
            listing.truncated = true;
            let last = listing.versions.last().unwrap();
            listing.next_key_marker = last.key.clone();
            listing.next_version_id_marker = object_version_id(last);
            return listing;
        }
        listing.versions.push(object.clone());
    }
    listing
}

// --- ListBucketResult / ListVersionsResult XML -----------------------------

/// Parameters for rendering `<ListBucketResult>`. Field order matches the Go
/// `listBucketResult` struct.
#[derive(Debug, Default, Clone)]
pub struct ListBucketResult {
    pub name: String,
    pub prefix: String,
    pub delimiter: String,
    pub marker: String,
    pub next_marker: String,
    pub continuation_token: String,
    pub next_continuation_token: String,
    pub start_after: String,
    pub key_count: i64,
    pub max_keys: i64,
    pub is_truncated: bool,
    pub list_type: i64,
    pub contents: Vec<ObjectElement>,
    pub common_prefixes: Vec<String>,
}

/// A `<Contents>` entry.
#[derive(Debug, Default, Clone)]
pub struct ObjectElement {
    pub key: String,
    pub last_modified: String,
    pub etag: String,
    pub size: i64,
    pub storage_class: String,
}

impl ListBucketResult {
    pub fn to_xml(&self) -> Vec<u8> {
        let mut el = Element::new("ListBucketResult")
            .attr("xmlns", XMLNS)
            .text_child("Name", &self.name)
            .text_child("Prefix", &self.prefix);
        if !self.delimiter.is_empty() {
            el = el.text_child("Delimiter", &self.delimiter);
        }
        if !self.marker.is_empty() {
            el = el.text_child("Marker", &self.marker);
        }
        if !self.next_marker.is_empty() {
            el = el.text_child("NextMarker", &self.next_marker);
        }
        if !self.continuation_token.is_empty() {
            el = el.text_child("ContinuationToken", &self.continuation_token);
        }
        if !self.next_continuation_token.is_empty() {
            el = el.text_child("NextContinuationToken", &self.next_continuation_token);
        }
        if !self.start_after.is_empty() {
            el = el.text_child("StartAfter", &self.start_after);
        }
        el = el
            .text_child("KeyCount", &self.key_count.to_string())
            .text_child("MaxKeys", &self.max_keys.to_string())
            .text_child("IsTruncated", &bool_str(self.is_truncated));
        if self.list_type != 0 {
            el = el.text_child("ListType", &self.list_type.to_string());
        }
        for c in &self.contents {
            el = el.child(
                Element::new("Contents")
                    .text_child("Key", &c.key)
                    .text_child("LastModified", &c.last_modified)
                    .text_child("ETag", &c.etag)
                    .text_child("Size", &c.size.to_string())
                    .text_child("StorageClass", &c.storage_class),
            );
        }
        for prefix in &self.common_prefixes {
            el = el.child(Element::new("CommonPrefixes").text_child("Prefix", prefix));
        }
        encode(&el)
    }
}

/// Parameters for rendering `<ListVersionsResult>`.
#[derive(Debug, Default, Clone)]
pub struct ListVersionsResult {
    pub name: String,
    pub prefix: String,
    pub key_marker: String,
    pub version_id_marker: String,
    pub next_key_marker: String,
    pub next_version_id_marker: String,
    pub max_keys: i64,
    pub is_truncated: bool,
    pub versions: Vec<VersionElement>,
    pub delete_markers: Vec<DeleteMarkerElement>,
}

/// A `<Version>` entry.
#[derive(Debug, Default, Clone)]
pub struct VersionElement {
    pub key: String,
    pub version_id: String,
    pub is_latest: bool,
    pub last_modified: String,
    pub etag: String,
    pub size: i64,
    pub storage_class: String,
}

/// A `<DeleteMarker>` entry.
#[derive(Debug, Default, Clone)]
pub struct DeleteMarkerElement {
    pub key: String,
    pub version_id: String,
    pub is_latest: bool,
    pub last_modified: String,
}

impl ListVersionsResult {
    pub fn to_xml(&self) -> Vec<u8> {
        let mut el = Element::new("ListVersionsResult")
            .attr("xmlns", XMLNS)
            .text_child("Name", &self.name)
            .text_child("Prefix", &self.prefix);
        if !self.key_marker.is_empty() {
            el = el.text_child("KeyMarker", &self.key_marker);
        }
        if !self.version_id_marker.is_empty() {
            el = el.text_child("VersionIdMarker", &self.version_id_marker);
        }
        if !self.next_key_marker.is_empty() {
            el = el.text_child("NextKeyMarker", &self.next_key_marker);
        }
        if !self.next_version_id_marker.is_empty() {
            el = el.text_child("NextVersionIdMarker", &self.next_version_id_marker);
        }
        el = el
            .text_child("MaxKeys", &self.max_keys.to_string())
            .text_child("IsTruncated", &bool_str(self.is_truncated));
        for v in &self.versions {
            el = el.child(
                Element::new("Version")
                    .text_child("Key", &v.key)
                    .text_child("VersionId", &v.version_id)
                    .text_child("IsLatest", &bool_str(v.is_latest))
                    .text_child("LastModified", &v.last_modified)
                    .text_child("ETag", &v.etag)
                    .text_child("Size", &v.size.to_string())
                    .text_child("StorageClass", &v.storage_class),
            );
        }
        for d in &self.delete_markers {
            el = el.child(
                Element::new("DeleteMarker")
                    .text_child("Key", &d.key)
                    .text_child("VersionId", &d.version_id)
                    .text_child("IsLatest", &bool_str(d.is_latest))
                    .text_child("LastModified", &d.last_modified),
            );
        }
        encode(&el)
    }
}
