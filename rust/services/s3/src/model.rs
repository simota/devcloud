//! The S3 persistence model — bucket, object, and multipart metadata structs.
//!
//! Field order and `serde` attributes reproduce the Go `encoding/json` output
//! byte-for-byte:
//!
//!  - Fields are declared in the **same order** as the Go structs in `types.go`
//!    (serde derive preserves declaration order).
//!  - Go `omitempty` on a **string / int / bool / map / pointer** is reproduced
//!    with `skip_serializing_if`. Go `omitempty` on a **struct** field is a
//!    no-op (the empty struct still serializes as `{}`), so those fields carry
//!    no `skip_serializing_if` and always emit.
//!  - Time fields (`createdAt`/`lastModified`/`updatedAt`) serialize as
//!    RFC3339Nano strings and always emit (Go's `time.Time` ignores `omitempty`).
//!  - `#[serde(default)]` at the container level makes decoding tolerant of
//!    missing keys, matching `json.Unmarshal`.
//!  - The Go structs' `xml.Name` fields are `json:"-"` and are omitted here; the
//!    XML response layer (a later part) models XML separately.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

fn is_zero_i64(n: &i64) -> bool {
    *n == 0
}

fn is_false(b: &bool) -> bool {
    !*b
}

/// The version ID Go writes for objects in a versioning-suspended bucket.
pub const NULL_VERSION_ID: &str = "null";

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct Bucket {
    pub name: String,
    #[serde(rename = "createdAt")]
    pub created_at: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub versioning: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub acl: String,
    #[serde(rename = "objectLockConfig")]
    pub object_lock_config: ObjectLockConfiguration,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct ObjectLockConfiguration {
    #[serde(skip_serializing_if = "String::is_empty")]
    pub xmlns: String,
    #[serde(rename = "objectLockEnabled", skip_serializing_if = "String::is_empty")]
    pub object_lock_enabled: String,
    pub rule: ObjectLockRule,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct ObjectLockRule {
    #[serde(rename = "defaultRetention")]
    pub default_retention: DefaultRetention,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct DefaultRetention {
    #[serde(skip_serializing_if = "String::is_empty")]
    pub mode: String,
    #[serde(skip_serializing_if = "is_zero_i64")]
    pub days: i64,
    #[serde(skip_serializing_if = "is_zero_i64")]
    pub years: i64,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct ServerSideEncryption {
    #[serde(skip_serializing_if = "String::is_empty")]
    pub algorithm: String,
    #[serde(rename = "kmsKeyId", skip_serializing_if = "String::is_empty")]
    pub kms_key_id: String,
    #[serde(rename = "bucketKeyEnabled", skip_serializing_if = "Option::is_none")]
    pub bucket_key_enabled: Option<bool>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct ObjectRetention {
    #[serde(skip_serializing_if = "String::is_empty")]
    pub mode: String,
    #[serde(rename = "retainUntilDate", skip_serializing_if = "String::is_empty")]
    pub retain_until_date: String,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct ObjectLegalHold {
    #[serde(skip_serializing_if = "String::is_empty")]
    pub status: String,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct Object {
    pub bucket: String,
    pub key: String,
    pub etag: String,
    pub size: i64,
    #[serde(rename = "createdAt")]
    pub created_at: String,
    #[serde(rename = "lastModified")]
    pub last_modified: String,
    #[serde(rename = "updatedAt")]
    pub updated_at: String,
    #[serde(skip_serializing_if = "is_zero_i64")]
    pub metageneration: i64,
    #[serde(rename = "contentType", skip_serializing_if = "String::is_empty")]
    pub content_type: String,
    #[serde(rename = "contentEncoding", skip_serializing_if = "String::is_empty")]
    pub content_encoding: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub crc32c: String,
    #[serde(rename = "cacheControl", skip_serializing_if = "String::is_empty")]
    pub cache_control: String,
    #[serde(
        rename = "contentDisposition",
        skip_serializing_if = "String::is_empty"
    )]
    pub content_disposition: String,
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: BTreeMap<String, String>,
    #[serde(rename = "versionId", skip_serializing_if = "String::is_empty")]
    pub version_id: String,
    #[serde(rename = "deleteMarker", skip_serializing_if = "is_false")]
    pub delete_marker: bool,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub acl: String,
    pub encryption: ServerSideEncryption,
    pub retention: ObjectRetention,
    #[serde(rename = "legalHold")]
    pub legal_hold: ObjectLegalHold,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct MultipartUpload {
    pub bucket: String,
    pub key: String,
    #[serde(rename = "uploadId")]
    pub upload_id: String,
    #[serde(rename = "createdAt")]
    pub created_at: String,
    #[serde(rename = "contentType", skip_serializing_if = "String::is_empty")]
    pub content_type: String,
    #[serde(rename = "contentEncoding", skip_serializing_if = "String::is_empty")]
    pub content_encoding: String,
    #[serde(rename = "cacheControl", skip_serializing_if = "String::is_empty")]
    pub cache_control: String,
    #[serde(
        rename = "contentDisposition",
        skip_serializing_if = "String::is_empty"
    )]
    pub content_disposition: String,
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: BTreeMap<String, String>,
    pub encryption: ServerSideEncryption,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct LifecycleConfiguration {
    #[serde(skip_serializing_if = "String::is_empty")]
    pub xmlns: String,
    pub rules: Vec<LifecycleRule>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct LifecycleRule {
    #[serde(skip_serializing_if = "String::is_empty")]
    pub id: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub prefix: String,
    pub filter: LifecycleFilter,
    pub status: String,
    pub expiration: LifecycleExpiration,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct LifecycleFilter {
    #[serde(skip_serializing_if = "String::is_empty")]
    pub prefix: String,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct LifecycleExpiration {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub days: Option<i64>,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub date: String,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct MultipartPart {
    #[serde(rename = "partNumber")]
    pub part_number: i64,
    pub etag: String,
    pub size: i64,
    #[serde(rename = "lastModified")]
    pub last_modified: String,
}
