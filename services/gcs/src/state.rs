//! File-backed GCS-specific bucket and object sidecar persistence.
//!
//! All state lives under `<bucket_path>/gcs/`, which `devcloud_s3`'s
//! `FileBucketStore::delete_bucket` treats as a metadata sidecar directory so
//! deleting an empty bucket removes it cleanly without reporting
//! `BucketNotEmpty`.

use std::collections::BTreeMap;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use devcloud_s3::base64;
use devcloud_s3::store::FileBucketStore;
use devcloud_s3::wire_json;
use serde::{Deserialize, Deserializer, Serialize};

fn is_false(v: &bool) -> bool {
    !*v
}

/// Accepts either a JSON string (`"86400"`) or integer (`86400`) and stores it
/// as a canonical string for GCS int64 wire compatibility.
pub fn deserialize_string_or_i64<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: Deserializer<'de>,
{
    let value = serde_json::Value::deserialize(deserializer)?;
    match value {
        serde_json::Value::String(s) => Ok(s),
        serde_json::Value::Number(n) => Ok(n.to_string()),
        serde_json::Value::Null => Ok(String::new()),
        _ => Err(serde::de::Error::custom("expected string or integer")),
    }
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct BucketRetentionPolicy {
    #[serde(
        rename = "retentionPeriod",
        deserialize_with = "deserialize_string_or_i64",
        default
    )]
    pub retention_period: String,
    #[serde(rename = "effectiveTime", skip_serializing_if = "String::is_empty")]
    pub effective_time: String,
    #[serde(rename = "isLocked", skip_serializing_if = "is_false")]
    pub is_locked: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct UniformBucketLevelAccess {
    pub enabled: bool,
    #[serde(rename = "lockedTime", skip_serializing_if = "String::is_empty")]
    pub locked_time: String,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct IamConfiguration {
    #[serde(rename = "uniformBucketLevelAccess")]
    pub uniform_bucket_level_access: UniformBucketLevelAccess,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct BucketObjectRetention {
    #[serde(skip_serializing_if = "String::is_empty")]
    pub mode: String,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct GcsLifecycleAction {
    #[serde(rename = "type")]
    pub action_type: String,
    #[serde(rename = "storageClass", skip_serializing_if = "String::is_empty")]
    pub storage_class: String,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct GcsLifecycleCondition {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub age: Option<i64>,
    #[serde(rename = "createdBefore", skip_serializing_if = "String::is_empty")]
    pub created_before: String,
    #[serde(rename = "isLive", skip_serializing_if = "Option::is_none")]
    pub is_live: Option<bool>,
    #[serde(rename = "matchesPrefix", skip_serializing_if = "Vec::is_empty")]
    pub matches_prefix: Vec<String>,
    #[serde(rename = "matchesSuffix", skip_serializing_if = "Vec::is_empty")]
    pub matches_suffix: Vec<String>,
    #[serde(rename = "matchesStorageClass", skip_serializing_if = "Vec::is_empty")]
    pub matches_storage_class: Vec<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct GcsLifecycleRule {
    pub action: GcsLifecycleAction,
    pub condition: GcsLifecycleCondition,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct GcsLifecycle {
    #[serde(default)]
    pub rule: Vec<GcsLifecycleRule>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct GcsBucketMeta {
    #[serde(skip_serializing_if = "String::is_empty")]
    pub location: String,
    #[serde(rename = "storageClass", skip_serializing_if = "String::is_empty")]
    pub storage_class: String,
    pub metageneration: i64,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub updated: String,
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    pub labels: BTreeMap<String, String>,
    #[serde(rename = "defaultEventBasedHold", skip_serializing_if = "is_false")]
    pub default_event_based_hold: bool,
    #[serde(rename = "retentionPolicy", skip_serializing_if = "Option::is_none")]
    pub retention_policy: Option<BucketRetentionPolicy>,
    #[serde(rename = "objectRetention", skip_serializing_if = "Option::is_none")]
    pub object_retention: Option<BucketObjectRetention>,
    #[serde(rename = "iamConfiguration", skip_serializing_if = "Option::is_none")]
    pub iam_configuration: Option<IamConfiguration>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lifecycle: Option<GcsLifecycle>,
}

impl Default for GcsBucketMeta {
    fn default() -> Self {
        Self {
            location: String::new(),
            storage_class: String::new(),
            metageneration: 1,
            updated: String::new(),
            labels: BTreeMap::new(),
            default_event_based_hold: false,
            retention_policy: None,
            object_retention: None,
            iam_configuration: None,
            lifecycle: None,
        }
    }
}

impl GcsBucketMeta {
    pub fn uniform_bucket_level_access_enabled(&self) -> bool {
        self.iam_configuration
            .as_ref()
            .is_some_and(|cfg| cfg.uniform_bucket_level_access.enabled)
    }

    pub fn object_retention_enabled(&self) -> bool {
        self.object_retention
            .as_ref()
            .is_some_and(|r| r.mode.eq_ignore_ascii_case("Enabled"))
    }
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct GcsObjectRetention {
    #[serde(skip_serializing_if = "String::is_empty")]
    pub mode: String,
    #[serde(rename = "retainUntilTime", skip_serializing_if = "String::is_empty")]
    pub retain_until_time: String,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct GcsObjectMeta {
    #[serde(rename = "storageClass", skip_serializing_if = "String::is_empty")]
    pub storage_class: String,
    #[serde(rename = "temporaryHold", skip_serializing_if = "is_false")]
    pub temporary_hold: bool,
    #[serde(rename = "eventBasedHold", skip_serializing_if = "is_false")]
    pub event_based_hold: bool,
    #[serde(
        rename = "retentionExpirationTime",
        skip_serializing_if = "String::is_empty"
    )]
    pub retention_expiration_time: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retention: Option<GcsObjectRetention>,
}

/// Object settings captured from XML multipart initiate headers
/// (`x-goog-acl`, `x-goog-storage-class`, `x-goog-object-retention-*`) and
/// applied to the object when the upload completes.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct GcsMultipartInit {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub acl: Option<Vec<AccessControlEntry>>,
    #[serde(rename = "storageClass", skip_serializing_if = "String::is_empty")]
    pub storage_class: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retention: Option<GcsObjectRetention>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct IamCondition {
    #[serde(skip_serializing_if = "String::is_empty")]
    pub title: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub description: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub expression: String,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct IamBinding {
    pub role: String,
    #[serde(default)]
    pub members: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub condition: Option<IamCondition>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct GcsIamPolicy {
    pub version: i32,
    pub etag: String,
    pub revision: u64,
    #[serde(default)]
    pub bindings: Vec<IamBinding>,
}

impl Default for GcsIamPolicy {
    fn default() -> Self {
        Self {
            version: 1,
            etag: "CAE=".to_string(),
            revision: 1,
            bindings: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct AccessControlEntry {
    pub entity: String,
    pub role: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub email: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub domain: String,
    #[serde(rename = "entityId", skip_serializing_if = "String::is_empty")]
    pub entity_id: String,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct NotificationConfig {
    pub id: String,
    pub topic: String,
    #[serde(default)]
    pub event_types: Vec<String>,
    #[serde(default)]
    pub custom_attributes: BTreeMap<String, String>,
    pub payload_format: String,
    #[serde(default)]
    pub object_name_prefix: String,
    pub etag: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct NotificationStore {
    pub next_id: u64,
    pub items: Vec<NotificationConfig>,
}

impl Default for NotificationStore {
    fn default() -> Self {
        Self {
            next_id: 1,
            items: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct NotificationEvent {
    #[serde(rename = "notificationId")]
    pub notification_id: String,
    pub topic: String,
    #[serde(rename = "eventType")]
    pub event_type: String,
    #[serde(rename = "bucketId")]
    pub bucket_id: String,
    #[serde(rename = "objectId")]
    pub object_id: String,
    #[serde(rename = "objectGeneration")]
    pub object_generation: String,
    #[serde(rename = "payloadFormat")]
    pub payload_format: String,
    #[serde(rename = "eventTime")]
    pub event_time: String,
    #[serde(default)]
    pub attributes: BTreeMap<String, String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub payload: Option<serde_json::Value>,
}

// --- path helpers ----------------------------------------------------------

pub fn gcs_dir(store: &FileBucketStore, bucket: &str) -> PathBuf {
    store.bucket_path(bucket).join("gcs")
}

fn bucket_meta_path(store: &FileBucketStore, bucket: &str) -> PathBuf {
    gcs_dir(store, bucket).join("bucket_meta.json")
}

fn iam_policy_path(store: &FileBucketStore, bucket: &str) -> PathBuf {
    gcs_dir(store, bucket).join("iam_policy.json")
}

fn bucket_acl_path(store: &FileBucketStore, bucket: &str) -> PathBuf {
    gcs_dir(store, bucket).join("acl.json")
}

fn default_object_acl_path(store: &FileBucketStore, bucket: &str) -> PathBuf {
    gcs_dir(store, bucket).join("default_object_acl.json")
}

fn object_acl_path(store: &FileBucketStore, bucket: &str, key: &str) -> PathBuf {
    gcs_dir(store, bucket)
        .join("object_acls")
        .join(format!("{}.json", base64::raw_url_encode(key.as_bytes())))
}

fn object_meta_path(store: &FileBucketStore, bucket: &str, key: &str) -> PathBuf {
    gcs_dir(store, bucket)
        .join("object_meta")
        .join(format!("{}.json", base64::raw_url_encode(key.as_bytes())))
}

fn multipart_init_path(store: &FileBucketStore, bucket: &str, upload_id: &str) -> PathBuf {
    gcs_dir(store, bucket)
        .join("multipart_uploads")
        .join(format!(
            "{}.json",
            base64::raw_url_encode(upload_id.as_bytes())
        ))
}

fn notifications_path(store: &FileBucketStore, bucket: &str) -> PathBuf {
    gcs_dir(store, bucket).join("notifications.json")
}

fn notification_events_path(store: &FileBucketStore, bucket: &str) -> PathBuf {
    gcs_dir(store, bucket).join("notification_events.json")
}

// --- generic atomic JSON I/O -----------------------------------------------

fn read_json_opt<T: serde::de::DeserializeOwned>(path: &Path) -> io::Result<Option<T>> {
    match fs::read(path) {
        Ok(data) => serde_json::from_slice(&data)
            .map(Some)
            .map_err(io::Error::other),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e),
    }
}

fn write_json_atomic<T: Serialize>(path: &Path, value: &T) -> io::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let tmp = path.with_extension("json.tmp");
    let data = wire_json::to_vec_indent(value);
    if let Err(e) = fs::write(&tmp, &data) {
        let _ = fs::remove_file(&tmp);
        return Err(e);
    }
    if let Err(e) = fs::rename(&tmp, path) {
        let _ = fs::remove_file(&tmp);
        return Err(e);
    }
    Ok(())
}

fn remove_if_exists(path: &Path) -> io::Result<()> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e),
    }
}

// --- typed accessors -------------------------------------------------------

pub fn read_bucket_meta(store: &FileBucketStore, bucket: &str) -> io::Result<GcsBucketMeta> {
    Ok(read_json_opt(&bucket_meta_path(store, bucket))?.unwrap_or_default())
}

pub fn write_bucket_meta(
    store: &FileBucketStore,
    bucket: &str,
    meta: &GcsBucketMeta,
) -> io::Result<()> {
    write_json_atomic(&bucket_meta_path(store, bucket), meta)
}

pub fn read_iam_policy(store: &FileBucketStore, bucket: &str) -> io::Result<GcsIamPolicy> {
    Ok(read_json_opt(&iam_policy_path(store, bucket))?.unwrap_or_default())
}

pub fn write_iam_policy(
    store: &FileBucketStore,
    bucket: &str,
    policy: &GcsIamPolicy,
) -> io::Result<()> {
    write_json_atomic(&iam_policy_path(store, bucket), policy)
}

pub fn read_bucket_acl(
    store: &FileBucketStore,
    bucket: &str,
) -> io::Result<Vec<AccessControlEntry>> {
    Ok(read_json_opt(&bucket_acl_path(store, bucket))?.unwrap_or_default())
}

pub fn write_bucket_acl(
    store: &FileBucketStore,
    bucket: &str,
    entries: &[AccessControlEntry],
) -> io::Result<()> {
    write_json_atomic(&bucket_acl_path(store, bucket), &entries)
}

pub fn read_default_object_acl(
    store: &FileBucketStore,
    bucket: &str,
) -> io::Result<Vec<AccessControlEntry>> {
    Ok(read_json_opt(&default_object_acl_path(store, bucket))?.unwrap_or_default())
}

pub fn write_default_object_acl(
    store: &FileBucketStore,
    bucket: &str,
    entries: &[AccessControlEntry],
) -> io::Result<()> {
    write_json_atomic(&default_object_acl_path(store, bucket), &entries)
}

pub fn read_object_acl(
    store: &FileBucketStore,
    bucket: &str,
    key: &str,
) -> io::Result<Option<Vec<AccessControlEntry>>> {
    read_json_opt(&object_acl_path(store, bucket, key))
}

pub fn write_object_acl(
    store: &FileBucketStore,
    bucket: &str,
    key: &str,
    entries: &[AccessControlEntry],
) -> io::Result<()> {
    write_json_atomic(&object_acl_path(store, bucket, key), &entries)
}

pub fn read_object_meta(
    store: &FileBucketStore,
    bucket: &str,
    key: &str,
) -> io::Result<GcsObjectMeta> {
    Ok(read_json_opt(&object_meta_path(store, bucket, key))?.unwrap_or_default())
}

pub fn write_object_meta(
    store: &FileBucketStore,
    bucket: &str,
    key: &str,
    meta: &GcsObjectMeta,
) -> io::Result<()> {
    write_json_atomic(&object_meta_path(store, bucket, key), meta)
}

pub fn read_multipart_init(
    store: &FileBucketStore,
    bucket: &str,
    upload_id: &str,
) -> io::Result<GcsMultipartInit> {
    Ok(read_json_opt(&multipart_init_path(store, bucket, upload_id))?.unwrap_or_default())
}

pub fn write_multipart_init(
    store: &FileBucketStore,
    bucket: &str,
    upload_id: &str,
    init: &GcsMultipartInit,
) -> io::Result<()> {
    write_json_atomic(&multipart_init_path(store, bucket, upload_id), init)
}

pub fn delete_multipart_init(
    store: &FileBucketStore,
    bucket: &str,
    upload_id: &str,
) -> io::Result<()> {
    remove_if_exists(&multipart_init_path(store, bucket, upload_id))
}

pub fn delete_object_sidecars(store: &FileBucketStore, bucket: &str, key: &str) -> io::Result<()> {
    remove_if_exists(&object_meta_path(store, bucket, key))?;
    remove_if_exists(&object_acl_path(store, bucket, key))?;
    Ok(())
}

pub fn read_notifications(store: &FileBucketStore, bucket: &str) -> io::Result<NotificationStore> {
    Ok(read_json_opt(&notifications_path(store, bucket))?.unwrap_or_default())
}

pub fn write_notifications(
    store: &FileBucketStore,
    bucket: &str,
    notifications: &NotificationStore,
) -> io::Result<()> {
    write_json_atomic(&notifications_path(store, bucket), notifications)
}

pub fn read_notification_events(
    store: &FileBucketStore,
    bucket: &str,
) -> io::Result<Vec<NotificationEvent>> {
    Ok(read_json_opt(&notification_events_path(store, bucket))?.unwrap_or_default())
}

pub fn append_notification_event(
    store: &FileBucketStore,
    bucket: &str,
    event: NotificationEvent,
) -> io::Result<()> {
    let mut events = read_notification_events(store, bucket)?;
    events.push(event);
    write_json_atomic(&notification_events_path(store, bucket), &events)
}
