//! Bucket sub-resource persistence on `FileBucketStore`: versioning, object-lock,
//! lifecycle (including expiration application), ACL, and policy. Notification,
//! inventory, analytics, and replication land in a later part.

use crate::model::{
    AnalyticsConfiguration, LifecycleConfiguration, NotificationConfiguration,
    NotificationEventRecord, Object, ObjectLockConfiguration, ReplicationConfiguration,
};
use crate::store::{read_json_file, remove_if_exists, FileBucketStore, Result, StoreError};
use crate::time_fmt::{parse_lifecycle_date, parse_rfc3339};
use serde::{Deserialize, Serialize};
use std::io;

/// The `versioning.json` sidecar (`{"status": "..."}`).
#[derive(Debug, Default, Serialize, Deserialize)]
struct VersioningFile {
    status: String,
}

/// The `acl.json` sidecar (`{"acl": "..."}`), shared by bucket and object ACLs.
#[derive(Debug, Default, Serialize, Deserialize)]
pub(crate) struct AclFile {
    pub acl: String,
}

impl FileBucketStore {
    /// Sets a bucket's versioning status (`Enabled` or `Suspended`), updating both
    /// `bucket.json` and the `versioning.json` sidecar.
    pub fn put_bucket_versioning(&self, bucket: &str, status: &str) -> Result<()> {
        if status != "Enabled" && status != "Suspended" {
            return Err(StoreError::Io(io::Error::other(
                "invalid versioning status",
            )));
        }
        let mut existing = self.get_bucket(bucket)?.ok_or(StoreError::BucketNotExist)?;
        existing.versioning = status.to_string();
        Self::write_json(&self.bucket_path(bucket).join("bucket.json"), &existing)?;
        Self::write_json(
            &self.bucket_path(bucket).join("versioning.json"),
            &VersioningFile {
                status: status.to_string(),
            },
        )
    }

    /// Reads a bucket's versioning status. Returns `(status, found)`; `found` is
    /// false when the bucket does not exist, and `status` is empty when never set.
    pub fn get_bucket_versioning(&self, bucket: &str) -> Result<(String, bool)> {
        let existing = match self.get_bucket(bucket)? {
            Some(b) => b,
            None => return Ok((String::new(), false)),
        };
        if !existing.versioning.is_empty() {
            return Ok((existing.versioning, true));
        }
        match std::fs::read(self.bucket_path(bucket).join("versioning.json")) {
            Ok(data) => {
                let file: VersioningFile = serde_json::from_slice(&data)
                    .map_err(|e| StoreError::Io(io::Error::other(e)))?;
                Ok((file.status, true))
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok((String::new(), true)),
            Err(e) => Err(StoreError::Io(e)),
        }
    }

    // --- object lock --------------------------------------------------------

    /// Sets the bucket's object-lock configuration (`bucket.json` + sidecar).
    pub fn put_bucket_object_lock_configuration(
        &self,
        bucket: &str,
        config: ObjectLockConfiguration,
    ) -> Result<()> {
        let mut existing = self.get_bucket(bucket)?.ok_or(StoreError::BucketNotExist)?;
        existing.object_lock_config = config.clone();
        Self::write_json(&self.bucket_path(bucket).join("bucket.json"), &existing)?;
        Self::write_json(&self.bucket_path(bucket).join("object-lock.json"), &config)
    }

    /// Reads the bucket's object-lock configuration. `None` if the bucket is
    /// absent; the bool indicates whether a configuration is present.
    pub fn get_bucket_object_lock_configuration(
        &self,
        bucket: &str,
    ) -> Result<Option<(ObjectLockConfiguration, bool)>> {
        let existing = match self.get_bucket(bucket)? {
            Some(b) => b,
            None => return Ok(None),
        };
        if !existing.object_lock_config.object_lock_enabled.is_empty() {
            return Ok(Some((existing.object_lock_config, true)));
        }
        match std::fs::read(self.bucket_path(bucket).join("object-lock.json")) {
            Ok(data) => {
                let config: ObjectLockConfiguration = serde_json::from_slice(&data)
                    .map_err(|e| StoreError::Io(io::Error::other(e)))?;
                Ok(Some((config, true)))
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                Ok(Some((ObjectLockConfiguration::default(), false)))
            }
            Err(e) => Err(StoreError::Io(e)),
        }
    }

    /// Clears the bucket's object-lock configuration. Errors if the bucket is
    /// absent.
    pub fn delete_bucket_object_lock_configuration(&self, bucket: &str) -> Result<bool> {
        let mut existing = self.get_bucket(bucket)?.ok_or(StoreError::BucketNotExist)?;
        existing.object_lock_config = ObjectLockConfiguration::default();
        Self::write_json(&self.bucket_path(bucket).join("bucket.json"), &existing)?;
        remove_if_exists(&self.bucket_path(bucket).join("object-lock.json"))?;
        Ok(true)
    }

    // --- lifecycle ----------------------------------------------------------

    /// Persists the bucket's lifecycle configuration. Errors if absent.
    pub fn put_bucket_lifecycle(
        &self,
        bucket: &str,
        config: &LifecycleConfiguration,
    ) -> Result<()> {
        self.get_bucket(bucket)?.ok_or(StoreError::BucketNotExist)?;
        Self::write_json(&self.bucket_path(bucket).join("lifecycle.json"), config)
    }

    /// Reads the bucket's lifecycle configuration. `None` if the bucket is
    /// absent; the bool indicates whether a configuration is present.
    pub fn get_bucket_lifecycle(
        &self,
        bucket: &str,
    ) -> Result<Option<(LifecycleConfiguration, bool)>> {
        if self.get_bucket(bucket)?.is_none() {
            return Ok(None);
        }
        match std::fs::read(self.bucket_path(bucket).join("lifecycle.json")) {
            Ok(data) => {
                let config: LifecycleConfiguration = serde_json::from_slice(&data)
                    .map_err(|e| StoreError::Io(io::Error::other(e)))?;
                Ok(Some((config, true)))
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                Ok(Some((LifecycleConfiguration::default(), false)))
            }
            Err(e) => Err(StoreError::Io(e)),
        }
    }

    /// Removes the bucket's lifecycle configuration. Errors if absent.
    pub fn delete_bucket_lifecycle(&self, bucket: &str) -> Result<bool> {
        self.get_bucket(bucket)?.ok_or(StoreError::BucketNotExist)?;
        remove_if_exists(&self.bucket_path(bucket).join("lifecycle.json"))?;
        Ok(true)
    }

    /// Applies expiration rules at `now` (RFC3339 UTC), deleting expired objects.
    /// Returns `(expired_count, bucket_exists)`. Object-locked objects are skipped.
    pub fn apply_bucket_lifecycle(&self, bucket: &str, now: &str) -> Result<(i64, bool)> {
        let config = match self.get_bucket_lifecycle(bucket)? {
            None => return Ok((0, false)),
            Some((_, false)) => return Ok((0, true)),
            Some((config, true)) => config,
        };
        let objects = match self.list_objects(bucket, "")? {
            None => return Ok((0, false)),
            Some(objects) => objects,
        };
        let mut expired = 0;
        for object in objects {
            if lifecycle_expires_object(&config, &object, now) {
                match self.delete_object_with_result(&object.bucket, &object.key, false) {
                    Ok((_, deleted)) => {
                        if deleted {
                            expired += 1;
                        }
                    }
                    Err(StoreError::ObjectLocked) => continue,
                    Err(e) => return Err(e),
                }
            }
        }
        Ok((expired, true))
    }

    // --- policy -------------------------------------------------------------

    /// Stores a raw bucket policy document. Errors if the bucket is absent.
    pub fn put_bucket_policy(&self, bucket: &str, policy: &[u8]) -> Result<()> {
        self.get_bucket(bucket)?.ok_or(StoreError::BucketNotExist)?;
        std::fs::write(self.bucket_path(bucket).join("policy.json"), policy)?;
        Ok(())
    }

    /// Reads the raw bucket policy. `None` if the bucket is absent; the bool
    /// indicates whether a policy is present.
    pub fn get_bucket_policy(&self, bucket: &str) -> Result<Option<(Vec<u8>, bool)>> {
        if self.get_bucket(bucket)?.is_none() {
            return Ok(None);
        }
        match std::fs::read(self.bucket_path(bucket).join("policy.json")) {
            Ok(data) => Ok(Some((data, true))),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(Some((Vec::new(), false))),
            Err(e) => Err(StoreError::Io(e)),
        }
    }

    /// Removes the bucket policy. Errors if the bucket is absent.
    pub fn delete_bucket_policy(&self, bucket: &str) -> Result<bool> {
        self.get_bucket(bucket)?.ok_or(StoreError::BucketNotExist)?;
        remove_if_exists(&self.bucket_path(bucket).join("policy.json"))?;
        Ok(true)
    }

    // --- ACL ----------------------------------------------------------------

    /// Sets the bucket ACL (`bucket.json` + `acl.json`). Errors if absent.
    pub fn put_bucket_acl(&self, bucket: &str, acl: &str) -> Result<()> {
        let mut existing = self.get_bucket(bucket)?.ok_or(StoreError::BucketNotExist)?;
        existing.acl = acl.to_string();
        Self::write_json(&self.bucket_path(bucket).join("bucket.json"), &existing)?;
        Self::write_json(
            &self.bucket_path(bucket).join("acl.json"),
            &AclFile {
                acl: acl.to_string(),
            },
        )
    }

    /// Reads the bucket ACL, defaulting to `private`. `None` if the bucket is
    /// absent.
    pub fn get_bucket_acl(&self, bucket: &str) -> Result<Option<String>> {
        let existing = match self.get_bucket(bucket)? {
            Some(b) => b,
            None => return Ok(None),
        };
        if !existing.acl.is_empty() {
            return Ok(Some(existing.acl));
        }
        match std::fs::read(self.bucket_path(bucket).join("acl.json")) {
            Ok(data) => {
                let file: AclFile = serde_json::from_slice(&data)
                    .map_err(|e| StoreError::Io(io::Error::other(e)))?;
                if file.acl.is_empty() {
                    Ok(Some("private".to_string()))
                } else {
                    Ok(Some(file.acl))
                }
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(Some("private".to_string())),
            Err(e) => Err(StoreError::Io(e)),
        }
    }
}

impl FileBucketStore {
    // --- notification -------------------------------------------------------

    /// Persists the bucket's notification configuration. Errors if absent.
    pub fn put_bucket_notification(
        &self,
        bucket: &str,
        config: &NotificationConfiguration,
    ) -> Result<()> {
        self.get_bucket(bucket)?.ok_or(StoreError::BucketNotExist)?;
        Self::write_json(&self.bucket_path(bucket).join("notification.json"), config)
    }

    /// Reads the bucket's notification configuration (default if unset). `None`
    /// if the bucket does not exist.
    pub fn get_bucket_notification(
        &self,
        bucket: &str,
    ) -> Result<Option<NotificationConfiguration>> {
        if self.get_bucket(bucket)?.is_none() {
            return Ok(None);
        }
        Ok(Some(
            read_json_file(&self.bucket_path(bucket).join("notification.json"))?
                .unwrap_or_default(),
        ))
    }

    /// Appends a notification event record. Returns whether the bucket exists.
    pub fn append_notification_event(
        &self,
        bucket: &str,
        event: NotificationEventRecord,
    ) -> Result<bool> {
        if self.get_bucket(bucket)?.is_none() {
            return Ok(false);
        }
        let path = self.bucket_path(bucket).join("notification-events.json");
        let mut events: Vec<NotificationEventRecord> = read_json_file(&path)?.unwrap_or_default();
        events.push(event);
        Self::write_json(&path, &events)?;
        Ok(true)
    }

    /// Lists notification event records. `None` if the bucket does not exist.
    pub fn list_notification_events(
        &self,
        bucket: &str,
    ) -> Result<Option<Vec<NotificationEventRecord>>> {
        if self.get_bucket(bucket)?.is_none() {
            return Ok(None);
        }
        Ok(Some(
            read_json_file(&self.bucket_path(bucket).join("notification-events.json"))?
                .unwrap_or_default(),
        ))
    }

    // --- replication --------------------------------------------------------

    /// Persists the bucket's replication configuration. Errors if absent.
    pub fn put_bucket_replication(
        &self,
        bucket: &str,
        config: &ReplicationConfiguration,
    ) -> Result<()> {
        self.get_bucket(bucket)?.ok_or(StoreError::BucketNotExist)?;
        Self::write_json(&self.bucket_path(bucket).join("replication.json"), config)
    }

    /// Reads the bucket's replication configuration. `None` if the bucket is
    /// absent; the bool indicates whether a configuration is present.
    pub fn get_bucket_replication(
        &self,
        bucket: &str,
    ) -> Result<Option<(ReplicationConfiguration, bool)>> {
        if self.get_bucket(bucket)?.is_none() {
            return Ok(None);
        }
        match read_json_file(&self.bucket_path(bucket).join("replication.json"))? {
            Some(config) => Ok(Some((config, true))),
            None => Ok(Some((ReplicationConfiguration::default(), false))),
        }
    }

    /// Removes the bucket's replication configuration. Errors if absent.
    pub fn delete_bucket_replication(&self, bucket: &str) -> Result<bool> {
        self.get_bucket(bucket)?.ok_or(StoreError::BucketNotExist)?;
        remove_if_exists(&self.bucket_path(bucket).join("replication.json"))?;
        Ok(true)
    }

    // --- analytics ----------------------------------------------------------

    /// Persists an analytics configuration under `id`. Errors if absent.
    pub fn put_bucket_analytics(
        &self,
        bucket: &str,
        id: &str,
        mut config: AnalyticsConfiguration,
    ) -> Result<()> {
        self.get_bucket(bucket)?.ok_or(StoreError::BucketNotExist)?;
        config.id = id.to_string();
        std::fs::create_dir_all(self.analytics_path(bucket))?;
        Self::write_json(&self.analytics_config_path(bucket, id), &config)
    }

    /// Reads an analytics configuration. `None` if the bucket is absent; the bool
    /// indicates whether the configuration is present.
    pub fn get_bucket_analytics(
        &self,
        bucket: &str,
        id: &str,
    ) -> Result<Option<(AnalyticsConfiguration, bool)>> {
        if self.get_bucket(bucket)?.is_none() {
            return Ok(None);
        }
        match read_json_file(&self.analytics_config_path(bucket, id))? {
            Some(config) => Ok(Some((config, true))),
            None => Ok(Some((AnalyticsConfiguration::default(), false))),
        }
    }

    /// Lists analytics configurations, sorted by ID. `None` if the bucket absent.
    pub fn list_bucket_analytics(
        &self,
        bucket: &str,
    ) -> Result<Option<Vec<AnalyticsConfiguration>>> {
        if self.get_bucket(bucket)?.is_none() {
            return Ok(None);
        }
        let mut configs = read_config_dir(&self.analytics_path(bucket))?;
        configs.sort_by(|a: &AnalyticsConfiguration, b| a.id.cmp(&b.id));
        Ok(Some(configs))
    }

    /// Removes an analytics configuration. Errors if the bucket is absent.
    pub fn delete_bucket_analytics(&self, bucket: &str, id: &str) -> Result<bool> {
        self.get_bucket(bucket)?.ok_or(StoreError::BucketNotExist)?;
        remove_if_exists(&self.analytics_config_path(bucket, id))?;
        let _ = std::fs::remove_dir(self.analytics_path(bucket));
        Ok(true)
    }
}

/// Reads every `*.json` file in `dir` (skipping subdirectories) into `T`.
/// Returns an empty list if the directory does not exist.
pub(crate) fn read_config_dir<T: serde::de::DeserializeOwned>(
    dir: &std::path::Path,
) -> Result<Vec<T>> {
    let entries = match std::fs::read_dir(dir) {
        Ok(e) => e,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(StoreError::Io(e)),
    };
    let mut configs = Vec::new();
    for entry in entries {
        let entry = entry?;
        if entry.file_type()?.is_dir() {
            continue;
        }
        if let Some(config) = read_json_file(&entry.path())? {
            configs.push(config);
        }
    }
    Ok(configs)
}

/// Whether any enabled lifecycle rule expires `object` at `now` (RFC3339 UTC).
/// Mirrors the legacy `lifecycleExpiresObject`.
fn lifecycle_expires_object(config: &LifecycleConfiguration, object: &Object, now: &str) -> bool {
    let now_secs = match parse_rfc3339(now) {
        Some((secs, _)) => secs,
        None => return false,
    };
    for rule in &config.rules {
        if rule.status != "Enabled" {
            continue;
        }
        let prefix = if !rule.filter.prefix.is_empty() {
            &rule.filter.prefix
        } else {
            &rule.prefix
        };
        if !prefix.is_empty() && !object.key.starts_with(prefix.as_str()) {
            continue;
        }
        if let Some(days) = rule.expiration.days {
            if let Some((modified, _)) = parse_rfc3339(&object.last_modified) {
                let expires_at = modified + days * 86_400;
                if expires_at <= now_secs {
                    return true;
                }
            }
        }
        if !rule.expiration.date.is_empty() {
            if let Some(expires_at) = parse_lifecycle_date(&rule.expiration.date) {
                if expires_at <= now_secs {
                    return true;
                }
            }
        }
    }
    false
}
