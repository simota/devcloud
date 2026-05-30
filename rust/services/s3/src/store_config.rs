//! Bucket sub-resource persistence on `FileBucketStore`: versioning, object-lock,
//! lifecycle (including expiration application), ACL, and policy. Notification,
//! inventory, analytics, and replication land in a later part.

use crate::model::{LifecycleConfiguration, Object, ObjectLockConfiguration};
use crate::store::{remove_if_exists, FileBucketStore, Result, StoreError};
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

/// Whether any enabled lifecycle rule expires `object` at `now` (RFC3339 UTC).
/// Mirrors the Go `lifecycleExpiresObject`.
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
