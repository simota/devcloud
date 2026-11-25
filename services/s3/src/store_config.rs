//! Bucket sub-resource persistence on `FileBucketStore`: versioning, object-lock,
//! lifecycle (including expiration application), ACL, and policy. Notification,
//! inventory, analytics, and replication land in a later part.

use crate::cors::CorsConfiguration;
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

    /// Applies enabled lifecycle rules at `now` (RFC3339 UTC):
    /// - Current object `Expiration` (`Days` / `Date`) and `Transition`
    /// - `ExpiredObjectDeleteMarker` cleanup for lone delete markers
    /// - `NoncurrentVersionExpiration` and `NoncurrentVersionTransition`
    /// - `AbortIncompleteMultipartUpload`
    /// Returns `(affected_count, bucket_exists)`. Object-locked objects are skipped.
    pub fn apply_bucket_lifecycle(&self, bucket: &str, now: &str) -> Result<(i64, bool)> {
        let config = match self.get_bucket_lifecycle(bucket)? {
            None => return Ok((0, false)),
            Some((_, false)) => return Ok((0, true)),
            Some((config, true)) => config,
        };
        let Some((now_secs, _)) = parse_rfc3339(now) else {
            return Ok((0, true));
        };
        let objects = match self.list_objects(bucket, "")? {
            None => return Ok((0, false)),
            Some(objects) => objects,
        };
        let mut affected = 0;

        // 1. Current object expiration and transitions.
        for object in objects {
            if lifecycle_expires_object(&config, &object, now_secs) {
                match self.delete_object_with_result(&object.bucket, &object.key, false) {
                    Ok((_, deleted)) => {
                        if deleted {
                            affected += 1;
                        }
                    }
                    Err(StoreError::ObjectLocked) => continue,
                    Err(e) => return Err(e),
                }
                continue;
            }
            if let Some(target_class) =
                lifecycle_transition_storage_class(&config, &object, now_secs)
            {
                if self.update_object_storage_class(
                    &object.bucket,
                    &object.key,
                    &object.version_id,
                    &target_class,
                )? {
                    affected += 1;
                }
            }
        }

        // 2. Noncurrent version expiration/transitions and ExpiredObjectDeleteMarker.
        let has_version_rules = config.rules.iter().any(|r| {
            r.status == "Enabled"
                && (r.noncurrent_version_expiration.is_some()
                    || !r.noncurrent_version_transitions.is_empty()
                    || r.expiration.expired_object_delete_marker == Some(true))
        });
        if has_version_rules {
            if let Some(versions) = self.list_object_versions(bucket, "")? {
                let mut by_key: std::collections::BTreeMap<String, Vec<Object>> =
                    std::collections::BTreeMap::new();
                for v in versions {
                    by_key.entry(v.key.clone()).or_default().push(v);
                }
                for (key, key_versions) in by_key {
                    // Versions are already ordered newest -> oldest.
                    for (idx, version) in key_versions.iter().enumerate().skip(1) {
                        let noncurrent_rank = idx as i64; // 1 = newest noncurrent version
                        let became_noncurrent_at =
                            parse_rfc3339(&key_versions[idx - 1].last_modified)
                                .map(|(s, _)| s)
                                .unwrap_or(i64::MAX);
                        if lifecycle_expires_noncurrent_version(
                            &config,
                            &key,
                            noncurrent_rank,
                            became_noncurrent_at,
                            now_secs,
                        ) {
                            let vid = crate::objops::object_version_id(version);
                            match self.delete_object_version(bucket, &key, &vid, false) {
                                Ok((_, true)) => affected += 1,
                                Ok((_, false)) => {}
                                Err(StoreError::ObjectLocked) => continue,
                                Err(e) => return Err(e),
                            }
                            continue;
                        }
                        if !version.delete_marker {
                            if let Some(target_class) =
                                lifecycle_noncurrent_transition_storage_class(
                                    &config,
                                    &key,
                                    noncurrent_rank,
                                    became_noncurrent_at,
                                    now_secs,
                                )
                            {
                                let vid = crate::objops::object_version_id(version);
                                if self.update_object_storage_class(
                                    bucket,
                                    &key,
                                    &vid,
                                    &target_class,
                                )? {
                                    affected += 1;
                                }
                            }
                        }
                    }

                    // Re-check for ExpiredObjectDeleteMarker: if only delete marker(s) remain.
                    if lifecycle_removes_expired_delete_marker(&config, &key) {
                        if let Some(remaining) = self.list_object_versions(bucket, &key)? {
                            let key_only: Vec<Object> =
                                remaining.into_iter().filter(|o| o.key == key).collect();
                            if !key_only.is_empty() && key_only.iter().all(|o| o.delete_marker) {
                                for dm in key_only {
                                    let vid = crate::objops::object_version_id(&dm);
                                    if let Ok((_, true)) =
                                        self.delete_object_version(bucket, &key, &vid, false)
                                    {
                                        affected += 1;
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        // 3. Abort incomplete multipart uploads.
        let has_abort_rules = config
            .rules
            .iter()
            .any(|r| r.status == "Enabled" && r.abort_incomplete_multipart_upload.is_some());
        if has_abort_rules {
            if let Some(uploads) = self.list_multipart_uploads(bucket)? {
                for upload in uploads {
                    if lifecycle_aborts_multipart(
                        &config,
                        &upload.key,
                        &upload.created_at,
                        now_secs,
                    ) {
                        if self.abort_multipart_upload(bucket, &upload.key, &upload.upload_id)? {
                            affected += 1;
                        }
                    }
                }
            }
        }

        Ok((affected, true))
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

    // --- CORS ---------------------------------------------------------------

    /// Stores the bucket CORS configuration. Errors if the bucket is absent.
    pub fn put_bucket_cors(&self, bucket: &str, config: &CorsConfiguration) -> Result<()> {
        self.get_bucket(bucket)?.ok_or(StoreError::BucketNotExist)?;
        let data = serde_json::to_vec(config).map_err(io::Error::other)?;
        std::fs::write(self.bucket_path(bucket).join("cors.json"), data)?;
        Ok(())
    }

    /// Reads the bucket CORS configuration; `None` if none is set.
    pub fn get_bucket_cors(&self, bucket: &str) -> Result<Option<CorsConfiguration>> {
        if self.get_bucket(bucket)?.is_none() {
            return Ok(None);
        }
        match std::fs::read(self.bucket_path(bucket).join("cors.json")) {
            Ok(data) => Ok(Some(
                serde_json::from_slice(&data).map_err(io::Error::other)?,
            )),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(StoreError::Io(e)),
        }
    }

    /// Removes the bucket CORS configuration. Errors if the bucket is absent.
    pub fn delete_bucket_cors(&self, bucket: &str) -> Result<()> {
        self.get_bucket(bucket)?.ok_or(StoreError::BucketNotExist)?;
        remove_if_exists(&self.bucket_path(bucket).join("cors.json"))?;
        Ok(())
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

fn rule_matches_key(rule: &crate::model::LifecycleRule, key: &str) -> bool {
    if rule.status != "Enabled" {
        return false;
    }
    let prefix = if !rule.filter.prefix.is_empty() {
        &rule.filter.prefix
    } else {
        &rule.prefix
    };
    prefix.is_empty() || key.starts_with(prefix.as_str())
}

/// Whether any enabled lifecycle rule expires `object` at `now_secs`.
fn lifecycle_expires_object(
    config: &LifecycleConfiguration,
    object: &Object,
    now_secs: i64,
) -> bool {
    for rule in &config.rules {
        if !rule_matches_key(rule, &object.key) {
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

/// Returns the latest applicable transitioned storage class for `object` at `now_secs`.
fn lifecycle_transition_storage_class(
    config: &LifecycleConfiguration,
    object: &Object,
    now_secs: i64,
) -> Option<String> {
    let modified = parse_rfc3339(&object.last_modified).map(|(s, _)| s);
    let mut best: Option<(i64, String)> = None;
    for rule in &config.rules {
        if !rule_matches_key(rule, &object.key) {
            continue;
        }
        for tr in &rule.transitions {
            if tr.storage_class.is_empty() {
                continue;
            }
            let trigger_at = if let Some(days) = tr.days {
                modified.map(|m| m + days * 86_400)
            } else if !tr.date.is_empty() {
                parse_lifecycle_date(&tr.date)
            } else {
                None
            };
            if let Some(at) = trigger_at {
                if at <= now_secs && best.as_ref().is_none_or(|(prev, _)| at >= *prev) {
                    best = Some((at, tr.storage_class.clone()));
                }
            }
        }
    }
    best.map(|(_, class)| class)
}

fn lifecycle_expires_noncurrent_version(
    config: &LifecycleConfiguration,
    key: &str,
    noncurrent_rank: i64,
    became_noncurrent_at: i64,
    now_secs: i64,
) -> bool {
    for rule in &config.rules {
        if !rule_matches_key(rule, key) {
            continue;
        }
        let Some(exp) = &rule.noncurrent_version_expiration else {
            continue;
        };
        if let Some(keep) = exp.newer_noncurrent_versions {
            if noncurrent_rank <= keep {
                continue;
            }
        }
        if let Some(days) = exp.noncurrent_days {
            if became_noncurrent_at.saturating_add(days * 86_400) <= now_secs {
                return true;
            }
        }
    }
    false
}

fn lifecycle_noncurrent_transition_storage_class(
    config: &LifecycleConfiguration,
    key: &str,
    noncurrent_rank: i64,
    became_noncurrent_at: i64,
    now_secs: i64,
) -> Option<String> {
    let mut best: Option<(i64, String)> = None;
    for rule in &config.rules {
        if !rule_matches_key(rule, key) {
            continue;
        }
        for tr in &rule.noncurrent_version_transitions {
            if tr.storage_class.is_empty() {
                continue;
            }
            if let Some(keep) = tr.newer_noncurrent_versions {
                if noncurrent_rank <= keep {
                    continue;
                }
            }
            if let Some(days) = tr.noncurrent_days {
                let at = became_noncurrent_at.saturating_add(days * 86_400);
                if at <= now_secs && best.as_ref().is_none_or(|(prev, _)| at >= *prev) {
                    best = Some((at, tr.storage_class.clone()));
                }
            }
        }
    }
    best.map(|(_, class)| class)
}

fn lifecycle_removes_expired_delete_marker(config: &LifecycleConfiguration, key: &str) -> bool {
    config.rules.iter().any(|r| {
        rule_matches_key(r, key) && r.expiration.expired_object_delete_marker == Some(true)
    })
}

fn lifecycle_aborts_multipart(
    config: &LifecycleConfiguration,
    key: &str,
    created_at: &str,
    now_secs: i64,
) -> bool {
    let Some((created_secs, _)) = parse_rfc3339(created_at) else {
        return false;
    };
    for rule in &config.rules {
        if !rule_matches_key(rule, key) {
            continue;
        }
        if let Some(abort) = &rule.abort_incomplete_multipart_upload {
            if created_secs + abort.days_after_initiation * 86_400 <= now_secs {
                return true;
            }
        }
    }
    false
}
