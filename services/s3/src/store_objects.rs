//! The object data plane on `FileBucketStore`: put/get/update/delete objects,
//! versioning (versions directory, delete markers, null version, current-object
//! rebuild), and prefix listings. Ported 1:1 from the legacy `store_objects.rs`.

use crate::hashes::{crc32c_base64, md5_etag, validate_content_md5, ContentMd5Error};
use crate::model::{Object, NULL_VERSION_ID};
use crate::objops::{
    clean_metadata, clean_object_legal_hold, clean_object_retention, clean_server_side_encryption,
    default_object_retention, object_lock_prevents_delete, object_version_id, PutObjectInput,
    UpdateObjectMetadataInput,
};
use crate::store::{
    remove_dir_all_ignoring_missing, remove_if_exists, FileBucketStore, Result, StoreError,
};
use crate::time_fmt::{time_after, GO_ZERO_TIME};
use crate::validation::{valid_bucket_name, valid_object_key};
use std::cmp::Ordering;
use std::fs;
use std::io;
use std::path::Path;

/// Reads and decodes an `object.json`; `None` if the file does not exist.
fn read_object(path: &Path) -> Result<Option<Object>> {
    match fs::read(path) {
        Ok(data) => Ok(Some(
            serde_json::from_slice(&data).map_err(|e| StoreError::Io(io::Error::other(e)))?,
        )),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(StoreError::Io(e)),
    }
}

fn missing(what: &str) -> StoreError {
    StoreError::Io(io::Error::new(io::ErrorKind::NotFound, what.to_string()))
}

/// Reads object metadata for a version listing, defaulting an empty version ID
/// to `default_vid`. Mirrors the legacy `readObjectMetadataForVersionList`.
fn read_object_metadata_for_version_list(path: &Path, default_vid: &str) -> Result<Option<Object>> {
    match read_object(path)? {
        Some(mut object) => {
            if object.version_id.is_empty() {
                object.version_id = default_vid.to_string();
            }
            Ok(Some(object))
        }
        None => Ok(None),
    }
}

impl FileBucketStore {
    pub(crate) fn require_bucket_and_key(&self, bucket: &str, key: &str) -> Result<()> {
        if !valid_bucket_name(bucket) {
            return Err(StoreError::InvalidBucketName);
        }
        if !valid_object_key(key) {
            return Err(StoreError::InvalidObjectKey);
        }
        if self.get_bucket(bucket)?.is_none() {
            return Err(StoreError::BucketNotExist);
        }
        Ok(())
    }

    /// Stores an object (body + `object.json`, plus a version snapshot when the
    /// bucket has versioning). Returns the stored object.
    pub fn put_object(&self, input: PutObjectInput) -> Result<Object> {
        if !valid_bucket_name(&input.bucket) {
            return Err(StoreError::InvalidBucketName);
        }
        if !valid_object_key(&input.key) {
            return Err(StoreError::InvalidObjectKey);
        }
        let bucket = self
            .get_bucket(&input.bucket)?
            .ok_or(StoreError::BucketNotExist)?;
        match validate_content_md5(&input.content_md5, &input.body) {
            Ok(()) => {}
            Err(ContentMd5Error::Invalid) => return Err(StoreError::InvalidContentMd5),
            Err(ContentMd5Error::Mismatch) => return Err(StoreError::ContentMd5Mismatch),
        }

        let now = self.now();
        let mut object = Object {
            bucket: input.bucket.clone(),
            key: input.key.clone(),
            etag: md5_etag(&input.body),
            size: input.body.len() as i64,
            created_at: now.clone(),
            last_modified: now.clone(),
            updated_at: now.clone(),
            metageneration: 1,
            content_type: input.content_type.clone(),
            content_encoding: input.content_encoding.clone(),
            crc32c: crc32c_base64(&input.body),
            cache_control: input.cache_control.clone(),
            content_disposition: input.content_disposition.clone(),
            metadata: clean_metadata(&input.metadata),
            encryption: clean_server_side_encryption(&input.encryption),
            retention: clean_object_retention(&input.retention),
            legal_hold: clean_object_legal_hold(&input.legal_hold),
            ..Default::default()
        };
        if object.retention.mode.is_empty() {
            object.retention = default_object_retention(&bucket.object_lock_config, &now);
        }
        match bucket.versioning.as_str() {
            "Enabled" => object.version_id = self.new_version_id(),
            "Suspended" => object.version_id = NULL_VERSION_ID.to_string(),
            _ => {}
        }
        if object.content_type.is_empty() {
            object.content_type = "application/octet-stream".to_string();
        }

        let path = self.object_path(&input.bucket, &input.key);
        fs::create_dir_all(&path)?;
        fs::write(path.join("body"), &input.body)?;
        Self::write_json(&path.join("object.json"), &object)?;
        if !object.version_id.is_empty() {
            self.write_object_version(&path, &object, &input.body)?;
        }
        Ok(object)
    }

    /// Updates an existing object's metadata fields (content-type/encoding/cache/
    /// disposition/user metadata), bumping the metageneration. `None` if absent.
    pub fn update_object_metadata(
        &self,
        input: UpdateObjectMetadataInput,
    ) -> Result<Option<Object>> {
        if !valid_bucket_name(&input.bucket) {
            return Err(StoreError::InvalidBucketName);
        }
        if !valid_object_key(&input.key) {
            return Err(StoreError::InvalidObjectKey);
        }
        if self.get_bucket(&input.bucket)?.is_none() {
            return Err(StoreError::BucketNotExist);
        }
        let path = self.object_path(&input.bucket, &input.key);
        let mut object = match read_object(&path.join("object.json"))? {
            Some(o) => o,
            None => return Ok(None),
        };
        if !input.content_type.is_empty() {
            object.content_type = input.content_type;
        }
        if !input.content_encoding.is_empty() {
            object.content_encoding = input.content_encoding;
        }
        if !input.cache_control.is_empty() {
            object.cache_control = input.cache_control;
        }
        if !input.content_disposition.is_empty() {
            object.content_disposition = input.content_disposition;
        }
        if let Some(meta) = &input.metadata {
            object.metadata = clean_metadata(meta);
        }
        if object.created_at.is_empty() || object.created_at == GO_ZERO_TIME {
            object.created_at = object.last_modified.clone();
        }
        if object.metageneration < 1 {
            object.metageneration = 1;
        }
        object.metageneration += 1;
        object.updated_at = self.now();
        Self::write_json(&path.join("object.json"), &object)?;
        Ok(Some(object))
    }

    /// Reads the current object (body included); `None` if absent or a delete
    /// marker (matching the legacy `GetObject`).
    pub fn get_object(&self, bucket: &str, key: &str) -> Result<Option<(Object, Vec<u8>)>> {
        if !valid_bucket_name(bucket) {
            return Err(StoreError::InvalidBucketName);
        }
        if !valid_object_key(key) {
            return Err(StoreError::InvalidObjectKey);
        }
        if self.get_bucket(bucket)?.is_none() {
            return Err(StoreError::BucketNotExist);
        }
        let path = self.object_path(bucket, key);
        let object = match read_object(&path.join("object.json"))? {
            Some(o) => o,
            None => return Ok(None),
        };
        if object.delete_marker {
            return Ok(None);
        }
        let body = fs::read(path.join("body"))?;
        Ok(Some((object, body)))
    }

    /// Reads a specific object version; an empty `version_id` falls back to the
    /// current object. A delete-marker version returns `Some` with an empty body.
    pub fn get_object_version(
        &self,
        bucket: &str,
        key: &str,
        version_id: &str,
    ) -> Result<Option<(Object, Vec<u8>)>> {
        if version_id.is_empty() {
            return self.get_object(bucket, key);
        }
        self.require_bucket_and_key(bucket, key)?;
        if version_id == NULL_VERSION_ID {
            if let Some(found) = self.get_null_object_version(bucket, key)? {
                return Ok(Some(found));
            }
        }
        let path = self.object_versions_path(bucket, key).join(version_id);
        let object = match read_object(&path.join("object.json"))? {
            Some(o) => o,
            None => return Ok(None),
        };
        if object.delete_marker {
            return Ok(Some((object, Vec::new())));
        }
        let body = fs::read(path.join("body"))?;
        Ok(Some((object, body)))
    }

    /// Deletes the current object; `true` if it existed. With versioning, writes
    /// a delete marker instead of removing data.
    pub fn delete_object(&self, bucket: &str, key: &str) -> Result<bool> {
        Ok(self.delete_object_with_result(bucket, key, false)?.1)
    }

    /// Deletes the current object, returning the delete marker (when versioned)
    /// and whether anything existed. Honors object-lock retention/legal-hold.
    pub fn delete_object_with_result(
        &self,
        bucket: &str,
        key: &str,
        bypass_governance: bool,
    ) -> Result<(Object, bool)> {
        if !valid_bucket_name(bucket) {
            return Err(StoreError::InvalidBucketName);
        }
        if !valid_object_key(key) {
            return Err(StoreError::InvalidObjectKey);
        }
        let existing_bucket = self.get_bucket(bucket)?.ok_or(StoreError::BucketNotExist)?;
        let objects_path = self.objects_path(bucket);
        let path = self.object_path(bucket, key);
        let versioned =
            existing_bucket.versioning == "Enabled" || existing_bucket.versioning == "Suspended";

        if versioned {
            if !path.exists() {
                return Ok((Object::default(), false));
            }
            if let Some(current) = self.read_current_object_metadata(bucket, key)? {
                if !current.delete_marker
                    && object_lock_prevents_delete(&current, &self.now(), bypass_governance)
                {
                    return Err(StoreError::ObjectLocked);
                }
            }
            let now = self.now();
            let version_id = if existing_bucket.versioning == "Suspended" {
                NULL_VERSION_ID.to_string()
            } else {
                self.new_version_id()
            };
            let marker = Object {
                bucket: bucket.to_string(),
                key: key.to_string(),
                created_at: GO_ZERO_TIME.to_string(),
                last_modified: now.clone(),
                updated_at: now,
                version_id,
                delete_marker: true,
                ..Default::default()
            };
            Self::write_json(&path.join("object.json"), &marker)?;
            self.write_object_version(&path, &marker, &[])?;
            let _ = remove_if_exists(&path.join("body"));
            return Ok((marker, true));
        }

        if !path.exists() {
            return Ok((Object::default(), false));
        }
        if let Some(current) = self.read_current_object_metadata(bucket, key)? {
            if object_lock_prevents_delete(&current, &self.now(), bypass_governance) {
                return Err(StoreError::ObjectLocked);
            }
        }
        fs::remove_dir_all(&path)?;
        let _ = fs::remove_dir(&objects_path);
        Ok((Object::default(), true))
    }

    /// Deletes a specific version (or the null version), then rebuilds the
    /// current object from the remaining versions.
    pub fn delete_object_version(
        &self,
        bucket: &str,
        key: &str,
        version_id: &str,
        bypass_governance: bool,
    ) -> Result<(Object, bool)> {
        if version_id.is_empty() {
            return Err(StoreError::VersionIdRequired);
        }
        let object = match self.get_object_version(bucket, key, version_id)? {
            Some((o, _)) => o,
            None => return Ok((Object::default(), false)),
        };
        if !object.delete_marker
            && object_lock_prevents_delete(&object, &self.now(), bypass_governance)
        {
            return Err(StoreError::ObjectLocked);
        }
        let version_dir = self.object_versions_path(bucket, key).join(version_id);
        remove_dir_all_ignoring_missing(&version_dir)?;
        self.rebuild_current_object(bucket, key)?;
        Ok((object, true))
    }

    /// Lists current objects under `prefix`, sorted by key. `None` if the bucket
    /// does not exist.
    pub fn list_objects(&self, bucket: &str, prefix: &str) -> Result<Option<Vec<Object>>> {
        if !valid_bucket_name(bucket) {
            return Err(StoreError::InvalidBucketName);
        }
        if self.get_bucket(bucket)?.is_none() {
            return Ok(None);
        }
        let objects_path = self.objects_path(bucket);
        let entries = match fs::read_dir(&objects_path) {
            Ok(e) => e,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(Some(Vec::new())),
            Err(e) => return Err(StoreError::Io(e)),
        };
        let mut objects = Vec::new();
        for entry in entries {
            let entry = entry?;
            if !entry.file_type()?.is_dir() {
                continue;
            }
            let object = read_object(&entry.path().join("object.json"))?
                .ok_or_else(|| missing("read object metadata"))?;
            if !object.delete_marker && object.key.starts_with(prefix) {
                objects.push(object);
            }
        }
        objects.sort_by(|a, b| a.key.cmp(&b.key));
        Ok(Some(objects))
    }

    /// Lists all object versions (and delete markers) under `prefix`, sorted by
    /// key ascending then last-modified descending. `None` if the bucket is absent.
    pub fn list_object_versions(&self, bucket: &str, prefix: &str) -> Result<Option<Vec<Object>>> {
        if !valid_bucket_name(bucket) {
            return Err(StoreError::InvalidBucketName);
        }
        if self.get_bucket(bucket)?.is_none() {
            return Ok(None);
        }
        let objects_path = self.objects_path(bucket);
        let entries = match fs::read_dir(&objects_path) {
            Ok(e) => e,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(Some(Vec::new())),
            Err(e) => return Err(StoreError::Io(e)),
        };
        let mut versions: Vec<Object> = Vec::new();
        for entry in entries {
            let entry = entry?;
            if !entry.file_type()?.is_dir() {
                continue;
            }
            let object_dir = entry.path();
            let key_versions_path = object_dir.join("versions");
            match fs::read_dir(&key_versions_path) {
                Err(e) if e.kind() == io::ErrorKind::NotFound => {
                    if let Some(object) = read_object_metadata_for_version_list(
                        &object_dir.join("object.json"),
                        NULL_VERSION_ID,
                    )? {
                        if object.key.starts_with(prefix) {
                            versions.push(object);
                        }
                    }
                    continue;
                }
                Err(e) => return Err(StoreError::Io(e)),
                Ok(version_entries) => {
                    for ve in version_entries {
                        let ve = ve?;
                        if !ve.file_type()?.is_dir() {
                            continue;
                        }
                        let object = read_object(&ve.path().join("object.json"))?
                            .ok_or_else(|| missing("read object version metadata"))?;
                        if object.key.starts_with(prefix) {
                            versions.push(object);
                        }
                    }
                    if !key_versions_path.join(NULL_VERSION_ID).exists() {
                        if let Some(object) = read_object_metadata_for_version_list(
                            &object_dir.join("object.json"),
                            NULL_VERSION_ID,
                        )? {
                            if object_version_id(&object) == NULL_VERSION_ID
                                && object.key.starts_with(prefix)
                            {
                                versions.push(object);
                            }
                        }
                    }
                }
            }
        }
        versions.sort_by(|a, b| match a.key.cmp(&b.key) {
            Ordering::Equal => {
                if time_after(&a.last_modified, &b.last_modified) {
                    Ordering::Less
                } else if time_after(&b.last_modified, &a.last_modified) {
                    Ordering::Greater
                } else {
                    Ordering::Equal
                }
            }
            other => other,
        });
        Ok(Some(versions))
    }

    // --- version internals --------------------------------------------------

    fn read_current_object_metadata(&self, bucket: &str, key: &str) -> Result<Option<Object>> {
        read_object(&self.object_path(bucket, key).join("object.json"))
    }

    pub(crate) fn write_object_version(
        &self,
        object_path: &Path,
        object: &Object,
        body: &[u8],
    ) -> Result<()> {
        if object.version_id.is_empty() {
            return Ok(());
        }
        let version_path = object_path.join("versions").join(&object.version_id);
        fs::create_dir_all(&version_path)?;
        Self::write_json(&version_path.join("object.json"), object)?;
        if object.delete_marker {
            let _ = remove_if_exists(&version_path.join("body"));
            return Ok(());
        }
        fs::write(version_path.join("body"), body)?;
        Ok(())
    }

    fn get_null_object_version(
        &self,
        bucket: &str,
        key: &str,
    ) -> Result<Option<(Object, Vec<u8>)>> {
        let version_path = self.object_versions_path(bucket, key).join(NULL_VERSION_ID);
        if let Some(object) = read_object(&version_path.join("object.json"))? {
            if object.delete_marker {
                return Ok(Some((object, Vec::new())));
            }
            let body = fs::read(version_path.join("body"))?;
            return Ok(Some((object, body)));
        }
        let current_path = self.object_path(bucket, key);
        let mut object = match read_object(&current_path.join("object.json"))? {
            Some(o) => o,
            None => return Ok(None),
        };
        if !object.version_id.is_empty() && object.version_id != NULL_VERSION_ID {
            return Ok(None);
        }
        object.version_id = NULL_VERSION_ID.to_string();
        if object.delete_marker {
            return Ok(Some((object, Vec::new())));
        }
        let body = fs::read(current_path.join("body"))?;
        Ok(Some((object, body)))
    }

    fn rebuild_current_object(&self, bucket: &str, key: &str) -> Result<()> {
        let path = self.object_path(bucket, key);
        let versions_dir = path.join("versions");
        let entries = match fs::read_dir(&versions_dir) {
            Ok(e) => e,
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                return remove_dir_all_ignoring_missing(&path);
            }
            Err(e) => return Err(StoreError::Io(e)),
        };
        let mut latest: Option<Object> = None;
        let mut latest_body: Vec<u8> = Vec::new();
        for entry in entries {
            let entry = entry?;
            if !entry.file_type()?.is_dir() {
                continue;
            }
            let version_path = entry.path();
            let object = read_object(&version_path.join("object.json"))?
                .ok_or_else(|| missing("read object version metadata"))?;
            if let Some(ref l) = latest {
                if !time_after(&object.last_modified, &l.last_modified) {
                    continue;
                }
            }
            latest_body = if object.delete_marker {
                Vec::new()
            } else {
                fs::read(version_path.join("body"))?
            };
            latest = Some(object);
        }
        let latest = match latest {
            Some(l) => l,
            None => return remove_dir_all_ignoring_missing(&path),
        };
        Self::write_json(&path.join("object.json"), &latest)?;
        let body_path = path.join("body");
        if latest.delete_marker {
            let _ = remove_if_exists(&body_path);
            return Ok(());
        }
        fs::write(body_path, &latest_body)?;
        Ok(())
    }
}
