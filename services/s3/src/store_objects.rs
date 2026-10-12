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
    remove_dir_all_ignoring_missing, remove_if_exists, write_atomic, FileBucketStore, Result,
    StoreError,
};
use crate::time_fmt::{time_after, GO_ZERO_TIME};
use crate::validation::{valid_bucket_name, valid_object_key, valid_upload_id};
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
        let _guard = self.lock_object_writes();
        self.put_object_locked(input)
    }

    /// `put_object` for a caller already holding `lock_object_writes`.
    pub(crate) fn put_object_locked(&self, input: PutObjectInput) -> Result<Object> {
        self.put_object_with_resumable_id_locked(input, "")
    }

    fn put_object_with_resumable_id_locked(
        &self,
        input: PutObjectInput,
        upload_id: &str,
    ) -> Result<Object> {
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
            resumable_upload_id: upload_id.to_string(),
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
        self.preserve_current_resumable_upload(&input.bucket, &input.key)?;
        write_atomic(&path.join("body"), &input.body)?;
        Self::write_json(&path.join("object.json"), &object)?;
        if !object.version_id.is_empty() {
            self.write_object_version(&path, &object, &input.body)?;
        }
        Ok(object)
    }

    /// Commits a GCS resumable upload exactly once. The identity is included in
    /// the atomic object metadata write, so a crash before the completion
    /// receipt is linked can be recovered without assigning a new generation.
    pub fn put_resumable_object(&self, input: PutObjectInput, upload_id: &str) -> Result<Object> {
        if !valid_upload_id(upload_id) {
            return Err(StoreError::InvalidUploadId);
        }
        let _guard = self.lock_object_writes();
        if let Some(receipt) = self.read_resumable_upload_receipt(upload_id)? {
            if receipt.bucket != input.bucket || receipt.key != input.key {
                return Err(StoreError::InvalidUploadId);
            }
            return Ok(receipt);
        }
        if let Some(receipt) =
            self.get_resumable_upload_result_locked(&input.bucket, Some(&input.key), upload_id)?
        {
            return Ok(receipt);
        }
        let object = self.put_object_with_resumable_id_locked(input, upload_id)?;
        // The version snapshot is already complete on this path. Recovery of
        // an interrupted commit uses preserve_resumable_upload_result instead.
        fs::hard_link(
            self.object_path(&object.bucket, &object.key)
                .join("object.json"),
            self.resumable_upload_receipt_path(upload_id),
        )?;
        Ok(object)
    }

    /// Claims the best-effort dashboard notification after a durable result is
    /// available. Both an initial completion and a recovered retry use this
    /// boundary, so an I/O failure can be retried without emitting twice.
    pub fn claim_resumable_upload_event(&self, object: &Object) -> Result<bool> {
        let _guard = self.lock_object_writes();
        let receipt = self.read_resumable_upload_receipt(&object.resumable_upload_id)?;
        if receipt.as_ref() != Some(object) {
            return Err(StoreError::InvalidUploadId);
        }
        let path = self
            .resumable_upload_receipt_path(&object.resumable_upload_id)
            .with_extension("notified");
        match fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
        {
            Ok(_) => Ok(true),
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                if fs::metadata(path)?.is_file() {
                    Ok(false)
                } else {
                    Err(StoreError::Io(error))
                }
            }
            Err(error) => Err(StoreError::Io(error)),
        }
    }

    /// Returns the original completion result even after session cleanup or a
    /// later object overwrite/deletion. An outstanding session supplies `key`
    /// so the atomic object marker can recover a missing receipt after restart.
    pub fn get_resumable_upload_result(
        &self,
        bucket: &str,
        key: Option<&str>,
        upload_id: &str,
    ) -> Result<Option<Object>> {
        let _guard = self.lock_object_writes();
        self.get_resumable_upload_result_locked(bucket, key, upload_id)
    }

    fn get_resumable_upload_result_locked(
        &self,
        bucket: &str,
        key: Option<&str>,
        upload_id: &str,
    ) -> Result<Option<Object>> {
        if !valid_upload_id(upload_id)
            || !valid_bucket_name(bucket)
            || key.is_some_and(|key| !valid_object_key(key))
        {
            return Ok(None);
        }
        if let Some(receipt) = self.read_resumable_upload_receipt(upload_id)? {
            return Ok(
                (receipt.bucket == bucket && key.is_none_or(|key| receipt.key == key))
                    .then_some(receipt),
            );
        }
        let Some(key) = key else {
            return Ok(None);
        };
        let path = self.object_path(bucket, key).join("object.json");
        if let Some(object) = read_object(&path)? {
            if !object.delete_marker
                && object.resumable_upload_id == upload_id
                && object.bucket == bucket
                && object.key == key
            {
                self.preserve_resumable_upload_result(&object, &path)?;
                return self.read_resumable_upload_receipt(upload_id);
            }
        }
        Ok(None)
    }

    fn read_resumable_upload_receipt(&self, upload_id: &str) -> Result<Option<Object>> {
        if !valid_upload_id(upload_id) {
            return Ok(None);
        }
        let receipt = read_object(&self.resumable_upload_receipt_path(upload_id))?;
        if receipt.as_ref().is_some_and(|object| {
            object.resumable_upload_id != upload_id
                || object.delete_marker
                || !valid_bucket_name(&object.bucket)
                || !valid_object_key(&object.key)
        }) {
            return Err(StoreError::Io(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid resumable upload receipt",
            )));
        }
        Ok(receipt)
    }

    pub(crate) fn preserve_current_resumable_upload(&self, bucket: &str, key: &str) -> Result<()> {
        let path = self.object_path(bucket, key).join("object.json");
        if let Some(object) = read_object(&path)? {
            self.preserve_resumable_upload_result(&object, &path)?;
        }
        Ok(())
    }

    /// Must run under the root-wide write lock before replacing any metadata
    /// or deleting an object. Every metadata update uses rename, so a hard link
    /// atomically preserves the exact original result in an immutable inode.
    /// If the receipt cannot be preserved, the subsequent mutation must fail.
    pub(crate) fn preserve_resumable_upload_result(
        &self,
        object: &Object,
        source: &Path,
    ) -> Result<()> {
        let upload_id = &object.resumable_upload_id;
        if upload_id.is_empty() {
            return Ok(());
        }
        if !valid_upload_id(upload_id)
            || !valid_bucket_name(&object.bucket)
            || !valid_object_key(&object.key)
            || object.delete_marker
        {
            return Err(StoreError::InvalidUploadId);
        }
        if let Some(receipt) = self.read_resumable_upload_receipt(upload_id)? {
            if receipt.bucket != object.bucket
                || receipt.key != object.key
                || receipt.last_modified != object.last_modified
                || receipt.etag != object.etag
                || receipt.size != object.size
            {
                return Err(StoreError::InvalidUploadId);
            }
            return Ok(());
        }
        let path = self.object_path(&object.bucket, &object.key);
        if !object.version_id.is_empty() && source == path.join("object.json") {
            // Also recover a stop between the current metadata rename and its
            // version snapshot, before any subsequent writer discards the body.
            let body = fs::read(path.join("body"))?;
            self.write_object_version(&path, object, &body)?;
        }
        fs::hard_link(source, self.resumable_upload_receipt_path(upload_id))?;
        Ok(())
    }

    /// `put_object` guarded by `If-None-Match: *`: fails with
    /// `PreconditionFailed` when a current object (not a delete marker) exists.
    pub fn put_object_if_absent(&self, input: PutObjectInput) -> Result<Object> {
        let _guard = self.lock_object_writes();
        self.require_bucket_and_key(&input.bucket, &input.key)?;
        if self.current_object_exists(&input.bucket, &input.key)? {
            return Err(StoreError::PreconditionFailed);
        }
        self.put_object_locked(input)
    }

    /// Whether `key` has a current object that is not a delete marker. Callers
    /// validate the bucket and key first.
    pub(crate) fn current_object_exists(&self, bucket: &str, key: &str) -> Result<bool> {
        let path = self.object_path(bucket, key).join("object.json");
        Ok(read_object(&path)?.is_some_and(|object| !object.delete_marker))
    }

    /// Updates an existing object's metadata fields (content-type/encoding/cache/
    /// disposition/user metadata), bumping the metageneration. `None` if absent.
    pub fn update_object_metadata(
        &self,
        input: UpdateObjectMetadataInput,
    ) -> Result<Option<Object>> {
        let _guard = self.lock_object_writes();
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
        self.preserve_resumable_upload_result(&object, &path.join("object.json"))?;
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
        let path = Self::version_path(&self.object_path(bucket, key), version_id)?;
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
        let _guard = self.lock_object_writes();
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
                self.preserve_resumable_upload_result(&current, &path.join("object.json"))?;
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
            self.preserve_resumable_upload_result(&current, &path.join("object.json"))?;
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
        let _guard = self.lock_object_writes();
        self.require_bucket_and_key(bucket, key)?;
        let version_dir = Self::version_path(&self.object_path(bucket, key), version_id)?;
        self.preserve_current_resumable_upload(bucket, key)?;
        let object = match self.get_object_version(bucket, key, version_id)? {
            Some((o, _)) => o,
            None => return Ok((Object::default(), false)),
        };
        if !object.delete_marker
            && object_lock_prevents_delete(&object, &self.now(), bypass_governance)
        {
            return Err(StoreError::ObjectLocked);
        }
        // A null-version read may use the current object when no version
        // snapshot exists yet; the current preservation above covers it.
        if version_dir.join("object.json").is_file() {
            self.preserve_resumable_upload_result(&object, &version_dir.join("object.json"))?;
        }
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
                    // Equal timestamps: tie-break on version id (descending) so
                    // the "latest" version matches `rebuild_current_object`
                    // regardless of directory read order.
                    b.version_id.cmp(&a.version_id)
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
        let version_path = Self::version_path(object_path, &object.version_id)?;
        fs::create_dir_all(&version_path)?;
        Self::write_json(&version_path.join("object.json"), object)?;
        if object.delete_marker {
            let _ = remove_if_exists(&version_path.join("body"));
            return Ok(());
        }
        write_atomic(&version_path.join("body"), body)?;
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
        self.preserve_current_resumable_upload(bucket, key)?;
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
                if time_after(&l.last_modified, &object.last_modified) {
                    continue;
                }
                // On an exact timestamp tie, break deterministically on version id
                // rather than `fs::read_dir` order (which is filesystem-dependent).
                if !time_after(&object.last_modified, &l.last_modified)
                    && object.version_id <= l.version_id
                {
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
        write_atomic(&body_path, &latest_body)?;
        Ok(())
    }
}
