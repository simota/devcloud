//! The multipart-upload plane on `FileBucketStore`: create/upload-part/list/
//! complete/abort and the per-bucket upload listing. Ported 1:1 from the legacy
//! `store_multipart.rs`. The on-disk layout is
//! `multipart/<uploadId>/{upload.json, parts/<00001>/{part.json, body}}`.

use crate::hashes::{md5_etag, multipart_etag, validate_content_md5, ContentMd5Error};
use crate::model::{MultipartPart, MultipartUpload, Object};
use crate::objops::{
    clean_metadata, clean_server_side_encryption, CreateMultipartUploadInput, PutObjectInput,
};
use crate::store::{
    remove_dir_all_ignoring_missing, write_atomic, ConditionalError, FileBucketStore, Result,
    StoreError,
};
use crate::validation::valid_upload_id;
use std::fs;
use std::io;
use std::path::Path;

fn read_json<T: serde::de::DeserializeOwned>(path: &Path) -> Result<Option<T>> {
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

impl FileBucketStore {
    /// Begins a multipart upload, allocating an upload ID and writing
    /// `upload.json`. Defaults the content type to `application/octet-stream`.
    pub fn create_multipart_upload(
        &self,
        input: CreateMultipartUploadInput,
    ) -> Result<MultipartUpload> {
        let _guard = self.lock_object_writes();
        self.require_bucket_and_key(&input.bucket, &input.key)?;
        let mut upload = MultipartUpload {
            bucket: input.bucket.clone(),
            key: input.key.clone(),
            upload_id: self.new_version_id(),
            created_at: self.now(),
            content_type: input.content_type.clone(),
            content_encoding: input.content_encoding.clone(),
            cache_control: input.cache_control.clone(),
            content_disposition: input.content_disposition.clone(),
            metadata: clean_metadata(&input.metadata),
            encryption: clean_server_side_encryption(&input.encryption),
        };
        if upload.content_type.is_empty() {
            upload.content_type = "application/octet-stream".to_string();
        }
        let path = self.multipart_upload_path(&input.bucket, &upload.upload_id);
        fs::create_dir_all(path.join("parts"))?;
        Self::write_json(&path.join("upload.json"), &upload)?;
        Ok(upload)
    }

    /// Uploads one part (1..=10000), writing its body and `part.json`.
    pub fn upload_part(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        part_number: i64,
        body: &[u8],
        content_md5: &str,
    ) -> Result<MultipartPart> {
        self.upload_part_with_sse_c(bucket, key, upload_id, part_number, body, content_md5, None)
    }

    /// Uploads one part, encrypting the stored part bytes when `sse_c_key` is
    /// provided.
    pub fn upload_part_with_sse_c(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        part_number: i64,
        body: &[u8],
        content_md5: &str,
        sse_c_key: Option<&[u8; 32]>,
    ) -> Result<MultipartPart> {
        if !(1..=10000).contains(&part_number) {
            return Err(StoreError::InvalidPartNumber);
        }
        let upload = self.get_multipart_upload(bucket, key, upload_id)?;
        match upload {
            Some(u) if u.key == key => {}
            _ => return Err(StoreError::MultipartUploadNotExist),
        }
        match validate_content_md5(content_md5, body) {
            Ok(()) => {}
            Err(ContentMd5Error::Invalid) => return Err(StoreError::InvalidContentMd5),
            Err(ContentMd5Error::Mismatch) => return Err(StoreError::ContentMd5Mismatch),
        }
        let part = MultipartPart {
            part_number,
            etag: md5_etag(body),
            size: body.len() as i64,
            last_modified: self.now(),
        };
        let disk_body = match sse_c_key {
            Some(key) => crate::sse_c::encrypt_frame(key, body),
            None => body.to_vec(),
        };
        let path = self.multipart_part_path(bucket, upload_id, part_number);
        fs::create_dir_all(&path)?;
        write_atomic(&path.join("body"), &disk_body)?;
        Self::write_json(&path.join("part.json"), &part)?;
        Ok(part)
    }

    /// Reads an upload's metadata if it exists for `(bucket, key)`.
    pub fn get_multipart_upload_metadata(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
    ) -> Result<Option<MultipartUpload>> {
        self.get_multipart_upload(bucket, key, upload_id)
    }

    /// Lists an upload's parts (sorted by part number) with its metadata. `None`
    /// if the upload does not exist.
    pub fn list_parts(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
    ) -> Result<Option<(MultipartUpload, Vec<MultipartPart>)>> {
        let upload = match self.get_multipart_upload(bucket, key, upload_id)? {
            Some(u) => u,
            None => return Ok(None),
        };
        let parts = self.read_multipart_parts(bucket, upload_id)?;
        Ok(Some((upload, parts)))
    }

    /// Concatenates the listed parts into the final object (stamping the
    /// composite multipart ETag) and removes the upload. `None` if the upload is
    /// absent; `InvalidPart` if a listed part is missing or its on-disk body
    /// length no longer matches the size recorded in `part.json`.
    pub fn complete_multipart_upload(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        part_numbers: &[i64],
    ) -> Result<Option<Object>> {
        let _guard = self.lock_object_writes();
        self.complete_multipart_upload_locked(bucket, key, upload_id, part_numbers)
    }

    fn complete_multipart_upload_locked(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        part_numbers: &[i64],
    ) -> Result<Option<Object>> {
        let upload = match self.get_multipart_upload(bucket, key, upload_id)? {
            Some(u) => u,
            None => return Ok(None),
        };
        let is_sse_c = upload.encryption.is_sse_c();
        let mut combined: Vec<u8> = Vec::new();
        let mut part_etags: Vec<String> = Vec::with_capacity(part_numbers.len());
        let mut total_plaintext_size: i64 = 0;
        for &part_number in part_numbers {
            let part_path = self.multipart_part_path(bucket, upload_id, part_number);
            let part: MultipartPart = read_json(&part_path.join("part.json"))?
                .ok_or(StoreError::InvalidPart(part_number))?;
            let body = match fs::read(part_path.join("body")) {
                Ok(b) => b,
                Err(e) if e.kind() == io::ErrorKind::NotFound => {
                    return Err(StoreError::InvalidPart(part_number));
                }
                Err(e) => return Err(StoreError::Io(e)),
            };
            if is_sse_c {
                if crate::sse_c::framed_plaintext_len(&body).map(|n| n as i64) != Some(part.size) {
                    return Err(StoreError::InvalidPart(part_number));
                }
            } else if body.len() as i64 != part.size {
                return Err(StoreError::InvalidPart(part_number));
            }
            total_plaintext_size += part.size;
            combined.extend_from_slice(&body);
            part_etags.push(part.etag);
        }
        let put_input = PutObjectInput {
            bucket: upload.bucket.clone(),
            key: upload.key.clone(),
            body: combined.clone(),
            content_type: upload.content_type.clone(),
            content_encoding: upload.content_encoding.clone(),
            cache_control: upload.cache_control.clone(),
            content_disposition: upload.content_disposition.clone(),
            metadata: upload.metadata.clone(),
            encryption: upload.encryption.clone(),
            ..Default::default()
        };
        let mut object = if is_sse_c {
            self.put_pre_encrypted_object_locked(put_input, total_plaintext_size)?
        } else {
            self.put_object_locked(put_input)?
        };
        object.etag = multipart_etag(&part_etags);
        let object_path = self.object_path(&upload.bucket, &upload.key);
        Self::write_json(&object_path.join("object.json"), &object)?;
        if !object.version_id.is_empty() {
            self.write_object_version(&object_path, &object, &combined)?;
        }
        remove_dir_all_ignoring_missing(&self.multipart_upload_path(bucket, upload_id))?;
        let _ = fs::remove_dir(self.multipart_path(bucket));
        Ok(Some(object))
    }

    /// `complete_multipart_upload` guarded by `If-None-Match: *`: fails with
    /// `PreconditionFailed` (leaving the upload in place) when a current object
    /// exists at the key.
    pub fn complete_multipart_upload_if_absent(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        part_numbers: &[i64],
    ) -> Result<Option<Object>> {
        match self.complete_multipart_upload_if(bucket, key, upload_id, part_numbers, |current| {
            if current.is_some() {
                Err(())
            } else {
                Ok(())
            }
        }) {
            Ok(v) => Ok(v),
            Err(ConditionalError::Condition(())) => Err(StoreError::PreconditionFailed),
            Err(ConditionalError::Store(e)) => Err(e),
        }
    }

    /// Checks the current object under the root write lock and completes the
    /// multipart upload if `predicate` succeeds. Leaves the upload intact on
    /// condition failure.
    pub fn complete_multipart_upload_if<E>(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        part_numbers: &[i64],
        predicate: impl FnOnce(Option<&Object>) -> std::result::Result<(), E>,
    ) -> std::result::Result<Option<Object>, ConditionalError<E>> {
        let _guard = self.lock_object_writes();
        if self.get_multipart_upload(bucket, key, upload_id)?.is_none() {
            return Ok(None);
        }
        self.check_current_object_locked(bucket, key, predicate)?;
        Ok(self.complete_multipart_upload_locked(bucket, key, upload_id, part_numbers)?)
    }

    /// Aborts an upload, discarding its parts. `true` if it existed.
    pub fn abort_multipart_upload(&self, bucket: &str, key: &str, upload_id: &str) -> Result<bool> {
        if self.get_multipart_upload(bucket, key, upload_id)?.is_none() {
            return Ok(false);
        }
        remove_dir_all_ignoring_missing(&self.multipart_upload_path(bucket, upload_id))?;
        let _ = fs::remove_dir(self.multipart_path(bucket));
        Ok(true)
    }

    /// Lists a bucket's in-progress uploads, sorted by key then upload ID. `None`
    /// if the bucket does not exist.
    pub fn list_multipart_uploads(&self, bucket: &str) -> Result<Option<Vec<MultipartUpload>>> {
        if self.get_bucket(bucket)?.is_none() {
            return Ok(None);
        }
        let entries = match fs::read_dir(self.multipart_path(bucket)) {
            Ok(e) => e,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(Some(Vec::new())),
            Err(e) => return Err(StoreError::Io(e)),
        };
        let mut uploads = Vec::new();
        for entry in entries {
            let entry = entry?;
            if !entry.file_type()?.is_dir() {
                continue;
            }
            let upload: MultipartUpload = read_json(&entry.path().join("upload.json"))?
                .ok_or_else(|| missing("read multipart upload metadata"))?;
            uploads.push(upload);
        }
        uploads.sort_by(|a, b| match a.key.cmp(&b.key) {
            std::cmp::Ordering::Equal => a.upload_id.cmp(&b.upload_id),
            other => other,
        });
        Ok(Some(uploads))
    }

    fn get_multipart_upload(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
    ) -> Result<Option<MultipartUpload>> {
        if !valid_upload_id(upload_id) {
            return Err(StoreError::InvalidUploadId);
        }
        self.require_bucket_and_key(bucket, key)?;
        let upload: MultipartUpload = match read_json(
            &self
                .multipart_upload_path(bucket, upload_id)
                .join("upload.json"),
        )? {
            Some(u) => u,
            None => return Ok(None),
        };
        if upload.key != key {
            return Ok(None);
        }
        Ok(Some(upload))
    }

    fn read_multipart_parts(&self, bucket: &str, upload_id: &str) -> Result<Vec<MultipartPart>> {
        let parts_dir = self.multipart_upload_path(bucket, upload_id).join("parts");
        let entries = match fs::read_dir(&parts_dir) {
            Ok(e) => e,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) => return Err(StoreError::Io(e)),
        };
        let mut parts = Vec::new();
        for entry in entries {
            let entry = entry?;
            if !entry.file_type()?.is_dir() {
                continue;
            }
            let part: MultipartPart = read_json(&entry.path().join("part.json"))?
                .ok_or_else(|| missing("read multipart part metadata"))?;
            parts.push(part);
        }
        parts.sort_by(|a, b| a.part_number.cmp(&b.part_number));
        Ok(parts)
    }
}
