//! The multipart-upload plane on `FileBucketStore`: create/upload-part/list/
//! complete/abort and the per-bucket upload listing. Ported 1:1 from the legacy
//! `store_multipart.rs`. The on-disk layout is
//! `multipart/<uploadId>/{upload.json, parts/<00001>/{part.json, body}}`.

use crate::hashes::{md5_etag, multipart_etag, validate_content_md5, ContentMd5Error};
use crate::model::{MultipartPart, MultipartUpload, Object};
use crate::objops::{
    clean_metadata, clean_server_side_encryption, CreateMultipartUploadInput, PutObjectInput,
};
use crate::store::{remove_dir_all_ignoring_missing, FileBucketStore, Result, StoreError};
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
        let path = self.multipart_part_path(bucket, upload_id, part_number);
        fs::create_dir_all(&path)?;
        fs::write(path.join("body"), body)?;
        Self::write_json(&path.join("part.json"), &part)?;
        Ok(part)
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
    /// absent; `InvalidPart` if a listed part is missing.
    pub fn complete_multipart_upload(
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
        let mut combined: Vec<u8> = Vec::new();
        let mut part_etags: Vec<String> = Vec::with_capacity(part_numbers.len());
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
            combined.extend_from_slice(&body);
            part_etags.push(part.etag);
        }
        let mut object = self.put_object(PutObjectInput {
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
        })?;
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
