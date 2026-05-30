//! The on-disk `FileBucketStore` — the `BucketStore` boundary the Go S3 service
//! (and GCS/BigQuery/Redshift) share. Part 2 covers the storage root layout,
//! object-key path encoding, byte-compatible metadata persistence, and the bucket
//! CRUD plane (create / get / list / delete). The object and multipart data
//! planes and the bucket sub-resources land in later parts.

use crate::base64;
use crate::go_json;
use crate::model::Bucket;
use crate::time_fmt::now_rfc3339nano;
use crate::validation::valid_bucket_name;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

/// The metadata sidecar files (and `inventory`/`analytics` directories) that do
/// not count toward bucket emptiness — a bucket carrying only these is deletable.
/// Mirrors the allowlist in the Go `DeleteBucket`.
const SIDECAR_FILES: &[&str] = &[
    "bucket.json",
    "versioning.json",
    "policy.json",
    "acl.json",
    "lifecycle.json",
    "notification.json",
    "notification-events.json",
    "object-lock.json",
    "replication.json",
];
const SIDECAR_DIRS: &[&str] = &["inventory", "analytics"];

/// A `FileBucketStore` error. The Go store returns `error`; callers distinguish
/// only a few cases (invalid name, non-empty bucket), so we model those plus a
/// catch-all I/O variant.
#[derive(Debug)]
pub enum StoreError {
    InvalidBucketName,
    InvalidObjectKey,
    BucketNotEmpty,
    BucketNotExist,
    Io(io::Error),
}

impl From<io::Error> for StoreError {
    fn from(e: io::Error) -> Self {
        StoreError::Io(e)
    }
}

pub type Result<T> = std::result::Result<T, StoreError>;

pub struct FileBucketStore {
    root: PathBuf,
    /// Test hook: when set, `created_at` uses this fixed RFC3339Nano timestamp
    /// instead of the wall clock, so on-disk metadata is byte-deterministic.
    fixed_now: Option<String>,
}

impl FileBucketStore {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        FileBucketStore {
            root: root.into(),
            fixed_now: None,
        }
    }

    /// Pins the clock for deterministic metadata in tests.
    pub fn set_fixed_now(&mut self, ts: &str) {
        self.fixed_now = Some(ts.to_string());
    }

    fn now(&self) -> String {
        self.fixed_now.clone().unwrap_or_else(now_rfc3339nano)
    }

    // --- path layout --------------------------------------------------------

    pub fn bucket_path(&self, name: &str) -> PathBuf {
        self.root.join(name)
    }

    pub fn objects_path(&self, bucket: &str) -> PathBuf {
        self.bucket_path(bucket).join("objects")
    }

    /// `root/<bucket>/objects/<base64rawurl(key)>` — the object directory holding
    /// `body` + `object.json`.
    pub fn object_path(&self, bucket: &str, key: &str) -> PathBuf {
        self.objects_path(bucket)
            .join(base64::raw_url_encode(key.as_bytes()))
    }

    pub fn object_versions_path(&self, bucket: &str, key: &str) -> PathBuf {
        self.object_path(bucket, key).join("versions")
    }

    pub fn multipart_path(&self, bucket: &str) -> PathBuf {
        self.bucket_path(bucket).join("multipart")
    }

    pub fn multipart_upload_path(&self, bucket: &str, upload_id: &str) -> PathBuf {
        self.multipart_path(bucket).join(upload_id)
    }

    pub fn multipart_part_path(&self, bucket: &str, upload_id: &str, part_number: i64) -> PathBuf {
        self.multipart_upload_path(bucket, upload_id)
            .join("parts")
            .join(format!("{part_number:05}"))
    }

    // --- metadata io --------------------------------------------------------

    fn write_json<T: serde::Serialize>(path: &Path, value: &T) -> Result<()> {
        fs::write(path, go_json::to_vec_indent(value))?;
        Ok(())
    }

    // --- bucket CRUD --------------------------------------------------------

    /// Creates a bucket. Returns `(bucket, created)`; `created` is false when the
    /// bucket directory already existed (matching the Go `CreateBucket`).
    pub fn create_bucket(&self, name: &str) -> Result<(Bucket, bool)> {
        if !valid_bucket_name(name) {
            return Err(StoreError::InvalidBucketName);
        }
        let path = self.bucket_path(name);
        match fs::metadata(&path) {
            Ok(_) => {
                // Directory exists: mirror Go's `return bucket, !ok, err`.
                let existing = self.get_bucket(name)?;
                let created = existing.is_none();
                Ok((existing.unwrap_or_default(), created))
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                let bucket = Bucket {
                    name: name.to_string(),
                    created_at: self.now(),
                    ..Default::default()
                };
                fs::create_dir_all(&path)?;
                Self::write_json(&path.join("bucket.json"), &bucket)?;
                Ok((bucket, true))
            }
            Err(e) => Err(StoreError::Io(e)),
        }
    }

    /// Reads a bucket's metadata; `None` if it does not exist.
    pub fn get_bucket(&self, name: &str) -> Result<Option<Bucket>> {
        if !valid_bucket_name(name) {
            return Err(StoreError::InvalidBucketName);
        }
        match fs::read(self.bucket_path(name).join("bucket.json")) {
            Ok(data) => {
                let bucket: Bucket = serde_json::from_slice(&data)
                    .map_err(|e| StoreError::Io(io::Error::other(e)))?;
                Ok(Some(bucket))
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(StoreError::Io(e)),
        }
    }

    /// Lists all buckets, sorted by name.
    pub fn list_buckets(&self) -> Result<Vec<Bucket>> {
        let entries = match fs::read_dir(&self.root) {
            Ok(entries) => entries,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) => return Err(StoreError::Io(e)),
        };
        let mut buckets = Vec::new();
        for entry in entries {
            let entry = entry?;
            if !entry.file_type()?.is_dir() {
                continue;
            }
            let name = entry.file_name().to_string_lossy().into_owned();
            if let Some(bucket) = self.get_bucket(&name)? {
                buckets.push(bucket);
            }
        }
        buckets.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(buckets)
    }

    /// Deletes a bucket if it holds only metadata sidecars; returns whether it
    /// existed. Errors with `BucketNotEmpty` when objects/multipart remain.
    pub fn delete_bucket(&self, name: &str) -> Result<bool> {
        if !valid_bucket_name(name) {
            return Err(StoreError::InvalidBucketName);
        }
        let path = self.bucket_path(name);
        match fs::metadata(&path) {
            Ok(_) => {}
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(false),
            Err(e) => return Err(StoreError::Io(e)),
        }
        for entry in fs::read_dir(&path)? {
            let entry = entry?;
            let entry_name = entry.file_name().to_string_lossy().into_owned();
            if SIDECAR_DIRS.contains(&entry_name.as_str()) {
                continue;
            }
            if !SIDECAR_FILES.contains(&entry_name.as_str()) {
                return Err(StoreError::BucketNotEmpty);
            }
        }
        remove_if_exists(&path.join("bucket.json"))?;
        for sidecar in &SIDECAR_FILES[1..] {
            remove_if_exists(&path.join(sidecar))?;
        }
        for dir in SIDECAR_DIRS {
            remove_dir_all_if_exists(&path.join(dir))?;
        }
        fs::remove_dir(&path)?;
        Ok(true)
    }
}

fn remove_if_exists(path: &Path) -> Result<()> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(StoreError::Io(e)),
    }
}

fn remove_dir_all_if_exists(path: &Path) -> Result<()> {
    match fs::remove_dir_all(path) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(StoreError::Io(e)),
    }
}
