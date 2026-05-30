//! Bucket sub-resource persistence on `FileBucketStore`. Part 3 ports only
//! versioning (the object data plane depends on a bucket's versioning state);
//! object-lock, lifecycle, ACL, policy, notification, inventory, analytics, and
//! replication land in later parts.

use crate::store::{FileBucketStore, Result, StoreError};
use serde::{Deserialize, Serialize};
use std::io;

/// The `versioning.json` sidecar (`{"status": "..."}`).
#[derive(Debug, Default, Serialize, Deserialize)]
struct VersioningFile {
    status: String,
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
}
