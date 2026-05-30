//! Object-level ACL, retention, and legal-hold operations on `FileBucketStore`,
//! ported from the Go `store_objects.go` (`PutObjectACL`/`GetObjectACL`/
//! `PutObjectRetention`/`GetObjectRetention`/`PutObjectLegalHold`/
//! `GetObjectLegalHold`/`updateObjectLockMetadata`).

use crate::model::{Object, ObjectLegalHold, ObjectRetention};
use crate::objops::{clean_object_legal_hold, clean_object_retention};
use crate::store::{FileBucketStore, Result, StoreError};
use crate::store_config::AclFile;
use std::io;

fn read_acl_file(path: &std::path::Path) -> Result<Option<AclFile>> {
    match std::fs::read(path) {
        Ok(data) => Ok(Some(
            serde_json::from_slice(&data).map_err(|e| StoreError::Io(io::Error::other(e)))?,
        )),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(StoreError::Io(e)),
    }
}

impl FileBucketStore {
    /// Sets an object (version) ACL. Returns whether the object existed.
    pub fn put_object_acl(
        &self,
        bucket: &str,
        key: &str,
        version_id: &str,
        acl: &str,
    ) -> Result<bool> {
        let (mut object, mut body) = match self.get_object_version(bucket, key, version_id)? {
            Some(v) => v,
            None => return Ok(false),
        };
        object.acl = acl.to_string();
        if object.delete_marker {
            body = Vec::new();
        }
        if !version_id.is_empty() {
            let version_path = self.object_versions_path(bucket, key).join(version_id);
            Self::write_json(&version_path.join("object.json"), &object)?;
            return Ok(true);
        }
        let path = self.object_path(bucket, key);
        Self::write_json(&path.join("object.json"), &object)?;
        if !object.version_id.is_empty() {
            self.write_object_version(&path, &object, &body)?;
        }
        Self::write_json(
            &path.join("acl.json"),
            &AclFile {
                acl: acl.to_string(),
            },
        )?;
        Ok(true)
    }

    /// Reads an object (version) ACL, defaulting to `private`. `None` if absent.
    pub fn get_object_acl(
        &self,
        bucket: &str,
        key: &str,
        version_id: &str,
    ) -> Result<Option<String>> {
        let object = match self.get_object_version(bucket, key, version_id)? {
            Some((o, _)) => o,
            None => return Ok(None),
        };
        if !object.acl.is_empty() {
            return Ok(Some(object.acl));
        }
        match read_acl_file(&self.object_path(bucket, key).join("acl.json"))? {
            Some(file) if !file.acl.is_empty() => Ok(Some(file.acl)),
            _ => Ok(Some("private".to_string())),
        }
    }

    /// Sets object (version) retention.
    pub fn put_object_retention(
        &self,
        bucket: &str,
        key: &str,
        version_id: &str,
        retention: ObjectRetention,
    ) -> Result<Option<Object>> {
        let cleaned = clean_object_retention(&retention);
        self.update_object_lock_metadata(bucket, key, version_id, |object| {
            object.retention = cleaned.clone();
        })
    }

    /// Reads object (version) retention. `None` if the object is absent.
    pub fn get_object_retention(
        &self,
        bucket: &str,
        key: &str,
        version_id: &str,
    ) -> Result<Option<ObjectRetention>> {
        match self.get_object_version(bucket, key, version_id)? {
            Some((object, _)) => Ok(Some(clean_object_retention(&object.retention))),
            None => Ok(None),
        }
    }

    /// Sets object (version) legal hold.
    pub fn put_object_legal_hold(
        &self,
        bucket: &str,
        key: &str,
        version_id: &str,
        legal_hold: ObjectLegalHold,
    ) -> Result<Option<Object>> {
        let cleaned = clean_object_legal_hold(&legal_hold);
        self.update_object_lock_metadata(bucket, key, version_id, |object| {
            object.legal_hold = cleaned.clone();
        })
    }

    /// Reads object (version) legal hold. `None` if the object is absent.
    pub fn get_object_legal_hold(
        &self,
        bucket: &str,
        key: &str,
        version_id: &str,
    ) -> Result<Option<ObjectLegalHold>> {
        match self.get_object_version(bucket, key, version_id)? {
            Some((object, _)) => Ok(Some(clean_object_legal_hold(&object.legal_hold))),
            None => Ok(None),
        }
    }

    fn update_object_lock_metadata(
        &self,
        bucket: &str,
        key: &str,
        version_id: &str,
        update: impl FnOnce(&mut Object),
    ) -> Result<Option<Object>> {
        let (mut object, mut body) = match self.get_object_version(bucket, key, version_id)? {
            Some(v) => v,
            None => return Ok(None),
        };
        update(&mut object);
        if object.delete_marker {
            body = Vec::new();
        }
        if !version_id.is_empty() {
            let version_path = self.object_versions_path(bucket, key).join(version_id);
            Self::write_json(&version_path.join("object.json"), &object)?;
        } else {
            Self::write_json(&self.object_path(bucket, key).join("object.json"), &object)?;
        }
        if !object.version_id.is_empty() {
            self.write_object_version(&self.object_path(bucket, key), &object, &body)?;
        }
        Ok(Some(object))
    }
}
