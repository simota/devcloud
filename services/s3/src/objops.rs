//! Object-operation helpers: input structs, metadata normalization (`clean*`),
//! and the object-lock decision/derivation functions, ported from the legacy
//! `store.rs` helpers.

use crate::model::{
    DefaultRetention, Object, ObjectLegalHold, ObjectLockConfiguration, ObjectRetention,
    ServerSideEncryption,
};
use crate::time_fmt::{
    add_calendar_years_rfc3339, parse_rfc3339, rfc3339_seconds_from_unix, time_after,
};
use std::collections::BTreeMap;

/// Input to `FileBucketStore::put_object` (the legacy `PutObjectInput`, with the body
/// already read into memory).
#[derive(Debug, Default, Clone)]
pub struct PutObjectInput {
    pub bucket: String,
    pub key: String,
    pub body: Vec<u8>,
    pub content_md5: String,
    pub content_type: String,
    pub content_encoding: String,
    pub cache_control: String,
    pub content_disposition: String,
    pub metadata: BTreeMap<String, String>,
    pub encryption: ServerSideEncryption,
    pub retention: ObjectRetention,
    pub legal_hold: ObjectLegalHold,
}

/// Input to `FileBucketStore::create_multipart_upload` (the legacy
/// `CreateMultipartUploadInput`).
#[derive(Debug, Default, Clone)]
pub struct CreateMultipartUploadInput {
    pub bucket: String,
    pub key: String,
    pub content_type: String,
    pub content_encoding: String,
    pub cache_control: String,
    pub content_disposition: String,
    pub metadata: BTreeMap<String, String>,
    pub encryption: ServerSideEncryption,
}

/// Input to `FileBucketStore::update_object_metadata` (the legacy
/// `UpdateObjectMetadataInput`).
#[derive(Debug, Default, Clone)]
pub struct UpdateObjectMetadataInput {
    pub bucket: String,
    pub key: String,
    pub content_type: String,
    pub content_encoding: String,
    pub cache_control: String,
    pub content_disposition: String,
    /// `None` means "leave existing metadata untouched"; `Some` replaces it.
    pub metadata: Option<BTreeMap<String, String>>,
}

/// Lowercases metadata keys; an empty map stays empty (the legacy `cleanMetadata`
/// returns nil, which serializes the same as an empty map under `omitempty`).
pub fn clean_metadata(metadata: &BTreeMap<String, String>) -> BTreeMap<String, String> {
    metadata
        .iter()
        .map(|(k, v)| (k.to_lowercase(), v.clone()))
        .collect()
}

pub fn clean_server_side_encryption(e: &ServerSideEncryption) -> ServerSideEncryption {
    ServerSideEncryption {
        algorithm: e.algorithm.trim().to_string(),
        kms_key_id: e.kms_key_id.trim().to_string(),
        bucket_key_enabled: e.bucket_key_enabled,
    }
}

pub fn clean_object_retention(r: &ObjectRetention) -> ObjectRetention {
    ObjectRetention {
        mode: r.mode.trim().to_string(),
        retain_until_date: r.retain_until_date.trim().to_string(),
    }
}

pub fn clean_object_legal_hold(h: &ObjectLegalHold) -> ObjectLegalHold {
    ObjectLegalHold {
        status: h.status.trim().to_string(),
    }
}

/// Whether object-lock state blocks deletion of `object` at `now`
/// (RFC3339(/Nano) UTC). Mirrors the legacy `objectLockPreventsDelete`.
pub fn object_lock_prevents_delete(object: &Object, now: &str, bypass_governance: bool) -> bool {
    if object.legal_hold.status == "ON" {
        return true;
    }
    if object.retention.mode.is_empty() || object.retention.retain_until_date.is_empty() {
        return false;
    }
    if object.retention.mode == "GOVERNANCE" && bypass_governance {
        return false;
    }
    if parse_rfc3339(&object.retention.retain_until_date).is_none() {
        return true; // unparseable retain-until is treated as locked
    }
    time_after(&object.retention.retain_until_date, now)
}

/// Derives the default retention to stamp on a new object from a bucket's
/// object-lock configuration. `now` is RFC3339(/Nano) UTC. Mirrors the legacy
/// `defaultObjectRetention`.
pub fn default_object_retention(config: &ObjectLockConfiguration, now: &str) -> ObjectRetention {
    let default: &DefaultRetention = &config.rule.default_retention;
    if default.mode.is_empty() {
        return ObjectRetention::default();
    }
    let retain_until = if default.days > 0 {
        let (secs, _) = match parse_rfc3339(now) {
            Some(v) => v,
            None => return ObjectRetention::default(),
        };
        rfc3339_seconds_from_unix(secs + default.days * 86_400)
    } else if default.years > 0 {
        match add_calendar_years_rfc3339(now, default.years) {
            Some(v) => v,
            None => return ObjectRetention::default(),
        }
    } else {
        return ObjectRetention::default();
    };
    ObjectRetention {
        mode: default.mode.clone(),
        retain_until_date: retain_until,
    }
}

/// The effective version ID of an object for version listings: the empty string
/// maps to the literal `null`. Mirrors the legacy `objectVersionID`.
pub fn object_version_id(object: &Object) -> String {
    if object.version_id.is_empty() {
        crate::model::NULL_VERSION_ID.to_string()
    } else {
        object.version_id.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clean_metadata_lowercases_keys() {
        let mut m = BTreeMap::new();
        m.insert("X-Amz-Meta-Foo".to_string(), "Bar".to_string());
        let cleaned = clean_metadata(&m);
        assert_eq!(cleaned.get("x-amz-meta-foo"), Some(&"Bar".to_string()));
    }

    #[test]
    fn legal_hold_blocks_delete() {
        let mut o = Object::default();
        o.legal_hold.status = "ON".to_string();
        assert!(object_lock_prevents_delete(
            &o,
            "2026-05-30T12:00:00Z",
            true
        ));
    }

    #[test]
    fn governance_bypass_allows_delete() {
        let mut o = Object::default();
        o.retention.mode = "GOVERNANCE".to_string();
        o.retention.retain_until_date = "2030-01-01T00:00:00Z".to_string();
        assert!(object_lock_prevents_delete(
            &o,
            "2026-05-30T12:00:00Z",
            false
        ));
        assert!(!object_lock_prevents_delete(
            &o,
            "2026-05-30T12:00:00Z",
            true
        ));
    }

    #[test]
    fn compliance_retention_blocks_until_expiry() {
        let mut o = Object::default();
        o.retention.mode = "COMPLIANCE".to_string();
        o.retention.retain_until_date = "2026-06-30T12:00:00Z".to_string();
        assert!(object_lock_prevents_delete(
            &o,
            "2026-05-30T12:00:00Z",
            true
        ));
        assert!(!object_lock_prevents_delete(
            &o,
            "2026-07-30T12:00:00Z",
            true
        ));
    }

    #[test]
    fn default_retention_days_and_years() {
        let mut cfg = ObjectLockConfiguration::default();
        cfg.rule.default_retention.mode = "GOVERNANCE".to_string();
        cfg.rule.default_retention.days = 30;
        let r = default_object_retention(&cfg, "2026-05-30T12:00:00.5Z");
        assert_eq!(r.mode, "GOVERNANCE");
        assert_eq!(r.retain_until_date, "2026-06-29T12:00:00Z");

        cfg.rule.default_retention.days = 0;
        cfg.rule.default_retention.years = 1;
        let r2 = default_object_retention(&cfg, "2026-05-30T12:00:00Z");
        assert_eq!(r2.retain_until_date, "2027-05-30T12:00:00Z");
    }
}
