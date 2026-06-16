//! The inventory plane on `FileBucketStore`: configuration CRUD plus CSV report
//! generation (header + per-object rows + manifest). Ported 1:1 from the legacy
//! `store_inventory.rs`.

use crate::base64;
use crate::csv::write_csv;
use crate::model::{InventoryConfiguration, InventoryReportManifest, Object};
use crate::store::{
    read_json_file, remove_dir_all_ignoring_missing, remove_if_exists, FileBucketStore, Result,
    StoreError,
};
use crate::store_config::read_config_dir;
use crate::time_fmt::{parse_rfc3339, rfc3339_seconds_from_unix};
use std::collections::HashMap;
use std::collections::HashSet;
use std::fs;

impl FileBucketStore {
    /// Persists an inventory configuration under `id` and (re)generates its CSV
    /// report when enabled and CSV-formatted. Errors if the bucket is absent.
    pub fn put_bucket_inventory(
        &self,
        bucket: &str,
        id: &str,
        mut config: InventoryConfiguration,
    ) -> Result<()> {
        self.get_bucket(bucket)?.ok_or(StoreError::BucketNotExist)?;
        config.id = id.to_string();
        fs::create_dir_all(self.inventory_path(bucket))?;
        Self::write_json(&self.inventory_config_path(bucket, id), &config)?;
        if !config.is_enabled || inventory_report_format(&config) != "CSV" {
            remove_dir_all_ignoring_missing(&self.inventory_report_path(bucket, id))?;
            return Ok(());
        }
        self.write_inventory_report(bucket, id, &config)
    }

    /// Reads an inventory configuration. `None` if the bucket is absent; the bool
    /// indicates whether the configuration is present.
    pub fn get_bucket_inventory(
        &self,
        bucket: &str,
        id: &str,
    ) -> Result<Option<(InventoryConfiguration, bool)>> {
        if self.get_bucket(bucket)?.is_none() {
            return Ok(None);
        }
        match read_json_file(&self.inventory_config_path(bucket, id))? {
            Some(config) => Ok(Some((config, true))),
            None => Ok(Some((InventoryConfiguration::default(), false))),
        }
    }

    /// Lists inventory configurations, sorted by ID. `None` if the bucket absent.
    pub fn list_bucket_inventories(
        &self,
        bucket: &str,
    ) -> Result<Option<Vec<InventoryConfiguration>>> {
        if self.get_bucket(bucket)?.is_none() {
            return Ok(None);
        }
        let mut configs = read_config_dir(&self.inventory_path(bucket))?;
        configs.sort_by(|a: &InventoryConfiguration, b| a.id.cmp(&b.id));
        Ok(Some(configs))
    }

    /// Removes an inventory configuration and its report. Errors if absent.
    pub fn delete_bucket_inventory(&self, bucket: &str, id: &str) -> Result<bool> {
        self.get_bucket(bucket)?.ok_or(StoreError::BucketNotExist)?;
        remove_if_exists(&self.inventory_config_path(bucket, id))?;
        remove_dir_all_ignoring_missing(&self.inventory_report_path(bucket, id))?;
        let _ = fs::remove_dir(self.inventory_path(bucket));
        Ok(true)
    }

    fn write_inventory_report(
        &self,
        bucket: &str,
        id: &str,
        config: &InventoryConfiguration,
    ) -> Result<()> {
        let objects = self.inventory_report_objects(bucket, config)?;
        let fields = inventory_report_fields(config);
        let report_path = self.inventory_report_path(bucket, id);
        fs::create_dir_all(&report_path)?;

        let latest = latest_version_ids(&objects);
        let mut records: Vec<Vec<String>> = vec![fields.clone()];
        for object in &objects {
            if object.delete_marker {
                continue;
            }
            records.push(inventory_report_row(object, &latest, &fields));
        }
        fs::write(
            self.inventory_report_csv_path(bucket, id),
            write_csv(&records),
        )?;

        let encoded_id = base64::raw_url_encode(id.as_bytes());
        let manifest = InventoryReportManifest {
            configuration_id: id.to_string(),
            source_bucket: bucket.to_string(),
            format: inventory_report_format(config),
            included_versions: inventory_included_versions(config),
            fields,
            object_count: inventory_report_object_count(&objects),
            report_key: format!("inventory/reports/{encoded_id}/inventory.csv"),
        };
        Self::write_json(&self.inventory_report_manifest_path(bucket, id), &manifest)
    }

    fn inventory_report_objects(
        &self,
        bucket: &str,
        config: &InventoryConfiguration,
    ) -> Result<Vec<Object>> {
        if inventory_included_versions(config) == "All" {
            let objects = self
                .list_object_versions(bucket, "")?
                .ok_or(StoreError::BucketNotExist)?;
            Ok(deduplicate_inventory_objects(objects))
        } else {
            self.list_objects(bucket, "")?
                .ok_or(StoreError::BucketNotExist)
        }
    }
}

fn deduplicate_inventory_objects(objects: Vec<Object>) -> Vec<Object> {
    let mut seen = HashSet::new();
    let mut out = Vec::with_capacity(objects.len());
    for object in objects {
        let key = format!("{}\0{}\0{}", object.bucket, object.key, object.version_id);
        if seen.insert(key) {
            out.push(object);
        }
    }
    out
}

fn inventory_report_fields(config: &InventoryConfiguration) -> Vec<String> {
    let mut fields: Vec<String> = [
        "Bucket",
        "Key",
        "Size",
        "LastModifiedDate",
        "ETag",
        "StorageClass",
    ]
    .iter()
    .map(|s| s.to_string())
    .collect();
    if inventory_included_versions(config) == "All" {
        fields.push("VersionId".to_string());
        fields.push("IsLatest".to_string());
    }
    for field in &config.optional_fields {
        let field = field.trim();
        if field.is_empty() || fields.iter().any(|f| f == field) {
            continue;
        }
        fields.push(field.to_string());
    }
    fields
}

fn inventory_report_row(
    object: &Object,
    latest: &HashMap<String, String>,
    fields: &[String],
) -> Vec<String> {
    fields
        .iter()
        .map(|field| match field.as_str() {
            "Bucket" => object.bucket.clone(),
            "Key" => object.key.clone(),
            "Size" => object.size.to_string(),
            "LastModifiedDate" => match parse_rfc3339(&object.last_modified) {
                Some((secs, _)) => rfc3339_seconds_from_unix(secs),
                None => object.last_modified.clone(),
            },
            "ETag" => object.etag.clone(),
            "StorageClass" => "STANDARD".to_string(),
            "VersionId" => object.version_id.clone(),
            "IsLatest" => (latest.get(&object.key) == Some(&object.version_id)).to_string(),
            "EncryptionStatus" => object.encryption.algorithm.clone(),
            "ObjectLockRetainUntilDate" => object.retention.retain_until_date.clone(),
            "ObjectLockRetentionMode" => object.retention.mode.clone(),
            "ObjectLockLegalHoldStatus" => object.legal_hold.status.clone(),
            _ => String::new(),
        })
        .collect()
}

fn latest_version_ids(objects: &[Object]) -> HashMap<String, String> {
    let mut latest: HashMap<String, String> = HashMap::new();
    let mut latest_modified: HashMap<String, String> = HashMap::new();
    for object in objects {
        if let Some(current) = latest_modified.get(&object.key) {
            if !crate::time_fmt::time_after(&object.last_modified, current) {
                continue;
            }
        }
        latest.insert(object.key.clone(), object.version_id.clone());
        latest_modified.insert(object.key.clone(), object.last_modified.clone());
    }
    latest
}

fn inventory_report_object_count(objects: &[Object]) -> i64 {
    objects.iter().filter(|o| !o.delete_marker).count() as i64
}

fn inventory_included_versions(config: &InventoryConfiguration) -> String {
    if config.included_object_versions == "All" {
        "All".to_string()
    } else {
        "Current".to_string()
    }
}

fn inventory_report_format(config: &InventoryConfiguration) -> String {
    let format = config.destination.s3_bucket_destination.format.trim();
    if format.is_empty() {
        "CSV".to_string()
    } else {
        format.to_string()
    }
}
