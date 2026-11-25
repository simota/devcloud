//! Lambda layers: records, ARN/parameter validation, and merging attached
//! layer trees into a per-environment `/opt` stand-in directory.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::server::{
    code_sha256, parse_json, parse_pagination, query_encode, str_field, validate_runtime, Reply,
    Server,
};
use crate::time_fmt::now_lambda;

pub(crate) const MAX_LAYERS_PER_FUNCTION: usize = 5;
const MAX_COMPATIBLE_RUNTIMES: usize = 15;
const MAX_COMPATIBLE_ARCHITECTURES: usize = 2;
const MAX_LICENSE_INFO_LEN: usize = 512;

fn default_next_version() -> u64 {
    1
}

/// Persisted layer catalog entry (all published versions of one layer name).
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "PascalCase")]
pub(crate) struct LayerRecord {
    pub layer_name: String,
    #[serde(default = "default_next_version")]
    pub next_version: u64,
    #[serde(default)]
    pub versions: BTreeMap<u64, LayerVersionRecord>,
}

/// One immutable published layer version.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "PascalCase")]
pub(crate) struct LayerVersionRecord {
    pub version: u64,
    #[serde(default)]
    pub description: String,
    pub created_date: String,
    pub code_sha256: String,
    pub code_size: i64,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub compatible_runtimes: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub compatible_architectures: Vec<String>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub license_info: String,
}

/// Layer version attached to a function (`$LATEST` or a published version).
/// Keeps its own content hash so deleting the layer version from the catalog
/// does not break functions that already attached it.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "PascalCase")]
pub(crate) struct AttachedLayer {
    pub arn: String,
    pub layer_name: String,
    pub version: u64,
    pub code_sha256: String,
    pub code_size: i64,
}

impl AttachedLayer {
    pub(crate) fn summary_json(&self) -> Value {
        json!({
            "Arn": self.arn,
            "CodeSize": self.code_size,
        })
    }
}

pub(crate) fn layer_arn(region: &str, account_id: &str, name: &str) -> String {
    format!("arn:aws:lambda:{region}:{account_id}:layer:{name}")
}

pub(crate) fn layer_version_arn(
    region: &str,
    account_id: &str,
    name: &str,
    version: u64,
) -> String {
    format!("arn:aws:lambda:{region}:{account_id}:layer:{name}:{version}")
}

fn validation(message: &str) -> Reply {
    Reply::error(400, "ValidationException", message)
}

fn invalid_param(message: &str) -> Reply {
    Reply::error(400, "InvalidParameterValueException", message)
}

fn valid_layer_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 140
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
}

/// Extracts and validates the plain layer name from either a bare name or a
/// layer ARN (`arn:aws*:lambda:<region>:<account>:layer:<name>`).
pub(crate) fn parse_layer_name(raw: &str, region: &str, account_id: &str) -> Result<String, Reply> {
    if raw.is_empty() {
        return Err(validation(
            "1 validation error detected: Value null at 'layerName' failed to satisfy constraint: Member must not be null",
        ));
    }
    let name = if raw.starts_with("arn:") {
        let parts: Vec<&str> = raw.split(':').collect();
        if parts.len() != 7
            || parts[0] != "arn"
            || !parts[1].starts_with("aws")
            || parts[2] != "lambda"
            || parts[5] != "layer"
            || parts[6].is_empty()
        {
            return Err(validation(&format!(
                "1 validation error detected: Value '{raw}' at 'layerName' failed to satisfy constraint: Member must satisfy regular expression pattern: (arn:[a-zA-Z0-9-]+:lambda:[a-zA-Z0-9-]+:\\d{{12}}:layer:[a-zA-Z0-9-_]+)|[a-zA-Z0-9-_]+"
            )));
        }
        if parts[3] != region || parts[4] != account_id {
            return Err(invalid_param(&format!(
                "LayerName {raw} does not belong to this account and region ({region}:{account_id})"
            )));
        }
        parts[6]
    } else {
        raw
    };
    if !valid_layer_name(name) {
        return Err(validation(&format!(
            "1 validation error detected: Value '{raw}' at 'layerName' failed to satisfy constraint: Member must satisfy regular expression pattern: [a-zA-Z0-9-_]{{1,140}}"
        )));
    }
    Ok(name.to_string())
}

/// Parses a full layer version ARN for this account and region into
/// `(layer_name, version)`.
pub(crate) fn parse_layer_version_arn(
    arn: &str,
    region: &str,
    account_id: &str,
) -> Result<(String, u64), Reply> {
    let parts: Vec<&str> = arn.split(':').collect();
    if parts.len() != 8
        || parts[0] != "arn"
        || !parts[1].starts_with("aws")
        || parts[2] != "lambda"
        || parts[5] != "layer"
        || !valid_layer_name(parts[6])
    {
        return Err(invalid_param(&format!("Invalid layer version ARN: {arn}")));
    }
    let version = parts[7]
        .parse::<u64>()
        .ok()
        .filter(|&v| v >= 1)
        .ok_or_else(|| invalid_param(&format!("Invalid layer version number in ARN: {arn}")))?;
    if parts[3] != region || parts[4] != account_id {
        return Err(invalid_param(&format!(
            "Layer version ARN {arn} does not belong to this account and region ({region}:{account_id})"
        )));
    }
    Ok((parts[6].to_string(), version))
}

pub(crate) fn parse_compatible_runtimes(
    req: &Value,
    validate_rt: impl Fn(&str) -> Result<(), Reply>,
) -> Result<Vec<String>, Reply> {
    let Some(val) = req.get("CompatibleRuntimes").filter(|v| !v.is_null()) else {
        return Ok(Vec::new());
    };
    let Some(arr) = val.as_array() else {
        return Err(validation("CompatibleRuntimes must be a list of strings"));
    };
    if arr.len() > MAX_COMPATIBLE_RUNTIMES {
        return Err(validation(&format!(
            "CompatibleRuntimes cannot contain more than {MAX_COMPATIBLE_RUNTIMES} items"
        )));
    }
    let mut out = Vec::with_capacity(arr.len());
    for item in arr {
        let Some(rt) = item.as_str() else {
            return Err(validation("CompatibleRuntimes must be a list of strings"));
        };
        validate_rt(rt)?;
        if !out.iter().any(|existing| existing == rt) {
            out.push(rt.to_string());
        }
    }
    Ok(out)
}

pub(crate) fn parse_compatible_architectures(req: &Value) -> Result<Vec<String>, Reply> {
    let Some(val) = req.get("CompatibleArchitectures").filter(|v| !v.is_null()) else {
        return Ok(Vec::new());
    };
    let Some(arr) = val.as_array() else {
        return Err(validation(
            "CompatibleArchitectures must be a list of strings",
        ));
    };
    if arr.len() > MAX_COMPATIBLE_ARCHITECTURES {
        return Err(validation(&format!(
            "CompatibleArchitectures cannot contain more than {MAX_COMPATIBLE_ARCHITECTURES} items"
        )));
    }
    let mut out = Vec::with_capacity(arr.len());
    for item in arr {
        match item.as_str() {
            Some(arch @ ("x86_64" | "arm64")) => {
                if !out.iter().any(|existing| existing == arch) {
                    out.push(arch.to_string());
                }
            }
            _ => {
                return Err(validation(
                    "CompatibleArchitectures entries must be x86_64 or arm64",
                ));
            }
        }
    }
    Ok(out)
}

pub(crate) fn parse_license_info(req: &Value) -> Result<String, Reply> {
    match req.get("LicenseInfo") {
        None | Some(Value::Null) => Ok(String::new()),
        Some(Value::String(s)) if s.len() <= MAX_LICENSE_INFO_LEN => Ok(s.clone()),
        Some(Value::String(_)) => Err(validation(&format!(
            "LicenseInfo must be at most {MAX_LICENSE_INFO_LEN} characters"
        ))),
        Some(_) => Err(validation("LicenseInfo must be a string")),
    }
}

/// Parses `Layers` on CreateFunction / UpdateFunctionConfiguration.
/// Returns `Ok(None)` when absent (leave unchanged on update).
pub(crate) fn parse_layer_arns(req: &Value) -> Result<Option<Vec<String>>, Reply> {
    let Some(val) = req.get("Layers") else {
        return Ok(None);
    };
    if val.is_null() {
        return Ok(Some(Vec::new()));
    }
    let Some(arr) = val.as_array() else {
        return Err(validation("Layers must be a list of layer version ARNs"));
    };
    if arr.len() > MAX_LAYERS_PER_FUNCTION {
        return Err(invalid_param(&format!(
            "Functions can have at most {MAX_LAYERS_PER_FUNCTION} layers"
        )));
    }
    let mut out = Vec::with_capacity(arr.len());
    for item in arr {
        let Some(arn) = item.as_str() else {
            return Err(validation("Layers must be a list of layer version ARNs"));
        };
        out.push(arn.to_string());
    }
    Ok(Some(out))
}

pub(crate) fn version_list_item_json(
    region: &str,
    account_id: &str,
    name: &str,
    rec: &LayerVersionRecord,
) -> Value {
    let mut v = json!({
        "LayerVersionArn": layer_version_arn(region, account_id, name, rec.version),
        "Version": rec.version,
        "Description": rec.description,
        "CreatedDate": rec.created_date,
    });
    if !rec.compatible_runtimes.is_empty() {
        v["CompatibleRuntimes"] = json!(rec.compatible_runtimes);
    }
    if !rec.compatible_architectures.is_empty() {
        v["CompatibleArchitectures"] = json!(rec.compatible_architectures);
    }
    if !rec.license_info.is_empty() {
        v["LicenseInfo"] = json!(rec.license_info);
    }
    v
}

pub(crate) fn version_detail_json(
    region: &str,
    account_id: &str,
    base_endpoint: &str,
    name: &str,
    rec: &LayerVersionRecord,
    encoded_sha: &str,
) -> Value {
    let mut v = version_list_item_json(region, account_id, name, rec);
    v["LayerArn"] = json!(layer_arn(region, account_id, name));
    v["Content"] = json!({
        "Location": format!(
            "{base_endpoint}/_devcloud/layers/{name}/versions/{}/code.zip?CodeSha256={encoded_sha}",
            rec.version
        ),
        "CodeSha256": rec.code_sha256,
        "CodeSize": rec.code_size,
    });
    v
}

pub(crate) fn matches_filters(
    rec: &LayerVersionRecord,
    runtime: Option<&str>,
    architecture: Option<&str>,
) -> bool {
    if let Some(rt) = runtime.filter(|s| !s.is_empty()) {
        if !rec.compatible_runtimes.iter().any(|r| r == rt) {
            return false;
        }
    }
    if let Some(arch) = architecture.filter(|s| !s.is_empty()) {
        if !rec.compatible_architectures.iter().any(|a| a == arch) {
            return false;
        }
    }
    true
}

/// Merges `layer_dirs` into `dst` in order, so later layers overwrite earlier
/// files at the same path (matching AWS Lambda's sequential extraction into `/opt`).
pub(crate) fn materialize_layers(layer_dirs: &[PathBuf], dst: &Path) -> Result<(), String> {
    std::fs::create_dir_all(dst).map_err(|e| format!("create layer opt dir: {e}"))?;
    for src in layer_dirs {
        copy_tree_overlay(src, dst)?;
    }
    Ok(())
}

fn copy_tree_overlay(src: &Path, dst: &Path) -> Result<(), String> {
    let entries = std::fs::read_dir(src)
        .map_err(|e| format!("read layer directory {}: {e}", src.display()))?;
    for entry in entries {
        let entry = entry.map_err(|e| format!("read layer entry: {e}"))?;
        let src_path = entry.path();
        let dst_path = dst.join(entry.file_name());
        let meta = std::fs::symlink_metadata(&src_path)
            .map_err(|e| format!("stat {}: {e}", src_path.display()))?;
        let ft = meta.file_type();
        if ft.is_symlink() {
            remove_existing_entry(&dst_path)?;
            #[cfg(unix)]
            {
                let target = std::fs::read_link(&src_path)
                    .map_err(|e| format!("readlink {}: {e}", src_path.display()))?;
                std::os::unix::fs::symlink(&target, &dst_path)
                    .map_err(|e| format!("symlink {}: {e}", dst_path.display()))?;
            }
            #[cfg(not(unix))]
            {
                std::fs::copy(&src_path, &dst_path)
                    .map_err(|e| format!("copy {}: {e}", dst_path.display()))?;
            }
        } else if ft.is_dir() {
            if let Ok(existing) = std::fs::symlink_metadata(&dst_path) {
                if !existing.file_type().is_dir() {
                    remove_existing_entry(&dst_path)?;
                }
            }
            std::fs::create_dir_all(&dst_path)
                .map_err(|e| format!("mkdir {}: {e}", dst_path.display()))?;
            copy_tree_overlay(&src_path, &dst_path)?;
        } else {
            remove_existing_entry(&dst_path)?;
            std::fs::copy(&src_path, &dst_path).map_err(|e| {
                format!("copy {} -> {}: {e}", src_path.display(), dst_path.display())
            })?;
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                let mode = meta.permissions().mode();
                let _ = std::fs::set_permissions(&dst_path, std::fs::Permissions::from_mode(mode));
            }
        }
    }
    Ok(())
}

fn remove_existing_entry(path: &Path) -> Result<(), String> {
    match std::fs::symlink_metadata(path) {
        Ok(meta) if meta.file_type().is_dir() => {
            std::fs::remove_dir_all(path).map_err(|e| format!("remove {}: {e}", path.display()))
        }
        Ok(_) => std::fs::remove_file(path).map_err(|e| format!("remove {}: {e}", path.display())),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(format!("stat {}: {e}", path.display())),
    }
}

fn parse_version_number(raw: &str) -> Result<u64, Reply> {
    raw.parse::<u64>()
        .ok()
        .filter(|&v| v >= 1)
        .ok_or_else(|| validation(&format!("Invalid layer version number: {raw}")))
}

impl Server {
    fn layer_not_found(&self, name: &str, version: u64) -> Reply {
        Reply::error(
            404,
            "ResourceNotFoundException",
            &format!(
                "Layer version {} does not exist",
                layer_version_arn(self.region(), self.account_id(), name, version)
            ),
        )
    }

    /// `POST /2018-10-31/layers/<name>/versions` (`PublishLayerVersion`).
    pub fn publish_layer_version(&self, raw_name: &str, body: &[u8]) -> Reply {
        let req = match parse_json(body) {
            Ok(v) => v,
            Err(r) => return r,
        };
        let name = match parse_layer_name(raw_name, self.region(), self.account_id()) {
            Ok(n) => n,
            Err(r) => return r,
        };
        let compatible_runtimes = match parse_compatible_runtimes(&req, validate_runtime) {
            Ok(v) => v,
            Err(r) => return r,
        };
        let compatible_architectures = match parse_compatible_architectures(&req) {
            Ok(v) => v,
            Err(r) => return r,
        };
        let license_info = match parse_license_info(&req) {
            Ok(v) => v,
            Err(r) => return r,
        };
        let description = str_field(&req, "Description");
        let zip = match self.package_bytes(req.get("Content")) {
            Ok(z) => z,
            Err(r) => return r,
        };
        let sha = code_sha256(&zip);

        let lock = self.layer_lock(&name);
        let _serialized = lock.lock().unwrap();
        if let Err(r) = self.stage_layer_package(&name, &zip, &sha) {
            self.retire_orphaned_layers(&[(name.clone(), sha)]);
            return r;
        }
        let result = self.commit(|st| {
            let entry = st
                .layers
                .entry(name.clone())
                .or_insert_with(|| LayerRecord {
                    layer_name: name.clone(),
                    next_version: 1,
                    versions: BTreeMap::new(),
                });
            let ver_num = entry.next_version;
            entry.next_version += 1;
            let rec = LayerVersionRecord {
                version: ver_num,
                description: description.clone(),
                created_date: now_lambda(),
                code_sha256: sha.clone(),
                code_size: zip.len() as i64,
                compatible_runtimes: compatible_runtimes.clone(),
                compatible_architectures: compatible_architectures.clone(),
                license_info: license_info.clone(),
            };
            entry.versions.insert(ver_num, rec.clone());
            Ok(rec)
        });
        match result {
            Ok(rec) => Reply::json(
                201,
                &version_detail_json(
                    self.region(),
                    self.account_id(),
                    &self.base_endpoint(),
                    &name,
                    &rec,
                    &query_encode(&rec.code_sha256),
                ),
            ),
            Err(r) => {
                self.retire_orphaned_layers(&[(name, sha)]);
                r
            }
        }
    }

    /// `GET /2018-10-31/layers/<name>/versions/<version>` (`GetLayerVersion`).
    pub fn get_layer_version(&self, raw_name: &str, raw_version: &str) -> Reply {
        let name = match parse_layer_name(raw_name, self.region(), self.account_id()) {
            Ok(n) => n,
            Err(r) => return r,
        };
        let version = match parse_version_number(raw_version) {
            Ok(v) => v,
            Err(r) => return r,
        };
        let st = self.state.lock().unwrap();
        let Some(rec) = st.layers.get(&name).and_then(|l| l.versions.get(&version)) else {
            return self.layer_not_found(&name, version);
        };
        Reply::json(
            200,
            &version_detail_json(
                self.region(),
                self.account_id(),
                &self.base_endpoint(),
                &name,
                rec,
                &query_encode(&rec.code_sha256),
            ),
        )
    }

    /// `GET /2018-10-31/layers?find=LayerVersion&Arn=...` (`GetLayerVersionByArn`).
    pub fn get_layer_version_by_arn(&self, query: &BTreeMap<String, String>) -> Reply {
        let arn = query.get("Arn").map(String::as_str).unwrap_or("");
        if arn.is_empty() {
            return validation("Arn query parameter is required");
        }
        let (name, version) = match parse_layer_version_arn(arn, self.region(), self.account_id()) {
            Ok(parsed) => parsed,
            Err(r) => return r,
        };
        let st = self.state.lock().unwrap();
        let Some(rec) = st.layers.get(&name).and_then(|l| l.versions.get(&version)) else {
            return self.layer_not_found(&name, version);
        };
        Reply::json(
            200,
            &version_detail_json(
                self.region(),
                self.account_id(),
                &self.base_endpoint(),
                &name,
                rec,
                &query_encode(&rec.code_sha256),
            ),
        )
    }

    /// `GET /2018-10-31/layers` (`ListLayers`).
    pub fn list_layers(&self, query: &BTreeMap<String, String>) -> Reply {
        let (start, max_items) = match parse_pagination(query) {
            Ok(p) => p,
            Err(r) => return r,
        };
        let rt_filter = query.get("CompatibleRuntime").map(String::as_str);
        let arch_filter = query.get("CompatibleArchitecture").map(String::as_str);
        let st = self.state.lock().unwrap();
        let matching: Vec<Value> = st
            .layers
            .values()
            .filter_map(|layer| {
                let latest = layer
                    .versions
                    .values()
                    .rev()
                    .find(|v| matches_filters(v, rt_filter, arch_filter))?;
                Some(json!({
                    "LayerName": layer.layer_name,
                    "LayerArn": layer_arn(self.region(), self.account_id(), &layer.layer_name),
                    "LatestMatchingVersion": version_list_item_json(
                        self.region(),
                        self.account_id(),
                        &layer.layer_name,
                        latest,
                    ),
                }))
            })
            .collect();
        let page: Vec<Value> = matching
            .iter()
            .skip(start)
            .take(max_items)
            .cloned()
            .collect();
        let mut out = json!({ "Layers": page });
        let next = start.saturating_add(max_items);
        if next < matching.len() {
            out["NextMarker"] = json!(next.to_string());
        }
        Reply::json(200, &out)
    }

    /// `GET /2018-10-31/layers/<name>/versions` (`ListLayerVersions`).
    pub fn list_layer_versions(&self, raw_name: &str, query: &BTreeMap<String, String>) -> Reply {
        let (start, max_items) = match parse_pagination(query) {
            Ok(p) => p,
            Err(r) => return r,
        };
        let name = match parse_layer_name(raw_name, self.region(), self.account_id()) {
            Ok(n) => n,
            Err(r) => return r,
        };
        let rt_filter = query.get("CompatibleRuntime").map(String::as_str);
        let arch_filter = query.get("CompatibleArchitecture").map(String::as_str);
        let st = self.state.lock().unwrap();
        let matching: Vec<Value> = st
            .layers
            .get(&name)
            .map(|layer| {
                layer
                    .versions
                    .values()
                    .rev()
                    .filter(|v| matches_filters(v, rt_filter, arch_filter))
                    .map(|v| version_list_item_json(self.region(), self.account_id(), &name, v))
                    .collect()
            })
            .unwrap_or_default();
        let page: Vec<Value> = matching
            .iter()
            .skip(start)
            .take(max_items)
            .cloned()
            .collect();
        let mut out = json!({ "LayerVersions": page });
        let next = start.saturating_add(max_items);
        if next < matching.len() {
            out["NextMarker"] = json!(next.to_string());
        }
        Reply::json(200, &out)
    }

    /// `DELETE /2018-10-31/layers/<name>/versions/<version>` (`DeleteLayerVersion`).
    pub fn delete_layer_version(&self, raw_name: &str, raw_version: &str) -> Reply {
        let name = match parse_layer_name(raw_name, self.region(), self.account_id()) {
            Ok(n) => n,
            Err(r) => return r,
        };
        let version = match parse_version_number(raw_version) {
            Ok(v) => v,
            Err(r) => return r,
        };
        let lock = self.layer_lock(&name);
        let _serialized = lock.lock().unwrap();
        let result = self.commit(|st| {
            let layer = st
                .layers
                .get_mut(&name)
                .ok_or_else(|| self.layer_not_found(&name, version))?;
            let removed = layer
                .versions
                .remove(&version)
                .ok_or_else(|| self.layer_not_found(&name, version))?;
            Ok(removed.code_sha256)
        });
        match result {
            Ok(sha) => {
                // Only retire on-disk files if no function/version still attaches it.
                self.retire_orphaned_layers(&[(name, sha)]);
                Reply::empty(204)
            }
            Err(r) => r,
        }
    }

    /// `GET /_devcloud/layers/<name>/versions/<version>/code.zip` — `Content.Location`.
    pub fn layer_code_package(
        &self,
        raw_name: &str,
        raw_version: &str,
        expected_sha: Option<&str>,
    ) -> Reply {
        let name = match parse_layer_name(raw_name, self.region(), self.account_id()) {
            Ok(n) => n,
            Err(r) => return r,
        };
        let version = match parse_version_number(raw_version) {
            Ok(v) => v,
            Err(r) => return r,
        };
        let lock = self.layer_lock(&name);
        let _serialized = lock.lock().unwrap();
        let sha = {
            let st = self.state.lock().unwrap();
            let Some(rec) = st.layers.get(&name).and_then(|l| l.versions.get(&version)) else {
                return self.layer_not_found(&name, version);
            };
            if expected_sha.is_some_and(|want| want != rec.code_sha256) {
                return Reply::error(
                    410,
                    "ResourceNotFoundException",
                    "This layer code location has expired.",
                );
            }
            rec.code_sha256.clone()
        };
        match std::fs::read(self.layer_code.zip_path(&name, &sha)) {
            Ok(data) => Reply {
                status: 200,
                headers: Vec::new(),
                body: data,
                content_type: "application/zip",
            },
            Err(_) => Reply::error(500, "ServiceException", "layer package is missing"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_layer_names_and_version_arns() {
        assert_eq!(
            parse_layer_name("my-layer_1", "us-east-1", "000000000000").unwrap(),
            "my-layer_1"
        );
        assert_eq!(
            parse_layer_name(
                "arn:aws:lambda:us-east-1:000000000000:layer:my-layer",
                "us-east-1",
                "000000000000"
            )
            .unwrap(),
            "my-layer"
        );
        assert!(parse_layer_name("bad name!", "us-east-1", "000000000000").is_err());
        assert!(parse_layer_name(
            "arn:aws:lambda:eu-west-1:000000000000:layer:my-layer",
            "us-east-1",
            "000000000000"
        )
        .is_err());

        assert_eq!(
            parse_layer_version_arn(
                "arn:aws:lambda:us-east-1:000000000000:layer:utils:3",
                "us-east-1",
                "000000000000"
            )
            .unwrap(),
            ("utils".to_string(), 3)
        );
        assert!(parse_layer_version_arn(
            "arn:aws:lambda:us-east-1:000000000000:layer:utils:0",
            "us-east-1",
            "000000000000"
        )
        .is_err());
        assert!(parse_layer_version_arn(
            "arn:aws:lambda:us-east-1:000000000000:function:utils:1",
            "us-east-1",
            "000000000000"
        )
        .is_err());
    }

    #[test]
    fn materialize_layers_overlays_in_order() {
        let root = std::env::temp_dir().join(format!(
            "devcloud-lambda-layers-unit-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&root);
        let l1 = root.join("l1");
        let l2 = root.join("l2");
        let dst = root.join("opt");
        std::fs::create_dir_all(l1.join("python")).unwrap();
        std::fs::create_dir_all(l2.join("python")).unwrap();
        std::fs::write(l1.join("python/a.py"), b"from_l1").unwrap();
        std::fs::write(l1.join("python/shared.py"), b"v1").unwrap();
        std::fs::write(l2.join("python/shared.py"), b"v2").unwrap();
        std::fs::write(l2.join("python/b.py"), b"from_l2").unwrap();

        materialize_layers(&[l1, l2], &dst).unwrap();
        assert_eq!(std::fs::read(dst.join("python/a.py")).unwrap(), b"from_l1");
        assert_eq!(std::fs::read(dst.join("python/b.py")).unwrap(), b"from_l2");
        assert_eq!(std::fs::read(dst.join("python/shared.py")).unwrap(), b"v2");
        let _ = std::fs::remove_dir_all(&root);
    }
}
