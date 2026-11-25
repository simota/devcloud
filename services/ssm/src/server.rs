//! SSM Parameter Store over the AWS JSON 1.1 protocol.
//!
//! Supported operations: `PutParameter`, `GetParameter`, `GetParameters`,
//! `GetParametersByPath`, `GetParameterHistory`, `DeleteParameter`,
//! `DeleteParameters`, `DescribeParameters` (metadata only, used by the
//! dashboard), `LabelParameterVersion`, `UnlabelParameterVersion`,
//! `AddTagsToResource`, `RemoveTagsFromResource`, and `ListTagsForResource`.
//! State is one map behind a `Mutex`, persisted atomically to `state.json`.
//!
//! `SecureString` values are stored as plaintext in `state.json` — this is a
//! local emulator, there is no KMS. Read operations without `WithDecryption`
//! return an opaque placeholder instead of the value, so a caller that forgets
//! `WithDecryption=true` fails here the same way it would against AWS.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

const MAX_NAME_CHARS: usize = 2048;
const MAX_STANDARD_VALUE_BYTES: usize = 4096;
const MAX_ADVANCED_VALUE_BYTES: usize = 8192;
const DEFAULT_DESCRIBE_PAGE: usize = 50;
const MAX_DESCRIBE_PAGE: usize = 50;
const DEFAULT_BY_PATH_PAGE: usize = 10;
const MAX_BY_PATH_PAGE: usize = 10;
const DEFAULT_HISTORY_PAGE: usize = 50;
const MAX_HISTORY_PAGE: usize = 50;
const MAX_BATCH_NAMES: usize = 10;
const MAX_LABELS_PER_VERSION: usize = 10;
const MAX_LABEL_CHARS: usize = 100;
const MAX_HISTORY_VERSIONS: usize = 100;
const MAX_KEY_ID_CHARS: usize = 256;
const MAX_TAG_KEY_CHARS: usize = 128;
const MAX_TAG_VALUE_CHARS: usize = 256;

#[derive(Clone, Debug, Default)]
pub struct Config {
    pub addr: String,
    pub region: String,
    pub account_id: String,
    pub auth_mode: String,
    pub access_key_id: String,
    pub secret_access_key: String,
    pub storage_path: String,
}

/// One operation result: HTTP status, optional `X-Amzn-Errortype`, JSON body.
#[derive(Debug)]
pub struct Outcome {
    pub status: u16,
    pub error_type: Option<String>,
    pub body: Value,
}

impl Outcome {
    fn ok(body: Value) -> Self {
        Outcome {
            status: 200,
            error_type: None,
            body,
        }
    }
}

pub fn error_outcome(status: u16, name: &str, message: &str) -> Outcome {
    Outcome {
        status,
        error_type: Some(name.to_string()),
        body: json!({ "__type": name, "message": message }),
    }
}

fn validation(message: &str) -> Outcome {
    error_outcome(400, "ValidationException", message)
}

fn not_found(name: &str) -> Outcome {
    error_outcome(
        400,
        "ParameterNotFound",
        &format!("Parameter {name} not found."),
    )
}

fn invalid_resource_id() -> Outcome {
    error_outcome(400, "InvalidResourceId", "Invalid resource id.")
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
struct Tag {
    #[serde(rename = "Key")]
    key: String,
    #[serde(rename = "Value")]
    value: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
struct ParameterInlinePolicy {
    #[serde(rename = "PolicyText")]
    policy_text: String,
    #[serde(rename = "PolicyType")]
    policy_type: String,
    #[serde(rename = "PolicyStatus")]
    policy_status: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
struct ParameterVersion {
    #[serde(rename = "Type")]
    kind: String,
    #[serde(rename = "Value")]
    value: String,
    #[serde(rename = "Version")]
    version: u64,
    #[serde(rename = "LastModifiedDate")]
    last_modified: f64,
    #[serde(
        rename = "Description",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    description: String,
    #[serde(rename = "KeyId", default, skip_serializing_if = "String::is_empty")]
    key_id: String,
    #[serde(rename = "Tier")]
    tier: String,
    #[serde(rename = "DataType")]
    data_type: String,
    #[serde(rename = "Labels", default, skip_serializing_if = "Vec::is_empty")]
    labels: Vec<String>,
    #[serde(rename = "Policies", default, skip_serializing_if = "Vec::is_empty")]
    policies: Vec<ParameterInlinePolicy>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
struct Parameter {
    #[serde(rename = "Type")]
    kind: String,
    #[serde(rename = "Value")]
    value: String,
    #[serde(rename = "Version")]
    version: u64,
    #[serde(rename = "LastModifiedDate")]
    last_modified: f64,
    #[serde(
        rename = "Description",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    description: String,
    #[serde(rename = "KeyId", default, skip_serializing_if = "String::is_empty")]
    key_id: String,
    #[serde(rename = "Tier")]
    tier: String,
    #[serde(rename = "DataType")]
    data_type: String,
    #[serde(rename = "Policies", default, skip_serializing_if = "Vec::is_empty")]
    policies: Vec<ParameterInlinePolicy>,
    #[serde(rename = "Tags", default, skip_serializing_if = "Vec::is_empty")]
    tags: Vec<Tag>,
    #[serde(rename = "History", default, skip_serializing_if = "Vec::is_empty")]
    history: Vec<ParameterVersion>,
}

impl Parameter {
    fn ensure_history(&mut self) {
        if self.history.is_empty() {
            self.history.push(ParameterVersion {
                kind: self.kind.clone(),
                value: self.value.clone(),
                version: self.version,
                last_modified: self.last_modified,
                description: self.description.clone(),
                key_id: self.key_id.clone(),
                tier: self.tier.clone(),
                data_type: self.data_type.clone(),
                labels: Vec::new(),
                policies: self.policies.clone(),
            });
        }
    }

    fn current_version(&self) -> ParameterVersion {
        self.history
            .last()
            .cloned()
            .unwrap_or_else(|| ParameterVersion {
                kind: self.kind.clone(),
                value: self.value.clone(),
                version: self.version,
                last_modified: self.last_modified,
                description: self.description.clone(),
                key_id: self.key_id.clone(),
                tier: self.tier.clone(),
                data_type: self.data_type.clone(),
                labels: Vec::new(),
                policies: self.policies.clone(),
            })
    }

    fn find_version(&self, version: u64) -> Option<ParameterVersion> {
        if self.history.is_empty() {
            if self.version == version {
                return Some(self.current_version());
            }
            return None;
        }
        self.history.iter().find(|v| v.version == version).cloned()
    }

    fn find_label(&self, label: &str) -> Option<ParameterVersion> {
        self.history
            .iter()
            .find(|v| v.labels.iter().any(|l| l == label))
            .cloned()
    }
}

#[derive(Default, Serialize, Deserialize)]
struct PersistedState {
    #[serde(rename = "Parameters", default)]
    parameters: BTreeMap<String, Parameter>,
}

pub struct Server {
    config: Config,
    state: Mutex<BTreeMap<String, Parameter>>,
    load_err: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct PutParameterRequest {
    #[serde(default)]
    name: String,
    value: Option<String>,
    #[serde(rename = "Type", default)]
    kind: String,
    #[serde(default)]
    overwrite: bool,
    #[serde(default)]
    description: String,
    #[serde(default)]
    key_id: String,
    #[serde(default)]
    tier: String,
    #[serde(default)]
    data_type: String,
    #[serde(default)]
    tags: Option<Vec<Tag>>,
    #[serde(default)]
    policies: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct GetParameterRequest {
    #[serde(default)]
    name: String,
    #[serde(default)]
    with_decryption: bool,
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct GetParametersRequest {
    #[serde(default)]
    names: Vec<String>,
    #[serde(default)]
    with_decryption: bool,
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct ParameterStringFilter {
    #[serde(default)]
    key: String,
    #[serde(default)]
    option: String,
    #[serde(default)]
    values: Vec<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct GetParametersByPathRequest {
    #[serde(default)]
    path: String,
    #[serde(default)]
    recursive: bool,
    #[serde(default)]
    parameter_filters: Vec<ParameterStringFilter>,
    #[serde(default)]
    with_decryption: bool,
    max_results: Option<i64>,
    #[serde(default)]
    next_token: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct GetParameterHistoryRequest {
    #[serde(default)]
    name: String,
    #[serde(default)]
    with_decryption: bool,
    max_results: Option<i64>,
    #[serde(default)]
    next_token: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct DeleteParameterRequest {
    #[serde(default)]
    name: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct DeleteParametersRequest {
    #[serde(default)]
    names: Vec<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct DescribeParametersRequest {
    max_results: Option<i64>,
    #[serde(default)]
    next_token: String,
    #[serde(default)]
    filters: Vec<Value>,
    #[serde(default)]
    parameter_filters: Vec<Value>,
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct LabelParameterVersionRequest {
    #[serde(default)]
    name: String,
    parameter_version: Option<u64>,
    #[serde(default)]
    labels: Vec<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct UnlabelParameterVersionRequest {
    #[serde(default)]
    name: String,
    parameter_version: Option<u64>,
    #[serde(default)]
    labels: Vec<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct AddTagsToResourceRequest {
    #[serde(default)]
    resource_type: String,
    #[serde(default)]
    resource_id: String,
    #[serde(default)]
    tags: Vec<Tag>,
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct RemoveTagsFromResourceRequest {
    #[serde(default)]
    resource_type: String,
    #[serde(default)]
    resource_id: String,
    #[serde(default)]
    tag_keys: Vec<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct ListTagsForResourceRequest {
    #[serde(default)]
    resource_type: String,
    #[serde(default)]
    resource_id: String,
}

#[derive(Clone, Debug, PartialEq)]
enum Selector {
    Version(u64),
    Label(String),
}

fn decode<T: for<'de> Deserialize<'de>>(body: &[u8]) -> Result<T, Outcome> {
    let body = if body.is_empty() {
        b"{}" as &[u8]
    } else {
        body
    };
    serde_json::from_slice(body)
        .map_err(|_| error_outcome(400, "SerializationException", "invalid request body"))
}

fn now_epoch_seconds() -> f64 {
    let d = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    // Millisecond precision, as the AWS wire form carries.
    (d.as_millis() as f64) / 1000.0
}

/// AWS name rules this emulator enforces: non-empty, bounded length, a
/// hierarchical name starts with `/`, no reserved `aws`/`ssm` prefix, and only
/// the documented character set (`a-zA-Z0-9_.-/`).
fn validate_name(name: &str) -> Result<(), Outcome> {
    if name.is_empty() {
        return Err(validation("Parameter name must not be empty."));
    }
    if name.chars().count() > MAX_NAME_CHARS {
        return Err(validation(
            "Parameter name exceeds the maximum length of 2048 characters.",
        ));
    }
    if name.contains('/') && !name.starts_with('/') {
        return Err(validation(
            "Parameter name: hierarchical names must begin with a forward slash (/).",
        ));
    }
    let first_segment = name.trim_start_matches('/').to_ascii_lowercase();
    if first_segment.starts_with("aws") || first_segment.starts_with("ssm") {
        return Err(validation(
            "Parameter name: can't be prefixed with \"aws\" or \"ssm\" (case-insensitive).",
        ));
    }
    if !name
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'.' | b'-' | b'/'))
    {
        return Err(validation(
            "Parameter name: only a mix of letters, numbers and the following 4 symbols .-_/ are allowed.",
        ));
    }
    Ok(())
}

/// Validates a label according to AWS SSM parameter label rules:
/// 1..=100 chars, allowed characters `a-zA-Z0-9_.-`, cannot start with a digit
/// or with `aws`/`ssm` (case-insensitive).
fn is_valid_label(label: &str) -> bool {
    if label.is_empty() || label.chars().count() > MAX_LABEL_CHARS {
        return false;
    }
    if label.as_bytes()[0].is_ascii_digit() {
        return false;
    }
    let lower = label.to_ascii_lowercase();
    if lower.starts_with("aws") || lower.starts_with("ssm") {
        return false;
    }
    label
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'.' | b'-'))
}

/// Splits an optional `:<version>` or `:<label>` selector off a parameter name.
fn split_selector(name: &str) -> Result<(&str, Option<Selector>), Outcome> {
    let Some((base, selector)) = name.rsplit_once(':') else {
        return Ok((name, None));
    };
    if selector.is_empty() {
        return Err(validation("Parameter selector must not be empty."));
    }
    if selector.bytes().all(|b| b.is_ascii_digit()) {
        let v = selector
            .parse::<u64>()
            .map_err(|_| validation("Invalid parameter version selector."))?;
        if v == 0 {
            return Err(validation("Parameter version must be at least 1."));
        }
        return Ok((base, Some(Selector::Version(v))));
    }
    if !is_valid_label(selector) {
        return Err(validation("Invalid parameter label selector."));
    }
    Ok((base, Some(Selector::Label(selector.to_string()))))
}

fn validate_key_id(key_id: &str) -> Result<(), Outcome> {
    if key_id.chars().count() > MAX_KEY_ID_CHARS {
        return Err(validation(
            "KeyId exceeds the maximum length of 256 characters.",
        ));
    }
    if key_id.bytes().any(|b| b.is_ascii_control()) {
        return Err(validation("KeyId contains invalid characters."));
    }
    Ok(())
}

fn validate_tags(tags: &[Tag]) -> Result<(), Outcome> {
    let mut seen = std::collections::HashSet::new();
    for tag in tags {
        if tag.key.is_empty() || tag.key.chars().count() > MAX_TAG_KEY_CHARS {
            return Err(validation(
                "Tag key must be between 1 and 128 characters in length.",
            ));
        }
        if tag.key.to_ascii_lowercase().starts_with("aws:") {
            return Err(validation(
                "Tag key cannot start with reserved prefix aws:.",
            ));
        }
        if tag.value.chars().count() > MAX_TAG_VALUE_CHARS {
            return Err(validation(
                "Tag value must not exceed 256 characters in length.",
            ));
        }
        if !seen.insert(&tag.key) {
            return Err(validation("Duplicate tag keys are not allowed."));
        }
    }
    Ok(())
}

fn parse_policies(raw: &str) -> Result<Vec<ParameterInlinePolicy>, Outcome> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Ok(Vec::new());
    }
    let parsed: Value = serde_json::from_str(trimmed)
        .map_err(|_| validation("Policies must be a valid JSON array."))?;
    let Some(items) = parsed.as_array() else {
        return Err(validation("Policies must be a JSON array."));
    };
    let mut policies = Vec::with_capacity(items.len());
    for item in items {
        let Some(obj) = item.as_object() else {
            return Err(validation("Each policy in Policies must be a JSON object."));
        };
        let Some(policy_type) = obj.get("Type").and_then(Value::as_str) else {
            return Err(validation("Policy Type is required."));
        };
        if !matches!(
            policy_type,
            "Expiration" | "ExpirationNotification" | "NoChangeNotification"
        ) {
            return Err(validation(
                "Policy Type must be one of Expiration, ExpirationNotification, NoChangeNotification.",
            ));
        }
        let Some(version) = obj.get("Version").and_then(Value::as_str) else {
            return Err(validation("Policy Version is required."));
        };
        if version != "1.0" {
            return Err(validation("Policy Version must be \"1.0\"."));
        }
        let Some(attrs) = obj.get("Attributes").and_then(Value::as_object) else {
            return Err(validation("Policy Attributes must be a JSON object."));
        };
        match policy_type {
            "Expiration" => {
                let ts = attrs
                    .get("Timestamp")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                if ts.is_empty() {
                    return Err(validation(
                        "Expiration policy requires a non-empty Attributes.Timestamp.",
                    ));
                }
            }
            "ExpirationNotification" | "NoChangeNotification" => {
                let before = attrs
                    .get("Before")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                let unit = attrs
                    .get("Unit")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                if before.is_empty() || before.parse::<u64>().ok().filter(|&n| n > 0).is_none() {
                    return Err(validation(&format!(
                        "{policy_type} policy requires a positive numeric string Attributes.Before."
                    )));
                }
                if !matches!(unit, "Days" | "Hours") {
                    return Err(validation(&format!(
                        "{policy_type} policy Attributes.Unit must be Days or Hours."
                    )));
                }
            }
            _ => unreachable!(),
        }
        let policy_text = serde_json::to_string(item)
            .map_err(|_| validation("Failed to serialize policy text."))?;
        policies.push(ParameterInlinePolicy {
            policy_text,
            policy_type: policy_type.to_string(),
            policy_status: "Finished".to_string(),
        });
    }
    Ok(policies)
}

/// Stand-in for the KMS ciphertext AWS returns when `WithDecryption` is false:
/// opaque, stable per value, and never the plaintext.
fn encrypted_placeholder(value: &str) -> String {
    let digest = Sha256::digest(value.as_bytes());
    format!("devcloud-kms-encrypted:{}", hex::encode(digest))
}

impl Server {
    pub fn new(config: Config) -> Self {
        let mut server = Server {
            config,
            state: Mutex::new(BTreeMap::new()),
            load_err: None,
        };
        if !server.config.storage_path.is_empty() {
            if let Err(e) = server.load() {
                server.load_err = Some(e);
            }
        }
        server
    }

    pub fn config(&self) -> &Config {
        &self.config
    }

    pub fn load_err(&self) -> Option<&str> {
        self.load_err.as_deref()
    }

    fn state_path(&self) -> PathBuf {
        PathBuf::from(&self.config.storage_path).join("state.json")
    }

    fn load(&mut self) -> Result<(), String> {
        let data = match std::fs::read(self.state_path()) {
            Ok(d) => d,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(e) => return Err(e.to_string()),
        };
        let mut persisted: PersistedState =
            serde_json::from_slice(&data).map_err(|e| e.to_string())?;
        for param in persisted.parameters.values_mut() {
            param.ensure_history();
        }
        *self.state.get_mut().unwrap() = persisted.parameters;
        Ok(())
    }

    /// Atomic write via `state.json.tmp` + rename.
    fn persist(&self, parameters: &BTreeMap<String, Parameter>) -> Result<(), String> {
        if self.config.storage_path.is_empty() {
            return Ok(());
        }
        std::fs::create_dir_all(&self.config.storage_path).map_err(|e| e.to_string())?;
        let data = serde_json::to_vec(&PersistedState {
            parameters: parameters.clone(),
        })
        .map_err(|e| e.to_string())?;
        let path = self.state_path();
        let tmp = path.with_extension("json.tmp");
        std::fs::write(&tmp, &data).map_err(|e| e.to_string())?;
        std::fs::rename(&tmp, &path).map_err(|e| e.to_string())
    }

    fn region(&self) -> &str {
        if self.config.region.is_empty() {
            "us-east-1"
        } else {
            &self.config.region
        }
    }

    fn account_id(&self) -> &str {
        if self.config.account_id.is_empty() {
            "000000000000"
        } else {
            &self.config.account_id
        }
    }

    fn arn(&self, name: &str) -> String {
        let path = if name.starts_with('/') {
            name.to_string()
        } else {
            format!("/{name}")
        };
        format!(
            "arn:aws:ssm:{}:{}:parameter{path}",
            self.region(),
            self.account_id()
        )
    }

    fn render_parameter(
        &self,
        name: &str,
        pv: &ParameterVersion,
        with_decryption: bool,
        selector: Option<&Selector>,
    ) -> Value {
        let value = if pv.kind == "SecureString" && !with_decryption {
            encrypted_placeholder(&pv.value)
        } else {
            pv.value.clone()
        };
        let mut out = json!({
            "Name": name,
            "Type": pv.kind,
            "Value": value,
            "Version": pv.version,
            "LastModifiedDate": pv.last_modified,
            "ARN": self.arn(name),
            "DataType": pv.data_type,
        });
        match selector {
            Some(Selector::Version(v)) => {
                out["Selector"] = json!(format!(":{v}"));
            }
            Some(Selector::Label(label)) => {
                out["Selector"] = json!(format!(":{label}"));
            }
            None => {}
        }
        out
    }

    pub fn dispatch(&self, operation: &str, body: &[u8]) -> Outcome {
        match operation {
            "PutParameter" => self.put_parameter(body),
            "GetParameter" => self.get_parameter(body),
            "GetParameters" => self.get_parameters(body),
            "GetParametersByPath" => self.get_parameters_by_path(body),
            "GetParameterHistory" => self.get_parameter_history(body),
            "DeleteParameter" => self.delete_parameter(body),
            "DeleteParameters" => self.delete_parameters(body),
            "DescribeParameters" => self.describe_parameters(body),
            "LabelParameterVersion" => self.label_parameter_version(body),
            "UnlabelParameterVersion" => self.unlabel_parameter_version(body),
            "AddTagsToResource" => self.add_tags_to_resource(body),
            "RemoveTagsFromResource" => self.remove_tags_from_resource(body),
            "ListTagsForResource" => self.list_tags_for_resource(body),
            _ => error_outcome(400, "UnknownOperationException", "unknown operation"),
        }
    }

    fn put_parameter(&self, body: &[u8]) -> Outcome {
        let req: PutParameterRequest = match decode(body) {
            Ok(r) => r,
            Err(o) => return o,
        };
        if let Err(o) = validate_name(&req.name) {
            return o;
        }
        let Some(value) = req.value else {
            return validation("Value is required.");
        };
        if value.is_empty() {
            return validation("Parameter value must not be empty.");
        }
        if !req.kind.is_empty()
            && !matches!(req.kind.as_str(), "String" | "StringList" | "SecureString")
        {
            return validation("Type must be one of String, StringList, SecureString.");
        }
        if !req.key_id.is_empty() {
            if let Err(o) = validate_key_id(&req.key_id) {
                return o;
            }
        }
        if req.overwrite && req.tags.as_ref().is_some_and(|t| !t.is_empty()) {
            return validation(
                "You can't specify Tags and Overwrite in the same PutParameter request.",
            );
        }
        if let Some(tags) = &req.tags {
            if let Err(o) = validate_tags(tags) {
                return o;
            }
        }
        let parsed_policies = match &req.policies {
            Some(raw) => match parse_policies(raw) {
                Ok(p) => Some(p),
                Err(o) => return o,
            },
            None => None,
        };
        if parsed_policies.as_ref().is_some_and(|p| !p.is_empty()) && req.tier == "Standard" {
            return validation(
                "Parameter policies are only supported for Advanced tier parameters.",
            );
        }

        let mut parameters = self.state.lock().unwrap();
        let previous = parameters.get(&req.name).cloned();
        let kind = match (&previous, req.kind.as_str()) {
            (Some(_), _) if !req.overwrite => {
                return error_outcome(
                    400,
                    "ParameterAlreadyExists",
                    "The parameter already exists. To overwrite this value, set the overwrite option in the request to true.",
                );
            }
            (_, k) if !k.is_empty() => k.to_string(),
            (Some(p), _) => p.kind.clone(),
            (None, _) => return validation("Type is required when creating a parameter."),
        };
        if kind != "SecureString" && !req.key_id.is_empty() {
            return validation("KeyId is only supported for SecureString parameters.");
        }

        let policies = match parsed_policies {
            Some(p) => p,
            None => previous
                .as_ref()
                .map(|p| p.policies.clone())
                .unwrap_or_default(),
        };
        let tier = match req.tier.as_str() {
            "Advanced" => "Advanced",
            "Standard" => {
                if !policies.is_empty() {
                    return validation(
                        "Parameter policies are only supported for Advanced tier parameters.",
                    );
                }
                "Standard"
            }
            "Intelligent-Tiering" => {
                if !policies.is_empty()
                    || value.len() > MAX_STANDARD_VALUE_BYTES
                    || previous.as_ref().is_some_and(|p| p.tier == "Advanced")
                {
                    "Advanced"
                } else {
                    "Standard"
                }
            }
            "" => {
                if !policies.is_empty() || previous.as_ref().is_some_and(|p| p.tier == "Advanced") {
                    "Advanced"
                } else {
                    "Standard"
                }
            }
            _ => return validation("Tier must be one of Standard, Advanced, Intelligent-Tiering."),
        };
        let limit = if tier == "Advanced" {
            MAX_ADVANCED_VALUE_BYTES
        } else {
            MAX_STANDARD_VALUE_BYTES
        };
        if value.len() > limit {
            return validation(&format!(
                "Parameter value exceeds the maximum size of {limit} bytes for the {tier} tier."
            ));
        }
        let data_type = match req.data_type.as_str() {
            "" => previous
                .as_ref()
                .map(|p| p.data_type.as_str())
                .unwrap_or("text"),
            "text" => "text",
            "aws:ec2:image" | "aws:ssm:integration" => req.data_type.as_str(),
            _ => {
                return validation(
                    "DataType must be one of text, aws:ec2:image, aws:ssm:integration.",
                )
            }
        }
        .to_string();

        let version = previous.as_ref().map_or(1, |p| p.version + 1);
        let description = if req.description.is_empty() {
            previous
                .as_ref()
                .map(|p| p.description.clone())
                .unwrap_or_default()
        } else {
            req.description
        };
        let key_id = if kind == "SecureString" {
            if !req.key_id.is_empty() {
                req.key_id
            } else if let Some(prev) = &previous {
                if !prev.key_id.is_empty() {
                    prev.key_id.clone()
                } else {
                    "alias/aws/ssm".to_string()
                }
            } else {
                "alias/aws/ssm".to_string()
            }
        } else {
            String::new()
        };
        let tags = match req.tags {
            Some(t) => t,
            None => previous
                .as_ref()
                .map(|p| p.tags.clone())
                .unwrap_or_default(),
        };

        let last_modified = now_epoch_seconds();
        let mut history = previous
            .as_ref()
            .map(|p| {
                let mut cloned = p.clone();
                cloned.ensure_history();
                cloned.history
            })
            .unwrap_or_default();

        if history.len() >= MAX_HISTORY_VERSIONS {
            if let Some(idx) = history.iter().position(|v| v.labels.is_empty()) {
                history.remove(idx);
            } else {
                return error_outcome(
                    400,
                    "ParameterMaxVersionLimitExceeded",
                    "Parameter reached the maximum number of versions and all retained versions have labels attached.",
                );
            }
        }

        history.push(ParameterVersion {
            kind: kind.clone(),
            value: value.clone(),
            version,
            last_modified,
            description: description.clone(),
            key_id: key_id.clone(),
            tier: tier.to_string(),
            data_type: data_type.clone(),
            labels: Vec::new(),
            policies: policies.clone(),
        });

        parameters.insert(
            req.name.clone(),
            Parameter {
                kind,
                value,
                version,
                last_modified,
                description,
                key_id,
                tier: tier.to_string(),
                data_type,
                policies,
                tags,
                history,
            },
        );
        if self.persist(&parameters).is_err() {
            match previous {
                Some(p) => parameters.insert(req.name, p),
                None => parameters.remove(&req.name),
            };
            return error_outcome(500, "InternalServerError", "failed to persist ssm state");
        }
        Outcome::ok(json!({ "Version": version, "Tier": tier }))
    }

    fn get_parameter(&self, body: &[u8]) -> Outcome {
        let req: GetParameterRequest = match decode(body) {
            Ok(r) => r,
            Err(o) => return o,
        };
        let (name, selector) = match split_selector(&req.name) {
            Ok(v) => v,
            Err(o) => return o,
        };
        if let Err(o) = validate_name(name) {
            return o;
        }
        let parameters = self.state.lock().unwrap();
        let Some(p) = parameters.get(name) else {
            return not_found(&req.name);
        };
        let resolved = match &selector {
            None => p.current_version(),
            Some(Selector::Version(v)) => match p.find_version(*v) {
                Some(pv) => pv,
                None => {
                    return error_outcome(
                        400,
                        "ParameterVersionNotFound",
                        &format!("Systems Manager could not find version {v} of {name}."),
                    );
                }
            },
            Some(Selector::Label(label)) => match p.find_label(label) {
                Some(pv) => pv,
                None => {
                    return error_outcome(
                        400,
                        "ParameterVersionNotFound",
                        &format!("Systems Manager could not find label {label} of {name}."),
                    );
                }
            },
        };
        let out = self.render_parameter(name, &resolved, req.with_decryption, selector.as_ref());
        Outcome::ok(json!({ "Parameter": out }))
    }

    fn get_parameters(&self, body: &[u8]) -> Outcome {
        let req: GetParametersRequest = match decode(body) {
            Ok(r) => r,
            Err(o) => return o,
        };
        if req.names.is_empty() {
            return validation("Names must contain at least 1 parameter name.");
        }
        if req.names.len() > MAX_BATCH_NAMES {
            return validation("Names must not contain more than 10 parameter names.");
        }
        for raw_name in &req.names {
            let (base, _) = match split_selector(raw_name) {
                Ok(v) => v,
                Err(o) => return o,
            };
            if let Err(o) = validate_name(base) {
                return o;
            }
        }

        let parameters = self.state.lock().unwrap();
        let mut found = Vec::new();
        let mut invalid = Vec::new();
        let mut seen_requests = std::collections::HashSet::new();

        for raw_name in &req.names {
            if !seen_requests.insert(raw_name.clone()) {
                continue;
            }
            let (name, selector) = split_selector(raw_name).unwrap();
            let Some(p) = parameters.get(name) else {
                invalid.push(raw_name.clone());
                continue;
            };
            let resolved = match &selector {
                None => Some(p.current_version()),
                Some(Selector::Version(v)) => p.find_version(*v),
                Some(Selector::Label(label)) => p.find_label(label),
            };
            match resolved {
                Some(pv) => {
                    found.push(self.render_parameter(
                        name,
                        &pv,
                        req.with_decryption,
                        selector.as_ref(),
                    ));
                }
                None => invalid.push(raw_name.clone()),
            }
        }

        Outcome::ok(json!({
            "Parameters": found,
            "InvalidParameters": invalid,
        }))
    }

    fn get_parameters_by_path(&self, body: &[u8]) -> Outcome {
        let req: GetParametersByPathRequest = match decode(body) {
            Ok(r) => r,
            Err(o) => return o,
        };
        if req.path.is_empty() || !req.path.starts_with('/') {
            return validation("The parameter doesn't meet the parameter name requirements. The parameter name must begin with a forward slash \"/\".");
        }
        let first_segment = req.path.trim_start_matches('/').to_ascii_lowercase();
        if first_segment.starts_with("aws") || first_segment.starts_with("ssm") {
            return validation(
                "Parameter path: can't be prefixed with \"aws\" or \"ssm\" (case-insensitive).",
            );
        }
        if !req
            .path
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'.' | b'-' | b'/'))
        {
            return validation(
                "Parameter path: only a mix of letters, numbers and the following 4 symbols .-_/ are allowed.",
            );
        }
        let page = match req.max_results {
            None => DEFAULT_BY_PATH_PAGE,
            Some(n) if (1..=MAX_BY_PATH_PAGE as i64).contains(&n) => n as usize,
            Some(_) => return validation("MaxResults must be between 1 and 10."),
        };

        for f in &req.parameter_filters {
            let opt = if f.option.is_empty() {
                "Equals"
            } else {
                f.option.as_str()
            };
            if f.values.is_empty() {
                return validation("ParameterFilters Values must not be empty.");
            }
            match f.key.as_str() {
                "Type" | "KeyId" => {
                    if !matches!(opt, "Equals" | "BeginsWith") {
                        return validation("ParameterFilters Option must be Equals or BeginsWith.");
                    }
                }
                "Label" => {
                    if opt != "Equals" {
                        return validation(
                            "ParameterFilters Option for Label filter must be Equals.",
                        );
                    }
                }
                _ => {
                    return validation(
                        "ParameterFilters Key for GetParametersByPath must be one of Type, KeyId, Label.",
                    );
                }
            }
        }

        let normalized_prefix = if req.path == "/" {
            "/".to_string()
        } else {
            format!("{}/", req.path.trim_end_matches('/'))
        };

        let parameters = self.state.lock().unwrap();
        let mut matches: Vec<Value> = Vec::new();
        let mut match_names: Vec<String> = Vec::new();

        for (name, param) in parameters.iter() {
            if !req.next_token.is_empty() && name.as_str() <= req.next_token.as_str() {
                continue;
            }
            let Some(rest) = name.strip_prefix(&normalized_prefix) else {
                continue;
            };
            if rest.is_empty() {
                continue;
            }
            if !req.recursive && rest.contains('/') {
                continue;
            }

            let mut label_selector: Option<Selector> = None;
            let mut candidate = Some(param.current_version());
            for f in &req.parameter_filters {
                if f.key == "Label" {
                    let found_label = f
                        .values
                        .iter()
                        .find_map(|lbl| param.find_label(lbl).map(|pv| (lbl.clone(), pv)));
                    match found_label {
                        Some((lbl, pv)) => {
                            label_selector = Some(Selector::Label(lbl));
                            candidate = Some(pv);
                        }
                        None => {
                            candidate = None;
                            break;
                        }
                    }
                }
            }
            let Some(pv) = candidate else {
                continue;
            };

            let passes_filters = req.parameter_filters.iter().all(|f| {
                let opt = if f.option.is_empty() {
                    "Equals"
                } else {
                    f.option.as_str()
                };
                match f.key.as_str() {
                    "Type" => f.values.iter().any(|v| {
                        if opt == "BeginsWith" {
                            pv.kind.starts_with(v)
                        } else {
                            &pv.kind == v
                        }
                    }),
                    "KeyId" => f.values.iter().any(|v| {
                        if opt == "BeginsWith" {
                            pv.key_id.starts_with(v)
                        } else {
                            &pv.key_id == v
                        }
                    }),
                    "Label" => true,
                    _ => false,
                }
            });
            if !passes_filters {
                continue;
            }

            match_names.push(name.clone());
            matches.push(self.render_parameter(
                name,
                &pv,
                req.with_decryption,
                label_selector.as_ref(),
            ));
            if matches.len() > page {
                break;
            }
        }

        let has_more = matches.len() > page;
        matches.truncate(page);
        let mut out = json!({ "Parameters": matches });
        if has_more {
            out["NextToken"] = json!(match_names[page - 1]);
        }
        Outcome::ok(out)
    }

    fn get_parameter_history(&self, body: &[u8]) -> Outcome {
        let req: GetParameterHistoryRequest = match decode(body) {
            Ok(r) => r,
            Err(o) => return o,
        };
        if let Err(o) = validate_name(&req.name) {
            return o;
        }
        let page = match req.max_results {
            None => DEFAULT_HISTORY_PAGE,
            Some(n) if (1..=MAX_HISTORY_PAGE as i64).contains(&n) => n as usize,
            Some(_) => return validation("MaxResults must be between 1 and 50."),
        };
        let after_version = if req.next_token.is_empty() {
            0
        } else {
            match req.next_token.parse::<u64>() {
                Ok(v) => v,
                Err(_) => return validation("Invalid NextToken."),
            }
        };

        let parameters = self.state.lock().unwrap();
        let Some(param) = parameters.get(&req.name) else {
            return not_found(&req.name);
        };
        let items: Vec<&ParameterVersion> = param
            .history
            .iter()
            .filter(|pv| pv.version > after_version)
            .take(page + 1)
            .collect();
        let has_more = items.len() > page;
        let listed: Vec<Value> = items
            .iter()
            .take(page)
            .map(|pv| {
                let value = if pv.kind == "SecureString" && !req.with_decryption {
                    encrypted_placeholder(&pv.value)
                } else {
                    pv.value.clone()
                };
                let mut entry = json!({
                    "Name": req.name,
                    "Type": pv.kind,
                    "Value": value,
                    "Version": pv.version,
                    "LastModifiedDate": pv.last_modified,
                    "Labels": pv.labels,
                    "Tier": pv.tier,
                    "DataType": pv.data_type,
                    "Policies": pv.policies,
                });
                if !pv.description.is_empty() {
                    entry["Description"] = json!(pv.description);
                }
                if !pv.key_id.is_empty() {
                    entry["KeyId"] = json!(pv.key_id);
                }
                entry
            })
            .collect();
        let mut out = json!({ "Parameters": listed });
        if has_more {
            out["NextToken"] = json!(items[page - 1].version.to_string());
        }
        Outcome::ok(out)
    }

    fn label_parameter_version(&self, body: &[u8]) -> Outcome {
        let req: LabelParameterVersionRequest = match decode(body) {
            Ok(r) => r,
            Err(o) => return o,
        };
        if let Err(o) = validate_name(&req.name) {
            return o;
        }
        if req.labels.is_empty() {
            return validation("Labels must contain at least 1 label.");
        }
        if req.labels.len() > MAX_LABELS_PER_VERSION {
            return error_outcome(
                400,
                "ParameterVersionLabelLimitExceeded",
                "A parameter version can have at most 10 labels.",
            );
        }

        let mut parameters = self.state.lock().unwrap();
        let Some(previous) = parameters.get(&req.name).cloned() else {
            return not_found(&req.name);
        };
        let param = parameters.get_mut(&req.name).unwrap();
        param.ensure_history();
        let target_version = req.parameter_version.unwrap_or(param.version);
        let Some(target_idx) = param
            .history
            .iter()
            .position(|pv| pv.version == target_version)
        else {
            return error_outcome(
                400,
                "ParameterVersionNotFound",
                &format!(
                    "Systems Manager could not find version {target_version} of {}.",
                    req.name
                ),
            );
        };

        let mut invalid_labels = Vec::new();
        let mut valid_labels = Vec::new();
        for lbl in &req.labels {
            if is_valid_label(lbl) {
                if !valid_labels.contains(lbl) {
                    valid_labels.push(lbl.clone());
                }
            } else {
                invalid_labels.push(lbl.clone());
            }
        }

        let existing_on_target = &param.history[target_idx].labels;
        let new_unique_count = valid_labels
            .iter()
            .filter(|l| !existing_on_target.contains(l))
            .count();
        if existing_on_target.len() + new_unique_count > MAX_LABELS_PER_VERSION {
            return error_outcome(
                400,
                "ParameterVersionLabelLimitExceeded",
                "A parameter version can have at most 10 labels.",
            );
        }

        for lbl in &valid_labels {
            for (idx, pv) in param.history.iter_mut().enumerate() {
                if idx != target_idx {
                    pv.labels.retain(|existing| existing != lbl);
                }
            }
            if !param.history[target_idx].labels.contains(lbl) {
                param.history[target_idx].labels.push(lbl.clone());
            }
        }

        if self.persist(&parameters).is_err() {
            parameters.insert(req.name, previous);
            return error_outcome(500, "InternalServerError", "failed to persist ssm state");
        }
        Outcome::ok(json!({
            "InvalidLabels": invalid_labels,
            "ParameterVersion": target_version,
        }))
    }

    fn unlabel_parameter_version(&self, body: &[u8]) -> Outcome {
        let req: UnlabelParameterVersionRequest = match decode(body) {
            Ok(r) => r,
            Err(o) => return o,
        };
        if let Err(o) = validate_name(&req.name) {
            return o;
        }
        let Some(target_version) = req.parameter_version else {
            return validation("ParameterVersion is required.");
        };
        if req.labels.is_empty() {
            return validation("Labels must contain at least 1 label.");
        }
        if req.labels.len() > MAX_LABELS_PER_VERSION {
            return validation("Labels must not contain more than 10 labels.");
        }

        let mut parameters = self.state.lock().unwrap();
        let Some(previous) = parameters.get(&req.name).cloned() else {
            return not_found(&req.name);
        };
        let param = parameters.get_mut(&req.name).unwrap();
        param.ensure_history();
        let Some(pv) = param
            .history
            .iter_mut()
            .find(|pv| pv.version == target_version)
        else {
            return error_outcome(
                400,
                "ParameterVersionNotFound",
                &format!(
                    "Systems Manager could not find version {target_version} of {}.",
                    req.name
                ),
            );
        };

        let mut removed = Vec::new();
        let mut invalid = Vec::new();
        for lbl in &req.labels {
            if let Some(pos) = pv.labels.iter().position(|existing| existing == lbl) {
                pv.labels.remove(pos);
                removed.push(lbl.clone());
            } else {
                invalid.push(lbl.clone());
            }
        }

        if self.persist(&parameters).is_err() {
            parameters.insert(req.name, previous);
            return error_outcome(500, "InternalServerError", "failed to persist ssm state");
        }
        Outcome::ok(json!({
            "RemovedLabels": removed,
            "InvalidLabels": invalid,
        }))
    }

    fn add_tags_to_resource(&self, body: &[u8]) -> Outcome {
        let req: AddTagsToResourceRequest = match decode(body) {
            Ok(r) => r,
            Err(o) => return o,
        };
        if req.resource_type != "Parameter" {
            return validation("ResourceType must be Parameter.");
        }
        if req.resource_id.is_empty() {
            return validation("ResourceId is required.");
        }
        if req.tags.is_empty() {
            return validation("Tags must not be empty.");
        }
        if let Err(o) = validate_tags(&req.tags) {
            return o;
        }

        let mut parameters = self.state.lock().unwrap();
        let Some(previous) = parameters.get(&req.resource_id).cloned() else {
            return invalid_resource_id();
        };
        let param = parameters.get_mut(&req.resource_id).unwrap();
        for new_tag in req.tags {
            if let Some(existing) = param.tags.iter_mut().find(|t| t.key == new_tag.key) {
                existing.value = new_tag.value;
            } else {
                param.tags.push(new_tag);
            }
        }
        if self.persist(&parameters).is_err() {
            parameters.insert(req.resource_id, previous);
            return error_outcome(500, "InternalServerError", "failed to persist ssm state");
        }
        Outcome::ok(json!({}))
    }

    fn remove_tags_from_resource(&self, body: &[u8]) -> Outcome {
        let req: RemoveTagsFromResourceRequest = match decode(body) {
            Ok(r) => r,
            Err(o) => return o,
        };
        if req.resource_type != "Parameter" {
            return validation("ResourceType must be Parameter.");
        }
        if req.resource_id.is_empty() {
            return validation("ResourceId is required.");
        }
        if req.tag_keys.is_empty() {
            return validation("TagKeys must not be empty.");
        }
        for key in &req.tag_keys {
            if key.is_empty() || key.chars().count() > MAX_TAG_KEY_CHARS {
                return validation("Tag key must be between 1 and 128 characters in length.");
            }
            if key.to_ascii_lowercase().starts_with("aws:") {
                return validation("Tag key cannot start with reserved prefix aws:.");
            }
        }

        let mut parameters = self.state.lock().unwrap();
        let Some(previous) = parameters.get(&req.resource_id).cloned() else {
            return invalid_resource_id();
        };
        let param = parameters.get_mut(&req.resource_id).unwrap();
        param
            .tags
            .retain(|t| !req.tag_keys.iter().any(|k| k == &t.key));
        if self.persist(&parameters).is_err() {
            parameters.insert(req.resource_id, previous);
            return error_outcome(500, "InternalServerError", "failed to persist ssm state");
        }
        Outcome::ok(json!({}))
    }

    fn list_tags_for_resource(&self, body: &[u8]) -> Outcome {
        let req: ListTagsForResourceRequest = match decode(body) {
            Ok(r) => r,
            Err(o) => return o,
        };
        if req.resource_type != "Parameter" {
            return validation("ResourceType must be Parameter.");
        }
        if req.resource_id.is_empty() {
            return validation("ResourceId is required.");
        }

        let parameters = self.state.lock().unwrap();
        let Some(param) = parameters.get(&req.resource_id) else {
            return invalid_resource_id();
        };
        Outcome::ok(json!({ "TagList": param.tags }))
    }

    fn delete_parameter(&self, body: &[u8]) -> Outcome {
        let req: DeleteParameterRequest = match decode(body) {
            Ok(r) => r,
            Err(o) => return o,
        };
        if let Err(o) = validate_name(&req.name) {
            return o;
        }
        let mut parameters = self.state.lock().unwrap();
        let Some(previous) = parameters.remove(&req.name) else {
            return not_found(&req.name);
        };
        if self.persist(&parameters).is_err() {
            parameters.insert(req.name, previous);
            return error_outcome(500, "InternalServerError", "failed to persist ssm state");
        }
        Outcome::ok(json!({}))
    }

    fn delete_parameters(&self, body: &[u8]) -> Outcome {
        let req: DeleteParametersRequest = match decode(body) {
            Ok(r) => r,
            Err(o) => return o,
        };
        if req.names.is_empty() {
            return validation("Names must contain at least 1 parameter name.");
        }
        if req.names.len() > MAX_BATCH_NAMES {
            return validation("Names must not contain more than 10 parameter names.");
        }
        for name in &req.names {
            if let Err(o) = validate_name(name) {
                return o;
            }
        }
        let mut parameters = self.state.lock().unwrap();
        let snapshot = parameters.clone();
        let mut deleted = Vec::new();
        let mut invalid = Vec::new();
        for name in &req.names {
            if parameters.remove(name).is_some() {
                deleted.push(name.clone());
            } else if !deleted.contains(name) && !invalid.contains(name) {
                invalid.push(name.clone());
            }
        }
        if !deleted.is_empty() && self.persist(&parameters).is_err() {
            *parameters = snapshot;
            return error_outcome(500, "InternalServerError", "failed to persist ssm state");
        }
        Outcome::ok(json!({
            "DeletedParameters": deleted,
            "InvalidParameters": invalid,
        }))
    }

    /// Metadata only — never values. Filters are rejected rather than ignored
    /// so a caller relying on them does not silently get every parameter.
    fn describe_parameters(&self, body: &[u8]) -> Outcome {
        let req: DescribeParametersRequest = match decode(body) {
            Ok(r) => r,
            Err(o) => return o,
        };
        if !req.filters.is_empty() || !req.parameter_filters.is_empty() {
            return validation("DescribeParameters filters are not supported by devcloud.");
        }
        let page = match req.max_results {
            None => DEFAULT_DESCRIBE_PAGE,
            Some(n) if (1..=MAX_DESCRIBE_PAGE as i64).contains(&n) => n as usize,
            Some(_) => return validation("MaxResults must be between 1 and 50."),
        };
        let parameters = self.state.lock().unwrap();
        // NextToken is the last name of the previous page (names are unique and
        // the map is ordered).
        let items: Vec<(&String, &Parameter)> = parameters
            .iter()
            .filter(|(name, _)| {
                req.next_token.is_empty() || name.as_str() > req.next_token.as_str()
            })
            .take(page + 1)
            .collect();
        let has_more = items.len() > page;
        let listed: Vec<Value> = items
            .iter()
            .take(page)
            .map(|(name, p)| {
                let mut entry = json!({
                    "Name": name,
                    "ARN": self.arn(name),
                    "Type": p.kind,
                    "Version": p.version,
                    "LastModifiedDate": p.last_modified,
                    "Tier": p.tier,
                    "DataType": p.data_type,
                    "Policies": p.policies,
                });
                if !p.description.is_empty() {
                    entry["Description"] = json!(p.description);
                }
                if !p.key_id.is_empty() {
                    entry["KeyId"] = json!(p.key_id);
                }
                entry
            })
            .collect();
        let mut out = json!({ "Parameters": listed });
        if has_more {
            out["NextToken"] = json!(items[page - 1].0);
        }
        Outcome::ok(out)
    }
}
