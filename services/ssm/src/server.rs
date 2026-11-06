//! SSM Parameter Store over the AWS JSON 1.1 protocol.
//!
//! Supported operations: `PutParameter`, `GetParameter`, `DeleteParameter`,
//! and `DescribeParameters` (metadata only, used by the dashboard). State is
//! one map behind a `Mutex`, persisted atomically to `state.json`.
//!
//! `SecureString` values are stored as plaintext in `state.json` — this is a
//! local emulator, there is no KMS. `GetParameter` without `WithDecryption`
//! returns an opaque placeholder instead of the value, so a caller that forgets
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
struct DeleteParameterRequest {
    #[serde(default)]
    name: String,
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

/// Splits an optional `:<version>` selector off a `GetParameter` name.
/// Labels are not supported; a non-numeric selector is rejected.
fn split_selector(name: &str) -> Result<(&str, Option<u64>), Outcome> {
    let Some((base, selector)) = name.rsplit_once(':') else {
        return Ok((name, None));
    };
    match selector.parse::<u64>() {
        Ok(v) => Ok((base, Some(v))),
        Err(_) => Err(validation(
            "Parameter labels are not supported by devcloud; use a numeric version selector.",
        )),
    }
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
        let persisted: PersistedState = serde_json::from_slice(&data).map_err(|e| e.to_string())?;
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

    pub fn dispatch(&self, operation: &str, body: &[u8]) -> Outcome {
        match operation {
            "PutParameter" => self.put_parameter(body),
            "GetParameter" => self.get_parameter(body),
            "DeleteParameter" => self.delete_parameter(body),
            "DescribeParameters" => self.describe_parameters(body),
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
        let tier = match req.tier.as_str() {
            "" | "Standard" | "Intelligent-Tiering" => "Standard",
            "Advanced" => "Advanced",
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
            "" | "text" => "text",
            "aws:ec2:image" | "aws:ssm:integration" => req.data_type.as_str(),
            _ => {
                return validation(
                    "DataType must be one of text, aws:ec2:image, aws:ssm:integration.",
                )
            }
        };

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
            if req.key_id.is_empty() {
                "alias/aws/ssm".to_string()
            } else {
                req.key_id
            }
        } else {
            String::new()
        };
        parameters.insert(
            req.name.clone(),
            Parameter {
                kind,
                value,
                version,
                last_modified: now_epoch_seconds(),
                description,
                key_id,
                tier: tier.to_string(),
                data_type: data_type.to_string(),
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
        let (name, version) = match split_selector(&req.name) {
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
        // Only the current version is retained.
        if let Some(v) = version {
            if v != p.version {
                return error_outcome(
                    400,
                    "ParameterVersionNotFound",
                    &format!("Systems Manager could not find version {v} of {name}."),
                );
            }
        }
        let value = if p.kind == "SecureString" && !req.with_decryption {
            encrypted_placeholder(&p.value)
        } else {
            p.value.clone()
        };
        let mut out = json!({
            "Name": name,
            "Type": p.kind,
            "Value": value,
            "Version": p.version,
            "LastModifiedDate": p.last_modified,
            "ARN": self.arn(name),
            "DataType": p.data_type,
        });
        if version.is_some() {
            out["Selector"] = json!(format!(":{}", p.version));
        }
        Outcome::ok(json!({ "Parameter": out }))
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
