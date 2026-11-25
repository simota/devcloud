//! Local KMS keyring for SSE-KMS (`aws:kms`, `aws:kms:dsse`).
//!
//! Keys and aliases live in `.kms.json` at the storage root (a file, so bucket
//! listing never sees it) and are managed through `/_kms/*`:
//!
//! ```text
//! POST /_kms/keys                          {"Description"}      -> {"KeyMetadata"}
//! GET  /_kms/keys                                               -> {"Keys","Aliases"}
//! GET  /_kms/keys/{key-id}                                      -> {"KeyMetadata"}
//! POST /_kms/keys/{key-id}/enable | disable | cancel-deletion   -> {"KeyMetadata"}
//! POST /_kms/keys/{key-id}/schedule-deletion {"PendingWindowInDays"} -> {"KeyMetadata"}
//! POST /_kms/aliases                       {"AliasName","TargetKeyId"} -> {"AliasArn"}
//! ```
//!
//! The same keyring backs the AWS KMS JSON API at `POST /` (`kms_api.rs`).
//!
//! In `strict` auth mode, writes with SSE-KMS resolve the requested key (key
//! ID, key ARN, alias name, or alias ARN; none means the AWS managed
//! `alias/aws/s3`, created on first use), refuse missing, disabled, or
//! pending-deletion keys, and store the key ARN; reads of an object whose key
//! has since been disabled or scheduled for deletion are refused. In `relaxed`
//! mode the keyring is not consulted: the given key ID is stored verbatim and
//! reads are never refused, as before the keyring existed. Object bytes are not encrypted with the key: the
//! object store is shared in-process with GCS, BigQuery, Redshift, and Lambda,
//! which read objects directly, so the bytes stay plaintext at rest.

use std::path::Path;

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::http::{AuthConfig, Request, Response};
use crate::identities::{
    json_error, json_response, random_hex, unix_now, write_file, OWNER_ACCOUNT,
};

pub(crate) const KMS_PREFIX: &str = "/_kms/";
const KEYRING_FILE: &str = ".kms.json";
const DEFAULT_S3_ALIAS: &str = "alias/aws/s3";
const DEFAULT_REGION: &str = "us-east-1";

pub(crate) const ENABLED: &str = "Enabled";
pub(crate) const DISABLED: &str = "Disabled";
pub(crate) const PENDING_DELETION: &str = "PendingDeletion";

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub(crate) struct KmsKey {
    pub(crate) key_id: String,
    pub(crate) arn: String,
    #[serde(default)]
    pub(crate) description: String,
    pub(crate) key_state: String,
    /// `CUSTOMER` or `AWS` (the managed `alias/aws/s3` key).
    pub(crate) key_manager: String,
    pub(crate) creation_date: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) deletion_date: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub(crate) struct KmsAlias {
    pub(crate) alias_name: String,
    pub(crate) alias_arn: String,
    pub(crate) target_key_id: String,
}

#[derive(Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub(crate) struct Keyring {
    #[serde(default)]
    pub(crate) keys: Vec<KmsKey>,
    #[serde(default)]
    pub(crate) aliases: Vec<KmsAlias>,
}

/// A key state change requested through `/_kms/*` or the KMS API.
#[derive(Debug, Clone, Copy)]
pub(crate) enum Transition {
    Enable,
    Disable,
    ScheduleDeletion { days: i64 },
    CancelDeletion,
}

/// A refused keyring change: KMS error code and message (HTTP 400).
pub(crate) type KeyringError = (&'static str, &'static str);

impl KmsKey {
    pub(crate) fn apply(&mut self, transition: Transition) -> Result<(), KeyringError> {
        if self.key_manager == "AWS" {
            return Err((
                "UnsupportedOperationException",
                "AWS managed keys cannot be changed",
            ));
        }
        let pending = self.key_state == PENDING_DELETION;
        match transition {
            Transition::Enable | Transition::Disable if pending => {
                return Err(("KMSInvalidStateException", "key is pending deletion"))
            }
            Transition::Enable => self.key_state = ENABLED.to_string(),
            Transition::Disable => self.key_state = DISABLED.to_string(),
            Transition::ScheduleDeletion { days } => {
                if !(7..=30).contains(&days) {
                    return Err(("ValidationException", "PendingWindowInDays must be 7..=30"));
                }
                self.key_state = PENDING_DELETION.to_string();
                self.deletion_date = Some(crate::time_fmt::rfc3339_seconds_from_unix(
                    unix_now() + days * 86_400,
                ));
            }
            Transition::CancelDeletion if pending => {
                self.key_state = DISABLED.to_string();
                self.deletion_date = None;
            }
            Transition::CancelDeletion => {
                return Err(("KMSInvalidStateException", "key is not pending deletion"))
            }
        }
        Ok(())
    }
}

/// Why an SSE-KMS request was refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KmsError {
    NotFound,
    Disabled,
    InvalidState,
    Io,
}

impl KmsError {
    pub fn code(self) -> &'static str {
        match self {
            KmsError::NotFound => "KMS.NotFoundException",
            KmsError::Disabled => "KMS.DisabledException",
            KmsError::InvalidState => "KMS.KMSInvalidStateException",
            KmsError::Io => "InternalError",
        }
    }

    pub fn status(self) -> u16 {
        match self {
            KmsError::Io => 500,
            _ => 400,
        }
    }

    pub fn message(self) -> &'static str {
        match self {
            KmsError::NotFound => "The specified KMS key does not exist.",
            KmsError::Disabled => "The specified KMS key is disabled.",
            KmsError::InvalidState => "The specified KMS key is pending deletion.",
            KmsError::Io => "could not update the local KMS keyring",
        }
    }
}

impl Keyring {
    pub(crate) fn find_key(&self, key_ref: &str) -> Option<&KmsKey> {
        let alias_name = if key_ref.starts_with("alias/") {
            Some(key_ref)
        } else if key_ref.starts_with("arn:") && key_ref.contains(":alias/") {
            key_ref.find("alias/").map(|i| &key_ref[i..])
        } else {
            None
        };
        if let Some(name) = alias_name {
            let alias = self.aliases.iter().find(|a| a.alias_name == name)?;
            return self.keys.iter().find(|k| k.key_id == alias.target_key_id);
        }
        self.keys
            .iter()
            .find(|k| k.key_id == key_ref || k.arn == key_ref)
    }

    pub(crate) fn find_key_mut(&mut self, key_id: &str) -> Option<&mut KmsKey> {
        self.keys
            .iter_mut()
            .find(|k| k.key_id == key_id || k.arn == key_id)
    }

    pub(crate) fn create_key(&mut self, region: &str, description: &str, manager: &str) -> KmsKey {
        let seed = random_hex(description);
        let key_id = format!(
            "{}-{}-{}-{}-{}",
            &seed[0..8],
            &seed[8..12],
            &seed[12..16],
            &seed[16..20],
            &seed[20..32]
        );
        let key = KmsKey {
            arn: format!("arn:aws:kms:{region}:{OWNER_ACCOUNT}:key/{key_id}"),
            key_id,
            description: description.to_string(),
            key_state: ENABLED.to_string(),
            key_manager: manager.to_string(),
            creation_date: crate::time_fmt::rfc3339_seconds_from_unix(unix_now()),
            deletion_date: None,
        };
        self.keys.push(key.clone());
        key
    }

    /// Validates and adds a customer alias for an existing key.
    pub(crate) fn create_alias(
        &mut self,
        region: &str,
        name: &str,
        target: &str,
    ) -> Result<KmsAlias, KeyringError> {
        if !name.starts_with("alias/")
            || name.starts_with("alias/aws/")
            || name.len() <= "alias/".len()
        {
            return Err(("ValidationException", "invalid AliasName"));
        }
        if self.aliases.iter().any(|a| a.alias_name == name) {
            return Err(("AlreadyExistsException", "alias already exists"));
        }
        let Some(key_id) = self.find_key(target).map(|k| k.key_id.clone()) else {
            return Err(("NotFoundException", "key does not exist"));
        };
        Ok(self.add_alias(region, name, &key_id))
    }

    fn add_alias(&mut self, region: &str, name: &str, target_key_id: &str) -> KmsAlias {
        let alias = KmsAlias {
            alias_name: name.to_string(),
            alias_arn: format!("arn:aws:kms:{region}:{OWNER_ACCOUNT}:{name}"),
            target_key_id: target_key_id.to_string(),
        };
        self.aliases.push(alias.clone());
        alias
    }
}

pub(crate) fn usable(key: &KmsKey) -> Result<(), KmsError> {
    match key.key_state.as_str() {
        ENABLED => Ok(()),
        DISABLED => Err(KmsError::Disabled),
        _ => Err(KmsError::InvalidState),
    }
}

/// Resolves the key an SSE-KMS write names (empty: the AWS managed
/// `alias/aws/s3` key) and returns its ARN, which is stored with the object.
pub fn resolve_for_write(root: &Path, key_ref: &str) -> Result<String, KmsError> {
    let mut keyring = load(root);
    if key_ref.is_empty() || key_ref == DEFAULT_S3_ALIAS {
        if let Some(key) = keyring.find_key(DEFAULT_S3_ALIAS) {
            usable(key)?;
            return Ok(key.arn.clone());
        }
        let key = keyring.create_key(DEFAULT_REGION, "Default key for S3", "AWS");
        keyring.add_alias(DEFAULT_REGION, DEFAULT_S3_ALIAS, &key.key_id);
        save(root, &keyring).map_err(|_| KmsError::Io)?;
        return Ok(key.arn);
    }
    let key = keyring.find_key(key_ref).ok_or(KmsError::NotFound)?;
    usable(key)?;
    Ok(key.arn.clone())
}

/// Refuses reads of an object whose KMS key is no longer usable. Key IDs that
/// are not in the keyring (objects written before it existed, whose IDs were
/// only round-tripped) stay readable.
pub fn check_readable(root: &Path, key_arn: &str) -> Result<(), KmsError> {
    if key_arn.is_empty() {
        return Ok(());
    }
    match load(root).find_key(key_arn) {
        Some(key) => usable(key),
        None => Ok(()),
    }
}

/// Serves `/_kms/*`. Authorization (owner only) has already been applied.
pub(crate) fn handle(root: &Path, req: &Request, auth: &AuthConfig) -> Response {
    let region = crate::auth::configured_region(auth);
    let rest = &req.path[KMS_PREFIX.len()..];
    let body: Value = if req.body.is_empty() {
        json!({})
    } else {
        match serde_json::from_slice(&req.body) {
            Ok(v) => v,
            Err(_) => return json_error(400, "ValidationException", "invalid JSON body"),
        }
    };
    let mut keyring = load(root);
    let segments: Vec<&str> = rest.split('/').collect();
    let response = match (req.method.as_str(), segments.as_slice()) {
        ("GET", ["keys"]) => {
            return json_response(
                200,
                &json!({"Keys": keyring.keys, "Aliases": keyring.aliases}),
            )
        }
        ("POST", ["keys"]) => {
            let description = body
                .get("Description")
                .and_then(Value::as_str)
                .unwrap_or("");
            let key = keyring.create_key(region, description, "CUSTOMER");
            json!({"KeyMetadata": key})
        }
        ("GET", ["keys", id]) => {
            return match keyring.find_key(id) {
                Some(key) => json_response(200, &json!({"KeyMetadata": key})),
                None => json_error(404, "NotFoundException", "key does not exist"),
            }
        }
        ("POST", ["keys", id, action]) => {
            let transition = match *action {
                "enable" => Transition::Enable,
                "disable" => Transition::Disable,
                "schedule-deletion" => Transition::ScheduleDeletion {
                    days: body
                        .get("PendingWindowInDays")
                        .and_then(Value::as_i64)
                        .unwrap_or(30),
                },
                "cancel-deletion" => Transition::CancelDeletion,
                _ => return json_error(404, "NotFound", "unknown KMS endpoint"),
            };
            let Some(key) = keyring.find_key_mut(id) else {
                return json_error(404, "NotFoundException", "key does not exist");
            };
            if let Err((code, message)) = key.apply(transition) {
                return json_error(400, code, message);
            }
            json!({"KeyMetadata": key.clone()})
        }
        ("POST", ["aliases"]) => {
            let name = body.get("AliasName").and_then(Value::as_str).unwrap_or("");
            let target = body
                .get("TargetKeyId")
                .and_then(Value::as_str)
                .unwrap_or("");
            match keyring.create_alias(region, name, target) {
                Ok(alias) => json!({"AliasArn": alias.alias_arn}),
                Err(("NotFoundException", message)) => {
                    return json_error(404, "NotFoundException", message)
                }
                Err((code, message)) => return json_error(400, code, message),
            }
        }
        _ => return json_error(404, "NotFound", "unknown KMS endpoint"),
    };
    if save(root, &keyring).is_err() {
        return json_error(500, "InternalError", "could not write the KMS keyring");
    }
    json_response(200, &response)
}

pub(crate) fn load(root: &Path) -> Keyring {
    std::fs::read(root.join(KEYRING_FILE))
        .ok()
        .and_then(|b| serde_json::from_slice(&b).ok())
        .unwrap_or_default()
}

pub(crate) fn save(root: &Path, keyring: &Keyring) -> std::io::Result<()> {
    let bytes = serde_json::to_vec_pretty(keyring).map_err(std::io::Error::other)?;
    write_file(root, KEYRING_FILE, &bytes)
}
