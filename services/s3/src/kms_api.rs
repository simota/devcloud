//! A local subset of the AWS KMS JSON API (`X-Amz-Target: TrentService.<Op>`,
//! `POST /` on the S3 port), backed by the keyring that SSE-KMS uses
//! (`kms.rs`).
//!
//! Key management: `CreateKey`, `DescribeKey`, `ListKeys`, `EnableKey`,
//! `DisableKey`, `ScheduleKeyDeletion`, `CancelKeyDeletion`, `CreateAlias`,
//! `DeleteAlias`, `ListAliases`. Cryptography with symmetric keys
//! (`SYMMETRIC_DEFAULT` only): `Encrypt`, `Decrypt`, `GenerateDataKey`,
//! `GenerateDataKeyWithoutPlaintext`, `ReEncrypt`.
//!
//! Each key gets 256-bit key material on first cryptographic use, kept in
//! `.kms-material.json` in the storage root (never returned or logged). A
//! ciphertext blob is `DCKMS1`, the key ID, and an authenticated frame (the
//! SSE-C construction in `sse_c.rs`) under a key derived from the key material
//! and the encryption context, so `Decrypt` needs the same context. Blobs are
//! only meaningful to this emulator. Key material comes from a hash of the
//! clock, process, and a counter: fine for local tests, not for real secrets.
//!
//! In `strict` mode unsigned requests are refused; the owner key may call
//! everything; a registry principal of the owner account needs the `kms:<Op>`
//! action on the key ARN (`*` for `CreateKey`, `ListKeys`, `ListAliases`) from
//! its identity policies within its session policy, boundary, and SCPs; other
//! accounts are refused, since no key policy trusts them. As in AWS, AWS managed
//! keys (`alias/aws/s3`) cannot be used for cryptography directly. Relaxed mode
//! checks none of this.

use std::collections::BTreeMap;
use std::path::Path;

use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use sha2::Sha256;

use crate::http::{AuthConfig, Request, Response};
use crate::iam::Caller;
use crate::identities::{json_error, json_response, random_hex, write_file, OWNER_ACCOUNT};
use crate::kms::{self, Keyring, KmsKey, Transition, DISABLED, ENABLED};

const MATERIAL_FILE: &str = ".kms-material.json";
const BLOB_MAGIC: &[u8] = b"DCKMS1";
const ALGORITHM: &str = "SYMMETRIC_DEFAULT";
const MAX_PLAINTEXT: usize = 4096;

#[derive(Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct MaterialFile {
    /// Key ID -> base64 key material.
    #[serde(default)]
    keys: BTreeMap<String, String>,
}

type ApiResult = Result<Value, Response>;

fn err(code: &str, message: &str) -> Response {
    let mut response = json_error(400, code, message);
    response.headers.insert(
        "Content-Type".to_string(),
        "application/x-amz-json-1.1".to_string(),
    );
    response
}

pub(crate) fn handle(root: &Path, req: &Request, auth: &AuthConfig, caller: &Caller) -> Response {
    let strict = auth.auth_mode.eq_ignore_ascii_case("strict");
    if strict && *caller == Caller::Anonymous {
        let mut response = err(
            "MissingAuthenticationTokenException",
            "Request is missing Authentication Token",
        );
        response.status = 403;
        return response;
    }
    let operation = req
        .header("x-amz-target")
        .trim_start_matches("TrentService.")
        .to_string();
    let input: Value = if req.body.is_empty() {
        json!({})
    } else {
        match serde_json::from_slice(&req.body) {
            Ok(v @ Value::Object(_)) => v,
            _ => return err("SerializationException", "invalid JSON request body"),
        }
    };
    let api = Api {
        root,
        req,
        caller,
        strict,
        region: crate::auth::configured_region(auth).to_string(),
        input,
    };
    let result = match operation.as_str() {
        "CreateKey" => api.create_key(),
        "DescribeKey" => api.describe_key(),
        "ListKeys" => api.list_keys(),
        "EnableKey" => api.transition("kms:EnableKey", |_| Ok(Transition::Enable)),
        "DisableKey" => api.transition("kms:DisableKey", |_| Ok(Transition::Disable)),
        "ScheduleKeyDeletion" => api.transition("kms:ScheduleKeyDeletion", |input| {
            let days = match input.get("PendingWindowInDays") {
                None => 30,
                Some(v) => v
                    .as_i64()
                    .ok_or_else(|| err("ValidationException", "invalid PendingWindowInDays"))?,
            };
            Ok(Transition::ScheduleDeletion { days })
        }),
        "CancelKeyDeletion" => {
            api.transition("kms:CancelKeyDeletion", |_| Ok(Transition::CancelDeletion))
        }
        "CreateAlias" => api.create_alias(),
        "DeleteAlias" => api.delete_alias(),
        "ListAliases" => api.list_aliases(),
        "Encrypt" => api.encrypt(),
        "Decrypt" => api.decrypt(),
        "ReEncrypt" => api.re_encrypt(),
        "GenerateDataKey" => api.generate_data_key(true),
        "GenerateDataKeyWithoutPlaintext" => api.generate_data_key(false),
        _ => Err(err(
            "UnknownOperationException",
            "the operation is not supported by the local KMS API",
        )),
    };
    match result {
        Ok(value) => {
            let mut response = json_response(200, &value);
            response.headers.insert(
                "Content-Type".to_string(),
                "application/x-amz-json-1.1".to_string(),
            );
            response
        }
        Err(response) => response,
    }
}

struct Api<'a> {
    root: &'a Path,
    req: &'a Request,
    caller: &'a Caller,
    strict: bool,
    region: String,
    input: Value,
}

impl Api<'_> {
    fn str_param(&self, name: &str) -> &str {
        self.input.get(name).and_then(Value::as_str).unwrap_or("")
    }

    fn required(&self, name: &str) -> Result<&str, Response> {
        match self.str_param(name) {
            "" => Err(err("ValidationException", &format!("{name} is required"))),
            value => Ok(value),
        }
    }

    /// Strict mode: a registry principal needs `action` on `resource`.
    fn authorize(&self, action: &'static str, resource: &str) -> Result<(), Response> {
        let Caller::Iam(p) = self.caller else {
            return Ok(());
        };
        if !self.strict {
            return Ok(());
        }
        let allowed = p.account == OWNER_ACCOUNT
            && crate::iam::identity_allows(p, self.req, action, resource, self.caller);
        if allowed {
            Ok(())
        } else {
            Err(err(
                "AccessDeniedException",
                &format!(
                    "User: {} is not authorized to perform: {action} on resource: {resource}",
                    p.arn
                ),
            ))
        }
    }

    fn find<'k>(&self, keyring: &'k Keyring, key_ref: &str) -> Result<&'k KmsKey, Response> {
        keyring.find_key(key_ref).ok_or_else(|| {
            err(
                "NotFoundException",
                &format!("Key '{key_ref}' does not exist"),
            )
        })
    }

    /// Resolves a key for cryptographic use: it must exist, be enabled, and be a
    /// customer managed key (strict mode), and the caller needs `action`.
    fn crypto_key(
        &self,
        keyring: &Keyring,
        key_ref: &str,
        action: &'static str,
    ) -> Result<KmsKey, Response> {
        let key = self.find(keyring, key_ref)?.clone();
        self.authorize(action, &key.arn)?;
        if self.strict && key.key_manager == "AWS" {
            return Err(err(
                "AccessDeniedException",
                "AWS managed keys can be used only through the integrated AWS service",
            ));
        }
        usable(&key)?;
        Ok(key)
    }

    fn create_key(&self) -> ApiResult {
        self.authorize("kms:CreateKey", "*")?;
        for (name, allowed) in [
            ("KeySpec", ALGORITHM),
            ("CustomerMasterKeySpec", ALGORITHM),
            ("KeyUsage", "ENCRYPT_DECRYPT"),
            ("Origin", "AWS_KMS"),
        ] {
            let value = self.str_param(name);
            if !value.is_empty() && value != allowed {
                return Err(err(
                    "UnsupportedOperationException",
                    &format!("the local KMS API supports only {name} {allowed}"),
                ));
            }
        }
        if self.input.get("MultiRegion").and_then(Value::as_bool) == Some(true) {
            return Err(err(
                "UnsupportedOperationException",
                "the local KMS API does not support multi-Region keys",
            ));
        }
        let mut keyring = kms::load(self.root);
        let key = keyring.create_key(&self.region, self.str_param("Description"), "CUSTOMER");
        save_keyring(self.root, &keyring)?;
        Ok(json!({"KeyMetadata": metadata(&key)}))
    }

    fn describe_key(&self) -> ApiResult {
        let keyring = kms::load(self.root);
        let key = self.find(&keyring, self.required("KeyId")?)?;
        self.authorize("kms:DescribeKey", &key.arn)?;
        Ok(json!({"KeyMetadata": metadata(key)}))
    }

    fn list_keys(&self) -> ApiResult {
        self.authorize("kms:ListKeys", "*")?;
        let keyring = kms::load(self.root);
        let keys: Vec<Value> = keyring
            .keys
            .iter()
            .map(|k| json!({"KeyId": k.key_id, "KeyArn": k.arn}))
            .collect();
        Ok(json!({"Keys": keys, "Truncated": false}))
    }

    fn transition(
        &self,
        action: &'static str,
        make: impl Fn(&Value) -> Result<Transition, Response>,
    ) -> ApiResult {
        let transition = make(&self.input)?;
        let key_ref = self.required("KeyId")?.to_string();
        let mut keyring = kms::load(self.root);
        let arn = self.find(&keyring, &key_ref)?.arn.clone();
        self.authorize(action, &arn)?;
        let key = keyring.find_key_mut(&arn).expect("key was found above");
        key.apply(transition)
            .map_err(|(code, message)| err(code, message))?;
        let key = key.clone();
        save_keyring(self.root, &keyring)?;
        Ok(match transition {
            Transition::ScheduleDeletion { days } => json!({
                "KeyId": key.arn,
                "KeyState": key.key_state,
                "DeletionDate": key.deletion_date.as_deref().map(epoch),
                "PendingWindowInDays": days,
            }),
            Transition::CancelDeletion => json!({"KeyId": key.arn}),
            Transition::Enable | Transition::Disable => json!({}),
        })
    }

    fn create_alias(&self) -> ApiResult {
        let name = self.required("AliasName")?;
        let target = self.required("TargetKeyId")?;
        let mut keyring = kms::load(self.root);
        let arn = self.find(&keyring, target)?.arn.clone();
        self.authorize("kms:CreateAlias", &arn)?;
        keyring
            .create_alias(&self.region, name, target)
            .map_err(|(code, message)| err(code, message))?;
        save_keyring(self.root, &keyring)?;
        Ok(json!({}))
    }

    fn delete_alias(&self) -> ApiResult {
        let name = self.required("AliasName")?;
        let mut keyring = kms::load(self.root);
        let Some(index) = keyring.aliases.iter().position(|a| a.alias_name == name) else {
            return Err(err(
                "NotFoundException",
                &format!("Alias {name} is not found."),
            ));
        };
        let arn = self
            .find(&keyring, &keyring.aliases[index].target_key_id)?
            .arn
            .clone();
        self.authorize("kms:DeleteAlias", &arn)?;
        if name.starts_with("alias/aws/") {
            return Err(err(
                "NotAuthorizedException",
                "AWS managed aliases cannot be deleted",
            ));
        }
        keyring.aliases.remove(index);
        save_keyring(self.root, &keyring)?;
        Ok(json!({}))
    }

    fn list_aliases(&self) -> ApiResult {
        self.authorize("kms:ListAliases", "*")?;
        let keyring = kms::load(self.root);
        let filter = match self.str_param("KeyId") {
            "" => None,
            key_ref => Some(self.find(&keyring, key_ref)?.key_id.clone()),
        };
        let aliases: Vec<Value> = keyring
            .aliases
            .iter()
            .filter(|a| filter.as_ref().is_none_or(|id| *id == a.target_key_id))
            .map(|a| {
                json!({
                    "AliasName": a.alias_name,
                    "AliasArn": a.alias_arn,
                    "TargetKeyId": a.target_key_id,
                })
            })
            .collect();
        Ok(json!({"Aliases": aliases, "Truncated": false}))
    }

    fn encrypt(&self) -> ApiResult {
        let plaintext = self.blob("Plaintext")?;
        if plaintext.is_empty() || plaintext.len() > MAX_PLAINTEXT {
            return Err(err(
                "ValidationException",
                "Plaintext must be 1 to 4096 bytes",
            ));
        }
        let keyring = kms::load(self.root);
        let key = self.crypto_key(&keyring, self.required("KeyId")?, "kms:Encrypt")?;
        let context = self.context("EncryptionContext")?;
        let blob = seal(self.root, &key, &context, &plaintext)?;
        Ok(json!({
            "CiphertextBlob": crate::base64::std_encode(&blob),
            "KeyId": key.arn,
            "EncryptionAlgorithm": ALGORITHM,
        }))
    }

    fn decrypt(&self) -> ApiResult {
        let blob = self.blob("CiphertextBlob")?;
        let (key, plaintext) = self.open(&blob, "KeyId", "EncryptionContext", "kms:Decrypt")?;
        Ok(json!({
            "Plaintext": crate::base64::std_encode(&plaintext),
            "KeyId": key.arn,
            "EncryptionAlgorithm": ALGORITHM,
        }))
    }

    fn re_encrypt(&self) -> ApiResult {
        let blob = self.blob("CiphertextBlob")?;
        let (source, plaintext) = self.open(
            &blob,
            "SourceKeyId",
            "SourceEncryptionContext",
            "kms:ReEncryptFrom",
        )?;
        let keyring = kms::load(self.root);
        let target = self.crypto_key(
            &keyring,
            self.required("DestinationKeyId")?,
            "kms:ReEncryptTo",
        )?;
        let context = self.context("DestinationEncryptionContext")?;
        let blob = seal(self.root, &target, &context, &plaintext)?;
        Ok(json!({
            "CiphertextBlob": crate::base64::std_encode(&blob),
            "SourceKeyId": source.arn,
            "KeyId": target.arn,
            "SourceEncryptionAlgorithm": ALGORITHM,
            "DestinationEncryptionAlgorithm": ALGORITHM,
        }))
    }

    fn generate_data_key(&self, with_plaintext: bool) -> ApiResult {
        let size = match (self.str_param("KeySpec"), self.input.get("NumberOfBytes")) {
            ("", None) => {
                return Err(err(
                    "ValidationException",
                    "either KeySpec or NumberOfBytes is required",
                ))
            }
            (_, Some(_)) if !self.str_param("KeySpec").is_empty() => {
                return Err(err(
                    "ValidationException",
                    "KeySpec and NumberOfBytes are mutually exclusive",
                ))
            }
            ("AES_256", None) => 32,
            ("AES_128", None) => 16,
            ("", Some(n)) => match n.as_u64() {
                Some(n @ 1..=1024) => n as usize,
                _ => {
                    return Err(err(
                        "ValidationException",
                        "NumberOfBytes must be 1 to 1024",
                    ))
                }
            },
            _ => {
                return Err(err(
                    "ValidationException",
                    "KeySpec must be AES_256 or AES_128",
                ))
            }
        };
        let action = if with_plaintext {
            "kms:GenerateDataKey"
        } else {
            "kms:GenerateDataKeyWithoutPlaintext"
        };
        let keyring = kms::load(self.root);
        let key = self.crypto_key(&keyring, self.required("KeyId")?, action)?;
        let context = self.context("EncryptionContext")?;
        let data_key = random_bytes(size);
        let blob = seal(self.root, &key, &context, &data_key)?;
        let mut out = json!({
            "CiphertextBlob": crate::base64::std_encode(&blob),
            "KeyId": key.arn,
        });
        if with_plaintext {
            out["Plaintext"] = Value::String(crate::base64::std_encode(&data_key));
        }
        Ok(out)
    }

    /// Decrypts `blob`, checking the optional expected key and the context.
    fn open(
        &self,
        blob: &[u8],
        key_param: &str,
        context_param: &str,
        action: &'static str,
    ) -> Result<(KmsKey, Vec<u8>), Response> {
        let invalid = || err("InvalidCiphertextException", "the ciphertext is invalid");
        let (key_id, frame) = split_blob(blob).ok_or_else(invalid)?;
        let keyring = kms::load(self.root);
        let key = self.crypto_key(&keyring, key_id, action)?;
        if let Some(expected) = self.input.get(key_param).and_then(Value::as_str) {
            let matches = keyring
                .find_key(expected)
                .is_some_and(|k| k.key_id == key.key_id);
            if !matches {
                return Err(err(
                    "IncorrectKeyException",
                    "the key ID in the request does not identify the key that encrypted the ciphertext",
                ));
            }
        }
        let context = self.context(context_param)?;
        let material = load_material(self.root).keys.get(&key.key_id).cloned();
        let material = material
            .and_then(|m| crate::base64::std_decode(&m))
            .and_then(|m| <[u8; 32]>::try_from(m).ok())
            .ok_or_else(invalid)?;
        let plaintext = crate::sse_c::decrypt_frames(&context_key(&material, &context), frame)
            .ok_or_else(invalid)?;
        Ok((key, plaintext))
    }

    fn blob(&self, name: &str) -> Result<Vec<u8>, Response> {
        crate::base64::std_decode(self.required(name)?).ok_or_else(|| {
            err(
                "ValidationException",
                &format!("{name} is not valid base64"),
            )
        })
    }

    fn context(&self, name: &str) -> Result<Map<String, Value>, Response> {
        match self.input.get(name) {
            None | Some(Value::Null) => Ok(Map::new()),
            Some(Value::Object(map)) if map.values().all(Value::is_string) => Ok(map.clone()),
            Some(_) => Err(err(
                "ValidationException",
                &format!("{name} must map strings to strings"),
            )),
        }
    }
}

fn usable(key: &KmsKey) -> Result<(), Response> {
    match key.key_state.as_str() {
        ENABLED => Ok(()),
        DISABLED => Err(err(
            "DisabledException",
            &format!("{} is disabled.", key.arn),
        )),
        state => Err(err(
            "KMSInvalidStateException",
            &format!("{} is {state}", key.arn),
        )),
    }
}

/// Encrypts under `key`, creating its key material on first use.
fn seal(
    root: &Path,
    key: &KmsKey,
    context: &Map<String, Value>,
    plaintext: &[u8],
) -> Result<Vec<u8>, Response> {
    let mut materials = load_material(root);
    let material = match materials
        .keys
        .get(&key.key_id)
        .and_then(|m| crate::base64::std_decode(m))
        .and_then(|m| <[u8; 32]>::try_from(m).ok())
    {
        Some(material) => material,
        None => {
            let material: [u8; 32] = random_bytes(32).try_into().expect("32 bytes");
            materials
                .keys
                .insert(key.key_id.clone(), crate::base64::std_encode(&material));
            let bytes = serde_json::to_vec_pretty(&materials).unwrap_or_default();
            write_file(root, MATERIAL_FILE, &bytes).map_err(|_| internal())?;
            material
        }
    };
    let frame = crate::sse_c::encrypt_frame(&context_key(&material, context), plaintext);
    let mut blob = Vec::with_capacity(BLOB_MAGIC.len() + 1 + key.key_id.len() + frame.len());
    blob.extend_from_slice(BLOB_MAGIC);
    blob.push(key.key_id.len() as u8);
    blob.extend_from_slice(key.key_id.as_bytes());
    blob.extend_from_slice(&frame);
    Ok(blob)
}

fn split_blob(blob: &[u8]) -> Option<(&str, &[u8])> {
    let rest = blob.strip_prefix(BLOB_MAGIC)?;
    let (&len, rest) = rest.split_first()?;
    let len = len as usize;
    let key_id = std::str::from_utf8(rest.get(..len)?).ok()?;
    Some((key_id, &rest[len..]))
}

/// Binds the encryption context: `serde_json::Map` is sorted, so the JSON form
/// is canonical.
fn context_key(material: &[u8; 32], context: &Map<String, Value>) -> [u8; 32] {
    let mut mac = Hmac::<Sha256>::new_from_slice(material).expect("HMAC accepts any key length");
    mac.update(b"devcloud-kms-v1\n");
    mac.update(Value::Object(context.clone()).to_string().as_bytes());
    mac.finalize().into_bytes().into()
}

fn random_bytes(len: usize) -> Vec<u8> {
    let mut out = Vec::with_capacity(len + 32);
    while out.len() < len {
        out.extend(hex::decode(random_hex("kms-material")).unwrap_or_default());
    }
    out.truncate(len);
    out
}

fn load_material(root: &Path) -> MaterialFile {
    std::fs::read(root.join(MATERIAL_FILE))
        .ok()
        .and_then(|b| serde_json::from_slice(&b).ok())
        .unwrap_or_default()
}

fn save_keyring(root: &Path, keyring: &Keyring) -> Result<(), Response> {
    kms::save(root, keyring).map_err(|_| internal())
}

fn internal() -> Response {
    let mut response = err("KMSInternalException", "could not update the local keyring");
    response.status = 500;
    response
}

/// RFC3339 seconds -> epoch seconds, as the KMS JSON protocol sends timestamps.
fn epoch(value: &str) -> i64 {
    crate::time_fmt::parse_rfc3339(value).map_or(0, |(secs, _)| secs)
}

fn metadata(key: &KmsKey) -> Value {
    let mut out = json!({
        "AWSAccountId": OWNER_ACCOUNT,
        "KeyId": key.key_id,
        "Arn": key.arn,
        "CreationDate": epoch(&key.creation_date),
        "Enabled": key.key_state == ENABLED,
        "Description": key.description,
        "KeyUsage": "ENCRYPT_DECRYPT",
        "KeyState": key.key_state,
        "Origin": "AWS_KMS",
        "KeyManager": key.key_manager,
        "CustomerMasterKeySpec": ALGORITHM,
        "KeySpec": ALGORITHM,
        "EncryptionAlgorithms": [ALGORITHM],
        "MultiRegion": false,
    });
    if let Some(date) = &key.deletion_date {
        out["DeletionDate"] = json!(epoch(date));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn blobs_bind_the_key_id_and_context() {
        let material = [7u8; 32];
        let mut ctx = Map::new();
        ctx.insert("purpose".to_string(), json!("test"));
        let frame = crate::sse_c::encrypt_frame(&context_key(&material, &ctx), b"secret");
        let mut blob = BLOB_MAGIC.to_vec();
        blob.push(3);
        blob.extend_from_slice(b"abc");
        blob.extend_from_slice(&frame);
        let (key_id, frame) = split_blob(&blob).unwrap();
        assert_eq!(key_id, "abc");
        assert_eq!(
            crate::sse_c::decrypt_frames(&context_key(&material, &ctx), frame).unwrap(),
            b"secret"
        );
        assert!(
            crate::sse_c::decrypt_frames(&context_key(&material, &Map::new()), frame).is_none()
        );
        assert!(split_blob(b"DCKMS1").is_none());
        assert!(split_blob(b"nope").is_none());
    }
}
