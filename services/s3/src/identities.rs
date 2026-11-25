//! Local multi-account IAM users/roles and STS `AssumeRole` sessions.
//!
//! The registry lives in two files at the storage root (files, not directories,
//! so bucket listing never sees them):
//! - `.iam.json`: users (access key, secret, account, identity policies) and
//!   roles (account, trust policy, identity policies, max session duration),
//!   replaced as a whole with `PUT /_iam/config`.
//! - `.iam-sessions.json`: temporary credentials issued by
//!   `POST /_iam/assume-role`, each with its role, optional session policy, and
//!   expiry.
//!
//! In `strict` mode a request signed with a registry access key is verified with
//! that key's secret and authorized as that principal (see `iam.rs`); session
//! credentials also need a matching `x-amz-security-token` and must not be
//! expired. The configured access key stays the bucket owner in account
//! `000000000000`. In `relaxed` mode every request is the owner and trust
//! policies are not checked. Secrets are never logged and `GET /_iam/config`
//! omits them.

use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

use crate::http::{AuthConfig, Request, Response};
use crate::iam::Caller;

/// The account that owns every local bucket (and the configured credential).
pub const OWNER_ACCOUNT: &str = "000000000000";
pub(crate) const IAM_PREFIX: &str = "/_iam/";
pub(crate) const CONFIG_PATH: &str = "/_iam/config";
pub(crate) const ASSUME_ROLE_PATH: &str = "/_iam/assume-role";
const CONFIG_FILE: &str = ".iam.json";
const SESSIONS_FILE: &str = ".iam-sessions.json";
const MIN_SESSION_SECONDS: i64 = 900;
const MAX_SESSION_SECONDS: i64 = 43_200;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct IamConfig {
    #[serde(default)]
    pub users: Vec<IamUser>,
    #[serde(default)]
    pub roles: Vec<IamRole>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct IamUser {
    pub user_name: String,
    #[serde(default = "owner_account")]
    pub account: String,
    pub access_key_id: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub secret_access_key: String,
    #[serde(default)]
    pub policies: Vec<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct IamRole {
    pub role_name: String,
    #[serde(default = "owner_account")]
    pub account: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub assume_role_policy_document: Option<Value>,
    #[serde(default)]
    pub policies: Vec<Value>,
    #[serde(default = "default_max_session")]
    pub max_session_duration: i64,
}

impl IamUser {
    fn arn(&self) -> String {
        format!("arn:aws:iam::{}:user/{}", self.account, self.user_name)
    }
}

impl IamRole {
    fn arn(&self) -> String {
        format!("arn:aws:iam::{}:role/{}", self.account, self.role_name)
    }
}

fn owner_account() -> String {
    OWNER_ACCOUNT.to_string()
}

fn default_max_session() -> i64 {
    3600
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct StoredSession {
    access_key_id: String,
    secret_access_key: String,
    session_token: String,
    role_arn: String,
    session_name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    policy: Option<Value>,
    /// Unix seconds.
    expiration: i64,
}

#[derive(Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct SessionFile {
    #[serde(default)]
    sessions: Vec<StoredSession>,
}

/// An authenticated IAM user or assumed-role session.
#[derive(Debug, Clone, PartialEq)]
pub struct Principal {
    pub access_key_id: String,
    pub account: String,
    /// The user ARN, or `arn:aws:sts::<account>:assumed-role/<role>/<session>`.
    pub arn: String,
    /// `aws:PrincipalArn`: the user ARN, or the role ARN for a session.
    pub principal_arn: String,
    /// `aws:username` (users only).
    pub user_name: String,
    pub identity_policies: Vec<Value>,
    pub session_policy: Option<Value>,
}

impl Principal {
    /// Strings a policy `Principal` / `NotPrincipal` entry can name to match.
    pub(crate) fn principal_candidates(&self) -> Vec<String> {
        vec![
            self.arn.clone(),
            self.principal_arn.clone(),
            format!("arn:aws:iam::{}:root", self.account),
            self.account.clone(),
        ]
    }
}

/// A registry credential: the secret to verify the signature with, the
/// principal it authenticates as, and the session token/expiry for sessions.
pub(crate) struct ResolvedCredential {
    pub(crate) secret: String,
    pub(crate) principal: Principal,
    session: Option<(String, i64)>,
}

impl ResolvedCredential {
    /// The verification settings for this credential: `auth` with its key.
    pub(crate) fn auth_config(&self, auth: &AuthConfig) -> AuthConfig {
        AuthConfig {
            auth_mode: auth.auth_mode.clone(),
            access_key_id: self.principal.access_key_id.clone(),
            secret_access_key: self.secret.clone(),
            region: auth.region.clone(),
        }
    }

    /// Session credentials need the issued token and must not be expired.
    pub(crate) fn check_session(&self, req: &Request) -> Result<(), &'static str> {
        let Some((token, expiration)) = &self.session else {
            return Ok(());
        };
        let sent = match req.header("x-amz-security-token") {
            "" => req
                .query
                .get("X-Amz-Security-Token")
                .map(String::as_str)
                .unwrap_or(""),
            value => value,
        };
        if !crate::http::constant_time_eq(sent.as_bytes(), token.as_bytes()) {
            return Err("InvalidToken");
        }
        if unix_now() >= *expiration {
            return Err("ExpiredToken");
        }
        Ok(())
    }
}

/// Looks `access_key_id` up among registry users, then issued sessions.
pub(crate) fn resolve_access_key(root: &Path, access_key_id: &str) -> Option<ResolvedCredential> {
    let config = load_config(root);
    if let Some(user) = config
        .users
        .iter()
        .find(|u| u.access_key_id == access_key_id)
    {
        return Some(ResolvedCredential {
            secret: user.secret_access_key.clone(),
            principal: Principal {
                access_key_id: user.access_key_id.clone(),
                account: user.account.clone(),
                arn: user.arn(),
                principal_arn: user.arn(),
                user_name: user.user_name.clone(),
                identity_policies: policy_docs(&user.policies),
                session_policy: None,
            },
            session: None,
        });
    }
    let sessions = load_sessions(root);
    let session = sessions
        .sessions
        .into_iter()
        .find(|s| s.access_key_id == access_key_id)?;
    // A deleted role revokes its sessions.
    let role = config.roles.iter().find(|r| r.arn() == session.role_arn)?;
    Some(ResolvedCredential {
        secret: session.secret_access_key,
        principal: Principal {
            access_key_id: session.access_key_id,
            account: role.account.clone(),
            arn: format!(
                "arn:aws:sts::{}:assumed-role/{}/{}",
                role.account, role.role_name, session.session_name
            ),
            principal_arn: role.arn(),
            user_name: String::new(),
            identity_policies: policy_docs(&role.policies),
            session_policy: session.policy,
        },
        session: Some((session.session_token, session.expiration)),
    })
}

/// Serves `/_iam/*`. Authorization (owner-only except assume-role) has already
/// been applied by `iam::authorize_request`.
pub(crate) fn handle(root: &Path, req: &Request, auth: &AuthConfig, caller: &Caller) -> Response {
    match (req.method.as_str(), req.path.as_str()) {
        ("GET", CONFIG_PATH) => {
            let mut config = load_config(root);
            for user in &mut config.users {
                user.secret_access_key.clear();
            }
            json_response(200, &serde_json::to_value(config).unwrap_or(Value::Null))
        }
        ("PUT", CONFIG_PATH) => put_config(root, req, auth),
        ("POST", ASSUME_ROLE_PATH) => assume_role(root, req, auth, caller),
        (_, CONFIG_PATH) | (_, ASSUME_ROLE_PATH) => {
            json_error(405, "MethodNotAllowed", "method is not allowed")
        }
        _ => json_error(404, "NotFound", "unknown IAM endpoint"),
    }
}

fn put_config(root: &Path, req: &Request, auth: &AuthConfig) -> Response {
    let mut config: IamConfig = match serde_json::from_slice(&req.body) {
        Ok(c) => c,
        Err(_) => return json_error(400, "MalformedPolicyDocument", "invalid IAM config JSON"),
    };
    if let Err(message) = validate_config(&mut config, crate::auth::configured_access_key(auth)) {
        return json_error(400, "ValidationError", &message);
    }
    let Ok(bytes) = serde_json::to_vec_pretty(&config) else {
        return json_error(500, "InternalError", "could not encode IAM config");
    };
    if write_file(root, CONFIG_FILE, &bytes).is_err() {
        return json_error(500, "InternalError", "could not write IAM config");
    }
    json_response(
        200,
        &json!({"Users": config.users.len(), "Roles": config.roles.len()}),
    )
}

fn validate_config(config: &mut IamConfig, owner_key: &str) -> Result<(), String> {
    let mut keys = std::collections::BTreeSet::new();
    for user in &mut config.users {
        if !valid_name(&user.user_name) {
            return Err(format!("invalid UserName {:?}", user.user_name));
        }
        if !valid_account(&user.account) {
            return Err(format!("invalid Account for user {}", user.user_name));
        }
        if user.access_key_id.is_empty() || user.secret_access_key.is_empty() {
            return Err(format!(
                "user {} needs AccessKeyId and SecretAccessKey",
                user.user_name
            ));
        }
        if user.access_key_id == owner_key || !keys.insert(user.access_key_id.clone()) {
            return Err(format!(
                "AccessKeyId of user {} is already in use",
                user.user_name
            ));
        }
        user.policies = normalize_policies(&user.policies)?;
    }
    let mut roles = std::collections::BTreeSet::new();
    for role in &mut config.roles {
        if !valid_name(&role.role_name) || !valid_account(&role.account) {
            return Err(format!("invalid role {:?}", role.role_name));
        }
        if !roles.insert(role.arn()) {
            return Err(format!("duplicate role {}", role.arn()));
        }
        if !(MIN_SESSION_SECONDS..=MAX_SESSION_SECONDS).contains(&role.max_session_duration) {
            return Err(format!(
                "MaxSessionDuration of role {} must be {MIN_SESSION_SECONDS}..={MAX_SESSION_SECONDS}",
                role.role_name
            ));
        }
        role.policies = normalize_policies(&role.policies)?;
        if let Some(doc) = &role.assume_role_policy_document {
            role.assume_role_policy_document = Some(policy_doc(doc).ok_or_else(|| {
                format!(
                    "invalid AssumeRolePolicyDocument for role {}",
                    role.role_name
                )
            })?);
        }
    }
    Ok(())
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct AssumeRoleInput {
    role_arn: String,
    role_session_name: String,
    #[serde(default)]
    policy: Option<Value>,
    #[serde(default)]
    duration_seconds: Option<i64>,
}

fn assume_role(root: &Path, req: &Request, auth: &AuthConfig, caller: &Caller) -> Response {
    let input: AssumeRoleInput = match serde_json::from_slice(&req.body) {
        Ok(i) => i,
        Err(_) => return json_error(400, "ValidationError", "invalid AssumeRole JSON"),
    };
    let config = load_config(root);
    let Some(role) = config.roles.iter().find(|r| r.arn() == input.role_arn) else {
        return json_error(404, "NoSuchEntity", "role does not exist");
    };
    if !valid_session_name(&input.role_session_name) {
        return json_error(400, "ValidationError", "invalid RoleSessionName");
    }
    let duration = input.duration_seconds.unwrap_or(3600);
    if duration < MIN_SESSION_SECONDS || duration > role.max_session_duration {
        return json_error(
            400,
            "ValidationError",
            "DurationSeconds exceeds the role's MaxSessionDuration or is below 900",
        );
    }
    let policy = match &input.policy {
        None => None,
        Some(doc) => match policy_doc(doc) {
            Some(doc) => Some(doc),
            None => return json_error(400, "MalformedPolicyDocument", "invalid session Policy"),
        },
    };
    if auth.auth_mode.eq_ignore_ascii_case("strict") && !may_assume(role, req, caller) {
        return json_error(
            403,
            "AccessDenied",
            "not authorized to perform sts:AssumeRole",
        );
    }

    let now = unix_now();
    let seed = random_hex(&input.role_arn);
    let session = StoredSession {
        access_key_id: format!("ASIA{}", seed[..16].to_ascii_uppercase()),
        secret_access_key: crate::base64::std_encode(&hex::decode(&seed).unwrap_or_default())[..40]
            .to_string(),
        session_token: crate::base64::std_encode(
            format!("{}{}", random_hex("token-a"), random_hex("token-b")).as_bytes(),
        ),
        role_arn: role.arn(),
        session_name: input.role_session_name.clone(),
        policy,
        expiration: now + duration,
    };
    let mut sessions = load_sessions(root);
    sessions.sessions.retain(|s| s.expiration > now);
    sessions.sessions.push(session.clone());
    let Ok(bytes) = serde_json::to_vec_pretty(&sessions) else {
        return json_error(500, "InternalError", "could not encode sessions");
    };
    if write_file(root, SESSIONS_FILE, &bytes).is_err() {
        return json_error(500, "InternalError", "could not write sessions");
    }
    json_response(
        200,
        &json!({
            "Credentials": {
                "AccessKeyId": session.access_key_id,
                "SecretAccessKey": session.secret_access_key,
                "SessionToken": session.session_token,
                "Expiration": crate::time_fmt::rfc3339_seconds_from_unix(session.expiration),
            },
            "AssumedRoleUser": {
                "Arn": format!(
                    "arn:aws:sts::{}:assumed-role/{}/{}",
                    role.account, role.role_name, session.session_name
                ),
                "AssumedRoleId": format!("AROA{}:{}", &seed[16..32].to_ascii_uppercase(), session.session_name),
            },
        }),
    )
}

/// The role's trust policy must allow the caller; a caller from another
/// account also needs `sts:AssumeRole` on the role in its own policies.
fn may_assume(role: &IamRole, req: &Request, caller: &Caller) -> bool {
    let Some(trust) = &role.assume_role_policy_document else {
        return false;
    };
    let role_arn = role.arn();
    if !crate::iam::trust_allows(trust, &role_arn, req, caller) {
        return false;
    }
    match caller {
        Caller::Iam(p) if p.account != role.account => {
            crate::iam::identity_allows(p, req, "sts:AssumeRole", &role_arn, caller)
        }
        _ => true,
    }
}

fn load_config(root: &Path) -> IamConfig {
    std::fs::read(root.join(CONFIG_FILE))
        .ok()
        .and_then(|b| serde_json::from_slice(&b).ok())
        .unwrap_or_default()
}

fn load_sessions(root: &Path) -> SessionFile {
    std::fs::read(root.join(SESSIONS_FILE))
        .ok()
        .and_then(|b| serde_json::from_slice(&b).ok())
        .unwrap_or_default()
}

/// Writes `root/name` through a temporary file so readers never see a partial file.
pub(crate) fn write_file(root: &Path, name: &str, bytes: &[u8]) -> std::io::Result<()> {
    std::fs::create_dir_all(root)?;
    let tmp = root.join(format!("{name}.tmp"));
    std::fs::write(&tmp, bytes)?;
    std::fs::rename(tmp, root.join(name))
}

/// A policy given as a JSON object or as a JSON string holding one.
fn policy_doc(value: &Value) -> Option<Value> {
    let doc = match value {
        Value::String(s) => serde_json::from_str::<Value>(s).ok()?,
        other => other.clone(),
    };
    match doc.get("Statement") {
        Some(Value::Array(_)) | Some(Value::Object(_)) => Some(doc),
        _ => None,
    }
}

fn policy_docs(values: &[Value]) -> Vec<Value> {
    values.iter().filter_map(policy_doc).collect()
}

fn normalize_policies(values: &[Value]) -> Result<Vec<Value>, String> {
    values
        .iter()
        .map(|v| policy_doc(v).ok_or_else(|| "invalid policy document".to_string()))
        .collect()
}

fn valid_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 64
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || "+=,.@_-".contains(c))
}

fn valid_session_name(name: &str) -> bool {
    (2..=64).contains(&name.len())
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || "+=,.@_-".contains(c))
}

fn valid_account(account: &str) -> bool {
    account.len() == 12 && account.bytes().all(|b| b.is_ascii_digit())
}

pub(crate) fn unix_now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// 64 hex chars that differ per call (clock, process, counter, and `salt`).
pub(crate) fn random_hex(salt: &str) -> String {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let seed = format!(
        "{nanos}:{}:{}:{salt}",
        std::process::id(),
        COUNTER.fetch_add(1, Ordering::Relaxed)
    );
    hex::encode(Sha256::digest(seed.as_bytes()))
}

pub(crate) fn json_response(status: u16, value: &Value) -> Response {
    let mut response = Response::with_body(status, serde_json::to_vec(value).unwrap_or_default());
    response
        .headers
        .insert("Content-Type".to_string(), "application/json".to_string());
    response
}

pub(crate) fn json_error(status: u16, code: &str, message: &str) -> Response {
    json_response(status, &json!({"__type": code, "message": message}))
}
