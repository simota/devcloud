//! STS over the AWS Query protocol: `AssumeRole` and `GetCallerIdentity`.
//!
//! `AssumeRole` validates its inputs the way AWS does, accepts an inline
//! session `Policy` (it must be JSON), and issues fresh temporary credentials.
//! devcloud services in `relaxed` auth mode accept those credentials without
//! looking at them. In `strict` mode the STS server also remembers each issued
//! session in memory (until it expires or the process stops) and accepts
//! requests signed with it: the signature is verified with the session's
//! secret, `X-Amz-Security-Token` must match, and the session must not be
//! expired. Such requests are limited by the session policy: `AssumeRole`
//! (role chaining) needs an `Allow` for `sts:AssumeRole` on the target role
//! and no `Deny`; `GetCallerIdentity` needs no permission, as on AWS. Role
//! identity policies are not emulated, so without a session policy a session
//! may do anything. Other services in `strict` mode only accept their
//! configured static key, so temporary credentials are rejected there.

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::policy::{self, Decision, RequestContext};

const XMLNS: &str = "https://sts.amazonaws.com/doc/2011-06-15/";
const DEFAULT_DURATION_SECONDS: i64 = 3600;
const MIN_DURATION_SECONDS: i64 = 900;
const MAX_DURATION_SECONDS: i64 = 43200;
const MAX_POLICY_CHARS: usize = 2048;
/// AWS caps sessions assumed with temporary credentials at one hour.
const MAX_CHAINED_DURATION_SECONDS: i64 = 3600;

#[derive(Clone, Debug, Default)]
pub struct Config {
    pub addr: String,
    pub region: String,
    pub account_id: String,
    pub auth_mode: String,
    pub access_key_id: String,
    pub secret_access_key: String,
}

/// One operation result: HTTP status and XML body.
pub struct Outcome {
    pub status: u16,
    pub body: String,
}

pub fn error_outcome(status: u16, code: &str, message: &str) -> Outcome {
    let kind = if status >= 500 { "Receiver" } else { "Sender" };
    Outcome {
        status,
        body: format!(
            "<ErrorResponse xmlns=\"{XMLNS}\"><Error><Type>{kind}</Type><Code>{}</Code><Message>{}</Message></Error><RequestId>{}</RequestId></ErrorResponse>",
            xml_escape(code),
            xml_escape(message),
            request_id(),
        ),
    }
}

fn validation(message: &str) -> Outcome {
    error_outcome(400, "ValidationError", message)
}

pub struct Server {
    config: Config,
    /// Sessions issued in `strict` mode, by access key id.
    pub(crate) sessions: Mutex<HashMap<String, Session>>,
}

/// Temporary credentials issued by `AssumeRole` and what they are limited by.
#[derive(Clone, Debug)]
pub struct Session {
    pub access_key_id: String,
    pub secret_access_key: String,
    pub session_token: String,
    pub account: String,
    pub role_name: String,
    pub role_arn: String,
    pub session_name: String,
    pub policy: Option<Value>,
    /// Unix seconds.
    pub expiration: i64,
}

impl Session {
    /// `arn:aws:sts::<account>:assumed-role/<role>/<session>`.
    pub fn arn(&self) -> String {
        format!(
            "arn:aws:sts::{}:assumed-role/{}/{}",
            self.account, self.role_name, self.session_name
        )
    }
}

pub(crate) fn unix_now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

fn is_strict(mode: &str) -> bool {
    mode.eq_ignore_ascii_case("strict")
}

static COUNTER: AtomicU64 = AtomicU64::new(0);

/// 32 hex chars, unique per call within the process and across restarts
/// (time + counter + pid hashed). Not a secret: these credentials only gate
/// local emulators.
fn unique_hex(tag: &str) -> String {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let digest = Sha256::digest(format!("{tag}:{now}:{n}:{}", std::process::id()).as_bytes());
    hex::encode(digest)[..32].to_string()
}

fn request_id() -> String {
    let h = unique_hex("request");
    format!(
        "{}-{}-{}-{}-{}",
        &h[0..8],
        &h[8..12],
        &h[12..16],
        &h[16..20],
        &h[20..32]
    )
}

fn xml_escape(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for c in value.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&apos;"),
            _ => out.push(c),
        }
    }
    out
}

/// RFC 3339 UTC with second precision (`2026-11-03T10:00:00Z`).
fn rfc3339(secs: i64) -> String {
    let days = secs.div_euclid(86_400);
    let sod = secs.rem_euclid(86_400);
    // Howard Hinnant's civil_from_days.
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = yoe + era * 400 + if m <= 2 { 1 } else { 0 };
    format!(
        "{y:04}-{m:02}-{d:02}T{:02}:{:02}:{:02}Z",
        sod / 3600,
        (sod % 3600) / 60,
        sod % 60
    )
}

fn valid_session_name(name: &str) -> bool {
    (2..=64).contains(&name.len())
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"_+=,.@-".contains(&b))
}

/// `arn:aws:iam::<account>:role/<path/>name` → (account, role name).
fn parse_role_arn(arn: &str) -> Option<(&str, &str)> {
    let rest = arn.strip_prefix("arn:")?;
    let mut parts = rest.splitn(5, ':');
    let _partition = parts.next()?;
    let service = parts.next()?;
    let _region = parts.next()?;
    let account = parts.next()?;
    let resource = parts.next()?;
    let role_path = resource.strip_prefix("role/")?;
    let name = role_path.rsplit('/').next()?;
    if service != "iam" || account.is_empty() || name.is_empty() {
        return None;
    }
    Some((account, name))
}

impl Server {
    pub fn new(config: Config) -> Self {
        Server {
            config,
            sessions: Mutex::new(HashMap::new()),
        }
    }

    /// The session issued with `access_key_id`, if it is still remembered
    /// (only sessions issued in `strict` mode are).
    pub fn session(&self, access_key_id: &str) -> Option<Session> {
        self.sessions
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(access_key_id)
            .cloned()
    }

    pub fn config(&self) -> &Config {
        &self.config
    }

    fn account_id(&self) -> &str {
        if self.config.account_id.is_empty() {
            "000000000000"
        } else {
            &self.config.account_id
        }
    }

    /// `params` are the merged query-string and form-body parameters.
    pub fn dispatch(&self, params: &BTreeMap<String, String>) -> Outcome {
        self.dispatch_as(params, None)
    }

    /// Like `dispatch`, for a request signed with the temporary credentials
    /// of `caller` (`strict` mode); its session policy limits the request.
    pub fn dispatch_as(
        &self,
        params: &BTreeMap<String, String>,
        caller: Option<&Session>,
    ) -> Outcome {
        let action = params.get("Action").map(String::as_str).unwrap_or("");
        match action {
            "AssumeRole" => self.assume_role(params, caller),
            "GetCallerIdentity" => self.get_caller_identity(),
            "" => error_outcome(400, "MissingAction", "Missing Action"),
            other => error_outcome(
                400,
                "InvalidAction",
                &format!("Could not find operation {other} for version 2011-06-15"),
            ),
        }
    }

    fn assume_role(&self, params: &BTreeMap<String, String>, caller: Option<&Session>) -> Outcome {
        let get = |k: &str| params.get(k).map(String::as_str).unwrap_or("");
        let role_arn = get("RoleArn");
        let session_name = get("RoleSessionName");
        if role_arn.is_empty() {
            return validation("1 validation error detected: Value null at 'roleArn' failed to satisfy constraint: Member must not be null");
        }
        if session_name.is_empty() {
            return validation("1 validation error detected: Value null at 'roleSessionName' failed to satisfy constraint: Member must not be null");
        }
        if !valid_session_name(session_name) {
            return validation(
                "1 validation error detected: Value at 'roleSessionName' failed to satisfy constraint: Member must satisfy regular expression pattern: [\\w+=,.@-]*, with length between 2 and 64",
            );
        }
        let Some((account, role_name)) = parse_role_arn(role_arn) else {
            return validation(&format!("{role_arn} is invalid"));
        };
        let duration = match params.get("DurationSeconds") {
            None => DEFAULT_DURATION_SECONDS,
            Some(v) => match v.parse::<i64>() {
                Ok(d) if (MIN_DURATION_SECONDS..=MAX_DURATION_SECONDS).contains(&d) => d,
                _ => {
                    return validation(
                        "1 validation error detected: Value at 'durationSeconds' failed to satisfy constraint: Member must have value between 900 and 43200",
                    )
                }
            },
        };
        if caller.is_some() && duration > MAX_CHAINED_DURATION_SECONDS {
            return validation("The requested DurationSeconds exceeds the 1 hour session limit for roles assumed by role chaining.");
        }
        let policy = get("Policy");
        let mut packed_policy_size = None;
        let mut session_policy = None;
        if !policy.is_empty() {
            if policy.chars().count() > MAX_POLICY_CHARS
                || serde_json::from_str::<serde_json::Value>(policy).is_err()
            {
                return error_outcome(
                    400,
                    "MalformedPolicyDocument",
                    "The policy is not in the valid JSON format.",
                );
            }
            // AWS reports the packed size as a percentage of the limit.
            packed_policy_size = Some((policy.len() * 100 / MAX_POLICY_CHARS).max(1));
            session_policy = serde_json::from_str::<Value>(policy).ok();
        }
        if let Some(caller) = caller {
            if let Some(denied) = authorize(caller, "sts:AssumeRole", role_arn, session_name) {
                return denied;
            }
        }

        let now = unix_now();
        let access_key_id = format!("ASIA{}", &unique_hex("akid")[..16].to_ascii_uppercase());
        let secret_access_key = format!("{}{}", unique_hex("secret"), &unique_hex("secret2")[..8]);
        let session_token = format!(
            "devcloud-sts-{}{}",
            unique_hex("token"),
            unique_hex("token2")
        );
        let role_id = format!("AROA{}", &unique_hex(role_arn)[..17].to_ascii_uppercase());
        if is_strict(&self.config.auth_mode) {
            let mut sessions = self.sessions.lock().unwrap_or_else(|e| e.into_inner());
            sessions.retain(|_, s| s.expiration > now);
            sessions.insert(
                access_key_id.clone(),
                Session {
                    access_key_id: access_key_id.clone(),
                    secret_access_key: secret_access_key.clone(),
                    session_token: session_token.clone(),
                    account: account.to_string(),
                    role_name: role_name.to_string(),
                    role_arn: role_arn.to_string(),
                    session_name: session_name.to_string(),
                    policy: session_policy,
                    expiration: now + duration,
                },
            );
        }

        let packed = packed_policy_size
            .map(|n| format!("<PackedPolicySize>{n}</PackedPolicySize>"))
            .unwrap_or_default();
        Outcome {
            status: 200,
            body: format!(
                concat!(
                    "<AssumeRoleResponse xmlns=\"{xmlns}\"><AssumeRoleResult>",
                    "<Credentials><AccessKeyId>{akid}</AccessKeyId>",
                    "<SecretAccessKey>{secret}</SecretAccessKey>",
                    "<SessionToken>{token}</SessionToken>",
                    "<Expiration>{expiration}</Expiration></Credentials>",
                    "<AssumedRoleUser><AssumedRoleId>{role_id}:{session}</AssumedRoleId>",
                    "<Arn>arn:aws:sts::{account}:assumed-role/{role}/{session}</Arn></AssumedRoleUser>",
                    "{packed}",
                    "</AssumeRoleResult><ResponseMetadata><RequestId>{rid}</RequestId>",
                    "</ResponseMetadata></AssumeRoleResponse>"
                ),
                xmlns = XMLNS,
                akid = access_key_id,
                secret = secret_access_key,
                token = session_token,
                expiration = rfc3339(now + duration),
                role_id = role_id,
                session = xml_escape(session_name),
                account = xml_escape(account),
                role = xml_escape(role_name),
                packed = packed,
                rid = request_id(),
            ),
        }
    }

    /// Reports the configured account's root identity. Requests are not
    /// mapped back to an assumed role.
    fn get_caller_identity(&self) -> Outcome {
        let account = xml_escape(self.account_id());
        Outcome {
            status: 200,
            body: format!(
                concat!(
                    "<GetCallerIdentityResponse xmlns=\"{xmlns}\"><GetCallerIdentityResult>",
                    "<Arn>arn:aws:iam::{account}:root</Arn><UserId>{account}</UserId>",
                    "<Account>{account}</Account></GetCallerIdentityResult>",
                    "<ResponseMetadata><RequestId>{rid}</RequestId></ResponseMetadata>",
                    "</GetCallerIdentityResponse>"
                ),
                xmlns = XMLNS,
                account = account,
                rid = request_id(),
            ),
        }
    }
}

/// `None` when `caller`'s session policy (if any) allows `action` on
/// `resource`; otherwise the `AccessDenied` outcome AWS returns.
fn authorize(
    caller: &Session,
    action: &str,
    resource: &str,
    session_name: &str,
) -> Option<Outcome> {
    let doc = caller.policy.as_ref()?;
    let ctx = RequestContext {
        action,
        resource,
        principal_arn: &caller.role_arn,
        principal_account: &caller.account,
        role_session_name: session_name,
    };
    let reason = match policy::evaluate(doc, &ctx) {
        Decision::Allow => return None,
        Decision::Deny => "with an explicit deny in a session policy".to_string(),
        Decision::ImplicitDeny => format!("because no session policy allows the {action} action"),
    };
    Some(error_outcome(
        403,
        "AccessDenied",
        &format!(
            "User: {} is not authorized to perform: {action} on resource: {resource} {reason}",
            caller.arn()
        ),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rfc3339_formats_known_instants() {
        assert_eq!(rfc3339(0), "1970-01-01T00:00:00Z");
        assert_eq!(rfc3339(1_793_793_600), "2026-11-04T12:00:00Z");
    }

    #[test]
    fn role_arn_parsing() {
        assert_eq!(
            parse_role_arn("arn:aws:iam::123456789012:role/svc/site-a"),
            Some(("123456789012", "site-a"))
        );
        assert_eq!(parse_role_arn("arn:aws:iam::123456789012:user/bob"), None);
        assert_eq!(parse_role_arn("arn:aws:s3:::bucket"), None);
        assert_eq!(parse_role_arn("not-an-arn"), None);
    }
}
