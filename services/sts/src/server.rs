//! STS over the AWS Query protocol: `AssumeRole` and `GetCallerIdentity`.
//!
//! Stateless. `AssumeRole` validates its inputs the way AWS does, accepts an
//! inline session `Policy` (it must be JSON) without evaluating it, and issues
//! fresh temporary credentials. devcloud services in `relaxed` auth mode
//! accept those credentials; in `strict` mode they only accept the configured
//! static key, so temporary credentials are rejected there.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use sha2::{Digest, Sha256};

const XMLNS: &str = "https://sts.amazonaws.com/doc/2011-06-15/";
const DEFAULT_DURATION_SECONDS: i64 = 3600;
const MIN_DURATION_SECONDS: i64 = 900;
const MAX_DURATION_SECONDS: i64 = 43200;
const MAX_POLICY_CHARS: usize = 2048;

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
        Server { config }
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
        let action = params.get("Action").map(String::as_str).unwrap_or("");
        match action {
            "AssumeRole" => self.assume_role(params),
            "GetCallerIdentity" => self.get_caller_identity(),
            "" => error_outcome(400, "MissingAction", "Missing Action"),
            other => error_outcome(
                400,
                "InvalidAction",
                &format!("Could not find operation {other} for version 2011-06-15"),
            ),
        }
    }

    fn assume_role(&self, params: &BTreeMap<String, String>) -> Outcome {
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
        let policy = get("Policy");
        let mut packed_policy_size = None;
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
        }

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;
        let access_key_id = format!("ASIA{}", &unique_hex("akid")[..16].to_ascii_uppercase());
        let secret_access_key = format!("{}{}", unique_hex("secret"), &unique_hex("secret2")[..8]);
        let session_token = format!(
            "devcloud-sts-{}{}",
            unique_hex("token"),
            unique_hex("token2")
        );
        let role_id = format!("AROA{}", &unique_hex(role_arn)[..17].to_ascii_uppercase());

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
