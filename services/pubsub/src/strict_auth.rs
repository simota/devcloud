//! Topic/subscription IAM policy storage and `strict` auth mode enforcement
//! for both REST and gRPC.
//!
//! IAM policies set through `:setIamPolicy` are stored in every mode (and
//! persisted to `<storage>/iam-policies.json`); they are only enforced when
//! the auth mode is `strict`:
//!
//! - The configured bearer token is the project owner and may do anything.
//! - Tokens registered with [`Server::add_principal`] authenticate as an IAM
//!   member and are authorized against the topic or subscription policy.
//!   Project-level calls (create/list, snapshots, schemas, detach,
//!   introspection) are owner-only.
//! - Requests without a token are anonymous and only reach resources whose
//!   policy grants the permission to `allUsers`.
//! - Conditional bindings never grant access.

use std::collections::BTreeMap;

use serde_json::{json, Value};

use crate::errors::ApiError;
use crate::paths;
use crate::server::{RestResponse, Server};

pub const TOPICS_GET: &str = "pubsub.topics.get";
pub const TOPICS_UPDATE: &str = "pubsub.topics.update";
pub const TOPICS_DELETE: &str = "pubsub.topics.delete";
pub const TOPICS_PUBLISH: &str = "pubsub.topics.publish";
pub const SUBS_GET: &str = "pubsub.subscriptions.get";
pub const SUBS_UPDATE: &str = "pubsub.subscriptions.update";
pub const SUBS_DELETE: &str = "pubsub.subscriptions.delete";
pub const SUBS_CONSUME: &str = "pubsub.subscriptions.consume";

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Caller {
    Owner,
    Principal(String),
    Anonymous,
}

/// Parses `token=member[,token=member...]` (the `auth.pubsub.principals`
/// config value). Malformed entries are ignored.
pub fn parse_principals(raw: &str) -> Vec<(String, String)> {
    raw.split(',')
        .filter_map(|entry| {
            let (token, member) = entry.split_once('=')?;
            let (token, member) = (token.trim(), member.trim());
            (!token.is_empty() && !member.is_empty())
                .then(|| (token.to_string(), member.to_string()))
        })
        .collect()
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

fn role_grants(role: &str, permission: &str) -> bool {
    match role {
        "roles/owner" | "roles/pubsub.admin" => permission.starts_with("pubsub."),
        "roles/editor" | "roles/pubsub.editor" => {
            permission.starts_with("pubsub.") && !permission.ends_with("IamPolicy")
        }
        "roles/viewer" | "roles/pubsub.viewer" => {
            matches!(permission, TOPICS_GET | SUBS_GET)
        }
        "roles/pubsub.publisher" => permission == TOPICS_PUBLISH,
        "roles/pubsub.subscriber" => permission == SUBS_CONSUME,
        _ => false,
    }
}

fn member_matches(member: &str, caller: &Caller) -> bool {
    match (member, caller) {
        ("allUsers", _) => true,
        (_, Caller::Anonymous) => false,
        ("allAuthenticatedUsers", _) => true,
        (_, Caller::Owner) => false,
        (m, Caller::Principal(p)) => {
            if m == p {
                return true;
            }
            match (m.strip_prefix("domain:"), p.split_once(':')) {
                (Some(domain), Some((_, email))) => email
                    .rsplit_once('@')
                    .is_some_and(|(_, d)| d.eq_ignore_ascii_case(domain)),
                _ => false,
            }
        }
    }
}

fn policy_allows(policy: &Value, caller: &Caller, permission: &str) -> bool {
    let Some(bindings) = policy.get("bindings").and_then(Value::as_array) else {
        return false;
    };
    bindings.iter().any(|b| {
        b.get("condition").is_none_or(Value::is_null)
            && role_grants(
                b.get("role").and_then(Value::as_str).unwrap_or(""),
                permission,
            )
            && b.get("members")
                .and_then(Value::as_array)
                .is_some_and(|ms| {
                    ms.iter()
                        .filter_map(Value::as_str)
                        .any(|m| member_matches(m, caller))
                })
    })
}

fn default_policy() -> Value {
    json!({"version": 1, "bindings": []})
}

/// The `(resource name, permission)` a REST request needs; `Some(None)` for
/// calls any caller may make, `None` for owner-only project-level calls.
fn rest_requirement(method: &str, path: &str) -> Option<Option<(String, &'static str)>> {
    if let Some((project, topic_id, action)) = paths::topic_action_parts(path) {
        let name = paths::topic_name(&project, &topic_id);
        return match action.as_str() {
            "publish" => Some(Some((name, TOPICS_PUBLISH))),
            "getIamPolicy" => Some(Some((name, "pubsub.topics.getIamPolicy"))),
            "setIamPolicy" => Some(Some((name, "pubsub.topics.setIamPolicy"))),
            "testIamPermissions" => Some(None),
            _ => None,
        };
    }
    if let Some((project, sub_id, action)) = paths::subscription_action_parts(path) {
        let name = paths::subscription_name(&project, &sub_id);
        return match action.as_str() {
            "pull" | "acknowledge" | "modifyAckDeadline" | "seek" => {
                Some(Some((name, SUBS_CONSUME)))
            }
            "modifyPushConfig" => Some(Some((name, SUBS_UPDATE))),
            "getIamPolicy" => Some(Some((name, "pubsub.subscriptions.getIamPolicy"))),
            "setIamPolicy" => Some(Some((name, "pubsub.subscriptions.setIamPolicy"))),
            "testIamPermissions" => Some(None),
            _ => None,
        };
    }
    if let Some((project, topic_id)) =
        paths::topic_subscriptions_parts(path).or_else(|| paths::topic_snapshots_parts(path))
    {
        return Some(Some((paths::topic_name(&project, &topic_id), TOPICS_GET)));
    }
    if let Some((project, topic_id)) = paths::topic_name_parts(path) {
        let name = paths::topic_name(&project, &topic_id);
        return match method {
            "GET" => Some(Some((name, TOPICS_GET))),
            "PATCH" => Some(Some((name, TOPICS_UPDATE))),
            "DELETE" => Some(Some((name, TOPICS_DELETE))),
            _ => None,
        };
    }
    if let Some((project, sub_id)) = paths::subscription_name_parts(path) {
        let name = paths::subscription_name(&project, &sub_id);
        return match method {
            "GET" => Some(Some((name, SUBS_GET))),
            "PATCH" => Some(Some((name, SUBS_UPDATE))),
            "DELETE" => Some(Some((name, SUBS_DELETE))),
            _ => None,
        };
    }
    None
}

fn unauthenticated() -> ApiError {
    ApiError::new(401, "UNAUTHENTICATED", "invalid authentication credentials")
}

/// Extracts the token from an `Authorization: Bearer <token>` value.
pub(crate) fn bearer_token(header: &str) -> &str {
    match header.trim().split_once(' ') {
        Some((scheme, token)) if scheme.eq_ignore_ascii_case("bearer") => token.trim(),
        _ => "",
    }
}

impl Server {
    pub fn is_strict_auth(&self) -> bool {
        self.config.auth_mode.trim().eq_ignore_ascii_case("strict")
    }

    /// Registers a strict-mode bearer token that authenticates as IAM
    /// `member` (e.g. `serviceAccount:worker@devcloud.iam.gserviceaccount.com`).
    pub fn add_principal(&mut self, token: impl Into<String>, member: impl Into<String>) {
        self.principals.push((token.into(), member.into()));
    }

    fn strict_caller(&self, token: &str) -> Option<Caller> {
        if token.is_empty() {
            return Some(Caller::Anonymous);
        }
        let owner = self.config.bearer_token.trim();
        if !owner.is_empty() && constant_time_eq(token.as_bytes(), owner.as_bytes()) {
            return Some(Caller::Owner);
        }
        self.principals
            .iter()
            .find(|(t, _)| constant_time_eq(token.as_bytes(), t.as_bytes()))
            .map(|(_, member)| Caller::Principal(member.clone()))
    }

    /// Strict-mode check for a caller token against an optional
    /// `(resource name, permission)` requirement (`None` = owner-only).
    pub(crate) fn authorize_strict(
        &self,
        token: &str,
        requirement: Option<Option<(&str, &str)>>,
    ) -> Result<(), ApiError> {
        let caller = self.strict_caller(token).ok_or_else(unauthenticated)?;
        if caller == Caller::Owner {
            return Ok(());
        }
        let denied = |permission: &str, resource: &str| match &caller {
            Caller::Principal(member) => ApiError::new(
                403,
                "PERMISSION_DENIED",
                format!("{member} does not have {permission} on {resource}"),
            ),
            _ => unauthenticated(),
        };
        let Some(requirement) = requirement else {
            return Err(denied("project-level access", "the project"));
        };
        let Some((name, permission)) = requirement else {
            return Ok(());
        };
        let allowed = self
            .iam_policies
            .get(name)
            .is_some_and(|policy| policy_allows(policy, &caller, permission));
        if allowed {
            Ok(())
        } else {
            Err(denied(permission, name))
        }
    }

    /// REST entry point: the router calls this before dispatch in strict mode.
    pub(crate) fn authorize_rest(
        &self,
        method: &str,
        path: &str,
        authorization: &str,
    ) -> Result<(), ApiError> {
        let requirement = rest_requirement(method, path);
        let requirement = requirement
            .as_ref()
            .map(|r| r.as_ref().map(|(n, p)| (n.as_str(), *p)));
        self.authorize_strict(bearer_token(authorization), requirement)
    }

    /// gRPC entry point; a no-op outside strict mode (gRPC has never checked
    /// credentials in the relaxed modes).
    pub fn authorize_grpc(
        &self,
        metadata: &tonic::metadata::MetadataMap,
        target: Option<(&str, &str)>,
    ) -> Result<(), tonic::Status> {
        if !self.is_strict_auth() {
            return Ok(());
        }
        let header = metadata
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        self.authorize_strict(bearer_token(header), target.map(Some))
            .map_err(|err| match err.status {
                401 => tonic::Status::unauthenticated(err.message),
                _ => tonic::Status::permission_denied(err.message),
            })
    }

    fn iam_policy_file(&self) -> Option<std::path::PathBuf> {
        let dir = &self.config.storage_path;
        (!dir.is_empty()).then(|| std::path::Path::new(dir).join("iam-policies.json"))
    }

    pub(crate) fn load_iam_policies(&mut self) -> Result<(), String> {
        let Some(path) = self.iam_policy_file() else {
            return Ok(());
        };
        match std::fs::read(&path) {
            Ok(data) => {
                self.iam_policies = serde_json::from_slice::<BTreeMap<String, Value>>(&data)
                    .map_err(|e| format!("{}: {e}", path.display()))?;
                Ok(())
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e.to_string()),
        }
    }

    fn write_iam_policies(&self) -> Result<(), ApiError> {
        let Some(path) = self.iam_policy_file() else {
            return Ok(());
        };
        let data = serde_json::to_vec_pretty(&self.iam_policies)
            .map_err(|_| ApiError::internal("encode iam policies"))?;
        let tmp = path.with_extension("json.tmp");
        std::fs::create_dir_all(path.parent().unwrap_or(std::path::Path::new(".")))
            .and_then(|_| std::fs::write(&tmp, data))
            .and_then(|_| std::fs::rename(&tmp, &path))
            .map_err(|_| ApiError::internal("persist iam policies"))
    }

    /// Drops policies of deleted topics/subscriptions so a recreated resource
    /// starts with an empty policy. Called from `persist`.
    pub(crate) fn prune_iam_policies(&mut self) -> Result<(), ApiError> {
        let before = self.iam_policies.len();
        let (topics, subs) = (&self.topics, &self.subscriptions);
        self.iam_policies
            .retain(|name, _| topics.contains_key(name) || subs.contains_key(name));
        if self.iam_policies.len() != before {
            self.write_iam_policies()?;
        }
        Ok(())
    }

    /// `getIamPolicy` / `setIamPolicy` / `testIamPermissions` on an existing
    /// topic or subscription `name`.
    pub(crate) fn iam_action(
        &mut self,
        name: &str,
        action: &str,
        body: &Value,
    ) -> Result<RestResponse, ApiError> {
        match action {
            "getIamPolicy" => Ok(RestResponse::ok_struct(
                self.iam_policies
                    .get(name)
                    .cloned()
                    .as_ref()
                    .unwrap_or(&default_policy()),
            )),
            "setIamPolicy" => {
                let policy = body
                    .get("policy")
                    .filter(|p| p.is_object())
                    .cloned()
                    .unwrap_or_else(default_policy);
                self.iam_policies.insert(name.to_string(), policy.clone());
                self.write_iam_policies()?;
                Ok(RestResponse::ok_struct(&policy))
            }
            "testIamPermissions" => {
                let perms = body.get("permissions").cloned().unwrap_or(Value::Null);
                let perms = if perms.is_array() {
                    perms
                } else {
                    Value::Array(Vec::new())
                };
                Ok(RestResponse::ok_struct(&json!({ "permissions": perms })))
            }
            _ => Err(ApiError::not_found("not found")),
        }
    }
}
