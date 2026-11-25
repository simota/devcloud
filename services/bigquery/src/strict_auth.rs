//! `strict` auth mode: caller identity from bearer tokens and dataset/table
//! IAM enforcement.
//!
//! Only active when the auth mode is `strict`; `relaxed`, `oauth-relaxed`, and
//! `bearer-dev` keep the plain [`crate::routes`] token check.
//!
//! - The configured bearer token is the project owner and may do anything.
//! - Tokens registered with [`Server::add_principal`] authenticate as an IAM
//!   member and are authorized against the dataset IAM policy
//!   (`datasets/{d}:setIamPolicy`) and, for table calls, the table policy
//!   (`tables/{t}:setIamPolicy`) as well. Project-level calls (jobs, queries,
//!   dataset list/create, introspection, uploads) are owner-only.
//! - Requests without a token are anonymous and only reach resources whose
//!   policy grants the permission to `allUsers`.
//! - Conditional bindings never grant access; dataset `access` entries are not
//!   consulted.

use crate::model::IamPolicy;
use crate::responses::ApiResponse;
use crate::routes::{
    bearer_token_from_request, constant_time_eq, resource_parts, split_resource_action, Request,
};
use crate::server::Server;

#[derive(Debug, Clone, PartialEq, Eq)]
enum Caller {
    Owner,
    Principal(String),
    Anonymous,
}

/// The resource a permission is checked on.
enum Target {
    Dataset(String, String),
    Table(String, String, String),
}

/// Parses `token=member[,token=member...]` (the `auth.bigquery.principals`
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

fn role_grants(role: &str, permission: &str) -> bool {
    const VIEWER: &[&str] = &[
        "bigquery.datasets.get",
        "bigquery.datasets.getIamPolicy",
        "bigquery.tables.get",
        "bigquery.tables.getData",
        "bigquery.tables.getIamPolicy",
        "bigquery.tables.list",
        "bigquery.models.getData",
        "bigquery.models.getMetadata",
        "bigquery.models.list",
        "bigquery.routines.get",
        "bigquery.routines.list",
    ];
    const METADATA_VIEWER: &[&str] = &[
        "bigquery.datasets.get",
        "bigquery.datasets.getIamPolicy",
        "bigquery.tables.get",
        "bigquery.tables.getIamPolicy",
        "bigquery.tables.list",
        "bigquery.models.getMetadata",
        "bigquery.models.list",
        "bigquery.routines.get",
        "bigquery.routines.list",
    ];
    match role {
        "roles/owner" | "roles/bigquery.admin" | "roles/bigquery.dataOwner" => {
            permission.starts_with("bigquery.")
        }
        "roles/editor" | "roles/bigquery.dataEditor" => {
            VIEWER.contains(&permission)
                || (["bigquery.tables.", "bigquery.models.", "bigquery.routines."]
                    .iter()
                    .any(|p| permission.starts_with(p))
                    && permission != "bigquery.tables.setIamPolicy")
        }
        "roles/viewer" | "roles/bigquery.dataViewer" => VIEWER.contains(&permission),
        "roles/bigquery.metadataViewer" => METADATA_VIEWER.contains(&permission),
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

fn policy_allows(policy: &IamPolicy, caller: &Caller, permission: &str) -> bool {
    policy.bindings.iter().any(|b| {
        b.condition.is_none()
            && role_grants(&b.role, permission)
            && b.members.iter().any(|m| member_matches(m, caller))
    })
}

/// `Some((target, permission))`, `Some(None)` for calls any caller may make
/// (`testIamPermissions`), `None` for owner-only project-level calls.
fn required_permission(req: &Request) -> Option<Option<(Target, &'static str)>> {
    let parts = resource_parts(&req.path, "/bigquery/v2/projects/")?;
    if !req.path.starts_with("/bigquery/v2/projects/") || parts.len() < 3 || parts[1] != "datasets"
    {
        return None;
    }
    let method = req.method.as_str();
    let project = parts[0].clone();
    let by_method = |get, update, delete| match method {
        "GET" => get,
        "DELETE" => delete,
        _ => update,
    };
    let (dataset_id, action) = split_resource_action(&parts[2]);
    let dataset = Target::Dataset(project.clone(), dataset_id.to_string());
    let required = match parts.len() {
        3 => match action {
            "" => (
                dataset,
                by_method(
                    "bigquery.datasets.get",
                    "bigquery.datasets.update",
                    "bigquery.datasets.delete",
                ),
            ),
            "getIamPolicy" => (dataset, "bigquery.datasets.getIamPolicy"),
            "setIamPolicy" => (dataset, "bigquery.datasets.setIamPolicy"),
            _ => return Some(None),
        },
        4 => {
            let read = method == "GET";
            let perm = match (parts[3].as_str(), read) {
                ("tables", true) => "bigquery.tables.list",
                ("tables", false) => "bigquery.tables.create",
                ("routines", true) => "bigquery.routines.list",
                ("routines", false) => "bigquery.routines.create",
                ("models", _) => "bigquery.models.list",
                _ => return None,
            };
            (dataset, perm)
        }
        5 => match parts[3].as_str() {
            "tables" => {
                let (table_id, action) = split_resource_action(&parts[4]);
                let table = Target::Table(project, dataset_id.to_string(), table_id.to_string());
                match action {
                    "" => (
                        table,
                        by_method(
                            "bigquery.tables.get",
                            "bigquery.tables.update",
                            "bigquery.tables.delete",
                        ),
                    ),
                    "getIamPolicy" => (table, "bigquery.tables.getIamPolicy"),
                    "setIamPolicy" => (table, "bigquery.tables.setIamPolicy"),
                    _ => return Some(None),
                }
            }
            "routines" => (
                dataset,
                by_method(
                    "bigquery.routines.get",
                    "bigquery.routines.update",
                    "bigquery.routines.delete",
                ),
            ),
            "models" => (
                dataset,
                by_method(
                    "bigquery.models.getMetadata",
                    "bigquery.models.updateMetadata",
                    "bigquery.models.delete",
                ),
            ),
            _ => return None,
        },
        6 if parts[3] == "tables" => {
            let table = Target::Table(project, dataset_id.to_string(), parts[4].clone());
            match parts[5].as_str() {
                "insertAll" => (table, "bigquery.tables.updateData"),
                "data" => (table, "bigquery.tables.getData"),
                _ => return None,
            }
        }
        _ => return None,
    };
    Some(Some(required))
}

fn unauthenticated() -> ApiResponse {
    ApiResponse::error(401, "authError", "invalid authentication credentials")
        .with_www_authenticate()
}

fn access_denied(member: &str, permission: &str, resource: &str) -> ApiResponse {
    ApiResponse::error(
        403,
        "accessDenied",
        &format!(
            "Access Denied: {resource}: Permission {permission} denied for {member} (or it may not exist)."
        ),
    )
}

/// Strict-mode authentication + authorization. `Err` carries the 401/403.
pub(crate) fn authorize_strict(server: &Server, req: &Request) -> Result<(), ApiResponse> {
    let caller = server
        .strict_caller(&bearer_token_from_request(req))
        .ok_or_else(unauthenticated)?;
    if caller == Caller::Owner {
        return Ok(());
    }
    let denied = |permission: &str, resource: &str| match &caller {
        Caller::Principal(member) => access_denied(member, permission, resource),
        _ => unauthenticated(),
    };
    let Some(required) = required_permission(req) else {
        return Err(denied("project-level", "Project"));
    };
    let Some((target, permission)) = required else {
        return Ok(());
    };
    let read = |path: std::path::PathBuf| server.read_iam_policy(&path).unwrap_or_default();
    let (resource, policies) = match &target {
        Target::Dataset(p, d) => (
            format!("Dataset {p}:{d}"),
            vec![read(server.dataset_iam_policy_path(p, d))],
        ),
        Target::Table(p, d, t) => (
            format!("Table {p}:{d}.{t}"),
            vec![
                read(server.table_iam_policy_path(p, d, t)),
                read(server.dataset_iam_policy_path(p, d)),
            ],
        ),
    };
    if policies
        .iter()
        .any(|policy| policy_allows(policy, &caller, permission))
    {
        Ok(())
    } else {
        Err(denied(permission, &resource))
    }
}

impl Server {
    pub fn is_strict_auth(&self) -> bool {
        self.config.auth_mode.trim().eq_ignore_ascii_case("strict")
    }

    /// Registers a strict-mode bearer token that authenticates as IAM
    /// `member` (e.g. `user:alice@example.com`).
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
}
