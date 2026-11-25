//! `strict` auth mode: caller identity from bearer tokens and project /
//! dataset / table IAM enforcement.
//!
//! Only active when the auth mode is `strict`; `relaxed`, `oauth-relaxed`, and
//! `bearer-dev` keep the plain [`crate::routes`] token check.
//!
//! - The configured bearer token is the project owner and may do anything.
//! - Tokens registered with [`Server::add_principal`] authenticate as an IAM
//!   member. Dataset-scoped calls are authorized against the union of the
//!   project IAM policy (`/v1/projects/{p}:setIamPolicy`), the dataset IAM
//!   policy, the dataset `access` entries and, for table calls, the table
//!   policy. Dataset creation needs `bigquery.datasets.create` on the project
//!   and project IAM reads/writes need `roles/owner` on the project; other
//!   project-level calls (jobs, queries, dataset list, introspection, uploads)
//!   stay owner-only, because the local query engine cannot attribute the
//!   tables a query reads to the caller.
//! - The Storage API (gRPC) uses the same callers and policies; see
//!   [`crate::storage_api`].
//! - Requests without a token are anonymous and only reach resources whose
//!   policy grants the permission to `allUsers`.
//! - Conditional bindings never grant access.

use crate::model::{DatasetAccessEntry, IamBinding, IamPolicy};
use crate::responses::ApiResponse;
use crate::routes::{
    bearer_token_from_request, constant_time_eq, resource_parts, split_resource_action, Request,
};
use crate::server::Server;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Caller {
    Owner,
    Principal(String),
    Anonymous,
}

/// The resource a permission is checked on.
pub(crate) enum Target {
    Project(String),
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
    const READ_SESSIONS: &[&str] = &[
        "bigquery.readsessions.create",
        "bigquery.readsessions.getData",
        "bigquery.readsessions.update",
    ];
    let data_editor = |permission: &str| {
        VIEWER.contains(&permission)
            || permission == "bigquery.datasets.create"
            || (["bigquery.tables.", "bigquery.models.", "bigquery.routines."]
                .iter()
                .any(|p| permission.starts_with(p))
                && permission != "bigquery.tables.setIamPolicy")
    };
    match role {
        "roles/owner" => {
            permission.starts_with("bigquery.")
                || permission == "resourcemanager.projects.getIamPolicy"
                || permission == "resourcemanager.projects.setIamPolicy"
        }
        "roles/bigquery.admin" => permission.starts_with("bigquery."),
        "roles/bigquery.dataOwner" => {
            permission.starts_with("bigquery.")
                && !permission.starts_with("bigquery.readsessions.")
                && !permission.starts_with("bigquery.jobs.")
        }
        "roles/editor" | "roles/bigquery.dataEditor" => data_editor(permission),
        "roles/viewer" | "roles/bigquery.dataViewer" => VIEWER.contains(&permission),
        "roles/bigquery.metadataViewer" => METADATA_VIEWER.contains(&permission),
        "roles/bigquery.readSessionUser" => READ_SESSIONS.contains(&permission),
        "roles/bigquery.user" => {
            READ_SESSIONS.contains(&permission) || permission == "bigquery.datasets.create"
        }
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
    let method = req.method.as_str();
    if req.path.starts_with("/v1/projects/") {
        let parts = resource_parts(&req.path, "/v1/projects/")?;
        let [resource] = parts.as_slice() else {
            return None;
        };
        let (project, action) = split_resource_action(resource);
        let permission = match action {
            "getIamPolicy" => "resourcemanager.projects.getIamPolicy",
            "setIamPolicy" => "resourcemanager.projects.setIamPolicy",
            _ => return None,
        };
        return Some(Some((Target::Project(project.to_string()), permission)));
    }
    let parts = resource_parts(&req.path, "/bigquery/v2/projects/")?;
    if !req.path.starts_with("/bigquery/v2/projects/") || parts.len() < 2 || parts[1] != "datasets"
    {
        return None;
    }
    if parts.len() == 2 {
        return (method == "POST").then(|| {
            Some((
                Target::Project(parts[0].clone()),
                "bigquery.datasets.create",
            ))
        });
    }
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

/// Strict-mode authentication + authorization. `Ok` carries the caller;
/// `Err` carries the 401/403.
pub(crate) fn authorize_strict(server: &Server, req: &Request) -> Result<Caller, ApiResponse> {
    let caller = server
        .strict_caller(&bearer_token_from_request(req))
        .ok_or_else(unauthenticated)?;
    if caller == Caller::Owner {
        return Ok(caller);
    }
    let denied = |permission: &str, resource: &str| match &caller {
        Caller::Principal(member) => access_denied(member, permission, resource),
        _ => unauthenticated(),
    };
    let Some(required) = required_permission(req) else {
        return Err(denied("project-level", "Project"));
    };
    let Some((target, permission)) = required else {
        return Ok(caller);
    };
    if is_allowed(server, &caller, &target, permission) {
        Ok(caller)
    } else {
        Err(denied(permission, &describe(&target)))
    }
}

pub(crate) fn describe(target: &Target) -> String {
    match target {
        Target::Project(p) => format!("Project {p}"),
        Target::Dataset(p, d) => format!("Dataset {p}:{d}"),
        Target::Table(p, d, t) => format!("Table {p}:{d}.{t}"),
    }
}

/// Whether `caller` holds `permission` on `target` through the project IAM
/// policy, the dataset IAM policy and `access` entries, or the table policy.
pub(crate) fn is_allowed(
    server: &Server,
    caller: &Caller,
    target: &Target,
    permission: &str,
) -> bool {
    if *caller == Caller::Owner {
        return true;
    }
    let read = |path: std::path::PathBuf| server.read_iam_policy(&path).unwrap_or_default();
    let project_id = match target {
        Target::Project(p) | Target::Dataset(p, _) | Target::Table(p, _, _) => p,
    };
    let project = read(server.project_iam_policy_path(project_id));
    let mut policies = Vec::new();
    if let Target::Table(p, d, t) = target {
        policies.push(read(server.table_iam_policy_path(p, d, t)));
    }
    if let Target::Dataset(p, d) | Target::Table(p, d, _) = target {
        policies.push(read(server.dataset_iam_policy_path(p, d)));
        if let Ok(Some(dataset)) = server.read_dataset(p, d) {
            policies.push(access_policy(
                dataset.access.as_deref().unwrap_or_default(),
                &project,
            ));
        }
    }
    policies.push(project);
    policies
        .iter()
        .any(|policy| policy_allows(policy, caller, permission))
}

/// Dataset `access` entries as IAM bindings: legacy roles map to
/// `roles/bigquery.data{Viewer,Editor,Owner}`, `userByEmail` matches `user:`
/// and `serviceAccount:` members, and `projectOwners`/`projectWriters`/
/// `projectReaders` expand to the project's `roles/owner`/`editor`/`viewer`
/// members. Authorized view/routine/dataset entries grant callers nothing.
fn access_policy(entries: &[DatasetAccessEntry], project: &IamPolicy) -> IamPolicy {
    let project_members = |role: &str| -> Vec<String> {
        project
            .bindings
            .iter()
            .filter(|b| b.condition.is_none() && b.role == role)
            .flat_map(|b| b.members.iter().cloned())
            .collect()
    };
    let bindings = entries
        .iter()
        .filter(|entry| !entry.role.is_empty())
        .map(|entry| {
            let role = match entry.role.as_str() {
                "READER" => "roles/bigquery.dataViewer",
                "WRITER" => "roles/bigquery.dataEditor",
                "OWNER" => "roles/bigquery.dataOwner",
                role => role,
            };
            let members = if !entry.user_by_email.is_empty() {
                vec![
                    format!("user:{}", entry.user_by_email),
                    format!("serviceAccount:{}", entry.user_by_email),
                ]
            } else if !entry.group_by_email.is_empty() {
                vec![format!("group:{}", entry.group_by_email)]
            } else if !entry.domain.is_empty() {
                vec![format!("domain:{}", entry.domain)]
            } else if !entry.iam_member.is_empty() {
                vec![entry.iam_member.clone()]
            } else {
                match entry.special_group.as_str() {
                    "allAuthenticatedUsers" => vec!["allAuthenticatedUsers".to_string()],
                    "projectOwners" => project_members("roles/owner"),
                    "projectWriters" => project_members("roles/editor"),
                    "projectReaders" => project_members("roles/viewer"),
                    _ => Vec::new(),
                }
            };
            IamBinding {
                role: role.to_string(),
                members,
                condition: None,
            }
        })
        .collect();
    IamPolicy {
        bindings,
        ..Default::default()
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

    pub(crate) fn strict_caller(&self, token: &str) -> Option<Caller> {
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
