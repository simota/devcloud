//! `strict` auth mode: caller identity from bearer tokens and bucket IAM
//! enforcement.
//!
//! Only active when the auth mode is `strict`; `relaxed`, `oauth-relaxed`, and
//! `bearer-dev` keep their existing behavior.
//!
//! - The configured bearer token is the project owner and may do anything
//!   (project-level IAM inheritance).
//! - Tokens registered with [`Server::add_principal`] authenticate as an IAM
//!   member (`user:...`, `serviceAccount:...`) and are authorized against the
//!   target bucket's IAM policy (`/storage/v1/b/{b}/iam`). Project-level calls
//!   (bucket list/create, introspection) are owner-only.
//! - Requests without a token are anonymous and only reach buckets whose IAM
//!   policy grants the permission to `allUsers`.
//! - Conditional bindings are never treated as granting access, and ACLs are
//!   not consulted.

use crate::http::{json_error, url_decode, url_decode_path, Request, Response, Server};
use crate::state::{read_iam_policy, GcsIamPolicy};

/// Who the request authenticates as in strict mode.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Caller {
    Owner,
    Principal(String),
    Anonymous,
}

/// Parses `token=member[,token=member...]` (the `auth.gcs.principals` config
/// value). Entries without `=` or with an empty side are ignored.
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

pub(crate) fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

/// Whether `member` of a binding covers `caller`.
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

pub(crate) fn policy_allows(policy: &GcsIamPolicy, caller: &Caller, permission: &str) -> bool {
    policy.bindings.iter().any(|b| {
        b.condition.is_none()
            && crate::iam_acl::role_grants_permission(&b.role, permission)
            && b.members.iter().any(|m| member_matches(m, caller))
    })
}

/// The `(bucket, permission)` pairs a request needs, or `None` for
/// project-level calls that only the owner may make.
fn required_permissions(req: &Request) -> Option<Vec<(String, &'static str)>> {
    let path = req.path.as_str();
    let method = req.method.as_str();
    let read = matches!(method, "GET" | "HEAD");
    if let Some(rest) = path.strip_prefix("/storage/v1/b/") {
        let (bucket, suffix) = rest.split_once('/').unwrap_or((rest, ""));
        let bucket = url_decode(bucket);
        if bucket.is_empty() {
            return None;
        }
        let one = |perm: &'static str| Some(vec![(bucket.clone(), perm)]);
        let iam = |r: bool| {
            if r {
                "storage.buckets.getIamPolicy"
            } else {
                "storage.buckets.setIamPolicy"
            }
        };
        return match suffix {
            "" => one(match method {
                "GET" | "HEAD" => "storage.buckets.get",
                "DELETE" => "storage.buckets.delete",
                _ => "storage.buckets.update",
            }),
            "lockRetentionPolicy" => one("storage.buckets.update"),
            "iam" => one(iam(read)),
            "iam/testPermissions" => Some(Vec::new()),
            s if s == "acl"
                || s.starts_with("acl/")
                || s == "defaultObjectAcl"
                || s.starts_with("defaultObjectAcl/") =>
            {
                one(iam(read))
            }
            s if s == "notificationConfigs" || s.starts_with("notificationConfigs/") => {
                one(if read {
                    "storage.buckets.get"
                } else {
                    "storage.buckets.update"
                })
            }
            "o" => one(if read {
                "storage.objects.list"
            } else {
                "storage.objects.create"
            }),
            s => {
                let object = s.strip_prefix("o/")?;
                if object.ends_with("/compose") {
                    return Some(vec![
                        (bucket.clone(), "storage.objects.get"),
                        (bucket, "storage.objects.create"),
                    ]);
                }
                let dest = object
                    .split_once("/copyTo/b/")
                    .or_else(|| object.split_once("/rewriteTo/b/"))
                    .map(|(_, d)| url_decode(d.split('/').next().unwrap_or("")));
                if let Some(dest) = dest {
                    return Some(vec![
                        (bucket, "storage.objects.get"),
                        (dest, "storage.objects.create"),
                    ]);
                }
                if object.ends_with("/acl") || object.contains("/acl/") {
                    return one(if read {
                        "storage.objects.getIamPolicy"
                    } else {
                        "storage.objects.setIamPolicy"
                    });
                }
                one(match method {
                    "GET" | "HEAD" => "storage.objects.get",
                    "DELETE" => "storage.objects.delete",
                    _ => "storage.objects.update",
                })
            }
        };
    }
    if let Some(rest) = path.strip_prefix("/upload/storage/v1/b/") {
        let bucket = url_decode(rest.split('/').next().unwrap_or(""));
        return (!bucket.is_empty()).then(|| vec![(bucket, "storage.objects.create")]);
    }
    if let Some(rest) = path.strip_prefix("/download/storage/v1/b/") {
        let bucket = url_decode(rest.split('/').next().unwrap_or(""));
        return (!bucket.is_empty()).then(|| vec![(bucket, "storage.objects.get")]);
    }
    if path.starts_with("/storage/v1/")
        || path.starts_with("/upload/")
        || path.starts_with("/download/")
        || path.starts_with("/_introspect/")
    {
        return None;
    }
    // XML API: virtual-host or path-style.
    let trimmed = path.trim_start_matches('/');
    let (bucket, key) = match crate::xml_api::virtual_host_bucket(req.header("host")) {
        Some(bucket) => (bucket, trimmed.to_string()),
        None => {
            let (b, k) = trimmed.split_once('/').unwrap_or((trimmed, ""));
            (url_decode_path(b), k.to_string())
        }
    };
    if bucket.is_empty() {
        return None;
    }
    let has_acl = req.query.contains_key("acl");
    let perm = if key.is_empty() {
        match method {
            _ if has_acl && read => "storage.buckets.getIamPolicy",
            _ if has_acl => "storage.buckets.setIamPolicy",
            "GET" | "HEAD" => "storage.objects.list",
            "PUT" => return None, // bucket create
            "DELETE" => "storage.buckets.delete",
            _ => "storage.objects.create",
        }
    } else {
        match method {
            _ if has_acl && read => "storage.objects.getIamPolicy",
            _ if has_acl => "storage.objects.setIamPolicy",
            "GET" | "HEAD" => "storage.objects.get",
            "DELETE" => "storage.objects.delete",
            _ => "storage.objects.create",
        }
    };
    Some(vec![(bucket, perm)])
}

fn unauthenticated(json: bool) -> Response {
    let mut r = if json {
        json_error(401, "authError", "invalid authentication credentials")
    } else {
        crate::xml_api::xml_error(
            401,
            "AuthenticationRequired",
            "Anonymous caller does not have storage access.",
        )
    };
    r.headers.insert(
        "WWW-Authenticate".to_string(),
        "Bearer realm=\"devcloud-gcs\"".to_string(),
    );
    r
}

fn forbidden(json: bool, member: &str, permission: &str, bucket: &str) -> Response {
    let message = if bucket.is_empty() {
        format!("{member} does not have {permission} access to the project.")
    } else {
        format!("{member} does not have {permission} access to the Google Cloud Storage bucket {bucket}.")
    };
    if json {
        json_error(403, "forbidden", &message)
    } else {
        crate::xml_api::xml_error(403, "AccessDenied", &message)
    }
}

/// Strict-mode authentication + authorization for a request that is not a
/// signed URL. `Err` carries the 401/403 response.
pub(crate) fn authorize_strict(server: &Server, req: &Request, json: bool) -> Result<(), Response> {
    let caller = server
        .strict_caller(&req.bearer_token())
        .ok_or_else(|| unauthenticated(json))?;
    if caller == Caller::Owner {
        return Ok(());
    }
    let Some(required) = required_permissions(req) else {
        return match caller {
            Caller::Principal(member) => Err(forbidden(json, &member, "project-level", "")),
            _ => Err(unauthenticated(json)),
        };
    };
    for (bucket, permission) in required {
        let policy = read_iam_policy(server.store(), &bucket).unwrap_or_default();
        if !policy_allows(&policy, &caller, permission) {
            return Err(match &caller {
                Caller::Principal(member) => forbidden(json, member, permission, &bucket),
                _ => unauthenticated(json),
            });
        }
    }
    Ok(())
}

impl Server {
    pub fn is_strict_auth(&self) -> bool {
        self.auth_mode_normalized() == "strict"
    }

    /// Registers a strict-mode bearer token that authenticates as IAM
    /// `member` (e.g. `user:alice@example.com`).
    pub fn add_principal(&mut self, token: impl Into<String>, member: impl Into<String>) {
        self.principals.push((token.into(), member.into()));
    }

    /// Resolves a strict-mode caller; `None` for an unknown token.
    pub(crate) fn strict_caller(&self, token: &str) -> Option<Caller> {
        if token.is_empty() {
            return Some(Caller::Anonymous);
        }
        let owner = self.owner_token();
        if !owner.is_empty() && constant_time_eq(token.as_bytes(), owner.as_bytes()) {
            return Some(Caller::Owner);
        }
        self.principals
            .iter()
            .find(|(t, _)| constant_time_eq(token.as_bytes(), t.as_bytes()))
            .map(|(_, member)| Caller::Principal(member.clone()))
    }
}
