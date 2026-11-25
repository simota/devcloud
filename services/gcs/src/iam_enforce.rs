//! Request-level IAM authorization for `strict` auth mode.
//!
//! Every JSON / XML / upload / download request is mapped to the Cloud Storage
//! IAM permission it needs on its bucket, and the caller's identity is checked
//! against the bucket IAM policy (`GET/PUT /storage/v1/b/{bucket}/iam`).
//!
//! Identities:
//! - the configured `bearer_token`, and HMAC keys registered without a service
//!   account, act as the project owner (all permissions);
//! - tokens registered with `Server::add_access_token` (or configured through
//!   `auth.gcs.principals` / `DEVCLOUD_GCS_PRINCIPALS`, see [`parse_principals`])
//!   act as their IAM member;
//! - RSA / V2 signed URLs act as `serviceAccount:{signer}`, and HMAC keys
//!   registered with a service account act as that service account;
//! - unauthenticated requests match only `allUsers` bindings;
//! - `domain:{d}` members match `user:` / `serviceAccount:` members whose email
//!   is in domain `d`.
//!
//! Project-level operations (bucket list/create, service listing, the
//! introspection API) have no bucket policy and are owner-only. Conditional
//! bindings are not evaluated and never grant access. Legacy ACLs are not
//! consulted. This module is only invoked when `Server::is_strict_auth()`.

use crate::http::{
    bucket_suffix, json_error, url_decode, url_decode_path, Request, Response, Server,
};
use crate::iam_acl::role_grants_permission;
use crate::signed_urls::SignedUrlSigner;
use crate::state::{read_iam_policy, GcsIamPolicy};
use crate::xml_api::{virtual_host_bucket, xml_error};

/// Parses `token=member[,token=member...]` (the `auth.gcs.principals` config
/// value) into pairs for `Server::add_access_token`. Entries without `=` or with
/// an empty side are ignored.
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Principal {
    Owner,
    Member(String),
    Anonymous,
}

impl Principal {
    pub(crate) fn from_signer(server: &Server, signer: Option<SignedUrlSigner>) -> Self {
        match signer {
            Some(SignedUrlSigner::Hmac(id)) => match server.hmac_service_account(&id) {
                Some(email) => Principal::Member(format!("serviceAccount:{email}")),
                None => Principal::Owner,
            },
            Some(SignedUrlSigner::ServiceAccount(email)) => {
                Principal::Member(format!("serviceAccount:{email}"))
            }
            None => Principal::Anonymous,
        }
    }

    pub(crate) fn from_token(server: &Server, token: &str) -> Self {
        if server.is_owner_token(token) {
            return Principal::Owner;
        }
        match server.access_token_member(token) {
            Some(member) => Principal::Member(member.to_string()),
            None if token.is_empty() => Principal::Anonymous,
            // Non-strict modes never reach IAM enforcement.
            None => Principal::Owner,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Requirement {
    ProjectOwner,
    Bucket {
        bucket: String,
        permission: &'static str,
    },
}

fn need(bucket: &str, permission: &'static str) -> Requirement {
    Requirement::Bucket {
        bucket: bucket.to_string(),
        permission,
    }
}

/// Authorizes `req` for `principal`; returns the error response on denial.
pub fn authorize_request(
    server: &Server,
    req: &Request,
    principal: &Principal,
    json_surface: bool,
) -> Result<(), Response> {
    if *principal == Principal::Owner {
        return Ok(());
    }
    for requirement in required_permissions(req) {
        let allowed = match &requirement {
            Requirement::ProjectOwner => false,
            Requirement::Bucket { bucket, permission } => {
                let policy = read_iam_policy(server.store(), bucket).unwrap_or_default();
                policy_allows(&policy, principal, permission)
            }
        };
        if !allowed {
            return Err(denied(principal, &requirement, json_surface));
        }
    }
    Ok(())
}

fn denied(principal: &Principal, requirement: &Requirement, json_surface: bool) -> Response {
    if *principal == Principal::Anonymous {
        let mut r = if json_surface {
            json_error(401, "authError", "invalid authentication credentials")
        } else {
            xml_error(
                401,
                "AuthenticationRequired",
                "Anonymous caller does not have storage access.",
            )
        };
        r.headers.insert(
            "WWW-Authenticate".to_string(),
            "Bearer realm=\"devcloud-gcs\"".to_string(),
        );
        return r;
    }
    let member = match principal {
        Principal::Member(m) => m.as_str(),
        _ => "caller",
    };
    let message = match requirement {
        Requirement::ProjectOwner => {
            format!("{member} does not have project-level storage access.")
        }
        Requirement::Bucket { bucket, permission } => format!(
            "{member} does not have {permission} access to the Google Cloud Storage bucket {bucket}."
        ),
    };
    if json_surface {
        json_error(403, "forbidden", &message)
    } else {
        xml_error(403, "AccessDenied", &message)
    }
}

/// True when an unconditional binding grants `permission` to `principal`.
pub fn policy_allows(policy: &GcsIamPolicy, principal: &Principal, permission: &str) -> bool {
    policy.bindings.iter().any(|binding| {
        let conditional = binding
            .condition
            .as_ref()
            .is_some_and(|c| !c.expression.trim().is_empty());
        if conditional || !role_grants_permission(&binding.role, permission) {
            return false;
        }
        binding.members.iter().any(|m| match principal {
            Principal::Owner => true,
            Principal::Anonymous => m == "allUsers",
            Principal::Member(member) => {
                m == "allUsers"
                    || m == "allAuthenticatedUsers"
                    || m == member
                    || domain_matches(m, member)
            }
        })
    })
}

/// Whether binding member `domain:{d}` covers `member` (`user:`/`serviceAccount:`
/// with an email in domain `d`).
fn domain_matches(binding_member: &str, member: &str) -> bool {
    match (
        binding_member.strip_prefix("domain:"),
        member.split_once(':'),
    ) {
        (Some(domain), Some((_, email))) => email
            .rsplit_once('@')
            .is_some_and(|(_, d)| d.eq_ignore_ascii_case(domain)),
        _ => false,
    }
}

/// Maps a request to the permissions it needs. Unknown routes need nothing
/// here; they are answered with 404/405 by the normal handlers.
pub fn required_permissions(req: &Request) -> Vec<Requirement> {
    let path = req.path.as_str();
    if path.starts_with("/_introspect/") || path == "/storage/v1/b" {
        return vec![Requirement::ProjectOwner];
    }
    if let Some((bucket, suffix)) = bucket_suffix(path, "/storage/v1/b/") {
        return json_requirements(req, &bucket, &suffix);
    }
    if let Some((bucket, _)) = bucket_suffix(path, "/upload/storage/v1/b/") {
        return vec![need(&bucket, "storage.objects.create")];
    }
    if let Some((bucket, _)) = bucket_suffix(path, "/download/storage/v1/b/") {
        return vec![need(&bucket, "storage.objects.get")];
    }
    if path.starts_with("/storage/v1/")
        || path.starts_with("/upload/storage/v1/")
        || path.starts_with("/download/storage/v1/")
    {
        return Vec::new();
    }
    xml_requirements(req)
}

fn read_or_write(req: &Request, read: &'static str, write: &'static str) -> &'static str {
    if matches!(req.method.as_str(), "GET" | "HEAD") {
        read
    } else {
        write
    }
}

fn json_requirements(req: &Request, bucket: &str, suffix: &str) -> Vec<Requirement> {
    let method = req.method.as_str();
    let bucket_perm = match suffix {
        "" => Some(match method {
            "GET" | "HEAD" => "storage.buckets.get",
            "DELETE" => "storage.buckets.delete",
            _ => "storage.buckets.update",
        }),
        "lockRetentionPolicy" => Some("storage.buckets.update"),
        "iam" => Some(read_or_write(
            req,
            "storage.buckets.getIamPolicy",
            "storage.buckets.setIamPolicy",
        )),
        // Any authenticated caller may ask which permissions it holds.
        "iam/testPermissions" => return Vec::new(),
        s if s == "acl"
            || s.starts_with("acl/")
            || s == "defaultObjectAcl"
            || s.starts_with("defaultObjectAcl/") =>
        {
            Some(read_or_write(
                req,
                "storage.buckets.getIamPolicy",
                "storage.buckets.setIamPolicy",
            ))
        }
        s if s == "notificationConfigs" || s.starts_with("notificationConfigs/") => Some(
            read_or_write(req, "storage.buckets.get", "storage.buckets.update"),
        ),
        "o" => Some(read_or_write(
            req,
            "storage.objects.list",
            "storage.objects.create",
        )),
        _ => None,
    };
    if let Some(permission) = bucket_perm {
        return vec![need(bucket, permission)];
    }
    let Some(object) = suffix.strip_prefix("o/") else {
        return Vec::new();
    };
    if object.ends_with("/compose") {
        return vec![
            need(bucket, "storage.objects.get"),
            need(bucket, "storage.objects.create"),
        ];
    }
    let copy = object
        .split_once("/copyTo/b/")
        .or_else(|| object.split_once("/rewriteTo/b/"));
    if let Some((_, dest)) = copy {
        let dest_bucket = url_decode(dest.split('/').next().unwrap_or(""));
        return vec![
            need(bucket, "storage.objects.get"),
            need(&dest_bucket, "storage.objects.create"),
        ];
    }
    if object.ends_with("/acl") || object.contains("/acl/") {
        return vec![need(
            bucket,
            read_or_write(
                req,
                "storage.objects.getIamPolicy",
                "storage.objects.setIamPolicy",
            ),
        )];
    }
    vec![need(
        bucket,
        match method {
            "GET" | "HEAD" => "storage.objects.get",
            "DELETE" => "storage.objects.delete",
            _ => "storage.objects.update",
        },
    )]
}

fn xml_requirements(req: &Request) -> Vec<Requirement> {
    let trimmed = req.path.trim_start_matches('/');
    let (bucket, key) = match virtual_host_bucket(req.header("host")) {
        Some(b) => (b, trimmed.to_string()),
        None => {
            if trimmed.is_empty() {
                return vec![Requirement::ProjectOwner];
            }
            let (raw_bucket, raw_key) = trimmed.split_once('/').unwrap_or((trimmed, ""));
            (url_decode_path(raw_bucket), raw_key.to_string())
        }
    };
    let q = |name: &str| req.query.contains_key(name);
    let method = req.method.as_str();
    if key.is_empty() {
        let permission = match method {
            "PUT" if q("acl") => "storage.buckets.setIamPolicy",
            "PUT" if q("lifecycle") => "storage.buckets.update",
            "PUT" => return vec![Requirement::ProjectOwner],
            "GET" if q("acl") => "storage.buckets.getIamPolicy",
            "GET" if q("uploads") => "storage.multipartUploads.list",
            "GET" if q("lifecycle") || q("location") || q("storageClass") || q("versioning") => {
                "storage.buckets.get"
            }
            "GET" => "storage.objects.list",
            "HEAD" => "storage.buckets.get",
            "DELETE" => "storage.buckets.delete",
            _ => return Vec::new(),
        };
        return vec![need(&bucket, permission)];
    }
    let permission = match method {
        "GET" if q("uploadId") => "storage.multipartUploads.listParts",
        "GET" if q("acl") => "storage.objects.getIamPolicy",
        "GET" | "HEAD" => "storage.objects.get",
        "PUT" if q("acl") => "storage.objects.setIamPolicy",
        "PUT" if q("uploadId") => "storage.multipartUploads.create",
        "PUT" => {
            let source = req.header("x-goog-copy-source").trim();
            if !source.is_empty() {
                let source_bucket = url_decode_path(
                    source
                        .trim_start_matches('/')
                        .split('/')
                        .next()
                        .unwrap_or(""),
                );
                return vec![
                    need(&source_bucket, "storage.objects.get"),
                    need(&bucket, "storage.objects.create"),
                ];
            }
            "storage.objects.create"
        }
        "POST" if q("uploads") => "storage.multipartUploads.create",
        "POST" if q("uploadId") => "storage.objects.create",
        "DELETE" if q("uploadId") => "storage.multipartUploads.abort",
        "DELETE" => "storage.objects.delete",
        _ => return Vec::new(),
    };
    vec![need(&bucket, permission)]
}
