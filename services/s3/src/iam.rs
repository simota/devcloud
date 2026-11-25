//! Local bucket policy and canned ACL enforcement for `strict` auth mode.
//!
//! In `relaxed` mode (the default), authorization checks are bypassed so local
//! tooling and the dashboard continue working unchanged. In `strict` mode:
//! - Authenticated callers (valid SigV4 / SigV4a / SigV2) are subject to
//!   explicit `Deny` statements in the target bucket's policy and are otherwise
//!   allowed as the bucket owner.
//! - Anonymous (unsigned) callers are allowed only when the target bucket's
//!   policy explicitly allows the action without a matching `Deny`, or when the
//!   target bucket/object canned ACL grants public access (`public-read` /
//!   `public-read-write`) and no explicit `Deny` blocks it.

use serde_json::Value;

use crate::http::{parse_path_style, AuthConfig, Request};
use crate::store::FileBucketStore;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Caller<'a> {
    Anonymous,
    Authenticated { access_key_id: &'a str },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PolicyDecision {
    Allow,
    Deny,
    ImplicitDeny,
}

#[derive(Debug, Clone)]
struct ActionContext {
    action: &'static str,
    bucket: String,
    key: String,
    resource_arn: String,
}

/// Evaluates bucket policy and canned ACLs for `req` in `strict` mode.
/// Returns `Ok(())` if authorized or `Err(())` for `403 AccessDenied`.
pub(crate) fn authorize_request(
    store: &FileBucketStore,
    req: &Request,
    auth: &AuthConfig,
    caller: Caller<'_>,
) -> Result<(), ()> {
    if !auth.auth_mode.eq_ignore_ascii_case("strict") {
        return Ok(());
    }
    if req.path == "/" || crate::introspect::is_introspect_path(&req.path) {
        return match caller {
            Caller::Authenticated { .. } => Ok(()),
            Caller::Anonymous => Err(()),
        };
    }
    let Some(ctx) = resolve_action_context(req) else {
        return match caller {
            Caller::Authenticated { .. } => Ok(()),
            Caller::Anonymous => Err(()),
        };
    };

    let policy_decision = evaluate_bucket_policy(store, req, &ctx, caller);
    if policy_decision == PolicyDecision::Deny {
        return Err(());
    }

    match caller {
        Caller::Authenticated { .. } => Ok(()),
        Caller::Anonymous => {
            if policy_decision == PolicyDecision::Allow {
                return Ok(());
            }
            if acl_allows_anonymous(store, req, &ctx) {
                Ok(())
            } else {
                Err(())
            }
        }
    }
}

fn resolve_action_context(req: &Request) -> Option<ActionContext> {
    let (bucket, key) = parse_path_style(&req.path)?;
    let action = if key.is_empty() {
        bucket_action(req)
    } else {
        object_action(req)
    };
    let resource_arn = if key.is_empty() {
        format!("arn:aws:s3:::{bucket}")
    } else {
        format!("arn:aws:s3:::{bucket}/{key}")
    };
    Some(ActionContext {
        action,
        bucket,
        key,
        resource_arn,
    })
}

fn bucket_action(req: &Request) -> &'static str {
    match req.method.as_str() {
        "PUT" => {
            if req.query.contains_key("versioning") {
                "s3:PutBucketVersioning"
            } else if req.query.contains_key("acl") {
                "s3:PutBucketAcl"
            } else if req.query.contains_key("policy") {
                "s3:PutBucketPolicy"
            } else if req.query.contains_key("cors") {
                "s3:PutBucketCORS"
            } else if req.query.contains_key("lifecycle") {
                "s3:PutLifecycleConfiguration"
            } else if req.query.contains_key("object-lock") {
                "s3:PutBucketObjectLockConfiguration"
            } else if req.query.contains_key("notification") {
                "s3:PutBucketNotification"
            } else if req.query.contains_key("inventory") {
                "s3:PutInventoryConfiguration"
            } else if req.query.contains_key("analytics") {
                "s3:PutAnalyticsConfiguration"
            } else if req.query.contains_key("replication") {
                "s3:PutReplicationConfiguration"
            } else {
                "s3:CreateBucket"
            }
        }
        "HEAD" => "s3:ListBucket",
        "GET" => {
            if req.query.contains_key("location") {
                "s3:GetBucketLocation"
            } else if req.query.contains_key("versioning") {
                "s3:GetBucketVersioning"
            } else if req.query.contains_key("acl") {
                "s3:GetBucketAcl"
            } else if req.query.contains_key("policy") {
                "s3:GetBucketPolicy"
            } else if req.query.contains_key("cors") {
                "s3:GetBucketCORS"
            } else if req.query.contains_key("lifecycle") {
                "s3:GetLifecycleConfiguration"
            } else if req.query.contains_key("object-lock") {
                "s3:GetBucketObjectLockConfiguration"
            } else if req.query.contains_key("versions") {
                "s3:ListBucketVersions"
            } else if req.query.contains_key("notification") {
                "s3:GetBucketNotification"
            } else if req.query.contains_key("inventory") {
                "s3:GetInventoryConfiguration"
            } else if req.query.contains_key("analytics") {
                "s3:GetAnalyticsConfiguration"
            } else if req.query.contains_key("replication") {
                "s3:GetReplicationConfiguration"
            } else if req.query.contains_key("uploads") {
                "s3:ListBucketMultipartUploads"
            } else {
                "s3:ListBucket"
            }
        }
        "DELETE" => {
            if req.query.contains_key("policy") {
                "s3:DeleteBucketPolicy"
            } else if req.query.contains_key("cors") {
                "s3:PutBucketCORS"
            } else if req.query.contains_key("lifecycle") {
                "s3:PutLifecycleConfiguration"
            } else if req.query.contains_key("object-lock") {
                "s3:PutBucketObjectLockConfiguration"
            } else if req.query.contains_key("inventory") {
                "s3:PutInventoryConfiguration"
            } else if req.query.contains_key("analytics") {
                "s3:PutAnalyticsConfiguration"
            } else if req.query.contains_key("replication") {
                "s3:PutReplicationConfiguration"
            } else {
                "s3:DeleteBucket"
            }
        }
        _ => "s3:ListBucket",
    }
}

fn object_action(req: &Request) -> &'static str {
    let has_version = req.query.get("versionId").is_some_and(|v| !v.is_empty());
    match req.method.as_str() {
        "GET" | "HEAD" => {
            if req.query.contains_key("acl") {
                if has_version {
                    "s3:GetObjectVersionAcl"
                } else {
                    "s3:GetObjectAcl"
                }
            } else if req.query.contains_key("retention") {
                "s3:GetObjectRetention"
            } else if req.query.contains_key("legal-hold") {
                "s3:GetObjectLegalHold"
            } else if req.query.contains_key("uploadId") {
                "s3:ListMultipartUploadParts"
            } else if has_version {
                "s3:GetObjectVersion"
            } else {
                "s3:GetObject"
            }
        }
        "PUT" => {
            if req.query.contains_key("acl") {
                if has_version {
                    "s3:PutObjectVersionAcl"
                } else {
                    "s3:PutObjectAcl"
                }
            } else if req.query.contains_key("retention") {
                "s3:PutObjectRetention"
            } else if req.query.contains_key("legal-hold") {
                "s3:PutObjectLegalHold"
            } else {
                "s3:PutObject"
            }
        }
        "POST" => {
            if req.query.contains_key("select") {
                "s3:GetObject"
            } else {
                "s3:PutObject"
            }
        }
        "DELETE" => {
            if req.query.contains_key("uploadId") {
                "s3:AbortMultipartUpload"
            } else if has_version {
                "s3:DeleteObjectVersion"
            } else {
                "s3:DeleteObject"
            }
        }
        _ => "s3:GetObject",
    }
}

fn evaluate_bucket_policy(
    store: &FileBucketStore,
    req: &Request,
    ctx: &ActionContext,
    caller: Caller<'_>,
) -> PolicyDecision {
    let Ok(Some((raw, true))) = store.get_bucket_policy(&ctx.bucket) else {
        return PolicyDecision::ImplicitDeny;
    };
    let Ok(doc) = serde_json::from_slice::<Value>(&raw) else {
        return PolicyDecision::ImplicitDeny;
    };
    let statements = match doc.get("Statement") {
        Some(Value::Array(arr)) => arr.iter().collect::<Vec<_>>(),
        Some(obj @ Value::Object(_)) => vec![obj],
        _ => return PolicyDecision::ImplicitDeny,
    };

    let mut allowed = false;
    for stmt in statements {
        let Some(effect) = stmt.get("Effect").and_then(Value::as_str) else {
            continue;
        };
        if !statement_matches(stmt, req, ctx, caller) {
            continue;
        }
        if effect.eq_ignore_ascii_case("Deny") {
            return PolicyDecision::Deny;
        }
        if effect.eq_ignore_ascii_case("Allow") {
            allowed = true;
        }
    }
    if allowed {
        PolicyDecision::Allow
    } else {
        PolicyDecision::ImplicitDeny
    }
}

fn statement_matches(stmt: &Value, req: &Request, ctx: &ActionContext, caller: Caller<'_>) -> bool {
    let principal_ok = if let Some(p) = stmt.get("Principal") {
        principal_matches(p, caller)
    } else if let Some(np) = stmt.get("NotPrincipal") {
        !principal_matches(np, caller)
    } else {
        false
    };
    if !principal_ok {
        return false;
    }

    let action_ok = if let Some(a) = stmt.get("Action") {
        string_or_list_matches(a, ctx.action, true)
    } else if let Some(na) = stmt.get("NotAction") {
        !string_or_list_matches(na, ctx.action, true)
    } else {
        false
    };
    if !action_ok {
        return false;
    }

    let resource_ok = if let Some(r) = stmt.get("Resource") {
        string_or_list_matches(r, &ctx.resource_arn, false)
    } else if let Some(nr) = stmt.get("NotResource") {
        !string_or_list_matches(nr, &ctx.resource_arn, false)
    } else {
        false
    };
    if !resource_ok {
        return false;
    }

    if let Some(condition) = stmt.get("Condition") {
        if !condition_matches(condition, req, caller) {
            return false;
        }
    }
    true
}

fn principal_matches(principal: &Value, caller: Caller<'_>) -> bool {
    match principal {
        Value::String(s) => principal_pattern_matches(s, caller),
        Value::Object(map) => {
            if let Some(aws) = map.get("AWS") {
                match aws {
                    Value::String(s) => principal_pattern_matches(s, caller),
                    Value::Array(arr) => arr
                        .iter()
                        .filter_map(Value::as_str)
                        .any(|s| principal_pattern_matches(s, caller)),
                    _ => false,
                }
            } else {
                false
            }
        }
        _ => false,
    }
}

fn principal_pattern_matches(pattern: &str, caller: Caller<'_>) -> bool {
    if pattern == "*" {
        return true;
    }
    match caller {
        Caller::Anonymous => false,
        Caller::Authenticated { access_key_id } => {
            let candidates = [
                access_key_id.to_string(),
                format!("arn:aws:iam::000000000000:user/{access_key_id}"),
                "arn:aws:iam::000000000000:root".to_string(),
                "000000000000".to_string(),
            ];
            candidates.iter().any(|c| wildcard_match(pattern, c, false))
        }
    }
}

fn string_or_list_matches(value: &Value, candidate: &str, case_insensitive: bool) -> bool {
    match value {
        Value::String(pattern) => wildcard_match(pattern, candidate, case_insensitive),
        Value::Array(arr) => arr
            .iter()
            .filter_map(Value::as_str)
            .any(|pattern| wildcard_match(pattern, candidate, case_insensitive)),
        _ => false,
    }
}

fn condition_matches(condition: &Value, req: &Request, caller: Caller<'_>) -> bool {
    let Some(ops) = condition.as_object() else {
        return false;
    };
    for (op, entries) in ops {
        let Some(entries) = entries.as_object() else {
            return false;
        };
        for (key, expected) in entries {
            let actual = resolve_condition_key(key, req, caller);
            let matched = match op.as_str() {
                "StringEquals" => expected_strings(expected).iter().any(|e| actual == *e),
                "StringNotEquals" => expected_strings(expected).iter().all(|e| actual != *e),
                "StringLike" => expected_strings(expected)
                    .iter()
                    .any(|e| wildcard_match(e, &actual, false)),
                "StringNotLike" => expected_strings(expected)
                    .iter()
                    .all(|e| !wildcard_match(e, &actual, false)),
                "Bool" => expected_strings(expected)
                    .iter()
                    .any(|e| actual.eq_ignore_ascii_case(e)),
                _ => false,
            };
            if !matched {
                return false;
            }
        }
    }
    true
}

fn expected_strings(value: &Value) -> Vec<String> {
    match value {
        Value::String(s) => vec![s.clone()],
        Value::Bool(b) => vec![b.to_string()],
        Value::Array(arr) => arr
            .iter()
            .filter_map(|v| match v {
                Value::String(s) => Some(s.clone()),
                Value::Bool(b) => Some(b.to_string()),
                _ => None,
            })
            .collect(),
        _ => Vec::new(),
    }
}

fn resolve_condition_key(key: &str, req: &Request, caller: Caller<'_>) -> String {
    match key.to_ascii_lowercase().as_str() {
        "s3:prefix" => req.query.get("prefix").cloned().unwrap_or_default(),
        "s3:delimiter" => req.query.get("delimiter").cloned().unwrap_or_default(),
        "s3:x-amz-acl" => req.header("x-amz-acl").to_string(),
        "s3:x-amz-server-side-encryption" => req.header("x-amz-server-side-encryption").to_string(),
        "aws:securetransport" => "false".to_string(),
        "aws:username" => match caller {
            Caller::Authenticated { access_key_id } => access_key_id.to_string(),
            Caller::Anonymous => String::new(),
        },
        "aws:principalarn" => match caller {
            Caller::Authenticated { access_key_id } => {
                format!("arn:aws:iam::000000000000:user/{access_key_id}")
            }
            Caller::Anonymous => String::new(),
        },
        _ => String::new(),
    }
}

fn acl_allows_anonymous(store: &FileBucketStore, req: &Request, ctx: &ActionContext) -> bool {
    let bucket_acl = store
        .get_bucket_acl(&ctx.bucket)
        .ok()
        .flatten()
        .unwrap_or_else(|| "private".to_string());
    if ctx.key.is_empty() {
        return match ctx.action {
            "s3:ListBucket" | "s3:ListBucketVersions" => {
                matches!(bucket_acl.as_str(), "public-read" | "public-read-write")
            }
            _ => false,
        };
    }

    match ctx.action {
        "s3:GetObject" | "s3:GetObjectVersion" => {
            let version_id = req.query.get("versionId").map(String::as_str).unwrap_or("");
            let object_acl = store
                .get_object_acl(&ctx.bucket, &ctx.key, version_id)
                .ok()
                .flatten()
                .unwrap_or_else(|| "private".to_string());
            matches!(object_acl.as_str(), "public-read" | "public-read-write")
                || matches!(bucket_acl.as_str(), "public-read" | "public-read-write")
        }
        "s3:PutObject" | "s3:DeleteObject" | "s3:DeleteObjectVersion" => {
            bucket_acl == "public-read-write"
        }
        _ => false,
    }
}

/// Glob wildcard match supporting `*` (0+ chars) and `?` (1 char).
fn wildcard_match(pattern: &str, text: &str, case_insensitive: bool) -> bool {
    let p: Vec<char> = if case_insensitive {
        pattern.to_ascii_lowercase().chars().collect()
    } else {
        pattern.chars().collect()
    };
    let t: Vec<char> = if case_insensitive {
        text.to_ascii_lowercase().chars().collect()
    } else {
        text.chars().collect()
    };
    let (mut pi, mut ti) = (0usize, 0usize);
    let mut star: Option<(usize, usize)> = None;
    while ti < t.len() {
        if pi < p.len() && (p[pi] == '?' || p[pi] == t[ti]) {
            pi += 1;
            ti += 1;
        } else if pi < p.len() && p[pi] == '*' {
            star = Some((pi, ti));
            pi += 1;
        } else if let Some((star_pi, star_ti)) = star {
            pi = star_pi + 1;
            ti = star_ti + 1;
            star = Some((star_pi, ti));
        } else {
            return false;
        }
    }
    while pi < p.len() && p[pi] == '*' {
        pi += 1;
    }
    pi == p.len()
}
