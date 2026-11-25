//! Session policy evaluation for requests signed with STS temporary
//! credentials (`strict` auth mode only).
//!
//! Same subset and semantics as the S3 IAM registry (`services/s3/src/iam.rs`):
//! statements match on `Action`/`NotAction` (case-insensitive `*`/`?`
//! wildcards), `Resource`/`NotResource` (case-sensitive wildcards), and
//! `Condition` with `StringEquals`, `StringNotEquals`, `StringLike`,
//! `StringNotLike`, and `Bool`; any other operator never matches. An explicit
//! `Deny` wins, otherwise an `Allow` is required.

use serde_json::Value;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Decision {
    Allow,
    Deny,
    ImplicitDeny,
}

/// What a request does, and the condition keys it carries.
pub struct RequestContext<'a> {
    pub action: &'a str,
    pub resource: &'a str,
    /// `aws:PrincipalArn`: the role ARN of the calling session.
    pub principal_arn: &'a str,
    /// `aws:PrincipalAccount`.
    pub principal_account: &'a str,
    /// `sts:RoleSessionName` of an `AssumeRole` request.
    pub role_session_name: &'a str,
}

/// Evaluates one session policy document against `ctx`.
pub fn evaluate(doc: &Value, ctx: &RequestContext) -> Decision {
    let statements = match doc.get("Statement") {
        Some(Value::Array(arr)) => arr.iter().collect::<Vec<_>>(),
        Some(obj @ Value::Object(_)) => vec![obj],
        _ => return Decision::ImplicitDeny,
    };
    let mut allowed = false;
    for stmt in statements {
        let Some(effect) = stmt.get("Effect").and_then(Value::as_str) else {
            continue;
        };
        if !statement_matches(stmt, ctx) {
            continue;
        }
        if effect.eq_ignore_ascii_case("Deny") {
            return Decision::Deny;
        }
        if effect.eq_ignore_ascii_case("Allow") {
            allowed = true;
        }
    }
    if allowed {
        Decision::Allow
    } else {
        Decision::ImplicitDeny
    }
}

fn statement_matches(stmt: &Value, ctx: &RequestContext) -> bool {
    let action_ok = if let Some(a) = stmt.get("Action") {
        string_or_list_matches(a, ctx.action, true)
    } else if let Some(na) = stmt.get("NotAction") {
        !string_or_list_matches(na, ctx.action, true)
    } else {
        false
    };
    let resource_ok = if let Some(r) = stmt.get("Resource") {
        string_or_list_matches(r, ctx.resource, false)
    } else if let Some(nr) = stmt.get("NotResource") {
        !string_or_list_matches(nr, ctx.resource, false)
    } else {
        false
    };
    action_ok
        && resource_ok
        && stmt
            .get("Condition")
            .is_none_or(|condition| condition_matches(condition, ctx))
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

fn condition_matches(condition: &Value, ctx: &RequestContext) -> bool {
    let Some(ops) = condition.as_object() else {
        return false;
    };
    for (op, entries) in ops {
        let Some(entries) = entries.as_object() else {
            return false;
        };
        for (key, expected) in entries {
            let actual = condition_key(key, ctx);
            let expected = expected_strings(expected);
            let matched = match op.as_str() {
                "StringEquals" => expected.contains(&actual),
                "StringNotEquals" => expected.iter().all(|e| actual != *e),
                "StringLike" => expected.iter().any(|e| wildcard_match(e, &actual, false)),
                "StringNotLike" => expected.iter().all(|e| !wildcard_match(e, &actual, false)),
                "Bool" => expected.iter().any(|e| actual.eq_ignore_ascii_case(e)),
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

fn condition_key(key: &str, ctx: &RequestContext) -> String {
    match key.to_ascii_lowercase().as_str() {
        "aws:securetransport" => "false".to_string(),
        "aws:principalarn" => ctx.principal_arn.to_string(),
        "aws:principalaccount" => ctx.principal_account.to_string(),
        "sts:rolesessionname" => ctx.role_session_name.to_string(),
        _ => String::new(),
    }
}

/// Glob match with `*` (0+ chars) and `?` (1 char).
fn wildcard_match(pattern: &str, text: &str, case_insensitive: bool) -> bool {
    let fold = |s: &str| -> Vec<char> {
        if case_insensitive {
            s.to_ascii_lowercase().chars().collect()
        } else {
            s.chars().collect()
        }
    };
    let (p, t) = (fold(pattern), fold(text));
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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn ctx(resource: &str) -> RequestContext<'_> {
        RequestContext {
            action: "sts:AssumeRole",
            resource,
            principal_arn: "arn:aws:iam::111122223333:role/base",
            principal_account: "111122223333",
            role_session_name: "next",
        }
    }

    #[test]
    fn allow_deny_and_implicit_deny() {
        let doc = json!({"Statement": [
            {"Effect": "Allow", "Action": "STS:Assume*", "Resource": "arn:aws:iam::*:role/app-*"},
            {"Effect": "Deny", "Action": "*", "Resource": "arn:aws:iam::*:role/app-admin"},
        ]});
        assert_eq!(
            evaluate(&doc, &ctx("arn:aws:iam::1:role/app-web")),
            Decision::Allow
        );
        assert_eq!(
            evaluate(&doc, &ctx("arn:aws:iam::1:role/app-admin")),
            Decision::Deny
        );
        assert_eq!(
            evaluate(&doc, &ctx("arn:aws:iam::1:role/other")),
            Decision::ImplicitDeny
        );
        assert_eq!(
            evaluate(&json!({"Version": "2012-10-17"}), &ctx("x")),
            Decision::ImplicitDeny
        );
    }

    #[test]
    fn not_action_not_resource_and_conditions() {
        let doc = json!({"Statement": {
            "Effect": "Allow", "NotAction": "s3:*", "NotResource": "arn:aws:iam::*:role/secret",
            "Condition": {"StringLike": {"sts:RoleSessionName": "ne*"}, "Bool": {"aws:SecureTransport": false}}
        }});
        assert_eq!(
            evaluate(&doc, &ctx("arn:aws:iam::1:role/r")),
            Decision::Allow
        );
        assert_eq!(
            evaluate(&doc, &ctx("arn:aws:iam::1:role/secret")),
            Decision::ImplicitDeny
        );
        let unknown_op = json!({"Statement": {"Effect": "Allow", "Action": "*", "Resource": "*",
            "Condition": {"NumericLessThan": {"sts:DurationSeconds": "1000"}}}});
        assert_eq!(evaluate(&unknown_op, &ctx("r")), Decision::ImplicitDeny);
    }
}
