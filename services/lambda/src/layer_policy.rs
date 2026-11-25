//! Layer version resource-based policies: `AddLayerVersionPermission`,
//! `GetLayerVersionPolicy`, and `RemoveLayerVersionPermission`.
//!
//! Statements are validated like AWS (action `lambda:GetLayerVersion`,
//! principal `*` or a 12-digit account, optional organization id with `*`),
//! persisted on the layer version, and returned as an IAM policy document.
//! They are not consulted on access: every caller of a devcloud instance is
//! the layer owner's own account (layers of other accounts cannot exist).

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::layers::{layer_version_arn, parse_layer_name, parse_version_number};
use crate::server::{
    invalid_param, parse_json, revision_mismatch, str_field, validation, Reply, Server,
};

const GET_LAYER_VERSION: &str = "lambda:GetLayerVersion";

/// One `AddLayerVersionPermission` statement.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "PascalCase")]
pub(crate) struct LayerStatement {
    pub statement_id: String,
    pub action: String,
    /// `*` or a 12-digit account id.
    pub principal: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub organization_id: String,
}

impl LayerStatement {
    /// The statement as it appears in the policy document.
    fn to_json(&self, resource: &str) -> Value {
        let principal = if self.principal == "*" {
            json!("*")
        } else {
            json!({ "AWS": format!("arn:aws:iam::{}:root", self.principal) })
        };
        let mut v = json!({
            "Sid": self.statement_id,
            "Effect": "Allow",
            "Principal": principal,
            "Action": self.action,
            "Resource": resource,
        });
        if !self.organization_id.is_empty() {
            v["Condition"] = json!({
                "StringEquals": { "aws:PrincipalOrgID": self.organization_id }
            });
        }
        v
    }
}

fn valid_statement_id(id: &str) -> bool {
    !id.is_empty()
        && id.len() <= 100
        && id
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
}

/// `*`, a 12-digit account id, or `arn:aws:iam::<account>:root` (normalized
/// to the account id).
fn parse_principal(raw: &str) -> Option<String> {
    let is_account = |s: &str| s.len() == 12 && s.bytes().all(|b| b.is_ascii_digit());
    if raw == "*" || is_account(raw) {
        return Some(raw.to_string());
    }
    raw.strip_prefix("arn:aws:iam::")
        .and_then(|rest| rest.strip_suffix(":root"))
        .filter(|acct| is_account(acct))
        .map(str::to_string)
}

fn valid_organization_id(id: &str) -> bool {
    id.strip_prefix("o-").is_some_and(|rest| {
        (10..=32).contains(&rest.len())
            && rest
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit())
    })
}

/// Parses an `AddLayerVersionPermission` body into a statement.
pub(crate) fn parse_statement(req: &Value) -> Result<LayerStatement, Reply> {
    let statement_id = str_field(req, "StatementId");
    if statement_id.is_empty() {
        return Err(validation("1 validation error detected: Value null at 'statementId' failed to satisfy constraint: Member must not be null"));
    }
    if !valid_statement_id(&statement_id) {
        return Err(validation(&format!(
            "1 validation error detected: Value '{statement_id}' at 'statementId' failed to satisfy constraint: Member must satisfy regular expression pattern: ([a-zA-Z0-9-_]+)"
        )));
    }
    let action = str_field(req, "Action");
    if action.is_empty() {
        return Err(validation("1 validation error detected: Value null at 'action' failed to satisfy constraint: Member must not be null"));
    }
    if action != GET_LAYER_VERSION {
        return Err(validation(&format!(
            "1 validation error detected: Value '{action}' at 'action' failed to satisfy constraint: Member must satisfy regular expression pattern: lambda:GetLayerVersion"
        )));
    }
    let raw_principal = str_field(req, "Principal");
    if raw_principal.is_empty() {
        return Err(validation("1 validation error detected: Value null at 'principal' failed to satisfy constraint: Member must not be null"));
    }
    let Some(principal) = parse_principal(&raw_principal) else {
        return Err(validation(&format!(
            "1 validation error detected: Value '{raw_principal}' at 'principal' failed to satisfy constraint: Member must satisfy regular expression pattern: \\d{{12}}|\\*|arn:(aws[a-zA-Z-]*):iam::\\d{{12}}:root"
        )));
    };
    let organization_id = str_field(req, "OrganizationId");
    if !organization_id.is_empty() {
        if !valid_organization_id(&organization_id) {
            return Err(validation(&format!(
                "1 validation error detected: Value '{organization_id}' at 'organizationId' failed to satisfy constraint: Member must satisfy regular expression pattern: o-[a-z0-9]{{10,32}}"
            )));
        }
        if principal != "*" {
            return Err(invalid_param(
                "OrganizationId can only be specified with Principal '*'",
            ));
        }
    }
    Ok(LayerStatement {
        statement_id,
        action,
        principal,
        organization_id,
    })
}

impl Server {
    fn layer_policy_not_found(&self, name: &str, version: u64) -> Reply {
        Reply::error(
            404,
            "ResourceNotFoundException",
            &format!(
                "No policy is associated with the given resource: {}",
                layer_version_arn(self.region(), self.account_id(), name, version)
            ),
        )
    }

    fn layer_target(&self, raw_name: &str, raw_version: &str) -> Result<(String, u64), Reply> {
        let name = parse_layer_name(raw_name, self.region(), self.account_id())?;
        let version = parse_version_number(raw_version)?;
        Ok((name, version))
    }

    /// `POST /2018-10-31/layers/<name>/versions/<n>/policy`
    /// (`AddLayerVersionPermission`).
    pub fn add_layer_version_permission(
        &self,
        raw_name: &str,
        raw_version: &str,
        query: &BTreeMap<String, String>,
        body: &[u8],
    ) -> Reply {
        let req = match parse_json(body) {
            Ok(v) => v,
            Err(r) => return r,
        };
        let (name, version) = match self.layer_target(raw_name, raw_version) {
            Ok(t) => t,
            Err(r) => return r,
        };
        let stmt = match parse_statement(&req) {
            Ok(s) => s,
            Err(r) => return r,
        };
        let expected_revision = query.get("RevisionId").filter(|r| !r.is_empty()).cloned();
        let new_revision = self.new_id();
        let resource = layer_version_arn(self.region(), self.account_id(), &name, version);
        let result = self.commit(|st| {
            let rec = st
                .layers
                .get_mut(&name)
                .and_then(|l| l.versions.get_mut(&version))
                .ok_or_else(|| self.layer_not_found(&name, version))?;
            if expected_revision
                .as_ref()
                .is_some_and(|want| *want != rec.policy_revision_id)
            {
                return Err(revision_mismatch());
            }
            if rec
                .policy
                .iter()
                .any(|s| s.statement_id == stmt.statement_id)
            {
                return Err(Reply::error(
                    409,
                    "ResourceConflictException",
                    &format!(
                        "The statement id ({}) provided already exists. Please provide a new statement id, or remove the existing statement.",
                        stmt.statement_id
                    ),
                ));
            }
            rec.policy.push(stmt.clone());
            rec.policy_revision_id = new_revision.clone();
            Ok(())
        });
        match result {
            Ok(()) => Reply::json(
                201,
                &json!({
                    "Statement": stmt.to_json(&resource).to_string(),
                    "RevisionId": new_revision,
                }),
            ),
            Err(r) => r,
        }
    }

    /// `GET /2018-10-31/layers/<name>/versions/<n>/policy`
    /// (`GetLayerVersionPolicy`).
    pub fn get_layer_version_policy(&self, raw_name: &str, raw_version: &str) -> Reply {
        let (name, version) = match self.layer_target(raw_name, raw_version) {
            Ok(t) => t,
            Err(r) => return r,
        };
        let st = self.state.lock().unwrap();
        let Some(rec) = st.layers.get(&name).and_then(|l| l.versions.get(&version)) else {
            return self.layer_not_found(&name, version);
        };
        if rec.policy.is_empty() {
            return self.layer_policy_not_found(&name, version);
        }
        let resource = layer_version_arn(self.region(), self.account_id(), &name, version);
        let statements: Vec<Value> = rec.policy.iter().map(|s| s.to_json(&resource)).collect();
        let policy = json!({
            "Version": "2012-10-17",
            "Id": "default",
            "Statement": statements,
        });
        Reply::json(
            200,
            &json!({
                "Policy": policy.to_string(),
                "RevisionId": rec.policy_revision_id,
            }),
        )
    }

    /// `DELETE /2018-10-31/layers/<name>/versions/<n>/policy/<sid>`
    /// (`RemoveLayerVersionPermission`).
    pub fn remove_layer_version_permission(
        &self,
        raw_name: &str,
        raw_version: &str,
        statement_id: &str,
        query: &BTreeMap<String, String>,
    ) -> Reply {
        let (name, version) = match self.layer_target(raw_name, raw_version) {
            Ok(t) => t,
            Err(r) => return r,
        };
        let expected_revision = query.get("RevisionId").filter(|r| !r.is_empty()).cloned();
        let new_revision = self.new_id();
        let result = self.commit(|st| {
            let rec = st
                .layers
                .get_mut(&name)
                .and_then(|l| l.versions.get_mut(&version))
                .ok_or_else(|| self.layer_not_found(&name, version))?;
            if expected_revision
                .as_ref()
                .is_some_and(|want| *want != rec.policy_revision_id)
            {
                return Err(revision_mismatch());
            }
            let before = rec.policy.len();
            rec.policy.retain(|s| s.statement_id != statement_id);
            if rec.policy.len() == before {
                return Err(Reply::error(
                    404,
                    "ResourceNotFoundException",
                    &format!("Statement {statement_id} is not found in resource policy."),
                ));
            }
            rec.policy_revision_id = if rec.policy.is_empty() {
                String::new()
            } else {
                new_revision.clone()
            };
            Ok(())
        });
        match result {
            Ok(()) => Reply::empty(204),
            Err(r) => r,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_principals_and_statements() {
        assert_eq!(parse_principal("*").as_deref(), Some("*"));
        assert_eq!(
            parse_principal("123456789012").as_deref(),
            Some("123456789012")
        );
        assert_eq!(
            parse_principal("arn:aws:iam::123456789012:root").as_deref(),
            Some("123456789012")
        );
        assert!(parse_principal("12345").is_none());
        assert!(parse_principal("arn:aws:iam::123456789012:user/x").is_none());

        let ok = parse_statement(&json!({
            "StatementId": "org",
            "Action": "lambda:GetLayerVersion",
            "Principal": "*",
            "OrganizationId": "o-abcdefghij",
        }))
        .unwrap();
        assert_eq!(ok.organization_id, "o-abcdefghij");
        let doc = ok.to_json("arn:x");
        assert_eq!(
            doc["Condition"]["StringEquals"]["aws:PrincipalOrgID"],
            "o-abcdefghij"
        );

        for bad in [
            json!({ "Action": "lambda:GetLayerVersion", "Principal": "*" }),
            json!({ "StatementId": "a b", "Action": "lambda:GetLayerVersion", "Principal": "*" }),
            json!({ "StatementId": "a", "Action": "lambda:InvokeFunction", "Principal": "*" }),
            json!({ "StatementId": "a", "Action": "lambda:GetLayerVersion", "Principal": "bob" }),
            json!({ "StatementId": "a", "Action": "lambda:GetLayerVersion", "Principal": "123456789012", "OrganizationId": "o-abcdefghij" }),
            json!({ "StatementId": "a", "Action": "lambda:GetLayerVersion", "Principal": "*", "OrganizationId": "org" }),
        ] {
            assert!(parse_statement(&bad).is_err(), "{bad}");
        }
    }
}
