//! Mirrors the queue-policy + redrive-policy types/parsing from
//! `internal/services/sqs/{types,tags_policy,queue_attributes}.go`.

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Mirrors `queuePolicy` (json field order Version, Id, Statement — note `Id`,
/// not `ID`, which is load-bearing for byte-exact AddPermission output).
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct QueuePolicy {
    #[serde(rename = "Version", default)]
    pub version: String,
    #[serde(rename = "Id", default)]
    pub id: String,
    #[serde(rename = "Statement", default)]
    pub statement: Vec<QueuePolicyStatement>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct QueuePolicyStatement {
    #[serde(rename = "Sid", default)]
    pub sid: String,
    #[serde(rename = "Effect", default)]
    pub effect: String,
    #[serde(rename = "Principal", default)]
    pub principal: QueuePolicyPrincipal,
    #[serde(rename = "Action", default)]
    pub action: Vec<String>,
    #[serde(rename = "Resource", default)]
    pub resource: String,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct QueuePolicyPrincipal {
    #[serde(rename = "AWS", default)]
    pub aws: Vec<String>,
}

/// Mirrors `redrivePolicy`.
#[derive(Clone, Debug, Default)]
pub struct RedrivePolicy {
    pub dead_letter_target_arn: String,
    pub max_receive_count: i64,
}

/// Mirrors `redriveAllowPolicy`.
#[derive(Clone, Debug, Default)]
pub struct RedriveAllowPolicy {
    pub permission: String,
    pub source_queue_arns: Vec<String>,
}

/// Mirrors `parseRedrivePolicy`: accepts maxReceiveCount as a JSON string or an
/// integral number; requires deadLetterTargetArn.
pub fn parse_redrive_policy(raw: &str) -> Result<RedrivePolicy, String> {
    let values: Value =
        serde_json::from_str(raw).map_err(|_| "RedrivePolicy must be valid JSON".to_string())?;
    let mut policy = RedrivePolicy::default();
    if let Some(arn) = values.get("deadLetterTargetArn").and_then(Value::as_str) {
        policy.dead_letter_target_arn = arn.to_string();
    }
    match values.get("maxReceiveCount") {
        Some(Value::String(s)) => {
            policy.max_receive_count = s
                .parse()
                .map_err(|_| "RedrivePolicy maxReceiveCount must be an integer".to_string())?;
        }
        Some(Value::Number(n)) => {
            let f = n.as_f64().unwrap_or(f64::NAN);
            if f != f.trunc() {
                return Err("RedrivePolicy maxReceiveCount must be an integer".to_string());
            }
            policy.max_receive_count = f as i64;
        }
        _ => {}
    }
    if policy.dead_letter_target_arn.is_empty() {
        return Err("RedrivePolicy deadLetterTargetArn is required".to_string());
    }
    Ok(policy)
}

/// Mirrors `parseRedriveAllowPolicy`.
pub fn parse_redrive_allow_policy(raw: &str) -> Result<RedriveAllowPolicy, String> {
    let values: Value = serde_json::from_str(raw)
        .map_err(|_| "RedriveAllowPolicy must be valid JSON".to_string())?;
    let mut policy = RedriveAllowPolicy {
        permission: "allowAll".to_string(),
        source_queue_arns: Vec::new(),
    };
    if let Some(p) = values.get("redrivePermission").and_then(Value::as_str) {
        if !p.is_empty() {
            policy.permission = p.to_string();
        }
    }
    match policy.permission.as_str() {
        "allowAll" | "denyAll" => {}
        "byQueue" => {
            let arns = values
                .get("sourceQueueArns")
                .and_then(Value::as_array)
                .filter(|a| !a.is_empty())
                .ok_or_else(|| {
                    "RedriveAllowPolicy sourceQueueArns is required for byQueue".to_string()
                })?;
            if arns.len() > 10 {
                return Err(
                    "RedriveAllowPolicy sourceQueueArns must contain no more than 10 queues"
                        .to_string(),
                );
            }
            for raw_arn in arns {
                match raw_arn.as_str() {
                    Some(arn) if !arn.is_empty() => policy.source_queue_arns.push(arn.to_string()),
                    _ => {
                        return Err("RedriveAllowPolicy sourceQueueArns must contain queue ARNs"
                            .to_string())
                    }
                }
            }
        }
        _ => {
            return Err(
                "RedriveAllowPolicy redrivePermission must be allowAll, denyAll, or byQueue"
                    .to_string(),
            )
        }
    }
    Ok(policy)
}

/// Mirrors `normalizedPermissionActions`: drop empties; prefix bare actions with
/// `SQS:` (leave `*` and already-namespaced actions untouched).
pub fn normalized_permission_actions(actions: &[String]) -> Vec<String> {
    let mut out = Vec::with_capacity(actions.len());
    for action in actions {
        if action.is_empty() {
            continue;
        }
        if action == "*" || action.contains(':') {
            out.push(action.clone());
        } else {
            out.push(format!("SQS:{action}"));
        }
    }
    out
}
