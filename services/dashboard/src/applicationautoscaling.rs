//! Application Auto Scaling dashboard handler — read-only introspection over
//! the service's AWS JSON 1.1 provider protocol (`services/applicationautoscaling`).
//!
//! The service exposes no `/_introspect/` API (unlike DynamoDB/BigQuery/Redshift):
//! every read is a `POST /` with an `X-Amz-Target: AnyScaleFrontendService.<Action>`
//! header, mirroring what a real AWS SDK client would send. The dashboard only
//! ever issues the three `Describe*` actions (never a mutation), so this module
//! stays GET-only and read-only end to end. The service currently supports a
//! single `ServiceNamespace` ("dynamodb"; see `applicationautoscaling::server::
//! SUPPORTED_NAMESPACE`), so every request is scoped to it.

use serde_json::Value;

use crate::config::Config;
use crate::forward::{forward, ForwardError, ForwardRequest, ForwardResponse};
use crate::http::{Request, Response};

const TARGET_PREFIX: &str = "AnyScaleFrontendService.";
const SERVICE_NAMESPACE: &str = "dynamodb";

/// `GET /api/applicationautoscaling/status` — derives counts from the three
/// `Describe*` actions (there is no dedicated status/snapshot endpoint).
pub async fn handle_status(config: &Config, req: &Request) -> Response {
    if req.method != "GET" {
        return Response::method_not_allowed("GET");
    }
    let mut status = "disabled".to_string();
    let mut running = false;
    let mut scalable_target_count = 0usize;
    let mut scaling_policy_count = 0usize;
    let mut scheduled_action_count = 0usize;

    if !config.app_auto_scaling_base.is_empty() {
        status = "running".to_string();
        running = true;
        if let Ok(v) = describe(config, "DescribeScalableTargets").await {
            scalable_target_count = count_array(&v, "ScalableTargets");
        }
        if let Ok(v) = describe(config, "DescribeScalingPolicies").await {
            scaling_policy_count = count_array(&v, "ScalingPolicies");
        }
        if let Ok(v) = describe(config, "DescribeScheduledActions").await {
            scheduled_action_count = count_array(&v, "ScheduledActions");
        }
    }

    Response::json(
        200,
        &serde_json::json!({
            "service": "applicationautoscaling",
            "status": status,
            "running": running,
            "endpoint": if config.app_auto_scaling_endpoint.is_empty() {
                "http://127.0.0.1:18030".to_string()
            } else {
                config.app_auto_scaling_endpoint.clone()
            },
            "region": if config.app_auto_scaling_region.is_empty() {
                "us-east-1".to_string()
            } else {
                config.app_auto_scaling_region.clone()
            },
            "storagePath": config.app_auto_scaling_storage_path,
            "scalableTargetCount": scalable_target_count,
            "scalingPolicyCount": scaling_policy_count,
            "scheduledActionCount": scheduled_action_count,
        }),
    )
}

/// `GET /api/applicationautoscaling/scalable-targets` -> `{scalableTargets}`.
pub async fn handle_scalable_targets(config: &Config, req: &Request) -> Response {
    if req.method != "GET" {
        return Response::method_not_allowed("GET");
    }
    if config.app_auto_scaling_base.is_empty() {
        return Response::text_error(503, "applicationautoscaling service is disabled");
    }
    match describe(config, "DescribeScalableTargets").await {
        Ok(v) => Response::json(
            200,
            &serde_json::json!({ "scalableTargets": array_field(&v, "ScalableTargets") }),
        ),
        Err(resp) => resp,
    }
}

/// `GET /api/applicationautoscaling/scaling-policies` -> `{scalingPolicies}`.
pub async fn handle_scaling_policies(config: &Config, req: &Request) -> Response {
    if req.method != "GET" {
        return Response::method_not_allowed("GET");
    }
    if config.app_auto_scaling_base.is_empty() {
        return Response::text_error(503, "applicationautoscaling service is disabled");
    }
    match describe(config, "DescribeScalingPolicies").await {
        Ok(v) => Response::json(
            200,
            &serde_json::json!({ "scalingPolicies": array_field(&v, "ScalingPolicies") }),
        ),
        Err(resp) => resp,
    }
}

/// `GET /api/applicationautoscaling/scheduled-actions` -> `{scheduledActions}`.
pub async fn handle_scheduled_actions(config: &Config, req: &Request) -> Response {
    if req.method != "GET" {
        return Response::method_not_allowed("GET");
    }
    if config.app_auto_scaling_base.is_empty() {
        return Response::text_error(503, "applicationautoscaling service is disabled");
    }
    match describe(config, "DescribeScheduledActions").await {
        Ok(v) => Response::json(
            200,
            &serde_json::json!({ "scheduledActions": array_field(&v, "ScheduledActions") }),
        ),
        Err(resp) => resp,
    }
}

/// Issues a `Describe*` action against the provider protocol, scoped to the
/// only supported `ServiceNamespace`, and parses the JSON response body.
async fn describe(config: &Config, action: &str) -> Result<Value, Response> {
    let body = serde_json::to_vec(&serde_json::json!({ "ServiceNamespace": SERVICE_NAMESPACE }))
        .unwrap_or_default();
    match forward(ForwardRequest {
        base: &config.app_auto_scaling_base,
        method: "POST",
        path: "/",
        headers: vec![
            (
                "Content-Type".to_string(),
                "application/x-amz-json-1.1".to_string(),
            ),
            (
                "X-Amz-Target".to_string(),
                format!("{TARGET_PREFIX}{action}"),
            ),
        ],
        body,
    })
    .await
    {
        Ok(resp) if resp.status == 200 => {
            serde_json::from_slice(&resp.body).map_err(|_| invalid_json())
        }
        Ok(resp) => Err(relay(resp)),
        Err(e) => Err(forward_failure(e)),
    }
}

fn array_field(v: &Value, key: &str) -> Value {
    v.get(key).cloned().unwrap_or(Value::Array(vec![]))
}

fn count_array(v: &Value, key: &str) -> usize {
    v.get(key)
        .and_then(Value::as_array)
        .map(|a| a.len())
        .unwrap_or(0)
}

fn relay(resp: ForwardResponse) -> Response {
    let content_type = {
        let ct = resp.header("content-type");
        if ct.is_empty() {
            "application/json".to_string()
        } else {
            ct.to_string()
        }
    };
    Response::new(resp.status, &content_type, resp.body)
}

fn invalid_json() -> Response {
    Response::text_error(502, "applicationautoscaling service returned invalid json")
}

fn forward_failure(err: ForwardError) -> Response {
    match err {
        ForwardError::Unreachable(_) => {
            Response::text_error(502, "applicationautoscaling service is unreachable")
        }
        ForwardError::BadBase => Response::text_error(
            500,
            "applicationautoscaling service address is misconfigured",
        ),
        ForwardError::BadResponse => Response::text_error(
            502,
            "applicationautoscaling service returned an invalid response",
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn req(method: &str) -> Request {
        Request {
            method: method.to_string(),
            path: "/api/applicationautoscaling/status".to_string(),
            raw_path: "/api/applicationautoscaling/status".to_string(),
            query: String::new(),
            headers: std::collections::HashMap::new(),
            body: Vec::new(),
        }
    }

    #[tokio::test]
    async fn status_reports_disabled_when_base_empty() {
        let cfg = Config::default();
        let resp = handle_status(&cfg, &req("GET")).await;
        assert_eq!(resp.status, 200);
        let v: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(v["status"], "disabled");
        assert_eq!(v["running"], false);
    }

    #[tokio::test]
    async fn status_rejects_non_get() {
        let cfg = Config::default();
        let resp = handle_status(&cfg, &req("POST")).await;
        assert_eq!(resp.status, 405);
    }

    #[tokio::test]
    async fn scalable_targets_disabled_returns_503() {
        let cfg = Config::default();
        let resp = handle_scalable_targets(&cfg, &req("GET")).await;
        assert_eq!(resp.status, 503);
    }

    #[tokio::test]
    async fn scaling_policies_rejects_non_get() {
        let cfg = Config::default();
        let resp = handle_scaling_policies(&cfg, &req("POST")).await;
        assert_eq!(resp.status, 405);
    }

    #[tokio::test]
    async fn scheduled_actions_disabled_returns_503() {
        let cfg = Config::default();
        let resp = handle_scheduled_actions(&cfg, &req("GET")).await;
        assert_eq!(resp.status, 503);
    }
}
