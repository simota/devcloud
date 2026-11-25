//! Application Auto Scaling dashboard handler over the service's AWS JSON 1.1
//! provider protocol (`services/applicationautoscaling`).
//!
//! The service exposes no `/_introspect/` or `/_control/` API: every read and
//! mutation is a `POST /` with an `X-Amz-Target: AnyScaleFrontendService.<Action>`
//! header, mirroring what an AWS SDK client sends. Reads issue `Describe*`,
//! and mutations issue `RegisterScalableTarget`, `DeregisterScalableTarget`,
//! `PutScalingPolicy`, `DeleteScalingPolicy`, `PutScheduledAction`, and
//! `DeleteScheduledAction` through the provider protocol (never directly
//! touching storage). The service supports a single `ServiceNamespace`
//! (`"dynamodb"`; see `applicationautoscaling::server::SUPPORTED_NAMESPACE`),
//! so every request defaults `ServiceNamespace` to it when omitted.

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
/// `POST /api/applicationautoscaling/scalable-targets` -> `RegisterScalableTarget`.
pub async fn handle_scalable_targets(config: &Config, req: &Request) -> Response {
    if config.app_auto_scaling_base.is_empty() {
        return Response::text_error(503, "applicationautoscaling service is disabled");
    }
    match req.method.as_str() {
        "GET" => match describe(config, "DescribeScalableTargets").await {
            Ok(v) => Response::json(
                200,
                &serde_json::json!({ "scalableTargets": array_field(&v, "ScalableTargets") }),
            ),
            Err(resp) => resp,
        },
        "POST" => forward_action(config, req, "RegisterScalableTarget", None).await,
        _ => Response::method_not_allowed("GET, POST"),
    }
}

/// `POST /api/applicationautoscaling/scalable-targets/deregister` -> `DeregisterScalableTarget`.
pub async fn handle_deregister_scalable_target(config: &Config, req: &Request) -> Response {
    if req.method != "POST" {
        return Response::method_not_allowed("POST");
    }
    if config.app_auto_scaling_base.is_empty() {
        return Response::text_error(503, "applicationautoscaling service is disabled");
    }
    forward_action(
        config,
        req,
        "DeregisterScalableTarget",
        Some(("ResourceId", "confirmation must match ResourceId")),
    )
    .await
}

/// `GET /api/applicationautoscaling/scaling-policies` -> `{scalingPolicies}`.
/// `POST /api/applicationautoscaling/scaling-policies` -> `PutScalingPolicy`.
pub async fn handle_scaling_policies(config: &Config, req: &Request) -> Response {
    if config.app_auto_scaling_base.is_empty() {
        return Response::text_error(503, "applicationautoscaling service is disabled");
    }
    match req.method.as_str() {
        "GET" => match describe(config, "DescribeScalingPolicies").await {
            Ok(v) => Response::json(
                200,
                &serde_json::json!({ "scalingPolicies": array_field(&v, "ScalingPolicies") }),
            ),
            Err(resp) => resp,
        },
        "POST" => forward_action(config, req, "PutScalingPolicy", None).await,
        _ => Response::method_not_allowed("GET, POST"),
    }
}

/// `POST /api/applicationautoscaling/scaling-policies/delete` -> `DeleteScalingPolicy`.
pub async fn handle_delete_scaling_policy(config: &Config, req: &Request) -> Response {
    if req.method != "POST" {
        return Response::method_not_allowed("POST");
    }
    if config.app_auto_scaling_base.is_empty() {
        return Response::text_error(503, "applicationautoscaling service is disabled");
    }
    forward_action(
        config,
        req,
        "DeleteScalingPolicy",
        Some(("PolicyName", "confirmation must match PolicyName")),
    )
    .await
}

/// `GET /api/applicationautoscaling/scheduled-actions` -> `{scheduledActions}`.
/// `POST /api/applicationautoscaling/scheduled-actions` -> `PutScheduledAction`.
pub async fn handle_scheduled_actions(config: &Config, req: &Request) -> Response {
    if config.app_auto_scaling_base.is_empty() {
        return Response::text_error(503, "applicationautoscaling service is disabled");
    }
    match req.method.as_str() {
        "GET" => match describe(config, "DescribeScheduledActions").await {
            Ok(v) => Response::json(
                200,
                &serde_json::json!({ "scheduledActions": array_field(&v, "ScheduledActions") }),
            ),
            Err(resp) => resp,
        },
        "POST" => forward_action(config, req, "PutScheduledAction", None).await,
        _ => Response::method_not_allowed("GET, POST"),
    }
}

/// `POST /api/applicationautoscaling/scheduled-actions/delete` -> `DeleteScheduledAction`.
pub async fn handle_delete_scheduled_action(config: &Config, req: &Request) -> Response {
    if req.method != "POST" {
        return Response::method_not_allowed("POST");
    }
    if config.app_auto_scaling_base.is_empty() {
        return Response::text_error(503, "applicationautoscaling service is disabled");
    }
    forward_action(
        config,
        req,
        "DeleteScheduledAction",
        Some((
            "ScheduledActionName",
            "confirmation must match ScheduledActionName",
        )),
    )
    .await
}

/// Issues a `Describe*` action against the provider protocol, scoped to the
/// only supported `ServiceNamespace`, and parses the JSON response body.
async fn describe(config: &Config, action: &str) -> Result<Value, Response> {
    let body = serde_json::to_vec(&serde_json::json!({ "ServiceNamespace": SERVICE_NAMESPACE }))
        .unwrap_or_default();
    match call_provider(config, action, body).await {
        Ok(resp) if resp.status == 200 => {
            serde_json::from_slice(&resp.body).map_err(|_| invalid_json())
        }
        Ok(resp) => Err(relay(resp)),
        Err(e) => Err(forward_failure(e)),
    }
}

/// Forwards a dashboard mutation to the Application Auto Scaling provider
/// protocol (`POST /` with `X-Amz-Target: AnyScaleFrontendService.<action>`).
/// Accepts either `{input: {...}, confirmation?: "..."}` or a direct payload
/// object, defaulting `ServiceNamespace` to `"dynamodb"` when omitted.
async fn forward_action(
    config: &Config,
    req: &Request,
    action: &str,
    confirm_field: Option<(&str, &str)>,
) -> Response {
    let payload: Value = match serde_json::from_slice(&req.body) {
        Ok(v) => v,
        Err(_) => return Response::text_error(400, "invalid json request"),
    };
    let (raw_input, confirmation) = match payload.get("input") {
        Some(v) => (
            v.clone(),
            payload
                .get("confirmation")
                .and_then(Value::as_str)
                .map(str::to_string),
        ),
        None => (
            payload.clone(),
            payload
                .get("confirmation")
                .and_then(Value::as_str)
                .map(str::to_string),
        ),
    };
    let mut obj = match raw_input {
        Value::Object(mut m) => {
            m.remove("confirmation");
            m
        }
        _ => return Response::text_error(400, "input must be a JSON object"),
    };
    if let Some((field, err_msg)) = confirm_field {
        let target = obj.get(field).and_then(Value::as_str).unwrap_or("");
        if let Some(conf) = confirmation.as_deref() {
            if target.is_empty() || conf != target {
                return Response::text_error(400, err_msg);
            }
        }
    }
    let needs_ns = obj
        .get("ServiceNamespace")
        .and_then(Value::as_str)
        .map(str::is_empty)
        .unwrap_or(true);
    if needs_ns {
        obj.insert(
            "ServiceNamespace".to_string(),
            Value::String(SERVICE_NAMESPACE.to_string()),
        );
    }
    let body = match serde_json::to_vec(&Value::Object(obj)) {
        Ok(b) => b,
        Err(_) => return Response::text_error(400, "input could not be encoded"),
    };
    match call_provider(config, action, body).await {
        Ok(resp) => relay(resp),
        Err(e) => forward_failure(e),
    }
}

async fn call_provider(
    config: &Config,
    action: &str,
    body: Vec<u8>,
) -> Result<ForwardResponse, ForwardError> {
    let mut headers = vec![
        (
            "Content-Type".to_string(),
            "application/x-amz-json-1.1".to_string(),
        ),
        (
            "X-Amz-Target".to_string(),
            format!("{TARGET_PREFIX}{action}"),
        ),
    ];
    headers.extend(auth_headers(config, "POST", "/", &body));
    forward(ForwardRequest {
        base: &config.app_auto_scaling_base,
        method: "POST",
        path: "/",
        headers,
        body,
    })
    .await
}

/// SigV4 headers for the configured credentials when Application Auto Scaling
/// verifies signatures (`strict` / `signed-relaxed`); none in relaxed mode.
fn auth_headers(config: &Config, method: &str, path: &str, body: &[u8]) -> Vec<(String, String)> {
    let mode = config.app_auto_scaling_auth_mode.as_str();
    if mode.is_empty() || mode.eq_ignore_ascii_case("relaxed") {
        return Vec::new();
    }
    let host = config
        .app_auto_scaling_base
        .split_once("://")
        .map(|(_, rest)| rest)
        .unwrap_or(&config.app_auto_scaling_base)
        .trim_end_matches('/');
    let region = if config.app_auto_scaling_region.is_empty() {
        "us-east-1"
    } else {
        &config.app_auto_scaling_region
    };
    let access_key_id = if config.app_auto_scaling_access_key_id.is_empty() {
        "dev"
    } else {
        &config.app_auto_scaling_access_key_id
    };
    let secret_access_key = if config.app_auto_scaling_secret_access_key.is_empty() {
        "dev"
    } else {
        &config.app_auto_scaling_secret_access_key
    };
    crate::sigv4::sign(
        method,
        path,
        host,
        body,
        &crate::sigv4::Credentials {
            access_key_id,
            secret_access_key,
            region,
            service: "application-autoscaling",
        },
        &crate::sigv4::amz_date_now(),
    )
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
    async fn scaling_policies_rejects_unsupported_method() {
        let cfg = Config {
            app_auto_scaling_base: "http://127.0.0.1:1".to_string(),
            ..Config::default()
        };
        let resp = handle_scaling_policies(&cfg, &req("PUT")).await;
        assert_eq!(resp.status, 405);
    }

    #[tokio::test]
    async fn scheduled_actions_disabled_returns_503() {
        let cfg = Config::default();
        let resp = handle_scheduled_actions(&cfg, &req("GET")).await;
        assert_eq!(resp.status, 503);
    }

    #[tokio::test]
    async fn deregister_rejects_mismatched_confirmation() {
        let cfg = Config {
            app_auto_scaling_base: "http://127.0.0.1:1".to_string(),
            ..Config::default()
        };
        let mut r = req("POST");
        r.body = br#"{"input":{"ResourceId":"table/orders","ScalableDimension":"dynamodb:table:ReadCapacityUnits"},"confirmation":"wrong"}"#.to_vec();
        let resp = handle_deregister_scalable_target(&cfg, &r).await;
        assert_eq!(resp.status, 400);
    }
}
