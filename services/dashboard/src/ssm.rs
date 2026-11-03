//! SSM Parameter Store dashboard handler — read-only, over the service's AWS
//! JSON 1.1 provider protocol (`services/ssm`). Only `DescribeParameters` is
//! issued, so parameter values (including `SecureString` plaintext) never
//! reach the dashboard.

use serde_json::{json, Value};

use crate::config::Config;
use crate::forward::{forward, ForwardError, ForwardRequest, ForwardResponse};
use crate::http::{Request, Response};

/// Upper bound on `DescribeParameters` pages fetched for one listing (50 each).
const MAX_PAGES: usize = 40;

/// `GET /api/ssm/status`.
pub async fn handle_status(config: &Config, req: &Request) -> Response {
    if req.method != "GET" {
        return Response::method_not_allowed("GET");
    }
    let running = !config.ssm_base.is_empty();
    let parameter_count = if running {
        list_parameters(config).await.map(|p| p.len()).unwrap_or(0)
    } else {
        0
    };
    Response::json(
        200,
        &json!({
            "service": "ssm",
            "status": if running { "running" } else { "disabled" },
            "running": running,
            "endpoint": config.ssm_endpoint,
            "region": config.ssm_region,
            "storagePath": config.ssm_storage_path,
            "parameterCount": parameter_count,
        }),
    )
}

/// `GET /api/ssm/parameters` -> `{parameters}` (metadata only).
pub async fn handle_parameters(config: &Config, req: &Request) -> Response {
    if req.method != "GET" {
        return Response::method_not_allowed("GET");
    }
    if config.ssm_base.is_empty() {
        return Response::text_error(503, "ssm service is disabled");
    }
    match list_parameters(config).await {
        Ok(parameters) => Response::json(200, &json!({ "parameters": parameters })),
        Err(resp) => resp,
    }
}

async fn list_parameters(config: &Config) -> Result<Vec<Value>, Response> {
    let mut out = Vec::new();
    let mut next_token = String::new();
    for _ in 0..MAX_PAGES {
        let mut body = json!({ "MaxResults": 50 });
        if !next_token.is_empty() {
            body["NextToken"] = json!(next_token);
        }
        let page = describe(config, body).await?;
        if let Some(items) = page.get("Parameters").and_then(Value::as_array) {
            out.extend(items.iter().cloned());
        }
        match page.get("NextToken").and_then(Value::as_str) {
            Some(token) if !token.is_empty() => next_token = token.to_string(),
            _ => break,
        }
    }
    Ok(out)
}

async fn describe(config: &Config, body: Value) -> Result<Value, Response> {
    match forward(ForwardRequest {
        base: &config.ssm_base,
        method: "POST",
        path: "/",
        headers: vec![
            (
                "Content-Type".to_string(),
                "application/x-amz-json-1.1".to_string(),
            ),
            (
                "X-Amz-Target".to_string(),
                "AmazonSSM.DescribeParameters".to_string(),
            ),
        ],
        body: serde_json::to_vec(&body).unwrap_or_default(),
    })
    .await
    {
        Ok(resp) if resp.status == 200 => serde_json::from_slice(&resp.body)
            .map_err(|_| Response::text_error(502, "ssm service returned invalid json")),
        Ok(resp) => Err(relay(resp)),
        Err(e) => Err(forward_failure(e)),
    }
}

fn relay(resp: ForwardResponse) -> Response {
    let ct = resp.header("content-type");
    let content_type = if ct.is_empty() {
        "application/json"
    } else {
        ct
    }
    .to_string();
    Response::new(resp.status, &content_type, resp.body)
}

fn forward_failure(err: ForwardError) -> Response {
    match err {
        ForwardError::Unreachable(_) => Response::text_error(502, "ssm service is unreachable"),
        ForwardError::BadBase => Response::text_error(500, "ssm service address is misconfigured"),
        ForwardError::BadResponse => {
            Response::text_error(502, "ssm service returned an invalid response")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn req(method: &str, path: &str) -> Request {
        Request {
            method: method.to_string(),
            path: path.to_string(),
            raw_path: path.to_string(),
            query: String::new(),
            headers: std::collections::HashMap::new(),
            body: Vec::new(),
        }
    }

    #[tokio::test]
    async fn disabled_service() {
        let cfg = Config::default();
        let resp = handle_status(&cfg, &req("GET", "/api/ssm/status")).await;
        assert_eq!(resp.status, 200);
        let v: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(v["status"], "disabled");
        let resp = handle_parameters(&cfg, &req("GET", "/api/ssm/parameters")).await;
        assert_eq!(resp.status, 503);
    }

    #[tokio::test]
    async fn rejects_non_get() {
        let cfg = Config::default();
        let resp = handle_parameters(&cfg, &req("POST", "/api/ssm/parameters")).await;
        assert_eq!(resp.status, 405);
    }
}
