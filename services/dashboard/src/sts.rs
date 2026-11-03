//! STS dashboard handler — read-only, over the service's AWS Query provider
//! protocol (`services/sts`). Only `GetCallerIdentity` is issued; the
//! dashboard never calls `AssumeRole`, so it never handles credentials.

use serde_json::json;

use crate::config::Config;
use crate::forward::{forward, ForwardError, ForwardRequest};
use crate::http::{Request, Response};

/// `GET /api/sts/status`.
pub async fn handle_status(config: &Config, req: &Request) -> Response {
    if req.method != "GET" {
        return Response::method_not_allowed("GET");
    }
    let running = !config.sts_base.is_empty();
    Response::json(
        200,
        &json!({
            "service": "sts",
            "status": if running { "running" } else { "disabled" },
            "running": running,
            "endpoint": config.sts_endpoint,
            "region": config.sts_region,
        }),
    )
}

/// `GET /api/sts/caller-identity` -> `{account, arn, userId}`.
pub async fn handle_caller_identity(config: &Config, req: &Request) -> Response {
    if req.method != "GET" {
        return Response::method_not_allowed("GET");
    }
    if config.sts_base.is_empty() {
        return Response::text_error(503, "sts service is disabled");
    }
    let result = forward(ForwardRequest {
        base: &config.sts_base,
        method: "POST",
        path: "/",
        headers: vec![(
            "Content-Type".to_string(),
            "application/x-www-form-urlencoded".to_string(),
        )],
        body: b"Action=GetCallerIdentity&Version=2011-06-15".to_vec(),
    })
    .await;
    match result {
        Ok(resp) if resp.status == 200 => {
            let xml = String::from_utf8_lossy(&resp.body);
            Response::json(
                200,
                &json!({
                    "account": tag(&xml, "Account"),
                    "arn": tag(&xml, "Arn"),
                    "userId": tag(&xml, "UserId"),
                }),
            )
        }
        Ok(resp) => {
            Response::text_error(502, &format!("sts service returned status {}", resp.status))
        }
        Err(ForwardError::Unreachable(_)) => {
            Response::text_error(502, "sts service is unreachable")
        }
        Err(ForwardError::BadBase) => {
            Response::text_error(500, "sts service address is misconfigured")
        }
        Err(ForwardError::BadResponse) => {
            Response::text_error(502, "sts service returned an invalid response")
        }
    }
}

fn tag(xml: &str, name: &str) -> String {
    let open = format!("<{name}>");
    let close = format!("</{name}>");
    xml.find(&open)
        .map(|i| i + open.len())
        .and_then(|start| {
            xml[start..]
                .find(&close)
                .map(|end| xml[start..start + end].to_string())
        })
        .unwrap_or_default()
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
        let resp = handle_status(&cfg, &req("GET", "/api/sts/status")).await;
        let v: serde_json::Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(v["status"], "disabled");
        let resp = handle_caller_identity(&cfg, &req("GET", "/api/sts/caller-identity")).await;
        assert_eq!(resp.status, 503);
    }

    #[test]
    fn tag_extraction() {
        let xml = "<a><Account>123</Account><Arn>arn:x</Arn></a>";
        assert_eq!(tag(xml, "Account"), "123");
        assert_eq!(tag(xml, "Missing"), "");
    }
}
