//! Lambda dashboard handler over the service's provider protocol
//! (`services/lambda`): reads go through `GET /2015-03-31/functions/` and the
//! read-only `GET /_introspect/invocations`; the one mutation (test invoke)
//! goes through the real `Invoke` API, never through storage. Payloads and
//! logs are relayed to the browser but never logged.

use base64::Engine;
use serde_json::{json, Value};

use crate::config::Config;
use crate::forward::{forward, ForwardError, ForwardRequest, ForwardResponse};
use crate::http::{path_segment_decode, Request, Response};

const DISABLED: &str = "lambda service is disabled";

/// `GET /api/lambda/status`.
pub async fn handle_status(config: &Config, req: &Request) -> Response {
    if req.method != "GET" {
        return Response::method_not_allowed("GET");
    }
    let running = !config.lambda_base.is_empty();
    let mut function_count = 0usize;
    let mut invocation_count = 0usize;
    if running {
        if let Ok(functions) = all_functions(config).await {
            function_count = functions.len();
        }
        if let Ok(v) = get_json(config, "/_introspect/invocations").await {
            invocation_count = count(&v, "invocations");
        }
    }
    Response::json(
        200,
        &json!({
            "service": "lambda",
            "status": if running { "running" } else { "disabled" },
            "running": running,
            "endpoint": or_default(&config.lambda_endpoint, "http://127.0.0.1:19010"),
            "region": or_default(&config.lambda_region, "us-east-1"),
            "storagePath": config.lambda_storage_path,
            "functionCount": function_count,
            "invocationCount": invocation_count,
        }),
    )
}

/// `GET /api/lambda/functions` -> `{functions}` (every page).
pub async fn handle_functions(config: &Config, req: &Request) -> Response {
    if req.method != "GET" {
        return Response::method_not_allowed("GET");
    }
    if config.lambda_base.is_empty() {
        return Response::text_error(503, DISABLED);
    }
    match all_functions(config).await {
        Ok(functions) => Response::json(200, &json!({ "functions": functions })),
        Err(resp) => resp,
    }
}

/// Follows `NextMarker` until every `ListFunctions` page has been read.
async fn all_functions(config: &Config) -> Result<Vec<Value>, Response> {
    let mut functions: Vec<Value> = Vec::new();
    let mut marker = String::new();
    loop {
        let path = if marker.is_empty() {
            "/2015-03-31/functions/".to_string()
        } else {
            format!("/2015-03-31/functions/?Marker={marker}")
        };
        let page = get_json(config, &path).await?;
        if let Some(items) = page.get("Functions").and_then(Value::as_array) {
            functions.extend(items.iter().cloned());
        }
        match page.get("NextMarker").and_then(Value::as_str) {
            Some(next) if !next.is_empty() => marker = next.to_string(),
            _ => return Ok(functions),
        }
    }
}

/// `GET /api/lambda/invocations` -> `{invocations}` (newest first).
pub async fn handle_invocations(config: &Config, req: &Request) -> Response {
    if req.method != "GET" {
        return Response::method_not_allowed("GET");
    }
    if config.lambda_base.is_empty() {
        return Response::text_error(503, DISABLED);
    }
    match get_json(config, "/_introspect/invocations").await {
        Ok(v) => Response::json(
            200,
            &json!({ "invocations": v.get("invocations").cloned().unwrap_or(json!([])) }),
        ),
        Err(resp) => resp,
    }
}

/// `POST /api/lambda/functions/<name>/invoke` — synchronous test invoke with
/// the request body as the event. Returns `{statusCode, functionError,
/// payload, log}`.
pub async fn handle_invoke(config: &Config, req: &Request) -> Response {
    if req.method != "POST" {
        return Response::method_not_allowed("POST");
    }
    // Invoking runs code: refuse cross-site browser requests (CSRF). The SPA
    // itself is served from the loopback dashboard origin.
    if !crate::http::trusted_origin(req.header("origin"), req.header("sec-fetch-site")) {
        return Response::text_error(403, "cross-origin requests are not allowed");
    }
    if config.lambda_base.is_empty() {
        return Response::text_error(503, DISABLED);
    }
    let Some(raw) = req
        .raw_path
        .strip_prefix("/api/lambda/functions/")
        .and_then(|rest| rest.strip_suffix("/invoke"))
    else {
        return Response::text_error(404, "404 page not found");
    };
    let Some(name) = path_segment_decode(raw).filter(|n| !n.is_empty() && !n.contains('/')) else {
        return Response::text_error(400, "invalid function name");
    };
    let path = format!(
        "/2015-03-31/functions/{}/invocations",
        encode_segment(&name)
    );
    let resp = match forward(ForwardRequest {
        base: &config.lambda_base,
        method: "POST",
        path: &path,
        headers: [
            ("Content-Type".to_string(), "application/json".to_string()),
            (
                "X-Amz-Invocation-Type".to_string(),
                "RequestResponse".to_string(),
            ),
            ("X-Amz-Log-Type".to_string(), "Tail".to_string()),
        ]
        .into_iter()
        .chain(auth_headers(config, "POST", &path, &req.body))
        .collect(),
        body: req.body.clone(),
    })
    .await
    {
        Ok(r) => r,
        Err(e) => return forward_failure(e),
    };
    if resp.status != 200 {
        return relay(resp);
    }
    let log = base64::engine::general_purpose::STANDARD
        .decode(resp.header("x-amz-log-result"))
        .map(|b| String::from_utf8_lossy(&b).into_owned())
        .unwrap_or_default();
    let function_error = resp.header("x-amz-function-error");
    Response::json(
        200,
        &json!({
            "statusCode": resp.status,
            "functionError": if function_error.is_empty() { Value::Null } else { json!(function_error) },
            "payload": String::from_utf8_lossy(&resp.body),
            "log": log,
        }),
    )
}

async fn get_json(config: &Config, path: &str) -> Result<Value, Response> {
    match forward(ForwardRequest {
        base: &config.lambda_base,
        method: "GET",
        path,
        headers: auth_headers(config, "GET", path, b""),
        body: Vec::new(),
    })
    .await
    {
        Ok(resp) if resp.status == 200 => serde_json::from_slice(&resp.body)
            .map_err(|_| Response::text_error(502, "lambda service returned invalid json")),
        Ok(resp) => Err(relay(resp)),
        Err(e) => Err(forward_failure(e)),
    }
}

/// SigV4 headers for the configured credentials when the Lambda service
/// verifies signatures (`strict` / `signed-relaxed`); none in relaxed mode.
fn auth_headers(config: &Config, method: &str, path: &str, body: &[u8]) -> Vec<(String, String)> {
    let mode = config.lambda_auth_mode.as_str();
    if mode.is_empty() || mode.eq_ignore_ascii_case("relaxed") {
        return Vec::new();
    }
    let host = config
        .lambda_base
        .split_once("://")
        .map(|(_, rest)| rest)
        .unwrap_or(&config.lambda_base)
        .trim_end_matches('/');
    let region = or_default(&config.lambda_region, "us-east-1");
    crate::sigv4::sign(
        method,
        path,
        host,
        body,
        &crate::sigv4::Credentials {
            access_key_id: &config.lambda_access_key_id,
            secret_access_key: &config.lambda_secret_access_key,
            region: &region,
            service: "lambda",
        },
        &crate::sigv4::amz_date_now(),
    )
}

fn encode_segment(s: &str) -> String {
    s.bytes()
        .map(|b| match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                (b as char).to_string()
            }
            _ => format!("%{b:02X}"),
        })
        .collect()
}

fn count(v: &Value, key: &str) -> usize {
    v.get(key)
        .and_then(Value::as_array)
        .map(Vec::len)
        .unwrap_or(0)
}

fn or_default(value: &str, fallback: &str) -> String {
    if value.is_empty() {
        fallback.to_string()
    } else {
        value.to_string()
    }
}

fn relay(resp: ForwardResponse) -> Response {
    let ct = resp.header("content-type");
    let ct = if ct.is_empty() {
        "application/json"
    } else {
        ct
    }
    .to_string();
    Response::new(resp.status, &ct, resp.body)
}

fn forward_failure(err: ForwardError) -> Response {
    match err {
        ForwardError::Unreachable(_) => Response::text_error(502, "lambda service is unreachable"),
        ForwardError::BadBase => {
            Response::text_error(500, "lambda service address is misconfigured")
        }
        ForwardError::BadResponse => {
            Response::text_error(502, "lambda service returned an invalid response")
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
    async fn status_reports_disabled_when_base_empty() {
        let resp = handle_status(&Config::default(), &req("GET", "/api/lambda/status")).await;
        let v: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(v["status"], "disabled");
        assert_eq!(v["running"], false);
    }

    #[tokio::test]
    async fn functions_disabled_returns_503() {
        let resp = handle_functions(&Config::default(), &req("GET", "/api/lambda/functions")).await;
        assert_eq!(resp.status, 503);
    }

    #[tokio::test]
    async fn invoke_requires_post() {
        let resp = handle_invoke(
            &Config::default(),
            &req("GET", "/api/lambda/functions/f/invoke"),
        )
        .await;
        assert_eq!(resp.status, 405);
    }

    #[test]
    fn segment_encoding() {
        assert_eq!(encode_segment("my-fn_1"), "my-fn_1");
        assert_eq!(encode_segment("a:b"), "a%3Ab");
    }
}
