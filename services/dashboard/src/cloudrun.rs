//! Cloud Run dashboard handler — read-only views over the service's
//! introspection surface (`/_introspect/services|instances|logs`) and the
//! Admin API v2 revisions list (`services/cloudrun`). Never touches storage.

use serde_json::{json, Value};

use crate::config::Config;
use crate::forward::{forward, ForwardError, ForwardRequest, ForwardResponse};
use crate::http::{path_segment_decode, Request, Response};

const DISABLED: &str = "cloudrun service is disabled";

/// `GET /api/cloudrun/status`.
pub async fn handle_status(config: &Config, req: &Request) -> Response {
    if req.method != "GET" {
        return Response::method_not_allowed("GET");
    }
    let running = !config.cloudrun_base.is_empty();
    let mut service_count = 0usize;
    let mut instance_count = 0usize;
    if running {
        if let Ok(v) = get_json(config, "/_introspect/services").await {
            service_count = count(&v, "services");
        }
        if let Ok(v) = get_json(config, "/_introspect/instances").await {
            instance_count = count(&v, "instances");
        }
    }
    Response::json(
        200,
        &json!({
            "service": "cloudrun",
            "status": if running { "running" } else { "disabled" },
            "running": running,
            "endpoint": or_default(&config.cloudrun_endpoint, "http://127.0.0.1:18095"),
            "project": or_default(&config.cloudrun_project, "devcloud"),
            "region": or_default(&config.cloudrun_region, "us-central1"),
            "storagePath": config.cloudrun_storage_path,
            "serviceCount": service_count,
            "instanceCount": instance_count,
        }),
    )
}

/// `GET /api/cloudrun/services` -> `{services}` across every project/location.
pub async fn handle_services(config: &Config, req: &Request) -> Response {
    passthrough(config, req, "/_introspect/services", "services").await
}

/// `GET /api/cloudrun/instances` -> `{instances}` (running local instances).
pub async fn handle_instances(config: &Config, req: &Request) -> Response {
    passthrough(config, req, "/_introspect/instances", "instances").await
}

/// `GET /api/cloudrun/services/<project>/<location>/<service>/revisions` and
/// `GET /api/cloudrun/services/<project>/<location>/<service>/logs`.
pub async fn handle_service(config: &Config, req: &Request) -> Response {
    if req.method != "GET" {
        return Response::method_not_allowed("GET");
    }
    if config.cloudrun_base.is_empty() {
        return Response::text_error(503, DISABLED);
    }
    let rest = req
        .raw_path
        .strip_prefix("/api/cloudrun/services/")
        .unwrap_or("");
    let parts: Vec<Option<String>> = rest.split('/').map(path_segment_decode).collect();
    let [Some(project), Some(location), Some(service), Some(view)] = parts.as_slice() else {
        return Response::text_error(404, "404 page not found");
    };
    // The names are spliced into the upstream request line: accept resource
    // ids only (no CR/LF, spaces, `?`, ...) and re-encode them anyway.
    if ![project, location, service]
        .iter()
        .all(|n| is_resource_id(n))
    {
        return Response::text_error(400, "invalid Cloud Run resource name");
    }
    let (project, location, service) = (encode(project), encode(location), encode(service));
    match view.as_str() {
        "revisions" => {
            // Follow nextPageToken: the UI has no paging and must see every
            // revision, not just the first 100.
            let base =
                format!("/v2/projects/{project}/locations/{location}/services/{service}/revisions");
            let mut revisions: Vec<Value> = Vec::new();
            let mut token = String::new();
            loop {
                let path = if token.is_empty() {
                    base.clone()
                } else {
                    format!("{base}?pageToken={}", encode(&token))
                };
                let page = match get_json(config, &path).await {
                    Ok(v) => v,
                    Err(resp) => return resp,
                };
                if let Some(items) = page.get("revisions").and_then(Value::as_array) {
                    revisions.extend(items.iter().cloned());
                }
                match page.get("nextPageToken").and_then(Value::as_str) {
                    Some(next) if !next.is_empty() && next != token => token = next.to_string(),
                    _ => break,
                }
            }
            Response::json(200, &json!({ "revisions": revisions }))
        }
        "logs" => {
            let path = format!("/_introspect/logs/{project}/{location}/{service}");
            match get_json(config, &path).await {
                Ok(v) => Response::json(
                    200,
                    &json!({ "lines": v.get("lines").cloned().unwrap_or(json!([])) }),
                ),
                Err(resp) => resp,
            }
        }
        _ => Response::text_error(404, "404 page not found"),
    }
}

async fn passthrough(config: &Config, req: &Request, path: &str, key: &str) -> Response {
    if req.method != "GET" {
        return Response::method_not_allowed("GET");
    }
    if config.cloudrun_base.is_empty() {
        return Response::text_error(503, DISABLED);
    }
    match get_json(config, path).await {
        Ok(v) => Response::json(
            200,
            &json!({ key: v.get(key).cloned().unwrap_or(json!([])) }),
        ),
        Err(resp) => resp,
    }
}

async fn get_json(config: &Config, path: &str) -> Result<Value, Response> {
    match forward(ForwardRequest {
        base: &config.cloudrun_base,
        method: "GET",
        path,
        headers: auth_headers(config),
        body: Vec::new(),
    })
    .await
    {
        Ok(resp) if resp.status == 200 => serde_json::from_slice(&resp.body)
            .map_err(|_| Response::text_error(502, "cloudrun service returned invalid json")),
        Ok(resp) => Err(relay(resp)),
        Err(e) => Err(forward_failure(e)),
    }
}

/// The configured bearer token when the Cloud Run service runs in strict mode.
fn auth_headers(config: &Config) -> Vec<(String, String)> {
    if config.cloudrun_auth_mode.eq_ignore_ascii_case("strict")
        && !config.cloudrun_bearer_token.is_empty()
    {
        vec![(
            "Authorization".to_string(),
            format!("Bearer {}", config.cloudrun_bearer_token),
        )]
    } else {
        Vec::new()
    }
}

/// Project / location / service ids: letters, digits and `-._:`.
fn is_resource_id(name: &str) -> bool {
    !name.is_empty()
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'.' | b'_' | b':'))
}

/// Percent-encodes everything outside the RFC 3986 unreserved set.
fn encode(value: &str) -> String {
    value
        .bytes()
        .map(|b| match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
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
        ForwardError::Unreachable(_) => {
            Response::text_error(502, "cloudrun service is unreachable")
        }
        ForwardError::BadBase => {
            Response::text_error(500, "cloudrun service address is misconfigured")
        }
        ForwardError::BadResponse => {
            Response::text_error(502, "cloudrun service returned an invalid response")
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
        let resp = handle_status(&Config::default(), &req("GET", "/api/cloudrun/status")).await;
        let v: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(v["status"], "disabled");
    }

    #[tokio::test]
    async fn services_rejects_non_get() {
        let resp =
            handle_services(&Config::default(), &req("POST", "/api/cloudrun/services")).await;
        assert_eq!(resp.status, 405);
    }

    #[tokio::test]
    async fn service_view_rejects_header_injection() {
        let cfg = Config {
            cloudrun_base: "http://127.0.0.1:1".to_string(),
            ..Config::default()
        };
        for path in [
            "/api/cloudrun/services/p%0D%0AX-Injected%3A%20yes/l/web/revisions",
            "/api/cloudrun/services/p/l%0A/web/logs",
            "/api/cloudrun/services/p/l/we%20b/logs",
        ] {
            let resp = handle_service(&cfg, &req("GET", path)).await;
            assert_eq!(resp.status, 400, "{path}");
        }
        assert_eq!(encode("a:b c"), "a%3Ab%20c");
    }

    #[tokio::test]
    async fn service_view_rejects_traversal() {
        let cfg = Config {
            cloudrun_base: "http://127.0.0.1:1".to_string(),
            ..Config::default()
        };
        let resp = handle_service(
            &cfg,
            &req("GET", "/api/cloudrun/services/p/l/%2e%2e/revisions"),
        )
        .await;
        assert_eq!(resp.status, 404);
    }
}
