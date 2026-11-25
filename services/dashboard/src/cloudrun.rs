//! Cloud Run dashboard handler over the service's introspection surface
//! (`/_introspect/services|instances|logs`) and the Admin API v2 provider
//! protocol (`/v2/projects/{p}/locations/{l}/services...` in
//! `services/cloudrun`). Mutations always forward through the Admin API v2
//! provider protocol, never touching storage directly.

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

/// `POST /api/cloudrun/services/create` -> `CreateService` via Admin API v2
/// (`POST /v2/projects/{project}/locations/{location}/services?serviceId={serviceId}`).
pub async fn handle_create_service(config: &Config, req: &Request) -> Response {
    if req.method != "POST" {
        return Response::method_not_allowed("POST");
    }
    if config.cloudrun_base.is_empty() {
        return Response::text_error(503, DISABLED);
    }
    create_service(config, req).await
}

/// `GET /api/cloudrun/instances` -> `{instances}` (running local instances).
pub async fn handle_instances(config: &Config, req: &Request) -> Response {
    passthrough(config, req, "/_introspect/instances", "instances").await
}

/// `/api/cloudrun/services/<project>/<location>/<service>[/<subview>]`:
/// - `GET    .../revisions` -> `{revisions}` (follows `nextPageToken`)
/// - `DELETE .../revisions/<revision>` -> `DeleteRevision` via Admin API v2
/// - `GET    .../logs` -> `{lines}`
/// - `GET    .../iam` -> `GetIamPolicy` via Admin API v2
/// - `POST   .../iam` -> `SetIamPolicy` via Admin API v2
/// - `POST   .../update` or `PATCH ...` -> `UpdateService` via Admin API v2
/// - `DELETE ...` or `POST .../delete` -> `DeleteService` via Admin API v2
pub async fn handle_service(config: &Config, req: &Request) -> Response {
    if config.cloudrun_base.is_empty() {
        return Response::text_error(503, DISABLED);
    }
    let rest = req
        .raw_path
        .strip_prefix("/api/cloudrun/services/")
        .unwrap_or("");
    let parts: Vec<Option<String>> = rest.split('/').map(path_segment_decode).collect();
    if parts.len() < 3 || parts.iter().any(Option::is_none) {
        return Response::text_error(404, "404 page not found");
    }
    let decoded: Vec<String> = parts.into_iter().map(|p| p.unwrap()).collect();
    let (raw_project, raw_location, raw_service) = (&decoded[0], &decoded[1], &decoded[2]);
    // The names are spliced into the upstream request line: accept resource
    // ids only (no CR/LF, spaces, `?`, ...) and re-encode them anyway.
    if ![raw_project, raw_location, raw_service]
        .iter()
        .all(|n| is_resource_id(n))
    {
        return Response::text_error(400, "invalid Cloud Run resource name");
    }
    let (project, location, service) = (
        encode(raw_project),
        encode(raw_location),
        encode(raw_service),
    );
    let tail = &decoded[3..];

    match tail {
        [] => match req.method.as_str() {
            "PATCH" => update_service(config, req, &project, &location, &service).await,
            "DELETE" => {
                delete_service(config, req, &project, &location, &service, raw_service).await
            }
            _ => Response::method_not_allowed("PATCH, DELETE"),
        },
        [view] if view == "update" => {
            if req.method != "POST" && req.method != "PATCH" {
                return Response::method_not_allowed("POST, PATCH");
            }
            update_service(config, req, &project, &location, &service).await
        }
        [view] if view == "delete" => {
            if req.method != "POST" && req.method != "DELETE" {
                return Response::method_not_allowed("POST, DELETE");
            }
            delete_service(config, req, &project, &location, &service, raw_service).await
        }
        [view] if view == "revisions" => {
            if req.method != "GET" {
                return Response::method_not_allowed("GET");
            }
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
        [view, rev] if view == "revisions" => {
            if req.method != "DELETE" {
                return Response::method_not_allowed("DELETE");
            }
            if !is_resource_id(rev) {
                return Response::text_error(400, "invalid Cloud Run resource name");
            }
            let path = format!(
                "/v2/projects/{project}/locations/{location}/services/{service}/revisions/{}",
                encode(rev)
            );
            forward_admin(config, "DELETE", &path, Vec::new()).await
        }
        [view] if view == "logs" => {
            if req.method != "GET" {
                return Response::method_not_allowed("GET");
            }
            let path = format!("/_introspect/logs/{project}/{location}/{service}");
            match get_json(config, &path).await {
                Ok(v) => Response::json(
                    200,
                    &json!({ "lines": v.get("lines").cloned().unwrap_or(json!([])) }),
                ),
                Err(resp) => resp,
            }
        }
        [view] if view == "iam" => match req.method.as_str() {
            "GET" => {
                let path = format!(
                    "/v2/projects/{project}/locations/{location}/services/{service}:getIamPolicy"
                );
                forward_admin(config, "GET", &path, Vec::new()).await
            }
            "POST" => {
                let payload = match serde_json::from_slice::<Value>(&req.body) {
                    Ok(v) if v.is_object() => v,
                    _ => return Response::text_error(400, "invalid json request"),
                };
                let body = if payload.get("policy").is_some() {
                    req.body.clone()
                } else {
                    serde_json::to_vec(&json!({ "policy": payload })).unwrap_or_default()
                };
                let path = format!(
                    "/v2/projects/{project}/locations/{location}/services/{service}:setIamPolicy"
                );
                forward_admin(config, "POST", &path, body).await
            }
            _ => Response::method_not_allowed("GET, POST"),
        },
        _ => Response::text_error(404, "404 page not found"),
    }
}

async fn create_service(config: &Config, req: &Request) -> Response {
    let payload = match serde_json::from_slice::<Value>(&req.body) {
        Ok(v) if v.is_object() => v,
        _ => return Response::text_error(400, "invalid json request"),
    };
    let default_project = or_default(&config.cloudrun_project, "devcloud");
    let default_region = or_default(&config.cloudrun_region, "us-central1");
    let project = payload
        .get("project")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .unwrap_or(&default_project);
    let location = payload
        .get("location")
        .or_else(|| payload.get("region"))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .unwrap_or(&default_region);
    let service_id = payload
        .get("serviceId")
        .and_then(Value::as_str)
        .map(str::trim)
        .unwrap_or("");
    if service_id.is_empty() {
        return Response::text_error(400, "serviceId is required");
    }
    if ![project, location, service_id]
        .iter()
        .all(|n| is_resource_id(n))
    {
        return Response::text_error(400, "invalid Cloud Run resource name");
    }
    let service_body = match payload.get("service") {
        Some(v) if v.is_object() => v.clone(),
        Some(_) => return Response::text_error(400, "service must be a JSON object"),
        None => {
            let mut m = payload.as_object().unwrap().clone();
            m.remove("project");
            m.remove("location");
            m.remove("region");
            m.remove("serviceId");
            Value::Object(m)
        }
    };
    let path = format!(
        "/v2/projects/{}/locations/{}/services?serviceId={}",
        encode(project),
        encode(location),
        encode(service_id)
    );
    let body = serde_json::to_vec(&service_body).unwrap_or_default();
    forward_admin(config, "POST", &path, body).await
}

async fn update_service(
    config: &Config,
    req: &Request,
    project: &str,
    location: &str,
    service: &str,
) -> Response {
    let payload = match serde_json::from_slice::<Value>(&req.body) {
        Ok(v) if v.is_object() => v,
        _ => return Response::text_error(400, "invalid json request"),
    };
    let mut update_mask = query_param(&req.query, "updateMask").unwrap_or_default();
    if update_mask.is_empty() {
        if let Some(m) = payload.get("updateMask").and_then(Value::as_str) {
            update_mask = m.trim().to_string();
        }
    }
    let service_body = match payload.get("service") {
        Some(v) if v.is_object() => v.clone(),
        Some(_) => return Response::text_error(400, "service must be a JSON object"),
        None => {
            let mut m = payload.as_object().unwrap().clone();
            m.remove("updateMask");
            Value::Object(m)
        }
    };
    let mut path = format!("/v2/projects/{project}/locations/{location}/services/{service}");
    if !update_mask.is_empty() {
        path.push_str(&format!("?updateMask={}", encode(&update_mask)));
    }
    let body = serde_json::to_vec(&service_body).unwrap_or_default();
    forward_admin(config, "PATCH", &path, body).await
}

async fn delete_service(
    config: &Config,
    req: &Request,
    project: &str,
    location: &str,
    service: &str,
    raw_service: &str,
) -> Response {
    if !req.body.iter().all(u8::is_ascii_whitespace) {
        let payload = match serde_json::from_slice::<Value>(&req.body) {
            Ok(v) if v.is_object() => v,
            _ => return Response::text_error(400, "invalid json request"),
        };
        if let Some(conf) = payload.get("confirmation").and_then(Value::as_str) {
            let full_name = format!(
                "projects/{}/locations/{}/services/{raw_service}",
                percent_decode_segment(project),
                percent_decode_segment(location)
            );
            if conf != raw_service && conf != full_name {
                return Response::text_error(400, "confirmation must match service name");
            }
        }
    }
    let path = format!("/v2/projects/{project}/locations/{location}/services/{service}");
    forward_admin(config, "DELETE", &path, Vec::new()).await
}

async fn forward_admin(config: &Config, method: &str, path: &str, body: Vec<u8>) -> Response {
    let mut headers = auth_headers(config);
    if !body.is_empty() {
        headers.push(("Content-Type".to_string(), "application/json".to_string()));
    }
    match forward(ForwardRequest {
        base: &config.cloudrun_base,
        method,
        path,
        headers,
        body,
    })
    .await
    {
        Ok(resp) => relay(resp),
        Err(e) => forward_failure(e),
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

fn percent_decode_segment(s: &str) -> String {
    path_segment_decode(s).unwrap_or_else(|| s.to_string())
}

fn query_param(query: &str, key: &str) -> Option<String> {
    for pair in query.split('&') {
        if let Some((k, v)) = pair.split_once('=') {
            if k == key {
                return path_segment_decode(v);
            }
        }
    }
    None
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
    async fn create_service_disabled_returns_503() {
        let resp = handle_create_service(
            &Config::default(),
            &req("POST", "/api/cloudrun/services/create"),
        )
        .await;
        assert_eq!(resp.status, 503);
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

    #[tokio::test]
    async fn service_delete_rejects_mismatched_confirmation() {
        let cfg = Config {
            cloudrun_base: "http://127.0.0.1:1".to_string(),
            ..Config::default()
        };
        let mut r = req("DELETE", "/api/cloudrun/services/p/us-central1/web");
        r.body = br#"{"confirmation":"wrong"}"#.to_vec();
        let resp = handle_service(&cfg, &r).await;
        assert_eq!(resp.status, 400);
    }
}
