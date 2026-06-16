//! Pub/Sub dashboard handler — ports `internal/dashboard/pubsub_handlers.rs`.
//!
//!   READS  -> the Pub/Sub REST service's `/_introspect/` API (pubsub/introspect.rs):
//!             GET {pubsub_base}/_introspect/snapshot          (full Snapshot)
//!             GET {pubsub_base}/_introspect/messages/{id}     (MessageSnapshot)
//!
//!   MUTATIONS -> the Pub/Sub REST PROVIDER PROTOCOL. The legacy dashboard forwarded
//!             these via `s.pubsub.ServeHTTP` to the real REST paths under
//!             `/v1/projects/{project}/...`:
//!               PUT  .../topics/{id}                      (create topic)
//!               POST .../topics/{id}:publish
//!               PUT  .../subscriptions/{id}               (create subscription)
//!               POST .../subscriptions/{id}:pull|acknowledge|modifyAckDeadline
//!
//! The project id used in those paths is read from `Snapshot().Project` (the legacy
//! dashboard's `pubSubProject()`); single-topic / single-subscription reads are
//! resolved by scanning the snapshot, matching the legacy handlers. Every dashboard
//! envelope (`{project, topics}`, `{project, topic}`, `{message}`, …) is
//! reproduced exactly.

use serde_json::Value;

use crate::config::Config;
use crate::forward::{forward, ForwardError, ForwardRequest, ForwardResponse};
use crate::http::{path_segment_decode, Request, Response};

const DEFAULT_PROJECT: &str = "devcloud";

/// `GET /api/pubsub/status` (also mounted at `/api/pubsub/health`).
pub async fn handle_status(config: &Config, req: &Request) -> Response {
    if req.method != "GET" {
        return Response::method_not_allowed("GET");
    }
    let mut status = "disabled".to_string();
    let mut running = false;
    let mut project = DEFAULT_PROJECT.to_string();
    let mut topic_count = 0usize;
    let mut subscription_count = 0usize;

    if !config.pubsub_base.is_empty() {
        if let Ok(snap) = try_snapshot(config).await {
            status = snap
                .get("status")
                .and_then(Value::as_str)
                .unwrap_or("disabled")
                .to_string();
            running = snap
                .get("running")
                .and_then(Value::as_bool)
                .unwrap_or(false);
            if let Some(p) = snap.get("project").and_then(Value::as_str) {
                project = p.to_string();
            }
            topic_count = count_array(&snap, "topics");
            subscription_count = count_array(&snap, "subscriptions");
        }
    }

    Response::json(
        200,
        &serde_json::json!({
            "service": "pubsub",
            "status": status,
            "running": running,
            "grpcEndpoint": "127.0.0.1:8085",
            "restEndpoint": if config.pubsub_endpoint.is_empty() {
                "http://127.0.0.1:8086".to_string()
            } else {
                config.pubsub_endpoint.clone()
            },
            "project": project,
            "storagePath": config.pubsub_storage_path,
            "topicCount": topic_count,
            "subscriptionCount": subscription_count,
        }),
    )
}

/// `GET /api/pubsub/projects` -> `{projects: [{project, status, running}]}`.
pub async fn handle_projects(config: &Config, req: &Request) -> Response {
    if req.method != "GET" {
        return Response::method_not_allowed("GET");
    }
    let mut project = DEFAULT_PROJECT.to_string();
    let mut status = "disabled".to_string();
    let mut running = false;
    if !config.pubsub_base.is_empty() {
        if let Ok(snap) = try_snapshot(config).await {
            if let Some(p) = snap.get("project").and_then(Value::as_str) {
                project = p.to_string();
            }
            status = snap
                .get("status")
                .and_then(Value::as_str)
                .unwrap_or("disabled")
                .to_string();
            running = snap
                .get("running")
                .and_then(Value::as_bool)
                .unwrap_or(false);
        }
    }
    Response::json(
        200,
        &serde_json::json!({
            "projects": [{ "project": project, "status": status, "running": running }],
        }),
    )
}

/// `/api/pubsub/topics` — GET lists, POST creates.
pub async fn handle_topics(config: &Config, req: &Request) -> Response {
    if config.pubsub_base.is_empty() {
        return Response::text_error(503, "pubsub service is disabled");
    }
    match req.method.as_str() {
        "GET" => {
            let snap = match snapshot(config).await {
                Ok(v) => v,
                Err(resp) => return resp,
            };
            Response::json(
                200,
                &serde_json::json!({
                    "project": snap.get("project").cloned().unwrap_or(Value::String(String::new())),
                    "topics": snap.get("topics").cloned().unwrap_or(Value::Array(vec![])),
                }),
            )
        }
        "POST" => {
            let body: Value = match serde_json::from_slice(&req.body) {
                Ok(v) => v,
                Err(_) => return Response::text_error(400, "invalid json request"),
            };
            let topic_id = resource_id(first_non_empty(&body, &["topicId", "name"]));
            if topic_id.is_empty() {
                return Response::text_error(400, "topicId is required");
            }
            let project = project_for(config).await;
            let path = format!("/v1/projects/{}/topics/{}", enc(&project), enc(&topic_id));
            forward_rest(
                config,
                "PUT",
                &path,
                &req.query,
                Vec::new(),
                req.body.clone(),
            )
            .await
        }
        _ => Response::method_not_allowed("GET, POST"),
    }
}

/// `/api/pubsub/topics/{id}` and `/api/pubsub/topics/{id}:publish`.
pub async fn handle_topic(config: &Config, req: &Request) -> Response {
    if config.pubsub_base.is_empty() {
        return Response::text_error(503, "pubsub service is disabled");
    }
    let parts = match path_parts(&req.raw_path, "/api/pubsub/topics/") {
        Some(p) => p,
        None => return Response::text_error(400, "invalid pubsub topic path"),
    };
    if parts.len() == 2 {
        if parts[1] != "publish" {
            return Response::text_error(404, "404 page not found");
        }
        if req.method != "POST" {
            return Response::method_not_allowed("POST");
        }
        let project = project_for(config).await;
        let path = format!(
            "/v1/projects/{}/topics/{}:publish",
            enc(&project),
            enc(&parts[0])
        );
        return forward_rest(
            config,
            "POST",
            &path,
            &req.query,
            Vec::new(),
            req.body.clone(),
        )
        .await;
    }
    if parts.len() != 1 {
        return Response::text_error(404, "404 page not found");
    }
    if req.method != "GET" {
        return Response::method_not_allowed("GET");
    }
    let snap = match snapshot(config).await {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let project = snap.get("project").and_then(Value::as_str).unwrap_or("");
    let name = format!("projects/{}/topics/{}", project, parts[0]);
    if let Some(topic) = find_by_name(&snap, "topics", &name) {
        return Response::json(
            200,
            &serde_json::json!({ "project": project, "topic": topic }),
        );
    }
    Response::text_error(404, "404 page not found")
}

/// `/api/pubsub/subscriptions` — GET lists, POST creates.
pub async fn handle_subscriptions(config: &Config, req: &Request) -> Response {
    if config.pubsub_base.is_empty() {
        return Response::text_error(503, "pubsub service is disabled");
    }
    match req.method.as_str() {
        "GET" => {
            let snap = match snapshot(config).await {
                Ok(v) => v,
                Err(resp) => return resp,
            };
            Response::json(
                200,
                &serde_json::json!({
                    "project": snap.get("project").cloned().unwrap_or(Value::String(String::new())),
                    "subscriptions": snap.get("subscriptions").cloned().unwrap_or(Value::Array(vec![])),
                }),
            )
        }
        "POST" => {
            let body: Value = match serde_json::from_slice(&req.body) {
                Ok(v) => v,
                Err(_) => return Response::text_error(400, "invalid json request"),
            };
            let subscription_id = resource_id(first_non_empty(&body, &["subscriptionId", "name"]));
            let topic_id = resource_id(first_non_empty(&body, &["topicId", "topic"]));
            if subscription_id.is_empty() || topic_id.is_empty() {
                return Response::text_error(400, "subscriptionId and topicId are required");
            }
            let project = project_for(config).await;
            let mut create = serde_json::Map::new();
            create.insert(
                "topic".to_string(),
                Value::String(format!("projects/{project}/topics/{topic_id}")),
            );
            if let Some(ack) = body.get("ackDeadlineSeconds").and_then(Value::as_i64) {
                if ack > 0 {
                    create.insert("ackDeadlineSeconds".to_string(), Value::from(ack));
                }
            }
            let create_body = serde_json::to_vec(&Value::Object(create)).unwrap_or_default();
            let path = format!(
                "/v1/projects/{}/subscriptions/{}",
                enc(&project),
                enc(&subscription_id)
            );
            forward_rest(
                config,
                "PUT",
                &path,
                &req.query,
                vec![("Content-Type".to_string(), "application/json".to_string())],
                create_body,
            )
            .await
        }
        _ => Response::method_not_allowed("GET, POST"),
    }
}

/// `/api/pubsub/subscriptions/{id}` and its `:pull|ack|modifyAckDeadline` actions.
pub async fn handle_subscription(config: &Config, req: &Request) -> Response {
    if config.pubsub_base.is_empty() {
        return Response::text_error(503, "pubsub service is disabled");
    }
    let parts = match path_parts(&req.raw_path, "/api/pubsub/subscriptions/") {
        Some(p) => p,
        None => return Response::text_error(400, "invalid pubsub subscription path"),
    };
    if parts.len() == 2 {
        let action = match parts[1].as_str() {
            "pull" => "pull",
            "ack" => "acknowledge",
            "modifyAckDeadline" => "modifyAckDeadline",
            _ => return Response::text_error(404, "404 page not found"),
        };
        if req.method != "POST" {
            return Response::method_not_allowed("POST");
        }
        let project = project_for(config).await;
        let path = format!(
            "/v1/projects/{}/subscriptions/{}:{}",
            enc(&project),
            enc(&parts[0]),
            action
        );
        return forward_rest(
            config,
            "POST",
            &path,
            &req.query,
            Vec::new(),
            req.body.clone(),
        )
        .await;
    }
    if parts.len() != 1 {
        return Response::text_error(404, "404 page not found");
    }
    if req.method != "GET" {
        return Response::method_not_allowed("GET");
    }
    let snap = match snapshot(config).await {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let project = snap.get("project").and_then(Value::as_str).unwrap_or("");
    let name = format!("projects/{}/subscriptions/{}", project, parts[0]);
    if let Some(sub) = find_by_name(&snap, "subscriptions", &name) {
        return Response::json(
            200,
            &serde_json::json!({ "project": project, "subscription": sub }),
        );
    }
    Response::text_error(404, "404 page not found")
}

/// `GET /api/pubsub/messages/{id}` -> `{message: MessageSnapshot}`.
pub async fn handle_message(config: &Config, req: &Request) -> Response {
    if req.method != "GET" {
        return Response::method_not_allowed("GET");
    }
    if config.pubsub_base.is_empty() {
        return Response::text_error(503, "pubsub service is disabled");
    }
    let parts = match path_parts(&req.raw_path, "/api/pubsub/messages/") {
        Some(p) => p,
        None => return Response::text_error(400, "invalid pubsub message path"),
    };
    if parts.len() != 1 {
        return Response::text_error(404, "404 page not found");
    }
    match introspect(config, &format!("/_introspect/messages/{}", enc(&parts[0]))).await {
        Ok(resp) if resp.status == 200 => {
            let message: Value = match serde_json::from_slice(&resp.body) {
                Ok(v) => v,
                Err(_) => return invalid_json(),
            };
            Response::json(200, &serde_json::json!({ "message": message }))
        }
        Ok(resp) if resp.status == 404 => Response::text_error(404, "404 page not found"),
        Ok(resp) => relay(resp),
        Err(e) => forward_failure(e),
    }
}

// ── helpers ─────────────────────────────────────────────────────────────────

/// The project id used in REST forward paths — `Snapshot().Project`, falling back
/// to the default when the snapshot is unavailable (mirrors `pubSubProject()`).
async fn project_for(config: &Config) -> String {
    if let Ok(snap) = try_snapshot(config).await {
        if let Some(p) = snap.get("project").and_then(Value::as_str) {
            if !p.is_empty() {
                return p.to_string();
            }
        }
    }
    DEFAULT_PROJECT.to_string()
}

async fn forward_rest(
    config: &Config,
    method: &str,
    path: &str,
    query: &str,
    mut headers: Vec<(String, String)>,
    body: Vec<u8>,
) -> Response {
    let full_path = if query.is_empty() {
        path.to_string()
    } else {
        format!("{path}?{query}")
    };
    if !headers
        .iter()
        .any(|(k, _)| k.eq_ignore_ascii_case("content-type"))
    {
        // For passthrough actions, preserve the inbound content-type if any.
        headers.push(("Content-Type".to_string(), "application/json".to_string()));
    }
    match forward(ForwardRequest {
        base: &config.pubsub_base,
        method,
        path: &full_path,
        headers,
        body,
    })
    .await
    {
        Ok(resp) => relay(resp),
        Err(e) => forward_failure(e),
    }
}

async fn snapshot(config: &Config) -> Result<Value, Response> {
    match introspect(config, "/_introspect/snapshot").await {
        Ok(resp) if resp.status == 200 => {
            serde_json::from_slice(&resp.body).map_err(|_| invalid_json())
        }
        Ok(resp) => Err(relay(resp)),
        Err(e) => Err(forward_failure(e)),
    }
}

async fn try_snapshot(config: &Config) -> Result<Value, ()> {
    match introspect(config, "/_introspect/snapshot").await {
        Ok(resp) if resp.status == 200 => serde_json::from_slice(&resp.body).map_err(|_| ()),
        _ => Err(()),
    }
}

async fn introspect(config: &Config, path: &str) -> Result<ForwardResponse, ForwardError> {
    forward(ForwardRequest {
        base: &config.pubsub_base,
        method: "GET",
        path,
        headers: Vec::new(),
        body: Vec::new(),
    })
    .await
}

fn find_by_name(snap: &Value, key: &str, name: &str) -> Option<Value> {
    snap.get(key)
        .and_then(Value::as_array)?
        .iter()
        .find(|item| item.get("name").and_then(Value::as_str) == Some(name))
        .cloned()
}

fn count_array(v: &Value, key: &str) -> usize {
    v.get(key)
        .and_then(Value::as_array)
        .map(|a| a.len())
        .unwrap_or(0)
}

/// Returns the first non-empty string field among `keys`.
fn first_non_empty(body: &Value, keys: &[&str]) -> String {
    for key in keys {
        if let Some(s) = body.get(*key).and_then(Value::as_str) {
            if !s.trim().is_empty() {
                return s.to_string();
            }
        }
    }
    String::new()
}

/// Mirrors `dashboardPubSubResourceID`: trims, and if the value is a full
/// resource path returns the last segment.
fn resource_id(value: String) -> String {
    let value = value.trim();
    if value.is_empty() {
        return String::new();
    }
    if value.contains('/') {
        return value.rsplit('/').next().unwrap_or("").trim().to_string();
    }
    value.to_string()
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
    Response::text_error(502, "pubsub introspection returned invalid json")
}

fn forward_failure(err: ForwardError) -> Response {
    match err {
        ForwardError::Unreachable(_) => Response::text_error(502, "pubsub service is unreachable"),
        ForwardError::BadBase => {
            Response::text_error(500, "pubsub service address is misconfigured")
        }
        ForwardError::BadResponse => {
            Response::text_error(502, "pubsub service returned an invalid response")
        }
    }
}

fn path_parts(escaped_path: &str, prefix: &str) -> Option<Vec<String>> {
    let suffix = escaped_path.strip_prefix(prefix)?;
    let mut parts = Vec::new();
    for raw in suffix.trim_matches('/').split('/') {
        if raw.is_empty() {
            continue;
        }
        parts.push(path_segment_decode(raw)?);
    }
    Some(parts)
}

fn enc(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char)
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resource_id_last_segment() {
        assert_eq!(resource_id("projects/p/topics/t".to_string()), "t");
        assert_eq!(resource_id(" spaced ".to_string()), "spaced");
        assert_eq!(resource_id("".to_string()), "");
    }

    #[test]
    fn first_non_empty_prefers_first() {
        let body = serde_json::json!({ "topicId": "", "name": "fallback" });
        assert_eq!(first_non_empty(&body, &["topicId", "name"]), "fallback");
    }

    #[test]
    fn find_by_name_matches() {
        let snap = serde_json::json!({ "topics": [{ "name": "projects/p/topics/a" }] });
        assert!(find_by_name(&snap, "topics", "projects/p/topics/a").is_some());
        assert!(find_by_name(&snap, "topics", "projects/p/topics/b").is_none());
    }
}
