//! BigQuery dashboard handler — ports `internal/dashboard/bigquery_handlers.rs`.
//!
//!   READS  -> the BigQuery service's `/_introspect/` API (bigquery/introspect.rs):
//!             GET {bq_base}/_introspect/projects                              (full Snapshot)
//!             GET {bq_base}/_introspect/projects/{p}/datasets                 (Datasets)
//!             GET {bq_base}/_introspect/projects/{p}/datasets/{d}             (DatasetSnapshot)
//!             GET {bq_base}/_introspect/projects/{p}/datasets/{d}/tables/{t}  (TableSnapshot)
//!             GET {bq_base}/_introspect/projects/{p}/datasets/{d}/tables/{t}/rows?limit=
//!             GET {bq_base}/_introspect/projects/{p}/jobs                     (Jobs)
//!             GET {bq_base}/_introspect/projects/{p}/jobs/{jobId}             (JobSnapshot)
//!
//!   MUTATIONS -> the BigQuery REST PROVIDER PROTOCOL. The legacy dashboard forwarded
//!             these via `s.bq.ServeHTTP` (transparent body/header/query
//!             passthrough) to the real REST paths under `/bigquery/v2/...`:
//!               POST .../queries, .../jobs, .../datasets, .../datasets/{d}/tables,
//!               .../datasets/{d}/tables/{t}/insertAll.
//!
//! The legacy dashboard re-wrapped each introspection read into a bespoke envelope
//! (e.g. `{projectId, datasets}`); the table `schema` sub-resource is derived
//! from the full `TableSnapshot.schema`. We reproduce each envelope exactly.

use serde_json::Value;

use crate::config::Config;
use crate::forward::{forward, ForwardError, ForwardRequest, ForwardResponse};
use crate::http::{path_segment_decode, Request, Response};

/// `GET /api/bigquery/status` — derives from the `/_introspect/projects` snapshot.
pub async fn handle_status(config: &Config, req: &Request) -> Response {
    if req.method != "GET" {
        return Response::method_not_allowed("GET");
    }
    let mut status = "disabled".to_string();
    let mut running = false;
    let mut project = "devcloud".to_string();
    let mut location = "US".to_string();
    let mut dataset_count = 0usize;
    let mut job_count = 0usize;

    if !config.bigquery_base.is_empty() {
        if let Ok(resp) = introspect(config, "/_introspect/projects").await {
            if resp.status == 200 {
                if let Ok(snap) = serde_json::from_slice::<Value>(&resp.body) {
                    if let Some(s) = snap.get("status").and_then(Value::as_str) {
                        status = s.to_string();
                    }
                    running = snap
                        .get("running")
                        .and_then(Value::as_bool)
                        .unwrap_or(false);
                    if let Some(p) = snap.get("project").and_then(Value::as_str) {
                        project = p.to_string();
                    }
                    if let Some(l) = snap.get("location").and_then(Value::as_str) {
                        location = l.to_string();
                    }
                    dataset_count = count_array(&snap, "datasets");
                    job_count = count_array(&snap, "jobs");
                }
            }
        }
    }

    Response::json(
        200,
        &serde_json::json!({
            "service": "bigquery",
            "status": status,
            "running": running,
            "endpoint": if config.bigquery_endpoint.is_empty() {
                "http://127.0.0.1:9050".to_string()
            } else {
                config.bigquery_endpoint.clone()
            },
            "project": project,
            "location": location,
            "authMode": "relaxed",
            "storagePath": config.bigquery_storage_path,
            "datasetCount": dataset_count,
            "jobCount": job_count,
        }),
    )
}

/// `GET /api/bigquery/projects` -> `{projects: [{projectId, location, ...}]}`.
pub async fn handle_projects(config: &Config, req: &Request) -> Response {
    if req.method != "GET" {
        return Response::method_not_allowed("GET");
    }
    if config.bigquery_base.is_empty() {
        return Response::text_error(503, "bigquery service is disabled");
    }
    let snap = match snapshot(config).await {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let datasets = snap
        .get("datasets")
        .cloned()
        .unwrap_or(Value::Array(vec![]));
    let jobs = snap.get("jobs").cloned().unwrap_or(Value::Array(vec![]));
    Response::json(
        200,
        &serde_json::json!({
            "projects": [{
                "projectId": snap.get("project").cloned().unwrap_or(Value::String(String::new())),
                "location": snap.get("location").cloned().unwrap_or(Value::String(String::new())),
                "datasetCount": datasets.as_array().map(|a| a.len()).unwrap_or(0),
                "jobCount": jobs.as_array().map(|a| a.len()).unwrap_or(0),
                "datasets": datasets,
                "jobs": jobs,
            }],
        }),
    )
}

/// `/api/bigquery/projects/{p}/...` — reads + REST mutations.
pub async fn handle_project_resource(config: &Config, req: &Request) -> Response {
    if config.bigquery_base.is_empty() {
        return Response::text_error(503, "bigquery service is disabled");
    }
    let parts = match path_parts(&req.raw_path, "/api/bigquery/projects/") {
        Some(p) if !p.is_empty() => p,
        _ => return Response::text_error(400, "invalid bigquery path"),
    };
    let project = &parts[0];

    // --- REST mutation routes (POST), forwarded to the provider protocol ---
    if req.method == "POST" {
        if parts.len() == 2 && parts[1] == "queries" {
            return forward_rest(
                config,
                req,
                &format!("/bigquery/v2/projects/{}/queries", enc(project)),
            )
            .await;
        }
        if parts.len() == 2 && parts[1] == "jobs" {
            return forward_rest(
                config,
                req,
                &format!("/bigquery/v2/projects/{}/jobs", enc(project)),
            )
            .await;
        }
        if parts.len() == 2 && parts[1] == "datasets" {
            return forward_rest(
                config,
                req,
                &format!("/bigquery/v2/projects/{}/datasets", enc(project)),
            )
            .await;
        }
        if parts.len() == 4 && parts[1] == "datasets" && parts[3] == "tables" {
            return forward_rest(
                config,
                req,
                &format!(
                    "/bigquery/v2/projects/{}/datasets/{}/tables",
                    enc(project),
                    enc(&parts[2])
                ),
            )
            .await;
        }
        if parts.len() == 6
            && parts[1] == "datasets"
            && parts[3] == "tables"
            && parts[5] == "insertAll"
        {
            return forward_rest(
                config,
                req,
                &format!(
                    "/bigquery/v2/projects/{}/datasets/{}/tables/{}/insertAll",
                    enc(project),
                    enc(&parts[2]),
                    enc(&parts[4])
                ),
            )
            .await;
        }
    }

    if req.method != "GET" {
        return Response::method_not_allowed("GET");
    }

    // --- read routes (GET) ---
    if parts.len() == 2 && parts[1] == "datasets" {
        let snap = match snapshot(config).await {
            Ok(v) => v,
            Err(resp) => return resp,
        };
        if snap.get("project").and_then(Value::as_str) != Some(project.as_str()) {
            return Response::text_error(404, "404 page not found");
        }
        let datasets = snap
            .get("datasets")
            .cloned()
            .unwrap_or(Value::Array(vec![]));
        return Response::json(
            200,
            &serde_json::json!({ "projectId": project, "datasets": datasets }),
        );
    }
    if parts.len() == 4 && parts[1] == "datasets" && parts[3] == "tables" {
        let path = format!(
            "/_introspect/projects/{}/datasets/{}",
            enc(project),
            enc(&parts[2])
        );
        let dataset = match introspect_json(config, &path).await {
            Ok(v) => v,
            Err(resp) => return resp,
        };
        let tables = dataset
            .get("tables")
            .cloned()
            .unwrap_or(Value::Array(vec![]));
        return Response::json(
            200,
            &serde_json::json!({
                "projectId": project, "datasetId": parts[2], "tables": tables,
            }),
        );
    }
    if parts.len() == 5 && parts[1] == "datasets" && parts[3] == "tables" {
        let path = format!(
            "/_introspect/projects/{}/datasets/{}/tables/{}",
            enc(project),
            enc(&parts[2]),
            enc(&parts[4])
        );
        let table = match introspect_json(config, &path).await {
            Ok(v) => v,
            Err(resp) => return resp,
        };
        return Response::json(
            200,
            &serde_json::json!({
                "projectId": project, "datasetId": parts[2], "tableId": parts[4], "table": table,
            }),
        );
    }
    if parts.len() == 6 && parts[1] == "datasets" && parts[3] == "tables" && parts[5] == "schema" {
        let path = format!(
            "/_introspect/projects/{}/datasets/{}/tables/{}",
            enc(project),
            enc(&parts[2]),
            enc(&parts[4])
        );
        let table = match introspect_json(config, &path).await {
            Ok(v) => v,
            Err(resp) => return resp,
        };
        let schema = table.get("schema").cloned().unwrap_or(Value::Null);
        return Response::json(
            200,
            &serde_json::json!({
                "projectId": project, "datasetId": parts[2], "tableId": parts[4], "schema": schema,
            }),
        );
    }
    if parts.len() == 6 && parts[1] == "datasets" && parts[3] == "tables" && parts[5] == "rows" {
        let limit = match parse_limit(&req.query) {
            Ok(l) => l,
            Err(()) => return Response::text_error(400, "limit must be a positive integer"),
        };
        let path = format!(
            "/_introspect/projects/{}/datasets/{}/tables/{}/rows?limit={}",
            enc(project),
            enc(&parts[2]),
            enc(&parts[4]),
            limit
        );
        let table = match introspect_json(config, &path).await {
            Ok(v) => v,
            Err(resp) => return resp,
        };
        let rows = table.get("rows").cloned().unwrap_or(Value::Null);
        return Response::json(
            200,
            &serde_json::json!({
                "projectId": project, "datasetId": parts[2], "tableId": parts[4], "rows": rows,
            }),
        );
    }
    if parts.len() == 2 && parts[1] == "jobs" {
        let snap = match snapshot(config).await {
            Ok(v) => v,
            Err(resp) => return resp,
        };
        if snap.get("project").and_then(Value::as_str) != Some(project.as_str()) {
            return Response::text_error(404, "404 page not found");
        }
        let jobs = snap.get("jobs").cloned().unwrap_or(Value::Array(vec![]));
        return Response::json(
            200,
            &serde_json::json!({ "projectId": project, "jobs": jobs }),
        );
    }
    if parts.len() == 3 && parts[1] == "jobs" {
        let path = format!(
            "/_introspect/projects/{}/jobs/{}",
            enc(project),
            enc(&parts[2])
        );
        let job = match introspect_json(config, &path).await {
            Ok(v) => v,
            Err(resp) => return resp,
        };
        return Response::json(
            200,
            &serde_json::json!({
                "projectId": project, "jobId": parts[2], "job": job,
            }),
        );
    }

    Response::text_error(404, "404 page not found")
}

/// Forwards a dashboard REST mutation to the BigQuery provider protocol: a
/// transparent body + query passthrough to `path`, mirroring `forwardBigQueryRequest`.
async fn forward_rest(config: &Config, req: &Request, path: &str) -> Response {
    let full_path = if req.query.is_empty() {
        path.to_string()
    } else {
        format!("{path}?{}", req.query)
    };
    let mut headers = Vec::new();
    let ct = req.header("content-type");
    if !ct.is_empty() {
        headers.push(("Content-Type".to_string(), ct.to_string()));
    }
    match forward(ForwardRequest {
        base: &config.bigquery_base,
        method: "POST",
        path: &full_path,
        headers,
        body: req.body.clone(),
    })
    .await
    {
        Ok(resp) => relay(resp),
        Err(e) => forward_failure(e),
    }
}

async fn snapshot(config: &Config) -> Result<Value, Response> {
    introspect_json(config, "/_introspect/projects").await
}

async fn introspect_json(config: &Config, path: &str) -> Result<Value, Response> {
    match introspect(config, path).await {
        Ok(resp) if resp.status == 200 => {
            serde_json::from_slice(&resp.body).map_err(|_| invalid_json())
        }
        Ok(resp) if resp.status == 404 => Err(Response::text_error(404, "404 page not found")),
        Ok(resp) => Err(relay(resp)),
        Err(e) => Err(forward_failure(e)),
    }
}

async fn introspect(config: &Config, path: &str) -> Result<ForwardResponse, ForwardError> {
    forward(ForwardRequest {
        base: &config.bigquery_base,
        method: "GET",
        path,
        headers: Vec::new(),
        body: Vec::new(),
    })
    .await
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
    Response::text_error(502, "bigquery introspection returned invalid json")
}

fn forward_failure(err: ForwardError) -> Response {
    match err {
        ForwardError::Unreachable(_) => {
            Response::text_error(502, "bigquery service is unreachable")
        }
        ForwardError::BadBase => {
            Response::text_error(500, "bigquery service address is misconfigured")
        }
        ForwardError::BadResponse => {
            Response::text_error(502, "bigquery service returned an invalid response")
        }
    }
}

fn parse_limit(query: &str) -> Result<i64, ()> {
    for pair in query.split('&') {
        if let Some(v) = pair.strip_prefix("limit=") {
            if v.is_empty() {
                return Ok(100);
            }
            return match v.parse::<i64>() {
                Ok(n) if n > 0 => Ok(n),
                _ => Err(()),
            };
        }
    }
    Ok(100)
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
    fn path_parts_nested() {
        assert_eq!(
            path_parts(
                "/api/bigquery/projects/p/datasets/d/tables/t/rows",
                "/api/bigquery/projects/"
            ),
            Some(vec![
                "p".to_string(),
                "datasets".to_string(),
                "d".to_string(),
                "tables".to_string(),
                "t".to_string(),
                "rows".to_string(),
            ])
        );
    }

    #[test]
    fn parse_limit_cases() {
        assert_eq!(parse_limit(""), Ok(100));
        assert_eq!(parse_limit("limit=5"), Ok(5));
        assert_eq!(parse_limit("limit=0"), Err(()));
    }
}
