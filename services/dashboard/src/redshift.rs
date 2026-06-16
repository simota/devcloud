//! Redshift dashboard handler — ports `internal/dashboard/redshift_handlers.rs`.
//!
//!   READS  -> the redshift service's `/_introspect/` API (redshift/introspect.rs):
//!             GET {redshift_base}/_introspect/clusters              (full Snapshot)
//!             GET {redshift_base}/_introspect/catalog               (CatalogSnapshot)
//!             GET {redshift_base}/_introspect/statements            (StatementSnapshots)
//!             GET {redshift_base}/_introspect/tables/{schema}/{table}?limit= (TableDetailSnapshot)
//!
//!   MUTATION -> the redshift service's `/_control/` API (redshift/control.rs):
//!             POST {redshift_base}/_control/query   {sql,maxRows}  (ExecuteDashboardSQL)
//!
//! The introspection bodies are the raw snapshot structs; the legacy dashboard
//! re-wrapped them into `{clusters}` / `{catalog}` / `{statements}` and the
//! table detail into `{schema,table,detail,columns,rows}`. We reproduce those
//! envelopes exactly. The `/_control/query` response (`{result: ...}`) is already
//! byte-identical to the legacy dashboard, so it relays verbatim.

use serde_json::Value;

use crate::config::Config;
use crate::forward::{forward, ForwardError, ForwardRequest, ForwardResponse};
use crate::http::{path_segment_decode, Request, Response};

/// `GET /api/redshift/status` — derives from the `/_introspect/clusters` snapshot.
pub async fn handle_status(config: &Config, req: &Request) -> Response {
    if req.method != "GET" {
        return Response::method_not_allowed("GET");
    }

    let mut status = "disabled".to_string();
    let mut running = false;
    let mut region = "us-east-1".to_string();
    let mut cluster_count = 0usize;
    let mut backend_kind = "postgres".to_string();
    let mut backend_mode = "managed".to_string();

    if !config.redshift_base.is_empty() {
        if let Ok(resp) = introspect(config, "/_introspect/clusters").await {
            if resp.status == 200 {
                if let Ok(snap) = serde_json::from_slice::<Value>(&resp.body) {
                    if let Some(s) = snap.get("status").and_then(Value::as_str) {
                        status = s.to_string();
                    }
                    running = snap
                        .get("running")
                        .and_then(Value::as_bool)
                        .unwrap_or(false);
                    if let Some(r) = snap.get("region").and_then(Value::as_str) {
                        region = r.to_string();
                    }
                    cluster_count = snap
                        .get("clusters")
                        .and_then(Value::as_array)
                        .map(|c| c.len())
                        .unwrap_or(0);
                    if let Some(k) = snap.get("backendKind").and_then(Value::as_str) {
                        if !k.is_empty() {
                            backend_kind = k.to_string();
                        }
                    }
                    if let Some(m) = snap.get("backendMode").and_then(Value::as_str) {
                        if !m.is_empty() {
                            backend_mode = m.to_string();
                        }
                    }
                }
            }
        }
    }

    Response::json(
        200,
        &serde_json::json!({
            "service": "redshift",
            "status": status,
            "running": running,
            "sqlEndpoint": config.redshift_sql_endpoint.clone(),
            "apiEndpoint": if config.redshift_endpoint.is_empty() {
                "http://127.0.0.1:19099".to_string()
            } else {
                config.redshift_endpoint.clone()
            },
            "region": region,
            "clusterCount": cluster_count,
            "storagePath": config.redshift_storage_path,
            "backendKind": backend_kind,
            "backendMode": backend_mode,
        }),
    )
}

/// `GET /api/redshift/clusters` -> `{clusters: snapshot.clusters}`.
pub async fn handle_clusters(config: &Config, req: &Request) -> Response {
    if req.method != "GET" {
        return Response::method_not_allowed("GET");
    }
    if config.redshift_base.is_empty() {
        return Response::text_error(503, "redshift service is disabled");
    }
    match introspect(config, "/_introspect/clusters").await {
        Ok(resp) if resp.status == 200 => {
            let snap: Value = match serde_json::from_slice(&resp.body) {
                Ok(v) => v,
                Err(_) => return invalid_json(),
            };
            let clusters = snap
                .get("clusters")
                .cloned()
                .unwrap_or(Value::Array(vec![]));
            Response::json(200, &serde_json::json!({ "clusters": clusters }))
        }
        Ok(resp) => relay(resp),
        Err(e) => forward_failure(e),
    }
}

/// `GET /api/redshift/catalog` -> `{catalog: CatalogSnapshot}`.
pub async fn handle_catalog(config: &Config, req: &Request) -> Response {
    if req.method != "GET" {
        return Response::method_not_allowed("GET");
    }
    if config.redshift_base.is_empty() {
        return Response::text_error(503, "redshift service is disabled");
    }
    match introspect(config, "/_introspect/catalog").await {
        Ok(resp) if resp.status == 200 => {
            let catalog: Value = match serde_json::from_slice(&resp.body) {
                Ok(v) => v,
                Err(_) => return invalid_json(),
            };
            Response::json(200, &serde_json::json!({ "catalog": catalog }))
        }
        Ok(resp) => relay(resp),
        Err(e) => forward_failure(e),
    }
}

/// `GET /api/redshift/statements` -> `{statements: StatementSnapshots}`.
pub async fn handle_statements(config: &Config, req: &Request) -> Response {
    if req.method != "GET" {
        return Response::method_not_allowed("GET");
    }
    if config.redshift_base.is_empty() {
        return Response::text_error(503, "redshift service is disabled");
    }
    match introspect(config, "/_introspect/statements").await {
        Ok(resp) if resp.status == 200 => {
            let statements: Value = match serde_json::from_slice(&resp.body) {
                Ok(v) => v,
                Err(_) => return invalid_json(),
            };
            Response::json(200, &serde_json::json!({ "statements": statements }))
        }
        Ok(resp) => relay(resp),
        Err(e) => forward_failure(e),
    }
}

/// `GET /api/redshift/tables/{schema}/{table}` -> `{schema,table,detail,columns,rows}`.
pub async fn handle_table(config: &Config, req: &Request) -> Response {
    if config.redshift_base.is_empty() {
        return Response::text_error(503, "redshift service is disabled");
    }
    if req.method != "GET" {
        return Response::method_not_allowed("GET");
    }
    let parts = match path_parts(&req.raw_path, "/api/redshift/tables/") {
        Some(p) => p,
        None => return Response::text_error(400, "invalid redshift table path"),
    };
    if parts.len() != 2 {
        return Response::text_error(400, "invalid redshift table path");
    }
    let limit = match parse_limit(&req.query) {
        Ok(l) => l,
        Err(()) => return Response::text_error(400, "limit must be a positive integer"),
    };
    let path = format!(
        "/_introspect/tables/{}/{}?limit={}",
        encode_segment(&parts[0]),
        encode_segment(&parts[1]),
        limit
    );
    match introspect(config, &path).await {
        Ok(resp) if resp.status == 200 => {
            let detail: Value = match serde_json::from_slice(&resp.body) {
                Ok(v) => v,
                Err(_) => return invalid_json(),
            };
            let columns = detail
                .get("columns")
                .cloned()
                .unwrap_or(Value::Array(vec![]));
            // `rows` is `omitempty` in the detail; the legacy dashboard surfaces
            // `detail.Rows` which marshals to null when empty.
            let rows = detail.get("rows").cloned().unwrap_or(Value::Null);
            Response::json(
                200,
                &serde_json::json!({
                    "schema": parts[0],
                    "table": parts[1],
                    "detail": detail,
                    "columns": columns,
                    "rows": rows,
                }),
            )
        }
        Ok(resp) if resp.status == 404 => Response::text_error(404, "404 page not found"),
        Ok(resp) => relay(resp),
        Err(e) => forward_failure(e),
    }
}

/// `POST /api/redshift/query` — forwards to `/_control/query` (ExecuteDashboardSQL).
pub async fn handle_query(config: &Config, req: &Request) -> Response {
    if config.redshift_base.is_empty() {
        return Response::text_error(503, "redshift service is disabled");
    }
    if req.method != "POST" {
        return Response::method_not_allowed("POST");
    }
    match forward(ForwardRequest {
        base: &config.redshift_base,
        method: "POST",
        path: "/_control/query",
        headers: vec![("Content-Type".to_string(), "application/json".to_string())],
        body: req.body.clone(),
    })
    .await
    {
        Ok(resp) => relay(resp),
        Err(e) => forward_failure(e),
    }
}

async fn introspect(config: &Config, path: &str) -> Result<ForwardResponse, ForwardError> {
    forward(ForwardRequest {
        base: &config.redshift_base,
        method: "GET",
        path,
        headers: Vec::new(),
        body: Vec::new(),
    })
    .await
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
    Response::text_error(502, "redshift introspection returned invalid json")
}

fn forward_failure(err: ForwardError) -> Response {
    match err {
        ForwardError::Unreachable(_) => {
            Response::text_error(502, "redshift service is unreachable")
        }
        ForwardError::BadBase => {
            Response::text_error(500, "redshift service address is misconfigured")
        }
        ForwardError::BadResponse => {
            Response::text_error(502, "redshift service returned an invalid response")
        }
    }
}

/// Parses the `limit` query parameter (default 100). Mirrors
/// `positiveLimitFromRequest`: a present-but-invalid value is an error.
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

fn encode_segment(s: &str) -> String {
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
    fn parse_limit_default() {
        assert_eq!(parse_limit(""), Ok(100));
        assert_eq!(parse_limit("limit="), Ok(100));
    }

    #[test]
    fn parse_limit_valid() {
        assert_eq!(parse_limit("limit=50"), Ok(50));
        assert_eq!(parse_limit("foo=1&limit=7"), Ok(7));
    }

    #[test]
    fn parse_limit_invalid() {
        assert_eq!(parse_limit("limit=0"), Err(()));
        assert_eq!(parse_limit("limit=-3"), Err(()));
        assert_eq!(parse_limit("limit=abc"), Err(()));
    }

    #[test]
    fn path_parts_two_segments() {
        assert_eq!(
            path_parts("/api/redshift/tables/public/users", "/api/redshift/tables/"),
            Some(vec!["public".to_string(), "users".to_string()])
        );
    }
}
