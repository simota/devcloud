//! DynamoDB dashboard handler — ports `internal/dashboard/dynamodb_handlers.rs`.
//!
//!   READS  -> the DynamoDB service's `/_introspect/` API (dynamodb/introspect.rs):
//!             GET {dynamodb_base}/_introspect/tables                 (full Snapshot)
//!             GET {dynamodb_base}/_introspect/tables/{name}          (TableSnapshot)
//!             GET {dynamodb_base}/_introspect/tables/{name}/items?limit=
//!             GET {dynamodb_base}/_introspect/tables/{name}/indexes
//!             GET {dynamodb_base}/_introspect/tables/{name}/ttl
//!             GET {dynamodb_base}/_introspect/tables/{name}/streams
//!
//!   MUTATIONS -> the DynamoDB PROVIDER PROTOCOL (AWS JSON 1.0). The legacy dashboard
//!             forwarded these via `s.dynamo.ServeHTTP` with an
//!             `X-Amz-Target: DynamoDB_20120810.<Action>` header:
//!               CreateTable / PutItem / UpdateItem / DeleteItem / UpdateTimeToLive
//!               / Query / Scan / DeleteTable.
//!             DeleteItem & DeleteTable additionally require a `confirmation`
//!             field equal to the table name.
//!
//! The legacy dashboard re-wrapped introspection bodies: the table list into
//! `{tables}`, a single table into `{table}`. The items/indexes/ttl/streams
//! bodies are already identical between introspection and the dashboard, so
//! they relay verbatim.

use serde_json::Value;

use crate::config::Config;
use crate::forward::{forward, ForwardError, ForwardRequest, ForwardResponse};
use crate::http::{Request, Response};

/// `GET /api/dynamodb/status` — derives from `/_introspect/tables` snapshot.
pub async fn handle_status(config: &Config, req: &Request) -> Response {
    if req.method != "GET" {
        return Response::method_not_allowed("GET");
    }
    let mut status = "disabled".to_string();
    let mut running = false;
    let mut table_count = 0usize;

    if !config.dynamodb_base.is_empty() {
        if let Ok(resp) = introspect(config, "/_introspect/tables").await {
            if resp.status == 200 {
                if let Ok(snap) = serde_json::from_slice::<Value>(&resp.body) {
                    if let Some(s) = snap.get("status").and_then(Value::as_str) {
                        status = s.to_string();
                    }
                    running = snap
                        .get("running")
                        .and_then(Value::as_bool)
                        .unwrap_or(false);
                    table_count = snap
                        .get("tables")
                        .and_then(Value::as_array)
                        .map(|t| t.len())
                        .unwrap_or(0);
                }
            }
        }
    }

    Response::json(
        200,
        &serde_json::json!({
            "status": status,
            "running": running,
            "endpoint": if config.dynamodb_endpoint.is_empty() {
                "http://127.0.0.1:8000".to_string()
            } else {
                config.dynamodb_endpoint.clone()
            },
            "region": "us-east-1",
            "storagePath": config.dynamodb_storage_path,
            "tableCount": table_count,
        }),
    )
}

/// `/api/dynamodb/tables` — GET lists, POST creates a table.
pub async fn handle_tables(config: &Config, req: &Request) -> Response {
    if config.dynamodb_base.is_empty() {
        return Response::text_error(503, "dynamodb service is disabled");
    }
    match req.method.as_str() {
        "GET" => match introspect(config, "/_introspect/tables").await {
            Ok(resp) if resp.status == 200 => {
                let snap: Value = match serde_json::from_slice(&resp.body) {
                    Ok(v) => v,
                    Err(_) => return invalid_json(),
                };
                let tables = snap.get("tables").cloned().unwrap_or(Value::Array(vec![]));
                Response::json(200, &serde_json::json!({ "tables": tables }))
            }
            Ok(resp) => relay(resp),
            Err(e) => forward_failure(e),
        },
        "POST" => forward_operation(config, req, "CreateTable", "", None).await,
        _ => Response::method_not_allowed("GET, POST"),
    }
}

/// `/api/dynamodb/tables/{name}` and its sub-resources — mirrors legacy `handleDynamoDBTable`.
pub async fn handle_table(config: &Config, req: &Request) -> Response {
    if config.dynamodb_base.is_empty() {
        return Response::text_error(503, "dynamodb service is disabled");
    }
    let suffix = match req.raw_path.strip_prefix("/api/dynamodb/tables/") {
        Some(s) => s,
        None => return Response::text_error(400, "invalid table path"),
    };
    let (escaped_table, rest) = match suffix.split_once('/') {
        Some((t, r)) => (t, Some(r)),
        None => (suffix, None),
    };
    let table_name = match crate::http::path_segment_decode(escaped_table) {
        Some(t) => t,
        None => return Response::text_error(400, "invalid table path"),
    };
    if table_name.is_empty() {
        return Response::text_error(404, "404 page not found");
    }

    // Mutating sub-resources (handled before the GET fall-through, matching legacy).
    if let Some(rest) = rest {
        match rest {
            "items" if req.method == "POST" => {
                return forward_operation(config, req, "PutItem", &table_name, None).await;
            }
            "items/update" => {
                return forward_operation(config, req, "UpdateItem", &table_name, None).await;
            }
            "items/delete" => {
                return forward_operation(
                    config,
                    req,
                    "DeleteItem",
                    &table_name,
                    Some(&table_name),
                )
                .await;
            }
            "ttl" if req.method == "POST" => {
                return forward_operation(config, req, "UpdateTimeToLive", &table_name, None).await;
            }
            "query" => {
                return forward_operation(config, req, "Query", &table_name, None).await;
            }
            "scan" => {
                return forward_operation(config, req, "Scan", &table_name, None).await;
            }
            "delete" => {
                return forward_operation(
                    config,
                    req,
                    "DeleteTable",
                    &table_name,
                    Some(&table_name),
                )
                .await;
            }
            _ => {}
        }
    }

    // Read fall-through (GET-only), forwarding to introspection.
    if req.method != "GET" {
        return Response::method_not_allowed("GET");
    }
    let detail_path = format!("/_introspect/tables/{}", encode_segment(&table_name));
    let table = match introspect(config, &detail_path).await {
        Ok(resp) if resp.status == 200 => match serde_json::from_slice::<Value>(&resp.body) {
            Ok(v) => v,
            Err(_) => return invalid_json(),
        },
        Ok(resp) if resp.status == 404 => return Response::text_error(404, "404 page not found"),
        Ok(resp) => return relay(resp),
        Err(e) => return forward_failure(e),
    };

    match rest {
        None => Response::json(200, &serde_json::json!({ "table": table })),
        Some("indexes") => Response::json(
            200,
            &serde_json::json!({
                "tableName": table_name,
                "globalSecondaryIndexes": table.get("globalSecondaryIndexes").cloned().unwrap_or(Value::Null),
                "localSecondaryIndexes": table.get("localSecondaryIndexes").cloned().unwrap_or(Value::Null),
            }),
        ),
        Some("ttl") => Response::json(
            200,
            &serde_json::json!({
                "tableName": table_name,
                "timeToLiveDescription": table.get("timeToLiveDescription").cloned().unwrap_or(Value::Null),
            }),
        ),
        Some("streams") => {
            let stream_spec = table
                .get("streamSpecification")
                .cloned()
                .unwrap_or(Value::Null);
            let stream_enabled = stream_spec
                .get("streamEnabled")
                .and_then(Value::as_bool)
                .unwrap_or(false);
            Response::json(
                200,
                &serde_json::json!({
                    "tableName": table_name,
                    "streamEnabled": stream_enabled,
                    "latestStreamArn": table.get("latestStreamArn").cloned().unwrap_or(Value::Null),
                    "latestStreamLabel": table.get("latestStreamLabel").cloned().unwrap_or(Value::Null),
                    "streamSpecification": stream_spec,
                }),
            )
        }
        Some("items") => {
            // Forward the items read (with its limit) straight to introspection,
            // which produces the identical `{tableName, items}` body.
            let mut path = format!("/_introspect/tables/{}/items", encode_segment(&table_name));
            if !req.query.is_empty() {
                path.push('?');
                path.push_str(&req.query);
            }
            match introspect(config, &path).await {
                Ok(resp) => relay(resp),
                Err(e) => forward_failure(e),
            }
        }
        Some(_) => Response::text_error(404, "404 page not found"),
    }
}

/// Forwards a dashboard mutation to the DynamoDB provider protocol, mirroring
/// `forwardDynamoDBDashboardOperationWithConfirmation`: reads `{input, confirmation}`,
/// checks the optional confirmation, normalizes `TableName`, POSTs AWS JSON 1.0.
async fn forward_operation(
    config: &Config,
    req: &Request,
    operation: &str,
    table_name: &str,
    required_confirmation: Option<&str>,
) -> Response {
    if req.method != "POST" {
        return Response::method_not_allowed("POST");
    }
    let envelope: Value = match serde_json::from_slice(&req.body) {
        Ok(v) => v,
        Err(_) => return Response::text_error(400, "invalid json request"),
    };
    if let Some(required) = required_confirmation {
        let confirmation = envelope
            .get("confirmation")
            .and_then(Value::as_str)
            .unwrap_or("");
        if confirmation != required {
            return Response::text_error(400, "confirmation must match table name");
        }
    }
    let raw_input = envelope.get("input").cloned();
    let input = match normalize_input(raw_input, table_name) {
        Ok(v) => v,
        Err(msg) => return Response::text_error(400, &msg),
    };
    match forward(ForwardRequest {
        base: &config.dynamodb_base,
        method: "POST",
        path: "/",
        headers: vec![
            (
                "Content-Type".to_string(),
                "application/x-amz-json-1.0".to_string(),
            ),
            (
                "X-Amz-Target".to_string(),
                format!("DynamoDB_20120810.{operation}"),
            ),
        ],
        body: input,
    })
    .await
    {
        Ok(resp) => relay(resp),
        Err(e) => forward_failure(e),
    }
}

/// Mirrors `normalizeDynamoDBDashboardInput`: requires a JSON-object input,
/// fills/validates `TableName` against the selected table, re-encodes.
fn normalize_input(raw: Option<Value>, table_name: &str) -> Result<Vec<u8>, String> {
    let raw = match raw {
        Some(v) if !v.is_null() => v,
        _ => return Err("input is required".to_string()),
    };
    let mut obj = match raw {
        Value::Object(m) => m,
        _ => return Err("input must be a JSON object".to_string()),
    };
    if !table_name.is_empty() {
        match obj.get("TableName") {
            Some(existing) => match existing.as_str() {
                Some(name) if name == table_name => {}
                _ => return Err("input TableName must match the selected table".to_string()),
            },
            None => {
                obj.insert(
                    "TableName".to_string(),
                    Value::String(table_name.to_string()),
                );
            }
        }
    }
    serde_json::to_vec(&Value::Object(obj)).map_err(|_| "input could not be encoded".to_string())
}

async fn introspect(config: &Config, path: &str) -> Result<ForwardResponse, ForwardError> {
    forward(ForwardRequest {
        base: &config.dynamodb_base,
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
    Response::text_error(502, "dynamodb introspection returned invalid json")
}

fn forward_failure(err: ForwardError) -> Response {
    match err {
        ForwardError::Unreachable(_) => {
            Response::text_error(502, "dynamodb service is unreachable")
        }
        ForwardError::BadBase => {
            Response::text_error(500, "dynamodb service address is misconfigured")
        }
        ForwardError::BadResponse => {
            Response::text_error(502, "dynamodb service returned an invalid response")
        }
    }
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
    fn normalize_fills_table_name() {
        let input = serde_json::json!({ "Item": {} });
        let out = normalize_input(Some(input), "t").unwrap();
        let v: Value = serde_json::from_slice(&out).unwrap();
        assert_eq!(v["TableName"], "t");
    }

    #[test]
    fn normalize_rejects_mismatched_table() {
        let input = serde_json::json!({ "TableName": "other" });
        assert!(normalize_input(Some(input), "t").is_err());
    }

    #[test]
    fn normalize_requires_input() {
        assert!(normalize_input(None, "t").is_err());
        assert!(normalize_input(Some(Value::Null), "t").is_err());
    }

    #[test]
    fn normalize_rejects_non_object() {
        assert!(normalize_input(Some(serde_json::json!([1])), "").is_err());
    }
}
