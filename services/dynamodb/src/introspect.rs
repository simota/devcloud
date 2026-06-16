//! Read-only introspection API (`/_introspect/...`).
//!
//! A faithful port of `internal/services/dynamodb/introspect.rs` plus the
//! dashboard snapshot helpers in `internal/services/dynamodb/dashboard.rs`. The
//! single-binary dashboard's DynamoDB browse forwards metadata reads here; the
//! provider API itself (AWS JSON 1.0, dispatched by `X-Amz-Target`) is untouched.
//!
//! CONVENTION (reused verbatim across every service):
//!   - All introspection routes live under the `/_introspect/` prefix and are
//!     intercepted at the top of the HTTP handler, BEFORE the provider-protocol
//!     dispatch.
//!   - Methods are GET-only and read-only. No mutation endpoints here.
//!   - Each response body is the exact JSON encoding of the same snapshot shapes
//!     the legacy dashboard serializes in-process.
//!   - A missing resource returns 404; an unsupported method returns 405.
//!
//! JSON byte-compatibility: the top-level wrapper objects legacy builds as
//! `map[string]any` come out with sorted keys, so the wrapper structs below
//! declare their fields in sorted-key order; inner typed values keep their own
//! declaration order. Bodies are encoded through [`wire_json::to_vec`] (compact,
//! HTML-escaped, trailing newline) exactly like `writeJSON`.

use serde::Serialize;

use crate::errors::ApiError;
use crate::http::Outcome;
use crate::model::{
    GlobalSecondaryIndexDescription, KeySchemaElement, LocalSecondaryIndexDescription,
    StreamSpecification, TimeToLiveDescription,
};
use crate::server::Server;

pub const INTROSPECT_PREFIX: &str = "/_introspect/";

/// Reports whether the request targets the introspection API.
pub fn is_introspect_path(path: &str) -> bool {
    path.starts_with(INTROSPECT_PREFIX)
}

// --- snapshot shapes (mirror DashboardSnapshot / DashboardTableSnapshot) -----

/// Mirrors legacy `DashboardSnapshot` (struct field order: running, status, region,
/// tables).
#[derive(Debug, Serialize)]
struct DashboardSnapshot {
    running: bool,
    status: String,
    region: String,
    tables: Vec<DashboardTableSnapshot>,
}

/// Mirrors legacy `DashboardTableSnapshot`. Field declaration order and the
/// `omitempty` predicates match the legacy struct; `timeToLiveDescription` always
/// serializes (non-pointer in legacy).
#[derive(Debug, Serialize)]
struct DashboardTableSnapshot {
    #[serde(rename = "tableName")]
    table_name: String,
    #[serde(rename = "tableStatus")]
    table_status: String,
    #[serde(rename = "itemCount")]
    item_count: i64,
    #[serde(rename = "keySchema", skip_serializing_if = "Vec::is_empty")]
    key_schema: Vec<KeySchemaElement>,
    #[serde(
        rename = "globalSecondaryIndexes",
        skip_serializing_if = "Vec::is_empty"
    )]
    global_secondary_indexes: Vec<GlobalSecondaryIndexDescription>,
    #[serde(
        rename = "localSecondaryIndexes",
        skip_serializing_if = "Vec::is_empty"
    )]
    local_secondary_indexes: Vec<LocalSecondaryIndexDescription>,
    #[serde(rename = "latestStreamArn", skip_serializing_if = "String::is_empty")]
    latest_stream_arn: String,
    #[serde(rename = "latestStreamLabel", skip_serializing_if = "String::is_empty")]
    latest_stream_label: String,
    #[serde(
        rename = "streamSpecification",
        skip_serializing_if = "Option::is_none"
    )]
    stream_specification: Option<StreamSpecification>,
    #[serde(rename = "timeToLiveDescription")]
    time_to_live_description: TimeToLiveDescription,
}

/// Mirrors legacy `DashboardItemSnapshot` (struct field order: key, item). The key
/// and item maps are sorted-key objects (legacy `map[string]any`), so `Item`
/// (a `BTreeMap`) reproduces them.
#[derive(Debug, Serialize)]
struct DashboardItemSnapshot {
    key: crate::model::Item,
    item: crate::model::Item,
}

// --- top-level wrappers (legacy `map[string]any`, sorted keys) -------------------
//
// Each wrapper declares its fields in sorted-key order so direct serde
// serialization matches legacy map-key sorting at the top level.

/// `{"items": [...], "tableName": "..."}`.
#[derive(Debug, Serialize)]
struct ItemsResponse {
    items: Vec<DashboardItemSnapshot>,
    #[serde(rename = "tableName")]
    table_name: String,
}

/// `{"globalSecondaryIndexes": [...], "localSecondaryIndexes": [...], "tableName": "..."}`.
#[derive(Debug, Serialize)]
struct IndexesResponse {
    #[serde(rename = "globalSecondaryIndexes")]
    global_secondary_indexes: Vec<GlobalSecondaryIndexDescription>,
    #[serde(rename = "localSecondaryIndexes")]
    local_secondary_indexes: Vec<LocalSecondaryIndexDescription>,
    #[serde(rename = "tableName")]
    table_name: String,
}

/// `{"tableName": "...", "timeToLiveDescription": {...}}`.
#[derive(Debug, Serialize)]
struct TtlResponse {
    #[serde(rename = "tableName")]
    table_name: String,
    #[serde(rename = "timeToLiveDescription")]
    time_to_live_description: TimeToLiveDescription,
}

/// `{"latestStreamArn": "...", "latestStreamLabel": "...", "streamEnabled": bool, "streamSpecification": {...}|null, "tableName": "..."}`.
///
/// Unlike the snapshot struct, the stream-arn/label fields are placed into a legacy
/// `map[string]any` and therefore always serialize (no omitempty), and
/// `streamSpecification` serializes as `null` when absent.
#[derive(Debug, Serialize)]
struct StreamsResponse {
    #[serde(rename = "latestStreamArn")]
    latest_stream_arn: String,
    #[serde(rename = "latestStreamLabel")]
    latest_stream_label: String,
    #[serde(rename = "streamEnabled")]
    stream_enabled: bool,
    #[serde(rename = "streamSpecification")]
    stream_specification: Option<StreamSpecification>,
    #[serde(rename = "tableName")]
    table_name: String,
}

impl Server {
    /// Serves the read-only introspection endpoints. GET-only; non-GET → 405,
    /// unknown subpath → 404. Mirrors `handleIntrospect`.
    pub fn handle_introspect(&self, method: &str, path: &str, query: &str) -> Outcome {
        if method != "GET" {
            return introspect_error(
                405,
                "ValidationException",
                "introspection endpoints are read-only",
            );
        }

        let rest = path.strip_prefix(INTROSPECT_PREFIX).unwrap_or("");

        if rest == "tables" {
            return ok_body(self.dashboard_snapshot());
        }

        if let Some(after) = rest.strip_prefix("tables/") {
            let segments: Vec<&str> = after.split('/').collect();
            let name = segments[0];
            if !name.is_empty() {
                match segments.len() {
                    1 => {
                        let Some(table) = self.table_snapshot(name) else {
                            return not_found_table();
                        };
                        return ok_body(table);
                    }
                    2 => {
                        let Some(table) = self.table_snapshot(name) else {
                            return not_found_table();
                        };
                        match segments[1] {
                            "items" => return self.introspect_items(name, query),
                            "indexes" => {
                                return ok_body(IndexesResponse {
                                    global_secondary_indexes: table.global_secondary_indexes,
                                    local_secondary_indexes: table.local_secondary_indexes,
                                    table_name: name.to_string(),
                                });
                            }
                            "ttl" => {
                                return ok_body(TtlResponse {
                                    table_name: name.to_string(),
                                    time_to_live_description: table.time_to_live_description,
                                });
                            }
                            "streams" => {
                                let stream_enabled = table
                                    .stream_specification
                                    .as_ref()
                                    .map(|s| s.stream_enabled)
                                    .unwrap_or(false);
                                return ok_body(StreamsResponse {
                                    latest_stream_arn: table.latest_stream_arn,
                                    latest_stream_label: table.latest_stream_label,
                                    stream_enabled,
                                    stream_specification: table.stream_specification,
                                    table_name: name.to_string(),
                                });
                            }
                            _ => {}
                        }
                    }
                    _ => {}
                }
            }
        }

        introspect_error(
            404,
            "ResourceNotFoundException",
            "introspection endpoint not found",
        )
    }

    /// Serves `/_introspect/tables/{name}/items`, parsing the optional `limit`
    /// query parameter (positive integer; default 100). Mirrors the `items`
    /// branch of `handleIntrospect`.
    fn introspect_items(&self, name: &str, query: &str) -> Outcome {
        let mut limit: i64 = 100;
        if let Some(raw) = query_param(query, "limit") {
            match raw.parse::<i64>() {
                Ok(parsed) if parsed > 0 => limit = parsed,
                _ => {
                    return introspect_error(
                        400,
                        "ValidationException",
                        "limit must be a positive integer",
                    );
                }
            }
        }
        let Some(items) = self.table_items(name, limit) else {
            return not_found_table();
        };
        ok_body(ItemsResponse {
            items,
            table_name: name.to_string(),
        })
    }

    /// Mirrors legacy `Server.Snapshot()`.
    fn dashboard_snapshot(&self) -> DashboardSnapshot {
        // `tables` is a BTreeMap, so iteration is already name-sorted, matching
        // legacy explicit sort.Slice on TableName.
        let tables = self
            .tables
            .values()
            .map(|state| table_snapshot_from(&state.description))
            .collect();
        DashboardSnapshot {
            running: true,
            status: "running".to_string(),
            // `sigv4_region()` returns the defaulted region (us-east-1 when
            // unset), matching legacy `defaultString(s.config.Region, "us-east-1")`.
            region: self.sigv4_region().to_string(),
            tables,
        }
    }

    /// Mirrors legacy `Server.TableSnapshot(name)`.
    fn table_snapshot(&self, name: &str) -> Option<DashboardTableSnapshot> {
        self.tables
            .get(name)
            .map(|state| table_snapshot_from(&state.description))
    }

    /// Mirrors legacy `Server.TableItems(name, limit)`: sorted by the internal item
    /// key, capped at `min(limit, 100)`, returning `{key, item}` pairs. Items
    /// whose primary key cannot be extracted are skipped.
    fn table_items(&self, name: &str, mut limit: i64) -> Option<Vec<DashboardItemSnapshot>> {
        let state = self.tables.get(name)?;
        if limit <= 0 || limit > 100 {
            limit = 100;
        }
        let mut items = Vec::new();
        // `items` is a BTreeMap keyed by the internal item-key string, so
        // iteration matches legacy sortedItems ordering.
        for value in state.items.values() {
            if items.len() as i64 == limit {
                break;
            }
            let Ok(key) = crate::attribute::extract_key(&state.description, value) else {
                continue;
            };
            items.push(DashboardItemSnapshot {
                key,
                item: value.clone(),
            });
        }
        Some(items)
    }
}

/// Builds a `DashboardTableSnapshot` from a table description, mirroring the
/// field copying in legacy `Snapshot()`/`TableSnapshot()` (including `ttlDescription`).
fn table_snapshot_from(description: &crate::model::TableDescription) -> DashboardTableSnapshot {
    DashboardTableSnapshot {
        table_name: description.table_name.clone(),
        table_status: description.table_status.clone(),
        item_count: description.item_count,
        key_schema: description.key_schema.clone(),
        global_secondary_indexes: description.global_secondary_indexes.clone(),
        local_secondary_indexes: description.local_secondary_indexes.clone(),
        latest_stream_arn: description.latest_stream_arn.clone(),
        latest_stream_label: description.latest_stream_label.clone(),
        stream_specification: description.stream_specification.clone(),
        time_to_live_description: ttl_description(description),
    }
}

/// Mirrors legacy `ttlDescription`: a disabled description when the table carries
/// none, otherwise a clone of the stored one.
fn ttl_description(description: &crate::model::TableDescription) -> TimeToLiveDescription {
    match &description.time_to_live_description {
        None => TimeToLiveDescription {
            attribute_name: String::new(),
            time_to_live_status: "DISABLED".to_string(),
        },
        Some(ttl) => ttl.clone(),
    }
}

/// Encodes a successful introspection body through the legacy-compatible encoder.
fn ok_body<T: Serialize>(value: T) -> Outcome {
    Outcome {
        status: 200,
        error_type: None,
        body: crate::wire_json::to_vec(&value),
    }
}

/// Builds an introspection error outcome with the given status (legacy
/// `writeError` allows any status; `Outcome::error` hard-codes the ApiError
/// default, so build it explicitly here to preserve 404/405/400).
fn introspect_error(status: u16, name: &str, message: &str) -> Outcome {
    let err = ApiError::new(status, name, message);
    Outcome {
        status: err.status,
        error_type: Some(err.name.clone()),
        body: err.body_bytes(),
    }
}

/// The 404 returned when a named table is absent (matches `handleIntrospect`).
fn not_found_table() -> Outcome {
    introspect_error(404, "ResourceNotFoundException", "table does not exist")
}

/// Extracts the first value of `key` from a raw query string (`a=1&b=2`), with
/// no percent-decoding (the dashboard sends a bare integer `limit`).
fn query_param<'a>(query: &'a str, key: &str) -> Option<&'a str> {
    for pair in query.split('&') {
        if pair.is_empty() {
            continue;
        }
        match pair.split_once('=') {
            Some((k, v)) if k == key => return Some(v),
            None if pair == key => return Some(""),
            _ => {}
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    fn region_server() -> Server {
        Server::new(crate::server::Config {
            region: "us-east-1".to_string(),
            ..Default::default()
        })
    }

    #[test]
    fn is_introspect_path_matches_prefix() {
        assert!(is_introspect_path("/_introspect/tables"));
        assert!(!is_introspect_path("/"));
    }

    #[test]
    fn non_get_is_405() {
        let server = region_server();
        let out = server.handle_introspect("POST", "/_introspect/tables", "");
        assert_eq!(out.status, 405);
        assert_eq!(out.error_type.as_deref(), Some("ValidationException"));
    }

    #[test]
    fn unknown_subpath_is_404() {
        let server = region_server();
        let out = server.handle_introspect("GET", "/_introspect/nope", "");
        assert_eq!(out.status, 404);
        assert_eq!(out.error_type.as_deref(), Some("ResourceNotFoundException"));
    }

    #[test]
    fn empty_tables_snapshot_shape() {
        let server = region_server();
        let out = server.handle_introspect("GET", "/_introspect/tables", "");
        assert_eq!(out.status, 200);
        assert_eq!(
            String::from_utf8(out.body).unwrap(),
            "{\"running\":true,\"status\":\"running\",\"region\":\"us-east-1\",\"tables\":[]}\n"
        );
    }

    #[test]
    fn missing_table_detail_is_404() {
        let server = region_server();
        let out = server.handle_introspect("GET", "/_introspect/tables/missing", "");
        assert_eq!(out.status, 404);
    }

    #[test]
    fn missing_table_items_is_404_before_limit_parse() {
        // Mirrors handleIntrospect: the table 404 check precedes limit parsing,
        // so a bad limit on a missing table still returns 404.
        let server = region_server();
        let out = server.handle_introspect("GET", "/_introspect/tables/missing/items", "limit=0");
        assert_eq!(out.status, 404);
        assert_eq!(out.error_type.as_deref(), Some("ResourceNotFoundException"));
    }

    #[test]
    fn query_param_parses_first_match() {
        assert_eq!(query_param("limit=5", "limit"), Some("5"));
        assert_eq!(query_param("a=1&limit=7&b=2", "limit"), Some("7"));
        assert_eq!(query_param("a=1", "limit"), None);
        assert_eq!(query_param("", "limit"), None);
    }
}
