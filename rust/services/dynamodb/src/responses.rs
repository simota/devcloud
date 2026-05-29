//! Typed success-response envelopes for the table-management operations.
//!
//! Go builds these as `map[string]any{...}` wrapping typed descriptions, and
//! `json.NewEncoder` keeps struct fields in declaration order while sorting map
//! keys. Routing a struct through `serde_json::Value` would re-sort every field
//! (its object is a `BTreeMap`), so the operations serialize *these structs*
//! directly via `go_json::to_vec` to preserve Go's field order.

use serde::Serialize;

use crate::model::TableDescription;

/// `{"TableDescription": ...}` — Create/Delete/Update responses.
#[derive(Debug, Serialize)]
pub struct TableDescriptionResponse {
    #[serde(rename = "TableDescription")]
    pub table_description: TableDescription,
}

/// `{"Table": ...}` — DescribeTable response.
#[derive(Debug, Serialize)]
pub struct DescribeTableResponse {
    #[serde(rename = "Table")]
    pub table: TableDescription,
}

/// `{"TableNames": [...], "LastEvaluatedTableName"?: ...}` — ListTables.
#[derive(Debug, Serialize)]
pub struct ListTablesResponse {
    #[serde(rename = "TableNames")]
    pub table_names: Vec<String>,
    #[serde(
        rename = "LastEvaluatedTableName",
        skip_serializing_if = "Option::is_none"
    )]
    pub last_evaluated_table_name: Option<String>,
}

/// `DescribeLimits` — static numeric limits (map in Go, so keys are sorted; a
/// `BTreeMap`/`map[string]int` matches, but we keep an explicit struct in sorted
/// field order for clarity).
#[derive(Debug, Serialize)]
pub struct DescribeLimitsResponse {
    #[serde(rename = "AccountMaxReadCapacityUnits")]
    pub account_max_read_capacity_units: i64,
    #[serde(rename = "AccountMaxWriteCapacityUnits")]
    pub account_max_write_capacity_units: i64,
    #[serde(rename = "TableMaxReadCapacityUnits")]
    pub table_max_read_capacity_units: i64,
    #[serde(rename = "TableMaxWriteCapacityUnits")]
    pub table_max_write_capacity_units: i64,
}

/// One entry of `DescribeEndpoints`.
#[derive(Debug, Serialize)]
pub struct EndpointEntry {
    #[serde(rename = "Address")]
    pub address: String,
    #[serde(rename = "CachePeriodInMinutes")]
    pub cache_period_in_minutes: i64,
}

/// `{"Endpoints": [...]}` — DescribeEndpoints response.
#[derive(Debug, Serialize)]
pub struct DescribeEndpointsResponse {
    #[serde(rename = "Endpoints")]
    pub endpoints: Vec<EndpointEntry>,
}

/// A PartiQL batch/transaction statement error, mirroring Go
/// `batchStatementError` (field order `Code`, `Message`).
#[derive(Debug, Clone, Serialize)]
pub struct BatchStatementError {
    #[serde(rename = "Code")]
    pub code: String,
    #[serde(rename = "Message")]
    pub message: String,
}

/// One PartiQL batch/transaction statement result, mirroring Go
/// `batchStatementResponse`. Field declaration order is `Error`, `Item`,
/// `TableName`; all three are `omitempty`.
#[derive(Debug, Clone, Default, Serialize)]
pub struct BatchStatementResponse {
    #[serde(rename = "Error", skip_serializing_if = "Option::is_none")]
    pub error: Option<BatchStatementError>,
    #[serde(rename = "Item", skip_serializing_if = "Option::is_none")]
    pub item: Option<crate::model::Item>,
    #[serde(rename = "TableName", skip_serializing_if = "String::is_empty")]
    pub table_name: String,
}

/// Serializes any response envelope to the Go wire bytes.
pub fn encode<T: Serialize>(value: &T) -> Vec<u8> {
    crate::go_json::to_vec(value)
}
