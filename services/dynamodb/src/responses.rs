//! Typed success-response envelopes for the table-management operations.
//!
//! legacy builds these as `map[string]any{...}` wrapping typed descriptions, and
//! `json.NewEncoder` keeps struct fields in declaration order while sorting map
//! keys. Routing a struct through `serde_json::Value` would re-sort every field
//! (its object is a `BTreeMap`), so the operations serialize *these structs*
//! directly via `wire_json::to_vec` to preserve legacy field order.

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

/// `DescribeLimits` — static numeric limits (map in legacy, so keys are sorted; a
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

/// A PartiQL batch/transaction statement error, mirroring legacy
/// `batchStatementError` (field order `Code`, `Message`).
#[derive(Debug, Clone, Serialize)]
pub struct BatchStatementError {
    #[serde(rename = "Code")]
    pub code: String,
    #[serde(rename = "Message")]
    pub message: String,
}

/// One PartiQL batch/transaction statement result, mirroring legacy
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

/// A stream summary in `ListStreams`, mirroring legacy `streamSummary`
/// (`StreamArn`, `StreamLabel`, `TableName`).
#[derive(Debug, Clone, Serialize)]
pub struct StreamSummary {
    #[serde(rename = "StreamArn")]
    pub stream_arn: String,
    #[serde(rename = "StreamLabel")]
    pub stream_label: String,
    #[serde(rename = "TableName")]
    pub table_name: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct SequenceNumberRange {
    #[serde(
        rename = "EndingSequenceNumber",
        skip_serializing_if = "String::is_empty"
    )]
    pub ending_sequence_number: String,
    #[serde(rename = "StartingSequenceNumber")]
    pub starting_sequence_number: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct ShardDescription {
    #[serde(rename = "SequenceNumberRange")]
    pub sequence_number_range: SequenceNumberRange,
    #[serde(rename = "ShardId")]
    pub shard_id: String,
}

/// The `StreamDescription` body of `DescribeStream`, mirroring legacy
/// `streamDescription` field order. `LastEvaluatedShardId` is omitempty.
#[derive(Debug, Clone, Serialize)]
pub struct StreamDescription {
    #[serde(rename = "CreationRequestDateTime")]
    pub creation_request_date_time: i64,
    #[serde(rename = "KeySchema", skip_serializing_if = "Vec::is_empty")]
    pub key_schema: Vec<crate::model::KeySchemaElement>,
    #[serde(
        rename = "LastEvaluatedShardId",
        skip_serializing_if = "String::is_empty"
    )]
    pub last_evaluated_shard_id: String,
    #[serde(rename = "Shards")]
    pub shards: Vec<ShardDescription>,
    #[serde(rename = "StreamArn")]
    pub stream_arn: String,
    #[serde(rename = "StreamLabel")]
    pub stream_label: String,
    #[serde(rename = "StreamStatus")]
    pub stream_status: String,
    #[serde(rename = "StreamViewType")]
    pub stream_view_type: String,
    #[serde(rename = "TableName")]
    pub table_name: String,
}

/// `GetRecords` response. `Records` keeps `StreamRecord` field order (so it must
/// not be routed through `serde_json::Value`, which would re-sort keys).
#[derive(Debug, Clone, Serialize)]
pub struct GetRecordsResponse {
    #[serde(rename = "NextShardIterator")]
    pub next_shard_iterator: String,
    #[serde(rename = "Records")]
    pub records: Vec<crate::model::StreamRecord>,
}

/// Serializes any response envelope to the legacy wire bytes.
pub fn encode<T: Serialize>(value: &T) -> Vec<u8> {
    crate::wire_json::to_vec(value)
}
