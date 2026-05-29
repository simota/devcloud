//! Core DynamoDB data model — the persisted, byte-compatible shapes.
//!
//! Field declaration order and `omitempty` semantics mirror the Go structs in
//! `internal/services/dynamodb/types.go`. serde emits struct fields in
//! declaration order and `BTreeMap` keys sorted, matching Go's `encoding/json`;
//! the `skip_serializing_if` predicates here reproduce Go's `omitempty`. Generic
//! attribute values use `serde_json::Value` (sorted-key object), matching Go's
//! `map[string]any`.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// A single DynamoDB attribute value, e.g. `{"S": "x"}`, `{"N": "1"}`,
/// `{"M": {...}}`. Go models this as `map[string]any`.
pub type AttributeValue = Value;

/// An item: attribute name -> attribute value. Go: `map[string]attributeValue`,
/// marshaled with sorted keys, so a BTreeMap matches.
pub type Item = BTreeMap<String, AttributeValue>;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AttributeDefinition {
    #[serde(rename = "AttributeName")]
    pub attribute_name: String,
    #[serde(rename = "AttributeType")]
    pub attribute_type: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct KeySchemaElement {
    #[serde(rename = "AttributeName")]
    pub attribute_name: String,
    #[serde(rename = "KeyType")]
    pub key_type: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BillingModeSummary {
    #[serde(rename = "BillingMode")]
    pub billing_mode: String,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct StreamSpecification {
    #[serde(rename = "StreamEnabled")]
    pub stream_enabled: bool,
    #[serde(
        rename = "StreamViewType",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub stream_view_type: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TimeToLiveDescription {
    #[serde(
        rename = "AttributeName",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub attribute_name: String,
    #[serde(rename = "TimeToLiveStatus")]
    pub time_to_live_status: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IndexProjection {
    #[serde(rename = "ProjectionType")]
    pub projection_type: String,
    #[serde(
        rename = "NonKeyAttributes",
        default,
        skip_serializing_if = "Vec::is_empty"
    )]
    pub non_key_attributes: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GlobalSecondaryIndexDescription {
    #[serde(rename = "IndexArn")]
    pub index_arn: String,
    #[serde(rename = "IndexName")]
    pub index_name: String,
    #[serde(rename = "IndexSizeBytes")]
    pub index_size_bytes: i64,
    #[serde(rename = "IndexStatus")]
    pub index_status: String,
    #[serde(rename = "ItemCount")]
    pub item_count: i64,
    #[serde(rename = "KeySchema")]
    pub key_schema: Vec<KeySchemaElement>,
    #[serde(rename = "Projection")]
    pub projection: IndexProjection,
    #[serde(
        rename = "ProvisionedThroughput",
        default,
        skip_serializing_if = "BTreeMap::is_empty"
    )]
    pub provisioned_throughput: BTreeMap<String, i64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LocalSecondaryIndexDescription {
    #[serde(rename = "IndexArn")]
    pub index_arn: String,
    #[serde(rename = "IndexName")]
    pub index_name: String,
    #[serde(rename = "IndexSizeBytes")]
    pub index_size_bytes: i64,
    #[serde(rename = "ItemCount")]
    pub item_count: i64,
    #[serde(rename = "KeySchema")]
    pub key_schema: Vec<KeySchemaElement>,
    #[serde(rename = "Projection")]
    pub projection: IndexProjection,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PointInTimeRecoveryDescription {
    #[serde(rename = "PointInTimeRecoveryStatus")]
    pub point_in_time_recovery_status: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ContinuousBackupsDescription {
    #[serde(rename = "ContinuousBackupsStatus")]
    pub continuous_backups_status: String,
    #[serde(rename = "PointInTimeRecoveryDescription")]
    pub point_in_time_recovery_description: PointInTimeRecoveryDescription,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TableDescription {
    #[serde(
        rename = "AttributeDefinitions",
        default,
        skip_serializing_if = "Vec::is_empty"
    )]
    pub attribute_definitions: Vec<AttributeDefinition>,
    #[serde(
        rename = "BillingModeSummary",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub billing_mode_summary: Option<BillingModeSummary>,
    #[serde(rename = "CreationDateTime")]
    pub creation_date_time: i64,
    #[serde(
        rename = "GlobalSecondaryIndexes",
        default,
        skip_serializing_if = "Vec::is_empty"
    )]
    pub global_secondary_indexes: Vec<GlobalSecondaryIndexDescription>,
    #[serde(rename = "ItemCount")]
    pub item_count: i64,
    #[serde(rename = "KeySchema", default, skip_serializing_if = "Vec::is_empty")]
    pub key_schema: Vec<KeySchemaElement>,
    #[serde(
        rename = "LatestStreamArn",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub latest_stream_arn: String,
    #[serde(
        rename = "LatestStreamLabel",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub latest_stream_label: String,
    #[serde(
        rename = "LocalSecondaryIndexes",
        default,
        skip_serializing_if = "Vec::is_empty"
    )]
    pub local_secondary_indexes: Vec<LocalSecondaryIndexDescription>,
    #[serde(
        rename = "StreamSpecification",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub stream_specification: Option<StreamSpecification>,
    #[serde(rename = "TableArn")]
    pub table_arn: String,
    #[serde(rename = "TableName")]
    pub table_name: String,
    #[serde(rename = "TableSizeBytes")]
    pub table_size_bytes: i64,
    #[serde(rename = "TableStatus")]
    pub table_status: String,
    #[serde(
        rename = "TimeToLiveDescription",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub time_to_live_description: Option<TimeToLiveDescription>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BackupDetails {
    #[serde(rename = "BackupArn")]
    pub backup_arn: String,
    #[serde(rename = "BackupCreationDateTime")]
    pub backup_creation_date_time: i64,
    #[serde(rename = "BackupName")]
    pub backup_name: String,
    #[serde(rename = "BackupSizeBytes")]
    pub backup_size_bytes: i64,
    #[serde(rename = "BackupStatus")]
    pub backup_status: String,
    #[serde(rename = "BackupType")]
    pub backup_type: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SourceTableDetails {
    #[serde(
        rename = "AttributeDefinitions",
        default,
        skip_serializing_if = "Vec::is_empty"
    )]
    pub attribute_definitions: Vec<AttributeDefinition>,
    #[serde(
        rename = "BillingMode",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub billing_mode: String,
    #[serde(rename = "ItemCount")]
    pub item_count: i64,
    #[serde(rename = "KeySchema", default, skip_serializing_if = "Vec::is_empty")]
    pub key_schema: Vec<KeySchemaElement>,
    #[serde(rename = "TableArn")]
    pub table_arn: String,
    #[serde(rename = "TableCreationDateTime")]
    pub table_creation_date_time: i64,
    #[serde(rename = "TableId")]
    pub table_id: String,
    #[serde(rename = "TableName")]
    pub table_name: String,
    #[serde(rename = "TableSizeBytes")]
    pub table_size_bytes: i64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BackupDescription {
    #[serde(rename = "BackupDetails")]
    pub backup_details: BackupDetails,
    #[serde(rename = "SourceTableDetails")]
    pub source_table_details: SourceTableDetails,
}

/// A `ListBackups` summary entry, mirroring Go `backupSummary` field order.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct BackupSummary {
    #[serde(rename = "BackupArn")]
    pub backup_arn: String,
    #[serde(rename = "BackupCreationDateTime")]
    pub backup_creation_date_time: i64,
    #[serde(rename = "BackupName")]
    pub backup_name: String,
    #[serde(rename = "BackupSizeBytes")]
    pub backup_size_bytes: i64,
    #[serde(rename = "BackupStatus")]
    pub backup_status: String,
    #[serde(rename = "BackupType")]
    pub backup_type: String,
    #[serde(rename = "TableArn")]
    pub table_arn: String,
    #[serde(rename = "TableName")]
    pub table_name: String,
}

/// The `dynamodb` image of a stream record. Field declaration order and
/// `omitempty` mirror Go `streamRecordImage`. `New`/`OldImage` are dropped when
/// empty; the other fields always serialize.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct StreamRecordImage {
    #[serde(rename = "ApproximateCreationDateTime")]
    pub approximate_creation_date_time: i64,
    #[serde(rename = "Keys")]
    pub keys: Item,
    #[serde(rename = "NewImage", default, skip_serializing_if = "Item::is_empty")]
    pub new_image: Item,
    #[serde(rename = "OldImage", default, skip_serializing_if = "Item::is_empty")]
    pub old_image: Item,
    #[serde(rename = "SequenceNumber")]
    pub sequence_number: String,
    #[serde(rename = "SizeBytes")]
    pub size_bytes: i64,
    #[serde(rename = "StreamViewType")]
    pub stream_view_type: String,
}

/// A single stream record. Field declaration order and the lowercase JSON keys
/// mirror Go `streamRecord`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StreamRecord {
    #[serde(rename = "eventID")]
    pub event_id: String,
    #[serde(rename = "eventName")]
    pub event_name: String,
    #[serde(rename = "eventSource")]
    pub event_source: String,
    #[serde(rename = "eventVersion")]
    pub event_version: String,
    #[serde(rename = "awsRegion")]
    pub aws_region: String,
    #[serde(rename = "dynamodb")]
    pub dynamodb: StreamRecordImage,
}
