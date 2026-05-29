//! Request payload shapes for the table-management operations.
//!
//! Mirrors the request structs in `internal/services/dynamodb/types.go`. Fields
//! use `#[serde(default)]` so absent keys decode to zero values (matching Go's
//! decode-into-zero-value behavior), and integer fields tolerate the JSON
//! numbers AWS SDKs send.

use std::collections::BTreeMap;

use serde::Deserialize;

use crate::model::{
    AttributeDefinition, AttributeValue, Item, KeySchemaElement, StreamSpecification,
};

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct IndexProjectionRequest {
    #[serde(rename = "ProjectionType")]
    pub projection_type: String,
    #[serde(rename = "NonKeyAttributes")]
    pub non_key_attributes: Vec<String>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct GlobalSecondaryIndexRequest {
    #[serde(rename = "IndexName")]
    pub index_name: String,
    #[serde(rename = "KeySchema")]
    pub key_schema: Vec<KeySchemaElement>,
    #[serde(rename = "Projection")]
    pub projection: IndexProjectionRequest,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct LocalSecondaryIndexRequest {
    #[serde(rename = "IndexName")]
    pub index_name: String,
    #[serde(rename = "KeySchema")]
    pub key_schema: Vec<KeySchemaElement>,
    #[serde(rename = "Projection")]
    pub projection: IndexProjectionRequest,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct CreateTableRequest {
    #[serde(rename = "TableName")]
    pub table_name: String,
    #[serde(rename = "AttributeDefinitions")]
    pub attribute_definitions: Vec<AttributeDefinition>,
    #[serde(rename = "KeySchema")]
    pub key_schema: Vec<KeySchemaElement>,
    #[serde(rename = "GlobalSecondaryIndexes")]
    pub global_secondary_indexes: Vec<GlobalSecondaryIndexRequest>,
    #[serde(rename = "LocalSecondaryIndexes")]
    pub local_secondary_indexes: Vec<LocalSecondaryIndexRequest>,
    #[serde(rename = "BillingMode")]
    pub billing_mode: String,
    #[serde(rename = "StreamSpecification")]
    pub stream_specification: StreamSpecification,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct TableNameRequest {
    #[serde(rename = "TableName")]
    pub table_name: String,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct ListTablesRequest {
    #[serde(rename = "ExclusiveStartTableName")]
    pub exclusive_start_table_name: String,
    #[serde(rename = "Limit")]
    pub limit: i64,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct DeleteGlobalSecondaryIndex {
    #[serde(rename = "IndexName")]
    pub index_name: String,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct UpdateGlobalSecondaryIndex {
    #[serde(rename = "IndexName")]
    pub index_name: String,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct GlobalSecondaryIndexUpdate {
    #[serde(rename = "Create")]
    pub create: Option<GlobalSecondaryIndexRequest>,
    #[serde(rename = "Delete")]
    pub delete: Option<DeleteGlobalSecondaryIndex>,
    #[serde(rename = "Update")]
    pub update: Option<UpdateGlobalSecondaryIndex>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct UpdateTableRequest {
    #[serde(rename = "TableName")]
    pub table_name: String,
    #[serde(rename = "AttributeDefinitions")]
    pub attribute_definitions: Vec<AttributeDefinition>,
    #[serde(rename = "BillingMode")]
    pub billing_mode: String,
    #[serde(rename = "GlobalSecondaryIndexUpdates")]
    pub global_secondary_index_updates: Vec<GlobalSecondaryIndexUpdate>,
    #[serde(rename = "StreamSpecification")]
    pub stream_specification: Option<StreamSpecification>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct PutItemRequest {
    #[serde(rename = "TableName")]
    pub table_name: String,
    #[serde(rename = "Item")]
    pub item: Item,
    #[serde(rename = "ConditionExpression")]
    pub condition_expression: String,
    #[serde(rename = "ExpressionAttributeNames")]
    pub expression_attribute_names: BTreeMap<String, String>,
    #[serde(rename = "ExpressionAttributeValues")]
    pub expression_attribute_values: BTreeMap<String, AttributeValue>,
    #[serde(rename = "ReturnValues")]
    pub return_values: String,
    #[serde(rename = "ReturnValuesOnConditionCheckFailure")]
    pub return_values_on_condition_check_failure: String,
    #[serde(rename = "ReturnConsumedCapacity")]
    pub return_consumed_capacity: String,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct GetItemRequest {
    #[serde(rename = "TableName")]
    pub table_name: String,
    #[serde(rename = "Key")]
    pub key: Item,
    #[serde(rename = "ProjectionExpression")]
    pub projection_expression: String,
    #[serde(rename = "ExpressionAttributeNames")]
    pub expression_attribute_names: BTreeMap<String, String>,
    #[serde(rename = "ConsistentRead")]
    pub consistent_read: bool,
    #[serde(rename = "ReturnConsumedCapacity")]
    pub return_consumed_capacity: String,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct DeleteItemRequest {
    #[serde(rename = "TableName")]
    pub table_name: String,
    #[serde(rename = "Key")]
    pub key: Item,
    #[serde(rename = "ConditionExpression")]
    pub condition_expression: String,
    #[serde(rename = "ExpressionAttributeNames")]
    pub expression_attribute_names: BTreeMap<String, String>,
    #[serde(rename = "ExpressionAttributeValues")]
    pub expression_attribute_values: BTreeMap<String, AttributeValue>,
    #[serde(rename = "ReturnValues")]
    pub return_values: String,
    #[serde(rename = "ReturnValuesOnConditionCheckFailure")]
    pub return_values_on_condition_check_failure: String,
    #[serde(rename = "ReturnConsumedCapacity")]
    pub return_consumed_capacity: String,
}
