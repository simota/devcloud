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
pub struct ExecuteStatementRequest {
    #[serde(rename = "Statement")]
    pub statement: String,
    #[serde(rename = "Parameters")]
    pub parameters: Vec<AttributeValue>,
    #[serde(rename = "ConsistentRead")]
    pub consistent_read: bool,
    #[serde(rename = "Limit")]
    pub limit: i64,
    #[serde(rename = "ReturnConsumedCapacity")]
    pub return_consumed_capacity: String,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct BatchStatementRequest {
    #[serde(rename = "Statement")]
    pub statement: String,
    #[serde(rename = "Parameters")]
    pub parameters: Vec<AttributeValue>,
    #[serde(rename = "ConsistentRead")]
    pub consistent_read: bool,
    #[serde(rename = "ReturnValuesOnConditionCheckFailure")]
    pub return_values_on_condition_check_failure: String,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct BatchExecuteStatementRequest {
    #[serde(rename = "Statements")]
    pub statements: Vec<BatchStatementRequest>,
    #[serde(rename = "ReturnConsumedCapacity")]
    pub return_consumed_capacity: String,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct ExecuteTransactionRequest {
    #[serde(rename = "TransactStatements")]
    pub transact_statements: Vec<BatchStatementRequest>,
    #[serde(rename = "ReturnConsumedCapacity")]
    pub return_consumed_capacity: String,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct BatchGetTableRequest {
    #[serde(rename = "Keys")]
    pub keys: Vec<Item>,
    #[serde(rename = "ProjectionExpression")]
    pub projection_expression: String,
    #[serde(rename = "ExpressionAttributeNames")]
    pub expression_attribute_names: BTreeMap<String, String>,
    #[serde(rename = "ConsistentRead")]
    pub consistent_read: bool,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct BatchGetItemRequest {
    #[serde(rename = "RequestItems")]
    pub request_items: BTreeMap<String, BatchGetTableRequest>,
    #[serde(rename = "ReturnConsumedCapacity")]
    pub return_consumed_capacity: String,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct PutRequest {
    #[serde(rename = "Item")]
    pub item: Item,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct DeleteRequest {
    #[serde(rename = "Key")]
    pub key: Item,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct WriteRequest {
    #[serde(rename = "PutRequest")]
    pub put_request: Option<PutRequest>,
    #[serde(rename = "DeleteRequest")]
    pub delete_request: Option<DeleteRequest>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct BatchWriteItemRequest {
    #[serde(rename = "RequestItems")]
    pub request_items: BTreeMap<String, Vec<WriteRequest>>,
    #[serde(rename = "ReturnConsumedCapacity")]
    pub return_consumed_capacity: String,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct TransactGet {
    #[serde(rename = "TableName")]
    pub table_name: String,
    #[serde(rename = "Key")]
    pub key: Item,
    #[serde(rename = "ProjectionExpression")]
    pub projection_expression: String,
    #[serde(rename = "ExpressionAttributeNames")]
    pub expression_attribute_names: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct TransactGetItem {
    #[serde(rename = "Get")]
    pub get: Option<TransactGet>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct TransactGetItemsRequest {
    #[serde(rename = "TransactItems")]
    pub transact_items: Vec<TransactGetItem>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct TransactPut {
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
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct TransactUpdate {
    #[serde(rename = "TableName")]
    pub table_name: String,
    #[serde(rename = "Key")]
    pub key: Item,
    #[serde(rename = "UpdateExpression")]
    pub update_expression: String,
    #[serde(rename = "ConditionExpression")]
    pub condition_expression: String,
    #[serde(rename = "ExpressionAttributeNames")]
    pub expression_attribute_names: BTreeMap<String, String>,
    #[serde(rename = "ExpressionAttributeValues")]
    pub expression_attribute_values: BTreeMap<String, AttributeValue>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct TransactDelete {
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
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct TransactConditionCheck {
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
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct TransactWriteItem {
    #[serde(rename = "Put")]
    pub put: Option<TransactPut>,
    #[serde(rename = "Update")]
    pub update: Option<TransactUpdate>,
    #[serde(rename = "Delete")]
    pub delete: Option<TransactDelete>,
    #[serde(rename = "ConditionCheck")]
    pub condition_check: Option<TransactConditionCheck>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct TransactWriteItemsRequest {
    #[serde(rename = "TransactItems")]
    pub transact_items: Vec<TransactWriteItem>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct QueryRequest {
    #[serde(rename = "TableName")]
    pub table_name: String,
    #[serde(rename = "IndexName")]
    pub index_name: String,
    #[serde(rename = "KeyConditionExpression")]
    pub key_condition_expression: String,
    #[serde(rename = "ExpressionAttributeNames")]
    pub expression_attribute_names: BTreeMap<String, String>,
    #[serde(rename = "ExpressionAttributeValues")]
    pub expression_attribute_values: BTreeMap<String, AttributeValue>,
    #[serde(rename = "ProjectionExpression")]
    pub projection_expression: String,
    #[serde(rename = "Select")]
    pub select: String,
    #[serde(rename = "ExclusiveStartKey")]
    pub exclusive_start_key: Item,
    #[serde(rename = "Limit")]
    pub limit: i64,
    // Go uses `*bool` (absent => default ascending); model it as Option.
    #[serde(rename = "ScanIndexForward")]
    pub scan_index_forward: Option<bool>,
    #[serde(rename = "ReturnConsumedCapacity")]
    pub return_consumed_capacity: String,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct ScanRequest {
    #[serde(rename = "TableName")]
    pub table_name: String,
    #[serde(rename = "IndexName")]
    pub index_name: String,
    #[serde(rename = "FilterExpression")]
    pub filter_expression: String,
    #[serde(rename = "ExpressionAttributeNames")]
    pub expression_attribute_names: BTreeMap<String, String>,
    #[serde(rename = "ExpressionAttributeValues")]
    pub expression_attribute_values: BTreeMap<String, AttributeValue>,
    #[serde(rename = "ProjectionExpression")]
    pub projection_expression: String,
    #[serde(rename = "Select")]
    pub select: String,
    #[serde(rename = "ExclusiveStartKey")]
    pub exclusive_start_key: Item,
    #[serde(rename = "Limit")]
    pub limit: i64,
    #[serde(rename = "ReturnConsumedCapacity")]
    pub return_consumed_capacity: String,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct UpdateItemRequest {
    #[serde(rename = "TableName")]
    pub table_name: String,
    #[serde(rename = "Key")]
    pub key: Item,
    #[serde(rename = "ConditionExpression")]
    pub condition_expression: String,
    #[serde(rename = "UpdateExpression")]
    pub update_expression: String,
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
