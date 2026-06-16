//! Redshift Data API request/response models and pure helpers.
//!
//! Parity: `internal/services/redshift/dataapi.rs`. The handler wiring lives in
//! `http_api.rs`; this module holds the typed AWS-JSON request/response structs
//! and the value/metadata conversion helpers (`dataAPIField`,
//! `columnMetadataFromPGField`, pagination, CSV records, redaction).

use serde::{Deserialize, Serialize};

use crate::engine::QueryResult;
use crate::errors::SqlError;
use crate::model::Column;
use crate::pg_types::{
    pg_type_oid, pg_type_size, PgField, PG_TYPE_BOOL_OID, PG_TYPE_FLOAT8_OID, PG_TYPE_INT4_OID,
    PG_TYPE_VARCHAR_OID,
};
use crate::server::StatementRecord;
use crate::snapshot::safe_sql_preview;
use crate::storage::unix_seconds;

// --- requests --------------------------------------------------------------

#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub struct ExecuteStatementRequest {
    #[serde(rename = "ClusterIdentifier")]
    pub cluster_identifier: String,
    #[serde(rename = "Database")]
    pub database: String,
    #[serde(rename = "DbUser")]
    pub db_user: String,
    #[serde(rename = "Sql")]
    pub sql: String,
    #[serde(rename = "ClientToken")]
    pub client_token: String,
    #[serde(rename = "SessionId")]
    pub session_id: String,
    #[serde(rename = "SessionKeepAliveSeconds")]
    pub session_keep_alive_seconds: i64,
    #[serde(rename = "ResultFormat")]
    pub result_format: String,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub struct BatchExecuteStatementRequest {
    #[serde(rename = "ClusterIdentifier")]
    pub cluster_identifier: String,
    #[serde(rename = "Database")]
    pub database: String,
    #[serde(rename = "DbUser")]
    pub db_user: String,
    #[serde(rename = "Sqls")]
    pub sqls: Vec<String>,
    #[serde(rename = "ClientToken")]
    pub client_token: String,
    #[serde(rename = "SessionId")]
    pub session_id: String,
    #[serde(rename = "SessionKeepAliveSeconds")]
    pub session_keep_alive_seconds: i64,
    #[serde(rename = "ResultFormat")]
    pub result_format: String,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub struct StatementIdRequest {
    #[serde(rename = "Id")]
    pub id: String,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub struct GetStatementResultRequest {
    #[serde(rename = "Id")]
    pub id: String,
    #[serde(rename = "MaxResults")]
    pub max_results: i64,
    #[serde(rename = "NextToken")]
    pub next_token: String,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub struct ListStatementsRequest {
    #[serde(rename = "Status")]
    pub status: String,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub struct ListMetadataRequest {
    #[serde(rename = "ClusterIdentifier")]
    pub cluster_identifier: String,
    #[serde(rename = "ConnectedDatabase")]
    pub connected_database: String,
    #[serde(rename = "Database")]
    pub database: String,
    #[serde(rename = "DbUser")]
    pub db_user: String,
    #[serde(rename = "Schema")]
    pub schema: String,
    #[serde(rename = "SchemaPattern")]
    pub schema_pattern: String,
    #[serde(rename = "TablePattern")]
    pub table_pattern: String,
    #[serde(rename = "MaxResults")]
    pub max_results: i64,
    #[serde(rename = "NextToken")]
    pub next_token: String,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub struct DescribeTableRequest {
    #[serde(rename = "ClusterIdentifier")]
    pub cluster_identifier: String,
    #[serde(rename = "ConnectedDatabase")]
    pub connected_database: String,
    #[serde(rename = "Database")]
    pub database: String,
    #[serde(rename = "DbUser")]
    pub db_user: String,
    #[serde(rename = "Schema")]
    pub schema: String,
    #[serde(rename = "Table")]
    pub table: String,
    #[serde(rename = "MaxResults")]
    pub max_results: i64,
    #[serde(rename = "NextToken")]
    pub next_token: String,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub struct ServerlessListRequest {
    #[serde(rename = "maxResults")]
    pub max_results: i64,
    #[serde(rename = "nextToken")]
    pub next_token: String,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub struct ServerlessNamespaceRequest {
    #[serde(rename = "namespaceName")]
    pub namespace_name: String,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub struct ServerlessWorkgroupRequest {
    #[serde(rename = "workgroupName")]
    pub workgroup_name: String,
}

// --- responses -------------------------------------------------------------

#[derive(Debug, Default, Serialize)]
pub struct ExecuteStatementResponse {
    #[serde(rename = "Id")]
    pub id: String,
    #[serde(rename = "ClusterIdentifier")]
    pub cluster_identifier: String,
    #[serde(rename = "Database")]
    pub database: String,
    #[serde(rename = "DbUser")]
    pub db_user: String,
    #[serde(rename = "SessionId", skip_serializing_if = "String::is_empty")]
    pub session_id: String,
    #[serde(rename = "CreatedAt")]
    pub created_at: i64,
}

#[derive(Debug, Default, Serialize)]
pub struct DescribeStatementResponse {
    #[serde(rename = "Id")]
    pub id: String,
    #[serde(rename = "ClusterIdentifier")]
    pub cluster_identifier: String,
    #[serde(rename = "Database")]
    pub database: String,
    #[serde(rename = "DbUser")]
    pub db_user: String,
    #[serde(rename = "SessionId", skip_serializing_if = "String::is_empty")]
    pub session_id: String,
    #[serde(rename = "QueryString")]
    pub query_string: String,
    #[serde(rename = "Status")]
    pub status: String,
    #[serde(rename = "Error", skip_serializing_if = "String::is_empty")]
    pub error: String,
    #[serde(rename = "CreatedAt")]
    pub created_at: i64,
    #[serde(rename = "UpdatedAt")]
    pub updated_at: i64,
    #[serde(rename = "Duration")]
    pub duration: i64,
    #[serde(rename = "HasResultSet")]
    pub has_result_set: bool,
    #[serde(rename = "ResultRows")]
    pub result_rows: i64,
    #[serde(rename = "ResultSize")]
    pub result_size: i64,
    #[serde(rename = "RedshiftQueryId")]
    pub redshift_query_id: i64,
}

#[derive(Debug, Default, Serialize)]
pub struct StatementListItem {
    #[serde(rename = "Id")]
    pub id: String,
    #[serde(rename = "QueryString")]
    pub query_string: String,
    #[serde(rename = "Status")]
    pub status: String,
    #[serde(rename = "CreatedAt")]
    pub created_at: i64,
    #[serde(rename = "UpdatedAt")]
    pub updated_at: i64,
    #[serde(rename = "HasResultSet")]
    pub has_result_set: bool,
}

/// Mirrors `dataAPIResultField`. legacy uses pointers + omitempty so exactly one of
/// the value fields (or `isNull`) is present; `Option` + skip mirrors that.
#[derive(Debug, Default, Serialize)]
pub struct DataApiResultField {
    #[serde(rename = "isNull", skip_serializing_if = "is_false")]
    pub is_null: bool,
    #[serde(rename = "longValue", skip_serializing_if = "Option::is_none")]
    pub long_value: Option<i64>,
    #[serde(rename = "doubleValue", skip_serializing_if = "Option::is_none")]
    pub double_value: Option<f64>,
    #[serde(rename = "booleanValue", skip_serializing_if = "Option::is_none")]
    pub boolean_value: Option<bool>,
    #[serde(rename = "stringValue", skip_serializing_if = "Option::is_none")]
    pub string_value: Option<String>,
}

#[derive(Debug, Default, Serialize)]
pub struct ColumnMetadata {
    pub name: String,
    pub label: String,
    #[serde(rename = "schemaName", skip_serializing_if = "String::is_empty")]
    pub schema_name: String,
    #[serde(rename = "tableName", skip_serializing_if = "String::is_empty")]
    pub table_name: String,
    #[serde(rename = "typeName")]
    pub type_name: String,
    #[serde(rename = "columnDefault", skip_serializing_if = "String::is_empty")]
    pub column_default: String,
    #[serde(rename = "isCaseSensitive")]
    pub is_case_sensitive: bool,
    #[serde(rename = "isSigned")]
    pub is_signed: bool,
    pub nullable: i64,
    pub precision: i64,
    pub scale: i64,
}

#[derive(Debug, Clone)]
pub struct TableMember {
    pub name: String,
    pub schema: String,
    pub member_type: String,
}

fn is_false(value: &bool) -> bool {
    !*value
}

// --- conversions -----------------------------------------------------------

/// Mirrors `executeStatementResponseFromStatement`.
pub fn execute_statement_response_from_statement(
    stmt: &StatementRecord,
) -> ExecuteStatementResponse {
    ExecuteStatementResponse {
        id: stmt.id.clone(),
        cluster_identifier: stmt.cluster_identifier.clone(),
        database: stmt.database.clone(),
        db_user: stmt.db_user.clone(),
        session_id: stmt.session_id.clone(),
        created_at: unix_seconds(stmt.created_at),
    }
}

/// Mirrors `describeStatementResponseFromStatement`.
pub fn describe_statement_response_from_statement(
    stmt: &StatementRecord,
) -> DescribeStatementResponse {
    DescribeStatementResponse {
        id: stmt.id.clone(),
        cluster_identifier: stmt.cluster_identifier.clone(),
        database: stmt.database.clone(),
        db_user: stmt.db_user.clone(),
        session_id: stmt.session_id.clone(),
        query_string: safe_statement_query_string(&stmt.query_string),
        status: stmt.status.clone(),
        error: stmt.error.clone(),
        created_at: unix_seconds(stmt.created_at),
        updated_at: unix_seconds(stmt.updated_at),
        duration: 0,
        has_result_set: stmt.has_result_set,
        result_rows: stmt.result.rows.len() as i64,
        result_size: approximate_result_size(&stmt.result),
        redshift_query_id: crate::snapshot::redshift_query_id(&stmt.id),
    }
}

/// Mirrors `getStatementResultResponse` as an ordered JSON object.
pub fn get_statement_result_response(
    result: &QueryResult,
    rows: &[Vec<String>],
) -> serde_json::Value {
    let records: Vec<Vec<DataApiResultField>> = rows
        .iter()
        .map(|row| {
            row.iter()
                .enumerate()
                .map(|(i, value)| {
                    let type_oid = result
                        .fields
                        .get(i)
                        .map(|f| f.type_oid)
                        .unwrap_or(PG_TYPE_VARCHAR_OID);
                    data_api_field(value, type_oid)
                })
                .collect()
        })
        .collect();
    let metadata: Vec<ColumnMetadata> = result
        .fields
        .iter()
        .enumerate()
        .map(|(i, field)| column_metadata_from_pg_field(field, i))
        .collect();
    serde_json::json!({
        "ColumnMetadata": metadata,
        "Records": records,
        "TotalNumRows": result.rows.len(),
    })
}

/// Mirrors `getStatementResultV2Response`.
pub fn get_statement_result_v2_response(
    result: &QueryResult,
    rows: &[Vec<String>],
) -> Result<serde_json::Value, SqlError> {
    let mut records = Vec::with_capacity(rows.len());
    for row in rows {
        let record = csv_record(row)?;
        records.push(serde_json::json!({ "CSVRecords": record }));
    }
    let metadata: Vec<ColumnMetadata> = result
        .fields
        .iter()
        .enumerate()
        .map(|(i, field)| column_metadata_from_pg_field(field, i))
        .collect();
    Ok(serde_json::json!({
        "ColumnMetadata": metadata,
        "Records": records,
        "ResultFormat": "CSV",
        "TotalNumRows": result.rows.len(),
    }))
}

/// Mirrors `csvRecord` (legacy encoding/csv: comma-separated, quote on need,
/// doubled inner quotes, no trailing newline).
pub fn csv_record(row: &[String]) -> Result<String, SqlError> {
    let mut out = String::new();
    for (i, field) in row.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        if csv_field_needs_quoting(field) {
            out.push('"');
            out.push_str(&field.replace('"', "\"\""));
            out.push('"');
        } else {
            out.push_str(field);
        }
    }
    Ok(out)
}

/// legacy encoding/csv quotes a field that contains a comma, quote, CR, LF, or
/// that begins with a space, and quotes a field equal to a lone `﻿` BOM.
fn csv_field_needs_quoting(field: &str) -> bool {
    if field.is_empty() {
        return false;
    }
    if field.contains([',', '"', '\r', '\n']) {
        return true;
    }
    field.starts_with(' ')
}

/// Mirrors `dataAPIField`.
pub fn data_api_field(value: &str, type_oid: i32) -> DataApiResultField {
    match type_oid {
        PG_TYPE_INT4_OID => {
            if let Ok(parsed) = value.parse::<i64>() {
                return DataApiResultField {
                    long_value: Some(parsed),
                    ..DataApiResultField::default()
                };
            }
        }
        PG_TYPE_FLOAT8_OID => {
            if let Ok(parsed) = parse_go_float(value) {
                return DataApiResultField {
                    double_value: Some(parsed),
                    ..DataApiResultField::default()
                };
            }
        }
        PG_TYPE_BOOL_OID => {
            if let Some(parsed) = parse_go_bool(&value.to_lowercase()) {
                return DataApiResultField {
                    boolean_value: Some(parsed),
                    ..DataApiResultField::default()
                };
            }
        }
        _ => {}
    }
    DataApiResultField {
        string_value: Some(value.to_string()),
        ..DataApiResultField::default()
    }
}

/// legacy `strconv.ParseBool` accepts 1,t,T,TRUE,true,True,0,f,F,FALSE,false,False.
fn parse_go_bool(value: &str) -> Option<bool> {
    match value {
        "1" | "t" | "true" => Some(true),
        "0" | "f" | "false" => Some(false),
        _ => None,
    }
}

/// legacy `strconv.ParseFloat` (64-bit). Rust's `f64::from_str` is compatible for
/// the decimal forms devcloud emits.
fn parse_go_float(value: &str) -> Result<f64, ()> {
    value.parse::<f64>().map_err(|_| ())
}

/// Mirrors `columnMetadataFromPGField`.
pub fn column_metadata_from_pg_field(field: &PgField, _ordinal: usize) -> ColumnMetadata {
    let (type_name, precision, signed) = match field.type_oid {
        PG_TYPE_INT4_OID => ("int4", 10, true),
        PG_TYPE_FLOAT8_OID => ("float8", 17, true),
        PG_TYPE_BOOL_OID => ("bool", 1, false),
        _ => ("varchar", 256, false),
    };
    ColumnMetadata {
        name: field.name.clone(),
        label: field.name.clone(),
        type_name: type_name.to_string(),
        is_case_sensitive: false,
        is_signed: signed,
        nullable: 1,
        precision,
        scale: 0,
        ..ColumnMetadata::default()
    }
}

/// Mirrors `columnMetadataFromColumn`.
pub fn column_metadata_from_column(column: &Column, ordinal: usize) -> ColumnMetadata {
    column_metadata_from_pg_field(
        &PgField {
            name: column.name.clone(),
            type_oid: pg_type_oid(&column.data_type),
            type_size: pg_type_size(&column.data_type),
        },
        ordinal,
    )
}

/// Mirrors `approximateResultSize`.
pub fn approximate_result_size(result: &QueryResult) -> i64 {
    let mut size = 0;
    for row in &result.rows {
        for value in row {
            size += value.len() as i64;
        }
    }
    size
}

/// Mirrors `safeStatementQueryString`.
pub fn safe_statement_query_string(sql: &str) -> String {
    let (preview, redacted, _) = safe_sql_preview(sql, sql.len());
    if redacted {
        preview
    } else {
        sql.to_string()
    }
}

// --- pagination ------------------------------------------------------------

/// Mirrors `paginationStart`.
pub fn pagination_start(next_token: &str) -> Result<usize, SqlError> {
    if next_token.is_empty() {
        return Ok(0);
    }
    match next_token.parse::<i64>() {
        Ok(start) if start >= 0 => Ok(start as usize),
        _ => Err(SqlError::new("NextToken is invalid")),
    }
}

/// Mirrors `paginationEnd`.
pub fn pagination_end(start: usize, total: usize, max_results: i64) -> usize {
    if max_results <= 0 || start + (max_results as usize) > total {
        return total;
    }
    start + (max_results as usize)
}

/// Mirrors the `paginate*` family: returns (page, next_token). The page is a
/// borrowed slice of `values`.
pub fn paginate<'a, T>(
    values: &'a [T],
    max_results: i64,
    next_token: &str,
) -> Result<(&'a [T], String), SqlError> {
    let start = pagination_start(next_token)?;
    if start >= values.len() {
        return Ok((&[], String::new()));
    }
    let end = pagination_end(start, values.len(), max_results);
    let next = if end < values.len() {
        end.to_string()
    } else {
        String::new()
    };
    Ok((&values[start..end], next))
}
