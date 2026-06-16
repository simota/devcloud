//! Resource model — port of `internal/services/bigquery/types.rs`.
//!
//! Field declaration order matches the legacy structs so serde reproduces legacy
//! struct-field-ordered output. legacy `omitempty` semantics:
//!
//! * strings / ints / bools / slices / maps / pointers tagged `omitempty` are
//!   dropped when zero/empty/nil → `skip_serializing_if`;
//! * **struct-typed fields tagged `omitempty` are NEVER dropped** (legacy
//!   `omitempty` has no effect on struct values) — e.g. `schema`,
//!   `configuration.query/copy/load/extract`, `destinationTable`,
//!   `statistics.query` always serialize, empty or not.
//!
//! `json.RawMessage` maps to [`RawJson`] (`Box<serde_json::value::RawValue>`),
//! which round-trips the original bytes (number literals like `1.50`, nested
//! key order) exactly like legacy.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// legacy `json.RawMessage`: raw bytes of one JSON value, preserved verbatim.
pub type RawJson = Box<serde_json::value::RawValue>;

fn is_false(value: &bool) -> bool {
    !*value
}

fn is_zero(value: &i64) -> bool {
    *value == 0
}

/// legacy `map[string]string` with `omitempty`: nil **and** empty maps are
/// dropped. `None` ≙ legacy nil (absent/null in the request), `Some({})` ≙ a
/// present-but-empty map — distinguished by the patch handlers, identical on
/// the wire.
fn labels_empty(labels: &Option<BTreeMap<String, String>>) -> bool {
    labels.as_ref().map_or(true, BTreeMap::is_empty)
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct ProjectsListResponse {
    pub kind: String,
    pub projects: Vec<ProjectListItem>,
    #[serde(rename = "totalItems")]
    pub total_items: i64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct ProjectListItem {
    pub kind: String,
    pub id: String,
    #[serde(rename = "numericId")]
    pub numeric_id: String,
    #[serde(rename = "projectReference")]
    pub project_ref: ProjectReference,
    #[serde(rename = "friendlyName")]
    pub friendly_name: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ProjectReference {
    #[serde(rename = "projectId", default)]
    pub project_id: String,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct DatasetReference {
    #[serde(rename = "projectId", default)]
    pub project_id: String,
    #[serde(rename = "datasetId", default)]
    pub dataset_id: String,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct TableReference {
    #[serde(rename = "projectId", default)]
    pub project_id: String,
    #[serde(rename = "datasetId", default)]
    pub dataset_id: String,
    #[serde(rename = "tableId", default)]
    pub table_id: String,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct RoutineReference {
    #[serde(rename = "projectId", default)]
    pub project_id: String,
    #[serde(rename = "datasetId", default)]
    pub dataset_id: String,
    #[serde(rename = "routineId", default)]
    pub routine_id: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DatasetResource {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub kind: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub id: String,
    #[serde(rename = "selfLink", default, skip_serializing_if = "String::is_empty")]
    pub self_link: String,
    #[serde(rename = "datasetReference", default)]
    pub dataset_reference: DatasetReference,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub location: String,
    #[serde(
        rename = "friendlyName",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub friendly_name: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub description: String,
    #[serde(default, skip_serializing_if = "labels_empty")]
    pub labels: Option<BTreeMap<String, String>>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub etag: String,
    #[serde(
        rename = "creationTime",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub creation_time: String,
    #[serde(
        rename = "lastModifiedTime",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub last_modified_time: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct DatasetsListResponse {
    pub kind: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub datasets: Vec<DatasetListItem>,
    #[serde(rename = "totalItems", default)]
    pub total_items: i64,
    #[serde(
        rename = "nextPageToken",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub next_page_token: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct DatasetListItem {
    pub kind: String,
    pub id: String,
    #[serde(rename = "datasetReference")]
    pub dataset_reference: DatasetReference,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub location: String,
    #[serde(
        rename = "friendlyName",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub friendly_name: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TableResource {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub kind: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub id: String,
    #[serde(rename = "selfLink", default, skip_serializing_if = "String::is_empty")]
    pub self_link: String,
    #[serde(rename = "tableReference", default)]
    pub table_reference: TableReference,
    #[serde(rename = "type", default, skip_serializing_if = "String::is_empty")]
    pub table_type: String,
    /// legacy `tableSchema` with `omitempty`: structs are never omitted.
    #[serde(default)]
    pub schema: TableSchema,
    #[serde(
        rename = "friendlyName",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub friendly_name: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub description: String,
    #[serde(default, skip_serializing_if = "labels_empty")]
    pub labels: Option<BTreeMap<String, String>>,
    #[serde(
        rename = "timePartitioning",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub time_partitioning: Option<TimePartitioning>,
    #[serde(
        rename = "rangePartitioning",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub range_partitioning: Option<RangePartitioning>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub clustering: Option<Clustering>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub view: Option<ViewDefinition>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub etag: String,
    #[serde(
        rename = "creationTime",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub creation_time: String,
    #[serde(
        rename = "lastModifiedTime",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub last_modified_time: String,
    #[serde(rename = "numRows", default, skip_serializing_if = "String::is_empty")]
    pub num_rows: String,
    #[serde(rename = "numBytes", default, skip_serializing_if = "String::is_empty")]
    pub num_bytes: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub location: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TimePartitioning {
    #[serde(rename = "type", default, skip_serializing_if = "String::is_empty")]
    pub partition_type: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub field: String,
    #[serde(
        rename = "expirationMs",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub expiration_ms: String,
    #[serde(
        rename = "requirePartitionFilter",
        default,
        skip_serializing_if = "is_false"
    )]
    pub require_filter: bool,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RangePartitioning {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub field: String,
    /// Struct-typed `omitempty`: always serialized.
    #[serde(default)]
    pub range: PartitionRange,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PartitionRange {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub start: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub end: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub interval: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Clustering {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub fields: Vec<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ViewDefinition {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub query: String,
    /// No `omitempty` in legacy: always serialized.
    #[serde(rename = "useLegacySql", default)]
    pub use_legacy_sql: bool,
}

/// `PartialEq` mirrors legacy `reflect.DeepEqual` over `tableSchema` in the
/// query destination-table `WRITE_APPEND` check.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct TableSchema {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub fields: Vec<TableFieldSchema>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct TableFieldSchema {
    #[serde(default)]
    pub name: String,
    #[serde(rename = "type", default, skip_serializing_if = "String::is_empty")]
    pub field_type: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub mode: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub description: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub fields: Vec<TableFieldSchema>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct TablesListResponse {
    pub kind: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tables: Vec<TableListItem>,
    #[serde(rename = "totalItems", default)]
    pub total_items: i64,
    #[serde(
        rename = "nextPageToken",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub next_page_token: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct TableListItem {
    pub kind: String,
    pub id: String,
    #[serde(rename = "tableReference")]
    pub table_reference: TableReference,
    #[serde(rename = "type", default, skip_serializing_if = "String::is_empty")]
    pub table_type: String,
    #[serde(
        rename = "friendlyName",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub friendly_name: String,
    #[serde(
        rename = "timePartitioning",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub time_partitioning: Option<TimePartitioning>,
    #[serde(
        rename = "rangePartitioning",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub range_partitioning: Option<RangePartitioning>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub clustering: Option<Clustering>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub view: Option<ViewDefinition>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RoutineResource {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub kind: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub id: String,
    #[serde(rename = "selfLink", default, skip_serializing_if = "String::is_empty")]
    pub self_link: String,
    #[serde(rename = "routineReference", default)]
    pub routine_reference: RoutineReference,
    #[serde(
        rename = "routineType",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub routine_type: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub language: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub arguments: Vec<RoutineArgument>,
    #[serde(
        rename = "returnType",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub return_type: Option<StandardSqlDataType>,
    #[serde(
        rename = "definitionBody",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub definition_body: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub description: String,
    #[serde(
        rename = "determinismLevel",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub determinism_level: String,
    #[serde(
        rename = "importedLibraries",
        default,
        skip_serializing_if = "Vec::is_empty"
    )]
    pub imported_libraries: Vec<String>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub etag: String,
    #[serde(
        rename = "creationTime",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub creation_time: String,
    #[serde(
        rename = "lastModifiedTime",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub last_modified_time: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RoutineArgument {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub name: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub kind: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub mode: String,
    #[serde(rename = "dataType", default, skip_serializing_if = "Option::is_none")]
    pub data_type: Option<StandardSqlDataType>,
    #[serde(
        rename = "argumentKind",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub argument_kind: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct StandardSqlDataType {
    #[serde(rename = "typeKind", default, skip_serializing_if = "String::is_empty")]
    pub type_kind: String,
    #[serde(
        rename = "arrayElementType",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub array_element_type: Option<Box<StandardSqlDataType>>,
    #[serde(
        rename = "structType",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub struct_type: Option<StandardSqlStructType>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct StandardSqlStructType {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub fields: Vec<StandardSqlField>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct StandardSqlField {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub name: String,
    #[serde(rename = "type", default, skip_serializing_if = "Option::is_none")]
    pub field_type: Option<Box<StandardSqlDataType>>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct RoutinesListResponse {
    pub kind: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub routines: Vec<RoutineResource>,
    #[serde(rename = "totalItems", default)]
    pub total_items: i64,
    #[serde(
        rename = "nextPageToken",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub next_page_token: String,
}

#[derive(Debug, Default, Deserialize)]
pub struct InsertAllRequest {
    #[serde(rename = "skipInvalidRows", default)]
    pub skip_invalid_rows: bool,
    #[serde(rename = "ignoreUnknownValues", default)]
    pub ignore_unknown_values: bool,
    #[serde(default)]
    pub rows: Vec<InsertAllRow>,
}

#[derive(Debug, Deserialize)]
pub struct InsertAllRow {
    #[serde(rename = "insertId", default)]
    pub insert_id: String,
    #[serde(default = "empty_raw_map")]
    pub json: BTreeMap<String, RawJson>,
}

fn empty_raw_map() -> BTreeMap<String, RawJson> {
    BTreeMap::new()
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct InsertAllResponse {
    pub kind: String,
    #[serde(
        rename = "insertErrors",
        default,
        skip_serializing_if = "Vec::is_empty"
    )]
    pub insert_errors: Vec<InsertError>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct InsertError {
    pub index: i64,
    pub errors: Vec<InsertErrorItem>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct InsertErrorItem {
    pub reason: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub location: String,
    pub message: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct StoredRow {
    #[serde(rename = "insertId", default, skip_serializing_if = "String::is_empty")]
    pub insert_id: String,
    #[serde(default = "empty_raw_map")]
    pub json: BTreeMap<String, RawJson>,
    #[serde(rename = "insertedAt", default)]
    pub inserted_at: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct TableDataListResponse {
    pub kind: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub etag: String,
    #[serde(rename = "totalRows", default)]
    pub total_rows: String,
    #[serde(
        rename = "pageToken",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub page_token: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub rows: Vec<TableDataRow>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TableDataRow {
    #[serde(default)]
    pub f: Vec<TableCell>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TableCell {
    #[serde(default)]
    pub v: serde_json::Value,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct ServiceAccountResponse {
    pub kind: String,
    pub email: String,
}

#[derive(Debug, Default, Deserialize)]
pub struct QueryRequest {
    #[serde(default)]
    pub query: String,
    #[serde(rename = "useLegacySql", default)]
    pub use_legacy_sql: Option<bool>,
    #[serde(default)]
    pub location: String,
    #[serde(rename = "maxResults", default)]
    pub max_results: i64,
    #[serde(rename = "dryRun", default)]
    pub dry_run: bool,
    #[serde(rename = "queryParameters", default)]
    pub query_parameters: Vec<QueryParameter>,
}

#[derive(Debug, Default, Deserialize)]
pub struct JobInsertRequest {
    #[serde(rename = "jobReference", default)]
    pub job_reference: JobReference,
    #[serde(default)]
    pub configuration: JobConfiguration,
}

#[derive(Debug, Default, Deserialize)]
pub struct SetIamPolicyRequest {
    #[serde(default)]
    pub policy: IamPolicy,
}

#[derive(Debug, Default, Deserialize)]
pub struct TestIamPermissionsRequest {
    #[serde(default)]
    pub permissions: Vec<String>,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct TestIamPermissionsResponse {
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub permissions: Vec<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct IamPolicy {
    #[serde(default, skip_serializing_if = "is_zero")]
    pub version: i64,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub etag: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub bindings: Vec<IamBinding>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct IamBinding {
    #[serde(default)]
    pub role: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub members: Vec<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct JobConfiguration {
    #[serde(rename = "dryRun", default, skip_serializing_if = "is_false")]
    pub dry_run: bool,
    /// Struct-typed `omitempty` fields: always serialized.
    #[serde(default)]
    pub query: QueryJobConfiguration,
    #[serde(default)]
    pub copy: CopyJobConfiguration,
    #[serde(default)]
    pub load: LoadJobConfiguration,
    #[serde(default)]
    pub extract: ExtractJobConfiguration,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct QueryJobConfiguration {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub query: String,
    #[serde(
        rename = "useLegacySql",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub use_legacy_sql: Option<bool>,
    #[serde(
        rename = "queryParameters",
        default,
        skip_serializing_if = "Vec::is_empty"
    )]
    pub query_parameters: Vec<QueryParameter>,
    #[serde(rename = "destinationTable", default)]
    pub destination_table: TableReference,
    #[serde(
        rename = "createDisposition",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub create_disposition: String,
    #[serde(
        rename = "writeDisposition",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub write_disposition: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct QueryParameter {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub name: String,
    #[serde(rename = "parameterType", default)]
    pub parameter_type: QueryParameterType,
    #[serde(rename = "parameterValue", default)]
    pub parameter_value: QueryParameterValue,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct QueryParameterType {
    #[serde(rename = "type", default)]
    pub param_type: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct QueryParameterValue {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub value: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CopyJobConfiguration {
    #[serde(rename = "sourceTable", default)]
    pub source_table: TableReference,
    #[serde(
        rename = "sourceTables",
        default,
        skip_serializing_if = "Vec::is_empty"
    )]
    pub source_tables: Vec<TableReference>,
    #[serde(rename = "destinationTable", default)]
    pub destination_table: TableReference,
    #[serde(
        rename = "createDisposition",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub create_disposition: String,
    #[serde(
        rename = "writeDisposition",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub write_disposition: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct LoadJobConfiguration {
    #[serde(rename = "sourceUris", default, skip_serializing_if = "Vec::is_empty")]
    pub source_uris: Vec<String>,
    #[serde(rename = "destinationTable", default)]
    pub destination_table: TableReference,
    #[serde(default)]
    pub schema: TableSchema,
    #[serde(
        rename = "sourceFormat",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub source_format: String,
    #[serde(rename = "skipLeadingRows", default, skip_serializing_if = "is_zero")]
    pub skip_leading_rows: i64,
    #[serde(
        rename = "createDisposition",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub create_disposition: String,
    #[serde(
        rename = "writeDisposition",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub write_disposition: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ExtractJobConfiguration {
    #[serde(rename = "sourceTable", default)]
    pub source_table: TableReference,
    #[serde(
        rename = "destinationUris",
        default,
        skip_serializing_if = "Vec::is_empty"
    )]
    pub destination_uris: Vec<String>,
    #[serde(
        rename = "destinationFormat",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub destination_format: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct QueryResponse {
    pub kind: String,
    #[serde(default)]
    pub schema: TableSchema,
    #[serde(rename = "jobReference", default)]
    pub job_reference: JobReference,
    #[serde(rename = "totalRows", default)]
    pub total_rows: String,
    #[serde(
        rename = "pageToken",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub page_token: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub rows: Vec<TableDataRow>,
    #[serde(rename = "jobComplete", default)]
    pub job_complete: bool,
    #[serde(rename = "cacheHit", default)]
    pub cache_hit: bool,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct JobsListResponse {
    pub kind: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub jobs: Vec<JobResource>,
    #[serde(
        rename = "nextPageToken",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub next_page_token: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct JobCancelResponse {
    pub kind: String,
    pub job: JobResource,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct JobReference {
    #[serde(rename = "projectId", default)]
    pub project_id: String,
    #[serde(rename = "jobId", default)]
    pub job_id: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub location: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct JobResource {
    #[serde(default)]
    pub kind: String,
    #[serde(default)]
    pub id: String,
    #[serde(rename = "selfLink", default)]
    pub self_link: String,
    #[serde(rename = "jobReference", default)]
    pub job_reference: JobReference,
    /// Struct-typed `omitempty`: always serialized.
    #[serde(default)]
    pub configuration: JobConfiguration,
    #[serde(default)]
    pub status: JobStatus,
    #[serde(default)]
    pub statistics: JobStatistics,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct JobStatus {
    #[serde(default)]
    pub state: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct JobStatistics {
    #[serde(
        rename = "creationTime",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub creation_time: String,
    #[serde(
        rename = "startTime",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub start_time: String,
    #[serde(rename = "endTime", default, skip_serializing_if = "String::is_empty")]
    pub end_time: String,
    /// Struct-typed `omitempty`: always serialized.
    #[serde(default)]
    pub query: QueryStatistics,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct QueryStatistics {
    #[serde(
        rename = "totalRows",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub total_rows: String,
    #[serde(rename = "cacheHit", default)]
    pub cache_hit: bool,
    #[serde(rename = "dryRun", default, skip_serializing_if = "is_false")]
    pub dry_run: bool,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct QueryJobRecord {
    #[serde(default)]
    pub job: JobResource,
    #[serde(default)]
    pub response: QueryResponse,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct ErrorResponse {
    pub error: ErrorBody,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ErrorBody {
    #[serde(default)]
    pub code: i64,
    #[serde(default)]
    pub message: String,
    #[serde(default)]
    pub errors: Vec<ErrorItem>,
    #[serde(default)]
    pub status: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ErrorItem {
    #[serde(default)]
    pub domain: String,
    #[serde(default)]
    pub reason: String,
    #[serde(default)]
    pub message: String,
}
