//! In-memory server state and the table-management operations.
//!
//! Mirrors `internal/services/dynamodb/{server,table_handlers,persistence}.go`.
//! This part lands table lifecycle (Create/Describe/Delete/Update/List) plus the
//! static Describe{Limits,Endpoints} operations and the GSI/LSI/stream
//! description builders. Item operations, expressions, queries, and the HTTP
//! layer arrive in later parts.
//!
//! Each operation returns `Result<serde_json::Value, ApiError>`; the dispatch
//! layer renders the value (struct field order preserved, map keys sorted) and
//! errors to the wire. Table descriptions are assembled as `serde_json::Value`
//! built from the typed `model` structs so the success bodies match Go's
//! `map[string]any{"TableDescription": description}` envelopes exactly.

use std::collections::BTreeMap;

use serde_json::{Map, Value};

use crate::attribute::{
    attribute_values_equal, item_key, project_item, validate_item_attribute_values,
};
use crate::errors::ApiError;
use crate::expression::{check_condition, match_filter, match_key_condition};
use crate::model::{
    AttributeDefinition, BackupDescription, BackupDetails, BackupSummary, BillingModeSummary,
    ContinuousBackupsDescription, GlobalSecondaryIndexDescription, IndexProjection, Item,
    KeySchemaElement, LocalSecondaryIndexDescription, PointInTimeRecoveryDescription,
    SourceTableDetails, StreamSpecification, TableDescription, TimeToLiveDescription,
};
use crate::persistence::{PersistedState, PersistedTable};
use crate::requests::{
    BackupArnRequest, BatchExecuteStatementRequest, BatchGetItemRequest, BatchWriteItemRequest,
    CreateBackupRequest, CreateTableRequest, DeleteItemRequest, DescribeStreamRequest,
    ExecuteStatementRequest, ExecuteTransactionRequest, GetItemRequest, GetRecordsRequest,
    GetShardIteratorRequest, GlobalSecondaryIndexRequest, GlobalSecondaryIndexUpdate,
    IndexProjectionRequest, ListBackupsRequest, ListStreamsRequest, ListTablesRequest,
    LocalSecondaryIndexRequest, PutItemRequest, QueryRequest, RestoreTableFromBackupRequest,
    ScanRequest, TransactGetItemsRequest, TransactWriteItem, TransactWriteItemsRequest,
    UpdateContinuousBackupsRequest, UpdateItemRequest, UpdateTableRequest, UpdateTimeToLiveRequest,
};
use crate::responses::{
    encode, BatchStatementError, BatchStatementResponse, DescribeEndpointsResponse,
    DescribeLimitsResponse, DescribeTableResponse, EndpointEntry, ListTablesResponse,
    SequenceNumberRange, ShardDescription, StreamDescription, StreamSummary,
    TableDescriptionResponse,
};
use crate::streams::{decode_iterator, encode_iterator, StreamIterator};
use crate::validation::{
    validate_attribute_definition_updates, validate_create_table_request,
    validate_stream_specification,
};
use crate::{go_json, time_util};

/// The encoded Go-wire body for a successful operation (field order preserved,
/// map keys sorted, HTML-escaped, trailing newline).
pub type OpResult = Result<Vec<u8>, ApiError>;

const DEFAULT_REGION: &str = "us-east-1";
const ACCOUNT_ID: &str = "000000000000";

/// Configuration mirroring the table-management subset of Go `Config`.
#[derive(Clone, Debug, Default)]
pub struct Config {
    pub addr: String,
    pub region: String,
    pub auth_mode: String,
    pub access_key_id: String,
    pub secret_access_key: String,
    pub storage_path: String,
    pub max_item_bytes: i64,
    pub max_tables: i64,
}

impl Config {
    fn region(&self) -> &str {
        if self.region.is_empty() {
            DEFAULT_REGION
        } else {
            &self.region
        }
    }

    fn max_tables(&self) -> i64 {
        if self.max_tables > 0 {
            self.max_tables
        } else {
            256
        }
    }
}

/// In-memory state for a single table. Mirrors Go `tableState`.
#[derive(Clone, Debug)]
pub struct TableState {
    pub description: TableDescription,
    pub items: BTreeMap<String, Item>,
    pub stream_records: Vec<crate::model::StreamRecord>,
    pub tags: BTreeMap<String, String>,
    pub continuous_backups: Option<ContinuousBackupsDescription>,
    pub resource_policy: String,
    pub resource_policy_revision: String,
}

impl TableState {
    fn new(description: TableDescription) -> Self {
        TableState {
            description,
            items: BTreeMap::new(),
            stream_records: Vec::new(),
            tags: BTreeMap::new(),
            continuous_backups: None,
            resource_policy: String::new(),
            resource_policy_revision: String::new(),
        }
    }
}

pub struct Server {
    config: Config,
    pub(crate) tables: BTreeMap<String, TableState>,
    backups: BTreeMap<String, crate::model::BackupDescription>,
    backup_tables: BTreeMap<String, TableDescription>,
    backup_items: BTreeMap<String, BTreeMap<String, Item>>,
    load_err: Option<String>,
    /// Test hook: when set, used in place of the wall clock for `now_unix` /
    /// stream labels so success bodies are byte-reproducible.
    fixed_now_unix: Option<i64>,
    fixed_now_millis: Option<i64>,
}

impl Server {
    /// Builds a server, loading `state.json` from `config.storage_path` if present.
    pub fn new(config: Config) -> Self {
        let mut server = Server {
            config,
            tables: BTreeMap::new(),
            backups: BTreeMap::new(),
            backup_tables: BTreeMap::new(),
            backup_items: BTreeMap::new(),
            load_err: None,
            fixed_now_unix: None,
            fixed_now_millis: None,
        };
        if !server.config.storage_path.is_empty() {
            if let Err(err) = server.load() {
                server.load_err = Some(err);
            }
        }
        server
    }

    /// Pins the clock for deterministic tests (seconds + matching milliseconds).
    pub fn set_fixed_now(&mut self, unix_secs: i64) {
        self.fixed_now_unix = Some(unix_secs);
        self.fixed_now_millis = Some(unix_secs * 1000);
    }

    /// Pins the millisecond clock independently (for reproducing a stream label
    /// with a non-zero millisecond fraction).
    pub fn set_fixed_now_millis(&mut self, unix_millis: i64) {
        self.fixed_now_millis = Some(unix_millis);
    }

    pub fn load_err(&self) -> Option<&str> {
        self.load_err.as_deref()
    }

    fn now_unix(&self) -> i64 {
        self.fixed_now_unix.unwrap_or_else(time_util::now_unix)
    }

    fn now_millis(&self) -> i64 {
        self.fixed_now_millis.unwrap_or_else(time_util::now_millis)
    }

    /// Appends a stream record for a write to the named table, if the table's
    /// stream is enabled. Mirrors `appendStreamRecordLocked`.
    fn append_stream_record(
        &mut self,
        table: &str,
        event_name: &str,
        old_item: Option<&Item>,
        new_item: Option<&Item>,
    ) {
        let region = self.config.region().to_string();
        let now = self.now_unix();
        let Some(state) = self.tables.get_mut(table) else {
            return;
        };
        if let Some(record) = crate::streams::build_stream_record(
            &state.description,
            &region,
            state.stream_records.len(),
            event_name,
            old_item,
            new_item,
            now,
        ) {
            state.stream_records.push(record);
        }
    }

    fn state_path(&self) -> std::path::PathBuf {
        std::path::Path::new(&self.config.storage_path).join("state.json")
    }

    fn load(&mut self) -> Result<(), String> {
        let path = self.state_path();
        let data = match std::fs::read(&path) {
            Ok(data) => data,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(err) => return Err(err.to_string()),
        };
        let persisted = PersistedState::from_slice(&data).map_err(|e| e.to_string())?;
        for (name, table) in persisted.tables {
            let mut state = TableState {
                description: table.description,
                items: table.items,
                stream_records: table.stream_records,
                tags: table.tags,
                continuous_backups: table.continuous_backups,
                resource_policy: table.resource_policy,
                resource_policy_revision: table.resource_policy_revision,
            };
            state.description.item_count = state.items.len() as i64;
            update_index_item_counts(&mut state);
            self.tables.insert(name, state);
        }
        self.backups = persisted.backups;
        self.backup_tables = persisted.backup_tables;
        self.backup_items = persisted.backup_items;
        Ok(())
    }

    /// Writes `state.json` byte-compatibly with Go's `persistLocked`.
    fn persist(&self) -> Result<(), ApiError> {
        if self.config.storage_path.is_empty() {
            return Ok(());
        }
        let dir = std::path::Path::new(&self.config.storage_path);
        std::fs::create_dir_all(dir)
            .map_err(|_| ApiError::internal("failed to persist dynamodb state"))?;
        let mut persisted = PersistedState::default();
        for (name, state) in &self.tables {
            persisted.tables.insert(
                name.clone(),
                PersistedTable {
                    description: state.description.clone(),
                    items: state.items.clone(),
                    stream_records: state.stream_records.clone(),
                    tags: state.tags.clone(),
                    continuous_backups: state.continuous_backups.clone(),
                    resource_policy: state.resource_policy.clone(),
                    resource_policy_revision: state.resource_policy_revision.clone(),
                },
            );
        }
        persisted.backups = self.backups.clone();
        persisted.backup_tables = self.backup_tables.clone();
        persisted.backup_items = self.backup_items.clone();
        let bytes = go_json::to_vec(&persisted);
        let tmp = self.state_path().with_extension("json.tmp");
        std::fs::write(&tmp, &bytes)
            .map_err(|_| ApiError::internal("failed to persist dynamodb state"))?;
        std::fs::rename(&tmp, self.state_path())
            .map_err(|_| ApiError::internal("failed to persist dynamodb state"))?;
        Ok(())
    }

    fn table_arn(&self, table_name: &str) -> String {
        format!(
            "arn:aws:dynamodb:{}:{ACCOUNT_ID}:table/{table_name}",
            self.config.region()
        )
    }

    // --- operations -------------------------------------------------------

    /// `ListTables`.
    pub fn list_tables(&self, request: &ListTablesRequest) -> OpResult {
        if request.limit < 0 || request.limit > 100 {
            return Err(ApiError::validation("limit must be between 1 and 100"));
        }
        let names: Vec<String> = self.tables.keys().cloned().collect(); // BTreeMap => sorted
        let mut start = 0usize;
        if !request.exclusive_start_table_name.is_empty() {
            match names
                .iter()
                .position(|n| n == &request.exclusive_start_table_name)
            {
                Some(i) => start = i + 1,
                None => {
                    return Err(ApiError::validation(
                        "exclusive start table name does not exist",
                    ))
                }
            }
        }
        if start > names.len() {
            start = names.len();
        }
        let mut end = names.len();
        if request.limit > 0 && start + (request.limit as usize) < end {
            end = start + request.limit as usize;
        }
        let last_evaluated = if end < names.len() {
            Some(names[end - 1].clone())
        } else {
            None
        };
        Ok(encode(&ListTablesResponse {
            table_names: names[start..end].to_vec(),
            last_evaluated_table_name: last_evaluated,
        }))
    }

    /// `CreateTable`.
    pub fn create_table(&mut self, request: &CreateTableRequest) -> OpResult {
        validate_create_table_request(request).map_err(ApiError::validation)?;

        let region = self.config.region().to_string();
        let mut description = TableDescription {
            attribute_definitions: request.attribute_definitions.clone(),
            billing_mode_summary: Some(BillingModeSummary {
                billing_mode: billing_mode(&request.billing_mode),
            }),
            creation_date_time: self.now_unix(),
            global_secondary_indexes: gsi_descriptions(
                &region,
                &request.table_name,
                &request.global_secondary_indexes,
            ),
            item_count: 0,
            key_schema: request.key_schema.clone(),
            latest_stream_arn: String::new(),
            latest_stream_label: String::new(),
            local_secondary_indexes: lsi_descriptions(
                &region,
                &request.table_name,
                &request.local_secondary_indexes,
            ),
            stream_specification: None,
            table_arn: self.table_arn(&request.table_name),
            table_name: request.table_name.clone(),
            table_size_bytes: 0,
            table_status: "ACTIVE".to_string(),
            time_to_live_description: None,
        };
        if request.stream_specification.stream_enabled {
            validate_stream_specification(&request.stream_specification)
                .map_err(ApiError::validation)?;
            self.enable_stream_description(&mut description, &request.stream_specification);
        }

        if self.tables.contains_key(&request.table_name) {
            return Err(ApiError::in_use("table already exists"));
        }
        if self.tables.len() as i64 >= self.config.max_tables() {
            return Err(ApiError::new(
                400,
                "LimitExceededException",
                "table limit exceeded",
            ));
        }
        self.tables.insert(
            request.table_name.clone(),
            TableState::new(description.clone()),
        );
        if let Err(err) = self.persist() {
            self.tables.remove(&request.table_name);
            return Err(err);
        }
        Ok(encode(&TableDescriptionResponse {
            table_description: description,
        }))
    }

    /// `DescribeTable`.
    pub fn describe_table(&self, table_name: &str) -> OpResult {
        if table_name.is_empty() {
            return Err(ApiError::validation("table name is required"));
        }
        match self.tables.get(table_name) {
            Some(state) => Ok(encode(&DescribeTableResponse {
                table: state.description.clone(),
            })),
            None => Err(ApiError::not_found("table not found")),
        }
    }

    /// `DeleteTable`.
    pub fn delete_table(&mut self, table_name: &str) -> OpResult {
        if table_name.is_empty() {
            return Err(ApiError::validation("table name is required"));
        }
        let removed = self.tables.remove(table_name);
        match removed {
            None => Err(ApiError::not_found("table not found")),
            Some(state) => {
                if let Err(err) = self.persist() {
                    self.tables.insert(table_name.to_string(), state);
                    return Err(err);
                }
                Ok(encode(&TableDescriptionResponse {
                    table_description: state.description,
                }))
            }
        }
    }

    /// `UpdateTable`.
    pub fn update_table(&mut self, request: &UpdateTableRequest) -> OpResult {
        if request.table_name.is_empty() {
            return Err(ApiError::validation("table name is required"));
        }
        if !request.billing_mode.is_empty()
            && request.billing_mode != "PAY_PER_REQUEST"
            && request.billing_mode != "PROVISIONED"
        {
            return Err(ApiError::validation(
                "billing mode must be PAY_PER_REQUEST or PROVISIONED",
            ));
        }
        if let Some(spec) = &request.stream_specification {
            validate_stream_specification(spec).map_err(ApiError::validation)?;
        }

        if !self.tables.contains_key(&request.table_name) {
            return Err(ApiError::not_found("table not found"));
        }
        let region = self.config.region().to_string();
        let previous = self.tables[&request.table_name].description.clone();

        // Apply mutations to a working copy, then validate GSI updates before
        // committing — mirroring Go's rollback-on-error behavior.
        let mut description = previous.clone();
        if !request.billing_mode.is_empty() {
            description.billing_mode_summary = Some(BillingModeSummary {
                billing_mode: request.billing_mode.clone(),
            });
        }
        if let Some(spec) = &request.stream_specification {
            if spec.stream_enabled {
                self.enable_stream_description(&mut description, spec);
            } else {
                description.stream_specification = Some(StreamSpecification {
                    stream_enabled: false,
                    stream_view_type: String::new(),
                });
                description.latest_stream_arn = String::new();
                description.latest_stream_label = String::new();
            }
        }
        if !request.global_secondary_index_updates.is_empty() {
            apply_global_secondary_index_updates(
                &mut description,
                &region,
                &request.attribute_definitions,
                &request.global_secondary_index_updates,
            )
            .map_err(ApiError::validation)?;
        }

        let state = self.tables.get_mut(&request.table_name).unwrap();
        state.description = description.clone();
        update_index_item_counts(state);
        let committed = state.description.clone();
        if let Err(err) = self.persist() {
            self.tables
                .get_mut(&request.table_name)
                .unwrap()
                .description = previous;
            return Err(err);
        }
        Ok(encode(&TableDescriptionResponse {
            table_description: committed,
        }))
    }

    /// `DescribeLimits` — static.
    pub fn describe_limits(&self) -> Vec<u8> {
        encode(&DescribeLimitsResponse {
            account_max_read_capacity_units: 80000,
            account_max_write_capacity_units: 80000,
            table_max_read_capacity_units: 40000,
            table_max_write_capacity_units: 40000,
        })
    }

    /// `DescribeEndpoints` — static.
    pub fn describe_endpoints(&self) -> Vec<u8> {
        let address = if self.config.addr.is_empty() {
            "127.0.0.1:8000".to_string()
        } else {
            self.config.addr.clone()
        };
        encode(&DescribeEndpointsResponse {
            endpoints: vec![EndpointEntry {
                address,
                cache_period_in_minutes: 1440,
            }],
        })
    }

    fn max_item_bytes(&self) -> i64 {
        if self.config.max_item_bytes > 0 {
            self.config.max_item_bytes
        } else {
            400_000
        }
    }

    /// Validates attribute values and the encoded item size. Mirrors
    /// `validateItemSize`.
    fn validate_item_size(&self, value: &Item) -> Result<(), String> {
        validate_item_attribute_values(value)?;
        let encoded = crate::go_json::marshal(value);
        if encoded.len() as i64 > self.max_item_bytes() {
            return Err(format!(
                "item size exceeds maximum of {} bytes",
                self.max_item_bytes()
            ));
        }
        Ok(())
    }

    // --- item operations --------------------------------------------------

    /// `PutItem`.
    pub fn put_item(&mut self, request: &PutItemRequest) -> OpResult {
        if request.table_name.is_empty() {
            return Err(ApiError::validation("table name is required"));
        }
        if request.item.is_empty() {
            return Err(ApiError::validation("item is required"));
        }
        self.validate_item_size(&request.item)
            .map_err(ApiError::validation)?;
        let return_values = upper_or_none(&request.return_values);
        if !matches!(return_values.as_str(), "NONE" | "ALL_OLD") {
            return Err(ApiError::validation(
                "return values must be NONE or ALL_OLD",
            ));
        }
        let condition_failure_return =
            upper_or_none(&request.return_values_on_condition_check_failure);
        if !matches!(condition_failure_return.as_str(), "NONE" | "ALL_OLD") {
            return Err(ApiError::validation(
                "return values on condition check failure must be NONE or ALL_OLD",
            ));
        }
        if !valid_return_consumed_capacity(&request.return_consumed_capacity) {
            return Err(ApiError::validation(
                "return consumed capacity must be NONE, TOTAL, or INDEXES",
            ));
        }

        let state = self
            .tables
            .get(&request.table_name)
            .ok_or_else(|| ApiError::not_found("table not found"))?;
        let key = item_key(&state.description, &request.item).map_err(ApiError::validation)?;
        let old_item = state.items.get(&key).cloned();
        check_condition(
            &request.condition_expression,
            &request.expression_attribute_names,
            &request.expression_attribute_values,
            old_item.as_ref(),
        )
        .map_err(|msg| {
            ApiError::condition_check_failed(msg, &condition_failure_return, old_item.as_ref())
        })?;

        let existed = old_item.is_some();
        let stream_len = self.tables[&request.table_name].stream_records.len();
        let state = self.tables.get_mut(&request.table_name).unwrap();
        state.items.insert(key.clone(), request.item.clone());
        state.description.item_count = state.items.len() as i64;
        update_index_item_counts(state);
        self.append_stream_record(
            &request.table_name,
            crate::streams::stream_event_name(existed, false),
            old_item.as_ref(),
            Some(&request.item),
        );
        if let Err(err) = self.persist() {
            let state = self.tables.get_mut(&request.table_name).unwrap();
            match &old_item {
                Some(prev) => {
                    state.items.insert(key, prev.clone());
                }
                None => {
                    state.items.remove(&key);
                }
            }
            state.stream_records.truncate(stream_len);
            state.description.item_count = state.items.len() as i64;
            update_index_item_counts(state);
            return Err(err);
        }

        let mut response = Map::new();
        if return_values == "ALL_OLD" {
            if let Some(prev) = &old_item {
                response.insert(
                    "Attributes".to_string(),
                    serde_json::to_value(prev).unwrap(),
                );
            }
        }
        add_consumed_capacity(
            &mut response,
            &request.table_name,
            &request.return_consumed_capacity,
        );
        Ok(crate::go_json::to_vec(&Value::Object(response)))
    }

    /// `GetItem`.
    pub fn get_item(&self, request: &GetItemRequest) -> OpResult {
        if request.table_name.is_empty() {
            return Err(ApiError::validation("table name is required"));
        }
        if !valid_return_consumed_capacity(&request.return_consumed_capacity) {
            return Err(ApiError::validation(
                "return consumed capacity must be NONE, TOTAL, or INDEXES",
            ));
        }
        let state = self
            .tables
            .get(&request.table_name)
            .ok_or_else(|| ApiError::not_found("table not found"))?;
        let key = item_key(&state.description, &request.key).map_err(ApiError::validation)?;
        let mut response = Map::new();
        if let Some(found) = state.items.get(&key) {
            let projected = project_item(
                found,
                &request.projection_expression,
                &request.expression_attribute_names,
            );
            response.insert(
                "Item".to_string(),
                serde_json::to_value(&projected).unwrap(),
            );
        }
        add_consumed_capacity(
            &mut response,
            &request.table_name,
            &request.return_consumed_capacity,
        );
        Ok(crate::go_json::to_vec(&Value::Object(response)))
    }

    /// `DeleteItem`.
    pub fn delete_item(&mut self, request: &DeleteItemRequest) -> OpResult {
        if request.table_name.is_empty() {
            return Err(ApiError::validation("table name is required"));
        }
        let state = self
            .tables
            .get(&request.table_name)
            .ok_or_else(|| ApiError::not_found("table not found"))?;
        let key = item_key(&state.description, &request.key).map_err(ApiError::validation)?;
        let return_values = upper_or_none(&request.return_values);
        if !matches!(return_values.as_str(), "NONE" | "ALL_OLD") {
            return Err(ApiError::validation(
                "return values must be NONE or ALL_OLD",
            ));
        }
        let condition_failure_return =
            upper_or_none(&request.return_values_on_condition_check_failure);
        if !matches!(condition_failure_return.as_str(), "NONE" | "ALL_OLD") {
            return Err(ApiError::validation(
                "return values on condition check failure must be NONE or ALL_OLD",
            ));
        }
        if !valid_return_consumed_capacity(&request.return_consumed_capacity) {
            return Err(ApiError::validation(
                "return consumed capacity must be NONE, TOTAL, or INDEXES",
            ));
        }
        let old_item = state.items.get(&key).cloned();
        check_condition(
            &request.condition_expression,
            &request.expression_attribute_names,
            &request.expression_attribute_values,
            old_item.as_ref(),
        )
        .map_err(|msg| {
            ApiError::condition_check_failed(msg, &condition_failure_return, old_item.as_ref())
        })?;

        let stream_len = self.tables[&request.table_name].stream_records.len();
        let state = self.tables.get_mut(&request.table_name).unwrap();
        state.items.remove(&key);
        state.description.item_count = state.items.len() as i64;
        update_index_item_counts(state);
        // Append the REMOVE record (no-op if the stream is disabled or the item
        // did not exist).
        self.append_stream_record(&request.table_name, "REMOVE", old_item.as_ref(), None);
        if let Err(err) = self.persist() {
            let state = self.tables.get_mut(&request.table_name).unwrap();
            if let Some(prev) = &old_item {
                state.items.insert(key, prev.clone());
            }
            state.stream_records.truncate(stream_len);
            state.description.item_count = state.items.len() as i64;
            update_index_item_counts(state);
            return Err(err);
        }

        let mut response = Map::new();
        if return_values == "ALL_OLD" {
            if let Some(prev) = &old_item {
                response.insert(
                    "Attributes".to_string(),
                    serde_json::to_value(prev).unwrap(),
                );
            }
        }
        add_consumed_capacity(
            &mut response,
            &request.table_name,
            &request.return_consumed_capacity,
        );
        Ok(crate::go_json::to_vec(&Value::Object(response)))
    }

    /// `UpdateItem`.
    pub fn update_item(&mut self, request: &UpdateItemRequest) -> OpResult {
        if request.table_name.is_empty() {
            return Err(ApiError::validation("table name is required"));
        }
        let state = self
            .tables
            .get(&request.table_name)
            .ok_or_else(|| ApiError::not_found("table not found"))?;
        let key = item_key(&state.description, &request.key).map_err(ApiError::validation)?;
        let return_values = upper_or_none(&request.return_values);
        if !matches!(
            return_values.as_str(),
            "NONE" | "ALL_OLD" | "UPDATED_OLD" | "ALL_NEW" | "UPDATED_NEW"
        ) {
            return Err(ApiError::validation(
                "return values must be NONE, ALL_OLD, UPDATED_OLD, ALL_NEW, or UPDATED_NEW",
            ));
        }
        let condition_failure_return =
            upper_or_none(&request.return_values_on_condition_check_failure);
        if !matches!(condition_failure_return.as_str(), "NONE" | "ALL_OLD") {
            return Err(ApiError::validation(
                "return values on condition check failure must be NONE or ALL_OLD",
            ));
        }
        if !valid_return_consumed_capacity(&request.return_consumed_capacity) {
            return Err(ApiError::validation(
                "return consumed capacity must be NONE, TOTAL, or INDEXES",
            ));
        }

        let old_item = state.items.get(&key).cloned();
        // Working item starts as the existing item, or the key when absent.
        let mut updated = old_item.clone().unwrap_or_else(|| request.key.clone());

        check_condition(
            &request.condition_expression,
            &request.expression_attribute_names,
            &request.expression_attribute_values,
            old_item.as_ref(),
        )
        .map_err(|msg| {
            ApiError::condition_check_failed(msg, &condition_failure_return, old_item.as_ref())
        })?;

        crate::update_expression::apply_update_expression(
            &mut updated,
            &request.update_expression,
            &request.expression_attribute_names,
            &request.expression_attribute_values,
        )
        .map_err(ApiError::validation)?;

        self.validate_item_size(&updated)
            .map_err(ApiError::validation)?;

        let stream_len = self.tables[&request.table_name].stream_records.len();
        let state = self.tables.get_mut(&request.table_name).unwrap();
        let existed = old_item.is_some();
        state.items.insert(key.clone(), updated.clone());
        state.description.item_count = state.items.len() as i64;
        update_index_item_counts(state);
        self.append_stream_record(
            &request.table_name,
            crate::streams::stream_event_name(existed, false),
            old_item.as_ref(),
            Some(&updated),
        );
        if let Err(err) = self.persist() {
            let state = self.tables.get_mut(&request.table_name).unwrap();
            match &old_item {
                Some(prev) => {
                    state.items.insert(key, prev.clone());
                }
                None => {
                    state.items.remove(&key);
                }
            }
            state.stream_records.truncate(stream_len);
            state.description.item_count = state.items.len() as i64;
            update_index_item_counts(state);
            return Err(err);
        }

        let empty = Item::new();
        let old_ref = old_item.as_ref().unwrap_or(&empty);
        let mut response = Map::new();
        match return_values.as_str() {
            "NONE" => {}
            "ALL_NEW" => {
                response.insert(
                    "Attributes".to_string(),
                    serde_json::to_value(&updated).unwrap(),
                );
            }
            "ALL_OLD" => {
                if existed {
                    response.insert(
                        "Attributes".to_string(),
                        serde_json::to_value(old_ref).unwrap(),
                    );
                }
            }
            "UPDATED_NEW" => {
                let attrs = updated_attributes(old_ref, &updated);
                if !attrs.is_empty() {
                    response.insert(
                        "Attributes".to_string(),
                        serde_json::to_value(&attrs).unwrap(),
                    );
                }
            }
            "UPDATED_OLD" => {
                let attrs = updated_old_attributes(old_ref, &updated);
                if !attrs.is_empty() {
                    response.insert(
                        "Attributes".to_string(),
                        serde_json::to_value(&attrs).unwrap(),
                    );
                }
            }
            _ => unreachable!(),
        }
        add_consumed_capacity(
            &mut response,
            &request.table_name,
            &request.return_consumed_capacity,
        );
        Ok(crate::go_json::to_vec(&Value::Object(response)))
    }

    /// `Query`.
    pub fn query(&self, request: &QueryRequest) -> OpResult {
        if request.table_name.is_empty() {
            return Err(ApiError::validation("table name is required"));
        }
        if request.key_condition_expression.trim().is_empty() {
            return Err(ApiError::validation("key condition expression is required"));
        }
        crate::query::validate_select(&request.select, &request.projection_expression)
            .map_err(ApiError::validation)?;
        if !valid_return_consumed_capacity(&request.return_consumed_capacity) {
            return Err(ApiError::validation(
                "return consumed capacity must be NONE, TOTAL, or INDEXES",
            ));
        }
        let state = self
            .tables
            .get(&request.table_name)
            .ok_or_else(|| ApiError::not_found("table not found"))?;
        if !request.index_name.is_empty()
            && !table_has_index(&state.description, &request.index_name)
        {
            return Err(ApiError::validation("index not found"));
        }
        let mut items = crate::query::sorted_items_for_query(
            &state.items,
            &state.description,
            &request.index_name,
        );
        if request.scan_index_forward == Some(false) {
            crate::query::reverse_items(&mut items);
        }
        let start_key =
            crate::query::start_key_string(&state.description, &request.exclusive_start_key)
                .map_err(ApiError::validation)?;
        let names = &request.expression_attribute_names;
        let values = &request.expression_attribute_values;
        let key_expr = &request.key_condition_expression;
        let mut response = crate::query::collect_items(
            &state.description,
            &request.index_name,
            &items,
            request.limit,
            &start_key,
            &request.projection_expression,
            names,
            false,
            |candidate| match_key_condition(key_expr, names, values, candidate),
        )
        .map_err(ApiError::validation)?;
        crate::query::apply_select(&mut response, &request.select);
        add_consumed_capacity(
            &mut response,
            &request.table_name,
            &request.return_consumed_capacity,
        );
        Ok(crate::go_json::to_vec(&Value::Object(response)))
    }

    /// `Scan`.
    pub fn scan(&self, request: &ScanRequest) -> OpResult {
        if request.table_name.is_empty() {
            return Err(ApiError::validation("table name is required"));
        }
        crate::query::validate_select(&request.select, &request.projection_expression)
            .map_err(ApiError::validation)?;
        if !valid_return_consumed_capacity(&request.return_consumed_capacity) {
            return Err(ApiError::validation(
                "return consumed capacity must be NONE, TOTAL, or INDEXES",
            ));
        }
        let state = self
            .tables
            .get(&request.table_name)
            .ok_or_else(|| ApiError::not_found("table not found"))?;
        if !request.index_name.is_empty()
            && !table_has_index(&state.description, &request.index_name)
        {
            return Err(ApiError::validation("index not found"));
        }
        let start_key =
            crate::query::start_key_string(&state.description, &request.exclusive_start_key)
                .map_err(ApiError::validation)?;
        let items = crate::query::sorted_items_for_scan(
            &state.items,
            &state.description,
            &request.index_name,
        );
        let names = &request.expression_attribute_names;
        let values = &request.expression_attribute_values;
        let filter = &request.filter_expression;
        let mut response = crate::query::collect_items(
            &state.description,
            &request.index_name,
            &items,
            request.limit,
            &start_key,
            &request.projection_expression,
            names,
            true,
            |candidate| match_filter(filter, names, values, candidate),
        )
        .map_err(ApiError::validation)?;
        crate::query::apply_select(&mut response, &request.select);
        add_consumed_capacity(
            &mut response,
            &request.table_name,
            &request.return_consumed_capacity,
        );
        Ok(crate::go_json::to_vec(&Value::Object(response)))
    }

    /// `BatchGetItem`.
    pub fn batch_get_item(&self, request: &BatchGetItemRequest) -> OpResult {
        if request.request_items.is_empty() {
            return Err(ApiError::validation("request items are required"));
        }
        if !valid_return_consumed_capacity(&request.return_consumed_capacity) {
            return Err(ApiError::validation(
                "return consumed capacity must be NONE, TOTAL, or INDEXES",
            ));
        }
        let mut responses = Map::new();
        let mut consumed: Vec<Value> = Vec::new();
        for (table_name, table_request) in &request.request_items {
            let state = self
                .tables
                .get(table_name)
                .ok_or_else(|| ApiError::not_found("table not found"))?;
            if table_request.keys.is_empty() {
                return Err(ApiError::validation("keys are required"));
            }
            let mut found_items: Vec<Value> = Vec::new();
            for key_value in &table_request.keys {
                let key = item_key(&state.description, key_value).map_err(ApiError::validation)?;
                if let Some(found) = state.items.get(&key) {
                    let projected = project_item(
                        found,
                        &table_request.projection_expression,
                        &table_request.expression_attribute_names,
                    );
                    found_items.push(serde_json::to_value(&projected).unwrap());
                }
            }
            responses.insert(table_name.clone(), Value::Array(found_items));
            append_batch_consumed_capacity(
                &mut consumed,
                table_name,
                &request.return_consumed_capacity,
            );
        }
        let mut response = Map::new();
        response.insert("Responses".to_string(), Value::Object(responses));
        response.insert("UnprocessedKeys".to_string(), Value::Object(Map::new()));
        if !consumed.is_empty() {
            response.insert("ConsumedCapacity".to_string(), Value::Array(consumed));
        }
        Ok(crate::go_json::to_vec(&Value::Object(response)))
    }

    /// `BatchWriteItem`.
    pub fn batch_write_item(&mut self, request: &BatchWriteItemRequest) -> OpResult {
        if request.request_items.is_empty() {
            return Err(ApiError::validation("request items are required"));
        }
        if !valid_return_consumed_capacity(&request.return_consumed_capacity) {
            return Err(ApiError::validation(
                "return consumed capacity must be NONE, TOTAL, or INDEXES",
            ));
        }
        // Validate everything first, planning (table, key, put-or-delete) writes.
        let mut plan: Vec<PlannedWrite> = Vec::new();
        let mut consumed: Vec<Value> = Vec::new();
        for (table_name, writes) in &request.request_items {
            let state = self
                .tables
                .get(table_name)
                .ok_or_else(|| ApiError::not_found("table not found"))?;
            if writes.is_empty() {
                return Err(ApiError::validation("write requests are required"));
            }
            for write in writes {
                if write.put_request.is_some() == write.delete_request.is_some() {
                    return Err(ApiError::validation(
                        "each write request must contain exactly one operation",
                    ));
                }
                if let Some(put) = &write.put_request {
                    if put.item.is_empty() {
                        return Err(ApiError::validation("put item is required"));
                    }
                    self.validate_item_size(&put.item)
                        .map_err(ApiError::validation)?;
                    let key =
                        item_key(&state.description, &put.item).map_err(ApiError::validation)?;
                    plan.push(PlannedWrite {
                        table: table_name.clone(),
                        key,
                        put: Some(put.item.clone()),
                    });
                }
                if let Some(del) = &write.delete_request {
                    let key =
                        item_key(&state.description, &del.key).map_err(ApiError::validation)?;
                    plan.push(PlannedWrite {
                        table: table_name.clone(),
                        key,
                        put: None,
                    });
                }
            }
            append_batch_consumed_capacity(
                &mut consumed,
                table_name,
                &request.return_consumed_capacity,
            );
        }

        self.apply_planned_writes(&plan)?;

        let mut response = Map::new();
        response.insert("UnprocessedItems".to_string(), Value::Object(Map::new()));
        if !consumed.is_empty() {
            response.insert("ConsumedCapacity".to_string(), Value::Array(consumed));
        }
        Ok(crate::go_json::to_vec(&Value::Object(response)))
    }

    /// `TransactGetItems`.
    pub fn transact_get_items(&self, request: &TransactGetItemsRequest) -> OpResult {
        if request.transact_items.is_empty() {
            return Err(ApiError::validation("transaction items are required"));
        }
        let mut responses: Vec<Value> = Vec::new();
        for transaction_item in &request.transact_items {
            let get = transaction_item.get.as_ref().ok_or_else(|| {
                ApiError::validation("each transaction item must contain a Get operation")
            })?;
            if get.table_name.is_empty() {
                return Err(ApiError::validation("table name is required"));
            }
            let state = self
                .tables
                .get(&get.table_name)
                .ok_or_else(|| ApiError::not_found("table not found"))?;
            let key = item_key(&state.description, &get.key).map_err(ApiError::validation)?;
            match state.items.get(&key) {
                None => responses.push(Value::Object(Map::new())),
                Some(found) => {
                    let projected = project_item(
                        found,
                        &get.projection_expression,
                        &get.expression_attribute_names,
                    );
                    let mut entry = Map::new();
                    entry.insert(
                        "Item".to_string(),
                        serde_json::to_value(&projected).unwrap(),
                    );
                    responses.push(Value::Object(entry));
                }
            }
        }
        let mut response = Map::new();
        response.insert("Responses".to_string(), Value::Array(responses));
        Ok(crate::go_json::to_vec(&Value::Object(response)))
    }

    /// `TransactWriteItems`.
    pub fn transact_write_items(&mut self, request: &TransactWriteItemsRequest) -> OpResult {
        if request.transact_items.is_empty() {
            return Err(ApiError::validation("transaction items are required"));
        }
        let mut plan: Vec<PlannedWrite> = Vec::new();
        for transaction_item in &request.transact_items {
            if count_transact_write_operations(transaction_item) != 1 {
                return Err(ApiError::validation(
                    "each transaction item must contain exactly one operation",
                ));
            }
            if let Some(put) = &transaction_item.put {
                plan.push(self.validate_transact_put(put)?);
            } else if let Some(update) = &transaction_item.update {
                plan.push(self.validate_transact_update(update)?);
            } else if let Some(delete) = &transaction_item.delete {
                plan.push(self.validate_transact_delete(delete)?);
            } else if let Some(check) = &transaction_item.condition_check {
                self.validate_transact_condition_check(check)?;
            }
        }
        self.apply_planned_writes(&plan)?;
        Ok(crate::go_json::to_vec(&Value::Object(Map::new())))
    }

    /// Applies validated writes and persists, rolling back item state on a
    /// persistence failure. Mirrors the apply/persist/restore tail shared by
    /// BatchWriteItem and TransactWriteItems.
    fn apply_planned_writes(&mut self, plan: &[PlannedWrite]) -> Result<(), ApiError> {
        // Back up affected items for rollback.
        let mut backups: Vec<(String, String, Option<Item>)> = Vec::new();
        let mut touched: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
        for write in plan {
            if let Some(state) = self.tables.get(&write.table) {
                backups.push((
                    write.table.clone(),
                    write.key.clone(),
                    state.items.get(&write.key).cloned(),
                ));
            }
        }
        for write in plan {
            let state = self.tables.get_mut(&write.table).unwrap();
            match &write.put {
                Some(item) => {
                    state.items.insert(write.key.clone(), item.clone());
                }
                None => {
                    state.items.remove(&write.key);
                }
            }
            touched.insert(write.table.clone());
        }
        for table in &touched {
            let state = self.tables.get_mut(table).unwrap();
            state.description.item_count = state.items.len() as i64;
            update_index_item_counts(state);
        }
        if let Err(err) = self.persist() {
            for (table, key, prev) in backups.into_iter().rev() {
                if let Some(state) = self.tables.get_mut(&table) {
                    match prev {
                        Some(item) => {
                            state.items.insert(key, item);
                        }
                        None => {
                            state.items.remove(&key);
                        }
                    }
                }
            }
            for table in &touched {
                if let Some(state) = self.tables.get_mut(table) {
                    state.description.item_count = state.items.len() as i64;
                    update_index_item_counts(state);
                }
            }
            return Err(err);
        }
        Ok(())
    }

    fn validate_transact_put(
        &self,
        request: &crate::requests::TransactPut,
    ) -> Result<PlannedWrite, ApiError> {
        if request.table_name.is_empty() {
            return Err(ApiError::validation("table name is required"));
        }
        if request.item.is_empty() {
            return Err(ApiError::validation("item is required"));
        }
        let state = self
            .tables
            .get(&request.table_name)
            .ok_or_else(|| ApiError::not_found("table not found"))?;
        let key = item_key(&state.description, &request.item).map_err(ApiError::validation)?;
        let old_item = state.items.get(&key);
        check_condition(
            &request.condition_expression,
            &request.expression_attribute_names,
            &request.expression_attribute_values,
            old_item,
        )
        .map_err(|_| transaction_cancelled())?;
        self.validate_item_size(&request.item)
            .map_err(ApiError::validation)?;
        Ok(PlannedWrite {
            table: request.table_name.clone(),
            key,
            put: Some(request.item.clone()),
        })
    }

    fn validate_transact_update(
        &self,
        request: &crate::requests::TransactUpdate,
    ) -> Result<PlannedWrite, ApiError> {
        if request.table_name.is_empty() {
            return Err(ApiError::validation("table name is required"));
        }
        let state = self
            .tables
            .get(&request.table_name)
            .ok_or_else(|| ApiError::not_found("table not found"))?;
        let key = item_key(&state.description, &request.key).map_err(ApiError::validation)?;
        let old_item = state.items.get(&key).cloned();
        let mut updated = old_item.clone().unwrap_or_else(|| request.key.clone());
        check_condition(
            &request.condition_expression,
            &request.expression_attribute_names,
            &request.expression_attribute_values,
            old_item.as_ref(),
        )
        .map_err(|_| transaction_cancelled())?;
        crate::update_expression::apply_update_expression(
            &mut updated,
            &request.update_expression,
            &request.expression_attribute_names,
            &request.expression_attribute_values,
        )
        .map_err(ApiError::validation)?;
        self.validate_item_size(&updated)
            .map_err(ApiError::validation)?;
        Ok(PlannedWrite {
            table: request.table_name.clone(),
            key,
            put: Some(updated),
        })
    }

    fn validate_transact_delete(
        &self,
        request: &crate::requests::TransactDelete,
    ) -> Result<PlannedWrite, ApiError> {
        if request.table_name.is_empty() {
            return Err(ApiError::validation("table name is required"));
        }
        let state = self
            .tables
            .get(&request.table_name)
            .ok_or_else(|| ApiError::not_found("table not found"))?;
        let key = item_key(&state.description, &request.key).map_err(ApiError::validation)?;
        let old_item = state.items.get(&key);
        check_condition(
            &request.condition_expression,
            &request.expression_attribute_names,
            &request.expression_attribute_values,
            old_item,
        )
        .map_err(|_| transaction_cancelled())?;
        Ok(PlannedWrite {
            table: request.table_name.clone(),
            key,
            put: None,
        })
    }

    fn validate_transact_condition_check(
        &self,
        request: &crate::requests::TransactConditionCheck,
    ) -> Result<(), ApiError> {
        if request.table_name.is_empty() {
            return Err(ApiError::validation("table name is required"));
        }
        if request.condition_expression.trim().is_empty() {
            return Err(ApiError::validation("condition expression is required"));
        }
        let state = self
            .tables
            .get(&request.table_name)
            .ok_or_else(|| ApiError::not_found("table not found"))?;
        let key = item_key(&state.description, &request.key).map_err(ApiError::validation)?;
        let old_item = state.items.get(&key);
        check_condition(
            &request.condition_expression,
            &request.expression_attribute_names,
            &request.expression_attribute_values,
            old_item,
        )
        .map_err(|_| transaction_cancelled())?;
        Ok(())
    }

    /// `ExecuteStatement` (PartiQL SELECT).
    pub fn execute_statement(&self, request: &ExecuteStatementRequest) -> OpResult {
        let statement = crate::partiql::parse_select(&request.statement, &request.parameters)
            .map_err(ApiError::validation)?;
        let state = self
            .tables
            .get(&statement.table_name)
            .ok_or_else(|| ApiError::not_found("table not found"))?;
        let source = crate::query::sorted_items_for_query(&state.items, &state.description, "");
        let mut items: Vec<Value> = Vec::new();
        for candidate in &source {
            if !crate::partiql::conditions_match(&candidate.value, &statement.conditions) {
                continue;
            }
            let projected = crate::partiql::project_item(&candidate.value, &statement.projections);
            items.push(serde_json::to_value(&projected).unwrap());
            if request.limit > 0 && items.len() as i64 == request.limit {
                break;
            }
        }
        let mut response = Map::new();
        response.insert("Items".to_string(), Value::Array(items));
        add_consumed_capacity(
            &mut response,
            &statement.table_name,
            &request.return_consumed_capacity,
        );
        Ok(crate::go_json::to_vec(&Value::Object(response)))
    }

    /// `BatchExecuteStatement` (per-statement PartiQL SELECT; errors are reported
    /// inline rather than failing the whole batch).
    pub fn batch_execute_statement(&self, request: &BatchExecuteStatementRequest) -> OpResult {
        if request.statements.is_empty() {
            return Err(ApiError::validation("statements are required"));
        }
        if request.statements.len() > 25 {
            return Err(ApiError::validation(
                "statements must contain 25 or fewer entries",
            ));
        }
        let mut responses: Vec<BatchStatementResponse> = Vec::new();
        let mut consumed: Vec<Value> = Vec::new();
        for statement_request in &request.statements {
            let statement = match crate::partiql::parse_select(
                &statement_request.statement,
                &statement_request.parameters,
            ) {
                Ok(s) => s,
                Err(msg) => {
                    responses.push(BatchStatementResponse {
                        error: Some(BatchStatementError {
                            code: "ValidationError".to_string(),
                            message: msg,
                        }),
                        ..Default::default()
                    });
                    continue;
                }
            };
            let Some(state) = self.tables.get(&statement.table_name) else {
                responses.push(BatchStatementResponse {
                    error: Some(BatchStatementError {
                        code: "ResourceNotFound".to_string(),
                        message: "table not found".to_string(),
                    }),
                    table_name: statement.table_name.clone(),
                    ..Default::default()
                });
                append_batch_consumed_capacity(
                    &mut consumed,
                    &statement.table_name,
                    &request.return_consumed_capacity,
                );
                continue;
            };
            if !crate::partiql::conditions_cover_key(&state.description, &statement.conditions) {
                responses.push(BatchStatementResponse {
                    error: Some(BatchStatementError {
                        code: "ValidationError".to_string(),
                        message:
                            "SELECT statement must include equality conditions for all key attributes"
                                .to_string(),
                    }),
                    table_name: statement.table_name.clone(),
                    ..Default::default()
                });
                append_batch_consumed_capacity(
                    &mut consumed,
                    &statement.table_name,
                    &request.return_consumed_capacity,
                );
                continue;
            }
            let mut found: Option<Item> = None;
            for candidate in
                crate::query::sorted_items_for_query(&state.items, &state.description, "")
            {
                if crate::partiql::conditions_match(&candidate.value, &statement.conditions) {
                    found = Some(crate::partiql::project_item(
                        &candidate.value,
                        &statement.projections,
                    ));
                    break;
                }
            }
            responses.push(BatchStatementResponse {
                item: found,
                table_name: statement.table_name.clone(),
                ..Default::default()
            });
            append_batch_consumed_capacity(
                &mut consumed,
                &statement.table_name,
                &request.return_consumed_capacity,
            );
        }
        let mut response = Map::new();
        response.insert(
            "Responses".to_string(),
            serde_json::to_value(&responses).unwrap(),
        );
        if !consumed.is_empty() {
            response.insert("ConsumedCapacity".to_string(), Value::Array(consumed));
        }
        Ok(crate::go_json::to_vec(&Value::Object(response)))
    }

    /// `ExecuteTransaction` (PartiQL SELECT; any failure fails the whole call).
    pub fn execute_transaction(&self, request: &ExecuteTransactionRequest) -> OpResult {
        if request.transact_statements.is_empty() {
            return Err(ApiError::validation("transaction statements are required"));
        }
        if request.transact_statements.len() > 100 {
            return Err(ApiError::validation(
                "transaction statements must contain 100 or fewer entries",
            ));
        }
        let mut responses: Vec<BatchStatementResponse> = Vec::new();
        let mut consumed: Vec<Value> = Vec::new();
        for statement_request in &request.transact_statements {
            let statement = crate::partiql::parse_select(
                &statement_request.statement,
                &statement_request.parameters,
            )
            .map_err(ApiError::validation)?;
            let state = self
                .tables
                .get(&statement.table_name)
                .ok_or_else(|| ApiError::not_found("table not found"))?;
            if !crate::partiql::conditions_cover_key(&state.description, &statement.conditions) {
                return Err(ApiError::validation(
                    "SELECT statement must include equality conditions for all key attributes",
                ));
            }
            let mut found: Option<Item> = None;
            for candidate in
                crate::query::sorted_items_for_query(&state.items, &state.description, "")
            {
                if crate::partiql::conditions_match(&candidate.value, &statement.conditions) {
                    found = Some(crate::partiql::project_item(
                        &candidate.value,
                        &statement.projections,
                    ));
                    break;
                }
            }
            responses.push(BatchStatementResponse {
                item: found,
                table_name: statement.table_name.clone(),
                ..Default::default()
            });
            append_batch_consumed_capacity(
                &mut consumed,
                &statement.table_name,
                &request.return_consumed_capacity,
            );
        }
        let mut response = Map::new();
        response.insert(
            "Responses".to_string(),
            serde_json::to_value(&responses).unwrap(),
        );
        if !consumed.is_empty() {
            response.insert("ConsumedCapacity".to_string(), Value::Array(consumed));
        }
        Ok(crate::go_json::to_vec(&Value::Object(response)))
    }

    /// `ListStreams`.
    pub fn list_streams(&self, request: &ListStreamsRequest) -> OpResult {
        if request.limit < 0 || request.limit > 100 {
            return Err(ApiError::validation("limit must be between 1 and 100"));
        }
        if !request.table_name.is_empty() && !self.tables.contains_key(&request.table_name) {
            return Err(ApiError::not_found("table not found"));
        }
        let mut streams: Vec<StreamSummary> = Vec::new();
        for state in self.tables.values() {
            let d = &state.description;
            if !request.table_name.is_empty() && d.table_name != request.table_name {
                continue;
            }
            if d.latest_stream_arn.is_empty() || !stream_enabled(d) {
                continue;
            }
            streams.push(StreamSummary {
                stream_arn: d.latest_stream_arn.clone(),
                stream_label: d.latest_stream_label.clone(),
                table_name: d.table_name.clone(),
            });
        }
        streams.sort_by(|a, b| {
            if a.table_name == b.table_name {
                a.stream_arn.cmp(&b.stream_arn)
            } else {
                a.table_name.cmp(&b.table_name)
            }
        });
        let mut start = 0usize;
        if !request.exclusive_start_stream_arn.is_empty() {
            match streams
                .iter()
                .position(|s| s.stream_arn == request.exclusive_start_stream_arn)
            {
                Some(i) => start = i + 1,
                None => {
                    return Err(ApiError::validation(
                        "exclusive start stream arn does not exist",
                    ))
                }
            }
        }
        let mut end = streams.len();
        if request.limit > 0 && start + (request.limit as usize) < end {
            end = start + request.limit as usize;
        }
        let mut response = Map::new();
        response.insert(
            "Streams".to_string(),
            serde_json::to_value(&streams[start..end]).unwrap(),
        );
        if end < streams.len() {
            response.insert(
                "LastEvaluatedStreamArn".to_string(),
                Value::String(streams[end - 1].stream_arn.clone()),
            );
        }
        Ok(crate::go_json::to_vec(&Value::Object(response)))
    }

    /// `DescribeStream`.
    pub fn describe_stream(&self, request: &DescribeStreamRequest) -> OpResult {
        if request.stream_arn.is_empty() {
            return Err(ApiError::validation("stream arn is required"));
        }
        if request.limit < 0 || request.limit > 100 {
            return Err(ApiError::validation("limit must be between 1 and 100"));
        }
        if !request.exclusive_start_shard_id.is_empty() {
            return Err(ApiError::validation(
                "exclusive start shard id does not exist",
            ));
        }
        let description = self
            .table_for_stream(&request.stream_arn)
            .map(|s| s.description.clone())
            .ok_or_else(|| ApiError::not_found("stream not found"))?;
        let body = stream_description_for_table(&description);
        let mut response = Map::new();
        response.insert(
            "StreamDescription".to_string(),
            serde_json::to_value(&body).unwrap(),
        );
        Ok(crate::go_json::to_vec(&Value::Object(response)))
    }

    /// `GetShardIterator`.
    pub fn get_shard_iterator(&self, request: &GetShardIteratorRequest) -> OpResult {
        if request.stream_arn.is_empty() {
            return Err(ApiError::validation("stream arn is required"));
        }
        if request.shard_id.is_empty() {
            return Err(ApiError::validation("shard id is required"));
        }
        match request.shard_iterator_type.as_str() {
            "TRIM_HORIZON" | "LATEST" | "AT_SEQUENCE_NUMBER" | "AFTER_SEQUENCE_NUMBER" => {}
            "" => return Err(ApiError::validation("shard iterator type is required")),
            _ => return Err(ApiError::validation("unsupported shard iterator type")),
        }
        let needs_seq = matches!(
            request.shard_iterator_type.as_str(),
            "AT_SEQUENCE_NUMBER" | "AFTER_SEQUENCE_NUMBER"
        );
        if needs_seq && request.sequence_number.is_empty() {
            return Err(ApiError::validation("sequence number is required"));
        }
        if !self.stream_shard_exists(&request.stream_arn, &request.shard_id) {
            return Err(ApiError::not_found("stream shard not found"));
        }
        let mut position = 0i64;
        if request.shard_iterator_type == "LATEST" {
            position = self
                .table_for_stream(&request.stream_arn)
                .map(|s| s.stream_records.len() as i64)
                .unwrap_or(0);
        }
        if needs_seq {
            match self.stream_position_for_sequence(
                &request.stream_arn,
                &request.sequence_number,
                request.shard_iterator_type == "AFTER_SEQUENCE_NUMBER",
            ) {
                Some(p) => position = p,
                None => {
                    return Err(ApiError::new(
                        400,
                        "TrimmedDataAccessException",
                        "sequence number is invalid",
                    ))
                }
            }
        }
        let iterator = encode_iterator(&StreamIterator {
            stream_arn: request.stream_arn.clone(),
            shard_id: request.shard_id.clone(),
            position,
        });
        let mut response = Map::new();
        response.insert("ShardIterator".to_string(), Value::String(iterator));
        Ok(crate::go_json::to_vec(&Value::Object(response)))
    }

    /// `GetRecords`.
    pub fn get_records(&self, request: &GetRecordsRequest) -> OpResult {
        if request.shard_iterator.is_empty() {
            return Err(ApiError::validation("shard iterator is required"));
        }
        if request.limit < 0 || request.limit > 1000 {
            return Err(ApiError::validation("limit must be between 1 and 1000"));
        }
        let iterator = decode_iterator(&request.shard_iterator).map_err(|_| {
            ApiError::new(
                400,
                "TrimmedDataAccessException",
                "shard iterator is invalid",
            )
        })?;
        if !self.stream_shard_exists(&iterator.stream_arn, &iterator.shard_id) {
            return Err(ApiError::not_found("stream shard not found"));
        }
        let records = self.stream_records(&iterator.stream_arn, iterator.position, request.limit);
        let next = encode_iterator(&StreamIterator {
            stream_arn: iterator.stream_arn.clone(),
            shard_id: iterator.shard_id.clone(),
            position: iterator.position + records.len() as i64,
        });
        Ok(encode(&crate::responses::GetRecordsResponse {
            next_shard_iterator: next,
            records,
        }))
    }

    // --- stream helpers ---------------------------------------------------

    fn table_for_stream(&self, stream_arn: &str) -> Option<&TableState> {
        self.tables.values().find(|state| {
            state.description.latest_stream_arn == stream_arn && stream_enabled(&state.description)
        })
    }

    fn stream_shard_exists(&self, stream_arn: &str, shard_id: &str) -> bool {
        // The single shard is always `shardId-000000000000`.
        self.table_for_stream(stream_arn).is_some() && shard_id == "shardId-000000000000"
    }

    fn stream_records(
        &self,
        stream_arn: &str,
        position: i64,
        limit: i64,
    ) -> Vec<crate::model::StreamRecord> {
        let Some(state) = self.table_for_stream(stream_arn) else {
            return Vec::new();
        };
        let len = state.stream_records.len() as i64;
        if position >= len {
            return Vec::new();
        }
        let start = position.max(0) as usize;
        let effective_limit = if limit <= 0 || limit > 1000 {
            1000
        } else {
            limit
        };
        let end = ((start as i64 + effective_limit).min(len)) as usize;
        state.stream_records[start..end].to_vec()
    }

    fn stream_position_for_sequence(
        &self,
        stream_arn: &str,
        sequence_number: &str,
        after: bool,
    ) -> Option<i64> {
        let state = self.table_for_stream(stream_arn)?;
        for (i, record) in state.stream_records.iter().enumerate() {
            if record.dynamodb.sequence_number == sequence_number {
                return Some(if after { i as i64 + 1 } else { i as i64 });
            }
        }
        None
    }

    // --- TTL --------------------------------------------------------------

    /// `DescribeTimeToLive`.
    pub fn describe_time_to_live(&self, table_name: &str) -> OpResult {
        if table_name.is_empty() {
            return Err(ApiError::validation("table name is required"));
        }
        let state = self
            .tables
            .get(table_name)
            .ok_or_else(|| ApiError::not_found("table not found"))?;
        let description = ttl_description(&state.description);
        let mut response = Map::new();
        response.insert(
            "TimeToLiveDescription".to_string(),
            serde_json::to_value(&description).unwrap(),
        );
        Ok(crate::go_json::to_vec(&Value::Object(response)))
    }

    /// `UpdateTimeToLive`.
    pub fn update_time_to_live(&mut self, request: &UpdateTimeToLiveRequest) -> OpResult {
        if request.table_name.is_empty() {
            return Err(ApiError::validation("table name is required"));
        }
        let spec = &request.time_to_live_specification;
        if spec.enabled && spec.attribute_name.is_empty() {
            return Err(ApiError::validation(
                "ttl attribute name is required when ttl is enabled",
            ));
        }
        let state = self
            .tables
            .get_mut(&request.table_name)
            .ok_or_else(|| ApiError::not_found("table not found"))?;
        let previous = state.description.time_to_live_description.clone();
        state.description.time_to_live_description = Some(TimeToLiveDescription {
            attribute_name: spec.attribute_name.clone(),
            time_to_live_status: if spec.enabled { "ENABLED" } else { "DISABLED" }.to_string(),
        });
        if let Err(err) = self.persist() {
            self.tables
                .get_mut(&request.table_name)
                .unwrap()
                .description
                .time_to_live_description = previous;
            return Err(err);
        }
        let mut response = Map::new();
        response.insert(
            "TimeToLiveSpecification".to_string(),
            serde_json::to_value(spec).unwrap(),
        );
        Ok(crate::go_json::to_vec(&Value::Object(response)))
    }

    /// Removes expired TTL items across all tables and persists if anything
    /// changed. Mirrors `expireTTLItems` (called before each operation by the
    /// HTTP layer). `now_unix` is the comparison time.
    pub fn expire_ttl_items(&mut self, now_unix: i64) -> Result<(), ApiError> {
        let mut changed_tables: Vec<(String, Vec<String>)> = Vec::new();
        for (name, state) in &self.tables {
            let ttl = ttl_description(&state.description);
            if ttl.time_to_live_status != "ENABLED" || ttl.attribute_name.is_empty() {
                continue;
            }
            let expired: Vec<String> = state
                .items
                .iter()
                .filter(|(_, item)| ttl_item_expired(item, &ttl.attribute_name, now_unix))
                .map(|(k, _)| k.clone())
                .collect();
            if !expired.is_empty() {
                changed_tables.push((name.clone(), expired));
            }
        }
        if changed_tables.is_empty() {
            return Ok(());
        }
        // Back up for rollback.
        let mut backups: Vec<(String, String, Item)> = Vec::new();
        for (table, keys) in &changed_tables {
            let state = self.tables.get_mut(table).unwrap();
            for key in keys {
                if let Some(item) = state.items.remove(key) {
                    backups.push((table.clone(), key.clone(), item));
                }
            }
            state.description.item_count = state.items.len() as i64;
            update_index_item_counts(state);
        }
        if let Err(err) = self.persist() {
            for (table, key, item) in backups {
                if let Some(state) = self.tables.get_mut(&table) {
                    state.items.insert(key, item);
                }
            }
            for (table, _) in &changed_tables {
                if let Some(state) = self.tables.get_mut(table) {
                    state.description.item_count = state.items.len() as i64;
                    update_index_item_counts(state);
                }
            }
            return Err(err);
        }
        Ok(())
    }

    // --- continuous backups ----------------------------------------------

    /// `DescribeContinuousBackups`.
    pub fn describe_continuous_backups(&self, table_name: &str) -> OpResult {
        if table_name.is_empty() {
            return Err(ApiError::validation("table name is required"));
        }
        let state = self
            .tables
            .get(table_name)
            .ok_or_else(|| ApiError::not_found("table not found"))?;
        let description = continuous_backups_for_state(state.continuous_backups.as_ref());
        let mut response = Map::new();
        response.insert(
            "ContinuousBackupsDescription".to_string(),
            serde_json::to_value(&description).unwrap(),
        );
        Ok(crate::go_json::to_vec(&Value::Object(response)))
    }

    /// `UpdateContinuousBackups`.
    pub fn update_continuous_backups(
        &mut self,
        request: &UpdateContinuousBackupsRequest,
    ) -> OpResult {
        if request.table_name.is_empty() {
            return Err(ApiError::validation("table name is required"));
        }
        let status = if request
            .point_in_time_recovery_specification
            .point_in_time_recovery_enabled
        {
            "ENABLED"
        } else {
            "DISABLED"
        };
        let description = ContinuousBackupsDescription {
            continuous_backups_status: "ENABLED".to_string(),
            point_in_time_recovery_description: PointInTimeRecoveryDescription {
                point_in_time_recovery_status: status.to_string(),
            },
        };
        let state = self
            .tables
            .get_mut(&request.table_name)
            .ok_or_else(|| ApiError::not_found("table not found"))?;
        let previous = state.continuous_backups.clone();
        state.continuous_backups = Some(description.clone());
        if let Err(err) = self.persist() {
            self.tables
                .get_mut(&request.table_name)
                .unwrap()
                .continuous_backups = previous;
            return Err(err);
        }
        let mut response = Map::new();
        response.insert(
            "ContinuousBackupsDescription".to_string(),
            serde_json::to_value(&description).unwrap(),
        );
        Ok(crate::go_json::to_vec(&Value::Object(response)))
    }

    // --- backups ----------------------------------------------------------

    /// `CreateBackup`.
    pub fn create_backup(&mut self, request: &CreateBackupRequest) -> OpResult {
        if request.table_name.is_empty() {
            return Err(ApiError::validation("table name is required"));
        }
        if request.backup_name.is_empty() {
            return Err(ApiError::validation("backup name is required"));
        }
        let state = self
            .tables
            .get(&request.table_name)
            .ok_or_else(|| ApiError::not_found("table not found"))?;
        let created_at = self.now_unix();
        let description =
            backup_description_for_table(&state.description, &request.backup_name, created_at);
        let arn = description.backup_details.backup_arn.clone();
        if self.backups.contains_key(&arn) {
            return Err(ApiError::new(
                400,
                "BackupInUseException",
                "backup already exists",
            ));
        }
        let table_desc = state.description.clone();
        let items = state.items.clone();
        self.backups.insert(arn.clone(), description.clone());
        self.backup_tables.insert(arn.clone(), table_desc);
        self.backup_items.insert(arn.clone(), items);
        if let Err(err) = self.persist() {
            self.backups.remove(&arn);
            self.backup_tables.remove(&arn);
            self.backup_items.remove(&arn);
            return Err(err);
        }
        let mut response = Map::new();
        response.insert(
            "BackupDetails".to_string(),
            serde_json::to_value(&description.backup_details).unwrap(),
        );
        Ok(crate::go_json::to_vec(&Value::Object(response)))
    }

    /// `DescribeBackup`.
    pub fn describe_backup(&self, request: &BackupArnRequest) -> OpResult {
        if request.backup_arn.is_empty() {
            return Err(ApiError::validation("backup arn is required"));
        }
        let description = self
            .backups
            .get(&request.backup_arn)
            .ok_or_else(|| ApiError::new(400, "BackupNotFoundException", "backup not found"))?;
        let mut response = Map::new();
        response.insert(
            "BackupDescription".to_string(),
            serde_json::to_value(description).unwrap(),
        );
        Ok(crate::go_json::to_vec(&Value::Object(response)))
    }

    /// `ListBackups`.
    pub fn list_backups(&self, request: &ListBackupsRequest) -> OpResult {
        if request.limit < 0 || request.limit > 100 {
            return Err(ApiError::validation("limit must be between 1 and 100"));
        }
        let mut summaries: Vec<BackupSummary> = self
            .backups
            .values()
            .filter(|d| {
                request.table_name.is_empty()
                    || d.source_table_details.table_name == request.table_name
            })
            .map(backup_summary_for_description)
            .collect();
        summaries.sort_by(|a, b| {
            if a.table_name == b.table_name {
                a.backup_arn.cmp(&b.backup_arn)
            } else {
                a.table_name.cmp(&b.table_name)
            }
        });
        let mut start = 0usize;
        if !request.exclusive_start_backup_arn.is_empty() {
            match summaries
                .iter()
                .position(|s| s.backup_arn == request.exclusive_start_backup_arn)
            {
                Some(i) => start = i + 1,
                None => {
                    return Err(ApiError::validation(
                        "exclusive start backup arn does not exist",
                    ))
                }
            }
        }
        let mut end = summaries.len();
        if request.limit > 0 && start + (request.limit as usize) < end {
            end = start + request.limit as usize;
        }
        let mut response = Map::new();
        response.insert(
            "BackupSummaries".to_string(),
            serde_json::to_value(&summaries[start..end]).unwrap(),
        );
        if end < summaries.len() {
            response.insert(
                "LastEvaluatedBackupArn".to_string(),
                Value::String(summaries[end - 1].backup_arn.clone()),
            );
        }
        Ok(crate::go_json::to_vec(&Value::Object(response)))
    }

    /// `DeleteBackup`.
    pub fn delete_backup(&mut self, request: &BackupArnRequest) -> OpResult {
        if request.backup_arn.is_empty() {
            return Err(ApiError::validation("backup arn is required"));
        }
        let arn = &request.backup_arn;
        let Some(description) = self.backups.get(arn).cloned() else {
            return Err(ApiError::new(
                400,
                "BackupNotFoundException",
                "backup not found",
            ));
        };
        let table_backup = self.backup_tables.remove(arn);
        let items_backup = self.backup_items.remove(arn);
        self.backups.remove(arn);
        if let Err(err) = self.persist() {
            self.backups.insert(arn.clone(), description.clone());
            if let Some(t) = table_backup {
                self.backup_tables.insert(arn.clone(), t);
            }
            if let Some(it) = items_backup {
                self.backup_items.insert(arn.clone(), it);
            }
            return Err(err);
        }
        let mut response = Map::new();
        response.insert(
            "BackupDescription".to_string(),
            serde_json::to_value(&description).unwrap(),
        );
        Ok(crate::go_json::to_vec(&Value::Object(response)))
    }

    /// `RestoreTableFromBackup`.
    pub fn restore_table_from_backup(
        &mut self,
        request: &RestoreTableFromBackupRequest,
    ) -> OpResult {
        if request.backup_arn.is_empty() {
            return Err(ApiError::validation("backup arn is required"));
        }
        if request.target_table_name.is_empty() {
            return Err(ApiError::validation("target table name is required"));
        }
        let Some(backup) = self.backups.get(&request.backup_arn).cloned() else {
            return Err(ApiError::new(
                400,
                "BackupNotFoundException",
                "backup not found",
            ));
        };
        if self.tables.contains_key(&request.target_table_name) {
            return Err(ApiError::in_use("table already exists"));
        }
        if self.tables.len() as i64 >= self.config.max_tables() {
            return Err(ApiError::new(
                400,
                "LimitExceededException",
                "table limit exceeded",
            ));
        }
        let created_at = self.now_unix();
        let description =
            self.restored_table_description(&request.target_table_name, &backup, created_at);
        let items = self
            .backup_items
            .get(&request.backup_arn)
            .cloned()
            .unwrap_or_default();
        let mut state = TableState::new(description);
        state.items = items;
        state.description.item_count = state.items.len() as i64;
        update_index_item_counts(&mut state);
        let committed = state.description.clone();
        self.tables.insert(request.target_table_name.clone(), state);
        if let Err(err) = self.persist() {
            self.tables.remove(&request.target_table_name);
            return Err(err);
        }
        Ok(encode(&TableDescriptionResponse {
            table_description: committed,
        }))
    }

    fn restored_table_description(
        &self,
        target_table_name: &str,
        backup: &BackupDescription,
        created_at: i64,
    ) -> TableDescription {
        let region = self.config.region().to_string();
        let mut description = match self.backup_tables.get(&backup.backup_details.backup_arn) {
            Some(desc) => desc.clone(),
            None => TableDescription {
                attribute_definitions: backup.source_table_details.attribute_definitions.clone(),
                billing_mode_summary: Some(BillingModeSummary {
                    billing_mode: if backup.source_table_details.billing_mode.is_empty() {
                        "PAY_PER_REQUEST".to_string()
                    } else {
                        backup.source_table_details.billing_mode.clone()
                    },
                }),
                creation_date_time: 0,
                global_secondary_indexes: Vec::new(),
                item_count: 0,
                key_schema: backup.source_table_details.key_schema.clone(),
                latest_stream_arn: String::new(),
                latest_stream_label: String::new(),
                local_secondary_indexes: Vec::new(),
                stream_specification: None,
                table_arn: String::new(),
                table_name: String::new(),
                table_size_bytes: 0,
                table_status: String::new(),
                time_to_live_description: None,
            },
        };
        let new_arn = format!("arn:aws:dynamodb:{region}:{ACCOUNT_ID}:table/{target_table_name}");
        description.creation_date_time = created_at;
        description.item_count = self
            .backup_items
            .get(&backup.backup_details.backup_arn)
            .map(|i| i.len() as i64)
            .unwrap_or(0);
        description.latest_stream_arn = String::new();
        description.latest_stream_label = String::new();
        description.stream_specification = None;
        description.table_arn = new_arn.clone();
        description.table_name = target_table_name.to_string();
        description.table_size_bytes = backup.backup_details.backup_size_bytes;
        description.table_status = "ACTIVE".to_string();
        for gsi in &mut description.global_secondary_indexes {
            gsi.index_arn = format!("{new_arn}/index/{}", gsi.index_name);
        }
        for lsi in &mut description.local_secondary_indexes {
            lsi.index_arn = format!("{new_arn}/index/{}", lsi.index_name);
        }
        description
    }

    fn enable_stream_description(
        &self,
        description: &mut TableDescription,
        spec: &StreamSpecification,
    ) {
        let mut label = description.latest_stream_label.clone();
        if label.is_empty() {
            label = time_util::stream_label(self.now_millis());
        }
        description.latest_stream_label = label.clone();
        // Go: `description.TableArn + "/stream/" + label`, with a fallback for an
        // empty TableArn. Our TableArn is always populated, but reproduce the
        // fallback path for exactness.
        let mut arn = format!("{}/stream/{label}", description.table_arn);
        if arn == format!("/stream/{label}") {
            arn = format!(
                "arn:aws:dynamodb:{}:{ACCOUNT_ID}:table/{}/stream/{label}",
                self.config.region(),
                description.table_name
            );
        }
        description.latest_stream_arn = arn;
        description.stream_specification = Some(StreamSpecification {
            stream_enabled: true,
            stream_view_type: spec.stream_view_type.clone(),
        });
    }
}

fn billing_mode(value: &str) -> String {
    if value.is_empty() {
        "PAY_PER_REQUEST".to_string()
    } else {
        value.to_string()
    }
}

/// `strings.ToUpper(defaultString(value, "NONE"))` — empty becomes "NONE".
fn upper_or_none(value: &str) -> String {
    if value.is_empty() {
        "NONE".to_string()
    } else {
        value.to_uppercase()
    }
}

/// Mirrors `validReturnConsumedCapacity` (empty allowed; case-insensitive).
fn valid_return_consumed_capacity(value: &str) -> bool {
    matches!(upper_or_none(value).as_str(), "NONE" | "TOTAL" | "INDEXES")
}

/// Attributes that are new or changed from old to new, mirroring
/// `updatedAttributes` (used by `ReturnValues=UPDATED_NEW`).
fn updated_attributes(old: &Item, new: &Item) -> Item {
    let mut result = Item::new();
    for (name, new_attr) in new {
        match old.get(name) {
            Some(old_attr) if attribute_values_equal(old_attr, new_attr) => {}
            _ => {
                result.insert(name.clone(), new_attr.clone());
            }
        }
    }
    result
}

/// Old values of attributes that were changed or removed, mirroring
/// `updatedOldAttributes` (used by `ReturnValues=UPDATED_OLD`).
fn updated_old_attributes(old: &Item, new: &Item) -> Item {
    let mut result = Item::new();
    for (name, old_attr) in old {
        match new.get(name) {
            Some(new_attr) if attribute_values_equal(old_attr, new_attr) => {}
            _ => {
                result.insert(name.clone(), old_attr.clone());
            }
        }
    }
    result
}

/// Adds the `ConsumedCapacity` field unless the mode is empty/NONE. Mirrors
/// `addConsumedCapacity`: `CapacityUnits` is the JSON integer `1` (Go writes
/// `float64(1)`, which marshals as `1`).
fn add_consumed_capacity(response: &mut Map<String, Value>, table_name: &str, mode: &str) {
    if mode.is_empty() || mode.eq_ignore_ascii_case("NONE") {
        return;
    }
    let mut cap = Map::new();
    cap.insert(
        "TableName".to_string(),
        Value::String(table_name.to_string()),
    );
    cap.insert("CapacityUnits".to_string(), Value::from(1));
    response.insert("ConsumedCapacity".to_string(), Value::Object(cap));
}

/// Appends a per-table `ConsumedCapacity` entry unless mode is empty/NONE.
/// Mirrors `appendBatchConsumedCapacity`.
fn append_batch_consumed_capacity(values: &mut Vec<Value>, table_name: &str, mode: &str) {
    if mode.is_empty() || mode.eq_ignore_ascii_case("NONE") {
        return;
    }
    let mut cap = Map::new();
    cap.insert(
        "TableName".to_string(),
        Value::String(table_name.to_string()),
    );
    cap.insert("CapacityUnits".to_string(), Value::from(1));
    values.push(Value::Object(cap));
}

/// A planned write for batch/transact apply: a target table, internal key, and
/// either the item to put (`Some`) or a delete (`None`).
struct PlannedWrite {
    table: String,
    key: String,
    put: Option<Item>,
}

/// Counts the set operations on a transact-write item, mirroring
/// `countTransactWriteOperations`.
fn count_transact_write_operations(item: &TransactWriteItem) -> usize {
    item.put.is_some() as usize
        + item.update.is_some() as usize
        + item.delete.is_some() as usize
        + item.condition_check.is_some() as usize
}

/// The cancelled-transaction error Go returns for any failed condition inside a
/// transact-write (`TransactionCanceledException` / "transaction cancelled").
fn transaction_cancelled() -> ApiError {
    ApiError::new(400, "TransactionCanceledException", "transaction cancelled")
}

/// The TTL description for a table (DISABLED when unset). Mirrors `ttlDescription`.
fn ttl_description(description: &TableDescription) -> TimeToLiveDescription {
    match &description.time_to_live_description {
        Some(d) => d.clone(),
        None => TimeToLiveDescription {
            attribute_name: String::new(),
            time_to_live_status: "DISABLED".to_string(),
        },
    }
}

/// True when an item's TTL attribute (a UNIX-seconds number) is at or before
/// `now_unix`. Mirrors `ttlItemExpired`.
fn ttl_item_expired(value: &Item, attribute_name: &str, now_unix: i64) -> bool {
    let Some(seconds) = value
        .get(attribute_name)
        .and_then(|a| a.as_object())
        .and_then(|o| o.get("N"))
        .and_then(Value::as_str)
    else {
        return false;
    };
    if !crate::number::is_valid_number(seconds) {
        return false;
    }
    // Expired when the stored expiry is <= now.
    crate::number::compare_number_strings(seconds, &now_unix.to_string())
        != std::cmp::Ordering::Greater
}

/// The default/active continuous-backups description for a table. Mirrors
/// `continuousBackupsDescriptionForState`.
fn continuous_backups_for_state(
    value: Option<&ContinuousBackupsDescription>,
) -> ContinuousBackupsDescription {
    match value {
        Some(d) => d.clone(),
        None => ContinuousBackupsDescription {
            continuous_backups_status: "ENABLED".to_string(),
            point_in_time_recovery_description: PointInTimeRecoveryDescription {
                point_in_time_recovery_status: "DISABLED".to_string(),
            },
        },
    }
}

/// Builds a backup description for a table snapshot. Mirrors
/// `backupDescriptionForTable`.
fn backup_description_for_table(
    description: &TableDescription,
    backup_name: &str,
    created_at: i64,
) -> BackupDescription {
    let backup_arn = format!(
        "{}/backup/{created_at}-{backup_name}",
        description.table_arn
    );
    let billing_mode = description
        .billing_mode_summary
        .as_ref()
        .map(|b| b.billing_mode.clone())
        .unwrap_or_else(|| "PAY_PER_REQUEST".to_string());
    BackupDescription {
        backup_details: BackupDetails {
            backup_arn,
            backup_creation_date_time: created_at,
            backup_name: backup_name.to_string(),
            backup_size_bytes: description.table_size_bytes,
            backup_status: "AVAILABLE".to_string(),
            backup_type: "USER".to_string(),
        },
        source_table_details: SourceTableDetails {
            attribute_definitions: description.attribute_definitions.clone(),
            billing_mode,
            item_count: description.item_count,
            key_schema: description.key_schema.clone(),
            table_arn: description.table_arn.clone(),
            table_creation_date_time: description.creation_date_time,
            table_id: description.table_arn.clone(),
            table_name: description.table_name.clone(),
            table_size_bytes: description.table_size_bytes,
        },
    }
}

/// Builds a `ListBackups` summary, mirroring `backupSummaryForDescription`.
fn backup_summary_for_description(description: &BackupDescription) -> BackupSummary {
    BackupSummary {
        backup_arn: description.backup_details.backup_arn.clone(),
        backup_creation_date_time: description.backup_details.backup_creation_date_time,
        backup_name: description.backup_details.backup_name.clone(),
        backup_size_bytes: description.backup_details.backup_size_bytes,
        backup_status: description.backup_details.backup_status.clone(),
        backup_type: description.backup_details.backup_type.clone(),
        table_arn: description.source_table_details.table_arn.clone(),
        table_name: description.source_table_details.table_name.clone(),
    }
}

/// True when a table has an enabled stream.
fn stream_enabled(description: &TableDescription) -> bool {
    description
        .stream_specification
        .as_ref()
        .map(|s| s.stream_enabled)
        .unwrap_or(false)
}

/// Builds the `StreamDescription` for a table, mirroring
/// `streamDescriptionForTable` (one fixed shard `shardId-000000000000`).
fn stream_description_for_table(description: &TableDescription) -> StreamDescription {
    let stream_view_type = description
        .stream_specification
        .as_ref()
        .map(|s| s.stream_view_type.clone())
        .unwrap_or_default();
    StreamDescription {
        creation_request_date_time: description.creation_date_time,
        key_schema: description.key_schema.clone(),
        last_evaluated_shard_id: String::new(),
        shards: vec![ShardDescription {
            sequence_number_range: SequenceNumberRange {
                ending_sequence_number: String::new(),
                starting_sequence_number: "0".to_string(),
            },
            shard_id: "shardId-000000000000".to_string(),
        }],
        stream_arn: description.latest_stream_arn.clone(),
        stream_label: description.latest_stream_label.clone(),
        stream_status: "ENABLED".to_string(),
        stream_view_type,
        table_name: description.table_name.clone(),
    }
}

fn projection_from_request(req: &IndexProjectionRequest) -> IndexProjection {
    let mut projection_type = req.projection_type.clone();
    if projection_type.is_empty() {
        projection_type = "ALL".to_string();
    }
    IndexProjection {
        projection_type,
        non_key_attributes: req.non_key_attributes.clone(),
    }
}

fn gsi_descriptions(
    region: &str,
    table_name: &str,
    indexes: &[GlobalSecondaryIndexRequest],
) -> Vec<GlobalSecondaryIndexDescription> {
    indexes
        .iter()
        .map(|index| GlobalSecondaryIndexDescription {
            index_arn: format!(
                "arn:aws:dynamodb:{region}:{ACCOUNT_ID}:table/{table_name}/index/{}",
                index.index_name
            ),
            index_name: index.index_name.clone(),
            index_size_bytes: 0,
            index_status: "ACTIVE".to_string(),
            item_count: 0,
            key_schema: index.key_schema.clone(),
            projection: projection_from_request(&index.projection),
            provisioned_throughput: BTreeMap::new(),
        })
        .collect()
}

fn lsi_descriptions(
    region: &str,
    table_name: &str,
    indexes: &[LocalSecondaryIndexRequest],
) -> Vec<LocalSecondaryIndexDescription> {
    indexes
        .iter()
        .map(|index| LocalSecondaryIndexDescription {
            index_arn: format!(
                "arn:aws:dynamodb:{region}:{ACCOUNT_ID}:table/{table_name}/index/{}",
                index.index_name
            ),
            index_name: index.index_name.clone(),
            index_size_bytes: 0,
            item_count: 0,
            key_schema: index.key_schema.clone(),
            projection: projection_from_request(&index.projection),
        })
        .collect()
}

fn table_has_index(description: &TableDescription, index_name: &str) -> bool {
    description
        .global_secondary_indexes
        .iter()
        .any(|i| i.index_name == index_name)
        || description
            .local_secondary_indexes
            .iter()
            .any(|i| i.index_name == index_name)
}

fn attribute_definition_set(
    existing: &[AttributeDefinition],
    updates: &[AttributeDefinition],
) -> std::collections::BTreeSet<String> {
    let mut set = std::collections::BTreeSet::new();
    for definition in existing {
        set.insert(definition.attribute_name.clone());
    }
    for definition in updates {
        if !definition.attribute_name.is_empty() {
            set.insert(definition.attribute_name.clone());
        }
    }
    set
}

fn merge_attribute_definitions(
    existing: &[AttributeDefinition],
    updates: &[AttributeDefinition],
) -> Vec<AttributeDefinition> {
    let mut merged = existing.to_vec();
    let mut seen: std::collections::BTreeSet<String> =
        existing.iter().map(|d| d.attribute_name.clone()).collect();
    for definition in updates {
        if definition.attribute_name.is_empty() || seen.contains(&definition.attribute_name) {
            continue;
        }
        merged.push(definition.clone());
        seen.insert(definition.attribute_name.clone());
    }
    merged
}

fn validate_global_secondary_index_create(
    index: &GlobalSecondaryIndexRequest,
    attributes: &std::collections::BTreeSet<String>,
    description: &TableDescription,
) -> Result<(), String> {
    if index.index_name.is_empty() {
        return Err("global secondary index name is required".to_string());
    }
    if table_has_index(description, &index.index_name) {
        return Err("secondary index name already exists".to_string());
    }
    if index.key_schema.is_empty() {
        return Err("global secondary index key schema is required".to_string());
    }
    let mut hash_keys = 0;
    let mut range_keys = 0;
    for element in &index.key_schema {
        if !attributes.contains(element.attribute_name.as_str()) {
            return Err("global secondary index key schema attributes must be defined".to_string());
        }
        match element.key_type.as_str() {
            "HASH" => hash_keys += 1,
            "RANGE" => range_keys += 1,
            _ => return Err("global secondary index key type must be HASH or RANGE".to_string()),
        }
    }
    if hash_keys != 1 || range_keys > 1 || index.key_schema.len() > 2 {
        return Err(
            "global secondary index key schema must include one HASH key and at most one RANGE key"
                .to_string(),
        );
    }
    if !matches!(
        index.projection.projection_type.as_str(),
        "" | "ALL" | "KEYS_ONLY" | "INCLUDE"
    ) {
        return Err(
            "global secondary index projection type must be ALL, KEYS_ONLY, or INCLUDE".to_string(),
        );
    }
    Ok(())
}

fn apply_global_secondary_index_updates(
    description: &mut TableDescription,
    region: &str,
    definitions: &[AttributeDefinition],
    updates: &[GlobalSecondaryIndexUpdate],
) -> Result<(), String> {
    validate_attribute_definition_updates(&description.attribute_definitions, definitions)?;
    let attributes = attribute_definition_set(&description.attribute_definitions, definitions);
    for update in updates {
        let actions = update.create.is_some() as u8
            + update.delete.is_some() as u8
            + update.update.is_some() as u8;
        if actions != 1 {
            return Err(
                "each global secondary index update must contain exactly one action".to_string(),
            );
        }
        if update.update.is_some() {
            return Err("global secondary index throughput updates are not supported".to_string());
        }
        if let Some(create) = &update.create {
            validate_global_secondary_index_create(create, &attributes, description)?;
            let merged =
                merge_attribute_definitions(&description.attribute_definitions, definitions);
            description.attribute_definitions = merged;
            let mut created = gsi_descriptions(
                region,
                &description.table_name,
                std::slice::from_ref(create),
            );
            description.global_secondary_indexes.append(&mut created);
            continue;
        }
        if let Some(delete) = &update.delete {
            match description
                .global_secondary_indexes
                .iter()
                .position(|i| i.index_name == delete.index_name)
            {
                Some(i) => {
                    description.global_secondary_indexes.remove(i);
                }
                None => return Err("global secondary index does not exist".to_string()),
            }
        }
    }
    Ok(())
}

fn item_has_all_keys(value: &Item, schema: &[KeySchemaElement]) -> bool {
    schema
        .iter()
        .all(|element| value.contains_key(&element.attribute_name))
}

/// Recomputes per-index `ItemCount` from the table's items. Mirrors
/// `updateIndexItemCounts`.
pub(crate) fn update_index_item_counts(state: &mut TableState) {
    let items: Vec<&Item> = state.items.values().collect();
    for index in &mut state.description.global_secondary_indexes {
        index.item_count = items
            .iter()
            .filter(|item| item_has_all_keys(item, &index.key_schema))
            .count() as i64;
    }
    for index in &mut state.description.local_secondary_indexes {
        index.item_count = items
            .iter()
            .filter(|item| item_has_all_keys(item, &index.key_schema))
            .count() as i64;
    }
}
