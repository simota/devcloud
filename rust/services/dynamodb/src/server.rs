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
use crate::expression::check_condition;
use crate::model::{
    AttributeDefinition, BillingModeSummary, ContinuousBackupsDescription,
    GlobalSecondaryIndexDescription, IndexProjection, Item, KeySchemaElement,
    LocalSecondaryIndexDescription, StreamSpecification, TableDescription,
};
use crate::persistence::{PersistedState, PersistedTable};
use crate::requests::{
    CreateTableRequest, DeleteItemRequest, GetItemRequest, GlobalSecondaryIndexRequest,
    GlobalSecondaryIndexUpdate, IndexProjectionRequest, ListTablesRequest,
    LocalSecondaryIndexRequest, PutItemRequest, UpdateItemRequest, UpdateTableRequest,
};
use crate::responses::{
    encode, DescribeEndpointsResponse, DescribeLimitsResponse, DescribeTableResponse,
    EndpointEntry, ListTablesResponse, TableDescriptionResponse,
};
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

    pub fn load_err(&self) -> Option<&str> {
        self.load_err.as_deref()
    }

    fn now_unix(&self) -> i64 {
        self.fixed_now_unix.unwrap_or_else(time_util::now_unix)
    }

    fn now_millis(&self) -> i64 {
        self.fixed_now_millis.unwrap_or_else(time_util::now_millis)
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
                tags: table.tags,
                continuous_backups: table.continuous_backups,
                resource_policy: table.resource_policy,
                resource_policy_revision: table.resource_policy_revision,
            };
            state.description.item_count = state.items.len() as i64;
            update_index_item_counts(&mut state);
            self.tables.insert(name, state);
        }
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
                    stream_records: Vec::new(),
                    tags: state.tags.clone(),
                    continuous_backups: state.continuous_backups.clone(),
                    resource_policy: state.resource_policy.clone(),
                    resource_policy_revision: state.resource_policy_revision.clone(),
                },
            );
        }
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

        let state = self.tables.get_mut(&request.table_name).unwrap();
        state.items.insert(key.clone(), request.item.clone());
        state.description.item_count = state.items.len() as i64;
        update_index_item_counts(state);
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

        let state = self.tables.get_mut(&request.table_name).unwrap();
        state.items.remove(&key);
        state.description.item_count = state.items.len() as i64;
        update_index_item_counts(state);
        if let Err(err) = self.persist() {
            let state = self.tables.get_mut(&request.table_name).unwrap();
            if let Some(prev) = &old_item {
                state.items.insert(key, prev.clone());
            }
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

        let state = self.tables.get_mut(&request.table_name).unwrap();
        let existed = old_item.is_some();
        state.items.insert(key.clone(), updated.clone());
        state.description.item_count = state.items.len() as i64;
        update_index_item_counts(state);
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
