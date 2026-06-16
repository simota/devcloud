//! HTTP control plane + Data API dispatch (`ServeHTTP` parity).
//!
//! Parity: `internal/services/redshift/server.rs` (`routes`/`handleAPI`/
//! `handleHealth`/`handleServerlessTarget`/`handleDataAPITarget`),
//! `cluster_handlers.rs`, `dataapi_handlers.rs`, and `serverless_handlers.rs`.
//! The legacy control plane speaks the AWS Query protocol (form-encoded request,
//! `text/xml` response); the Data API and serverless control planes speak AWS
//! JSON 1.1 dispatched by `X-Amz-Target`. This module exposes a synchronous
//! `Server::dispatch_http` that the daemon HTTP seam (part 5) and the parity
//! tests drive directly, exactly as legacy tests drive `ServeHTTP`.

use std::collections::BTreeMap;
use std::time::SystemTime;

use serde::{Deserialize, Serialize};

use crate::cluster::{
    cluster_snapshot_from_config, cluster_snapshot_from_snapshot_metadata,
    cluster_snapshot_metadata_from_cluster, default_cluster_identifier, delete_tags, merge_tags,
    ClusterEndpoint, ClusterSnapshot, ClusterSnapshotMetadata, Tag,
};
use crate::dataapi::{
    self, column_metadata_from_column, describe_statement_response_from_statement,
    execute_statement_response_from_statement, get_statement_result_response,
    get_statement_result_v2_response, safe_statement_query_string, BatchExecuteStatementRequest,
    ColumnMetadata, DescribeTableRequest, ExecuteStatementRequest, GetStatementResultRequest,
    ListMetadataRequest, ListStatementsRequest, ServerlessListRequest, ServerlessNamespaceRequest,
    ServerlessWorkgroupRequest, StatementIdRequest, StatementListItem, TableMember,
};
use crate::model::{lookup_table, QualifiedName};
use crate::server::{compact_sql_statements, default_str, Server, StatementRecord};

const XMLNS: &str = "http://redshift.amazonaws.com/doc/2012-12-01/";
const REQUEST_ID: &str = "devcloud-redshift";
const DEFAULT_PARAMETER_GROUP_NAME: &str = "default.redshift-1.0";

#[derive(Deserialize)]
struct ControlQueryRequest {
    sql: String,
    #[serde(rename = "maxRows")]
    max_rows: Option<usize>,
}

/// An already-rendered HTTP response.
pub struct HttpResponse {
    pub status: u16,
    pub content_type: String,
    pub body: String,
}

impl HttpResponse {
    fn json(status: u16, value: &serde_json::Value) -> HttpResponse {
        HttpResponse {
            status,
            content_type: "application/json".to_string(),
            body: serde_json::to_string(value).unwrap_or_default(),
        }
    }

    fn data_api_value(status: u16, value: serde_json::Value) -> HttpResponse {
        HttpResponse {
            status,
            content_type: "application/x-amz-json-1.1".to_string(),
            body: serde_json::to_string(&value).unwrap_or_default(),
        }
    }

    fn data_api<T: Serialize>(status: u16, value: &T) -> HttpResponse {
        HttpResponse::data_api_value(
            status,
            serde_json::to_value(value).unwrap_or(serde_json::Value::Null),
        )
    }

    fn xml(body: String) -> HttpResponse {
        HttpResponse {
            status: 200,
            content_type: "text/xml; charset=utf-8".to_string(),
            body,
        }
    }
}

fn json_error(status: u16, code: &str, message: &str) -> HttpResponse {
    HttpResponse::json(
        status,
        &serde_json::json!({ "__type": code, "message": message }),
    )
}

fn data_api_error(status: u16, code: &str, message: &str) -> HttpResponse {
    HttpResponse::data_api_value(
        status,
        serde_json::json!({ "__type": code, "message": message }),
    )
}

fn method_not_allowed() -> HttpResponse {
    json_error(405, "MethodNotAllowed", "method not allowed")
}

impl Server {
    /// Mirrors `ServeHTTP` → `routes` → handlers. `headers` keys are lowercased.
    pub fn dispatch_http(
        &self,
        method: &str,
        path: &str,
        query: &str,
        headers: &BTreeMap<String, String>,
        body: &[u8],
    ) -> HttpResponse {
        if path.starts_with("/_introspect/") {
            return self.handle_introspect(method, path, query);
        }
        if path.starts_with("/_control/") {
            return self.handle_control(method, path, headers, body);
        }

        match path {
            "/health" | "/ready" => self.handle_health(method),
            "/" => self.handle_api(method, query, headers, body),
            _ => json_error(404, "NotFound", "not found"),
        }
    }

    fn handle_health(&self, method: &str) -> HttpResponse {
        if method != "GET" && method != "HEAD" {
            return method_not_allowed();
        }
        HttpResponse::json(
            200,
            &serde_json::json!({
                "service": "redshift",
                "status": "running",
                "running": true,
            }),
        )
    }

    fn handle_api(
        &self,
        method: &str,
        query: &str,
        headers: &BTreeMap<String, String>,
        body: &[u8],
    ) -> HttpResponse {
        if method != "GET" && method != "POST" {
            return method_not_allowed();
        }
        if let Some(target) = headers.get("x-amz-target").filter(|t| !t.is_empty()) {
            if method != "POST" {
                return method_not_allowed();
            }
            if target.starts_with("RedshiftServerless.") {
                return self.handle_serverless_target(operation_name(target), body);
            }
            return self.handle_data_api_target(operation_name(target), body);
        }

        // AWS Query protocol: Action from query string or POST form body.
        let mut form = parse_query(query);
        let mut action = form.get("Action").cloned().unwrap_or_default();
        if action.is_empty() && method == "POST" {
            let body_form = parse_query(std::str::from_utf8(body).unwrap_or(""));
            for (k, v) in body_form {
                form.entry(k).or_insert(v);
            }
            action = form.get("Action").cloned().unwrap_or_default();
        }
        match action.as_str() {
            "DescribeClusters" => self.handle_describe_clusters(),
            "GetClusterCredentials" => self.handle_get_cluster_credentials(&form),
            "CreateCluster" => self.handle_create_cluster(&form),
            "DeleteCluster" => self.handle_delete_cluster(&form),
            "DescribeClusterSnapshots" => self.handle_describe_cluster_snapshots(&form),
            "CreateClusterSnapshot" => self.handle_create_cluster_snapshot(&form),
            "DeleteClusterSnapshot" => self.handle_delete_cluster_snapshot(&form),
            "RestoreFromClusterSnapshot" => self.handle_restore_from_cluster_snapshot(&form),
            "DescribeTags" => self.handle_describe_tags(&form),
            "DescribeClusterParameterGroups" => self.handle_describe_parameter_groups(&form),
            "DescribeClusterParameters" => self.handle_describe_parameters(&form),
            "CreateTags" => self.handle_create_tags(&form),
            "DeleteTags" => self.handle_delete_tags(&form),
            "" => HttpResponse::json(200, &serde_json::to_value(self.service_snapshot()).unwrap()),
            _ => json_error(400, "InvalidAction", "unsupported redshift action"),
        }
    }

    fn handle_introspect(&self, method: &str, path: &str, query: &str) -> HttpResponse {
        if method != "GET" {
            return json_error(
                405,
                "MethodNotAllowed",
                "introspection endpoints are read-only",
            );
        }

        let rest = path.trim_start_matches("/_introspect/");
        if rest == "clusters" {
            return HttpResponse::json(
                200,
                &serde_json::to_value(self.service_snapshot()).unwrap_or_default(),
            );
        }
        if rest == "catalog" {
            return HttpResponse::json(
                200,
                &serde_json::to_value(self.catalog_snapshot()).unwrap_or_default(),
            );
        }
        if rest == "statements" {
            return HttpResponse::json(
                200,
                &serde_json::to_value(self.statement_snapshots()).unwrap_or_default(),
            );
        }
        if let Some(tail) = rest.strip_prefix("tables/") {
            let mut parts = tail.split('/');
            let Some(schema_name) = parts.next().filter(|s| !s.is_empty()) else {
                return json_error(404, "NotFound", "introspection endpoint not found");
            };
            let Some(table_name) = parts.next().filter(|s| !s.is_empty()) else {
                return json_error(404, "NotFound", "introspection endpoint not found");
            };
            if parts.next().is_some() {
                return json_error(404, "NotFound", "introspection endpoint not found");
            }
            let limit = parse_query(query)
                .get("limit")
                .and_then(|value| value.parse::<usize>().ok())
                .filter(|value| *value > 0)
                .unwrap_or(100);
            return match self.table_detail_snapshot(
                &url_decode(schema_name),
                &url_decode(table_name),
                limit,
            ) {
                Some(detail) => {
                    HttpResponse::json(200, &serde_json::to_value(detail).unwrap_or_default())
                }
                None => json_error(404, "NotFound", "table not found"),
            };
        }

        json_error(404, "NotFound", "introspection endpoint not found")
    }

    fn handle_control(
        &self,
        method: &str,
        path: &str,
        _headers: &BTreeMap<String, String>,
        body: &[u8],
    ) -> HttpResponse {
        if method != "POST" {
            return json_error(
                405,
                "MethodNotAllowed",
                "/_control/ endpoints accept POST only",
            );
        }

        match path.trim_start_matches("/_control/") {
            "query" => self.handle_control_query(body),
            _ => json_error(404, "NotFound", "control endpoint not found"),
        }
    }

    fn handle_control_query(&self, body: &[u8]) -> HttpResponse {
        let request: ControlQueryRequest = match decode(body) {
            Ok(req) => req,
            Err(_) => {
                return json_error(400, "BadRequest", "invalid control query request");
            }
        };
        if request.sql.trim().is_empty() {
            return json_error(400, "BadRequest", "sql is required");
        }

        match self.execute_dashboard_sql(&request.sql, request.max_rows.unwrap_or(0)) {
            Ok(result) => HttpResponse::json(200, &serde_json::json!({ "result": result })),
            Err(_) => json_error(400, "BadRequest", "redshift query failed"),
        }
    }

    fn handle_serverless_target(&self, operation: &str, body: &[u8]) -> HttpResponse {
        match operation {
            "ListNamespaces" => match decode::<ServerlessListRequest>(body) {
                Ok(_) => HttpResponse::data_api_value(
                    200,
                    serde_json::json!({ "namespaces": [self.shared.serverless_namespace()] }),
                ),
                Err(resp) => resp,
            },
            "GetNamespace" => match decode::<ServerlessNamespaceRequest>(body) {
                Ok(req) => {
                    let namespace = self.shared.serverless_namespace();
                    if !req.namespace_name.is_empty()
                        && req.namespace_name != namespace.namespace_name
                    {
                        return data_api_error(
                            404,
                            "ResourceNotFoundException",
                            "namespace does not exist",
                        );
                    }
                    HttpResponse::data_api_value(200, serde_json::json!({ "namespace": namespace }))
                }
                Err(resp) => resp,
            },
            "ListWorkgroups" => match decode::<ServerlessListRequest>(body) {
                Ok(_) => HttpResponse::data_api_value(
                    200,
                    serde_json::json!({ "workgroups": [self.shared.serverless_workgroup()] }),
                ),
                Err(resp) => resp,
            },
            "GetWorkgroup" => match decode::<ServerlessWorkgroupRequest>(body) {
                Ok(req) => {
                    let workgroup = self.shared.serverless_workgroup();
                    if !req.workgroup_name.is_empty()
                        && req.workgroup_name != workgroup.workgroup_name
                    {
                        return data_api_error(
                            404,
                            "ResourceNotFoundException",
                            "workgroup does not exist",
                        );
                    }
                    HttpResponse::data_api_value(200, serde_json::json!({ "workgroup": workgroup }))
                }
                Err(resp) => resp,
            },
            _ => data_api_error(
                400,
                "ValidationException",
                "unsupported redshift-serverless action",
            ),
        }
    }

    fn handle_data_api_target(&self, operation: &str, body: &[u8]) -> HttpResponse {
        match operation {
            "BatchExecuteStatement" => self.handle_batch_execute_statement(body),
            "ExecuteStatement" => self.handle_execute_statement(body),
            "DescribeStatement" => self.handle_describe_statement(body),
            "GetStatementResult" => self.handle_get_statement_result(body),
            "GetStatementResultV2" => self.handle_get_statement_result_v2(body),
            "ListStatements" => self.handle_list_statements(body),
            "CancelStatement" => self.handle_cancel_statement(body),
            "ListDatabases" => self.handle_list_databases(body),
            "ListSchemas" => self.handle_list_schemas(body),
            "ListTables" => self.handle_list_tables(body),
            "DescribeTable" => self.handle_describe_table(body),
            _ => data_api_error(
                400,
                "ValidationException",
                "unsupported redshift-data action",
            ),
        }
    }

    // --- Data API: statement lifecycle -------------------------------------

    fn handle_execute_statement(&self, body: &[u8]) -> HttpResponse {
        let request: ExecuteStatementRequest = match decode(body) {
            Ok(req) => req,
            Err(resp) => return resp,
        };
        if request.sql.trim().is_empty() {
            return data_api_error(400, "ValidationException", "Sql is required");
        }
        if let Err(err) = self.shared.validate_statement_size(&request.sql) {
            return data_api_error(400, "ValidationException", &err.to_string());
        }
        if !request.client_token.is_empty() {
            if let Some(resp) = self.idempotent_response(&request.client_token) {
                return resp;
            }
        }
        let result_format = match normalize_data_api_result_format(&request.result_format) {
            Ok(format) => format,
            Err(message) => return data_api_error(400, "ValidationException", &message),
        };
        let created_at = SystemTime::now();
        let session_id = match self.shared.session_id_for_request(
            &request.session_id,
            request.session_keep_alive_seconds,
            created_at,
        ) {
            Ok(id) => id,
            Err(err) => return data_api_error(400, "ValidationException", &err.to_string()),
        };
        let execution = self.execute_sql(&request.sql);
        let mut stmt = self.new_statement(
            &request.cluster_identifier,
            &request.database,
            &request.db_user,
            session_id,
            request.sql.clone(),
            result_format,
            created_at,
        );
        match execution {
            Ok(result) => {
                stmt.has_result_set = !result.fields.is_empty();
                stmt.result = result;
            }
            Err(err) => {
                stmt.status = "FAILED".to_string();
                stmt.error = err.to_string();
            }
        }
        let response = self.store_statement(stmt.clone(), &request.client_token);
        self.emit_event(
            "redshift.statement.executed",
            serde_json::json!({ "statementID": stmt.id }),
        );
        response
    }

    fn handle_batch_execute_statement(&self, body: &[u8]) -> HttpResponse {
        let request: BatchExecuteStatementRequest = match decode(body) {
            Ok(req) => req,
            Err(resp) => return resp,
        };
        let sqls = compact_sql_statements(&request.sqls);
        if sqls.is_empty() {
            return data_api_error(400, "ValidationException", "Sqls is required");
        }
        for sql in &sqls {
            if let Err(err) = self.shared.validate_statement_size(sql) {
                return data_api_error(400, "ValidationException", &err.to_string());
            }
        }
        if !request.client_token.is_empty() {
            if let Some(resp) = self.idempotent_response(&request.client_token) {
                return resp;
            }
        }
        let result_format = match normalize_data_api_result_format(&request.result_format) {
            Ok(format) => format,
            Err(message) => return data_api_error(400, "ValidationException", &message),
        };
        let created_at = SystemTime::now();
        let session_id = match self.shared.session_id_for_request(
            &request.session_id,
            request.session_keep_alive_seconds,
            created_at,
        ) {
            Ok(id) => id,
            Err(err) => return data_api_error(400, "ValidationException", &err.to_string()),
        };
        let query_string = sqls.join(";\n");
        let execution = self.execute_sql_batch(&sqls);
        let mut stmt = self.new_statement(
            &request.cluster_identifier,
            &request.database,
            &request.db_user,
            session_id,
            query_string,
            result_format,
            created_at,
        );
        match execution {
            Ok(result) => {
                stmt.has_result_set = !result.fields.is_empty();
                stmt.result = result;
            }
            Err(err) => {
                stmt.status = "FAILED".to_string();
                stmt.error = err.to_string();
                stmt.has_result_set = false;
                stmt.result = crate::engine::QueryResult::default();
            }
        }
        let response = self.store_statement(stmt.clone(), &request.client_token);
        self.emit_event(
            "redshift.statement.batch_executed",
            serde_json::json!({ "statementID": stmt.id }),
        );
        response
    }

    /// Mirrors the legacy `events.Emit(s.eventPublisher, …)` calls: prints a
    /// `DEVCLOUD_EVENT {json}` line on stdout for the daemon's dashboard bridge
    /// when enabled (binary only). Never logs credentials or payloads.
    /// In single-binary mode the orchestrator installs an EVENT_SINK so events
    /// reach the dashboard relay without stdout. The sink path runs regardless
    /// of `events_enabled`; stdout is gated by that flag as before.
    pub(crate) fn emit_event(&self, event_type: &str, payload: serde_json::Value) {
        let json = serde_json::json!({
            "type": event_type,
            "service": "redshift",
            "payload": payload,
        })
        .to_string();
        if let Some(tx) = crate::event_sink() {
            let _ = tx.send(json.clone());
        }
        if self.shared.config.events_enabled {
            println!("DEVCLOUD_EVENT {json}");
        }
    }

    /// Mirrors the post-execution statement insert + idempotency persist.
    fn store_statement(&self, stmt: StatementRecord, client_token: &str) -> HttpResponse {
        let response = execute_statement_response_from_statement(&stmt);
        let mut state = self.shared.lock_state();
        state.statements.insert(stmt.id.clone(), stmt.clone());
        if !client_token.is_empty() {
            state
                .client_token_index
                .insert(client_token.to_string(), stmt.id.clone());
        }
        let _ = self.shared.persist_locked(&state);
        drop(state);
        HttpResponse::data_api(200, &response)
    }

    /// Returns the idempotent ExecuteStatement response when `client_token` was
    /// already seen.
    fn idempotent_response(&self, client_token: &str) -> Option<HttpResponse> {
        let state = self.shared.lock_state();
        let id = state.client_token_index.get(client_token)?;
        let stmt = state.statements.get(id)?;
        Some(HttpResponse::data_api(
            200,
            &execute_statement_response_from_statement(stmt),
        ))
    }

    fn new_statement(
        &self,
        cluster_identifier: &str,
        database: &str,
        db_user: &str,
        session_id: String,
        query_string: String,
        result_format: String,
        created_at: SystemTime,
    ) -> StatementRecord {
        let config = &self.shared.config;
        StatementRecord {
            id: self.shared.next_statement_id_value(),
            cluster_identifier: default_str(
                cluster_identifier,
                &default_str(&config.cluster_identifier, "devcloud"),
            ),
            database: default_str(database, &default_str(&config.database, "dev")),
            db_user: default_str(db_user, &default_str(&config.user, "dev")),
            session_id,
            query_string,
            result_format,
            created_at,
            updated_at: created_at,
            status: "FINISHED".to_string(),
            error: String::new(),
            has_result_set: false,
            result: crate::engine::QueryResult::default(),
        }
    }

    fn handle_describe_statement(&self, body: &[u8]) -> HttpResponse {
        let request: StatementIdRequest = match decode(body) {
            Ok(req) => req,
            Err(resp) => return resp,
        };
        match self.statement_by_id(&request.id) {
            Ok(stmt) => {
                HttpResponse::data_api(200, &describe_statement_response_from_statement(&stmt))
            }
            Err(resp) => resp,
        }
    }

    fn handle_get_statement_result(&self, body: &[u8]) -> HttpResponse {
        let request: GetStatementResultRequest = match decode(body) {
            Ok(req) => req,
            Err(resp) => return resp,
        };
        let stmt = match self.statement_by_id(&request.id) {
            Ok(stmt) => stmt,
            Err(resp) => return resp,
        };
        if stmt.status != "FINISHED" {
            return data_api_error(
                400,
                "ValidationException",
                "statement has no finished result",
            );
        }
        let (rows, next_token) =
            match dataapi::paginate(&stmt.result.rows, request.max_results, &request.next_token) {
                Ok(page) => page,
                Err(err) => return data_api_error(400, "ValidationException", &err.to_string()),
            };
        let mut response = get_statement_result_response(&stmt.result, rows);
        if !next_token.is_empty() {
            response["NextToken"] = serde_json::Value::String(next_token);
        }
        HttpResponse::data_api_value(200, response)
    }

    fn handle_get_statement_result_v2(&self, body: &[u8]) -> HttpResponse {
        let request: GetStatementResultRequest = match decode(body) {
            Ok(req) => req,
            Err(resp) => return resp,
        };
        let stmt = match self.statement_by_id(&request.id) {
            Ok(stmt) => stmt,
            Err(resp) => return resp,
        };
        if stmt.status != "FINISHED" {
            return data_api_error(
                400,
                "ValidationException",
                "statement has no finished result",
            );
        }
        if !default_str(&stmt.result_format, "JSON").eq_ignore_ascii_case("CSV") {
            return data_api_error(
                400,
                "ValidationException",
                "GetStatementResultV2 requires a statement executed with ResultFormat CSV",
            );
        }
        let (rows, next_token) =
            match dataapi::paginate(&stmt.result.rows, request.max_results, &request.next_token) {
                Ok(page) => page,
                Err(err) => return data_api_error(400, "ValidationException", &err.to_string()),
            };
        let mut response = match get_statement_result_v2_response(&stmt.result, rows) {
            Ok(response) => response,
            Err(_) => {
                return data_api_error(
                    500,
                    "InternalServerException",
                    "failed to encode CSV result",
                )
            }
        };
        if !next_token.is_empty() {
            response["NextToken"] = serde_json::Value::String(next_token);
        }
        HttpResponse::data_api_value(200, response)
    }

    fn handle_cancel_statement(&self, body: &[u8]) -> HttpResponse {
        let request: StatementIdRequest = match decode(body) {
            Ok(req) => req,
            Err(resp) => return resp,
        };
        if request.id.is_empty() {
            return data_api_error(400, "ValidationException", "Id is required");
        }
        let mut state = self.shared.lock_state();
        let Some(stmt) = state.statements.get_mut(&request.id) else {
            return data_api_error(404, "ResourceNotFoundException", "statement does not exist");
        };
        let cancelled = stmt.status == "SUBMITTED" || stmt.status == "STARTED";
        if cancelled {
            stmt.status = "ABORTED".to_string();
            stmt.updated_at = SystemTime::now();
            let _ = self.shared.persist_locked(&state);
        }
        drop(state);
        HttpResponse::data_api_value(200, serde_json::json!({ "Status": cancelled }))
    }

    fn handle_list_statements(&self, body: &[u8]) -> HttpResponse {
        let request: ListStatementsRequest = match decode(body) {
            Ok(req) => req,
            Err(resp) => return resp,
        };
        let state = self.shared.lock_state();
        let statements: Vec<StatementListItem> = state
            .statements
            .values()
            .filter(|stmt| {
                request.status.is_empty() || stmt.status.eq_ignore_ascii_case(&request.status)
            })
            .map(|stmt| StatementListItem {
                id: stmt.id.clone(),
                query_string: safe_statement_query_string(&stmt.query_string),
                status: stmt.status.clone(),
                created_at: crate::storage::unix_seconds(stmt.created_at),
                updated_at: crate::storage::unix_seconds(stmt.updated_at),
                has_result_set: stmt.has_result_set,
            })
            .collect();
        drop(state);
        HttpResponse::data_api_value(200, serde_json::json!({ "Statements": statements }))
    }

    /// Mirrors `statementByID`.
    fn statement_by_id(&self, id: &str) -> Result<StatementRecord, HttpResponse> {
        if id.is_empty() {
            return Err(data_api_error(400, "ValidationException", "Id is required"));
        }
        let state = self.shared.lock_state();
        match state.statements.get(id) {
            Some(stmt) => Ok(stmt.clone()),
            None => Err(data_api_error(
                404,
                "ResourceNotFoundException",
                "statement does not exist",
            )),
        }
    }

    // --- Data API: metadata ------------------------------------------------

    fn handle_list_databases(&self, body: &[u8]) -> HttpResponse {
        if let Err(resp) = decode::<ListMetadataRequest>(body) {
            return resp;
        }
        HttpResponse::data_api_value(
            200,
            serde_json::json!({ "Databases": [default_str(&self.shared.config.database, "dev")] }),
        )
    }

    fn handle_list_schemas(&self, body: &[u8]) -> HttpResponse {
        let request: ListMetadataRequest = match decode(body) {
            Ok(req) => req,
            Err(resp) => return resp,
        };
        let state = self.shared.lock_state();
        let schemas: Vec<String> = state
            .db
            .schemas
            .keys()
            .filter(|name| metadata_pattern_matches(name, &request.schema_pattern))
            .cloned()
            .collect();
        drop(state);
        let (page, next_token) =
            match dataapi::paginate(&schemas, request.max_results, &request.next_token) {
                Ok(page) => page,
                Err(err) => return data_api_error(400, "ValidationException", &err.to_string()),
            };
        let mut response = serde_json::json!({ "Schemas": page });
        if !next_token.is_empty() {
            response["NextToken"] = serde_json::Value::String(next_token);
        }
        HttpResponse::data_api_value(200, response)
    }

    fn handle_list_tables(&self, body: &[u8]) -> HttpResponse {
        let request: ListMetadataRequest = match decode(body) {
            Ok(req) => req,
            Err(resp) => return resp,
        };
        let state = self.shared.lock_state();
        let mut tables: Vec<TableMember> = Vec::new();
        for (schema_name, schema_state) in &state.db.schemas {
            if !request.schema.is_empty() && schema_name != &request.schema {
                continue;
            }
            if !metadata_pattern_matches(schema_name, &request.schema_pattern) {
                continue;
            }
            for (table_name, table_state) in &schema_state.tables {
                if !metadata_pattern_matches(table_name, &request.table_pattern) {
                    continue;
                }
                tables.push(TableMember {
                    name: table_name.clone(),
                    schema: schema_name.clone(),
                    member_type: data_api_table_type(table_state),
                });
            }
        }
        drop(state);
        let (page, next_token) =
            match dataapi::paginate(&tables, request.max_results, &request.next_token) {
                Ok(page) => page,
                Err(err) => return data_api_error(400, "ValidationException", &err.to_string()),
            };
        let members: Vec<serde_json::Value> = page
            .iter()
            .map(|table| {
                serde_json::json!({
                    "name": table.name,
                    "schema": table.schema,
                    "type": table.member_type,
                })
            })
            .collect();
        let mut response = serde_json::json!({ "Tables": members });
        if !next_token.is_empty() {
            response["NextToken"] = serde_json::Value::String(next_token);
        }
        HttpResponse::data_api_value(200, response)
    }

    fn handle_describe_table(&self, body: &[u8]) -> HttpResponse {
        let request: DescribeTableRequest = match decode(body) {
            Ok(req) => req,
            Err(resp) => return resp,
        };
        let name = QualifiedName {
            schema: default_str(&request.schema, "public"),
            table: request.table.clone(),
        };
        if name.table.is_empty() {
            return data_api_error(400, "ValidationException", "Table is required");
        }
        let state = self.shared.lock_state();
        let Some(table_state) = lookup_table(&state.db, &name) else {
            return data_api_error(404, "ResourceNotFoundException", "table does not exist");
        };
        let columns: Vec<ColumnMetadata> = table_state
            .columns
            .iter()
            .enumerate()
            .map(|(i, column)| column_metadata_from_column(column, i))
            .collect();
        drop(state);
        let (page, next_token) =
            match dataapi::paginate(&columns, request.max_results, &request.next_token) {
                Ok(page) => page,
                Err(err) => return data_api_error(400, "ValidationException", &err.to_string()),
            };
        let mut response = serde_json::json!({ "ColumnList": page, "TableName": name.table });
        if !next_token.is_empty() {
            response["NextToken"] = serde_json::Value::String(next_token);
        }
        HttpResponse::data_api_value(200, response)
    }

    // --- control plane: clusters -------------------------------------------

    fn handle_describe_clusters(&self) -> HttpResponse {
        let clusters = self.shared.cluster_snapshots_locked();
        let mut xml = XmlBuilder::new("DescribeClustersResponse");
        let mut result = XmlElement::new("DescribeClustersResult");
        let mut members = XmlElement::new("Clusters");
        for cluster in &clusters {
            members.push_child(cluster_xml(cluster));
        }
        result.push_child(members);
        xml.push_child(result);
        xml.push_child(response_metadata());
        HttpResponse::xml(xml.render())
    }

    fn handle_get_cluster_credentials(&self, form: &Form) -> HttpResponse {
        let identifier = default_str(
            form.get("ClusterIdentifier")
                .map(String::as_str)
                .unwrap_or(""),
            &default_cluster_identifier(&self.shared.config.cluster_identifier),
        );
        let exists = self.shared.lock_state().clusters.contains_key(&identifier);
        if !exists {
            return json_error(404, "ClusterNotFound", "cluster does not exist");
        }
        let db_user = default_str(
            form.get("DbUser").map(String::as_str).unwrap_or(""),
            &default_str(&self.shared.config.user, "dev"),
        );
        let db_password = default_str(&self.shared.config.password, "dev");
        // Expiration text is opaque to the parity tests (they only assert the
        // tag is present), so emit a stable placeholder timestamp.
        let mut xml = XmlBuilder::new("GetClusterCredentialsResponse");
        let mut result = XmlElement::new("GetClusterCredentialsResult");
        result.push_text_child("DbUser", &db_user);
        result.push_text_child("DbPassword", &db_password);
        result.push_text_child(
            "Expiration",
            &crate::storage::format_rfc3339_nano(SystemTime::now()),
        );
        xml.push_child(result);
        xml.push_child(response_metadata());
        HttpResponse::xml(xml.render())
    }

    fn handle_create_cluster(&self, form: &Form) -> HttpResponse {
        let cluster = self.cluster_snapshot_from_form(form);
        let mut state = self.shared.lock_state();
        if state.clusters.contains_key(&cluster.cluster_identifier) {
            return json_error(400, "ClusterAlreadyExists", "cluster already exists");
        }
        state
            .clusters
            .insert(cluster.cluster_identifier.clone(), cluster.clone());
        if self.shared.persist_locked(&state).is_err() {
            return json_error(
                500,
                "InternalFailure",
                "persist redshift cluster metadata failed",
            );
        }
        drop(state);
        cluster_action_xml("CreateClusterResponse", "CreateClusterResult", &cluster)
    }

    fn handle_delete_cluster(&self, form: &Form) -> HttpResponse {
        let identifier = default_str(
            form.get("ClusterIdentifier")
                .map(String::as_str)
                .unwrap_or(""),
            &default_cluster_identifier(&self.shared.config.cluster_identifier),
        );
        let mut state = self.shared.lock_state();
        let cluster = state.clusters.remove(&identifier);
        if cluster.is_some() && self.shared.persist_locked(&state).is_err() {
            return json_error(
                500,
                "InternalFailure",
                "persist redshift cluster metadata failed",
            );
        }
        drop(state);
        match cluster {
            Some(cluster) => {
                cluster_action_xml("DeleteClusterResponse", "DeleteClusterResult", &cluster)
            }
            None => json_error(404, "ClusterNotFound", "cluster does not exist"),
        }
    }

    fn cluster_snapshot_from_form(&self, form: &Form) -> ClusterSnapshot {
        let mut cluster = cluster_snapshot_from_config(&self.shared.config);
        cluster.cluster_identifier = default_str(
            form.get("ClusterIdentifier")
                .map(String::as_str)
                .unwrap_or(""),
            &cluster.cluster_identifier,
        );
        cluster.database_name = default_str(
            form.get("DBName").map(String::as_str).unwrap_or(""),
            &cluster.database_name,
        );
        cluster.node_type = default_str(
            form.get("NodeType").map(String::as_str).unwrap_or(""),
            &cluster.node_type,
        );
        cluster.master_username = default_str(
            form.get("MasterUsername").map(String::as_str).unwrap_or(""),
            &cluster.master_username,
        );
        if let Some(nodes) = form
            .get("NumberOfNodes")
            .and_then(|n| n.parse::<i64>().ok())
            .filter(|n| *n > 0)
        {
            cluster.number_of_nodes = nodes;
        }
        cluster.tags = parse_tag_members(form);
        cluster
    }

    // --- control plane: cluster snapshots ----------------------------------

    fn handle_describe_cluster_snapshots(&self, form: &Form) -> HttpResponse {
        let cluster_identifier = form
            .get("ClusterIdentifier")
            .map(|s| s.trim().to_string())
            .unwrap_or_default();
        let snapshot_identifier = form
            .get("SnapshotIdentifier")
            .map(|s| s.trim().to_string())
            .unwrap_or_default();
        let state = self.shared.lock_state();
        let mut members = XmlElement::new("Snapshots");
        for (id, snapshot) in &state.snapshots {
            if !snapshot_identifier.is_empty() && id != &snapshot_identifier {
                continue;
            }
            if !cluster_identifier.is_empty() && snapshot.cluster_identifier != cluster_identifier {
                continue;
            }
            members.push_child(cluster_snapshot_xml(snapshot));
        }
        drop(state);
        let mut xml = XmlBuilder::new("DescribeClusterSnapshotsResponse");
        let mut result = XmlElement::new("DescribeClusterSnapshotsResult");
        result.push_child(members);
        xml.push_child(result);
        xml.push_child(response_metadata());
        HttpResponse::xml(xml.render())
    }

    fn handle_create_cluster_snapshot(&self, form: &Form) -> HttpResponse {
        let snapshot_identifier = form
            .get("SnapshotIdentifier")
            .map(|s| s.trim().to_string())
            .unwrap_or_default();
        if snapshot_identifier.is_empty() {
            return json_error(
                400,
                "InvalidParameterValue",
                "SnapshotIdentifier is required",
            );
        }
        let cluster_identifier = default_str(
            form.get("ClusterIdentifier")
                .map(|s| s.trim())
                .unwrap_or(""),
            &default_cluster_identifier(&self.shared.config.cluster_identifier),
        );
        let mut state = self.shared.lock_state();
        let Some(cluster) = state.clusters.get(&cluster_identifier).cloned() else {
            return json_error(404, "ClusterNotFound", "cluster does not exist");
        };
        if state.snapshots.contains_key(&snapshot_identifier) {
            return json_error(
                400,
                "ClusterSnapshotAlreadyExists",
                "cluster snapshot already exists",
            );
        }
        let now = crate::storage::format_rfc3339(SystemTime::now());
        let snapshot = cluster_snapshot_metadata_from_cluster(&snapshot_identifier, &cluster, &now);
        state
            .snapshots
            .insert(snapshot_identifier.clone(), snapshot.clone());
        if self.shared.persist_locked(&state).is_err() {
            return json_error(
                500,
                "InternalFailure",
                "persist redshift snapshot metadata failed",
            );
        }
        drop(state);
        cluster_snapshot_action_xml(
            "CreateClusterSnapshotResponse",
            "CreateClusterSnapshotResult",
            &snapshot,
        )
    }

    fn handle_delete_cluster_snapshot(&self, form: &Form) -> HttpResponse {
        let snapshot_identifier = form
            .get("SnapshotIdentifier")
            .map(|s| s.trim().to_string())
            .unwrap_or_default();
        if snapshot_identifier.is_empty() {
            return json_error(
                400,
                "InvalidParameterValue",
                "SnapshotIdentifier is required",
            );
        }
        let mut state = self.shared.lock_state();
        let snapshot = state.snapshots.remove(&snapshot_identifier);
        if snapshot.is_some() && self.shared.persist_locked(&state).is_err() {
            return json_error(
                500,
                "InternalFailure",
                "persist redshift snapshot metadata failed",
            );
        }
        drop(state);
        match snapshot {
            Some(snapshot) => cluster_snapshot_action_xml(
                "DeleteClusterSnapshotResponse",
                "DeleteClusterSnapshotResult",
                &snapshot,
            ),
            None => json_error(
                404,
                "ClusterSnapshotNotFound",
                "cluster snapshot does not exist",
            ),
        }
    }

    fn handle_restore_from_cluster_snapshot(&self, form: &Form) -> HttpResponse {
        let snapshot_identifier = form
            .get("SnapshotIdentifier")
            .map(|s| s.trim().to_string())
            .unwrap_or_default();
        if snapshot_identifier.is_empty() {
            return json_error(
                400,
                "InvalidParameterValue",
                "SnapshotIdentifier is required",
            );
        }
        let cluster_identifier = form
            .get("ClusterIdentifier")
            .map(|s| s.trim().to_string())
            .unwrap_or_default();
        if cluster_identifier.is_empty() {
            return json_error(
                400,
                "InvalidParameterValue",
                "ClusterIdentifier is required",
            );
        }
        let mut state = self.shared.lock_state();
        let Some(snapshot) = state.snapshots.get(&snapshot_identifier).cloned() else {
            return json_error(
                404,
                "ClusterSnapshotNotFound",
                "cluster snapshot does not exist",
            );
        };
        if state.clusters.contains_key(&cluster_identifier) {
            return json_error(400, "ClusterAlreadyExists", "cluster already exists");
        }
        let cluster = cluster_snapshot_from_snapshot_metadata(
            &cluster_identifier,
            &snapshot,
            &self.shared.config,
        );
        state
            .clusters
            .insert(cluster_identifier.clone(), cluster.clone());
        if self.shared.persist_locked(&state).is_err() {
            return json_error(
                500,
                "InternalFailure",
                "persist redshift restored cluster metadata failed",
            );
        }
        drop(state);
        cluster_action_xml(
            "RestoreFromClusterSnapshotResponse",
            "RestoreFromClusterSnapshotResult",
            &cluster,
        )
    }

    // --- control plane: tags + parameters ----------------------------------

    fn handle_describe_tags(&self, form: &Form) -> HttpResponse {
        let resource_name = form
            .get("ResourceName")
            .map(String::as_str)
            .unwrap_or("")
            .to_string();
        let clusters = self.shared.cluster_snapshots_locked();
        let mut members = XmlElement::new("TaggedResources");
        for cluster in &clusters {
            let arn = self.cluster_arn(&cluster.cluster_identifier);
            if !resource_name.is_empty() && resource_name != arn {
                continue;
            }
            for tag in &cluster.tags {
                let mut member = XmlElement::new("member");
                member.push_text_child("ResourceName", &arn);
                member.push_text_child("ResourceType", "cluster");
                let mut tag_el = XmlElement::new("Tag");
                tag_el.push_text_child("Key", &tag.key);
                tag_el.push_text_child("Value", &tag.value);
                member.push_child(tag_el);
                members.push_child(member);
            }
        }
        let mut xml = XmlBuilder::new("DescribeTagsResponse");
        let mut result = XmlElement::new("DescribeTagsResult");
        result.push_child(members);
        xml.push_child(result);
        xml.push_child(response_metadata());
        HttpResponse::xml(xml.render())
    }

    fn handle_describe_parameter_groups(&self, form: &Form) -> HttpResponse {
        let name = form
            .get("ParameterGroupName")
            .map(|s| s.trim().to_string())
            .unwrap_or_default();
        let mut members = XmlElement::new("ParameterGroups");
        if name.is_empty() || name == DEFAULT_PARAMETER_GROUP_NAME {
            members.push_child(default_parameter_group_xml());
        }
        let mut xml = XmlBuilder::new("DescribeClusterParameterGroupsResponse");
        let mut result = XmlElement::new("DescribeClusterParameterGroupsResult");
        result.push_child(members);
        xml.push_child(result);
        xml.push_child(response_metadata());
        HttpResponse::xml(xml.render())
    }

    fn handle_describe_parameters(&self, form: &Form) -> HttpResponse {
        let name = default_str(
            form.get("ParameterGroupName")
                .map(|s| s.trim())
                .unwrap_or(""),
            DEFAULT_PARAMETER_GROUP_NAME,
        );
        if name != DEFAULT_PARAMETER_GROUP_NAME {
            return json_error(
                404,
                "ClusterParameterGroupNotFound",
                "cluster parameter group does not exist",
            );
        }
        let mut members = XmlElement::new("Parameters");
        for parameter in default_cluster_parameters() {
            members.push_child(parameter);
        }
        let mut xml = XmlBuilder::new("DescribeClusterParametersResponse");
        let mut result = XmlElement::new("DescribeClusterParametersResult");
        result.push_child(members);
        xml.push_child(result);
        xml.push_child(response_metadata());
        HttpResponse::xml(xml.render())
    }

    fn handle_create_tags(&self, form: &Form) -> HttpResponse {
        let resource_name = form
            .get("ResourceName")
            .map(String::as_str)
            .unwrap_or("")
            .to_string();
        if resource_name.is_empty() {
            return json_error(400, "InvalidParameterValue", "ResourceName is required");
        }
        let tags = parse_tag_members(form);
        if tags.is_empty() {
            return json_error(400, "InvalidParameterValue", "Tags are required");
        }
        let mut state = self.shared.lock_state();
        let Some((id, mut cluster)) = self.cluster_by_resource_name(&state, &resource_name) else {
            return json_error(404, "ClusterNotFound", "cluster does not exist");
        };
        cluster.tags = merge_tags(&cluster.tags, &tags);
        state.clusters.insert(id, cluster);
        if self.shared.persist_locked(&state).is_err() {
            return json_error(
                500,
                "InternalFailure",
                "persist redshift tag metadata failed",
            );
        }
        drop(state);
        empty_query_xml("CreateTagsResponse")
    }

    fn handle_delete_tags(&self, form: &Form) -> HttpResponse {
        let resource_name = form
            .get("ResourceName")
            .map(String::as_str)
            .unwrap_or("")
            .to_string();
        if resource_name.is_empty() {
            return json_error(400, "InvalidParameterValue", "ResourceName is required");
        }
        let keys = parse_tag_key_members(form);
        if keys.is_empty() {
            return json_error(400, "InvalidParameterValue", "TagKeys are required");
        }
        let mut state = self.shared.lock_state();
        let Some((id, mut cluster)) = self.cluster_by_resource_name(&state, &resource_name) else {
            return json_error(404, "ClusterNotFound", "cluster does not exist");
        };
        cluster.tags = delete_tags(&cluster.tags, &keys);
        state.clusters.insert(id, cluster);
        if self.shared.persist_locked(&state).is_err() {
            return json_error(
                500,
                "InternalFailure",
                "persist redshift tag metadata failed",
            );
        }
        drop(state);
        empty_query_xml("DeleteTagsResponse")
    }

    /// Mirrors `clusterByResourceNameLocked`.
    fn cluster_by_resource_name(
        &self,
        state: &crate::server::ServerState,
        resource_name: &str,
    ) -> Option<(String, ClusterSnapshot)> {
        state.clusters.iter().find_map(|(id, cluster)| {
            if self.cluster_arn(id) == resource_name {
                Some((id.clone(), cluster.clone()))
            } else {
                None
            }
        })
    }

    /// Test seam mirroring legacy tests that write directly into `server.statements`
    /// to stage a non-terminal statement (e.g. CancelStatement on a STARTED
    /// statement). Not part of the production control plane.
    #[doc(hidden)]
    pub fn seed_statement(&self, stmt: StatementRecord) {
        let mut state = self.shared.lock_state();
        state.statements.insert(stmt.id.clone(), stmt);
    }

    /// Mirrors `clusterARN`.
    fn cluster_arn(&self, identifier: &str) -> String {
        format!(
            "arn:aws:redshift:{}:{}:cluster:{}",
            default_str(&self.shared.config.region, "us-east-1"),
            default_str(&self.shared.config.account_id, "000000000000"),
            identifier
        )
    }
}

// --- decoding / form parsing -----------------------------------------------

type Form = BTreeMap<String, String>;

/// Mirrors `decodeDataAPIRequest`. An empty body decodes to the zero value.
fn decode<T: serde::de::DeserializeOwned>(body: &[u8]) -> Result<T, HttpResponse> {
    let bytes: &[u8] = if body.is_empty() { b"{}" } else { body };
    serde_json::from_slice(bytes)
        .map_err(|_| data_api_error(400, "ValidationException", "invalid JSON request"))
}

fn operation_name(target: &str) -> &str {
    match target.rfind('.') {
        Some(index) => &target[index + 1..],
        None => target,
    }
}

/// Parses `application/x-www-form-urlencoded` / query string content. Later
/// duplicate keys do not overwrite earlier ones (legacy `Form.Get` returns the
/// first), which only matters for the indexed tag members handled separately.
fn parse_query(input: &str) -> Form {
    let mut form = Form::new();
    for pair in input.split('&') {
        if pair.is_empty() {
            continue;
        }
        let (key, value) = match pair.split_once('=') {
            Some((k, v)) => (k, v),
            None => (pair, ""),
        };
        let key = url_decode(key);
        if key.is_empty() {
            continue;
        }
        form.entry(key).or_insert_with(|| url_decode(value));
    }
    form
}

fn url_decode(input: &str) -> String {
    let bytes = input.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'+' => {
                out.push(b' ');
                i += 1;
            }
            b'%' if i + 2 < bytes.len() => {
                let hi = hex_value(bytes[i + 1]);
                let lo = hex_value(bytes[i + 2]);
                if let (Some(hi), Some(lo)) = (hi, lo) {
                    out.push((hi << 4) | lo);
                    i += 3;
                } else {
                    out.push(bytes[i]);
                    i += 1;
                }
            }
            byte => {
                out.push(byte);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn hex_value(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

/// Mirrors `parseTagMembers` (Tags.member.N.Key/Value).
fn parse_tag_members(form: &Form) -> Vec<Tag> {
    let mut tags = Vec::new();
    let mut i = 1;
    loop {
        let key = form
            .get(&format!("Tags.member.{i}.Key"))
            .map(|s| s.trim().to_string())
            .unwrap_or_default();
        let value = form
            .get(&format!("Tags.member.{i}.Value"))
            .cloned()
            .unwrap_or_default();
        if key.is_empty() && value.is_empty() {
            break;
        }
        if !key.is_empty() {
            tags.push(Tag { key, value });
        }
        i += 1;
    }
    tags
}

/// Mirrors `parseTagKeyMembers` (TagKeys.member.N).
fn parse_tag_key_members(form: &Form) -> Vec<String> {
    let mut keys = Vec::new();
    let mut i = 1;
    loop {
        let key = form
            .get(&format!("TagKeys.member.{i}"))
            .map(|s| s.trim().to_string())
            .unwrap_or_default();
        if key.is_empty() {
            break;
        }
        keys.push(key);
        i += 1;
    }
    keys
}

/// Mirrors `normalizeDataAPIResultFormat`.
fn normalize_data_api_result_format(value: &str) -> Result<String, String> {
    if value.is_empty() {
        return Ok("JSON".to_string());
    }
    let normalized = value.trim().to_uppercase();
    match normalized.as_str() {
        "JSON" | "CSV" => Ok(normalized),
        _ => Err(format!("unsupported ResultFormat \"{value}\"")),
    }
}

/// Mirrors `metadataPatternMatches`.
fn metadata_pattern_matches(value: &str, pattern: &str) -> bool {
    if pattern.is_empty() {
        return true;
    }
    sql_like_match(&value.to_lowercase(), &pattern.to_lowercase())
}

/// Mirrors `sqlLikeMatch` (% / _ wildcards).
fn sql_like_match(value: &str, pattern: &str) -> bool {
    let value: Vec<char> = value.chars().collect();
    let pattern: Vec<char> = pattern.chars().collect();
    // Iterative backtracking LIKE matcher.
    let (mut vi, mut pi) = (0usize, 0usize);
    let (mut star_pi, mut star_vi): (Option<usize>, usize) = (None, 0);
    while vi < value.len() {
        if pi < pattern.len() && (pattern[pi] == '_' || pattern[pi] == value[vi]) {
            vi += 1;
            pi += 1;
        } else if pi < pattern.len() && pattern[pi] == '%' {
            star_pi = Some(pi);
            star_vi = vi;
            pi += 1;
        } else if let Some(sp) = star_pi {
            pi = sp + 1;
            star_vi += 1;
            vi = star_vi;
        } else {
            return false;
        }
    }
    while pi < pattern.len() && pattern[pi] == '%' {
        pi += 1;
    }
    pi == pattern.len()
}

/// Mirrors `tableDataAPIType`.
fn data_api_table_type(table: &crate::model::Table) -> String {
    if table.is_materialized_view() {
        "MATERIALIZED_VIEW".to_string()
    } else if table.is_view() {
        "VIEW".to_string()
    } else {
        "TABLE".to_string()
    }
}

// --- XML rendering ---------------------------------------------------------

/// A minimal element tree renderer matching `encoding/xml`'s element output for
/// the shapes the control plane emits (`<name>text</name>` / nested children,
/// XML-escaped text, no self-closing empty elements with children).
struct XmlElement {
    name: String,
    text: Option<String>,
    children: Vec<XmlElement>,
}

impl XmlElement {
    fn new(name: &str) -> XmlElement {
        XmlElement {
            name: name.to_string(),
            text: None,
            children: Vec::new(),
        }
    }

    fn text(name: &str, text: &str) -> XmlElement {
        XmlElement {
            name: name.to_string(),
            text: Some(text.to_string()),
            children: Vec::new(),
        }
    }

    fn push_child(&mut self, child: XmlElement) {
        self.children.push(child);
    }

    fn push_text_child(&mut self, name: &str, text: &str) {
        self.children.push(XmlElement::text(name, text));
    }

    fn render(&self, out: &mut String) {
        out.push('<');
        out.push_str(&self.name);
        out.push('>');
        if let Some(text) = &self.text {
            out.push_str(&xml_escape(text));
        }
        for child in &self.children {
            child.render(out);
        }
        out.push_str("</");
        out.push_str(&self.name);
        out.push('>');
    }
}

/// A top-level response element carrying the `xmlns` attribute.
struct XmlBuilder {
    name: String,
    children: Vec<XmlElement>,
}

impl XmlBuilder {
    fn new(name: &str) -> XmlBuilder {
        XmlBuilder {
            name: name.to_string(),
            children: Vec::new(),
        }
    }

    fn push_child(&mut self, child: XmlElement) {
        self.children.push(child);
    }

    fn render(&self) -> String {
        let mut out = String::new();
        out.push('<');
        out.push_str(&self.name);
        out.push_str(" xmlns=\"");
        out.push_str(XMLNS);
        out.push_str("\">");
        for child in &self.children {
            child.render(&mut out);
        }
        out.push_str("</");
        out.push_str(&self.name);
        out.push('>');
        out
    }
}

fn xml_escape(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for ch in text.chars() {
        match ch {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&#34;"),
            '\'' => out.push_str("&#39;"),
            other => out.push(other),
        }
    }
    out
}

fn response_metadata() -> XmlElement {
    let mut metadata = XmlElement::new("ResponseMetadata");
    metadata.push_text_child("RequestId", REQUEST_ID);
    metadata
}

fn cluster_xml(cluster: &ClusterSnapshot) -> XmlElement {
    let mut member = XmlElement::new("member");
    member.push_text_child("ClusterIdentifier", &cluster.cluster_identifier);
    member.push_text_child("ClusterStatus", &cluster.cluster_status);
    member.push_text_child("DBName", &cluster.database_name);
    member.push_child(endpoint_xml(&cluster.endpoint));
    member.push_text_child("NodeType", &cluster.node_type);
    member.push_text_child("NumberOfNodes", &cluster.number_of_nodes.to_string());
    member.push_text_child("MasterUsername", &cluster.master_username);
    member
}

fn endpoint_xml(endpoint: &ClusterEndpoint) -> XmlElement {
    let mut el = XmlElement::new("Endpoint");
    el.push_text_child("Address", &endpoint.address);
    el.push_text_child("Port", &endpoint.port.to_string());
    el
}

fn cluster_snapshot_xml(snapshot: &ClusterSnapshotMetadata) -> XmlElement {
    let mut member = XmlElement::new("member");
    member.push_text_child("SnapshotIdentifier", &snapshot.snapshot_identifier);
    member.push_text_child("ClusterIdentifier", &snapshot.cluster_identifier);
    member.push_text_child("SnapshotCreateTime", &snapshot.snapshot_create_time);
    member.push_text_child("Status", &snapshot.status);
    member.push_text_child("Port", &snapshot.port.to_string());
    member.push_text_child("AvailabilityZone", &snapshot.availability_zone);
    member.push_text_child("ClusterCreateTime", &snapshot.cluster_create_time);
    member.push_text_child("MasterUsername", &snapshot.master_username);
    member.push_text_child("ClusterVersion", &snapshot.cluster_version);
    member.push_text_child("EngineFullVersion", &snapshot.engine_full_version);
    member.push_text_child("NodeType", &snapshot.node_type);
    member.push_text_child("NumberOfNodes", &snapshot.number_of_nodes.to_string());
    member.push_text_child("DBName", &snapshot.db_name);
    member.push_text_child(
        "Encrypted",
        if snapshot.encrypted { "true" } else { "false" },
    );
    member
}

/// Mirrors `writeClusterActionXML`.
fn cluster_action_xml(
    response_name: &str,
    result_name: &str,
    cluster: &ClusterSnapshot,
) -> HttpResponse {
    let mut xml = XmlBuilder::new(response_name);
    let mut result = XmlElement::new(result_name);
    let mut wrapper = XmlElement::new("Cluster");
    let member = cluster_xml(cluster);
    // The action XML wraps the cluster directly in <Cluster> (not <member>).
    wrapper.children = member.children;
    result.push_child(wrapper);
    xml.push_child(result);
    xml.push_child(response_metadata());
    HttpResponse::xml(xml.render())
}

/// Mirrors `writeClusterSnapshotActionXML`.
fn cluster_snapshot_action_xml(
    response_name: &str,
    result_name: &str,
    snapshot: &ClusterSnapshotMetadata,
) -> HttpResponse {
    let mut xml = XmlBuilder::new(response_name);
    let mut result = XmlElement::new(result_name);
    let mut wrapper = XmlElement::new("Snapshot");
    let member = cluster_snapshot_xml(snapshot);
    wrapper.children = member.children;
    result.push_child(wrapper);
    xml.push_child(result);
    xml.push_child(response_metadata());
    HttpResponse::xml(xml.render())
}

/// Mirrors `writeEmptyQueryXML`.
fn empty_query_xml(response_name: &str) -> HttpResponse {
    let mut xml = XmlBuilder::new(response_name);
    xml.push_child(response_metadata());
    HttpResponse::xml(xml.render())
}

fn default_parameter_group_xml() -> XmlElement {
    let mut member = XmlElement::new("member");
    member.push_text_child("ParameterGroupName", DEFAULT_PARAMETER_GROUP_NAME);
    member.push_text_child("ParameterGroupFamily", "redshift-1.0");
    member.push_text_child(
        "Description",
        "Default devcloud Redshift-compatible parameter group",
    );
    member
}

fn default_cluster_parameters() -> Vec<XmlElement> {
    let make = |name: &str,
                value: &str,
                description: &str,
                data_type: &str,
                allowed: &str,
                apply: &str| {
        let mut member = XmlElement::new("member");
        member.push_text_child("ParameterName", name);
        member.push_text_child("ParameterValue", value);
        member.push_text_child("Description", description);
        member.push_text_child("Source", "engine-default");
        member.push_text_child("DataType", data_type);
        if !allowed.is_empty() {
            member.push_text_child("AllowedValues", allowed);
        }
        member.push_text_child("ApplyType", apply);
        member.push_text_child("IsModifiable", "false");
        member.push_text_child("MinimumEngineVersion", "1.0");
        member
    };
    vec![
        make(
            "datestyle",
            "ISO, MDY",
            "Sets the display format for date and time values.",
            "string",
            "",
            "static",
        ),
        make(
            "enable_user_activity_logging",
            "false",
            "Controls user activity logging metadata for the local Redshift-compatible server.",
            "boolean",
            "true,false",
            "dynamic",
        ),
        make(
            "max_query_execution_time",
            "0",
            "Maximum query execution time in seconds. Zero means unlimited in devcloud.",
            "integer",
            "0-86400",
            "dynamic",
        ),
    ]
}
