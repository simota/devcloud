//! Table CRUD handlers — port of
//! `internal/services/bigquery/table_handlers.rs`.
//!
//! Same shape as `dataset_handlers`: plain methods on [`Server`] taking
//! pre-routed parameters and returning an [`ApiResponse`]; the HTTP routing
//! layer (part 4) maps paths/methods onto them.

use crate::model::{TableListItem, TableResource, TablesListResponse};
use crate::responses::{dataset_etag, default_string, unix_millis_string, ApiResponse};
use crate::server::{now_unix_nanos, Server};
use crate::validation::{
    decode_body, max_results_from_request, row_offset_from_request, validate_resource_id,
    validate_table_schema, Query,
};

impl Server {
    /// `POST /bigquery/v2/projects/{p}/datasets/{d}/tables` (legacy `createTable`).
    pub fn create_table(&self, project_id: &str, dataset_id: &str, body: &[u8]) -> ApiResponse {
        match self.read_dataset(project_id, dataset_id) {
            Err(_) => return ApiResponse::error(500, "backendError", "internal error"),
            Ok(None) => {
                return ApiResponse::error(
                    404,
                    "notFound",
                    &format!("Not found: Dataset {project_id}:{dataset_id}"),
                )
            }
            Ok(Some(_)) => {}
        }

        let mut request: TableResource = match decode_body(body, self.max_request_bytes()) {
            Ok(request) => request,
            Err(()) => return ApiResponse::error(400, "invalid", "invalid json request"),
        };
        if request.table_reference.project_id.is_empty() {
            request.table_reference.project_id = project_id.to_string();
        }
        if request.table_reference.dataset_id.is_empty() {
            request.table_reference.dataset_id = dataset_id.to_string();
        }
        if request.table_reference.project_id != project_id
            || request.table_reference.dataset_id != dataset_id
        {
            return ApiResponse::error(
                400,
                "invalid",
                "tableReference must match request project and dataset",
            );
        }
        if let Err(message) = validate_resource_id(&request.table_reference.table_id, "table") {
            return ApiResponse::error(400, "invalid", &message);
        }
        if let Err(message) = validate_table_schema(&request.schema) {
            return ApiResponse::error(400, "invalid", &message);
        }

        let table_id = request.table_reference.table_id.clone();
        let path = self.table_path(project_id, dataset_id, &table_id);
        match std::fs::metadata(&path) {
            Ok(_) => {
                return ApiResponse::error(
                    409,
                    "duplicate",
                    &format!("Already Exists: Table {project_id}:{dataset_id}.{table_id}"),
                )
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(_) => return ApiResponse::error(500, "backendError", "internal error"),
        }

        let now = now_unix_nanos();
        let resource = TableResource {
            kind: "bigquery#table".to_string(),
            id: format!("{project_id}:{dataset_id}.{table_id}"),
            self_link: self.table_self_link(project_id, dataset_id, &table_id),
            table_reference: request.table_reference,
            table_type: default_string(request.table_type, "TABLE"),
            schema: request.schema,
            friendly_name: request.friendly_name,
            description: request.description,
            labels: request.labels,
            time_partitioning: request.time_partitioning,
            range_partitioning: request.range_partitioning,
            clustering: request.clustering,
            view: request.view,
            etag: dataset_etag(now),
            creation_time: unix_millis_string(now),
            last_modified_time: unix_millis_string(now),
            num_rows: "0".to_string(),
            num_bytes: "0".to_string(),
            location: self.default_location().to_string(),
        };
        if self.write_table(&resource).is_err() {
            return ApiResponse::error(500, "backendError", "internal error");
        }
        self.emit_event(
            "bigquery.table.created",
            serde_json::json!({"project": project_id, "dataset": dataset_id, "table": resource.table_reference.table_id}),
        );
        let location = resource.self_link.clone();
        ApiResponse::json(200, &resource).with_location(location)
    }

    /// `GET /bigquery/v2/projects/{p}/datasets/{d}/tables/{t}` (legacy `getTable`).
    pub fn get_table(&self, project_id: &str, dataset_id: &str, table_id: &str) -> ApiResponse {
        match self.read_table(project_id, dataset_id, table_id) {
            Err(_) => ApiResponse::error(500, "backendError", "internal error"),
            Ok(None) => ApiResponse::error(
                404,
                "notFound",
                &format!("Not found: Table {project_id}:{dataset_id}.{table_id}"),
            ),
            Ok(Some(resource)) => ApiResponse::json(200, &resource),
        }
    }

    /// `GET /bigquery/v2/projects/{p}/datasets/{d}/tables` (legacy `listTables`).
    pub fn list_tables(&self, project_id: &str, dataset_id: &str, query: &Query) -> ApiResponse {
        match self.read_dataset(project_id, dataset_id) {
            Err(_) => return ApiResponse::error(500, "backendError", "internal error"),
            Ok(None) => {
                return ApiResponse::error(
                    404,
                    "notFound",
                    &format!("Not found: Dataset {project_id}:{dataset_id}"),
                )
            }
            Ok(Some(_)) => {}
        }
        let tables = match self.read_tables(project_id, dataset_id) {
            Ok(tables) => tables,
            Err(_) => return ApiResponse::error(500, "backendError", "internal error"),
        };
        let offset = match row_offset_from_request(query) {
            Ok(offset) => offset,
            Err(message) => return ApiResponse::error(400, "invalid", &message),
        };
        let max_results = match max_results_from_request(query) {
            Ok(max_results) => max_results,
            Err(message) => return ApiResponse::error(400, "invalid", &message),
        };
        let total = tables.len() as i64;
        let offset = offset.min(total);
        let end = (offset + max_results).min(total);
        let items: Vec<TableListItem> = tables[offset as usize..end as usize]
            .iter()
            .map(|table| TableListItem {
                kind: "bigquery#table".to_string(),
                id: table.id.clone(),
                table_reference: table.table_reference.clone(),
                table_type: table.table_type.clone(),
                friendly_name: table.friendly_name.clone(),
                time_partitioning: table.time_partitioning.clone(),
                range_partitioning: table.range_partitioning.clone(),
                clustering: table.clustering.clone(),
                view: table.view.clone(),
            })
            .collect();
        let response = TablesListResponse {
            kind: "bigquery#tableList".to_string(),
            tables: items,
            total_items: total,
            next_page_token: if end < total {
                end.to_string()
            } else {
                String::new()
            },
        };
        ApiResponse::json(200, &response)
    }

    /// `PATCH`/`PUT /bigquery/v2/projects/{p}/datasets/{d}/tables/{t}`
    /// (legacy `patchTable`; `replace` distinguishes PUT from PATCH).
    pub fn patch_table(
        &self,
        project_id: &str,
        dataset_id: &str,
        table_id: &str,
        replace: bool,
        body: &[u8],
    ) -> ApiResponse {
        let mut existing = match self.read_table(project_id, dataset_id, table_id) {
            Err(_) => return ApiResponse::error(500, "backendError", "internal error"),
            Ok(None) => {
                return ApiResponse::error(
                    404,
                    "notFound",
                    &format!("Not found: Table {project_id}:{dataset_id}.{table_id}"),
                )
            }
            Ok(Some(existing)) => existing,
        };
        let request: TableResource = match decode_body(body, self.max_request_bytes()) {
            Ok(request) => request,
            Err(()) => return ApiResponse::error(400, "invalid", "invalid json request"),
        };
        if !request.table_reference.project_id.is_empty()
            && request.table_reference.project_id != project_id
            || !request.table_reference.dataset_id.is_empty()
                && request.table_reference.dataset_id != dataset_id
            || !request.table_reference.table_id.is_empty()
                && request.table_reference.table_id != table_id
        {
            return ApiResponse::error(400, "invalid", "tableReference must match request table");
        }
        // legacy gates on `request.Schema.Fields != nil`; the Rust model keeps a
        // plain Vec, so an explicit `"fields": []` patch (which clears the
        // schema in legacy) is indistinguishable from an absent schema here.
        if !request.schema.fields.is_empty() {
            if let Err(message) = validate_table_schema(&request.schema) {
                return ApiResponse::error(400, "invalid", &message);
            }
        }

        let now = now_unix_nanos();
        if replace {
            existing.friendly_name = request.friendly_name;
            existing.description = request.description;
            existing.labels = request.labels;
            existing.schema = request.schema;
            existing.table_type = default_string(request.table_type, "TABLE");
            existing.time_partitioning = request.time_partitioning;
            existing.range_partitioning = request.range_partitioning;
            existing.clustering = request.clustering;
            existing.view = request.view;
        } else {
            if !request.friendly_name.is_empty() {
                existing.friendly_name = request.friendly_name;
            }
            if !request.description.is_empty() {
                existing.description = request.description;
            }
            if request.labels.is_some() {
                existing.labels = request.labels;
            }
            if !request.schema.fields.is_empty() {
                existing.schema = request.schema;
            }
            if !request.table_type.is_empty() {
                existing.table_type = request.table_type;
            }
            if request.time_partitioning.is_some() {
                existing.time_partitioning = request.time_partitioning;
            }
            if request.range_partitioning.is_some() {
                existing.range_partitioning = request.range_partitioning;
            }
            if request.clustering.is_some() {
                existing.clustering = request.clustering;
            }
            if request.view.is_some() {
                existing.view = request.view;
            }
        }
        existing.etag = dataset_etag(now);
        existing.last_modified_time = unix_millis_string(now);
        if self.write_table(&existing).is_err() {
            return ApiResponse::error(500, "backendError", "internal error");
        }
        ApiResponse::json(200, &existing)
    }

    /// `DELETE /bigquery/v2/projects/{p}/datasets/{d}/tables/{t}`
    /// (legacy `deleteTable`).
    pub fn delete_table(&self, project_id: &str, dataset_id: &str, table_id: &str) -> ApiResponse {
        let dir = self.table_dir(project_id, dataset_id, table_id);
        match std::fs::metadata(dir.join("table.json")) {
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                return ApiResponse::error(
                    404,
                    "notFound",
                    &format!("Not found: Table {project_id}:{dataset_id}.{table_id}"),
                )
            }
            Err(_) => return ApiResponse::error(500, "backendError", "internal error"),
            Ok(_) => {}
        }
        if std::fs::remove_dir_all(&dir).is_err() {
            return ApiResponse::error(500, "backendError", "internal error");
        }
        self.emit_event(
            "bigquery.table.deleted",
            serde_json::json!({"project": project_id, "dataset": dataset_id, "table": table_id}),
        );
        ApiResponse::no_content()
    }
}
