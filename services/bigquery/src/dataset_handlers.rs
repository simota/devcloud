//! Dataset CRUD handlers — port of
//! `internal/services/bigquery/dataset_handlers.rs`.
//!
//! Handlers are plain methods on [`Server`] taking pre-routed parameters
//! (project id, dataset id, raw body, parsed query) and returning an
//! [`ApiResponse`]; the HTTP routing layer (part 4) maps paths/methods onto
//! them, mirroring how the pubsub/dynamodb crates split store vs handlers.

use crate::model::{DatasetListItem, DatasetResource, DatasetsListResponse};
use crate::responses::{
    dataset_etag, default_string, has_children, unix_millis_string, ApiResponse,
};
use crate::server::{now_unix_nanos, Server};
use crate::validation::{
    decode_body, max_results_from_request, row_offset_from_request, validate_resource_id, Query,
};

impl Server {
    /// `POST /bigquery/v2/projects/{project}/datasets` (legacy `createDataset`).
    pub fn create_dataset(&self, project_id: &str, body: &[u8]) -> ApiResponse {
        let mut request: DatasetResource = match decode_body(body, self.max_request_bytes()) {
            Ok(request) => request,
            Err(()) => return ApiResponse::error(400, "invalid", "invalid json request"),
        };
        if request.dataset_reference.project_id.is_empty() {
            request.dataset_reference.project_id = project_id.to_string();
        }
        if request.dataset_reference.project_id != project_id {
            return ApiResponse::error(
                400,
                "invalid",
                "datasetReference.projectId must match request project",
            );
        }
        if let Err(message) = validate_resource_id(&request.dataset_reference.dataset_id, "dataset")
        {
            return ApiResponse::error(400, "invalid", &message);
        }

        let dataset_id = request.dataset_reference.dataset_id.clone();
        let path = self.dataset_path(project_id, &dataset_id);
        match std::fs::metadata(&path) {
            Ok(_) => {
                return ApiResponse::error(
                    409,
                    "duplicate",
                    &format!("Already Exists: Dataset {project_id}:{dataset_id}"),
                )
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(_) => return ApiResponse::error(500, "backendError", "internal error"),
        }

        let now = now_unix_nanos();
        let resource = DatasetResource {
            kind: "bigquery#dataset".to_string(),
            id: format!("{project_id}:{dataset_id}"),
            self_link: self.dataset_self_link(project_id, &dataset_id),
            dataset_reference: request.dataset_reference,
            location: default_string(request.location, self.default_location()),
            friendly_name: request.friendly_name,
            description: request.description,
            labels: request.labels,
            etag: dataset_etag(now),
            creation_time: unix_millis_string(now),
            last_modified_time: unix_millis_string(now),
        };
        if self.write_dataset(&resource).is_err() {
            return ApiResponse::error(500, "backendError", "internal error");
        }
        self.emit_event(
            "bigquery.dataset.created",
            serde_json::json!({"project": project_id, "dataset": resource.dataset_reference.dataset_id}),
        );
        let location = resource.self_link.clone();
        ApiResponse::json(200, &resource).with_location(location)
    }

    /// `GET /bigquery/v2/projects/{project}/datasets/{dataset}` (legacy `getDataset`).
    pub fn get_dataset(&self, project_id: &str, dataset_id: &str) -> ApiResponse {
        match self.read_dataset(project_id, dataset_id) {
            Err(_) => ApiResponse::error(500, "backendError", "internal error"),
            Ok(None) => ApiResponse::error(
                404,
                "notFound",
                &format!("Not found: Dataset {project_id}:{dataset_id}"),
            ),
            Ok(Some(resource)) => ApiResponse::json(200, &resource),
        }
    }

    /// `GET /bigquery/v2/projects/{project}/datasets` (legacy `listDatasets`).
    pub fn list_datasets(&self, project_id: &str, query: &Query) -> ApiResponse {
        let datasets = match self.read_datasets(project_id) {
            Ok(datasets) => datasets,
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
        let total = datasets.len() as i64;
        let offset = offset.min(total);
        let end = (offset + max_results).min(total);
        let items: Vec<DatasetListItem> = datasets[offset as usize..end as usize]
            .iter()
            .map(|dataset| DatasetListItem {
                kind: "bigquery#dataset".to_string(),
                id: dataset.id.clone(),
                dataset_reference: dataset.dataset_reference.clone(),
                location: dataset.location.clone(),
                friendly_name: dataset.friendly_name.clone(),
            })
            .collect();
        let response = DatasetsListResponse {
            kind: "bigquery#datasetList".to_string(),
            datasets: items,
            total_items: total,
            next_page_token: if end < total {
                end.to_string()
            } else {
                String::new()
            },
        };
        ApiResponse::json(200, &response)
    }

    /// `PATCH`/`PUT /bigquery/v2/projects/{project}/datasets/{dataset}`
    /// (legacy `patchDataset`; `replace` distinguishes PUT from PATCH).
    pub fn patch_dataset(
        &self,
        project_id: &str,
        dataset_id: &str,
        replace: bool,
        body: &[u8],
    ) -> ApiResponse {
        let existing = match self.read_dataset(project_id, dataset_id) {
            Ok(existing) => existing,
            Err(_) => return ApiResponse::error(500, "backendError", "internal error"),
        };
        if existing.is_none() && !replace {
            return ApiResponse::error(
                404,
                "notFound",
                &format!("Not found: Dataset {project_id}:{dataset_id}"),
            );
        }

        let request: DatasetResource = match decode_body(body, self.max_request_bytes()) {
            Ok(request) => request,
            Err(()) => return ApiResponse::error(400, "invalid", "invalid json request"),
        };
        if !request.dataset_reference.project_id.is_empty()
            && request.dataset_reference.project_id != project_id
        {
            return ApiResponse::error(
                400,
                "invalid",
                "datasetReference.projectId must match request project",
            );
        }
        if !request.dataset_reference.dataset_id.is_empty()
            && request.dataset_reference.dataset_id != dataset_id
        {
            return ApiResponse::error(
                400,
                "invalid",
                "datasetReference.datasetId must match request dataset",
            );
        }

        let now = now_unix_nanos();
        let mut existing = existing.unwrap_or_else(|| DatasetResource {
            kind: "bigquery#dataset".to_string(),
            id: format!("{project_id}:{dataset_id}"),
            self_link: self.dataset_self_link(project_id, dataset_id),
            dataset_reference: crate::model::DatasetReference {
                project_id: project_id.to_string(),
                dataset_id: dataset_id.to_string(),
            },
            location: self.default_location().to_string(),
            creation_time: unix_millis_string(now),
            ..Default::default()
        });
        if replace {
            existing.friendly_name = request.friendly_name;
            existing.description = request.description;
            existing.labels = request.labels;
            existing.location = default_string(request.location, self.default_location());
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
            if !request.location.is_empty() {
                existing.location = request.location;
            }
        }
        existing.etag = dataset_etag(now);
        existing.last_modified_time = unix_millis_string(now);
        if self.write_dataset(&existing).is_err() {
            return ApiResponse::error(500, "backendError", "internal error");
        }
        ApiResponse::json(200, &existing)
    }

    /// `DELETE /bigquery/v2/projects/{project}/datasets/{dataset}`
    /// (legacy `deleteDataset`; `?deleteContents=true` allows non-empty datasets).
    pub fn delete_dataset(&self, project_id: &str, dataset_id: &str, query: &Query) -> ApiResponse {
        let dir = self.dataset_dir(project_id, dataset_id);
        match std::fs::metadata(dir.join("dataset.json")) {
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                return ApiResponse::error(
                    404,
                    "notFound",
                    &format!("Not found: Dataset {project_id}:{dataset_id}"),
                )
            }
            Err(_) => return ApiResponse::error(500, "backendError", "internal error"),
            Ok(_) => {}
        }
        if query.get("deleteContents") != "true"
            && (has_children(&dir.join("tables")) || has_children(&dir.join("routines")))
        {
            return ApiResponse::error(409, "duplicate", "dataset is not empty");
        }
        if std::fs::remove_dir_all(&dir).is_err() {
            return ApiResponse::error(500, "backendError", "internal error");
        }
        self.emit_event(
            "bigquery.dataset.deleted",
            serde_json::json!({"project": project_id, "dataset": dataset_id}),
        );
        ApiResponse::no_content()
    }
}
