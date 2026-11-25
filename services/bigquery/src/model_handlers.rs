//! BigQuery ML REST v2 model catalog handlers:
//! - `GET    /bigquery/v2/projects/{p}/datasets/{d}/models`
//! - `GET    /bigquery/v2/projects/{p}/datasets/{d}/models/{m}`
//! - `PATCH  /bigquery/v2/projects/{p}/datasets/{d}/models/{m}`
//! - `DELETE /bigquery/v2/projects/{p}/datasets/{d}/models/{m}`

use crate::model::{ModelResource, ModelsListResponse};
use crate::responses::{dataset_etag, unix_millis_string, ApiResponse};
use crate::server::{now_unix_nanos, Server};
use crate::validation::{decode_body, max_results_from_request, row_offset_from_request, Query};

impl Server {
    /// `GET /bigquery/v2/projects/{p}/datasets/{d}/models`
    pub fn list_models(&self, project_id: &str, dataset_id: &str, query: &Query) -> ApiResponse {
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
        let models = match self.read_models(project_id, dataset_id) {
            Ok(models) => models,
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
        let total = models.len() as i64;
        let offset = offset.min(total);
        let end = (offset + max_results).min(total);
        let response = ModelsListResponse {
            models: models[offset as usize..end as usize].to_vec(),
            next_page_token: if end < total {
                end.to_string()
            } else {
                String::new()
            },
        };
        ApiResponse::json(200, &response)
    }

    /// `GET /bigquery/v2/projects/{p}/datasets/{d}/models/{m}`
    pub fn get_model(&self, project_id: &str, dataset_id: &str, model_id: &str) -> ApiResponse {
        match self.read_model(project_id, dataset_id, model_id) {
            Err(_) => ApiResponse::error(500, "backendError", "internal error"),
            Ok(None) => ApiResponse::error(
                404,
                "notFound",
                &format!("Not found: Model {project_id}:{dataset_id}.{model_id}"),
            ),
            Ok(Some(model)) => ApiResponse::json(200, &model),
        }
    }

    /// `PATCH /bigquery/v2/projects/{p}/datasets/{d}/models/{m}`
    pub fn patch_model(
        &self,
        project_id: &str,
        dataset_id: &str,
        model_id: &str,
        body: &[u8],
    ) -> ApiResponse {
        let mut existing = match self.read_model(project_id, dataset_id, model_id) {
            Err(_) => return ApiResponse::error(500, "backendError", "internal error"),
            Ok(None) => {
                return ApiResponse::error(
                    404,
                    "notFound",
                    &format!("Not found: Model {project_id}:{dataset_id}.{model_id}"),
                )
            }
            Ok(Some(existing)) => existing,
        };
        let request: ModelResource = match decode_body(body, self.max_request_bytes()) {
            Ok(request) => request,
            Err(()) => return ApiResponse::error(400, "invalid", "invalid json request"),
        };
        if (!request.model_reference.project_id.is_empty()
            && request.model_reference.project_id != project_id)
            || (!request.model_reference.dataset_id.is_empty()
                && request.model_reference.dataset_id != dataset_id)
            || (!request.model_reference.model_id.is_empty()
                && request.model_reference.model_id != model_id)
        {
            return ApiResponse::error(400, "invalid", "modelReference must match request model");
        }
        if !request.friendly_name.is_empty() {
            existing.friendly_name = request.friendly_name;
        }
        if !request.description.is_empty() {
            existing.description = request.description;
        }
        if !request.expiration_time.is_empty() {
            existing.expiration_time = request.expiration_time;
        }
        if request.labels.is_some() {
            existing.labels = request.labels;
        }
        let now = now_unix_nanos();
        existing.etag = dataset_etag(now);
        existing.last_modified_time = unix_millis_string(now);
        if self.write_model(&existing).is_err() {
            return ApiResponse::error(500, "backendError", "internal error");
        }
        ApiResponse::json(200, &existing)
    }

    /// `DELETE /bigquery/v2/projects/{p}/datasets/{d}/models/{m}`
    pub fn delete_model(&self, project_id: &str, dataset_id: &str, model_id: &str) -> ApiResponse {
        let dir = self.model_dir(project_id, dataset_id, model_id);
        match std::fs::metadata(dir.join("model.json")) {
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                return ApiResponse::error(
                    404,
                    "notFound",
                    &format!("Not found: Model {project_id}:{dataset_id}.{model_id}"),
                )
            }
            Err(_) => return ApiResponse::error(500, "backendError", "internal error"),
            Ok(_) => {}
        }
        if std::fs::remove_dir_all(&dir).is_err() {
            return ApiResponse::error(500, "backendError", "internal error");
        }
        ApiResponse::no_content()
    }
}
