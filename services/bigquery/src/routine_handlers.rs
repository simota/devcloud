//! Routine CRUD handlers — port of
//! `internal/services/bigquery/routine_handlers.rs`.

use crate::model::{RoutineReference, RoutineResource, RoutinesListResponse};
use crate::responses::{dataset_etag, unix_millis_string, ApiResponse};
use crate::server::{now_unix_nanos, Server};
use crate::validation::{
    decode_body, max_results_from_request, row_offset_from_request, validate_routine_resource,
    Query,
};

impl Server {
    /// `POST /bigquery/v2/projects/{p}/datasets/{d}/routines`
    /// (legacy `createRoutine`).
    pub fn create_routine(&self, project_id: &str, dataset_id: &str, body: &[u8]) -> ApiResponse {
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

        let mut request: RoutineResource = match decode_body(body, self.max_request_bytes()) {
            Ok(request) => request,
            Err(()) => return ApiResponse::error(400, "invalid", "invalid json request"),
        };
        if request.routine_reference.project_id.is_empty() {
            request.routine_reference.project_id = project_id.to_string();
        }
        if request.routine_reference.dataset_id.is_empty() {
            request.routine_reference.dataset_id = dataset_id.to_string();
        }
        if request.routine_reference.project_id != project_id
            || request.routine_reference.dataset_id != dataset_id
        {
            return ApiResponse::error(
                400,
                "invalid",
                "routineReference must match request project and dataset",
            );
        }
        if let Err(message) = validate_routine_resource(&request) {
            return ApiResponse::error(400, "invalid", &message);
        }

        let routine_id = request.routine_reference.routine_id.clone();
        let path = self.routine_path(project_id, dataset_id, &routine_id);
        match std::fs::metadata(&path) {
            Ok(_) => {
                return ApiResponse::error(
                    409,
                    "duplicate",
                    &format!("Already Exists: Routine {project_id}:{dataset_id}.{routine_id}"),
                )
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(_) => return ApiResponse::error(500, "backendError", "internal error"),
        }

        let now = now_unix_nanos();
        let mut resource = request;
        resource.kind = "bigquery#routine".to_string();
        resource.id = format!("{project_id}:{dataset_id}.{routine_id}");
        resource.self_link = self.routine_self_link(project_id, dataset_id, &routine_id);
        resource.etag = dataset_etag(now);
        resource.creation_time = unix_millis_string(now);
        resource.last_modified_time = unix_millis_string(now);
        if self.write_routine(&resource).is_err() {
            return ApiResponse::error(500, "backendError", "internal error");
        }
        ApiResponse::json(200, &resource)
    }

    /// `GET /bigquery/v2/projects/{p}/datasets/{d}/routines/{r}`
    /// (legacy `getRoutine`).
    pub fn get_routine(&self, project_id: &str, dataset_id: &str, routine_id: &str) -> ApiResponse {
        match self.read_routine(project_id, dataset_id, routine_id) {
            Err(_) => ApiResponse::error(500, "backendError", "internal error"),
            Ok(None) => ApiResponse::error(
                404,
                "notFound",
                &format!("Not found: Routine {project_id}:{dataset_id}.{routine_id}"),
            ),
            Ok(Some(resource)) => ApiResponse::json(200, &resource),
        }
    }

    /// `GET /bigquery/v2/projects/{p}/datasets/{d}/routines`
    /// (legacy `listRoutines`).
    pub fn list_routines(&self, project_id: &str, dataset_id: &str, query: &Query) -> ApiResponse {
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
        let routines = match self.read_routines(project_id, dataset_id) {
            Ok(routines) => routines,
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
        let total = routines.len() as i64;
        let offset = offset.min(total);
        let end = (offset + max_results).min(total);
        let response = RoutinesListResponse {
            kind: "bigquery#routineList".to_string(),
            routines: routines[offset as usize..end as usize].to_vec(),
            total_items: total,
            next_page_token: if end < total {
                end.to_string()
            } else {
                String::new()
            },
        };
        ApiResponse::json(200, &response)
    }

    /// `PATCH`/`PUT /bigquery/v2/projects/{p}/datasets/{d}/routines/{r}`
    /// (legacy `patchRoutine`; `replace` distinguishes PUT from PATCH).
    pub fn patch_routine(
        &self,
        project_id: &str,
        dataset_id: &str,
        routine_id: &str,
        replace: bool,
        body: &[u8],
    ) -> ApiResponse {
        let mut existing = match self.read_routine(project_id, dataset_id, routine_id) {
            Err(_) => return ApiResponse::error(500, "backendError", "internal error"),
            Ok(None) => {
                return ApiResponse::error(
                    404,
                    "notFound",
                    &format!("Not found: Routine {project_id}:{dataset_id}.{routine_id}"),
                )
            }
            Ok(Some(existing)) => existing,
        };
        let request: RoutineResource = match decode_body(body, self.max_request_bytes()) {
            Ok(request) => request,
            Err(()) => return ApiResponse::error(400, "invalid", "invalid json request"),
        };
        if !request.routine_reference.project_id.is_empty()
            && request.routine_reference.project_id != project_id
            || !request.routine_reference.dataset_id.is_empty()
                && request.routine_reference.dataset_id != dataset_id
            || !request.routine_reference.routine_id.is_empty()
                && request.routine_reference.routine_id != routine_id
        {
            return ApiResponse::error(
                400,
                "invalid",
                "routineReference must match request routine",
            );
        }

        let now = now_unix_nanos();
        if replace {
            let creation_time = existing.creation_time.clone();
            existing = request;
            existing.kind = "bigquery#routine".to_string();
            existing.id = format!("{project_id}:{dataset_id}.{routine_id}");
            existing.self_link = self.routine_self_link(project_id, dataset_id, routine_id);
            existing.routine_reference = RoutineReference {
                project_id: project_id.to_string(),
                dataset_id: dataset_id.to_string(),
                routine_id: routine_id.to_string(),
            };
            existing.creation_time = creation_time;
        } else {
            if !request.routine_type.is_empty() {
                existing.routine_type = request.routine_type;
            }
            if !request.language.is_empty() {
                existing.language = request.language;
            }
            // legacy gates on `!= nil`; with plain Vecs, an explicit `[]` patch
            // (which clears in legacy) is indistinguishable from an absent field.
            if !request.arguments.is_empty() {
                existing.arguments = request.arguments;
            }
            if request.return_type.is_some() {
                existing.return_type = request.return_type;
            }
            if !request.definition_body.is_empty() {
                existing.definition_body = request.definition_body;
            }
            if !request.description.is_empty() {
                existing.description = request.description;
            }
            if !request.determinism_level.is_empty() {
                existing.determinism_level = request.determinism_level;
            }
            if !request.imported_libraries.is_empty() {
                existing.imported_libraries = request.imported_libraries;
            }
        }
        if let Err(message) = validate_routine_resource(&existing) {
            return ApiResponse::error(400, "invalid", &message);
        }
        existing.etag = dataset_etag(now);
        existing.last_modified_time = unix_millis_string(now);
        if self.write_routine(&existing).is_err() {
            return ApiResponse::error(500, "backendError", "internal error");
        }
        ApiResponse::json(200, &existing)
    }

    /// `DELETE /bigquery/v2/projects/{p}/datasets/{d}/routines/{r}`
    /// (legacy `deleteRoutine`).
    pub fn delete_routine(
        &self,
        project_id: &str,
        dataset_id: &str,
        routine_id: &str,
    ) -> ApiResponse {
        let dir = self.routine_dir(project_id, dataset_id, routine_id);
        match std::fs::metadata(dir.join("routine.json")) {
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                return ApiResponse::error(
                    404,
                    "notFound",
                    &format!("Not found: Routine {project_id}:{dataset_id}.{routine_id}"),
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
