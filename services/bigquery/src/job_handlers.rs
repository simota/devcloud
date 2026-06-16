//! Job handlers — port of `internal/services/bigquery/job_handlers.rs` plus
//! the inline job GET from legacy `handleJobs` (routes.rs, 3-part path).
//!
//! `insertJob` dispatches on the populated configuration exactly like legacy;
//! the copy/load/extract arms execute through `job_load_extract`.

use crate::model::{JobCancelResponse, JobInsertRequest, JobResource, JobsListResponse};
use crate::responses::ApiResponse;
use crate::server::Server;
use crate::validation::{
    decode_body, max_results_from_request, row_offset_from_request, validate_resource_id, Query,
};

impl Server {
    /// `POST /bigquery/v2/projects/{p}/jobs` (legacy `insertJob`).
    pub fn insert_job(&self, project_id: &str, body: &[u8]) -> ApiResponse {
        let request: JobInsertRequest = match decode_body(body, self.max_request_bytes()) {
            Ok(request) => request,
            Err(()) => return ApiResponse::error(400, "invalid", "invalid json request"),
        };
        if !request.job_reference.project_id.is_empty()
            && request.job_reference.project_id != project_id
        {
            return ApiResponse::error(
                400,
                "invalid",
                "jobReference.projectId must match request project",
            );
        }
        let config = request.configuration;
        if !config.query.query.trim().is_empty() {
            let use_legacy_sql = self.effective_use_legacy_sql(config.query.use_legacy_sql);
            return match self.create_query_job(
                project_id,
                &request.job_reference,
                config.query,
                0,
                true,
                config.dry_run,
                use_legacy_sql,
            ) {
                Err(message) => ApiResponse::error(400, "invalidQuery", &message),
                Ok(job) => {
                    self.emit_job_inserted(project_id, "query");
                    ApiResponse::json(200, &job.job)
                }
            };
        }
        if !config.copy.destination_table.table_id.is_empty() {
            return match self.create_copy_job(project_id, &request.job_reference, config.copy) {
                Err(message) => ApiResponse::error(400, "invalid", &message),
                Ok(job) => {
                    self.emit_job_inserted(project_id, "copy");
                    ApiResponse::json(200, &job.job)
                }
            };
        }
        if !config.load.destination_table.table_id.is_empty() {
            return match self.create_load_job(project_id, &request.job_reference, config.load) {
                Err(message) => ApiResponse::error(400, "invalid", &message),
                Ok(job) => {
                    self.emit_job_inserted(project_id, "load");
                    ApiResponse::json(200, &job.job)
                }
            };
        }
        if !config.extract.source_table.table_id.is_empty() {
            return match self.create_extract_job(project_id, &request.job_reference, config.extract)
            {
                Err(message) => ApiResponse::error(400, "invalid", &message),
                Ok(job) => {
                    self.emit_job_inserted(project_id, "extract");
                    ApiResponse::json(200, &job.job)
                }
            };
        }
        ApiResponse::error(
            400,
            "invalid",
            "configuration.query.query, configuration.copy, configuration.load, or configuration.extract is required",
        )
    }

    /// legacy `events.Emit(... "bigquery.job.inserted" ...)` in `insertJob`.
    fn emit_job_inserted(&self, project_id: &str, job_type: &str) {
        self.emit_event(
            "bigquery.job.inserted",
            serde_json::json!({"project": project_id, "jobType": job_type}),
        );
    }

    /// `GET /bigquery/v2/projects/{p}/jobs/{j}` — the inline handler in legacy
    /// `handleJobs` (routes.rs). Note: legacy does not `validateResourceID` the
    /// job id on this path; parity kept.
    pub fn get_job(&self, project_id: &str, job_id: &str) -> ApiResponse {
        match self.read_query_job(project_id, job_id) {
            Err(_) => ApiResponse::error(500, "backendError", "internal error"),
            Ok(None) => ApiResponse::error(
                404,
                "notFound",
                &format!("Not found: Job {project_id}:{job_id}"),
            ),
            Ok(Some(job)) => ApiResponse::json(200, &job.job),
        }
    }

    /// `GET /bigquery/v2/projects/{p}/jobs` (legacy `listJobs`).
    pub fn list_jobs(&self, project_id: &str, query: &Query) -> ApiResponse {
        let jobs = match self.read_query_job_records(project_id) {
            Ok(jobs) => jobs,
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
        let total = jobs.len() as i64;
        let offset = offset.min(total);
        let end = (offset + max_results).min(total);
        let items: Vec<JobResource> = jobs[offset as usize..end as usize]
            .iter()
            .map(|job| job.job.clone())
            .collect();
        let response = JobsListResponse {
            kind: "bigquery#jobList".to_string(),
            jobs: items,
            next_page_token: if end < total {
                end.to_string()
            } else {
                String::new()
            },
        };
        ApiResponse::json(200, &response)
    }

    /// `POST /bigquery/v2/projects/{p}/jobs/{j}/cancel` (legacy `cancelJob`).
    /// Every devcloud job is already DONE, so cancel just echoes it.
    pub fn cancel_job(&self, project_id: &str, job_id: &str) -> ApiResponse {
        if let Err(message) = validate_resource_id(job_id, "job") {
            return ApiResponse::error(400, "invalid", &message);
        }
        match self.read_query_job(project_id, job_id) {
            Err(_) => ApiResponse::error(500, "backendError", "internal error"),
            Ok(None) => ApiResponse::error(
                404,
                "notFound",
                &format!("Not found: Job {project_id}:{job_id}"),
            ),
            Ok(Some(job)) => ApiResponse::json(
                200,
                &JobCancelResponse {
                    kind: "bigquery#jobCancelResponse".to_string(),
                    job: job.job,
                },
            ),
        }
    }

    /// `DELETE /bigquery/v2/projects/{p}/jobs/{j}[/delete]`
    /// (legacy `deleteJobMetadata`).
    pub fn delete_job_metadata(&self, project_id: &str, job_id: &str) -> ApiResponse {
        if let Err(message) = validate_resource_id(job_id, "job") {
            return ApiResponse::error(400, "invalid", &message);
        }
        let path = self.query_job_path(project_id, job_id);
        match std::fs::remove_file(&path) {
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => ApiResponse::error(
                404,
                "notFound",
                &format!("Not found: Job {project_id}:{job_id}"),
            ),
            Err(_) => ApiResponse::error(500, "backendError", "internal error"),
            Ok(()) => ApiResponse::no_content(),
        }
    }

    /// `GET /bigquery/v2/projects/{p}/queries/{j}` and
    /// `GET /bigquery/v2/projects/{p}/jobs/{j}/getQueryResults`
    /// (legacy `getQueryResults`, mounted on both paths).
    pub fn get_query_results(&self, project_id: &str, job_id: &str, query: &Query) -> ApiResponse {
        if let Err(message) = validate_resource_id(job_id, "job") {
            return ApiResponse::error(400, "invalid", &message);
        }
        let job = match self.read_query_job(project_id, job_id) {
            Err(_) => return ApiResponse::error(500, "backendError", "internal error"),
            Ok(None) => {
                return ApiResponse::error(
                    404,
                    "notFound",
                    &format!("Not found: Job {project_id}:{job_id}"),
                )
            }
            Ok(Some(job)) => job,
        };
        let offset = match row_offset_from_request(query) {
            Ok(offset) => offset,
            Err(message) => return ApiResponse::error(400, "invalid", &message),
        };
        let max_results = match max_results_from_request(query) {
            Ok(max_results) => max_results,
            Err(message) => return ApiResponse::error(400, "invalid", &message),
        };
        ApiResponse::json(
            200,
            &self.page_query_response(job.response, offset, max_results),
        )
    }
}
