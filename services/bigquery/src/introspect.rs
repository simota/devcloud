//! Read-only introspection API — port of
//! `internal/services/bigquery/introspect.rs`.
//!
//! CONVENTION (reused verbatim across every service):
//!   - All introspection routes live under the `/_introspect/` prefix and are
//!     intercepted at the top of [`crate::routes::handle`], BEFORE the
//!     provider-protocol dispatch.
//!   - Methods are GET-only and read-only. No mutation endpoints here.
//!   - Each response body is the exact JSON encoding of the same snapshot
//!     struct the dashboard already serializes in-process (`dashboard.rs`).
//!   - A missing resource returns 404; an unsupported method returns 405.
//!   - Auth gating matches the rest of the server (`authorize()` runs before
//!     this in `handle()`), so relaxed mode stays open and strict mode is
//!     honored.

use crate::responses::ApiResponse;
use crate::routes::Request;
use crate::server::Server;

/// legacy `introspectPrefix`.
const INTROSPECT_PREFIX: &str = "/_introspect/";

/// legacy `isIntrospectPath`.
pub(crate) fn is_introspect_path(path: &str) -> bool {
    path.starts_with(INTROSPECT_PREFIX)
}

/// legacy `handleIntrospect`.
///
/// ```text
/// GET /_introspect/projects                                                          -> Snapshot()
/// GET /_introspect/projects/{projectId}/datasets                                     -> Snapshot().Datasets for projectId
/// GET /_introspect/projects/{projectId}/datasets/{datasetId}                         -> DatasetSnapshot(projectId, datasetId)
/// GET /_introspect/projects/{projectId}/datasets/{datasetId}/tables/{tableId}        -> TableSnapshot(projectId, datasetId, tableId, 0)
/// GET /_introspect/projects/{projectId}/datasets/{datasetId}/tables/{tableId}/rows   -> TableSnapshot(projectId, datasetId, tableId, limit)
/// GET /_introspect/projects/{projectId}/jobs                                         -> Snapshot().Jobs for projectId
/// GET /_introspect/projects/{projectId}/jobs/{jobId}                                 -> JobSnapshot(projectId, jobId)
/// ```
pub(crate) fn handle_introspect(server: &Server, req: &Request) -> ApiResponse {
    if req.method != "GET" {
        // legacy: writeError(405, "methodNotAllowed", "introspection endpoints are
        // read-only") with `Allow: GET`.
        let mut response = ApiResponse::error(
            405,
            "methodNotAllowed",
            "introspection endpoints are read-only",
        );
        response.allow = Some("GET".to_string());
        return response;
    }

    let rest = req
        .path
        .strip_prefix(INTROSPECT_PREFIX)
        .unwrap_or(&req.path);

    // GET /_introspect/projects
    if rest == "projects" {
        return ApiResponse::json(200, &server.snapshot());
    }

    // All remaining paths start with "projects/{projectId}/..."
    let Some(after_projects) = rest.strip_prefix("projects/") else {
        return not_found();
    };
    let (project_id, sub_path) = match after_projects.split_once('/') {
        Some((project_id, sub_path)) => (project_id, sub_path),
        // No trailing path — not a valid introspection endpoint at this level.
        None => return not_found(),
    };
    if project_id.is_empty() {
        return not_found();
    }

    // GET /_introspect/projects/{projectId}/datasets
    if sub_path == "datasets" {
        let snap = server.snapshot();
        if snap.project != project_id {
            return ApiResponse::error(404, "notFound", "project not found");
        }
        return ApiResponse::json(200, &snap.datasets);
    }

    // GET /_introspect/projects/{projectId}/datasets/{datasetId}[/...]
    if let Some(dataset_rest) = sub_path.strip_prefix("datasets/") {
        return handle_introspect_dataset_path(server, req, project_id, dataset_rest);
    }

    // GET /_introspect/projects/{projectId}/jobs
    if sub_path == "jobs" {
        let snap = server.snapshot();
        if snap.project != project_id {
            return ApiResponse::error(404, "notFound", "project not found");
        }
        return ApiResponse::json(200, &snap.jobs);
    }

    // GET /_introspect/projects/{projectId}/jobs/{jobId}
    if let Some(job_id) = sub_path.strip_prefix("jobs/") {
        if job_id.contains('/') || job_id.is_empty() {
            return not_found();
        }
        return match server.job_snapshot(project_id, job_id) {
            Some(job) => ApiResponse::json(200, &job),
            None => ApiResponse::error(404, "notFound", "job not found"),
        };
    }

    not_found()
}

/// legacy `handleIntrospectDatasetPath`: dataset sub-paths under
/// `/_introspect/projects/{projectId}/datasets/{rest}`.
fn handle_introspect_dataset_path(
    server: &Server,
    req: &Request,
    project_id: &str,
    rest: &str,
) -> ApiResponse {
    let (dataset_id, after_dataset) = match rest.split_once('/') {
        Some((dataset_id, after_dataset)) => (dataset_id, Some(after_dataset)),
        None => (rest, None),
    };
    if dataset_id.is_empty() {
        return not_found();
    }

    let Some(after_dataset) = after_dataset else {
        // GET /_introspect/projects/{projectId}/datasets/{datasetId}
        return match server.dataset_snapshot(project_id, dataset_id) {
            Some(dataset) => ApiResponse::json(200, &dataset),
            None => ApiResponse::error(404, "notFound", "dataset not found"),
        };
    };

    let Some(table_rest) = after_dataset.strip_prefix("tables/") else {
        return not_found();
    };

    let (table_id, after_table) = match table_rest.split_once('/') {
        Some((table_id, after_table)) => (table_id, Some(after_table)),
        None => (table_rest, None),
    };
    if table_id.is_empty() {
        return not_found();
    }

    let Some(after_table) = after_table else {
        // GET /_introspect/projects/{projectId}/datasets/{datasetId}/tables/{tableId}
        return match server.table_snapshot_for(project_id, dataset_id, table_id, 0) {
            Some(table) => ApiResponse::json(200, &table),
            None => ApiResponse::error(404, "notFound", "table not found"),
        };
    };

    if after_table == "rows" {
        // GET /_introspect/projects/{projectId}/datasets/{datasetId}/tables/{tableId}/rows
        let mut limit = 100usize;
        let lv = req.query.get("limit");
        if !lv.is_empty() {
            if let Ok(n) = lv.parse::<i64>() {
                if n > 0 {
                    limit = n as usize;
                }
            }
        }
        return match server.table_snapshot_for(project_id, dataset_id, table_id, limit) {
            Some(table) => ApiResponse::json(200, &table),
            None => ApiResponse::error(404, "notFound", "table not found"),
        };
    }

    not_found()
}

/// legacy `writeError(w, http.StatusNotFound, "notFound", "introspection endpoint not found")`.
fn not_found() -> ApiResponse {
    ApiResponse::error(404, "notFound", "introspection endpoint not found")
}
