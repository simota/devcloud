//! Dataset/table IAM policy stubs — port of
//! `internal/services/bigquery/iam_handlers.rs`.
//!
//! The legacy handlers take the already-split `:action` suffix from the routing
//! layer; these take the same parameters plus the request method (legacy checks
//! it inside `handleIAMPolicy`).

use std::path::PathBuf;

use crate::model::{SetIamPolicyRequest, TestIamPermissionsRequest, TestIamPermissionsResponse};
use crate::responses::{normalize_iam_policy, ApiResponse};
use crate::server::{now_unix_nanos, Server};
use crate::validation::decode_body;

impl Server {
    /// legacy `handleDatasetIAMPolicy`.
    pub fn handle_dataset_iam_policy(
        &self,
        method: &str,
        project_id: &str,
        dataset_id: &str,
        action: &str,
        body: &[u8],
    ) -> ApiResponse {
        match self.read_dataset(project_id, dataset_id) {
            Err(_) => ApiResponse::error(500, "backendError", "internal error"),
            Ok(None) => ApiResponse::error(
                404,
                "notFound",
                &format!("Not found: Dataset {project_id}:{dataset_id}"),
            ),
            Ok(Some(_)) => self.handle_iam_policy(
                method,
                action,
                self.dataset_iam_policy_path(project_id, dataset_id),
                body,
            ),
        }
    }

    /// legacy `handleTableIAMPolicy`.
    pub fn handle_table_iam_policy(
        &self,
        method: &str,
        project_id: &str,
        dataset_id: &str,
        table_id: &str,
        action: &str,
        body: &[u8],
    ) -> ApiResponse {
        match self.read_table(project_id, dataset_id, table_id) {
            Err(_) => ApiResponse::error(500, "backendError", "internal error"),
            Ok(None) => ApiResponse::error(
                404,
                "notFound",
                &format!("Not found: Table {project_id}:{dataset_id}.{table_id}"),
            ),
            Ok(Some(_)) => self.handle_iam_policy(
                method,
                action,
                self.table_iam_policy_path(project_id, dataset_id, table_id),
                body,
            ),
        }
    }

    /// legacy `handleIAMPolicy`.
    fn handle_iam_policy(
        &self,
        method: &str,
        action: &str,
        path: PathBuf,
        body: &[u8],
    ) -> ApiResponse {
        if method != "POST" {
            return ApiResponse::method_not_allowed("POST");
        }
        match action {
            "getIamPolicy" => match self.read_iam_policy(&path) {
                Err(_) => ApiResponse::error(500, "backendError", "internal error"),
                Ok(policy) => ApiResponse::json(200, &policy),
            },
            "setIamPolicy" => {
                let request: SetIamPolicyRequest = match decode_body(body, self.max_request_bytes())
                {
                    Ok(request) => request,
                    Err(()) => return ApiResponse::error(400, "invalid", "invalid json request"),
                };
                let policy = normalize_iam_policy(request.policy, now_unix_nanos());
                match self.write_iam_policy(&path, &policy) {
                    Err(_) => ApiResponse::error(500, "backendError", "internal error"),
                    Ok(()) => ApiResponse::json(200, &policy),
                }
            }
            "testIamPermissions" => {
                let request: TestIamPermissionsRequest =
                    match decode_body(body, self.max_request_bytes()) {
                        Ok(request) => request,
                        Err(()) => {
                            return ApiResponse::error(400, "invalid", "invalid json request")
                        }
                    };
                ApiResponse::json(
                    200,
                    &TestIamPermissionsResponse {
                        permissions: request.permissions,
                    },
                )
            }
            _ => ApiResponse::error(404, "notFound", "not found"),
        }
    }
}
