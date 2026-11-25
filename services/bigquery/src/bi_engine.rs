//! BigQuery BI Engine local subset:
//! - Reservation API v1 singleton management:
//!   - `GET   /v1/projects/{project}/locations/{location}/biReservation`
//!   - `PATCH /v1/projects/{project}/locations/{location}/biReservation`
//! - Query job `statistics.query.biEngineStatistics` reporting when a project +
//!   location has a BI Engine reservation with `size > 0`.
//!
//! Real in-memory columnar caching and query latency acceleration are not
//! emulated; this provides deterministic reservation persistence and job
//! statistics metadata compatibility.

use crate::model::{BiEngineReason, BiEngineStatistics, BiReservation};
use crate::responses::ApiResponse;
use crate::server::{now_unix_nanos, Server};
use crate::sql_parser::FromSource;
use crate::tabledata_handlers::format_rfc3339_nanos;
use crate::validation::{decode_body, validate_resource_id, Query};

impl Server {
    /// `GET /v1/projects/{project}/locations/{location}/biReservation`
    pub fn get_bi_reservation(&self, project_id: &str, location: &str) -> ApiResponse {
        if let Err(message) = validate_resource_id(project_id, "project") {
            return ApiResponse::error(400, "invalid", &message);
        }
        if let Err(message) = validate_resource_id(location, "location") {
            return ApiResponse::error(400, "invalid", &message);
        }
        match self.read_bi_reservation(project_id, location) {
            Err(_) => ApiResponse::error(500, "backendError", "internal error"),
            Ok(Some(res)) => ApiResponse::json(200, &res),
            Ok(None) => ApiResponse::json(200, &default_bi_reservation(project_id, location)),
        }
    }

    /// `PATCH /v1/projects/{project}/locations/{location}/biReservation`
    pub fn update_bi_reservation(
        &self,
        project_id: &str,
        location: &str,
        query: &Query,
        body: &[u8],
    ) -> ApiResponse {
        if let Err(message) = validate_resource_id(project_id, "project") {
            return ApiResponse::error(400, "invalid", &message);
        }
        if let Err(message) = validate_resource_id(location, "location") {
            return ApiResponse::error(400, "invalid", &message);
        }

        let request: BiReservation = match decode_body(body, self.max_request_bytes()) {
            Ok(request) => request,
            Err(()) => return ApiResponse::error(400, "invalid", "invalid json request"),
        };

        let expected_name = bi_reservation_name(project_id, location);
        if !request.name.is_empty() && request.name != expected_name {
            return ApiResponse::error(
                400,
                "invalid",
                "biReservation name must match request project and location",
            );
        }

        let mut existing = match self.read_bi_reservation(project_id, location) {
            Err(_) => return ApiResponse::error(500, "backendError", "internal error"),
            Ok(Some(res)) => res,
            Ok(None) => default_bi_reservation(project_id, location),
        };

        let mask = parse_update_mask(query.get("updateMask"));
        let update_size = mask.is_empty() || mask.iter().any(|f| f == "size");
        let update_preferred = mask.is_empty()
            || mask
                .iter()
                .any(|f| f == "preferred_tables" || f == "preferredtables");

        if !mask.is_empty() {
            for field in &mask {
                if field != "size" && field != "preferred_tables" && field != "preferredtables" {
                    return ApiResponse::error(
                        400,
                        "invalid",
                        &format!("unsupported updateMask field {field:?}"),
                    );
                }
            }
        }

        if update_size {
            let size_str = if request.size.trim().is_empty() {
                "0"
            } else {
                request.size.trim()
            };
            match size_str.parse::<i64>() {
                Ok(n) if n >= 0 => existing.size = n.to_string(),
                _ => {
                    return ApiResponse::error(
                        400,
                        "invalid",
                        "biReservation size must be a non-negative integer",
                    )
                }
            }
        }

        if update_preferred {
            let mut normalized = Vec::with_capacity(request.preferred_tables.len());
            for mut table_ref in request.preferred_tables {
                if table_ref.project_id.trim().is_empty() {
                    table_ref.project_id = project_id.to_string();
                }
                if let Err(message) = validate_resource_id(&table_ref.project_id, "project") {
                    return ApiResponse::error(400, "invalid", &message);
                }
                if let Err(message) = validate_resource_id(&table_ref.dataset_id, "dataset") {
                    return ApiResponse::error(400, "invalid", &message);
                }
                if let Err(message) = validate_resource_id(&table_ref.table_id, "table") {
                    return ApiResponse::error(400, "invalid", &message);
                }
                normalized.push(table_ref);
            }
            existing.preferred_tables = normalized;
        }

        existing.name = expected_name;
        existing.update_time = format_rfc3339_nanos(now_unix_nanos());

        if self
            .write_bi_reservation(project_id, location, &existing)
            .is_err()
        {
            return ApiResponse::error(500, "backendError", "internal error");
        }
        ApiResponse::json(200, &existing)
    }

    /// Computes `statistics.query.biEngineStatistics` when a BI Engine
    /// reservation with `size > 0` is configured for `(project_id, location)`.
    /// Returns `None` when no active reservation exists so existing query job
    /// payloads remain unchanged.
    pub(crate) fn compute_bi_engine_statistics(
        &self,
        project_id: &str,
        location: &str,
        source: Option<&FromSource>,
    ) -> Option<BiEngineStatistics> {
        let reservation = self.read_bi_reservation(project_id, location).ok()??;
        let size: i64 = reservation.size.trim().parse().ok()?;
        if size <= 0 {
            return None;
        }
        match source {
            Some(FromSource::Table {
                project_id: table_project,
                dataset_id,
                table_id,
            }) => {
                if reservation.preferred_tables.is_empty() {
                    Some(BiEngineStatistics {
                        bi_engine_mode: "FULL".to_string(),
                        acceleration_mode: "BI_ENGINE".to_string(),
                        bi_engine_reasons: Vec::new(),
                    })
                } else {
                    let matched = reservation.preferred_tables.iter().any(|t| {
                        t.project_id == *table_project
                            && t.dataset_id == *dataset_id
                            && t.table_id == *table_id
                    });
                    if matched {
                        Some(BiEngineStatistics {
                            bi_engine_mode: "FULL".to_string(),
                            acceleration_mode: "BI_ENGINE_PREFERRED".to_string(),
                            bi_engine_reasons: Vec::new(),
                        })
                    } else {
                        Some(BiEngineStatistics {
                            bi_engine_mode: "DISABLED".to_string(),
                            acceleration_mode: String::new(),
                            bi_engine_reasons: vec![BiEngineReason {
                                code: "OTHER".to_string(),
                                message: "Table is not in BI Engine preferredTables".to_string(),
                            }],
                        })
                    }
                }
            }
            _ => Some(BiEngineStatistics {
                bi_engine_mode: "DISABLED".to_string(),
                acceleration_mode: String::new(),
                bi_engine_reasons: vec![BiEngineReason {
                    code: "UNSUPPORTED_SQL_TEXT".to_string(),
                    message: "Query shape is not eligible for BI Engine acceleration".to_string(),
                }],
            }),
        }
    }
}

fn bi_reservation_name(project_id: &str, location: &str) -> String {
    format!("projects/{project_id}/locations/{location}/biReservation")
}

fn default_bi_reservation(project_id: &str, location: &str) -> BiReservation {
    BiReservation {
        name: bi_reservation_name(project_id, location),
        update_time: String::new(),
        size: "0".to_string(),
        preferred_tables: Vec::new(),
    }
}

fn parse_update_mask(raw: &str) -> Vec<String> {
    raw.split(',')
        .map(|s| s.trim().to_ascii_lowercase())
        .filter(|s| !s.is_empty())
        .collect()
}
