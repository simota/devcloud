//! Streaming-insert and row-listing handlers — port of
//! `internal/services/bigquery/tabledata_handlers.rs` (`insertRows`,
//! `listRows`, `queryRows`, and the insertAll row validation helpers).
//!
//! The cell-rendering helpers this file leans on (`isJSONNull`,
//! `formatRowValues`, `rawValueForResponse`, `rawValueForFieldResponse`)
//! landed with the SQL engine in `sql_eval`; the pagination parameters
//! (`rowOffsetFromRequest`, `maxResultsFromRequest`) live in `validation`.

use std::collections::{BTreeMap, HashMap, HashSet};

use crate::model::{
    InsertAllRequest, InsertAllResponse, InsertError, InsertErrorItem, JobReference,
    QueryJobConfiguration, QueryRequest, RawJson, StoredRow, TableDataListResponse, TableDataRow,
    TableFieldSchema, TableSchema,
};
use crate::responses::{dataset_etag, default_string, ApiResponse};
use crate::server::{now_unix_nanos, Server};
use crate::sql_eval::{format_row_values, is_json_null, raw_float, raw_int};
use crate::validation::{decode_body, max_results_from_request, row_offset_from_request, Query};

impl Server {
    /// `POST .../tables/{t}/insertAll` (legacy `insertRows`).
    pub fn insert_rows(
        &self,
        project_id: &str,
        dataset_id: &str,
        table_id: &str,
        body: &[u8],
    ) -> ApiResponse {
        let table = match self.read_table(project_id, dataset_id, table_id) {
            Err(_) => return ApiResponse::error(500, "backendError", "internal error"),
            Ok(None) => {
                return ApiResponse::error(
                    404,
                    "notFound",
                    &format!("Not found: Table {project_id}:{dataset_id}.{table_id}"),
                )
            }
            Ok(Some(table)) => table,
        };
        let request: InsertAllRequest = match decode_body(body, self.max_request_bytes()) {
            Ok(request) => request,
            Err(()) => return ApiResponse::error(400, "invalid", "invalid json request"),
        };
        let existing_rows = match self.read_rows(project_id, dataset_id, table_id) {
            Ok(rows) => rows,
            Err(_) => return ApiResponse::error(500, "backendError", "internal error"),
        };
        let mut seen_insert_ids: HashSet<String> = existing_rows
            .iter()
            .filter(|row| !row.insert_id.is_empty())
            .map(|row| row.insert_id.clone())
            .collect();

        let inserted_at = format_rfc3339_nanos(now_unix_nanos());
        let mut accepted: Vec<StoredRow> = Vec::with_capacity(request.rows.len());
        let mut insert_errors: Vec<InsertError> = Vec::new();
        for (i, row) in request.rows.into_iter().enumerate() {
            let (values, row_errors) =
                validate_row_json(row.json, &table.schema, request.ignore_unknown_values);
            if !row_errors.is_empty() {
                insert_errors.push(InsertError {
                    index: i as i64,
                    errors: row_errors,
                });
                continue;
            }
            if !row.insert_id.is_empty() && !seen_insert_ids.insert(row.insert_id.clone()) {
                continue; // best-effort dedup, like the real service
            }
            accepted.push(StoredRow {
                insert_id: row.insert_id,
                json: values,
                inserted_at: inserted_at.clone(),
            });
        }
        if !insert_errors.is_empty() && !request.skip_invalid_rows {
            accepted.clear();
        }
        if !accepted.is_empty() {
            if (existing_rows.len() + accepted.len()) as i64 > self.max_rows_per_table() {
                return ApiResponse::error(400, "quotaExceeded", "table row limit exceeded");
            }
            if self
                .append_rows(project_id, dataset_id, table_id, &accepted)
                .is_err()
            {
                return ApiResponse::error(500, "backendError", "internal error");
            }
            if self.refresh_table_row_stats(&table).is_err() {
                return ApiResponse::error(500, "backendError", "internal error");
            }
        }

        ApiResponse::json(
            200,
            &InsertAllResponse {
                kind: "bigquery#tableDataInsertAllResponse".to_string(),
                insert_errors,
            },
        )
    }

    /// `GET .../tables/{t}/data` (legacy `listRows`).
    pub fn list_rows(
        &self,
        project_id: &str,
        dataset_id: &str,
        table_id: &str,
        query: &Query,
    ) -> ApiResponse {
        let table = match self.read_table(project_id, dataset_id, table_id) {
            Err(_) => return ApiResponse::error(500, "backendError", "internal error"),
            Ok(None) => {
                return ApiResponse::error(
                    404,
                    "notFound",
                    &format!("Not found: Table {project_id}:{dataset_id}.{table_id}"),
                )
            }
            Ok(Some(table)) => table,
        };
        let rows = match self.read_rows(project_id, dataset_id, table_id) {
            Ok(rows) => rows,
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
        let total = rows.len() as i64;
        let offset = offset.min(total);
        let end = (offset + max_results).min(total);
        let fields = selected_row_fields(&table.schema, query.get("selectedFields"));
        let response_rows: Vec<TableDataRow> = rows[offset as usize..end as usize]
            .iter()
            .map(|row| TableDataRow {
                f: format_row_values(&row.json, &fields),
            })
            .collect();
        let response = TableDataListResponse {
            kind: "bigquery#tableDataList".to_string(),
            etag: dataset_etag(now_unix_nanos()),
            total_rows: total.to_string(),
            page_token: if end < total {
                end.to_string()
            } else {
                String::new()
            },
            rows: response_rows,
        };
        ApiResponse::json(200, &response)
    }

    /// `POST /bigquery/v2/projects/{p}/queries` (legacy `queryRows`).
    pub fn query_rows(&self, project_id: &str, body: &[u8]) -> ApiResponse {
        let request: QueryRequest = match decode_body(body, self.max_request_bytes()) {
            Ok(request) => request,
            Err(()) => return ApiResponse::error(400, "invalid", "invalid json request"),
        };
        let use_legacy_sql = self.effective_use_legacy_sql(request.use_legacy_sql);
        let job = self.create_query_job(
            project_id,
            &JobReference {
                location: request.location,
                ..Default::default()
            },
            QueryJobConfiguration {
                query: request.query,
                use_legacy_sql: request.use_legacy_sql,
                query_parameters: request.query_parameters,
                ..Default::default()
            },
            request.max_results,
            false,
            request.dry_run,
            use_legacy_sql,
        );
        match job {
            Err(message) => ApiResponse::error(400, "invalidQuery", &message),
            Ok(job) => ApiResponse::json(200, &job.response),
        }
    }
}

/// legacy `validateRowJSON`: filters the row down to schema-known, type-valid
/// values and reports per-field errors (plus missing REQUIRED fields).
pub(crate) fn validate_row_json(
    row: BTreeMap<String, RawJson>,
    schema: &TableSchema,
    ignore_unknown_values: bool,
) -> (BTreeMap<String, RawJson>, Vec<InsertErrorItem>) {
    let mut errors: Vec<InsertErrorItem> = Vec::new();
    let mut values: BTreeMap<String, RawJson> = BTreeMap::new();
    let fields_by_name: HashMap<&str, &TableFieldSchema> = schema
        .fields
        .iter()
        .map(|field| (field.name.as_str(), field))
        .collect();
    for (key, raw) in row {
        let Some(field) = fields_by_name.get(key.as_str()) else {
            if ignore_unknown_values {
                continue;
            }
            errors.push(InsertErrorItem {
                reason: "invalid".to_string(),
                location: key.clone(),
                message: format!("no such field: {key}"),
            });
            continue;
        };
        if let Err(message) = validate_field_value(raw.get(), field) {
            errors.push(InsertErrorItem {
                reason: "invalid".to_string(),
                location: key,
                message,
            });
            continue;
        }
        values.insert(key, raw);
    }
    for field in &schema.fields {
        if field.mode.eq_ignore_ascii_case("REQUIRED") {
            let present = values
                .get(&field.name)
                .map(|raw| !is_json_null(raw.get()))
                .unwrap_or(false);
            if !present {
                errors.push(InsertErrorItem {
                    reason: "invalid".to_string(),
                    location: field.name.clone(),
                    message: format!("required field {:?} is missing", field.name),
                });
            }
        }
    }
    (values, errors)
}

/// legacy `validateFieldValue`.
fn validate_field_value(raw: &str, field: &TableFieldSchema) -> Result<(), String> {
    if is_json_null(raw) {
        if field.mode.eq_ignore_ascii_case("REQUIRED") {
            return Err(format!("required field {:?} cannot be null", field.name));
        }
        return Ok(());
    }
    if field.mode.eq_ignore_ascii_case("REPEATED") {
        let values: Vec<&serde_json::value::RawValue> = match serde_json::from_str(raw) {
            Ok(values) => values,
            Err(_) => return Err(format!("field {:?} must be an array", field.name)),
        };
        let mut item_field = field.clone();
        item_field.mode = "NULLABLE".to_string();
        for value in values {
            validate_field_value(value.get(), &item_field)?;
        }
        return Ok(());
    }

    let field_type = default_string(field.field_type.clone(), "STRING").to_uppercase();
    match field_type.as_str() {
        "STRING" | "BYTES" | "NUMERIC" | "BIGNUMERIC" | "TIMESTAMP" | "DATE" | "TIME"
        | "DATETIME" | "GEOGRAPHY" => {
            if serde_json::from_str::<String>(raw).is_err() {
                return Err(format!("field {:?} must be a string", field.name));
            }
        }
        // legacy `isIntegerJSON`/`isFloatJSON`: a JSON string parsed with
        // ParseInt/ParseFloat, or a JSON number whose literal parses —
        // exactly what `raw_int`/`raw_float` implement.
        "INTEGER" | "INT64" => {
            if raw_int(raw).is_none() {
                return Err(format!("field {:?} must be an integer", field.name));
            }
        }
        "FLOAT" | "FLOAT64" => {
            if raw_float(raw).is_none() {
                return Err(format!("field {:?} must be a number", field.name));
            }
        }
        "BOOLEAN" | "BOOL" => {
            if serde_json::from_str::<bool>(raw).is_err() {
                return Err(format!("field {:?} must be a boolean", field.name));
            }
        }
        "JSON" => {}
        "RECORD" | "STRUCT" => {
            let object: BTreeMap<String, RawJson> = match serde_json::from_str(raw) {
                Ok(object) => object,
                Err(_) => return Err(format!("field {:?} must be an object", field.name)),
            };
            let schema = TableSchema {
                fields: field.fields.clone(),
            };
            let (_, errors) = validate_row_json(object, &schema, false);
            if !errors.is_empty() {
                return Err(format!("field {:?} has invalid nested values", field.name));
            }
        }
        _ => return Err(format!("unsupported field type {:?}", field.field_type)),
    }
    Ok(())
}

/// legacy `selectedRowFields`.
fn selected_row_fields(schema: &TableSchema, selected_fields: &str) -> Vec<TableFieldSchema> {
    if selected_fields.trim().is_empty() {
        return schema.fields.clone();
    }
    let allowed: HashSet<&str> = selected_fields
        .split(',')
        .map(str::trim)
        .filter(|name| !name.is_empty())
        .collect();
    schema
        .fields
        .iter()
        .filter(|field| allowed.contains(field.name.as_str()))
        .cloned()
        .collect()
}

/// legacy `time.Now().UTC().Format(time.RFC3339Nano)`: trailing zeros trimmed
/// from the fraction, fraction omitted entirely when zero, `Z` suffix.
pub(crate) fn format_rfc3339_nanos(unix_nanos: i64) -> String {
    let secs = unix_nanos.div_euclid(1_000_000_000);
    let nanos = unix_nanos.rem_euclid(1_000_000_000);
    let days = secs.div_euclid(86_400);
    let secs_of_day = secs.rem_euclid(86_400);
    let (year, month, day) = civil_from_days(days);
    let (hour, minute, second) = (
        secs_of_day / 3_600,
        (secs_of_day / 60) % 60,
        secs_of_day % 60,
    );
    let mut out = format!("{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}");
    if nanos > 0 {
        out.push('.');
        let frac = format!("{nanos:09}");
        out.push_str(frac.trim_end_matches('0'));
    }
    out.push('Z');
    out
}

/// Days since 1970-01-01 → (year, month, day), Howard Hinnant's algorithm.
fn civil_from_days(days: i64) -> (i64, i64, i64) {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let year = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    (if month <= 2 { year + 1 } else { year }, month, day)
}

#[cfg(test)]
mod tests {
    use super::format_rfc3339_nanos;

    #[test]
    fn rfc3339_nano_matches_legacy_formatting() {
        // legacy: time.Unix(0, 1700000000123450000).UTC().Format(time.RFC3339Nano)
        assert_eq!(
            format_rfc3339_nanos(1_700_000_000_123_450_000),
            "2023-11-14T22:13:20.12345Z"
        );
        // Whole seconds drop the fraction entirely.
        assert_eq!(format_rfc3339_nanos(0), "1970-01-01T00:00:00Z");
        assert_eq!(
            format_rfc3339_nanos(1_700_000_000_000_000_000),
            "2023-11-14T22:13:20Z"
        );
    }
}
