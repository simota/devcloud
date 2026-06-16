//! Query execution over stored table data — port of
//! `internal/services/bigquery/query_engine.rs`, plus the helpers it leans on
//! from the job layer: `pageQueryResponse` (job_handlers.rs) and
//! `normalizeTableReference`/`validateTableReference` (job_load_extract.rs),
//! which the job handlers (part 3) reuse.
//!
//! Engine errors are plain strings; the legacy handlers turn every error from
//! these paths into a 400 `invalidQuery`/`invalid` envelope (part 3 wires
//! that mapping).

use crate::model::{
    JobConfiguration, JobReference, JobResource, JobStatistics, JobStatus, QueryJobConfiguration,
    QueryJobRecord, QueryResponse, QueryStatistics, StoredRow, TableReference, TableResource,
    TableSchema,
};
use crate::responses::{dataset_etag, default_string, unix_millis_string};
use crate::server::{now_unix_nanos, Server};
use crate::sql_eval::{
    execute_parsed_query, query_result_rows_to_stored_rows, QueryExecutionResult,
};
use crate::sql_parser::{
    aggregate_dry_run_fields, bind_query_parameters, fields_for_query,
    grouped_aggregate_dry_run_fields, parse_simple_select, SimpleSelectQuery,
};
use crate::validation::{path_escape, validate_resource_id};

impl Server {
    /// legacy `createQueryJob`.
    pub fn create_query_job(
        &self,
        request_project_id: &str,
        requested_ref: &JobReference,
        mut config: QueryJobConfiguration,
        max_results: i64,
        include_configuration: bool,
        dry_run: bool,
        use_legacy_sql: bool,
    ) -> Result<QueryJobRecord, String> {
        if use_legacy_sql {
            return Err("legacy SQL is not supported; set useLegacySql to false".to_string());
        }
        let effective_query = bind_query_parameters(&config.query, &config.query_parameters)?;
        let result = self.execute_query_for_job(request_project_id, &effective_query, dry_run)?;
        if !config.destination_table.table_id.is_empty() && !dry_run {
            self.write_query_destination_table(request_project_id, &config, &result)?;
        }
        let now = now_unix_nanos();
        let mut job_id = requested_ref.job_id.trim().to_string();
        if job_id.is_empty() {
            job_id = format!("devcloud_query_{now}");
        } else {
            validate_resource_id(&job_id, "job")?;
            let existing = self
                .read_query_job(request_project_id, &job_id)
                .map_err(|err| err.to_string())?;
            if existing.is_some() {
                return Err(format!("already exists: job {request_project_id}:{job_id}"));
            }
        }
        let job_ref = JobReference {
            project_id: request_project_id.to_string(),
            job_id: job_id.clone(),
            location: default_string(requested_ref.location.clone(), self.default_location()),
        };
        let response = QueryResponse {
            kind: "bigquery#queryResponse".to_string(),
            schema: TableSchema {
                fields: result.fields.clone(),
            },
            job_reference: job_ref.clone(),
            total_rows: result.rows.len().to_string(),
            page_token: String::new(),
            rows: result.rows,
            job_complete: true,
            cache_hit: false,
        };
        let mut resource = JobResource {
            kind: "bigquery#job".to_string(),
            id: format!("{request_project_id}:{job_id}"),
            self_link: format!(
                "/bigquery/v2/projects/{}/jobs/{}",
                path_escape(request_project_id),
                path_escape(&job_id)
            ),
            job_reference: job_ref,
            configuration: JobConfiguration::default(),
            status: JobStatus {
                state: "DONE".to_string(),
            },
            statistics: JobStatistics {
                creation_time: unix_millis_string(now),
                start_time: unix_millis_string(now),
                end_time: unix_millis_string(now),
                query: QueryStatistics {
                    total_rows: response.total_rows.clone(),
                    cache_hit: false,
                    dry_run,
                },
            },
        };
        if include_configuration {
            config.use_legacy_sql = Some(false);
            resource.configuration = JobConfiguration {
                dry_run,
                query: config,
                ..Default::default()
            };
        }
        let mut job = QueryJobRecord {
            job: resource,
            response: response.clone(),
        };
        self.write_query_job(request_project_id, &job_id, &job)
            .map_err(|err| err.to_string())?;
        job.response = self.page_query_response(response, 0, max_results);
        Ok(job)
    }

    /// legacy `writeQueryDestinationTable`.
    fn write_query_destination_table(
        &self,
        request_project_id: &str,
        config: &QueryJobConfiguration,
        result: &QueryExecutionResult,
    ) -> Result<(), String> {
        let destination_ref =
            normalize_table_reference(config.destination_table.clone(), request_project_id);
        validate_table_reference(&destination_ref)?;
        let dataset = self
            .read_dataset(&destination_ref.project_id, &destination_ref.dataset_id)
            .map_err(|err| err.to_string())?;
        if dataset.is_none() {
            return Err(format!(
                "not found: dataset {}:{}",
                destination_ref.project_id, destination_ref.dataset_id
            ));
        }
        let create_disposition =
            default_string(config.create_disposition.clone(), "CREATE_IF_NEEDED");
        let write_disposition = default_string(config.write_disposition.clone(), "WRITE_EMPTY");
        if create_disposition != "CREATE_IF_NEEDED" && create_disposition != "CREATE_NEVER" {
            return Err(format!(
                "unsupported createDisposition {:?}",
                config.create_disposition
            ));
        }
        if write_disposition != "WRITE_EMPTY"
            && write_disposition != "WRITE_TRUNCATE"
            && write_disposition != "WRITE_APPEND"
        {
            return Err(format!(
                "unsupported writeDisposition {:?}",
                config.write_disposition
            ));
        }

        let destination = self
            .read_table(
                &destination_ref.project_id,
                &destination_ref.dataset_id,
                &destination_ref.table_id,
            )
            .map_err(|err| err.to_string())?;
        if destination.is_none() && create_disposition == "CREATE_NEVER" {
            return Err(format!(
                "not found: table {}:{}.{}",
                destination_ref.project_id, destination_ref.dataset_id, destination_ref.table_id
            ));
        }
        if destination.is_some() && write_disposition == "WRITE_EMPTY" {
            let existing_rows = self
                .read_rows(
                    &destination_ref.project_id,
                    &destination_ref.dataset_id,
                    &destination_ref.table_id,
                )
                .map_err(|err| err.to_string())?;
            if !existing_rows.is_empty() {
                return Err("destination table is not empty".to_string());
            }
        }
        let result_schema = TableSchema {
            fields: result.fields.clone(),
        };
        if let Some(destination) = &destination {
            if write_disposition == "WRITE_APPEND" && destination.schema != result_schema {
                return Err("destination table schema does not match query result".to_string());
            }
        }

        let now = now_unix_nanos();
        let mut destination = match destination {
            Some(destination) if write_disposition != "WRITE_TRUNCATE" => destination,
            _ => TableResource {
                kind: "bigquery#table".to_string(),
                id: format!(
                    "{}:{}.{}",
                    destination_ref.project_id,
                    destination_ref.dataset_id,
                    destination_ref.table_id
                ),
                self_link: self.table_self_link(
                    &destination_ref.project_id,
                    &destination_ref.dataset_id,
                    &destination_ref.table_id,
                ),
                table_reference: destination_ref.clone(),
                table_type: "TABLE".to_string(),
                schema: result_schema.clone(),
                etag: dataset_etag(now),
                creation_time: unix_millis_string(now),
                last_modified_time: unix_millis_string(now),
                num_rows: "0".to_string(),
                num_bytes: "0".to_string(),
                location: self.default_location().to_string(),
                ..Default::default()
            },
        };
        destination.table_type = default_string(destination.table_type, "TABLE");
        destination.schema = result_schema;
        destination.etag = dataset_etag(now);
        destination.last_modified_time = unix_millis_string(now);
        self.write_table(&destination)
            .map_err(|err| err.to_string())?;
        if write_disposition == "WRITE_TRUNCATE" {
            let rows_path = self.rows_path(
                &destination_ref.project_id,
                &destination_ref.dataset_id,
                &destination_ref.table_id,
            );
            if let Err(err) = std::fs::remove_file(&rows_path) {
                if err.kind() != std::io::ErrorKind::NotFound {
                    return Err(err.to_string());
                }
            }
        }
        let rows = query_result_rows_to_stored_rows(result);
        if !rows.is_empty() {
            self.append_rows(
                &destination_ref.project_id,
                &destination_ref.dataset_id,
                &destination_ref.table_id,
                &rows,
            )
            .map_err(|err| err.to_string())?;
        }
        self.refresh_table_row_stats(&destination)
            .map_err(|err| err.to_string())
    }

    /// legacy `executeQueryForJob`.
    fn execute_query_for_job(
        &self,
        request_project_id: &str,
        raw_query: &str,
        dry_run: bool,
    ) -> Result<QueryExecutionResult, String> {
        if dry_run {
            return self.dry_run_query_with_depth(request_project_id, raw_query, 0);
        }
        self.execute_query_with_depth(request_project_id, raw_query, 0)
    }

    /// legacy `dryRunQuery`.
    pub fn dry_run_query(
        &self,
        request_project_id: &str,
        raw_query: &str,
    ) -> Result<QueryExecutionResult, String> {
        self.dry_run_query_with_depth(request_project_id, raw_query, 0)
    }

    /// legacy `dryRunQueryWithDepth`.
    fn dry_run_query_with_depth(
        &self,
        request_project_id: &str,
        raw_query: &str,
        depth: i32,
    ) -> Result<QueryExecutionResult, String> {
        let query = parse_simple_select(raw_query, request_project_id)?;
        let (schema, _) = self.query_source(&query, true, depth)?;
        if let Some(aggregate) = &query.aggregate {
            let fields = if !query.group_by.is_empty() {
                grouped_aggregate_dry_run_fields(&schema, &query.group_by, aggregate)?
            } else {
                aggregate_dry_run_fields(&schema, aggregate)?
            };
            return Ok(QueryExecutionResult {
                fields,
                rows: Vec::new(),
            });
        }
        let fields = fields_for_query(&schema, &query.selected_fields)?;
        Ok(QueryExecutionResult {
            fields,
            rows: Vec::new(),
        })
    }

    /// legacy `executeQuery`.
    pub fn execute_query(
        &self,
        request_project_id: &str,
        raw_query: &str,
    ) -> Result<QueryExecutionResult, String> {
        self.execute_query_with_depth(request_project_id, raw_query, 0)
    }

    /// legacy `executeQueryWithDepth`.
    fn execute_query_with_depth(
        &self,
        request_project_id: &str,
        raw_query: &str,
        depth: i32,
    ) -> Result<QueryExecutionResult, String> {
        let query = parse_simple_select(raw_query, request_project_id)?;
        let (schema, rows) = self.query_source(&query, false, depth)?;
        execute_parsed_query(&schema, &rows, &query)
    }

    /// legacy `querySource`: the table's schema + rows, resolving VIEWs by
    /// executing their query (bounded recursion depth of 8).
    fn query_source(
        &self,
        query: &SimpleSelectQuery,
        dry_run: bool,
        depth: i32,
    ) -> Result<(TableSchema, Vec<StoredRow>), String> {
        let table = self
            .read_table(&query.project_id, &query.dataset_id, &query.table_id)
            .map_err(|err| err.to_string())?;
        let Some(table) = table else {
            return Err(format!(
                "not found: table {}:{}.{}",
                query.project_id, query.dataset_id, query.table_id
            ));
        };

        if table.table_type.eq_ignore_ascii_case("VIEW") {
            let view = match &table.view {
                Some(view) if !view.query.trim().is_empty() => view,
                _ => {
                    return Err(format!(
                        "view {}:{}.{} has no query",
                        query.project_id, query.dataset_id, query.table_id
                    ));
                }
            };
            if view.use_legacy_sql {
                return Err("legacy SQL views are not supported".to_string());
            }
            if depth >= 8 {
                return Err("view reference depth exceeded".to_string());
            }
            let result =
                self.execute_query_for_view(&query.project_id, &view.query, dry_run, depth + 1)?;
            let schema = TableSchema {
                fields: result.fields.clone(),
            };
            if dry_run {
                return Ok((schema, Vec::new()));
            }
            return Ok((schema, query_result_rows_to_stored_rows(&result)));
        }

        if dry_run {
            return Ok((table.schema, Vec::new()));
        }
        let rows = self
            .read_rows(&query.project_id, &query.dataset_id, &query.table_id)
            .map_err(|err| err.to_string())?;
        Ok((table.schema, rows))
    }

    /// legacy `executeQueryForView`.
    fn execute_query_for_view(
        &self,
        request_project_id: &str,
        raw_query: &str,
        dry_run: bool,
        depth: i32,
    ) -> Result<QueryExecutionResult, String> {
        if dry_run {
            return self.dry_run_query_with_depth(request_project_id, raw_query, depth);
        }
        self.execute_query_with_depth(request_project_id, raw_query, depth)
    }

    /// legacy `pageQueryResponse` (job_handlers.rs): slices `rows` to the window
    /// and sets `pageToken` to the next offset when truncated.
    pub fn page_query_response(
        &self,
        response: QueryResponse,
        offset: i64,
        max_results: i64,
    ) -> QueryResponse {
        let total = response.rows.len() as i64;
        let offset = offset.max(0).min(total);
        let max_results = if max_results <= 0 || max_results > self.max_result_rows() {
            self.max_result_rows()
        } else {
            max_results
        };
        let end = (offset + max_results).min(total);
        let mut paged = response;
        paged.rows = paged.rows[offset as usize..end as usize].to_vec();
        paged.page_token = if end < total {
            end.to_string()
        } else {
            String::new()
        };
        paged
    }
}

/// legacy `normalizeTableReference` (job_load_extract.rs).
pub(crate) fn normalize_table_reference(
    mut table_ref: TableReference,
    default_project_id: &str,
) -> TableReference {
    if table_ref.project_id.is_empty() {
        table_ref.project_id = if default_project_id.is_empty() {
            "devcloud".to_string()
        } else {
            default_project_id.to_string()
        };
    }
    table_ref
}

/// legacy `validateTableReference` (job_load_extract.rs).
pub(crate) fn validate_table_reference(table_ref: &TableReference) -> Result<(), String> {
    validate_resource_id(&table_ref.project_id, "project")?;
    validate_resource_id(&table_ref.dataset_id, "dataset")?;
    validate_resource_id(&table_ref.table_id, "table")
}
