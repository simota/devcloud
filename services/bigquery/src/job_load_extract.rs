//! Load/extract/copy job execution — port of
//! `internal/services/bigquery/job_load_extract.rs` plus
//! `createCompletedJob` from `job_handlers.rs`.
//!
//! Load and extract jobs read/write `gs://` URIs through the shared S3
//! `FileBucketStore` (the same on-disk layout the legacy `s3svc.BucketStore`
//! uses), so objects written by either engine are visible to the other.

use std::collections::BTreeMap;

use devcloud_s3::objops::PutObjectInput;
use devcloud_s3::store::StoreError;

use crate::model::{
    CopyJobConfiguration, ExtractJobConfiguration, JobConfiguration, JobReference, JobResource,
    JobStatistics, JobStatus, LoadJobConfiguration, QueryJobRecord, QueryResponse, RawJson,
    StoredRow, TableFieldSchema, TableReference, TableResource, TableSchema,
};
use crate::query_engine::{normalize_table_reference, validate_table_reference};
use crate::responses::{dataset_etag, default_string, unix_millis_string};
use crate::server::{now_unix_nanos, Server};
use crate::sql_eval::{is_json_null, raw_value_for_response};
use crate::tabledata_handlers::{format_rfc3339_nanos, validate_row_json};
use crate::validation::{path_escape, validate_resource_id, validate_table_schema};
use crate::wire_json;

impl Server {
    /// legacy `createCopyJob`.
    pub(crate) fn create_copy_job(
        &self,
        request_project_id: &str,
        requested_ref: &JobReference,
        config: CopyJobConfiguration,
    ) -> Result<QueryJobRecord, String> {
        let source_table_refs = copy_source_tables(&config)?;
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

        let mut source = TableResource::default();
        let mut source_rows: Vec<StoredRow> = Vec::new();
        for (index, source_table_ref) in source_table_refs.into_iter().enumerate() {
            let source_table_ref = normalize_table_reference(source_table_ref, request_project_id);
            validate_table_reference(&source_table_ref)?;
            let source_table = self
                .read_table(
                    &source_table_ref.project_id,
                    &source_table_ref.dataset_id,
                    &source_table_ref.table_id,
                )
                .map_err(|err| err.to_string())?;
            let Some(source_table) = source_table else {
                return Err(format!(
                    "not found: table {}:{}.{}",
                    source_table_ref.project_id,
                    source_table_ref.dataset_id,
                    source_table_ref.table_id
                ));
            };
            if index == 0 {
                source = source_table;
            } else if source.schema != source_table.schema {
                return Err("source tables must have matching schemas".to_string());
            }
            let rows = self
                .read_rows(
                    &source_table_ref.project_id,
                    &source_table_ref.dataset_id,
                    &source_table_ref.table_id,
                )
                .map_err(|err| err.to_string())?;
            source_rows.extend(rows);
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

        let now = now_unix_nanos();
        let mut destination = match destination {
            Some(destination) if write_disposition != "WRITE_TRUNCATE" => destination,
            _ => {
                let mut destination = source;
                destination.id = format!(
                    "{}:{}.{}",
                    destination_ref.project_id,
                    destination_ref.dataset_id,
                    destination_ref.table_id
                );
                destination.self_link = self.table_self_link(
                    &destination_ref.project_id,
                    &destination_ref.dataset_id,
                    &destination_ref.table_id,
                );
                destination.table_reference = destination_ref.clone();
                destination.creation_time = unix_millis_string(now);
                destination
            }
        };
        destination.etag = dataset_etag(now);
        destination.last_modified_time = unix_millis_string(now);
        self.write_table(&destination)
            .map_err(|err| err.to_string())?;
        if write_disposition == "WRITE_TRUNCATE" {
            self.remove_rows_file(&destination_ref)?;
        }
        if !source_rows.is_empty() {
            self.append_rows(
                &destination_ref.project_id,
                &destination_ref.dataset_id,
                &destination_ref.table_id,
                &source_rows,
            )
            .map_err(|err| err.to_string())?;
        }
        self.refresh_table_row_stats(&destination)
            .map_err(|err| err.to_string())?;

        self.create_completed_job(
            request_project_id,
            requested_ref,
            JobConfiguration {
                copy: config,
                ..Default::default()
            },
            JobStatistics {
                creation_time: unix_millis_string(now),
                start_time: unix_millis_string(now),
                end_time: unix_millis_string(now),
                ..Default::default()
            },
        )
    }

    /// legacy `createLoadJob`.
    pub(crate) fn create_load_job(
        &self,
        request_project_id: &str,
        requested_ref: &JobReference,
        config: LoadJobConfiguration,
    ) -> Result<QueryJobRecord, String> {
        if self.object_store.is_none() {
            return Err("local GCS object store is not configured".to_string());
        }
        let destination_ref =
            normalize_table_reference(config.destination_table.clone(), request_project_id);
        validate_table_reference(&destination_ref)?;
        let create_disposition =
            default_string(config.create_disposition.clone(), "CREATE_IF_NEEDED");
        if create_disposition != "CREATE_IF_NEEDED" && create_disposition != "CREATE_NEVER" {
            return Err(format!(
                "unsupported createDisposition {:?}",
                config.create_disposition
            ));
        }
        let source_format = normalize_data_format(&config.source_format, "NEWLINE_DELIMITED_JSON");
        if source_format != "NEWLINE_DELIMITED_JSON" && source_format != "CSV" {
            return Err(format!(
                "unsupported load sourceFormat {:?}",
                config.source_format
            ));
        }
        if config.source_uris.is_empty() {
            return Err("configuration.load.sourceUris is required".to_string());
        }
        let table = self
            .read_table(
                &destination_ref.project_id,
                &destination_ref.dataset_id,
                &destination_ref.table_id,
            )
            .map_err(|err| err.to_string())?;
        let table = match table {
            Some(table) => table,
            None => {
                if create_disposition == "CREATE_NEVER" {
                    return Err(format!(
                        "not found: table {}:{}.{}",
                        destination_ref.project_id,
                        destination_ref.dataset_id,
                        destination_ref.table_id
                    ));
                }
                self.create_load_destination_table(&destination_ref, config.schema.clone())?
            }
        };
        let write_disposition = default_string(config.write_disposition.clone(), "WRITE_APPEND");
        self.check_load_write_disposition(&config, &write_disposition, &destination_ref)?;

        if config.skip_leading_rows < 0 {
            return Err("configuration.load.skipLeadingRows must be non-negative".to_string());
        }
        let rows = self.load_rows_from_gcs_objects(
            &config.source_uris,
            &source_format,
            &table.schema,
            config.skip_leading_rows,
        )?;
        if write_disposition == "WRITE_TRUNCATE" {
            self.remove_rows_file(&destination_ref)?;
        }
        if !rows.is_empty() {
            self.append_rows(
                &destination_ref.project_id,
                &destination_ref.dataset_id,
                &destination_ref.table_id,
                &rows,
            )
            .map_err(|err| err.to_string())?;
        }
        self.refresh_table_row_stats(&table)
            .map_err(|err| err.to_string())?;

        let now = now_unix_nanos();
        self.create_completed_job(
            request_project_id,
            requested_ref,
            JobConfiguration {
                load: config,
                ..Default::default()
            },
            JobStatistics {
                creation_time: unix_millis_string(now),
                start_time: unix_millis_string(now),
                end_time: unix_millis_string(now),
                ..Default::default()
            },
        )
    }

    /// legacy `createUploadLoadJob` (multipart media upload variant — no object
    /// store required).
    pub(crate) fn create_upload_load_job(
        &self,
        request_project_id: &str,
        requested_ref: &JobReference,
        config: LoadJobConfiguration,
        media: &[u8],
    ) -> Result<QueryJobRecord, String> {
        let destination_ref =
            normalize_table_reference(config.destination_table.clone(), request_project_id);
        validate_table_reference(&destination_ref)?;
        let create_disposition =
            default_string(config.create_disposition.clone(), "CREATE_IF_NEEDED");
        if create_disposition != "CREATE_IF_NEEDED" && create_disposition != "CREATE_NEVER" {
            return Err(format!(
                "unsupported createDisposition {:?}",
                config.create_disposition
            ));
        }
        let source_format = normalize_data_format(&config.source_format, "NEWLINE_DELIMITED_JSON");
        if source_format != "NEWLINE_DELIMITED_JSON" && source_format != "CSV" {
            return Err(format!(
                "unsupported load sourceFormat {:?}",
                config.source_format
            ));
        }
        let table = self
            .read_table(
                &destination_ref.project_id,
                &destination_ref.dataset_id,
                &destination_ref.table_id,
            )
            .map_err(|err| err.to_string())?;
        let table = match table {
            Some(table) => table,
            None => {
                if create_disposition == "CREATE_NEVER" {
                    return Err(format!(
                        "not found: table {}:{}.{}",
                        destination_ref.project_id,
                        destination_ref.dataset_id,
                        destination_ref.table_id
                    ));
                }
                self.create_load_destination_table(&destination_ref, config.schema.clone())?
            }
        };
        let write_disposition = default_string(config.write_disposition.clone(), "WRITE_APPEND");
        self.check_load_write_disposition(&config, &write_disposition, &destination_ref)?;

        if config.skip_leading_rows < 0 {
            return Err("configuration.load.skipLeadingRows must be non-negative".to_string());
        }
        let rows = load_rows(
            media,
            &source_format,
            &table.schema,
            config.skip_leading_rows,
        )?;
        if write_disposition == "WRITE_TRUNCATE" {
            self.remove_rows_file(&destination_ref)?;
        }
        if !rows.is_empty() {
            self.append_rows(
                &destination_ref.project_id,
                &destination_ref.dataset_id,
                &destination_ref.table_id,
                &rows,
            )
            .map_err(|err| err.to_string())?;
        }
        self.refresh_table_row_stats(&table)
            .map_err(|err| err.to_string())?;

        let now = now_unix_nanos();
        self.create_completed_job(
            request_project_id,
            requested_ref,
            JobConfiguration {
                load: config,
                ..Default::default()
            },
            JobStatistics {
                creation_time: unix_millis_string(now),
                start_time: unix_millis_string(now),
                end_time: unix_millis_string(now),
                ..Default::default()
            },
        )
    }

    /// The shared writeDisposition validation + WRITE_EMPTY check (inlined
    /// twice in legacy; behavior identical).
    fn check_load_write_disposition(
        &self,
        config: &LoadJobConfiguration,
        write_disposition: &str,
        destination_ref: &TableReference,
    ) -> Result<(), String> {
        if write_disposition != "WRITE_APPEND"
            && write_disposition != "WRITE_TRUNCATE"
            && write_disposition != "WRITE_EMPTY"
        {
            return Err(format!(
                "unsupported writeDisposition {:?}",
                config.write_disposition
            ));
        }
        if write_disposition == "WRITE_EMPTY" {
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
        Ok(())
    }

    /// legacy `os.Remove(rowsPath)` ignoring `os.ErrNotExist`.
    fn remove_rows_file(&self, destination_ref: &TableReference) -> Result<(), String> {
        let path = self.rows_path(
            &destination_ref.project_id,
            &destination_ref.dataset_id,
            &destination_ref.table_id,
        );
        match std::fs::remove_file(&path) {
            Err(err) if err.kind() != std::io::ErrorKind::NotFound => Err(err.to_string()),
            _ => Ok(()),
        }
    }

    /// legacy `createLoadDestinationTable`.
    fn create_load_destination_table(
        &self,
        table_ref: &TableReference,
        schema: TableSchema,
    ) -> Result<TableResource, String> {
        if schema.fields.is_empty() {
            return Err(
                "configuration.load.schema is required when creating a destination table"
                    .to_string(),
            );
        }
        validate_table_schema(&schema)?;
        let dataset = self
            .read_dataset(&table_ref.project_id, &table_ref.dataset_id)
            .map_err(|err| err.to_string())?;
        if dataset.is_none() {
            return Err(format!(
                "not found: dataset {}:{}",
                table_ref.project_id, table_ref.dataset_id
            ));
        }

        let now = now_unix_nanos();
        let table = TableResource {
            kind: "bigquery#table".to_string(),
            id: format!(
                "{}:{}.{}",
                table_ref.project_id, table_ref.dataset_id, table_ref.table_id
            ),
            self_link: self.table_self_link(
                &table_ref.project_id,
                &table_ref.dataset_id,
                &table_ref.table_id,
            ),
            table_reference: table_ref.clone(),
            table_type: "TABLE".to_string(),
            schema,
            etag: dataset_etag(now),
            creation_time: unix_millis_string(now),
            last_modified_time: unix_millis_string(now),
            num_rows: "0".to_string(),
            num_bytes: "0".to_string(),
            location: self.default_location().to_string(),
            ..Default::default()
        };
        self.write_table(&table).map_err(|err| err.to_string())?;
        Ok(table)
    }

    /// legacy `createExtractJob`.
    pub(crate) fn create_extract_job(
        &self,
        request_project_id: &str,
        requested_ref: &JobReference,
        config: ExtractJobConfiguration,
    ) -> Result<QueryJobRecord, String> {
        let Some(store) = &self.object_store else {
            return Err("local GCS object store is not configured".to_string());
        };
        let source_ref = normalize_table_reference(config.source_table.clone(), request_project_id);
        validate_table_reference(&source_ref)?;
        let destination_format =
            normalize_data_format(&config.destination_format, "NEWLINE_DELIMITED_JSON");
        if destination_format != "NEWLINE_DELIMITED_JSON"
            && destination_format != "JSON"
            && destination_format != "CSV"
        {
            return Err(format!(
                "unsupported extract destinationFormat {:?}",
                config.destination_format
            ));
        }
        if config.destination_uris.len() != 1 {
            return Err(
                "configuration.extract.destinationUris must contain exactly one URI".to_string(),
            );
        }
        let (destination_bucket, destination_key) = parse_gcs_uri(&config.destination_uris[0])?;
        let table = self
            .read_table(
                &source_ref.project_id,
                &source_ref.dataset_id,
                &source_ref.table_id,
            )
            .map_err(|err| err.to_string())?;
        let Some(table) = table else {
            return Err(format!(
                "not found: table {}:{}.{}",
                source_ref.project_id, source_ref.dataset_id, source_ref.table_id
            ));
        };
        let rows = self
            .read_rows(
                &source_ref.project_id,
                &source_ref.dataset_id,
                &source_ref.table_id,
            )
            .map_err(|err| err.to_string())?;
        let (body, content_type) = extracted_rows(&rows, &destination_format, &table.schema)?;
        store
            .put_object(PutObjectInput {
                bucket: destination_bucket.clone(),
                key: destination_key,
                body,
                content_type: content_type.to_string(),
                ..Default::default()
            })
            .map_err(|err| store_error_text(&err, &destination_bucket))?;

        let now = now_unix_nanos();
        self.create_completed_job(
            request_project_id,
            requested_ref,
            JobConfiguration {
                extract: config,
                ..Default::default()
            },
            JobStatistics {
                creation_time: unix_millis_string(now),
                start_time: unix_millis_string(now),
                end_time: unix_millis_string(now),
                ..Default::default()
            },
        )
    }

    /// legacy `loadRowsFromGCSObjects`.
    fn load_rows_from_gcs_objects(
        &self,
        source_uris: &[String],
        source_format: &str,
        schema: &TableSchema,
        skip_leading_rows: i64,
    ) -> Result<Vec<StoredRow>, String> {
        let store = self
            .object_store
            .as_ref()
            .expect("checked by create_load_job");
        let mut rows: Vec<StoredRow> = Vec::new();
        for uri in source_uris {
            let (bucket, key) = parse_gcs_uri(uri)?;
            let found = store
                .get_object(&bucket, &key)
                .map_err(|err| store_error_text(&err, &bucket))?;
            let Some((_, body)) = found else {
                return Err("source object not found".to_string());
            };
            let loaded_rows = load_rows(&body, source_format, schema, skip_leading_rows)?;
            rows.extend(loaded_rows);
        }
        Ok(rows)
    }

    /// legacy `createCompletedJob` (job_handlers.rs): persists a DONE job record
    /// with an empty query response.
    pub(crate) fn create_completed_job(
        &self,
        request_project_id: &str,
        requested_ref: &JobReference,
        config: JobConfiguration,
        statistics: JobStatistics,
    ) -> Result<QueryJobRecord, String> {
        let now = now_unix_nanos();
        let mut job_id = requested_ref.job_id.trim().to_string();
        if job_id.is_empty() {
            job_id = format!("devcloud_job_{now}");
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
        let resource = JobResource {
            kind: "bigquery#job".to_string(),
            id: format!("{request_project_id}:{job_id}"),
            self_link: format!(
                "/bigquery/v2/projects/{}/jobs/{}",
                path_escape(request_project_id),
                path_escape(&job_id)
            ),
            job_reference: job_ref.clone(),
            configuration: config,
            status: JobStatus {
                state: "DONE".to_string(),
            },
            statistics,
        };
        let job = QueryJobRecord {
            job: resource,
            response: QueryResponse {
                kind: "bigquery#queryResponse".to_string(),
                job_reference: job_ref,
                total_rows: "0".to_string(),
                job_complete: true,
                cache_hit: false,
                ..Default::default()
            },
        };
        self.write_query_job(request_project_id, &job_id, &job)
            .map_err(|err| err.to_string())?;
        Ok(job)
    }
}

/// legacy `loadRows`.
fn load_rows(
    body: &[u8],
    source_format: &str,
    schema: &TableSchema,
    skip_leading_rows: i64,
) -> Result<Vec<StoredRow>, String> {
    match normalize_data_format(source_format, "NEWLINE_DELIMITED_JSON").as_str() {
        "NEWLINE_DELIMITED_JSON" => load_rows_from_ndjson(body, schema),
        "CSV" => load_rows_from_csv(body, schema, skip_leading_rows),
        _ => Err(format!("unsupported load sourceFormat {source_format:?}")),
    }
}

/// legacy `loadRowsFromNDJSON`: a `json.Decoder` stream of objects (newlines are
/// irrelevant to the decoder, exactly like legacy).
fn load_rows_from_ndjson(body: &[u8], schema: &TableSchema) -> Result<Vec<StoredRow>, String> {
    let now = format_rfc3339_nanos(now_unix_nanos());
    let mut rows: Vec<StoredRow> = Vec::new();
    for row in serde_json::Deserializer::from_slice(body).into_iter::<BTreeMap<String, RawJson>>() {
        let row = row.map_err(|_| "invalid newline-delimited JSON object".to_string())?;
        let (values, row_errors) = validate_row_json(row, schema, false);
        if !row_errors.is_empty() {
            return Err("source row does not match destination schema".to_string());
        }
        rows.push(StoredRow {
            insert_id: String::new(),
            json: values,
            inserted_at: now.clone(),
        });
    }
    Ok(rows)
}

/// legacy `loadRowsFromCSV` (`csv.Reader` with `FieldsPerRecord = len(fields)`).
fn load_rows_from_csv(
    body: &[u8],
    schema: &TableSchema,
    skip_leading_rows: i64,
) -> Result<Vec<StoredRow>, String> {
    let now = format_rfc3339_nanos(now_unix_nanos());
    let mut records =
        read_csv(body, schema.fields.len()).map_err(|()| "invalid CSV row".to_string())?;
    if skip_leading_rows > records.len() as i64 {
        records = Vec::new();
    } else if skip_leading_rows > 0 {
        records.drain(..skip_leading_rows as usize);
    }
    let mut rows: Vec<StoredRow> = Vec::with_capacity(records.len());
    for record in records {
        let mut row: BTreeMap<String, RawJson> = BTreeMap::new();
        for (i, field) in schema.fields.iter().enumerate() {
            let raw = csv_cell_raw_message(&record[i], field)?;
            row.insert(field.name.clone(), raw);
        }
        let (values, row_errors) = validate_row_json(row, schema, false);
        if !row_errors.is_empty() {
            return Err("source row does not match destination schema".to_string());
        }
        rows.push(StoredRow {
            insert_id: String::new(),
            json: values,
            inserted_at: now.clone(),
        });
    }
    Ok(rows)
}

/// legacy `csvCellRawMessage`.
fn csv_cell_raw_message(value: &str, field: &TableFieldSchema) -> Result<RawJson, String> {
    let mismatch = || "source row does not match destination schema".to_string();
    if value.is_empty() {
        return raw_json("null").map_err(|_| mismatch());
    }
    let field_type = default_string(field.field_type.clone(), "STRING").to_uppercase();
    match field_type.as_str() {
        "STRING" | "BYTES" | "NUMERIC" | "BIGNUMERIC" | "TIMESTAMP" | "DATE" | "TIME"
        | "DATETIME" | "GEOGRAPHY" | "JSON" => {
            let encoded = wire_json::marshal(&value);
            raw_json(&String::from_utf8(encoded).map_err(|err| err.to_string())?)
                .map_err(|err| err.to_string())
        }
        "INTEGER" | "INT64" => {
            if value.parse::<i64>().is_err() {
                return Err(mismatch());
            }
            raw_json(value).map_err(|_| mismatch())
        }
        "FLOAT" | "FLOAT64" => {
            if legacy_parse_float(value).is_none() {
                return Err(mismatch());
            }
            raw_json(value).map_err(|_| mismatch())
        }
        "BOOLEAN" | "BOOL" => match legacy_parse_bool(value) {
            Some(true) => raw_json("true").map_err(|_| mismatch()),
            Some(false) => raw_json("false").map_err(|_| mismatch()),
            None => Err(mismatch()),
        },
        "RECORD" | "STRUCT" => Err("CSV load does not support RECORD fields".to_string()),
        _ => Err(format!("unsupported field type {:?}", field.field_type)),
    }
}

fn raw_json(value: &str) -> Result<RawJson, serde_json::Error> {
    serde_json::value::RawValue::from_string(value.to_string())
}

/// legacy `strconv.ParseBool`.
fn legacy_parse_bool(value: &str) -> Option<bool> {
    match value {
        "1" | "t" | "T" | "true" | "TRUE" | "True" => Some(true),
        "0" | "f" | "F" | "false" | "FALSE" | "False" => Some(false),
        _ => None,
    }
}

/// legacy `strconv.ParseFloat(value, 64)` (accepts Inf/NaN spellings and
/// underscores are rejected — Rust's parser is close enough for the formats
/// this path sees; both reject empty/garbage).
fn legacy_parse_float(value: &str) -> Option<f64> {
    value.parse::<f64>().ok()
}

/// legacy `extractedRows`.
fn extracted_rows(
    rows: &[StoredRow],
    format: &str,
    schema: &TableSchema,
) -> Result<(Vec<u8>, &'static str), String> {
    match normalize_data_format(format, "NEWLINE_DELIMITED_JSON").as_str() {
        "NEWLINE_DELIMITED_JSON" | "JSON" => {
            Ok((extracted_ndjson(rows, schema), "application/x-ndjson"))
        }
        "CSV" => Ok((extracted_csv(rows, schema), "text/csv")),
        _ => Err(format!("unsupported extract destinationFormat {format:?}")),
    }
}

/// legacy `extractedNDJSON`: one `json.Encoder` line per row, schema-known fields
/// only, missing/null → JSON null, sorted keys (legacy map marshal).
fn extracted_ndjson(rows: &[StoredRow], schema: &TableSchema) -> Vec<u8> {
    let mut body = Vec::new();
    for row in rows {
        let mut value: BTreeMap<&str, serde_json::Value> = BTreeMap::new();
        for field in &schema.fields {
            match row.json.get(&field.name) {
                Some(raw) if !is_json_null(raw.get()) => {
                    value.insert(&field.name, raw_value_for_response(raw.get()));
                }
                _ => {
                    value.insert(&field.name, serde_json::Value::Null);
                }
            }
        }
        body.extend_from_slice(&wire_json::to_vec(&value));
    }
    body
}

/// legacy `extractedCSV` (`csv.Writer` quoting via the shared legacy-parity writer).
fn extracted_csv(rows: &[StoredRow], schema: &TableSchema) -> Vec<u8> {
    let mut records: Vec<Vec<String>> = Vec::with_capacity(rows.len());
    for row in rows {
        let mut record: Vec<String> = Vec::with_capacity(schema.fields.len());
        for field in &schema.fields {
            match row.json.get(&field.name) {
                Some(raw) if !is_json_null(raw.get()) => {
                    record.push(csv_value_string(&raw_value_for_response(raw.get())));
                }
                _ => record.push(String::new()),
            }
        }
        records.push(record);
    }
    devcloud_s3::csv::write_csv(&records)
}

/// legacy `csvValueString`: nil → "", string → itself, bool → FormatBool,
/// float64 → FormatFloat('f', -1, 64), default → fmt.Sprint.
fn csv_value_string(value: &serde_json::Value) -> String {
    use serde_json::Value;
    match value {
        Value::Null => String::new(),
        Value::String(s) => s.clone(),
        Value::Bool(b) => b.to_string(),
        Value::Number(n) => {
            // legacy decodes JSON numbers as float64 here; FormatFloat('f', -1)
            // and Rust's shortest Display agree on integral values ("37").
            match n.as_f64() {
                Some(f) => format_float_legacy(f),
                None => n.to_string(),
            }
        }
        other => crate::sql_eval::legacy_sprint(other),
    }
}

/// legacy `strconv.FormatFloat(value, 'f', -1, 64)`.
fn format_float_legacy(value: f64) -> String {
    if value.is_nan() {
        return "NaN".to_string();
    }
    if value.is_infinite() {
        return if value > 0.0 { "+Inf" } else { "-Inf" }.to_string();
    }
    format!("{value}")
}

/// legacy `normalizeDataFormat`.
fn normalize_data_format(value: &str, fallback: &str) -> String {
    default_string(value.to_string(), fallback)
        .trim()
        .to_uppercase()
}

/// legacy `parseGCSURI`.
fn parse_gcs_uri(uri: &str) -> Result<(String, String), String> {
    if uri.contains(['*', '?', '[']) {
        return Err("wildcard GCS URIs are not supported".to_string());
    }
    let trimmed = uri.trim();
    let Some(without_scheme) = trimmed.strip_prefix("gs://") else {
        return Err("only gs:// URIs are supported".to_string());
    };
    match without_scheme.split_once('/') {
        Some((bucket, key)) if !bucket.is_empty() && !key.is_empty() => {
            Ok((bucket.to_string(), key.to_string()))
        }
        _ => Err("gs:// URI must include bucket and object".to_string()),
    }
}

/// legacy `copySourceTables`.
fn copy_source_tables(config: &CopyJobConfiguration) -> Result<Vec<TableReference>, String> {
    if !config.source_tables.is_empty() {
        if !config.source_table.table_id.is_empty() {
            return Err("copy job supports sourceTable or sourceTables, not both".to_string());
        }
        return Ok(config.source_tables.clone());
    }
    if config.source_table.table_id.is_empty() {
        return Err("configuration.copy.sourceTable is required".to_string());
    }
    Ok(vec![config.source_table.clone()])
}

/// The legacy S3 store's error strings, as surfaced verbatim through `err.Error()`
/// by the BigQuery load/extract handlers.
fn store_error_text(err: &StoreError, bucket: &str) -> String {
    match err {
        StoreError::InvalidBucketName => format!("invalid bucket name {bucket:?}"),
        StoreError::InvalidObjectKey => "object key is required".to_string(),
        StoreError::BucketNotExist => "bucket does not exist".to_string(),
        StoreError::BucketNotEmpty => "bucket is not empty".to_string(),
        StoreError::ObjectLocked => "object is locked".to_string(),
        StoreError::Io(io_err) => io_err.to_string(),
        _ => "internal error".to_string(),
    }
}

/// A minimal legacy `encoding/csv` reader: comma separator, `\r\n`/`\n` record
/// terminators, RFC 4180 quoting with doubled quotes, blank lines skipped,
/// and (legacy `FieldsPerRecord > 0`) every record must have exactly
/// `fields_per_record` fields. Any malformation is an error — the legacy caller
/// collapses every reader error into "invalid CSV row".
fn read_csv(data: &[u8], fields_per_record: usize) -> Result<Vec<Vec<String>>, ()> {
    let text = std::str::from_utf8(data).map_err(|_| ())?;
    let mut records: Vec<Vec<String>> = Vec::new();
    let mut chars = text.chars().peekable();
    'records: loop {
        // Skip blank lines between records (legacy skips empty lines).
        while let Some(&c) = chars.peek() {
            if c == '\n' {
                chars.next();
            } else if c == '\r' {
                chars.next();
                if chars.peek() == Some(&'\n') {
                    chars.next();
                }
            } else {
                break;
            }
        }
        if chars.peek().is_none() {
            break;
        }
        let mut record: Vec<String> = Vec::new();
        let mut field = String::new();
        let mut in_quotes = false;
        let mut field_started = false;
        loop {
            match chars.next() {
                None => {
                    record.push(field);
                    if record.len() != fields_per_record {
                        return Err(());
                    }
                    records.push(record);
                    break 'records;
                }
                Some('"') if !field_started && field.is_empty() && !in_quotes => {
                    in_quotes = true;
                    field_started = true;
                }
                Some('"') if in_quotes => {
                    if chars.peek() == Some(&'"') {
                        chars.next();
                        field.push('"');
                    } else {
                        in_quotes = false;
                    }
                }
                Some('"') => return Err(()), // bare quote (legacy LazyQuotes=false)
                Some(',') if !in_quotes => {
                    record.push(std::mem::take(&mut field));
                    field_started = false;
                }
                Some('\r') if !in_quotes && chars.peek() == Some(&'\n') => {
                    chars.next();
                    record.push(field);
                    if record.len() != fields_per_record {
                        return Err(());
                    }
                    records.push(record);
                    continue 'records;
                }
                Some('\n') if !in_quotes => {
                    record.push(field);
                    if record.len() != fields_per_record {
                        return Err(());
                    }
                    records.push(record);
                    continue 'records;
                }
                Some(c) => {
                    field.push(c);
                    field_started = true;
                }
            }
        }
    }
    Ok(records)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_gcs_uri_matches_legacy() {
        assert_eq!(
            parse_gcs_uri("gs://bucket/key/with/slashes").unwrap(),
            ("bucket".to_string(), "key/with/slashes".to_string())
        );
        assert_eq!(
            parse_gcs_uri("gs://bucket/*").unwrap_err(),
            "wildcard GCS URIs are not supported"
        );
        assert_eq!(
            parse_gcs_uri("s3://bucket/key").unwrap_err(),
            "only gs:// URIs are supported"
        );
        assert_eq!(
            parse_gcs_uri("gs://bucket").unwrap_err(),
            "gs:// URI must include bucket and object"
        );
    }

    #[test]
    fn read_csv_enforces_field_count_and_quotes() {
        let records = read_csv(b"6,Barbara,39,true\n7,Donald,44,false\n", 4).unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0], vec!["6", "Barbara", "39", "true"]);
        let quoted = read_csv(b"1,\"has,comma\",2,\"with \"\"quote\"\"\"\n", 4).unwrap();
        assert_eq!(quoted[0][1], "has,comma");
        assert_eq!(quoted[0][3], "with \"quote\"");
        assert!(read_csv(b"1,2\n", 4).is_err());
    }
}
