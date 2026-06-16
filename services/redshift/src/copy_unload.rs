//! COPY ingest / UNLOAD export over local files and `s3://` URIs.
//!
//! Parity: `internal/services/redshift/sql_copy_unload.rs`. COPY reads CSV or
//! line-delimited JSON into a table; UNLOAD runs a SELECT and writes CSV. Both
//! reach `s3://` URIs through the shared `FileBucketStore` (legacy
//! `Config.ObjectStore`), which is the same on-disk layout the legacy
//! `s3svc.BucketStore` uses. `gs://` URIs are not a COPY/UNLOAD prefix in legacy —
//! only local paths and `s3://` are recognized, so this matches.

use std::sync::Arc;

use devcloud_s3::objops::PutObjectInput;
use devcloud_s3::store::StoreError;

use crate::engine::QueryResult;
use crate::errors::SqlError;
use crate::model::Column;
use crate::server::ServerShared;
use crate::sql_parse::{matching_paren, parse_leading_sql_string_literal, parse_qualified_name};

#[derive(Debug, Clone)]
struct CopyCsvOptions {
    delimiter: char,
    format: String,
    ignore_header: usize,
    null_as: String,
    has_null_as: bool,
}

impl Default for CopyCsvOptions {
    fn default() -> Self {
        CopyCsvOptions {
            delimiter: ',',
            format: "csv".to_string(),
            ignore_header: 0,
            null_as: String::new(),
            has_null_as: false,
        }
    }
}

impl ServerShared {
    /// Mirrors `copyFromLocalCSV`.
    pub(crate) fn copy_from_local_csv(
        self: &Arc<Self>,
        statement: &str,
    ) -> Result<QueryResult, SqlError> {
        let lower = statement.to_ascii_lowercase();
        let from_index = lower
            .find(" from ")
            .ok_or_else(|| SqlError::new("COPY requires FROM"))?;
        let name = parse_qualified_name(&statement["copy ".len()..from_index]);
        let after_from = statement[from_index + " from ".len()..].trim_start();
        let (path, rest) = parse_leading_sql_string_literal(after_from).map_err(|err| {
            SqlError::new(format!("COPY requires a local file path or s3 URI: {err}"))
        })?;
        let options = parse_copy_csv_options(&rest)?;

        // First lock: validate target exists and snapshot columns.
        let columns = {
            let state = self.lock_state();
            let table = state
                .db
                .schemas
                .get(&name.schema)
                .and_then(|schema| schema.tables.get(&name.table));
            let Some(table) = table else {
                return Err(SqlError::new(format!(
                    "table {}.{} does not exist",
                    name.schema, name.table
                )));
            };
            if table.is_read_only_relation() {
                return Err(SqlError::new(format!(
                    "cannot copy into view {}.{}",
                    name.schema, name.table
                )));
            }
            table.columns.clone()
        };

        let records = self.read_copy_records(&path, &options, &columns)?;

        // Second lock: re-validate and append.
        let mut state = self.lock_state();
        let column_count = {
            let table = state
                .db
                .schemas
                .get(&name.schema)
                .and_then(|schema| schema.tables.get(&name.table));
            let Some(table) = table else {
                return Err(SqlError::new(format!(
                    "table {}.{} does not exist",
                    name.schema, name.table
                )));
            };
            if table.is_read_only_relation() {
                return Err(SqlError::new(format!(
                    "cannot copy into view {}.{}",
                    name.schema, name.table
                )));
            }
            table.columns.len()
        };
        for (line, record) in records.iter().enumerate() {
            if record.len() != column_count {
                return Err(SqlError::new(format!(
                    "COPY row {} has {} values for {} columns",
                    line + 1,
                    record.len(),
                    column_count
                )));
            }
        }
        let count = records.len();
        let table = state
            .db
            .schemas
            .get_mut(&name.schema)
            .and_then(|schema| schema.tables.get_mut(&name.table))
            .expect("table existence re-checked above");
        for record in records {
            table.rows.push(record);
        }
        self.persist_locked(&state)?;
        Ok(QueryResult::tag_only(&format!("COPY {count}")))
    }

    /// Mirrors `unloadToLocalCSV`.
    pub(crate) fn unload_to_local_csv(
        self: &Arc<Self>,
        statement: &str,
    ) -> Result<QueryResult, SqlError> {
        let rest = statement["unload ".len()..].trim();
        if !rest.starts_with('(') {
            return Err(SqlError::new("UNLOAD requires a parenthesized SELECT"));
        }
        let close = matching_paren(rest, 0)
            .ok_or_else(|| SqlError::new("UNLOAD has an unterminated SELECT"))?;
        let (select_sql, _) =
            parse_leading_sql_string_literal(rest[1..close].trim()).map_err(|err| {
                SqlError::new(format!(
                    "UNLOAD requires SELECT SQL as a string literal: {err}"
                ))
            })?;
        let after_select = rest[close + 1..].trim();
        if !after_select.to_ascii_lowercase().starts_with("to ") {
            return Err(SqlError::new("UNLOAD requires TO"));
        }
        let (target_prefix, _) = parse_leading_sql_string_literal(
            after_select["to ".len()..].trim(),
        )
        .map_err(|err| {
            SqlError::new(format!(
                "UNLOAD requires a local target prefix or s3 URI: {err}"
            ))
        })?;

        let result = self.execute_sql_memory(&select_sql)?;
        let mut output = String::new();
        for row in &result.rows {
            write_csv_record(&mut output, row);
        }

        if target_prefix.to_ascii_lowercase().starts_with("s3://") {
            self.write_s3_object(&format!("{target_prefix}000"), output.into_bytes())?;
            return Ok(QueryResult::tag_only("UNLOAD"));
        }

        let output_path = clean_path(&format!("{target_prefix}000"));
        if let Some(parent) = std::path::Path::new(&output_path).parent() {
            std::fs::create_dir_all(parent)
                .map_err(|err| SqlError::new(format!("UNLOAD create target directory: {err}")))?;
        }
        std::fs::write(&output_path, output.as_bytes())
            .map_err(|err| SqlError::new(format!("UNLOAD write target file: {err}")))?;
        Ok(QueryResult::tag_only("UNLOAD"))
    }

    /// Mirrors `readCopyRecords`.
    fn read_copy_records(
        &self,
        source: &str,
        options: &CopyCsvOptions,
        columns: &[Column],
    ) -> Result<Vec<Vec<String>>, SqlError> {
        if options.format == "json" {
            self.read_copy_json_records(source, columns)
        } else {
            self.read_copy_csv_records(source, options)
        }
    }

    /// Mirrors `readCopyCSVRecords`.
    fn read_copy_csv_records(
        &self,
        source: &str,
        options: &CopyCsvOptions,
    ) -> Result<Vec<Vec<String>>, SqlError> {
        let data = self.read_copy_source(source)?;
        let text =
            String::from_utf8(data).map_err(|_| SqlError::new("COPY read CSV: invalid UTF-8"))?;
        let mut records = read_csv_records(&text, options.delimiter)?;
        // Per-record size validation (legacy validateCopyRecordSize).
        for record in &records {
            self.validate_copy_record_size(record, options.delimiter)?;
        }
        if options.ignore_header > 0 {
            if options.ignore_header >= records.len() {
                records = Vec::new();
            } else {
                records.drain(..options.ignore_header);
            }
        }
        if options.has_null_as {
            for record in &mut records {
                for value in record.iter_mut() {
                    if *value == options.null_as {
                        value.clear();
                    }
                }
            }
        }
        Ok(records)
    }

    /// Mirrors `readCopyJSONRecords`.
    fn read_copy_json_records(
        &self,
        source: &str,
        columns: &[Column],
    ) -> Result<Vec<Vec<String>>, SqlError> {
        let data = self.read_copy_source(source)?;
        let text =
            String::from_utf8(data).map_err(|_| SqlError::new("COPY read JSON: invalid UTF-8"))?;
        let mut records = Vec::new();
        for raw_line in text.lines() {
            let line = raw_line.trim();
            if line.is_empty() {
                continue;
            }
            self.validate_copy_json_line_size(line)?;
            records.push(json_line_to_record(line, columns)?);
        }
        Ok(records)
    }

    /// Reads a COPY source: `s3://` via the object store, otherwise a local file.
    fn read_copy_source(&self, source: &str) -> Result<Vec<u8>, SqlError> {
        if source.to_ascii_lowercase().starts_with("s3://") {
            self.read_s3_object(source)
        } else {
            std::fs::read(clean_path(source))
                .map_err(|err| SqlError::new(format!("COPY open source file: {err}")))
        }
    }

    /// Mirrors `validateCopyJSONLineSize`.
    fn validate_copy_json_line_size(&self, line: &str) -> Result<(), SqlError> {
        let max = self.config.max_copy_input_bytes;
        if max <= 0 {
            return Ok(());
        }
        if line.len() as i64 > max {
            return Err(SqlError::new("COPY input row exceeds maxCopyInputBytes"));
        }
        Ok(())
    }

    /// Mirrors `validateCopyRecordSize`.
    fn validate_copy_record_size(
        &self,
        record: &[String],
        delimiter: char,
    ) -> Result<(), SqlError> {
        let max = self.config.max_copy_input_bytes;
        if max <= 0 {
            return Ok(());
        }
        let delimiter_len = delimiter.len_utf8();
        let mut size = 0usize;
        for (index, value) in record.iter().enumerate() {
            if index > 0 {
                size += delimiter_len;
            }
            size += value.len();
            if size as i64 > max {
                return Err(SqlError::new("COPY input row exceeds maxCopyInputBytes"));
            }
        }
        Ok(())
    }

    /// Mirrors `readS3Object`.
    fn read_s3_object(&self, uri: &str) -> Result<Vec<u8>, SqlError> {
        let Some(store) = &self.config.object_store else {
            return Err(SqlError::new(
                "COPY from s3 URI requires local S3 service to be enabled",
            ));
        };
        let (bucket, key) = parse_s3_uri(uri)?;
        let found = store.get_object(&bucket, &key).map_err(|err| {
            SqlError::new(format!("COPY read S3 object: {}", store_error_text(&err)))
        })?;
        match found {
            Some((_, data)) => Ok(data),
            None => Err(SqlError::new("COPY source S3 object does not exist")),
        }
    }

    /// Mirrors `writeS3Object`.
    fn write_s3_object(&self, uri: &str, body: Vec<u8>) -> Result<(), SqlError> {
        let Some(store) = &self.config.object_store else {
            return Err(SqlError::new(
                "UNLOAD to s3 URI requires local S3 service to be enabled",
            ));
        };
        let (bucket, key) = parse_s3_uri(uri)?;
        store
            .put_object(PutObjectInput {
                bucket,
                key,
                body,
                content_type: "text/csv".to_string(),
                ..Default::default()
            })
            .map_err(|err| {
                SqlError::new(format!(
                    "UNLOAD write S3 object: {}",
                    store_error_text(&err)
                ))
            })?;
        Ok(())
    }
}

/// Renders a `StoreError` to text (parity with legacy wrapped store error string).
fn store_error_text(err: &StoreError) -> String {
    match err {
        StoreError::InvalidBucketName => "invalid bucket name".to_string(),
        StoreError::InvalidObjectKey => "object key is required".to_string(),
        StoreError::BucketNotExist => "bucket does not exist".to_string(),
        StoreError::BucketNotEmpty => "bucket is not empty".to_string(),
        StoreError::ObjectLocked => "object is locked".to_string(),
        StoreError::Io(io_err) => io_err.to_string(),
        other => format!("{other:?}"),
    }
}

/// Mirrors `parseS3URI`.
fn parse_s3_uri(uri: &str) -> Result<(String, String), SqlError> {
    if !uri.to_ascii_lowercase().starts_with("s3://") {
        return Err(SqlError::new("expected s3 URI"));
    }
    let rest = &uri["s3://".len()..];
    match rest.split_once('/') {
        Some((bucket, key)) if !bucket.is_empty() && !key.is_empty() => {
            Ok((bucket.to_string(), key.to_string()))
        }
        _ => Err(SqlError::new("s3 URI requires bucket and key")),
    }
}

/// Mirrors `jsonLineToRecord`.
fn json_line_to_record(line: &str, columns: &[Column]) -> Result<Vec<String>, SqlError> {
    let object: serde_json::Map<String, serde_json::Value> = serde_json::from_str(line)
        .map_err(|err| SqlError::new(format!("COPY read JSON: {err}")))?;
    let mut lower_object: std::collections::HashMap<String, &serde_json::Value> =
        std::collections::HashMap::with_capacity(object.len());
    for (key, value) in &object {
        lower_object.insert(key.to_lowercase(), value);
    }
    let mut record = Vec::with_capacity(columns.len());
    for column in columns {
        match lower_object.get(&column.name.to_lowercase()) {
            None => record.push(String::new()),
            Some(serde_json::Value::Null) => record.push(String::new()),
            Some(value) => record.push(json_copy_value_string(value)),
        }
    }
    Ok(record)
}

/// Mirrors `jsonCopyValueString` (UseNumber preserves number literals).
fn json_copy_value_string(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Number(n) => n.to_string(),
        serde_json::Value::Bool(b) => {
            if *b {
                "true".to_string()
            } else {
                "false".to_string()
            }
        }
        other => serde_json::to_string(other).unwrap_or_else(|_| other.to_string()),
    }
}

/// Mirrors `parseCopyCSVOptions`.
fn parse_copy_csv_options(value: &str) -> Result<CopyCsvOptions, SqlError> {
    let tokens = tokenize_sql_options(value)?;
    let mut options = CopyCsvOptions::default();
    let mut i = 0;
    while i < tokens.len() {
        let token = tokens[i].to_lowercase();
        match token.as_str() {
            "" | "csv" => {
                options.format = "csv".to_string();
                i += 1;
                continue;
            }
            "json" => {
                options.format = "json".to_string();
                if i + 1 < tokens.len()
                    && (tokens[i + 1].eq_ignore_ascii_case("auto")
                        || tokens[i + 1].eq_ignore_ascii_case("noshred"))
                {
                    i += 1;
                }
            }
            "iam_role" | "credentials" | "region" => {
                if i + 1 < tokens.len() {
                    i += 1;
                }
            }
            "delimiter" => {
                let mut next = i + 1;
                if next < tokens.len() && tokens[next].eq_ignore_ascii_case("as") {
                    next += 1;
                }
                if next >= tokens.len() {
                    return Err(SqlError::new("COPY DELIMITER requires a value"));
                }
                options.delimiter = parse_csv_delimiter(&tokens[next])?;
                i = next;
            }
            "ignoreheader" => {
                if i + 1 >= tokens.len() {
                    return Err(SqlError::new("COPY IGNOREHEADER requires a row count"));
                }
                let count: i64 = tokens[i + 1].parse().map_err(|_| {
                    SqlError::new("COPY IGNOREHEADER requires a non-negative row count")
                })?;
                if count < 0 {
                    return Err(SqlError::new(
                        "COPY IGNOREHEADER requires a non-negative row count",
                    ));
                }
                options.ignore_header = count as usize;
                i += 1;
            }
            "null" => {
                let mut next = i + 1;
                if next < tokens.len() && tokens[next].eq_ignore_ascii_case("as") {
                    next += 1;
                }
                if next >= tokens.len() {
                    return Err(SqlError::new("COPY NULL AS requires a value"));
                }
                options.null_as = tokens[next].clone();
                options.has_null_as = true;
                i = next;
            }
            _ => {}
        }
        i += 1;
    }
    Ok(options)
}

/// Mirrors `tokenizeSQLOptions`: whitespace/`;`-separated tokens with `'...'`
/// string literals.
fn tokenize_sql_options(value: &str) -> Result<Vec<String>, SqlError> {
    let bytes = value.as_bytes();
    let mut tokens = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        let c = bytes[i];
        if c.is_ascii_whitespace() || c == b';' {
            i += 1;
            continue;
        }
        if c == b'\'' {
            let (parsed, rest) = parse_leading_sql_string_literal(&value[i..])?;
            tokens.push(parsed);
            i = value.len() - rest.len();
            continue;
        }
        let start = i;
        while i < bytes.len() && !bytes[i].is_ascii_whitespace() && bytes[i] != b';' {
            i += 1;
        }
        tokens.push(value[start..i].to_string());
    }
    Ok(tokens)
}

/// Mirrors `parseCSVDelimiter`.
fn parse_csv_delimiter(value: &str) -> Result<char, SqlError> {
    if value == "\\t" {
        return Ok('\t');
    }
    let runes: Vec<char> = value.chars().collect();
    if runes.len() != 1 {
        return Err(SqlError::new(
            "COPY DELIMITER requires exactly one character",
        ));
    }
    let r = runes[0];
    if r == '\r' || r == '\n' || r == '\u{fffd}' {
        return Err(SqlError::new(
            "COPY DELIMITER contains an unsupported character",
        ));
    }
    Ok(r)
}

/// Mirrors legacy `encoding/csv` reading with `FieldsPerRecord = -1` (variable
/// field counts allowed) and a configurable comma. Blank lines are skipped.
fn read_csv_records(text: &str, delimiter: char) -> Result<Vec<Vec<String>>, SqlError> {
    let mut records: Vec<Vec<String>> = Vec::new();
    let mut chars = text.chars().peekable();
    'records: loop {
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
                Some('"') => return Err(SqlError::new("COPY read CSV: bare quote in field")),
                Some(c) if c == delimiter && !in_quotes => {
                    record.push(std::mem::take(&mut field));
                    field_started = false;
                }
                Some('\r') if !in_quotes && chars.peek() == Some(&'\n') => {
                    chars.next();
                    record.push(field);
                    records.push(record);
                    continue 'records;
                }
                Some('\n') if !in_quotes => {
                    record.push(field);
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

/// Mirrors legacy `csv.Writer.Write` for a single record with the default `,`
/// delimiter: quotes a field when it contains `,`, `"`, `\r`, or `\n`, doubles
/// embedded quotes, and terminates with `\n`.
fn write_csv_record(out: &mut String, record: &[String]) {
    for (i, field) in record.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        if field_needs_quotes(field) {
            out.push('"');
            for ch in field.chars() {
                if ch == '"' {
                    out.push('"');
                }
                out.push(ch);
            }
            out.push('"');
        } else {
            out.push_str(field);
        }
    }
    out.push('\n');
}

fn field_needs_quotes(field: &str) -> bool {
    field
        .chars()
        .any(|c| c == ',' || c == '"' || c == '\r' || c == '\n')
}

/// Mirrors `filepath.Clean` closely enough for the local-target paths COPY/
/// UNLOAD see (the tests use already-clean absolute temp paths).
fn clean_path(path: &str) -> String {
    std::path::Path::new(path)
        .components()
        .collect::<std::path::PathBuf>()
        .to_string_lossy()
        .into_owned()
}

#[cfg(test)]
mod tests {
    use crate::{Config, Server};
    use devcloud_s3::objops::PutObjectInput;
    use devcloud_s3::store::FileBucketStore;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;

    static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

    fn temp_dir(tag: &str) -> std::path::PathBuf {
        let n = TEMP_COUNTER.fetch_add(1, Ordering::SeqCst);
        let dir = std::env::temp_dir().join(format!(
            "devcloud-redshift-copy-{}-{}-{}",
            tag,
            std::process::id(),
            n
        ));
        std::fs::create_dir_all(&dir).expect("create temp dir");
        dir
    }

    /// Mirrors `TestCopyAndUnloadLocalCSVWorkflow`.
    #[test]
    fn copy_and_unload_local_csv_workflow() {
        let server = Server::new(Config::default());
        let dir = temp_dir("local-csv");
        let source = dir.join("events.csv");
        std::fs::write(&source, b"2,unload\n1,copy\n").expect("write source");
        let source_lit = source.to_string_lossy().replace('\'', "''");
        let export_prefix = dir.join("exports").join("events_");
        let export_lit = export_prefix.to_string_lossy().replace('\'', "''");

        for statement in [
            "drop table if exists public.copy_events".to_string(),
            "create table public.copy_events(id integer, payload varchar(64))".to_string(),
            format!("copy public.copy_events from '{source_lit}' csv"),
            format!(
                "unload ('select * from public.copy_events order by id') to '{export_lit}' csv allowoverwrite"
            ),
        ] {
            server
                .execute_sql(&statement)
                .unwrap_or_else(|e| panic!("execute {statement:?}: {e}"));
        }

        let data = std::fs::read(dir.join("exports").join("events_000")).expect("read export");
        assert_eq!(String::from_utf8(data).unwrap(), "1,copy\n2,unload\n");
    }

    /// Mirrors `TestCopyLocalCSVOptions`.
    #[test]
    fn copy_local_csv_options() {
        let server = Server::new(Config::default());
        let dir = temp_dir("csv-options");
        let source = dir.join("events.psv");
        std::fs::write(
            &source,
            b"id|payload|note\n1|created|NULL\n2|updated|kept\n",
        )
        .expect("write source");
        let source_lit = source.to_string_lossy().replace('\'', "''");

        for statement in [
            "create table public.copy_options(id integer, payload varchar(64), note varchar(64))"
                .to_string(),
            format!(
                "copy public.copy_options from '{source_lit}' csv delimiter '|' ignoreheader 1 null as 'NULL'"
            ),
        ] {
            server
                .execute_sql(&statement)
                .unwrap_or_else(|e| panic!("execute {statement:?}: {e}"));
        }

        let result = server
            .execute_sql("select id, payload, note from public.copy_options order by id")
            .expect("select copied rows");
        let want = vec![
            vec!["1".to_string(), "created".to_string(), String::new()],
            vec!["2".to_string(), "updated".to_string(), "kept".to_string()],
        ];
        assert_eq!(result.rows, want);
    }

    /// Mirrors `TestCopyAndUnloadLocalS3CSVWorkflow`.
    #[test]
    fn copy_and_unload_local_s3_csv_workflow() {
        let dir = temp_dir("s3-csv");
        let store = Arc::new(FileBucketStore::new(dir));
        store.create_bucket("demo-bucket").expect("create bucket");
        store
            .put_object(PutObjectInput {
                bucket: "demo-bucket".to_string(),
                key: "inputs/events.csv".to_string(),
                body: b"2,unload\n1,copy\n".to_vec(),
                content_type: "text/csv".to_string(),
                ..Default::default()
            })
            .expect("put source object");
        let server = Server::new(Config {
            object_store: Some(Arc::clone(&store)),
            ..Config::default()
        });

        for statement in [
            "create table public.copy_events(id integer, payload varchar(64))",
            "copy public.copy_events from 's3://demo-bucket/inputs/events.csv' iam_role default csv",
            "unload ('select * from public.copy_events order by id') to 's3://demo-bucket/exports/events_' iam_role default csv allowoverwrite",
        ] {
            server
                .execute_sql(statement)
                .unwrap_or_else(|e| panic!("execute {statement:?}: {e}"));
        }

        let found = store
            .get_object("demo-bucket", "exports/events_000")
            .expect("get export object");
        let (_, data) = found.expect("export object was not written");
        assert_eq!(String::from_utf8(data).unwrap(), "1,copy\n2,unload\n");
    }

    /// Mirrors `TestCopyFromLocalJSONAutoMapsObjectsByColumnName`.
    #[test]
    fn copy_from_local_json_auto_maps_objects_by_column_name() {
        let dir = temp_dir("json-auto");
        let source = dir.join("events.json");
        std::fs::write(
            &source,
            b"{\"payload\":\"created\",\"id\":1,\"active\":true}\n{\"id\":2,\"payload\":\"updated\",\"extra\":\"ignored\"}\n",
        )
        .expect("write source");
        let source_lit = source.to_string_lossy().replace('\'', "''");
        let server = Server::new(Config::default());
        for statement in [
            "create table public.copy_events(id integer, payload varchar(64), active boolean)"
                .to_string(),
            format!("copy public.copy_events from '{source_lit}' json 'auto'"),
        ] {
            server
                .execute_sql(&statement)
                .unwrap_or_else(|e| panic!("execute {statement:?}: {e}"));
        }

        let result = server
            .execute_sql("select id, payload, active from public.copy_events order by id")
            .expect("select copied rows");
        let want = vec![
            vec!["1".to_string(), "created".to_string(), "true".to_string()],
            vec!["2".to_string(), "updated".to_string(), String::new()],
        ];
        assert_eq!(result.rows, want);
    }

    /// Mirrors `TestCopyFromJSONRejectsRowsExceedingConfiguredInputLimit`.
    #[test]
    fn copy_from_json_rejects_rows_exceeding_configured_input_limit() {
        let dir = temp_dir("json-limit");
        let source = dir.join("events.json");
        std::fs::write(
            &source,
            b"{\"id\":1,\"payload\":\"this-row-is-too-long\"}\n",
        )
        .expect("write source");
        let source_lit = source.to_string_lossy().replace('\'', "''");
        let server = Server::new(Config {
            max_copy_input_bytes: 8,
            ..Config::default()
        });
        server
            .execute_sql("create table public.copy_events(id integer, payload varchar(64))")
            .expect("create table");

        let err = server
            .execute_sql(&format!(
                "copy public.copy_events from '{source_lit}' json 'auto'"
            ))
            .expect_err("COPY should fail");
        assert!(
            err.to_string().contains("maxCopyInputBytes"),
            "COPY error = {err}"
        );
        let result = server
            .execute_sql("select count(*) from public.copy_events")
            .expect("count rows");
        assert_eq!(result.rows, vec![vec!["0".to_string()]]);
    }

    /// Mirrors `TestCopyFromS3RequiresObjectStore`.
    #[test]
    fn copy_from_s3_requires_object_store() {
        let server = Server::new(Config::default());
        server
            .execute_sql("create table public.copy_events(id integer, payload varchar(64))")
            .expect("create table");
        let err = server
            .execute_sql("copy public.copy_events from 's3://demo-bucket/inputs/events.csv' csv")
            .expect_err("COPY should fail");
        assert!(
            err.to_string().contains("local S3 service"),
            "COPY error = {err}"
        );
    }

    /// Mirrors `TestCopyRejectsRowsExceedingConfiguredInputLimit`.
    #[test]
    fn copy_rejects_rows_exceeding_configured_input_limit() {
        let dir = temp_dir("csv-limit");
        let source = dir.join("events.csv");
        std::fs::write(&source, b"1,short\n2,this-row-is-too-long\n").expect("write source");
        let source_lit = source.to_string_lossy().replace('\'', "''");
        let server = Server::new(Config {
            max_copy_input_bytes: 8,
            ..Config::default()
        });
        server
            .execute_sql("create table public.copy_events(id integer, payload varchar(64))")
            .expect("create table");

        let err = server
            .execute_sql(&format!("copy public.copy_events from '{source_lit}' csv"))
            .expect_err("COPY should fail");
        assert!(
            err.to_string().contains("maxCopyInputBytes"),
            "COPY error = {err}"
        );
        let result = server
            .execute_sql("select count(*) from public.copy_events")
            .expect("count rows");
        assert_eq!(result.rows, vec![vec!["0".to_string()]]);
    }
}
