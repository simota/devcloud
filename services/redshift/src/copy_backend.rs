//! Redshift `COPY ... FROM 's3://...'` and `UNLOAD ('...') TO 's3://...'` for
//! SQL backends that do not implement them themselves (PostgreSQL).
//!
//! devcloud reads COPY objects from the local S3 store, parses them the way
//! Redshift does, and loads the rows with `INSERT` statements on the client's
//! own backend session — so a COPY into a temp table, or inside an open
//! transaction, behaves as it does on Redshift.
//!
//! For UNLOAD, devcloud translates the inner query through the server's
//! `RedshiftTranslator`, runs it on the client's backend session (or the
//! backend connection), formats the result rows as delimited text, CSV, or
//! JSON Lines (optionally GZIP-compressed, with `HEADER`, `DELIMITER`,
//! `NULL AS`, and `ALLOWOVERWRITE` / `CLEANPATH`), and writes the output to the
//! local S3 store.

use std::io::{Read, Write};

use devcloud_s3::objops::PutObjectInput;

use crate::backend::{ExecResult, Field, SqlSession};
use crate::copy_unload::{parse_csv_delimiter, tokenize_sql_options};
use crate::engine::QueryResult;
use crate::errors::SqlError;
use crate::server::{default_str, Server};
use crate::sql_parse::{matching_paren, parse_leading_sql_string_literal};
use crate::translator::Session as TranslatorSession;

/// Rows per generated INSERT.
const INSERT_BATCH_ROWS: usize = 500;

/// Whether `statement` is a COPY (Redshift has only `COPY ... FROM`).
pub(crate) fn is_copy(statement: &str) -> bool {
    // Bytes, not a str slice: the statement may start with any UTF-8 text.
    let b = statement.trim_start().as_bytes();
    b.len() > 4 && b[..4].eq_ignore_ascii_case(b"copy") && b[4].is_ascii_whitespace()
}

/// Whether `statement` is an UNLOAD (`UNLOAD ('...') TO 's3://...'`).
pub(crate) fn is_unload(statement: &str) -> bool {
    let b = statement.trim_start().as_bytes();
    b.len() > 6
        && b[..6].eq_ignore_ascii_case(b"unload")
        && (b[6].is_ascii_whitespace() || b[6] == b'(')
}

/// Byte offset of the `FROM` keyword: whitespace before it, whitespace or a
/// quote after it, and not inside a double-quoted identifier.
fn find_from_keyword(body: &str) -> Option<usize> {
    let b = body.as_bytes();
    let mut in_ident = false;
    let mut i = 0;
    while i + 4 <= b.len() {
        match b[i] {
            b'"' => in_ident = !in_ident,
            _ if !in_ident
                && i > 0
                && b[i - 1].is_ascii_whitespace()
                && b[i..i + 4].eq_ignore_ascii_case(b"from")
                && b.get(i + 4)
                    .is_some_and(|c| c.is_ascii_whitespace() || *c == b'\'') =>
            {
                return Some(i);
            }
            _ => {}
        }
        i += 1;
    }
    None
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Format {
    /// Redshift's default: delimited text, no quoting.
    Text,
    Csv,
    Json {
        ignore_case: bool,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Options {
    format: Format,
    delimiter: Option<char>,
    gzip: bool,
    ignore_header: usize,
    null_as: Option<String>,
    empty_as_null: bool,
    blanks_as_null: bool,
}

impl Default for Options {
    fn default() -> Self {
        Options {
            format: Format::Text,
            delimiter: None,
            gzip: false,
            ignore_header: 0,
            null_as: None,
            empty_as_null: false,
            blanks_as_null: false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CopyStatement {
    /// The target exactly as written (it is put back into SQL unchanged).
    table: String,
    source: String,
    options: Options,
}

/// A target column: its name and whether it holds character data (which
/// keeps an empty field as `''` rather than NULL).
struct TargetColumn {
    name: String,
    is_string: bool,
}

/// One field as read from the file. `quoted` is a CSV field in quotes.
struct RawField {
    text: String,
    quoted: bool,
}

impl Server {
    pub(crate) fn copy_through_backend(
        &self,
        backend_session: &mut Option<Box<dyn SqlSession>>,
        statement: &str,
    ) -> Result<QueryResult, SqlError> {
        let copy = parse_copy(statement)?;
        let mut exec = |sql: &str| -> Result<ExecResult, SqlError> {
            match backend_session.as_mut() {
                Some(s) => s.exec(sql),
                None => self.backend().exec(sql),
            }
        };
        let columns = target_columns(&mut exec, &copy.table)?;
        // Every object is read and parsed before anything is written, so a
        // bad file anywhere under the prefix loads nothing.
        let mut rows = Vec::new();
        for data in self.read_copy_objects(&copy)? {
            let text = String::from_utf8(data)
                .map_err(|_| SqlError::new("COPY input is not valid UTF-8"))?;
            rows.extend(match copy.options.format {
                Format::Json { ignore_case } => json_rows(&text, &columns, ignore_case)?,
                Format::Csv | Format::Text => delimited_rows(&text, &copy.options, &columns)?,
            });
        }
        self.check_row_sizes(&rows)?;
        if !rows.is_empty() {
            // One simple-protocol Query: PostgreSQL runs its statements as a
            // single implicit transaction, so a failing row (a type error)
            // undoes the whole COPY; inside an explicit transaction the
            // failure aborts that transaction, as a failed COPY does on
            // Redshift.
            let inserts: Vec<String> = rows
                .chunks(INSERT_BATCH_ROWS)
                .map(|batch| insert_statement(&copy.table, &columns, batch))
                .collect();
            exec(&inserts.join(";\n"))?;
        }
        Ok(QueryResult::tag_only(&format!("COPY {}", rows.len())))
    }

    /// The objects a COPY source names: every key under the prefix, in key
    /// order, decompressed when `GZIP` is set.
    fn read_copy_objects(&self, copy: &CopyStatement) -> Result<Vec<Vec<u8>>, SqlError> {
        let Some(store) = &self.shared.config.object_store else {
            return Err(SqlError::new(
                "COPY from s3 URI requires local S3 service to be enabled",
            ));
        };
        let Some(rest) = copy
            .source
            .get(..5)
            .filter(|scheme| scheme.eq_ignore_ascii_case("s3://"))
            .map(|_| &copy.source[5..])
        else {
            return Err(SqlError::new("COPY supports only s3:// sources"));
        };
        let (bucket, prefix) = rest.split_once('/').unwrap_or((rest, ""));
        let objects = store
            .list_objects(bucket, prefix)
            .map_err(|err| SqlError::new(format!("COPY list S3 objects: {err:?}")))?
            .ok_or_else(|| SqlError::new(format!("COPY source bucket {bucket} does not exist")))?;
        if objects.is_empty() {
            return Err(SqlError::new(format!(
                "The specified S3 prefix '{prefix}' does not exist"
            )));
        }
        let mut out = Vec::with_capacity(objects.len());
        for object in objects {
            let Some((_, data)) = store
                .get_object(bucket, &object.key)
                .map_err(|err| SqlError::new(format!("COPY read S3 object: {err:?}")))?
            else {
                continue;
            };
            out.push(if copy.options.gzip {
                gunzip(&data).map_err(|err| {
                    SqlError::new(format!("COPY gunzip s3://{bucket}/{}: {err}", object.key))
                })?
            } else {
                data
            });
        }
        Ok(out)
    }

    /// Applies the configured per-row input limit (`maxInputRowBytes`).
    fn check_row_sizes(&self, rows: &[Vec<Option<String>>]) -> Result<(), SqlError> {
        let max = self.shared.config.max_copy_input_bytes;
        if max <= 0 {
            return Ok(());
        }
        for row in rows {
            let size: usize = row.iter().flatten().map(String::len).sum();
            if size as i64 > max {
                return Err(SqlError::new("COPY input row exceeds maxCopyInputBytes"));
            }
        }
        Ok(())
    }
}

fn parse_copy(statement: &str) -> Result<CopyStatement, SqlError> {
    let body = statement.trim().trim_end_matches(';').trim();
    let from = find_from_keyword(body).ok_or_else(|| SqlError::new("COPY requires FROM"))?;
    let table = body["copy".len()..from].trim().to_string();
    if table.is_empty() {
        return Err(SqlError::new("COPY requires a target table"));
    }
    if table.contains('(') {
        return Err(SqlError::new(
            "COPY with a column list is not supported by devcloud",
        ));
    }
    let after_from = body[from + "from".len()..].trim_start();
    let (source, rest) = parse_leading_sql_string_literal(after_from)
        .map_err(|err| SqlError::new(format!("COPY requires an s3:// source: {err}")))?;
    Ok(CopyStatement {
        table,
        source,
        options: parse_options(&rest)?,
    })
}

fn parse_options(text: &str) -> Result<Options, SqlError> {
    let tokens = tokenize_sql_options(text)?;
    let mut options = Options::default();
    let mut i = 0;
    // The value after `name [AS]`.
    let value = |i: &mut usize, name: &str| -> Result<String, SqlError> {
        if tokens
            .get(*i + 1)
            .is_some_and(|t| t.eq_ignore_ascii_case("as"))
        {
            *i += 1;
        }
        *i += 1;
        tokens
            .get(*i)
            .cloned()
            .ok_or_else(|| SqlError::new(format!("COPY {name} requires a value")))
    };
    while i < tokens.len() {
        let token = tokens[i].to_ascii_lowercase();
        match token.as_str() {
            "format" => {
                if tokens
                    .get(i + 1)
                    .is_some_and(|t| t.eq_ignore_ascii_case("as"))
                {
                    i += 1;
                }
                // The format keyword itself is read on the next turn.
            }
            "csv" => options.format = Format::Csv,
            "json" => {
                let arg = tokens.get(i + 1).map(|t| t.to_ascii_lowercase());
                options.format = match arg.as_deref() {
                    Some("auto") => Format::Json { ignore_case: false },
                    Some("auto ignorecase") => Format::Json { ignore_case: true },
                    _ => return Err(SqlError::new(
                        "COPY JSON is supported by devcloud only with 'auto' or 'auto ignorecase'",
                    )),
                };
                i += 1;
            }
            "delimiter" => {
                options.delimiter = Some(parse_csv_delimiter(&value(&mut i, "DELIMITER")?)?)
            }
            "gzip" => options.gzip = true,
            "ignoreheader" => {
                let n = value(&mut i, "IGNOREHEADER")?;
                options.ignore_header = n.parse().map_err(|_| {
                    SqlError::new("COPY IGNOREHEADER requires a non-negative row count")
                })?;
            }
            "null" => options.null_as = Some(value(&mut i, "NULL AS")?),
            "emptyasnull" => options.empty_as_null = true,
            "blanksasnull" => options.blanks_as_null = true,
            "iam_role"
            | "credentials"
            | "access_key_id"
            | "secret_access_key"
            | "session_token"
            | "master_symmetric_key" => {
                i += 1;
            }
            "region" => {
                value(&mut i, "REGION")?;
            }
            "compupdate" | "statupdate" => {
                if tokens.get(i + 1).is_some_and(|t| {
                    matches!(
                        t.to_ascii_lowercase().as_str(),
                        "on" | "off" | "true" | "false" | "preset"
                    )
                }) {
                    i += 1;
                }
            }
            "" => {}
            other => {
                return Err(SqlError::new(format!(
                    "COPY option {} is not supported by devcloud",
                    other.to_ascii_uppercase()
                )))
            }
        }
        i += 1;
    }
    Ok(options)
}

/// The target's columns in table order, read on the session that will load
/// them (so a temp table of this session resolves).
fn target_columns(
    exec: &mut impl FnMut(&str) -> Result<ExecResult, SqlError>,
    table: &str,
) -> Result<Vec<TargetColumn>, SqlError> {
    let query = format!(
        "SELECT a.attname, t.typcategory FROM pg_catalog.pg_attribute a \
         JOIN pg_catalog.pg_type t ON t.oid = a.atttypid \
         WHERE a.attrelid = to_regclass({}) AND a.attnum > 0 AND NOT a.attisdropped \
         ORDER BY a.attnum",
        sql_literal(table)
    );
    let result = exec(&query)?;
    if result.rows.is_empty() {
        return Err(SqlError::new(format!("relation {table} does not exist")));
    }
    Ok(result
        .rows
        .into_iter()
        .map(|row| TargetColumn {
            is_string: row.get(1).is_some_and(|c| c == "S"),
            name: row.into_iter().next().unwrap_or_default(),
        })
        .collect())
}

/// Text or CSV input to rows, with Redshift's NULL rules: a field equal to
/// `NULL AS` (default `\N`) is NULL; an empty field is NULL except in a
/// character column, where it stays `''` unless `EMPTYASNULL`.
fn delimited_rows(
    text: &str,
    options: &Options,
    columns: &[TargetColumn],
) -> Result<Vec<Vec<Option<String>>>, SqlError> {
    let records = match options.format {
        Format::Csv => csv_records(text, options.delimiter.unwrap_or(','))?,
        _ => {
            let delimiter = options.delimiter.unwrap_or('|');
            text.split('\n')
                .map(|line| line.strip_suffix('\r').unwrap_or(line))
                .filter(|line| !line.is_empty())
                .map(|line| {
                    line.split(delimiter)
                        .map(|f| RawField {
                            text: f.to_string(),
                            quoted: false,
                        })
                        .collect::<Vec<_>>()
                })
                .collect::<Vec<_>>()
        }
    };
    let null_as = options.null_as.as_deref().unwrap_or("\\N");
    let mut rows = Vec::with_capacity(records.len());
    for (index, record) in records.into_iter().enumerate().skip(options.ignore_header) {
        if record.len() != columns.len() {
            return Err(SqlError::new(format!(
                "COPY line {} has {} fields for {} columns",
                index + 1,
                record.len(),
                columns.len()
            )));
        }
        let row = record
            .into_iter()
            .zip(columns)
            .map(|(field, column)| field_value(field, column, null_as, options))
            .collect();
        rows.push(row);
    }
    Ok(rows)
}

fn field_value(
    field: RawField,
    column: &TargetColumn,
    null_as: &str,
    options: &Options,
) -> Option<String> {
    if !field.quoted && field.text == null_as {
        return None;
    }
    if field.text.is_empty() {
        return (column.is_string && !options.empty_as_null).then(String::new);
    }
    if column.is_string && options.blanks_as_null && field.text.trim().is_empty() {
        return None;
    }
    Some(field.text)
}

/// RFC 4180 records: quoted fields may hold the delimiter, `""`, and line
/// breaks. Blank lines are skipped.
fn csv_records(text: &str, delimiter: char) -> Result<Vec<Vec<RawField>>, SqlError> {
    let mut records = Vec::new();
    let mut record: Vec<RawField> = Vec::new();
    let mut field = String::new();
    let mut quoted = false;
    let mut in_quotes = false;
    let mut chars = text.chars().peekable();
    let mut line_has_content = false;
    while let Some(c) = chars.next() {
        if in_quotes {
            if c == '"' {
                if chars.peek() == Some(&'"') {
                    chars.next();
                    field.push('"');
                } else {
                    in_quotes = false;
                }
            } else {
                field.push(c);
            }
            continue;
        }
        match c {
            '"' if field.is_empty() && !quoted => {
                in_quotes = true;
                quoted = true;
                line_has_content = true;
            }
            c if c == delimiter => {
                record.push(RawField {
                    text: std::mem::take(&mut field),
                    quoted,
                });
                quoted = false;
                line_has_content = true;
            }
            '\r' if chars.peek() == Some(&'\n') => {}
            '\n' => {
                if line_has_content {
                    record.push(RawField {
                        text: std::mem::take(&mut field),
                        quoted,
                    });
                    records.push(std::mem::take(&mut record));
                }
                quoted = false;
                line_has_content = false;
            }
            c => {
                field.push(c);
                line_has_content = true;
            }
        }
    }
    if in_quotes {
        return Err(SqlError::new("COPY CSV input ends inside a quoted field"));
    }
    if line_has_content {
        record.push(RawField {
            text: field,
            quoted,
        });
        records.push(record);
    }
    Ok(records)
}

/// `JSON 'auto'`: a stream of objects (one per line or concatenated); keys
/// match column names exactly (`ignorecase`: case-insensitively). Missing
/// keys and JSON null are NULL; nested values load as their JSON text.
fn json_rows(
    text: &str,
    columns: &[TargetColumn],
    ignore_case: bool,
) -> Result<Vec<Vec<Option<String>>>, SqlError> {
    let mut rows = Vec::new();
    for (index, value) in serde_json::Deserializer::from_str(text)
        .into_iter::<serde_json::Value>()
        .enumerate()
    {
        let value = value.map_err(|err| SqlError::new(format!("COPY read JSON: {err}")))?;
        let serde_json::Value::Object(object) = value else {
            return Err(SqlError::new(format!(
                "COPY JSON record {} is not an object",
                index + 1
            )));
        };
        let lookup = |name: &str| {
            if ignore_case {
                object
                    .iter()
                    .find(|(k, _)| k.eq_ignore_ascii_case(name))
                    .map(|(_, v)| v)
            } else {
                object.get(name)
            }
        };
        let row = columns
            .iter()
            .map(|column| match lookup(&column.name) {
                None | Some(serde_json::Value::Null) => None,
                Some(serde_json::Value::String(s)) if s.is_empty() && !column.is_string => None,
                Some(serde_json::Value::String(s)) => Some(s.clone()),
                Some(serde_json::Value::Bool(b)) => Some(b.to_string()),
                Some(serde_json::Value::Number(n)) => Some(n.to_string()),
                Some(other) => Some(other.to_string()),
            })
            .collect();
        rows.push(row);
    }
    Ok(rows)
}

fn insert_statement(table: &str, columns: &[TargetColumn], rows: &[Vec<Option<String>>]) -> String {
    let names: Vec<String> = columns
        .iter()
        .map(|c| format!("\"{}\"", c.name.replace('"', "\"\"")))
        .collect();
    let values: Vec<String> = rows
        .iter()
        .map(|row| {
            let fields: Vec<String> = row
                .iter()
                .map(|v| v.as_deref().map_or_else(|| "NULL".to_string(), sql_literal))
                .collect();
            format!("({})", fields.join(", "))
        })
        .collect();
    format!(
        "INSERT INTO {table} ({}) VALUES {}",
        names.join(", "),
        values.join(", ")
    )
}

/// An escape-string literal: its meaning does not depend on the server's
/// `standard_conforming_strings`.
fn sql_literal(value: &str) -> String {
    format!("E'{}'", value.replace('\\', "\\\\").replace('\'', "''"))
}

fn gunzip(data: &[u8]) -> std::io::Result<Vec<u8>> {
    let mut out = Vec::new();
    flate2::read::MultiGzDecoder::new(data).read_to_end(&mut out)?;
    Ok(out)
}

fn gzip(data: &[u8]) -> std::io::Result<Vec<u8>> {
    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    encoder.write_all(data)?;
    encoder.finish()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum UnloadFormat {
    Text,
    Csv,
    Json,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct UnloadOptions {
    format: UnloadFormat,
    delimiter: Option<char>,
    gzip: bool,
    header: bool,
    null_as: Option<String>,
    allow_overwrite: bool,
    clean_path: bool,
}

impl Default for UnloadOptions {
    fn default() -> Self {
        UnloadOptions {
            format: UnloadFormat::Text,
            delimiter: None,
            gzip: false,
            header: false,
            null_as: None,
            allow_overwrite: false,
            clean_path: false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct UnloadStatement {
    select_sql: String,
    target_prefix: String,
    options: UnloadOptions,
}

impl Server {
    pub(crate) fn unload_through_backend(
        &self,
        backend_session: &mut Option<Box<dyn SqlSession>>,
        statement: &str,
    ) -> Result<QueryResult, SqlError> {
        let unload = parse_unload(statement)?;
        if unload.options.format == UnloadFormat::Json {
            if unload.options.header {
                return Err(SqlError::new("UNLOAD HEADER is not supported with JSON"));
            }
            if unload.options.delimiter.is_some() {
                return Err(SqlError::new("UNLOAD DELIMITER is not supported with JSON"));
            }
            if unload.options.null_as.is_some() {
                return Err(SqlError::new("UNLOAD NULL AS is not supported with JSON"));
            }
        }
        let Some(store) = &self.shared.config.object_store else {
            return Err(SqlError::new(
                "UNLOAD to s3 URI requires local S3 service to be enabled",
            ));
        };
        let (bucket, prefix) = parse_unload_s3_uri(&unload.target_prefix)?;

        let existing = store
            .list_objects(&bucket, &prefix)
            .map_err(|err| SqlError::new(format!("UNLOAD list S3 objects: {err:?}")))?
            .ok_or_else(|| {
                SqlError::new(format!("UNLOAD target bucket {bucket} does not exist"))
            })?;

        if !existing.is_empty() && !unload.options.allow_overwrite && !unload.options.clean_path {
            return Err(SqlError::new(format!(
                "Specified unload destination on S3 is not empty: s3://{bucket}/{prefix}. Consider using a different bucket / prefix, manually removing the target files in S3, or using the ALLOWOVERWRITE option."
            )));
        }

        let session = TranslatorSession {
            database: default_str(&self.shared.config.database, "dev"),
            user: default_str(&self.shared.config.user, "dev"),
            schema: "public".to_string(),
        };
        let translated = self.translator.translate(&session, &unload.select_sql)?;
        let backend_sql = if translated.backend_sql.trim().is_empty() {
            unload.select_sql.as_str()
        } else {
            translated.backend_sql.as_str()
        };

        let (fields, rows) = match backend_session.as_mut() {
            Some(s) => s.exec_nullable(backend_sql)?,
            None => self.backend().exec_nullable(backend_sql)?,
        };

        let formatted = format_unload_output(&fields, &rows, &unload.options);
        let body = if unload.options.gzip {
            gzip(formatted.as_bytes())
                .map_err(|err| SqlError::new(format!("UNLOAD gzip: {err}")))?
        } else {
            formatted.into_bytes()
        };

        if unload.options.clean_path {
            for obj in existing {
                store
                    .delete_object(&bucket, &obj.key)
                    .map_err(|err| SqlError::new(format!("UNLOAD clean S3 object: {err:?}")))?;
            }
        }

        let suffix = match (unload.options.format, unload.options.gzip) {
            (UnloadFormat::Json, true) => "000.json.gz",
            (UnloadFormat::Json, false) => "000.json",
            (_, true) => "000.gz",
            (_, false) => "000",
        };
        let content_type = match (unload.options.format, unload.options.gzip) {
            (_, true) => "application/gzip",
            (UnloadFormat::Json, false) => "application/json",
            (UnloadFormat::Csv, false) => "text/csv",
            (UnloadFormat::Text, false) => "text/plain",
        };
        let key = format!("{prefix}{suffix}");
        store
            .put_object(PutObjectInput {
                bucket,
                key,
                body,
                content_type: content_type.to_string(),
                ..Default::default()
            })
            .map_err(|err| SqlError::new(format!("UNLOAD write S3 object: {err:?}")))?;

        Ok(QueryResult::tag_only("UNLOAD"))
    }
}

fn parse_unload(statement: &str) -> Result<UnloadStatement, SqlError> {
    let body = statement.trim().trim_end_matches(';').trim();
    let rest = body["unload".len()..].trim_start();
    if !rest.starts_with('(') {
        return Err(SqlError::new("UNLOAD requires a parenthesized SELECT"));
    }
    let close = matching_paren(rest, 0)
        .ok_or_else(|| SqlError::new("UNLOAD has an unterminated SELECT"))?;
    let inner = rest[1..close].trim();
    let (select_sql, inner_rest) = parse_leading_sql_string_literal(inner).map_err(|err| {
        SqlError::new(format!(
            "UNLOAD requires SELECT SQL as a string literal: {err}"
        ))
    })?;
    if !inner_rest.trim().is_empty() {
        return Err(SqlError::new(
            "UNLOAD requires SELECT SQL as a single string literal",
        ));
    }
    let select_sql = select_sql.trim().trim_end_matches(';').trim().to_string();
    if select_sql.is_empty() {
        return Err(SqlError::new("UNLOAD SELECT query cannot be empty"));
    }
    let after_select = rest[close + 1..].trim_start();
    let after_bytes = after_select.as_bytes();
    if after_bytes.len() < 2
        || !after_bytes[..2].eq_ignore_ascii_case(b"to")
        || !after_bytes
            .get(2)
            .is_some_and(|c| c.is_ascii_whitespace() || *c == b'\'')
    {
        return Err(SqlError::new("UNLOAD requires TO"));
    }
    let after_to = after_select["to".len()..].trim_start();
    let (target_prefix, options_text) = parse_leading_sql_string_literal(after_to)
        .map_err(|err| SqlError::new(format!("UNLOAD requires an s3:// target prefix: {err}")))?;
    Ok(UnloadStatement {
        select_sql,
        target_prefix,
        options: parse_unload_options(&options_text)?,
    })
}

fn parse_unload_s3_uri(uri: &str) -> Result<(String, String), SqlError> {
    let Some(rest) = uri
        .get(..5)
        .filter(|scheme| scheme.eq_ignore_ascii_case("s3://"))
        .map(|_| &uri[5..])
    else {
        return Err(SqlError::new("UNLOAD supports only s3:// targets"));
    };
    match rest.split_once('/') {
        Some((bucket, prefix)) if !bucket.is_empty() && !prefix.is_empty() => {
            Ok((bucket.to_string(), prefix.to_string()))
        }
        _ => Err(SqlError::new(
            "UNLOAD s3:// URI requires bucket and key prefix",
        )),
    }
}

fn parse_unload_options(text: &str) -> Result<UnloadOptions, SqlError> {
    let tokens = tokenize_sql_options(text)?;
    let mut options = UnloadOptions::default();
    let mut i = 0;
    let value = |i: &mut usize, name: &str| -> Result<String, SqlError> {
        if tokens
            .get(*i + 1)
            .is_some_and(|t| t.eq_ignore_ascii_case("as"))
        {
            *i += 1;
        }
        *i += 1;
        tokens
            .get(*i)
            .cloned()
            .ok_or_else(|| SqlError::new(format!("UNLOAD {name} requires a value")))
    };
    while i < tokens.len() {
        let token = tokens[i].to_ascii_lowercase();
        match token.as_str() {
            "format" => {
                if tokens
                    .get(i + 1)
                    .is_some_and(|t| t.eq_ignore_ascii_case("as"))
                {
                    i += 1;
                }
                i += 1;
                let Some(fmt) = tokens.get(i) else {
                    return Err(SqlError::new("UNLOAD FORMAT requires a value"));
                };
                match fmt.to_ascii_lowercase().as_str() {
                    "csv" => options.format = UnloadFormat::Csv,
                    "json" => options.format = UnloadFormat::Json,
                    other => {
                        return Err(SqlError::new(format!(
                            "UNLOAD FORMAT {} is not supported by devcloud",
                            other.to_ascii_uppercase()
                        )))
                    }
                }
            }
            "csv" => options.format = UnloadFormat::Csv,
            "json" => options.format = UnloadFormat::Json,
            "delimiter" => {
                options.delimiter = Some(parse_csv_delimiter(&value(&mut i, "DELIMITER")?)?)
            }
            "gzip" => options.gzip = true,
            "header" => options.header = true,
            "null" => options.null_as = Some(value(&mut i, "NULL AS")?),
            "allowoverwrite" => options.allow_overwrite = true,
            "cleanpath" => options.clean_path = true,
            "parallel" => {
                let v = value(&mut i, "PARALLEL")?;
                if !matches!(
                    v.to_ascii_lowercase().as_str(),
                    "on" | "off" | "true" | "false"
                ) {
                    return Err(SqlError::new("UNLOAD PARALLEL requires ON or OFF"));
                }
            }
            "iam_role"
            | "credentials"
            | "access_key_id"
            | "secret_access_key"
            | "session_token"
            | "master_symmetric_key" => {
                i += 1;
            }
            "region" => {
                value(&mut i, "REGION")?;
            }
            "" => {}
            other => {
                return Err(SqlError::new(format!(
                    "UNLOAD option {} is not supported by devcloud",
                    other.to_ascii_uppercase()
                )))
            }
        }
        i += 1;
    }
    Ok(options)
}

fn format_unload_output(
    fields: &[Field],
    rows: &[Vec<Option<String>>],
    options: &UnloadOptions,
) -> String {
    match options.format {
        UnloadFormat::Csv => format_unload_csv(fields, rows, options),
        UnloadFormat::Text => format_unload_text(fields, rows, options),
        UnloadFormat::Json => format_unload_json(fields, rows),
    }
}

fn format_unload_csv(
    fields: &[Field],
    rows: &[Vec<Option<String>>],
    options: &UnloadOptions,
) -> String {
    let delimiter = options.delimiter.unwrap_or(',');
    let null_str = options.null_as.as_deref().unwrap_or("");
    let mut out = String::new();
    if options.header && !fields.is_empty() {
        let header: Vec<&str> = fields.iter().map(|f| f.name.as_str()).collect();
        write_unload_csv_line(&mut out, &header, delimiter);
    }
    for row in rows {
        let cols: Vec<&str> = row
            .iter()
            .map(|v| v.as_deref().unwrap_or(null_str))
            .collect();
        write_unload_csv_line(&mut out, &cols, delimiter);
    }
    out
}

fn write_unload_csv_line(out: &mut String, cols: &[&str], delimiter: char) {
    for (i, field) in cols.iter().enumerate() {
        if i > 0 {
            out.push(delimiter);
        }
        if field
            .chars()
            .any(|c| c == delimiter || c == '"' || c == '\r' || c == '\n')
        {
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

fn format_unload_text(
    fields: &[Field],
    rows: &[Vec<Option<String>>],
    options: &UnloadOptions,
) -> String {
    let delimiter = options.delimiter.unwrap_or('|');
    let null_str = options.null_as.as_deref().unwrap_or("");
    let mut out = String::new();
    if options.header && !fields.is_empty() {
        for (i, f) in fields.iter().enumerate() {
            if i > 0 {
                out.push(delimiter);
            }
            out.push_str(&f.name);
        }
        out.push('\n');
    }
    for row in rows {
        for (i, v) in row.iter().enumerate() {
            if i > 0 {
                out.push(delimiter);
            }
            out.push_str(v.as_deref().unwrap_or(null_str));
        }
        out.push('\n');
    }
    out
}

fn format_unload_json(fields: &[Field], rows: &[Vec<Option<String>>]) -> String {
    let mut out = String::new();
    for row in rows {
        let mut obj = serde_json::Map::with_capacity(fields.len());
        for (i, field) in fields.iter().enumerate() {
            let val = match row.get(i).and_then(|v| v.as_deref()) {
                None => serde_json::Value::Null,
                Some(text) => json_value_for_oid(text, field.type_oid),
            };
            obj.insert(field.name.clone(), val);
        }
        if let Ok(line) = serde_json::to_string(&serde_json::Value::Object(obj)) {
            out.push_str(&line);
            out.push('\n');
        }
    }
    out
}

fn json_value_for_oid(text: &str, type_oid: i32) -> serde_json::Value {
    match type_oid {
        16 => match text.to_ascii_lowercase().as_str() {
            "t" | "true" | "1" => serde_json::Value::Bool(true),
            "f" | "false" | "0" => serde_json::Value::Bool(false),
            _ => serde_json::Value::String(text.to_string()),
        },
        20 | 21 | 23 => text
            .parse::<i64>()
            .map(|n| serde_json::Value::Number(n.into()))
            .unwrap_or_else(|_| serde_json::Value::String(text.to_string())),
        700 | 701 | 1700 => text
            .parse::<f64>()
            .ok()
            .and_then(serde_json::Number::from_f64)
            .map(serde_json::Value::Number)
            .unwrap_or_else(|| serde_json::Value::String(text.to_string())),
        114 | 3802 => serde_json::from_str(text)
            .unwrap_or_else(|_| serde_json::Value::String(text.to_string())),
        _ => serde_json::Value::String(text.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cols(spec: &[(&str, bool)]) -> Vec<TargetColumn> {
        spec.iter()
            .map(|(name, is_string)| TargetColumn {
                name: name.to_string(),
                is_string: *is_string,
            })
            .collect()
    }

    #[test]
    fn parses_the_statements_pushone_csa_sends() {
        let c = parse_copy("COPY csa_stg_file FROM 's3://b/in/a.csv' IAM_ROLE 'arn:aws:iam::1:role/r' FORMAT AS CSV GZIP").unwrap();
        assert_eq!(c.table, "csa_stg_file");
        assert_eq!(c.source, "s3://b/in/a.csv");
        assert_eq!(c.options.format, Format::Csv);
        assert!(c.options.gzip);

        let c = parse_copy(r"COPY csa_stg_file FROM 's3://b/a.tsv' IAM_ROLE 'r' DELIMITER '\t'")
            .unwrap();
        assert_eq!(
            (c.options.format, c.options.delimiter),
            (Format::Text, Some('\t'))
        );

        let c =
            parse_copy("COPY \"s\".\"t\" FROM 's3://b/a.json' IAM_ROLE 'r' FORMAT AS JSON 'auto';")
                .unwrap();
        assert_eq!(c.table, "\"s\".\"t\"");
        assert_eq!(c.options.format, Format::Json { ignore_case: false });
    }

    #[test]
    fn other_options() {
        let o = parse_options("CSV IGNOREHEADER AS 1 NULL AS 'NA' EMPTYASNULL BLANKSASNULL REGION AS 'us-east-1' COMPUPDATE OFF").unwrap();
        assert_eq!(o.ignore_header, 1);
        assert_eq!(o.null_as.as_deref(), Some("NA"));
        assert!(o.empty_as_null && o.blanks_as_null);
        let o = parse_options("JSON 'auto ignorecase' DELIMITER ','").unwrap();
        assert_eq!(o.format, Format::Json { ignore_case: true });
        for bad in [
            "MANIFEST",
            "JSON 's3://b/paths.json'",
            "BZIP2",
            "FIXEDWIDTH 'a:1'",
            "FORMAT AS PARQUET",
        ] {
            assert!(parse_options(bad).is_err(), "{bad}");
        }
        assert!(parse_copy("COPY t (a, b) FROM 's3://b/k'").is_err());
    }

    #[test]
    fn csv_null_rules() {
        let options = Options {
            format: Format::Csv,
            ..Options::default()
        };
        let columns = cols(&[("id", false), ("name", true), ("note", true), ("n", false)]);
        let rows = delimited_rows("1,\"a, b\",,\n2,\"\",\\N,\"\"\n", &options, &columns).unwrap();
        assert_eq!(
            rows,
            vec![
                vec![
                    Some("1".into()),
                    Some("a, b".into()),
                    Some(String::new()),
                    None
                ],
                vec![Some("2".into()), Some(String::new()), None, None],
            ]
        );
        let options = Options {
            format: Format::Csv,
            empty_as_null: true,
            ignore_header: 1,
            ..Options::default()
        };
        let rows = delimited_rows("id,name,note,n\n1,x,,5\n", &options, &columns).unwrap();
        assert_eq!(
            rows,
            vec![vec![
                Some("1".into()),
                Some("x".into()),
                None,
                Some("5".into())
            ]]
        );
    }

    #[test]
    fn csv_quotes_span_lines_and_escape_quotes() {
        let records = csv_records("\"a\nb\",\"say \"\"hi\"\"\"\r\n\r\nc,d", ',').unwrap();
        let texts: Vec<Vec<&str>> = records
            .iter()
            .map(|r| r.iter().map(|f| f.text.as_str()).collect())
            .collect();
        assert_eq!(texts, vec![vec!["a\nb", "say \"hi\""], vec!["c", "d"]]);
        assert!(csv_records("\"open", ',').is_err());
    }

    #[test]
    fn text_format_is_unquoted() {
        let options = Options {
            delimiter: Some('\t'),
            ..Options::default()
        };
        let columns = cols(&[("id", false), ("name", true)]);
        let rows = delimited_rows("1\t\"q\"\r\n2\t\\N\n", &options, &columns).unwrap();
        assert_eq!(
            rows,
            vec![
                vec![Some("1".into()), Some("\"q\"".into())],
                vec![Some("2".into()), None]
            ]
        );
        assert!(delimited_rows("1\t2\t3\n", &options, &columns).is_err());
    }

    #[test]
    fn json_auto_matches_column_names() {
        let columns = cols(&[
            ("id", false),
            ("name", true),
            ("meta", true),
            ("missing", true),
        ]);
        let text = "{\"id\":1,\"name\":\"a\",\"meta\":{\"k\":[1]}}\n{\"id\":2,\"Name\":\"B\",\"meta\":null}";
        let rows = json_rows(text, &columns, false).unwrap();
        assert_eq!(
            rows[0],
            vec![
                Some("1".into()),
                Some("a".into()),
                Some("{\"k\":[1]}".into()),
                None
            ]
        );
        assert_eq!(rows[1], vec![Some("2".into()), None, None, None]);
        let rows = json_rows(text, &columns, true).unwrap();
        assert_eq!(rows[1][1].as_deref(), Some("B"));
        assert!(json_rows("[1]", &columns, false).is_err());
    }

    #[test]
    fn insert_literals_are_escaped() {
        let columns = cols(&[("a\"b", true), ("n", false)]);
        let sql = insert_statement("t", &columns, &[vec![Some("it's \\ ok".into()), None]]);
        assert_eq!(
            sql,
            "INSERT INTO t (\"a\"\"b\", \"n\") VALUES (E'it''s \\\\ ok', NULL)"
        );
    }

    #[test]
    fn gunzip_reads_concatenated_members() {
        use std::io::Write;
        let gz = |s: &str| {
            let mut e = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
            e.write_all(s.as_bytes()).unwrap();
            e.finish().unwrap()
        };
        let mut data = gz("a\n");
        data.extend(gz("b\n"));
        assert_eq!(gunzip(&data).unwrap(), b"a\nb\n");
        assert!(gunzip(b"plain").is_err());
    }

    #[test]
    fn is_copy_detects_the_keyword() {
        assert!(is_copy("  copy t from 's3://b/k'"));
        assert!(is_copy("COPY\n  t FROM 's3://b/k'"));
        assert!(!is_copy("COPYRIGHT"));
        assert!(!is_copy("select 'copy '"));
        // Multi-byte text where a fixed-width str slice would split a char.
        assert!(!is_copy("-- 猫\nSELECT 1;"));
        assert!(!is_copy("猫猫"));
    }

    #[test]
    fn from_may_follow_any_whitespace() {
        let c = parse_copy("COPY t\nFROM\n's3://b/k'\tIAM_ROLE 'r'\tCSV").unwrap();
        assert_eq!((c.table.as_str(), c.source.as_str()), ("t", "s3://b/k"));
        assert_eq!(c.options.format, Format::Csv);
        let c = parse_copy("COPY\t\"from table\" FROM's3://b/k'").unwrap();
        assert_eq!(c.table, "\"from table\"");
        let c = parse_copy("COPY fromage FROM 's3://b/k'").unwrap();
        assert_eq!(c.table, "fromage");
    }

    #[test]
    fn is_unload_detects_the_keyword() {
        assert!(is_unload("  unload ('select 1') to 's3://b/k'"));
        assert!(is_unload("UNLOAD('select 1') TO 's3://b/k'"));
        assert!(is_unload("UNLOAD\n  ('select 1') TO 's3://b/k'"));
        assert!(!is_unload("UNLOADER"));
        assert!(!is_unload("select 'unload '"));
        assert!(!is_unload("-- 猫\nSELECT 1;"));
        assert!(!is_unload("猫猫"));
    }

    #[test]
    fn parses_unload_statements_and_options() {
        let u = parse_unload(
            "UNLOAD ('SELECT id, name FROM t WHERE name = ''alice''') TO 's3://b/out/part_' IAM_ROLE 'arn:aws:iam::1:role/r' FORMAT AS CSV HEADER GZIP ALLOWOVERWRITE NULL AS 'NULL' PARALLEL OFF;",
        )
        .unwrap();
        assert_eq!(u.select_sql, "SELECT id, name FROM t WHERE name = 'alice'");
        assert_eq!(u.target_prefix, "s3://b/out/part_");
        assert_eq!(u.options.format, UnloadFormat::Csv);
        assert!(u.options.header);
        assert!(u.options.gzip);
        assert!(u.options.allow_overwrite);
        assert_eq!(u.options.null_as.as_deref(), Some("NULL"));

        let u = parse_unload(
            r"UNLOAD('select 1') TO's3://b/out/' CREDENTIALS 'aws_access_key_id=a;aws_secret_access_key=b' DELIMITER '\t' CLEANPATH REGION AS 'us-east-1'",
        )
        .unwrap();
        assert_eq!(u.options.format, UnloadFormat::Text);
        assert_eq!(u.options.delimiter, Some('\t'));
        assert!(u.options.clean_path);

        let u = parse_unload("UNLOAD ('select 1') TO 's3://b/out/' FORMAT AS JSON").unwrap();
        assert_eq!(u.options.format, UnloadFormat::Json);

        for bad in [
            "MANIFEST",
            "BZIP2",
            "FORMAT AS PARQUET",
            "PARTITION BY (dt)",
            "MAXFILESIZE 100 MB",
            "ENCRYPTED",
        ] {
            assert!(parse_unload_options(bad).is_err(), "{bad}");
        }
        assert!(parse_unload("UNLOAD 'select 1' TO 's3://b/k'").is_err());
        assert!(parse_unload("UNLOAD ('') TO 's3://b/k'").is_err());
        assert!(parse_unload("UNLOAD ('select 1') 's3://b/k'").is_err());
    }

    #[test]
    fn formats_unload_text_csv_and_json() {
        let fields = vec![
            Field {
                name: "id".into(),
                type_oid: 23,
                type_size: 4,
            },
            Field {
                name: "name".into(),
                type_oid: 1043,
                type_size: -1,
            },
            Field {
                name: "ok".into(),
                type_oid: 16,
                type_size: 1,
            },
        ];
        let rows = vec![
            vec![Some("1".into()), Some("a, \"b\"".into()), Some("t".into())],
            vec![Some("2".into()), None, Some("f".into())],
        ];

        let text = format_unload_output(
            &fields,
            &rows,
            &UnloadOptions {
                format: UnloadFormat::Text,
                header: true,
                null_as: Some("\\N".into()),
                ..UnloadOptions::default()
            },
        );
        assert_eq!(text, "id|name|ok\n1|a, \"b\"|t\n2|\\N|f\n");

        let csv = format_unload_output(
            &fields,
            &rows,
            &UnloadOptions {
                format: UnloadFormat::Csv,
                header: true,
                ..UnloadOptions::default()
            },
        );
        assert_eq!(csv, "id,name,ok\n1,\"a, \"\"b\"\"\",t\n2,,f\n");

        let json = format_unload_output(
            &fields,
            &rows,
            &UnloadOptions {
                format: UnloadFormat::Json,
                ..UnloadOptions::default()
            },
        );
        assert_eq!(
            json,
            "{\"id\":1,\"name\":\"a, \\\"b\\\"\",\"ok\":true}\n{\"id\":2,\"name\":null,\"ok\":false}\n"
        );
    }
}
