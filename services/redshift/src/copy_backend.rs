//! Redshift `COPY ... FROM 's3://...'` for SQL backends that do not implement
//! it themselves (PostgreSQL).
//!
//! devcloud reads the objects from the local S3 store, parses them the way
//! Redshift does, and loads the rows with `INSERT` statements on the client's
//! own backend session — so a COPY into a temp table, or inside an open
//! transaction, behaves as it does on Redshift.
//!
//! Supported: `FROM 's3://bucket/prefix'` (every object whose key starts with
//! the prefix, in key order), the default text format with `DELIMITER`,
//! `CSV` / `FORMAT AS CSV`, `JSON 'auto'` / `JSON 'auto ignorecase'`, `GZIP`,
//! `IGNOREHEADER`, `NULL AS`, `EMPTYASNULL`, `BLANKSASNULL`. Authorization
//! (`IAM_ROLE`, `CREDENTIALS`, ...) and `REGION` are accepted and ignored.
//! Any other option is refused rather than silently ignored.

use std::io::Read;

use crate::backend::{ExecResult, SqlSession};
use crate::copy_unload::{parse_csv_delimiter, tokenize_sql_options};
use crate::engine::QueryResult;
use crate::errors::SqlError;
use crate::server::Server;
use crate::sql_parse::parse_leading_sql_string_literal;

/// Rows per generated INSERT.
const INSERT_BATCH_ROWS: usize = 500;

/// Whether `statement` is a COPY (Redshift has only `COPY ... FROM`).
pub(crate) fn is_copy(statement: &str) -> bool {
    // Bytes, not a str slice: the statement may start with any UTF-8 text.
    let b = statement.trim_start().as_bytes();
    b.len() > 4 && b[..4].eq_ignore_ascii_case(b"copy") && b[4].is_ascii_whitespace()
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
}
