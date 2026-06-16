//! pgwire extended query protocol: Parse/Bind/Describe/Execute/Close/Sync,
//! prepared statements, and portals.
//!
//! Parity: `internal/services/redshift/pgwire_extended.rs`. Handlers take raw
//! message payloads (exactly as legacy do) so protocol-error behavior — and the
//! handler-level tests — match byte for byte. Write errors are ignored just
//! like legacy unchecked `writeMessage` calls; a broken peer surfaces as a read
//! error in the connection loop.

use std::collections::HashMap;
use std::io::Write;

use crate::engine::QueryResult;
use crate::errors::SqlError;
use crate::pgwire_codec::{
    parse_describe_or_close_payload, write_command_complete, write_data_row, write_error_response,
    write_message, write_parameter_description, write_row_description, PayloadReader,
};
use crate::server::Server;

/// Mirrors `extendedQuerySession`.
pub struct ExtendedQuerySession {
    statements: HashMap<String, ExtendedPreparedStatement>,
    portals: HashMap<String, ExtendedPortal>,
    pub failed: bool,
}

/// Mirrors `extendedPreparedStatement`.
#[derive(Debug, Clone, Default)]
struct ExtendedPreparedStatement {
    statement: String,
    parameter_oids: Vec<i32>,
}

/// Mirrors `extendedPortal`.
#[derive(Debug, Clone, Default)]
struct ExtendedPortal {
    statement_name: String,
    executable_statement: String,
    executed: bool,
    result: QueryResult,
    next_row: usize,
}

/// Mirrors `extendedBindParameter`.
#[derive(Debug, Clone, Default)]
struct ExtendedBindParameter {
    value: String,
    null: bool,
}

impl Default for ExtendedQuerySession {
    fn default() -> Self {
        Self::new()
    }
}

impl ExtendedQuerySession {
    /// Mirrors `newExtendedQuerySession`.
    pub fn new() -> ExtendedQuerySession {
        ExtendedQuerySession {
            statements: HashMap::new(),
            portals: HashMap::new(),
            failed: false,
        }
    }

    /// Mirrors `handleParse`.
    pub fn handle_parse(&mut self, w: &mut impl Write, payload: &[u8]) {
        let mut reader = PayloadReader::new(payload);
        let Some(name) = reader.cstring() else {
            self.protocol_error(w);
            return;
        };
        let Some(statement) = reader.cstring() else {
            self.protocol_error(w);
            return;
        };
        let Some(parameter_count) = reader.i16() else {
            self.protocol_error(w);
            return;
        };
        if parameter_count < 0 {
            self.protocol_error(w);
            return;
        }
        let mut parameter_oids = Vec::with_capacity(parameter_count as usize);
        for _ in 0..parameter_count {
            let Some(oid) = reader.i32() else {
                self.protocol_error(w);
                return;
            };
            parameter_oids.push(oid);
        }
        self.statements.insert(
            name,
            ExtendedPreparedStatement {
                statement,
                parameter_oids,
            },
        );
        let _ = write_message(w, b'1', &[]);
    }

    /// Mirrors `handleBind`.
    pub fn handle_bind(&mut self, w: &mut impl Write, payload: &[u8]) {
        let mut reader = PayloadReader::new(payload);
        let Some(portal_name) = reader.cstring() else {
            self.protocol_error(w);
            return;
        };
        let Some(statement_name) = reader.cstring() else {
            self.protocol_error(w);
            return;
        };
        let Some(prepared) = self.statements.get(&statement_name).cloned() else {
            self.failed = true;
            let _ = write_error_response(w, "26000", "prepared statement does not exist");
            return;
        };
        let Some(format_code_count) = reader.i16() else {
            self.protocol_error(w);
            return;
        };
        let Some(format_codes) = reader.i16_values(format_code_count) else {
            self.protocol_error(w);
            return;
        };
        let Some(parameter_count) = reader.i16() else {
            self.protocol_error(w);
            return;
        };
        if parameter_count < 0 {
            self.protocol_error(w);
            return;
        }
        if !prepared.parameter_oids.is_empty()
            && parameter_count as usize != prepared.parameter_oids.len()
        {
            self.failed = true;
            let _ = write_error_response(
                w,
                "08P01",
                "bind parameter count does not match prepared statement",
            );
            return;
        }
        let mut parameters = Vec::with_capacity(parameter_count as usize);
        for i in 0..parameter_count as usize {
            let format_code = match format_codes.len() {
                0 => 0,
                1 => format_codes[0],
                _ => {
                    if i >= format_codes.len() {
                        self.protocol_error(w);
                        return;
                    }
                    format_codes[i]
                }
            };
            if format_code != 0 {
                self.failed = true;
                let _ = write_error_response(
                    w,
                    "0A000",
                    "binary extended query bind parameters are not supported in the local Redshift compatibility layer",
                );
                return;
            }
            let Some(value_length) = reader.i32() else {
                self.protocol_error(w);
                return;
            };
            if value_length < -1 {
                self.protocol_error(w);
                return;
            }
            if value_length == -1 {
                parameters.push(ExtendedBindParameter {
                    null: true,
                    ..ExtendedBindParameter::default()
                });
                continue;
            }
            if value_length as usize > reader.remaining() {
                self.protocol_error(w);
                return;
            }
            let Some(value) = reader.bytes(value_length as usize) else {
                self.protocol_error(w);
                return;
            };
            parameters.push(ExtendedBindParameter {
                value: String::from_utf8_lossy(value).into_owned(),
                null: false,
            });
        }
        let executable_statement =
            match apply_extended_bind_parameters(&prepared.statement, &parameters) {
                Ok(statement) => statement,
                Err(err) => {
                    self.failed = true;
                    let _ = write_error_response(w, "08P01", &err.to_string());
                    return;
                }
            };
        let Some(result_format_count) = reader.i16() else {
            self.protocol_error(w);
            return;
        };
        let Some(result_format_codes) = reader.i16_values(result_format_count) else {
            self.protocol_error(w);
            return;
        };
        for format_code in result_format_codes {
            if format_code != 0 {
                self.failed = true;
                let _ = write_error_response(
                    w,
                    "0A000",
                    "binary extended query result formats are not supported in the local Redshift compatibility layer",
                );
                return;
            }
        }
        self.portals.insert(
            portal_name,
            ExtendedPortal {
                statement_name,
                executable_statement,
                ..ExtendedPortal::default()
            },
        );
        let _ = write_message(w, b'2', &[]);
    }

    /// Mirrors `handleDescribe`.
    pub fn handle_describe(&mut self, server: &Server, w: &mut impl Write, payload: &[u8]) {
        let Some((target_type, name)) = parse_describe_or_close_payload(payload) else {
            self.protocol_error(w);
            return;
        };
        let Some(statement) = self.statement_for_target(target_type, &name) else {
            self.failed = true;
            let _ = write_error_response(w, "26000", "prepared statement or portal does not exist");
            return;
        };
        if target_type == b'S' {
            let parameter_oids = self
                .statements
                .get(&name)
                .map(|prepared| prepared.parameter_oids.clone())
                .unwrap_or_default();
            let _ = write_parameter_description(w, &parameter_oids);
        }
        match server.describe_extended_query(&statement) {
            Some(result) if !result.fields.is_empty() => {
                let _ = write_row_description(w, &result.fields);
            }
            _ => {
                let _ = write_message(w, b'n', &[]);
            }
        }
    }

    /// Mirrors `handleExecute`.
    pub fn handle_execute(&mut self, server: &Server, w: &mut impl Write, payload: &[u8]) {
        let mut reader = PayloadReader::new(payload);
        let Some(portal_name) = reader.cstring() else {
            self.protocol_error(w);
            return;
        };
        let Some(max_rows) = reader.i32() else {
            self.protocol_error(w);
            return;
        };
        if max_rows < 0 {
            self.protocol_error(w);
            return;
        }
        let Some(mut portal) = self.portals.get(&portal_name).cloned() else {
            self.failed = true;
            let _ = write_error_response(w, "26000", "portal does not exist");
            return;
        };
        // Like legacy zero-value map read: a closed statement yields "".
        let prepared_statement = self
            .statements
            .get(&portal.statement_name)
            .map(|prepared| prepared.statement.clone())
            .unwrap_or_default();
        if !portal.executed {
            match server.execute_sql(&portal.executable_statement) {
                Ok(result) => {
                    server.record_sql_history(&prepared_statement, &result, None);
                    portal.result = result;
                    portal.executed = true;
                }
                Err(err) => {
                    server.record_sql_history(
                        &prepared_statement,
                        &QueryResult::default(),
                        Some(&err),
                    );
                    self.failed = true;
                    let _ = write_error_response(w, "0A000", &err.to_string());
                    return;
                }
            }
        }
        let rows_to_write = &portal.result.rows[portal.next_row..];
        let limit = if max_rows > 0 && (max_rows as usize) < rows_to_write.len() {
            max_rows as usize
        } else {
            rows_to_write.len()
        };
        for row in &rows_to_write[..limit] {
            let _ = write_data_row(w, row);
        }
        portal.next_row += limit;
        if portal.next_row < portal.result.rows.len() {
            self.portals.insert(portal_name, portal);
            let _ = write_message(w, b's', &[]);
            return;
        }
        let tag = portal.result.tag.clone();
        self.portals.insert(portal_name, portal);
        let _ = write_command_complete(w, &tag);
    }

    /// Mirrors `handleClose`.
    pub fn handle_close(&mut self, w: &mut impl Write, payload: &[u8]) {
        let Some((target_type, name)) = parse_describe_or_close_payload(payload) else {
            self.protocol_error(w);
            return;
        };
        match target_type {
            b'S' => {
                self.statements.remove(&name);
            }
            b'P' => {
                self.portals.remove(&name);
            }
            _ => {}
        }
        let _ = write_message(w, b'3', &[]);
    }

    /// Mirrors `handleSync` (clears the failed flag; ReadyForQuery is written
    /// by the connection loop).
    pub fn handle_sync(&mut self) {
        self.failed = false;
    }

    /// Mirrors `statementForTarget`.
    fn statement_for_target(&self, target_type: u8, name: &str) -> Option<String> {
        match target_type {
            b'S' => self
                .statements
                .get(name)
                .map(|prepared| prepared.statement.clone()),
            b'P' => {
                let portal = self.portals.get(name)?;
                self.statements.get(&portal.statement_name)?;
                Some(portal.executable_statement.clone())
            }
            _ => None,
        }
    }

    /// Mirrors `protocolError`.
    fn protocol_error(&mut self, w: &mut impl Write) {
        self.failed = true;
        let _ = write_error_response(w, "08P01", "invalid PostgreSQL extended query message");
    }
}

/// Mirrors `applyExtendedBindParameters`: substitutes `$N` placeholders
/// (outside single-quoted strings) with SQL literals.
fn apply_extended_bind_parameters(
    statement: &str,
    parameters: &[ExtendedBindParameter],
) -> Result<String, SqlError> {
    if parameters.is_empty() {
        return Ok(statement.to_string());
    }
    let bytes = statement.as_bytes();
    let mut builder: Vec<u8> = Vec::with_capacity(bytes.len());
    let mut in_string = false;
    let mut i = 0;
    while i < bytes.len() {
        let ch = bytes[i];
        if ch == b'\'' {
            builder.push(ch);
            if in_string && i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                i += 1;
                builder.push(bytes[i]);
                i += 1;
                continue;
            }
            in_string = !in_string;
            i += 1;
            continue;
        }
        if !in_string && ch == b'$' && i + 1 < bytes.len() && bytes[i + 1].is_ascii_digit() {
            let mut j = i + 1;
            while j < bytes.len() && bytes[j].is_ascii_digit() {
                j += 1;
            }
            let digits = &statement[i + 1..j];
            let index: usize = digits.parse().unwrap_or(0);
            if index < 1 || index > parameters.len() {
                return Err(SqlError::new(format!(
                    "bind parameter {} has no value",
                    &statement[i..j]
                )));
            }
            builder.extend_from_slice(
                sql_literal_for_extended_bind(&parameters[index - 1]).as_bytes(),
            );
            i = j;
            continue;
        }
        builder.push(ch);
        i += 1;
    }
    Ok(String::from_utf8_lossy(&builder).into_owned())
}

/// Mirrors `sqlLiteralForExtendedBind`.
fn sql_literal_for_extended_bind(parameter: &ExtendedBindParameter) -> String {
    if parameter.null {
        return "NULL".to_string();
    }
    format!("'{}'", parameter.value.replace('\'', "''"))
}

impl Server {
    /// Mirrors `describeExtendedQuery`: SELECT statements are dry-run through
    /// the engine to obtain a row description (rows discarded).
    pub(crate) fn describe_extended_query(&self, statement: &str) -> Option<QueryResult> {
        let normalized = statement.trim_end_matches(';').trim();
        if !normalized.to_lowercase().starts_with("select ") {
            return None;
        }
        let mut result = self.execute_sql(normalized).ok()?;
        result.rows = Vec::new();
        Some(result)
    }
}
