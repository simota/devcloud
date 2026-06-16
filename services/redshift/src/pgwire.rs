//! pgwire protocol server: TCP accept loop, startup/auth handshake, the
//! per-connection message loop, the simple query protocol, and SQL statement
//! history recording.
//!
//! Parity: `internal/services/redshift/pgwire_conn.rs` plus `runSQL` from
//! `server.rs`. legacy `goroutine + net.Conn` becomes `tokio::spawn` + a generic
//! `AsyncRead + AsyncWrite` stream. Message handlers stay synchronous over
//! byte buffers (exactly as the legacy handler-level tests exercise them); the
//! async layer only frames reads and flushes the handlers' output.

use std::collections::HashMap;
use std::io::Write;
use std::sync::Arc;
use std::time::SystemTime;

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpListener;

use crate::engine::QueryResult;
use crate::errors::SqlError;
use crate::pgwire_codec::{
    parse_startup_parameters, read_cstring, write_auth_cleartext_password, write_authentication_ok,
    write_backend_key_data, write_command_complete, write_data_row, write_error_response,
    write_message, write_parameter_statuses, write_ready_for_query, write_row_description,
    PG_PROTOCOL_VERSION, PG_SSL_REQUEST_CODE,
};
use crate::pgwire_extended::ExtendedQuerySession;
use crate::server::{default_str, Server, StatementRecord};
use crate::snapshot::{statement_snapshot_from_record, StatementSnapshot};
use crate::sql_parse::split_sql_statements;

impl Server {
    /// Mirrors `runSQL`'s accept loop. Cancellation is the caller's job (drop
    /// the future / close the listener), matching legacy ctx-driven close.
    pub async fn serve_sql(self: Arc<Self>, listener: TcpListener) -> std::io::Result<()> {
        loop {
            let (conn, _) = listener.accept().await?;
            let server = Arc::clone(&self);
            tokio::spawn(async move { server.handle_sql_conn(conn).await });
        }
    }

    /// Mirrors `handleSQLConn`: any read/write error tears the connection down
    /// silently, exactly like legacy bare `return`s.
    pub async fn handle_sql_conn<S: AsyncRead + AsyncWrite + Unpin>(&self, mut conn: S) {
        let _ = self.handle_sql_conn_inner(&mut conn).await;
    }

    async fn handle_sql_conn_inner<S: AsyncRead + AsyncWrite + Unpin>(
        &self,
        conn: &mut S,
    ) -> std::io::Result<()> {
        let params = self.read_startup(conn).await?;
        write_buffered(conn, |buf| write_auth_cleartext_password(buf)).await?;
        let password = read_password_message(conn).await?;
        if !self.password_allowed(&password) {
            write_buffered(conn, |buf| {
                write_error_response(buf, "28P01", "password authentication failed")
            })
            .await?;
            return Ok(());
        }
        write_buffered(conn, |buf| {
            write_authentication_ok(buf)?;
            write_parameter_statuses(buf, &params)?;
            write_backend_key_data(buf)?;
            write_ready_for_query(buf)
        })
        .await?;

        let mut extended = ExtendedQuerySession::new();
        loop {
            let mut message_type = [0u8; 1];
            conn.read_exact(&mut message_type).await?;
            let payload = read_message_payload(conn).await?;
            let mut out = Vec::new();
            match message_type[0] {
                b'Q' => self.handle_simple_query(&mut out, &read_cstring(&payload)),
                b'P' => {
                    if !extended.failed {
                        extended.handle_parse(&mut out, &payload);
                    }
                }
                b'B' => {
                    if !extended.failed {
                        extended.handle_bind(&mut out, &payload);
                    }
                }
                b'D' => {
                    if !extended.failed {
                        extended.handle_describe(self, &mut out, &payload);
                    }
                }
                b'E' => {
                    if !extended.failed {
                        extended.handle_execute(self, &mut out, &payload);
                    }
                }
                b'C' => {
                    if !extended.failed {
                        extended.handle_close(&mut out, &payload);
                    }
                }
                b'S' => {
                    extended.handle_sync();
                    let _ = write_ready_for_query(&mut out);
                }
                b'X' => return Ok(()),
                _ => {
                    let _ = write_error_response(
                        &mut out,
                        "0A000",
                        "unsupported PostgreSQL wire message",
                    );
                    let _ = write_ready_for_query(&mut out);
                }
            }
            conn.write_all(&out).await?;
            conn.flush().await?;
        }
    }

    /// Mirrors `readStartup`.
    async fn read_startup<S: AsyncRead + AsyncWrite + Unpin>(
        &self,
        conn: &mut S,
    ) -> std::io::Result<HashMap<String, String>> {
        loop {
            let payload = read_message_payload(conn).await?;
            if payload.len() < 4 {
                return Err(invalid_data("short startup message"));
            }
            let code = i32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]);
            if code == PG_SSL_REQUEST_CODE {
                conn.write_all(b"N").await?;
                conn.flush().await?;
                continue;
            }
            if code == PG_PROTOCOL_VERSION {
                return Ok(parse_startup_parameters(&payload[4..]));
            }
            write_buffered(conn, |buf| {
                write_error_response(buf, "08P01", "unsupported PostgreSQL startup protocol")
            })
            .await?;
            return Err(invalid_data("unsupported startup protocol"));
        }
    }

    /// Mirrors `handleSimpleQuery`.
    pub fn handle_simple_query(&self, w: &mut impl Write, query: &str) {
        let statements = split_sql_statements(query);
        if statements.is_empty() {
            let _ = write_message(w, b'I', &[]);
            let _ = write_ready_for_query(w);
            return;
        }
        for statement in &statements {
            if let Err(err) = self.shared.validate_statement_size(statement) {
                self.record_sql_history(
                    "[statement exceeds maxStatementBytes]",
                    &QueryResult::default(),
                    Some(&err),
                );
                let _ = write_error_response(w, "54000", &err.to_string());
                break;
            }
            match self.execute_sql(statement) {
                Ok(result) => {
                    self.record_sql_history(statement, &result, None);
                    if !result.fields.is_empty() {
                        let _ = write_row_description(w, &result.fields);
                        for row in &result.rows {
                            let _ = write_data_row(w, row);
                        }
                    }
                    let _ = write_command_complete(w, &result.tag);
                }
                Err(err) => {
                    self.record_sql_history(statement, &QueryResult::default(), Some(&err));
                    let _ = write_error_response(w, "0A000", &err.to_string());
                    break;
                }
            }
        }
        let _ = write_ready_for_query(w);
    }

    /// Mirrors `recordSQLHistory`.
    pub(crate) fn record_sql_history(
        &self,
        statement_text: &str,
        result: &QueryResult,
        execution_err: Option<&SqlError>,
    ) -> StatementSnapshot {
        let now = SystemTime::now();
        let mut stmt = StatementRecord {
            id: self.shared.next_statement_id_value(),
            cluster_identifier: default_str(&self.shared.config.cluster_identifier, "devcloud"),
            database: default_str(&self.shared.config.database, "dev"),
            db_user: default_str(&self.shared.config.user, "dev"),
            session_id: String::new(),
            query_string: statement_text.to_string(),
            result_format: String::new(),
            created_at: now,
            updated_at: now,
            status: "FINISHED".to_string(),
            error: String::new(),
            has_result_set: !result.fields.is_empty(),
            result: result.clone(),
        };
        if let Some(err) = execution_err {
            stmt.status = "FAILED".to_string();
            stmt.error = err.to_string();
            stmt.has_result_set = false;
            stmt.result = QueryResult::default();
        }

        let snapshot = statement_snapshot_from_record(&stmt);
        let mut state = self.shared.lock_state();
        state.statements.insert(stmt.id.clone(), stmt);
        let _ = self.shared.persist_locked(&state);
        snapshot
    }

    /// Mirrors `passwordAllowed`.
    fn password_allowed(&self, password: &str) -> bool {
        if self.shared.config.auth_mode.eq_ignore_ascii_case("strict") {
            return password == self.shared.config.password;
        }
        let expected = default_str(&self.shared.config.password, "dev");
        password.is_empty() || password == expected
    }
}

/// Async flavor of `pgwire_codec::read_message_payload` for the conn loop.
async fn read_message_payload<S: AsyncRead + Unpin>(r: &mut S) -> std::io::Result<Vec<u8>> {
    let mut length_bytes = [0u8; 4];
    r.read_exact(&mut length_bytes).await?;
    let length = u32::from_be_bytes(length_bytes) as usize;
    if length < 4 {
        return Err(invalid_data("invalid PostgreSQL message length"));
    }
    let mut payload = vec![0u8; length - 4];
    r.read_exact(&mut payload).await?;
    Ok(payload)
}

/// Async flavor of `pgwire_codec::read_password_message`.
async fn read_password_message<S: AsyncRead + Unpin>(r: &mut S) -> std::io::Result<String> {
    let mut message_type = [0u8; 1];
    r.read_exact(&mut message_type).await?;
    if message_type[0] != b'p' {
        return Err(invalid_data("expected password message"));
    }
    let payload = read_message_payload(r).await?;
    Ok(read_cstring(&payload))
}

/// Builds a response with the synchronous codec writers, then flushes it to
/// the connection in one async write.
async fn write_buffered<S, F>(conn: &mut S, build: F) -> std::io::Result<()>
where
    S: AsyncWrite + Unpin,
    F: FnOnce(&mut Vec<u8>) -> std::io::Result<()>,
{
    let mut buf = Vec::new();
    build(&mut buf)?;
    conn.write_all(&buf).await?;
    conn.flush().await
}

fn invalid_data(message: &str) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidData, message)
}
