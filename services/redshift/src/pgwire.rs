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

use crate::backend::SqlSession;
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
            let (conn, _) = match listener.accept().await {
                Ok(a) => a,
                // Out of descriptors, or a peer gone before the accept: back
                // off and keep serving instead of stopping the listener.
                Err(e) => {
                    // Only transient errors; a broken listener still stops.
                    let transient = matches!(
                        e.kind(),
                        std::io::ErrorKind::ConnectionAborted
                            | std::io::ErrorKind::ConnectionReset
                            | std::io::ErrorKind::Interrupted
                            | std::io::ErrorKind::WouldBlock
                            | std::io::ErrorKind::TimedOut
                    ) || matches!(e.raw_os_error(), Some(12 | 23 | 24)); // ENOMEM, ENFILE, EMFILE
                    if !transient {
                        return Err(e);
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                    continue;
                }
            };
            let server = Arc::clone(&self);
            tokio::spawn(async move { server.negotiate_and_handle(conn).await });
        }
    }

    /// Answers the pre-startup requests a client may send before its startup
    /// message: `SSLRequest` (TLS when configured, else `N`) and
    /// `GSSENCRequest` (always `N`). The first 8 bytes of anything else are
    /// handed back to the startup reader.
    async fn negotiate_and_handle(&self, mut conn: tokio::net::TcpStream) {
        let mut head = [0u8; 8];
        loop {
            if conn.read_exact(&mut head).await.is_err() {
                return;
            }
            let len = i32::from_be_bytes([head[0], head[1], head[2], head[3]]);
            let code = i32::from_be_bytes([head[4], head[5], head[6], head[7]]);
            if len == 8 && code == PG_SSL_REQUEST_CODE {
                if let Some(tls) = &self.sql_tls {
                    if conn.write_all(b"S").await.is_err() {
                        return;
                    }
                    // A failed handshake (the client does not trust the
                    // certificate, or its name does not match) just ends
                    // the connection.
                    if let Ok(stream) = tls.acceptor.accept(conn).await {
                        self.handle_sql_conn_secured(stream, true).await;
                    }
                    return;
                }
                if conn.write_all(b"N").await.is_err() {
                    return;
                }
                continue;
            }
            if len == 8 && code == PG_GSSENC_REQUEST_CODE {
                if conn.write_all(b"N").await.is_err() {
                    return;
                }
                continue;
            }
            let replay = Prefixed {
                prefix: head,
                pos: 0,
                inner: conn,
            };
            self.handle_sql_conn_secured(replay, false).await;
            return;
        }
    }

    /// Mirrors `handleSQLConn`: any read/write error tears the connection down
    /// silently, exactly like legacy bare `return`s.
    pub async fn handle_sql_conn<S: AsyncRead + AsyncWrite + Unpin>(&self, conn: S) {
        self.handle_sql_conn_secured(conn, false).await
    }

    /// [`Self::handle_sql_conn`] for a stream that is (`tls`) or is not
    /// encrypted; when TLS is required, a plain stream is refused.
    async fn handle_sql_conn_secured<S: AsyncRead + AsyncWrite + Unpin>(
        &self,
        mut conn: S,
        tls: bool,
    ) {
        let _ = self.handle_sql_conn_inner(&mut conn, tls).await;
    }

    async fn handle_sql_conn_inner<S: AsyncRead + AsyncWrite + Unpin>(
        &self,
        conn: &mut S,
        tls: bool,
    ) -> std::io::Result<()> {
        let params = self.read_startup(conn).await?;
        if !tls && self.sql_tls.as_ref().is_some_and(|t| t.require) {
            write_buffered(conn, |buf| {
                write_error_response(
                    buf,
                    "28000",
                    "this devcloud Redshift endpoint requires TLS (connect with sslmode=require or stricter)",
                )
            })
            .await?;
            return Ok(());
        }
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
        // One backend connection per client, held until it disconnects.
        match self.backend().session() {
            Ok(session) => extended.backend_session = session,
            Err(err) => {
                write_buffered(conn, |buf| {
                    write_error_response(buf, "08006", &err.to_string())
                })
                .await?;
                return Ok(());
            }
        }
        loop {
            let mut message_type = [0u8; 1];
            conn.read_exact(&mut message_type).await?;
            let payload = read_message_payload(conn).await?;
            let mut out = Vec::new();
            match message_type[0] {
                b'Q' => self.handle_simple_query_in(
                    &mut extended.backend_session,
                    &mut out,
                    &read_cstring(&payload),
                ),
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
        self.handle_simple_query_in(&mut None, w, query)
    }

    /// [`Self::handle_simple_query`] on a client's own backend session.
    pub fn handle_simple_query_in(
        &self,
        session: &mut Option<Box<dyn SqlSession>>,
        w: &mut impl Write,
        query: &str,
    ) {
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
            match self.execute_sql_in(session, statement) {
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
        if let Err(err) = self.shared.persist_locked(&state) {
            eprintln!("devcloud-redshift: persist sql history failed: {err}");
        }
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

/// GSSAPI encryption request (`GSSENCRequest`); devcloud always declines.
const PG_GSSENC_REQUEST_CODE: i32 = 80877104;

/// A stream whose first bytes were already read: they are returned again
/// before the rest of `inner`.
struct Prefixed<S> {
    prefix: [u8; 8],
    pos: usize,
    inner: S,
}

impl<S: AsyncRead + Unpin> AsyncRead for Prefixed<S> {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        if self.pos < self.prefix.len() {
            let n = (self.prefix.len() - self.pos).min(buf.remaining());
            let start = self.pos;
            buf.put_slice(&self.prefix[start..start + n]);
            self.pos += n;
            return std::task::Poll::Ready(Ok(()));
        }
        std::pin::Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl<S: AsyncWrite + Unpin> AsyncWrite for Prefixed<S> {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        data: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        std::pin::Pin::new(&mut self.inner).poll_write(cx, data)
    }

    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}
