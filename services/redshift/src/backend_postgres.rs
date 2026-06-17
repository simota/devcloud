//! PostgreSQL `SqlBackend` over a hand-rolled wire-protocol client.
//!
//! Parity: `internal/services/redshift/backend/postgres/postgres.rs`. legacy uses
//! `database/sql` + `lib/pq` against a real PostgreSQL that the daemon manages
//! (`internal/app/managed_postgres.rs`); the legacy server connects to the DSN the
//! daemon provides. Here we connect to that same DSN over a minimal hand-rolled
//! PostgreSQL frontend protocol (startup, cleartext/md5 auth, simple query,
//! row decode, error parse) on tokio TCP — no external pg crate.
//!
//! The byte-level framing mirrors the part-3 pgwire SERVER codec
//! (`pgwire_codec.rs`); the client is its complement: it sends StartupMessage /
//! PasswordMessage / Query / Terminate and reads Authentication* /
//! ParameterStatus / BackendKeyData / RowDescription / DataRow /
//! CommandComplete / ErrorResponse / ReadyForQuery.
//!
//! Like legacy backend, the command tag is derived from the *statement* via
//! [`command_tag`] (not the wire CommandComplete), errors never echo the DSN,
//! and a nil/closed backend reports a clear error.

use std::future::Future;
use std::sync::Mutex;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::runtime::Runtime;

use crate::backend::{
    CatalogSnapshot, Column, ExecResult, Field, Schema, SqlBackend, SqlTransaction, Table,
};
use crate::errors::SqlError;

const CATALOG_QUERY: &str = "\nselect c.table_schema, c.table_name, t.table_type, c.column_name, c.data_type\nfrom information_schema.columns c\njoin information_schema.tables t\n  on t.table_schema = c.table_schema and t.table_name = c.table_name\nwhere c.table_schema not in ('pg_catalog', 'information_schema')\norder by c.table_schema, c.table_name, c.ordinal_position";

/// Mirrors `postgres.Config` (the `DriverName` field is legacy-`database/sql`
/// specific and has no analog in the wire client; the daemon never sets it for
/// the live path).
#[derive(Debug, Clone, Default)]
pub struct Config {
    pub dsn: String,
    pub query_timeout: Option<Duration>,
}

/// Parsed DSN connection target (host/port/credentials/database).
#[derive(Debug, Clone, Default)]
struct ConnInfo {
    host: String,
    port: u16,
    user: String,
    password: String,
    database: String,
}

/// A current-thread tokio runtime whose `Drop` never blocks, so it is safe to
/// drop from within an ambient async context.
///
/// Dropping a bare `Runtime` blocks to shut down its (blocking) thread pool,
/// which panics ("Cannot drop a runtime in a context where blocking is not
/// allowed") when the drop lands on a tokio worker thread. The orchestrator
/// holds the backend behind `Arc<Server>` cloned into spawned tasks, so the
/// final `Backend`/`Transaction` drop runs on a worker thread at shutdown.
/// `shutdown_background` releases the runtime without blocking on those threads.
struct SafeRuntime(Option<Runtime>);

impl SafeRuntime {
    fn new(runtime: Runtime) -> Self {
        SafeRuntime(Some(runtime))
    }
}

impl std::ops::Deref for SafeRuntime {
    type Target = Runtime;
    fn deref(&self) -> &Runtime {
        self.0.as_ref().expect("runtime present until drop")
    }
}

impl Drop for SafeRuntime {
    fn drop(&mut self) {
        if let Some(runtime) = self.0.take() {
            runtime.shutdown_background();
        }
    }
}

/// Mirrors `postgres.Backend`.
pub struct Backend {
    runtime: SafeRuntime,
    conn: Mutex<Option<PgConn>>,
    info: ConnInfo,
    #[allow(dead_code)]
    query_timeout: Option<Duration>,
}

impl Backend {
    /// Mirrors `Open`: requires a DSN, connects, and performs a startup/auth
    /// handshake (legacy `db.PingContext`).
    pub fn open(cfg: Config) -> Result<Backend, SqlError> {
        if cfg.dsn.trim().is_empty() {
            return Err(wrap_error(
                "open",
                "postgres redshift backend requires an external dsn",
            ));
        }
        let info = parse_dsn(&cfg.dsn).ok_or_else(|| wrap_error("open", "invalid postgres dsn"))?;
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|err| wrap_error("open", err.to_string()))?;
        let conn = runtime
            .block_on(PgConn::connect(&info))
            .map_err(|err| wrap_error("ping", err))?;
        Ok(Backend {
            runtime: SafeRuntime::new(runtime),
            conn: Mutex::new(Some(conn)),
            info,
            query_timeout: cfg.query_timeout,
        })
    }

    fn query(&self, statement: &str) -> Result<ExecResult, SqlError> {
        let mut guard = self.conn.lock().unwrap();
        let conn = guard
            .as_mut()
            .ok_or_else(|| SqlError::new("postgres redshift backend is not open"))?;
        drive(&self.runtime, conn.simple_query(statement))
    }
}

impl SqlBackend for Backend {
    /// Mirrors `Backend.Exec`.
    fn exec(&self, statement: &str) -> Result<ExecResult, SqlError> {
        if self.conn.lock().unwrap().is_none() {
            return Err(SqlError::new("postgres redshift backend is not open"));
        }
        self.query(statement).map_err(|err| wrap_error("exec", err))
    }

    /// Mirrors `Backend.Begin`: opens a fresh connection for the transaction and
    /// issues `BEGIN` (legacy `database/sql` reserves a connection per `*sql.Tx`).
    fn begin(&self) -> Result<Box<dyn SqlTransaction>, SqlError> {
        if self.conn.lock().unwrap().is_none() {
            return Err(SqlError::new("postgres redshift backend is not open"));
        }
        // The transaction owns its own runtime + connection: a tokio TcpStream
        // is bound to the reactor of the runtime that created it, so the
        // connection must be opened and driven on a single runtime.
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|err| wrap_error("begin", err.to_string()))?;
        let info = self.info.clone();
        let mut conn =
            drive(&runtime, PgConn::connect(&info)).map_err(|err| wrap_error("begin", err))?;
        drive(&runtime, conn.simple_query("BEGIN")).map_err(|err| wrap_error("begin", err))?;
        Ok(Box::new(Transaction {
            runtime: SafeRuntime::new(runtime),
            conn: Some(conn),
        }))
    }

    /// Mirrors `Backend.Catalog`.
    fn catalog(&self) -> Result<CatalogSnapshot, SqlError> {
        if self.conn.lock().unwrap().is_none() {
            return Err(SqlError::new("postgres redshift backend is not open"));
        }
        let result = self
            .query(CATALOG_QUERY)
            .map_err(|err| wrap_error("catalog", err))?;
        Ok(catalog_from_rows(&result.rows))
    }

    /// Mirrors `Backend.Close` (idempotent; nil-safe at the daemon level).
    fn close(&self) -> Result<(), SqlError> {
        if let Some(mut conn) = self.conn.lock().unwrap().take() {
            let _ = drive(&self.runtime, conn.terminate());
        }
        Ok(())
    }
}

/// Mirrors `transaction`.
struct Transaction {
    runtime: SafeRuntime,
    conn: Option<PgConn>,
}

impl SqlTransaction for Transaction {
    fn exec(&mut self, statement: &str) -> Result<ExecResult, SqlError> {
        let conn = self
            .conn
            .as_mut()
            .ok_or_else(|| SqlError::new("transaction is closed"))?;
        drive(&self.runtime, conn.simple_query(statement))
            .map_err(|err| wrap_error("transaction exec", err))
    }

    fn commit(&mut self) -> Result<(), SqlError> {
        let conn = self
            .conn
            .as_mut()
            .ok_or_else(|| SqlError::new("transaction is closed"))?;
        drive(&self.runtime, conn.simple_query("COMMIT"))
            .map_err(|err| wrap_error("commit", err))?;
        if let Some(mut conn) = self.conn.take() {
            let _ = drive(&self.runtime, conn.terminate());
        }
        Ok(())
    }

    fn rollback(&mut self) -> Result<(), SqlError> {
        let conn = self
            .conn
            .as_mut()
            .ok_or_else(|| SqlError::new("transaction is closed"))?;
        drive(&self.runtime, conn.simple_query("ROLLBACK"))
            .map_err(|err| wrap_error("rollback", err))?;
        if let Some(mut conn) = self.conn.take() {
            let _ = drive(&self.runtime, conn.terminate());
        }
        Ok(())
    }
}

/// The wire connection: owns a tokio TCP stream after a completed handshake.
struct PgConn {
    stream: TcpStream,
}

impl PgConn {
    /// Connects and runs StartupMessage → auth → ReadyForQuery.
    async fn connect(info: &ConnInfo) -> Result<PgConn, String> {
        let stream = TcpStream::connect((info.host.as_str(), info.port))
            .await
            .map_err(|err| err.to_string())?;
        let mut conn = PgConn { stream };
        conn.startup(info).await?;
        Ok(conn)
    }

    async fn startup(&mut self, info: &ConnInfo) -> Result<(), String> {
        // StartupMessage: i32 protocol version + key/value pairs + NUL.
        let mut body = Vec::new();
        body.extend_from_slice(&196608i32.to_be_bytes());
        put_cstr(&mut body, "user");
        put_cstr(&mut body, &default_str(&info.user, "dev"));
        if !info.database.is_empty() {
            put_cstr(&mut body, "database");
            put_cstr(&mut body, &info.database);
        }
        body.push(0);
        self.write_untagged(&body).await?;

        loop {
            let (tag, payload) = self.read_message().await?;
            match tag {
                b'R' => {
                    let code = i32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]);
                    match code {
                        0 => {} // AuthenticationOk
                        3 => {
                            // cleartext password
                            self.send_password(&info.password).await?;
                        }
                        5 => {
                            // md5 password: 4-byte salt follows the code.
                            let salt = &payload[4..8];
                            let hashed = md5_password(&info.user, &info.password, salt);
                            self.send_password(&hashed).await?;
                        }
                        10 => {
                            // AuthenticationSASL: the daemon-managed PostgreSQL is
                            // started with --auth-host=scram-sha-256, so host
                            // (TCP) connections negotiate SASL. The body lists
                            // NUL-terminated mechanism names; we require
                            // SCRAM-SHA-256 (no channel binding).
                            self.sasl_scram(info, &payload[4..]).await?;
                        }
                        other => {
                            return Err(format!("unsupported authentication request {other}"));
                        }
                    }
                }
                b'S' | b'K' => {}      // ParameterStatus / BackendKeyData
                b'Z' => return Ok(()), // ReadyForQuery
                b'E' => return Err(parse_error_response(&payload)),
                _ => {} // NoticeResponse and friends are ignored.
            }
        }
    }

    async fn send_password(&mut self, password: &str) -> Result<(), String> {
        let mut body = Vec::new();
        put_cstr(&mut body, password);
        self.write_tagged(b'p', &body).await
    }

    /// SCRAM-SHA-256 client exchange (RFC 5802 + RFC 7677, no channel binding).
    ///
    /// `mechanisms` is the AuthenticationSASL body after the 4-byte code: a list
    /// of NUL-terminated mechanism names terminated by an empty (final NUL)
    /// entry. We require `SCRAM-SHA-256` (not the `-PLUS` channel-binding
    /// variant). The full message sequence is:
    ///   client → SASLInitialResponse ('p')  : mechanism + Int32 len + client-first
    ///   server → AuthenticationSASLContinue (R/11) : server-first
    ///   client → SASLResponse ('p')         : client-final (with proof)
    ///   server → AuthenticationSASLFinal (R/12)    : v=<server-signature>
    /// then AuthenticationOk (R/0). The server signature is verified for mutual
    /// auth. No secrets are logged on any path.
    async fn sasl_scram(&mut self, info: &ConnInfo, mechanisms: &[u8]) -> Result<(), String> {
        const MECH: &str = "SCRAM-SHA-256";
        if !mechanism_offered(mechanisms, MECH) {
            return Err("server did not offer SCRAM-SHA-256 SASL mechanism".to_string());
        }

        // Client-first-message. The SCRAM username is left empty (`n=`):
        // PostgreSQL takes the user from the startup packet (RFC 5802 §5.1).
        // gs2 header `n,,` = no channel binding.
        let client_nonce = generate_nonce();
        let client_first_bare = format!("n=,r={client_nonce}");
        let client_first = format!("n,,{client_first_bare}");

        // SASLInitialResponse: mechanism (NUL) + Int32 client-first length + bytes.
        let mut body = Vec::new();
        put_cstr(&mut body, MECH);
        body.extend_from_slice(&(client_first.len() as i32).to_be_bytes());
        body.extend_from_slice(client_first.as_bytes());
        self.write_tagged(b'p', &body).await?;

        // AuthenticationSASLContinue (R, code 11) → server-first-message.
        let server_first = self.read_sasl_auth(11).await?;
        let server_first_str = String::from_utf8(server_first)
            .map_err(|_| "invalid UTF-8 in SCRAM server-first-message".to_string())?;
        let (server_nonce, salt, iterations) = parse_server_first(&server_first_str)?;
        if !server_nonce.starts_with(&client_nonce) {
            return Err("SCRAM server nonce does not extend client nonce".to_string());
        }

        // Key derivation (RFC 5802 §3).
        let salted = pbkdf2_hmac_sha256(info.password.as_bytes(), &salt, iterations);
        let client_key = hmac_sha256(&salted, b"Client Key");
        let stored_key = sha256(&client_key);
        let client_final_without_proof = format!("c=biws,r={server_nonce}");
        let auth_message =
            format!("{client_first_bare},{server_first_str},{client_final_without_proof}");
        let client_signature = hmac_sha256(&stored_key, auth_message.as_bytes());
        let client_proof: Vec<u8> = client_key
            .iter()
            .zip(client_signature.iter())
            .map(|(a, b)| a ^ b)
            .collect();
        let server_key = hmac_sha256(&salted, b"Server Key");
        let server_signature = hmac_sha256(&server_key, auth_message.as_bytes());

        // SASLResponse: client-final-message with the base64 proof.
        let client_final = format!(
            "{client_final_without_proof},p={}",
            devcloud_s3::base64::std_encode(&client_proof)
        );
        self.write_tagged(b'p', client_final.as_bytes()).await?;

        // AuthenticationSASLFinal (R, code 12) → v=<base64 server-signature>.
        let server_final = self.read_sasl_auth(12).await?;
        let server_final_str = String::from_utf8(server_final)
            .map_err(|_| "invalid UTF-8 in SCRAM server-final-message".to_string())?;
        let expected = parse_server_final(&server_final_str)?;
        if expected != server_signature {
            return Err("SCRAM server signature verification failed".to_string());
        }
        Ok(())
    }

    /// Reads the next message expecting an Authentication (`R`) frame whose code
    /// equals `want_code`, returning the bytes after the code. An ErrorResponse
    /// surfaces as the connection error; any other frame is a protocol fault.
    async fn read_sasl_auth(&mut self, want_code: i32) -> Result<Vec<u8>, String> {
        let (tag, payload) = self.read_message().await?;
        match tag {
            b'R' => {
                if payload.len() < 4 {
                    return Err("short SCRAM authentication message".to_string());
                }
                let code = i32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]);
                if code != want_code {
                    return Err(format!(
                        "unexpected SCRAM authentication code {code} (wanted {want_code})"
                    ));
                }
                Ok(payload[4..].to_vec())
            }
            b'E' => Err(parse_error_response(&payload)),
            other => Err(format!(
                "unexpected message '{}' during SCRAM exchange",
                other as char
            )),
        }
    }

    /// Simple query protocol: send `Q`, accumulate one result set up to
    /// ReadyForQuery. Mirrors `queryRows`: the returned tag is derived from the
    /// statement (not the wire CommandComplete).
    async fn simple_query(&mut self, statement: &str) -> Result<ExecResult, SqlError> {
        let mut body = Vec::new();
        put_cstr(&mut body, statement);
        self.write_tagged(b'Q', &body)
            .await
            .map_err(SqlError::new)?;

        let mut fields: Vec<Field> = Vec::new();
        let mut rows: Vec<Vec<String>> = Vec::new();
        let mut error: Option<String> = None;
        loop {
            let (tag, payload) = self.read_message().await.map_err(SqlError::new)?;
            match tag {
                b'T' => fields = parse_row_description(&payload),
                b'D' => rows.push(parse_data_row(&payload)),
                b'C' => {} // CommandComplete (tag ignored)
                b'E' => error = Some(parse_error_response(&payload)),
                b'I' => {}     // EmptyQueryResponse
                b'Z' => break, // ReadyForQuery
                _ => {}        // NoticeResponse, ParameterStatus, etc.
            }
        }
        if let Some(message) = error {
            return Err(SqlError::new(message));
        }
        Ok(ExecResult {
            tag: command_tag(statement, rows.len()),
            fields,
            rows,
        })
    }

    async fn terminate(&mut self) -> Result<(), String> {
        self.write_tagged(b'X', &[]).await
    }

    async fn write_untagged(&mut self, body: &[u8]) -> Result<(), String> {
        let len = (body.len() as u32 + 4).to_be_bytes();
        self.stream
            .write_all(&len)
            .await
            .map_err(|e| e.to_string())?;
        self.stream
            .write_all(body)
            .await
            .map_err(|e| e.to_string())?;
        self.stream.flush().await.map_err(|e| e.to_string())
    }

    async fn write_tagged(&mut self, tag: u8, body: &[u8]) -> Result<(), String> {
        self.stream
            .write_all(&[tag])
            .await
            .map_err(|e| e.to_string())?;
        let len = (body.len() as u32 + 4).to_be_bytes();
        self.stream
            .write_all(&len)
            .await
            .map_err(|e| e.to_string())?;
        self.stream
            .write_all(body)
            .await
            .map_err(|e| e.to_string())?;
        self.stream.flush().await.map_err(|e| e.to_string())
    }

    /// Reads one tagged backend message: 1-byte tag, i32 length (inclusive),
    /// `length - 4` payload bytes.
    async fn read_message(&mut self) -> Result<(u8, Vec<u8>), String> {
        let mut tag = [0u8; 1];
        self.stream
            .read_exact(&mut tag)
            .await
            .map_err(|e| e.to_string())?;
        let mut len_bytes = [0u8; 4];
        self.stream
            .read_exact(&mut len_bytes)
            .await
            .map_err(|e| e.to_string())?;
        let len = u32::from_be_bytes(len_bytes) as usize;
        if len < 4 {
            return Err("invalid PostgreSQL message length".to_string());
        }
        let mut payload = vec![0u8; len - 4];
        self.stream
            .read_exact(&mut payload)
            .await
            .map_err(|e| e.to_string())?;
        Ok((tag[0], payload))
    }
}

/// Drives `future` to completion on `runtime` (a current-thread runtime owning
/// the connection's TcpStream), safe to call from any thread.
///
/// The pgwire server (`pgwire.rs`) handles each connection inside its
/// **multi-thread** tokio runtime, and the synchronous `SqlBackend` methods are
/// invoked from those worker threads. Calling `Runtime::block_on` directly from
/// a worker thread panics ("Cannot start a runtime from within a runtime").
/// When an ambient multi-thread runtime is present we therefore hand the worker
/// to `block_in_place` first (which lets the scheduler relocate other tasks);
/// when there is none — e.g. the synchronous unit/integration tests that open a
/// `Backend` outside any runtime — we block directly.
fn drive<F: Future>(runtime: &Runtime, future: F) -> F::Output {
    match tokio::runtime::Handle::try_current() {
        Ok(_) => tokio::task::block_in_place(|| runtime.block_on(future)),
        Err(_) => runtime.block_on(future),
    }
}

/// Mirrors `queryCatalog`: builds the schema/table/column tree from the catalog
/// query rows (insertion order preserved, same as legacy index maps).
fn catalog_from_rows(rows: &[Vec<String>]) -> CatalogSnapshot {
    let mut catalog = CatalogSnapshot::default();
    for row in rows {
        if row.len() < 5 {
            continue;
        }
        let (schema_name, table_name, table_type, column_name, data_type) =
            (&row[0], &row[1], &row[2], &row[3], &row[4]);
        let schema_pos = match catalog.schemas.iter().position(|s| &s.name == schema_name) {
            Some(pos) => pos,
            None => {
                catalog.schemas.push(Schema {
                    name: schema_name.clone(),
                    tables: Vec::new(),
                });
                catalog.schemas.len() - 1
            }
        };
        let tables = &mut catalog.schemas[schema_pos].tables;
        let table_pos = match tables.iter().position(|t| &t.name == table_name) {
            Some(pos) => pos,
            None => {
                tables.push(Table {
                    schema: schema_name.clone(),
                    name: table_name.clone(),
                    kind: table_type.to_lowercase(),
                    columns: Vec::new(),
                });
                tables.len() - 1
            }
        };
        tables[table_pos].columns.push(Column {
            name: column_name.clone(),
            data_type: data_type.clone(),
        });
    }
    catalog
}

/// Reads a RowDescription body into [`Field`]s. The wire layout matches
/// `write_row_description`: i16 count, then per field {name, tableOID(i32),
/// attnum(i16), typeOID(i32), typeSize(i16), typmod(i32), format(i16)}.
fn parse_row_description(payload: &[u8]) -> Vec<Field> {
    let mut pos = 0;
    let count = read_i16(payload, &mut pos) as usize;
    let mut fields = Vec::with_capacity(count);
    for _ in 0..count {
        let name = read_cstr(payload, &mut pos);
        let _table_oid = read_i32(payload, &mut pos);
        let _attnum = read_i16(payload, &mut pos);
        let type_oid = read_i32(payload, &mut pos);
        let type_size = read_i16(payload, &mut pos);
        let _typmod = read_i32(payload, &mut pos);
        let _format = read_i16(payload, &mut pos);
        fields.push(Field {
            name,
            type_oid,
            type_size,
        });
    }
    fields
}

/// Reads a DataRow body into text values; a -1 column length is SQL NULL,
/// rendered as the empty string (legacy `stringifyRow` maps `nil` to `""`).
fn parse_data_row(payload: &[u8]) -> Vec<String> {
    let mut pos = 0;
    let count = read_i16(payload, &mut pos) as usize;
    let mut values = Vec::with_capacity(count);
    for _ in 0..count {
        let len = read_i32(payload, &mut pos);
        if len < 0 {
            values.push(String::new());
            continue;
        }
        let len = len as usize;
        let value = String::from_utf8_lossy(&payload[pos..pos + len]).into_owned();
        pos += len;
        values.push(value);
    }
    values
}

/// Reads an ErrorResponse body into its `M` (message) field text.
fn parse_error_response(payload: &[u8]) -> String {
    let mut pos = 0;
    let mut message = String::new();
    while pos < payload.len() {
        let field_type = payload[pos];
        pos += 1;
        if field_type == 0 {
            break;
        }
        let value = read_cstr(payload, &mut pos);
        if field_type == b'M' {
            message = value;
        }
    }
    if message.is_empty() {
        "postgres error".to_string()
    } else {
        message
    }
}

/// Mirrors `commandTag`: `SELECT n` for SELECTs, otherwise the uppercased first
/// word; empty when the statement is blank.
pub fn command_tag(statement: &str, rows: usize) -> String {
    let mut fields = statement.trim().split_whitespace();
    let Some(first) = fields.next() else {
        return String::new();
    };
    let command = first.to_uppercase();
    if command == "SELECT" {
        format!("SELECT {rows}")
    } else {
        command
    }
}

/// Mirrors `postgresTypeOID`: maps a database type name to a PG OID. The wire
/// client reads OIDs directly from RowDescription, so this exists only for the
/// `TestHelpers` parity case and any name-based callers.
pub fn postgres_type_oid(database_type: &str) -> i32 {
    match database_type.to_uppercase().as_str() {
        "BOOL" | "BOOLEAN" => 16,
        "INT2" | "SMALLINT" => 21,
        "INT4" | "INTEGER" | "INT" => 23,
        "INT8" | "BIGINT" => 20,
        "FLOAT4" | "REAL" => 700,
        "FLOAT8" | "DOUBLE PRECISION" => 701,
        "NUMERIC" | "DECIMAL" => 1700,
        "DATE" => 1082,
        "TIMESTAMP" | "TIMESTAMP WITHOUT TIME ZONE" => 1114,
        "TIMESTAMPTZ" | "TIMESTAMP WITH TIME ZONE" => 1184,
        "VARCHAR" | "CHARACTER VARYING" => 1043,
        "CHAR" | "CHARACTER" => 1042,
        _ => 25,
    }
}

/// Mirrors `wrapError` / `Error.Error()`: `postgres redshift backend <op>: <err>`
/// (and never leaks the DSN — callers pass only the underlying error text).
pub fn wrap_error(operation: &str, err: impl ToString) -> SqlError {
    let err = err.to_string();
    if operation.is_empty() {
        SqlError::new(format!("postgres redshift backend: {err}"))
    } else {
        SqlError::new(format!("postgres redshift backend {operation}: {err}"))
    }
}

/// Parses `postgres://user:pass@host:port/db?...#tag` (and `postgresql://`).
/// Tolerant of the trailing `#tag` the test DSNs carry and a missing port
/// (defaults to 5432). Returns `None` only when the host is unparseable.
fn parse_dsn(dsn: &str) -> Option<ConnInfo> {
    let rest = dsn
        .strip_prefix("postgres://")
        .or_else(|| dsn.strip_prefix("postgresql://"))?;
    // Strip query (?...) and fragment (#...).
    let rest = rest.split('#').next().unwrap_or(rest);
    let rest = rest.split('?').next().unwrap_or(rest);
    let (authority, path) = match rest.split_once('/') {
        Some((a, p)) => (a, p),
        None => (rest, ""),
    };
    let (credentials, host_port) = match authority.rsplit_once('@') {
        Some((c, h)) => (c, h),
        None => ("", authority),
    };
    let (user, password) = match credentials.split_once(':') {
        Some((u, p)) => (u.to_string(), p.to_string()),
        None => (credentials.to_string(), String::new()),
    };
    let (host, port) = match host_port.rsplit_once(':') {
        Some((h, p)) => (h.to_string(), p.parse().unwrap_or(5432)),
        None => (host_port.to_string(), 5432),
    };
    if host.is_empty() {
        return None;
    }
    Some(ConnInfo {
        host,
        port,
        user,
        password,
        database: path.to_string(),
    })
}

/// PostgreSQL md5 password: `"md5" + md5(md5(password + user) + salt)` (hex).
fn md5_password(user: &str, password: &str, salt: &[u8]) -> String {
    use md5::{Digest, Md5};
    let mut inner = Md5::new();
    inner.update(password.as_bytes());
    inner.update(user.as_bytes());
    let inner_hex = hex::encode(inner.finalize());
    let mut outer = Md5::new();
    outer.update(inner_hex.as_bytes());
    outer.update(salt);
    format!("md5{}", hex::encode(outer.finalize()))
}

/// Returns true if `name` appears in a SASL mechanism list (NUL-separated,
/// final empty entry terminates).
fn mechanism_offered(body: &[u8], name: &str) -> bool {
    let mut pos = 0;
    while pos < body.len() {
        let mech = read_cstr(body, &mut pos);
        if mech.is_empty() {
            break;
        }
        if mech == name {
            return true;
        }
    }
    false
}

/// Parses a SCRAM server-first-message `r=<nonce>,s=<base64 salt>,i=<iter>`.
fn parse_server_first(message: &str) -> Result<(String, Vec<u8>, u32), String> {
    let mut nonce = None;
    let mut salt = None;
    let mut iterations = None;
    for attr in message.split(',') {
        match attr.split_once('=') {
            Some(("r", v)) => nonce = Some(v.to_string()),
            Some(("s", v)) => {
                salt = Some(
                    devcloud_s3::base64::std_decode(v)
                        .ok_or_else(|| "invalid base64 SCRAM salt".to_string())?,
                )
            }
            Some(("i", v)) => {
                iterations = Some(
                    v.parse::<u32>()
                        .map_err(|_| "invalid SCRAM iteration count".to_string())?,
                )
            }
            _ => {}
        }
    }
    match (nonce, salt, iterations) {
        (Some(n), Some(s), Some(i)) if i > 0 => Ok((n, s, i)),
        _ => Err("malformed SCRAM server-first-message".to_string()),
    }
}

/// Parses a SCRAM server-final-message `v=<base64 server-signature>`.
fn parse_server_final(message: &str) -> Result<Vec<u8>, String> {
    for attr in message.split(',') {
        if let Some(("v", v)) = attr.split_once('=') {
            return devcloud_s3::base64::std_decode(v)
                .ok_or_else(|| "invalid base64 SCRAM server signature".to_string());
        }
        if let Some(("e", err)) = attr.split_once('=') {
            return Err(format!("SCRAM authentication error: {err}"));
        }
    }
    Err("malformed SCRAM server-final-message".to_string())
}

/// A printable-ASCII SCRAM client nonce (≥18 chars). This local-dev emulator
/// only needs uniqueness + printability, not cryptographic strength, so the
/// nonce is hex of (SystemTime nanos XOR a per-process atomic counter XOR this
/// stack address) rather than a `rand`/`uuid` crate (none are permitted). The
/// counter guarantees uniqueness across nonces drawn within the same nanosecond.
fn generate_nonce() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);
    let seq = COUNTER.fetch_add(1, Ordering::Relaxed);
    let local = 0u8;
    let addr = (&local as *const u8) as u64;
    // 32 hex chars from two 64-bit lanes — printable ASCII, well over 18 chars.
    format!("{:016x}{:016x}", nanos ^ addr, seq.wrapping_add(nanos))
}

/// HMAC-SHA256(key, message).
fn hmac_sha256(key: &[u8], message: &[u8]) -> Vec<u8> {
    use hmac::{Hmac, Mac};
    use sha2::Sha256;
    let mut mac = Hmac::<Sha256>::new_from_slice(key).expect("HMAC accepts any key length");
    mac.update(message);
    mac.finalize().into_bytes().to_vec()
}

/// SHA-256(input).
fn sha256(input: &[u8]) -> Vec<u8> {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(input);
    hasher.finalize().to_vec()
}

/// PBKDF2-HMAC-SHA256 with dkLen=32 (one SHA-256 block, so block index is fixed
/// at 1). U1 = HMAC(pw, salt || INT(1)); U2 = HMAC(pw, U1); ...; the derived
/// key is U1 XOR U2 XOR ... XOR U_iterations.
fn pbkdf2_hmac_sha256(password: &[u8], salt: &[u8], iterations: u32) -> Vec<u8> {
    let mut block = salt.to_vec();
    block.extend_from_slice(&1u32.to_be_bytes()); // INT(1)
    let mut u = hmac_sha256(password, &block);
    let mut result = u.clone();
    for _ in 1..iterations {
        u = hmac_sha256(password, &u);
        for (r, b) in result.iter_mut().zip(u.iter()) {
            *r ^= b;
        }
    }
    result
}

fn default_str(value: &str, fallback: &str) -> String {
    if value.is_empty() {
        fallback.to_string()
    } else {
        value.to_string()
    }
}

fn put_cstr(buf: &mut Vec<u8>, value: &str) {
    buf.extend_from_slice(value.as_bytes());
    buf.push(0);
}

fn read_i16(buf: &[u8], pos: &mut usize) -> i16 {
    let v = i16::from_be_bytes([buf[*pos], buf[*pos + 1]]);
    *pos += 2;
    v
}

fn read_i32(buf: &[u8], pos: &mut usize) -> i32 {
    let v = i32::from_be_bytes([buf[*pos], buf[*pos + 1], buf[*pos + 2], buf[*pos + 3]]);
    *pos += 4;
    v
}

fn read_cstr(buf: &[u8], pos: &mut usize) -> String {
    let start = *pos;
    while *pos < buf.len() && buf[*pos] != 0 {
        *pos += 1;
    }
    let value = String::from_utf8_lossy(&buf[start..*pos]).into_owned();
    if *pos < buf.len() {
        *pos += 1; // skip NUL
    }
    value
}

#[cfg(test)]
mod scram_tests {
    use super::*;

    // RFC 7677 §3 worked SCRAM-SHA-256 example. password = "pencil".
    const CLIENT_FIRST_BARE: &str = "n=user,r=rOprNGfwEbeRWgbNEkqO";
    const SERVER_FIRST: &str =
        "r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096";
    const CLIENT_FINAL_NO_PROOF: &str =
        "c=biws,r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0";
    const EXPECTED_CLIENT_PROOF_B64: &str = "dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ=";
    const EXPECTED_SERVER_SIG_B64: &str = "6rriTRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4=";

    #[test]
    fn scram_math_matches_rfc7677_vector() {
        let (nonce, salt, iterations) = parse_server_first(SERVER_FIRST).unwrap();
        assert_eq!(iterations, 4096);
        assert!(nonce.starts_with("rOprNGfwEbeRWgbNEkqO"));

        let salted = pbkdf2_hmac_sha256(b"pencil", &salt, iterations);
        let client_key = hmac_sha256(&salted, b"Client Key");
        let stored_key = sha256(&client_key);
        let auth_message = format!("{CLIENT_FIRST_BARE},{SERVER_FIRST},{CLIENT_FINAL_NO_PROOF}");
        let client_signature = hmac_sha256(&stored_key, auth_message.as_bytes());
        let client_proof: Vec<u8> = client_key
            .iter()
            .zip(client_signature.iter())
            .map(|(a, b)| a ^ b)
            .collect();
        assert_eq!(
            devcloud_s3::base64::std_encode(&client_proof),
            EXPECTED_CLIENT_PROOF_B64
        );

        let server_key = hmac_sha256(&salted, b"Server Key");
        let server_signature = hmac_sha256(&server_key, auth_message.as_bytes());
        assert_eq!(
            devcloud_s3::base64::std_encode(&server_signature),
            EXPECTED_SERVER_SIG_B64
        );
        // parse_server_final round-trips the same encoding back to bytes.
        assert_eq!(
            parse_server_final(&format!("v={EXPECTED_SERVER_SIG_B64}")).unwrap(),
            server_signature
        );
    }

    #[test]
    fn mechanism_offered_finds_scram_only() {
        let mut body = Vec::new();
        put_cstr(&mut body, "SCRAM-SHA-256-PLUS");
        put_cstr(&mut body, "SCRAM-SHA-256");
        body.push(0);
        assert!(mechanism_offered(&body, "SCRAM-SHA-256"));
        assert!(!mechanism_offered(&body, "SCRAM-SHA-512"));
    }

    #[test]
    fn nonce_is_printable_and_long_enough() {
        let a = generate_nonce();
        let b = generate_nonce();
        assert!(a.len() >= 18);
        assert_ne!(a, b, "consecutive nonces must differ");
        assert!(a.bytes().all(|c| c.is_ascii_graphic()));
    }

    #[test]
    fn pbkdf2_single_iteration_equals_one_hmac() {
        // With iterations=1 the derived key is exactly U1 = HMAC(pw, salt||INT(1)).
        let salt = b"salt-bytes";
        let mut block = salt.to_vec();
        block.extend_from_slice(&1u32.to_be_bytes());
        let expected = hmac_sha256(b"pw", &block);
        assert_eq!(pbkdf2_hmac_sha256(b"pw", salt, 1), expected);
    }

    // ---- parse_dsn variants ----

    #[test]
    fn parse_dsn_postgres_and_postgresql_prefix() {
        let a = parse_dsn("postgres://u:p@host:5433/db").unwrap();
        let b = parse_dsn("postgresql://u:p@host:5433/db").unwrap();
        assert_eq!(a.host, "host");
        assert_eq!(a.port, 5433);
        assert_eq!(a.user, "u");
        assert_eq!(a.password, "p");
        assert_eq!(a.database, "db");
        assert_eq!(b.host, a.host);
        assert_eq!(b.port, a.port);
    }

    #[test]
    fn parse_dsn_missing_port_defaults_5432() {
        let info = parse_dsn("postgres://u:p@host/db").unwrap();
        assert_eq!(info.port, 5432);
        assert_eq!(info.host, "host");
        assert_eq!(info.database, "db");
    }

    #[test]
    fn parse_dsn_no_credentials() {
        let info = parse_dsn("postgres://host/db").unwrap();
        assert_eq!(info.user, "");
        assert_eq!(info.password, "");
        assert_eq!(info.host, "host");
    }

    #[test]
    fn parse_dsn_strips_query_and_fragment() {
        let info = parse_dsn("postgres://u:p@host:5432/db?sslmode=disable#tag").unwrap();
        assert_eq!(info.host, "host");
        assert_eq!(info.port, 5432);
        assert_eq!(info.database, "db");
    }

    #[test]
    fn parse_dsn_empty_host_returns_none() {
        assert!(parse_dsn("postgres:///db").is_none());
        assert!(parse_dsn("postgres://:5432/db").is_none());
    }

    #[test]
    fn parse_dsn_user_only_no_password() {
        let info = parse_dsn("postgres://user@host/db").unwrap();
        assert_eq!(info.user, "user");
        assert_eq!(info.password, "");
        assert_eq!(info.host, "host");
    }

    // ---- md5_password ----

    #[test]
    fn md5_password_matches_known_vector() {
        // Hand-computed: inner = md5("secretdev") = md5(password+user),
        // outer = md5(inner_hex + salt) prefixed with "md5".
        // Use user="dev", password="secret", salt=b"\x01\x02\x03\x04".
        use md5::{Digest, Md5};
        let user = "dev";
        let password = "secret";
        let salt = b"\x01\x02\x03\x04";

        let mut inner = Md5::new();
        inner.update(password.as_bytes());
        inner.update(user.as_bytes());
        let inner_hex = hex::encode(inner.finalize());

        let mut outer = Md5::new();
        outer.update(inner_hex.as_bytes());
        outer.update(salt);
        let expected = format!("md5{}", hex::encode(outer.finalize()));

        assert_eq!(md5_password(user, password, salt), expected);
        assert!(md5_password(user, password, salt).starts_with("md5"));
        // length: "md5" + 32 hex chars
        assert_eq!(md5_password(user, password, salt).len(), 35);
    }

    // ---- SCRAM error paths ----

    #[test]
    fn parse_server_first_missing_field_is_err() {
        // Missing iteration count.
        assert!(parse_server_first("r=nonce,s=c2FsdA==").is_err());
        // Missing nonce.
        assert!(parse_server_first("s=c2FsdA==,i=4096").is_err());
        // Missing salt.
        assert!(parse_server_first("r=nonce,i=4096").is_err());
        // Iteration count zero is rejected.
        assert!(parse_server_first("r=nonce,s=c2FsdA==,i=0").is_err());
    }

    #[test]
    fn parse_server_first_invalid_base64_salt_is_err() {
        assert!(parse_server_first("r=nonce,s=!!!invalid!!!,i=4096").is_err());
    }

    #[test]
    fn parse_server_first_non_numeric_iterations_is_err() {
        assert!(parse_server_first("r=nonce,s=c2FsdA==,i=abc").is_err());
    }

    #[test]
    fn parse_server_final_missing_v_attribute_is_err() {
        assert!(parse_server_final("other=value").is_err());
        assert!(parse_server_final("").is_err());
    }

    #[test]
    fn parse_server_final_invalid_base64_is_err() {
        assert!(parse_server_final("v=!!!bad!!!").is_err());
    }

    #[test]
    fn parse_server_final_error_attribute_is_err() {
        let err = parse_server_final("e=unknown-user").unwrap_err();
        assert!(err.contains("unknown-user"), "error = {err}");
    }
}
