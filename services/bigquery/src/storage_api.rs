//! BigQuery Storage Read/Write API (plaintext gRPC) over the local table
//! store.
//!
//! Reads: Avro or Arrow snapshot sessions with top-level column selection,
//! `row_restriction` filters, and `snapshot_time` reads backed by row insert
//! times. Writes: the `_default` stream plus managed `COMMITTED`, `PENDING`
//! and `BUFFERED` streams (see [`write`]). In `strict` auth mode callers are
//! authorized like REST (see [`crate::strict_auth`]).

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use tokio::{net::TcpListener, sync::watch};
use tokio_stream::{wrappers::TcpListenerStream, Stream};
use tonic::{Request, Response, Status};

use crate::model::{StoredRow, TableResource};
use crate::routes::authorize;
use crate::server::{now_unix_nanos, Server};
use crate::sql_expr::{eval_expr, find_schema_field, parse_expr, SqlExpr, SqlValue};
use crate::strict_auth::{describe, is_allowed, Caller, Target};
use crate::validation::validate_resource_id;

mod arrow;
mod codec;
mod write;

pub mod proto {
    tonic::include_proto!("google.cloud.bigquery.storage.v1");
}

use arrow::ArrowEncoder;
use codec::{table_columns, AvroEncoder};
use proto::big_query_read_server::{BigQueryRead, BigQueryReadServer};
use proto::big_query_write_server::BigQueryWriteServer;
use proto::read_session::table_read_options::OutputFormatSerializationOptions;
use proto::{
    ArrowRecordBatch, ArrowSchema, AvroRows, AvroSchema, CreateReadSessionRequest, DataFormat,
    ReadRowsRequest, ReadRowsResponse, ReadSession, ReadStream,
};

const MAX_SESSIONS: usize = 16;
const MAX_SNAPSHOT_BYTES: usize = 16 * 1024 * 1024;
const MAX_READ_BATCH_BYTES: usize = 3 * 1024 * 1024;
const MAX_READ_BATCH_ROWS: usize = 256;
const MAX_SCHEMA_BYTES: usize = 512 * 1024;
const SESSION_SECONDS: i64 = 6 * 60 * 60;
/// BigQuery's default time travel window.
const TIME_TRAVEL_NANOS: i64 = 7 * 24 * 60 * 60 * 1_000_000_000;

type GrpcStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send>>;

enum SnapshotRows {
    Avro {
        schema: AvroSchema,
        rows: Vec<Vec<u8>>,
    },
    Arrow {
        schema: ArrowSchema,
        encoder: ArrowEncoder,
        rows: Vec<StoredRow>,
        sizes: Vec<usize>,
    },
}

struct Snapshot {
    project: String,
    rows: SnapshotRows,
    expires: i64,
}

impl Snapshot {
    fn len(&self) -> usize {
        match &self.rows {
            SnapshotRows::Avro { rows, .. } => rows.len(),
            SnapshotRows::Arrow { rows, .. } => rows.len(),
        }
    }
}

pub(crate) struct State {
    server: Arc<Server>,
    streams: Mutex<HashMap<String, Arc<Snapshot>>>,
    write_streams: Mutex<HashMap<String, write::ManagedStream>>,
    next_id: AtomicU64,
    shutdown: watch::Receiver<bool>,
}

#[derive(Clone)]
pub(crate) struct StorageApi(Arc<State>);

pub async fn serve(
    listener: TcpListener,
    server: Arc<Server>,
    shutdown: impl Future<Output = ()>,
) -> Result<(), tonic::transport::Error> {
    let (stop, stopped) = watch::channel(false);
    let max_request_bytes = server.max_request_bytes() as usize;
    let api = StorageApi(Arc::new(State {
        server,
        streams: Mutex::new(HashMap::new()),
        write_streams: Mutex::new(HashMap::new()),
        next_id: AtomicU64::new(now_unix_nanos() as u64),
        shutdown: stopped,
    }));
    tonic::transport::Server::builder()
        .add_service(
            BigQueryReadServer::new(api.clone()).max_decoding_message_size(max_request_bytes),
        )
        .add_service(BigQueryWriteServer::new(api).max_decoding_message_size(max_request_bytes))
        .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async move {
            shutdown.await;
            let _ = stop.send(true);
        })
        .await
}

impl State {
    /// Authenticates a call. `None` means a non-strict mode accepted it (no
    /// per-resource checks); `Some` is the strict-mode caller.
    fn caller<T>(&self, request: &Request<T>) -> Result<Option<Caller>, Status> {
        let authorization = request
            .metadata()
            .get("authorization")
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default();
        let unauthenticated = || Status::unauthenticated("invalid authentication credentials");
        if !self.server.is_strict_auth() {
            return authorize(&self.server, authorization)
                .then_some(None)
                .ok_or_else(unauthenticated);
        }
        let token = match authorization.trim().split_once(' ') {
            Some((scheme, token)) if scheme.eq_ignore_ascii_case("Bearer") => token.trim(),
            _ => "",
        };
        // The Storage API has no anonymous access: a missing token is refused
        // even where REST would treat the caller as `allUsers`.
        match self.server.strict_caller(token) {
            Some(Caller::Anonymous) | None => Err(unauthenticated()),
            caller => Ok(caller),
        }
    }

    fn require(
        &self,
        caller: &Option<Caller>,
        target: Target,
        permission: &str,
    ) -> Result<(), Status> {
        let Some(caller) = caller else {
            return Ok(());
        };
        if is_allowed(&self.server, caller, &target, permission) {
            return Ok(());
        }
        Err(match caller {
            Caller::Principal(member) => Status::permission_denied(format!(
                "Access Denied: {}: Permission {permission} denied for {member} (or it may not exist).",
                describe(&target)
            )),
            _ => Status::unauthenticated("invalid authentication credentials"),
        })
    }

    fn next_id(&self) -> u64 {
        self.next_id.fetch_add(1, Ordering::Relaxed)
    }
}

#[tonic::async_trait]
impl BigQueryRead for StorageApi {
    type ReadRowsStream = GrpcStream<ReadRowsResponse>;

    async fn create_read_session(
        &self,
        request: Request<CreateReadSessionRequest>,
    ) -> Result<Response<ReadSession>, Status> {
        let state = &self.0;
        let caller = state.caller(&request)?;
        let request = request.into_inner();
        let Some(project) = request.parent.strip_prefix("projects/") else {
            return Err(Status::invalid_argument(
                "expected parent projects/{project}",
            ));
        };
        validate_resource_id(project, "project").map_err(Status::invalid_argument)?;
        if request.preferred_min_stream_count < 0
            || request.max_stream_count < request.preferred_min_stream_count
        {
            return Err(Status::invalid_argument("invalid stream counts"));
        }
        let mut session = request
            .read_session
            .ok_or_else(|| Status::invalid_argument("read session is required"))?;
        let format = match DataFormat::try_from(session.data_format) {
            Ok(format @ (DataFormat::Avro | DataFormat::Arrow)) => format,
            _ => {
                return Err(Status::invalid_argument(
                    "AVRO or ARROW data format is required",
                ))
            }
        };
        let options = session.read_options.clone().unwrap_or_default();
        match &options.output_format_serialization_options {
            Some(OutputFormatSerializationOptions::ArrowSerializationOptions(arrow))
                if arrow.buffer_compression != 0 =>
            {
                return Err(Status::unimplemented(
                    "Arrow buffer compression is unsupported",
                ))
            }
            Some(OutputFormatSerializationOptions::AvroSerializationOptions(avro))
                if avro.enable_display_name_attribute =>
            {
                return Err(Status::unimplemented(
                    "Avro display name attributes are unsupported",
                ))
            }
            _ => {}
        }
        if options.sample_percentage.is_some()
            || options.response_compression_codec.unwrap_or_default() != 0
        {
            return Err(Status::unimplemented("unsupported read options"));
        }
        let table = TableName::parse(&session.table)?;
        state.require(
            &caller,
            Target::Project(project.to_string()),
            "bigquery.readsessions.create",
        )?;
        state.require(&caller, table.target(), "bigquery.tables.getData")?;
        let restriction = parse_restriction(&options.row_restriction)?;
        let snapshot_time = session
            .table_modifiers
            .as_ref()
            .and_then(|m| m.snapshot_time.as_ref())
            .map(|t| {
                t.seconds
                    .saturating_mul(1_000_000_000)
                    .saturating_add(i64::from(t.nanos))
            });

        let table_lock = state
            .server
            .table_lock(table.project, table.dataset, table.table);
        let guard = table_lock.lock().unwrap();
        let resource = table.load(&state.server)?;
        let fields = &resource.schema.fields;
        let selected = &options.selected_fields;
        if selected
            .iter()
            .any(|name| !fields.iter().any(|field| &field.name == name))
        {
            return Err(Status::invalid_argument("unknown selected column"));
        }
        let columns = table_columns(
            fields
                .iter()
                .filter(|field| selected.is_empty() || selected.contains(&field.name)),
        )?;
        if let Some(time) = snapshot_time {
            check_snapshot_time(&state.server, &table, &resource, time)?;
        }
        let rows = state
            .server
            .read_rows(table.project, table.dataset, table.table)
            .map_err(storage_error)?;
        drop(guard);

        let mut kept = Vec::with_capacity(rows.len());
        for row in rows {
            if let Some(time) = snapshot_time {
                // Rows without an insert time predate the history reset.
                if parse_rfc3339_nanos(&row.inserted_at).is_some_and(|at| at > time) {
                    continue;
                }
            }
            if let Some(expr) = &restriction {
                if !row_matches(&state.server, project, &resource, &row, expr)? {
                    continue;
                }
            }
            kept.push(row);
        }

        let mut snapshot_bytes = 0;
        let too_large = || Status::resource_exhausted("local read snapshot exceeds size limit");
        let rows = match format {
            DataFormat::Arrow => {
                let encoder = ArrowEncoder::new(columns);
                let serialized_schema = encoder.schema();
                if serialized_schema.len() > MAX_SCHEMA_BYTES {
                    return Err(Status::resource_exhausted(
                        "Arrow schema exceeds size limit",
                    ));
                }
                let mut sizes = Vec::with_capacity(kept.len());
                for row in &kept {
                    let size = row.json.values().map(|v| v.get().len()).sum::<usize>() + 16;
                    snapshot_bytes += size + std::mem::size_of::<StoredRow>();
                    if size > MAX_READ_BATCH_BYTES || snapshot_bytes > MAX_SNAPSHOT_BYTES {
                        return Err(too_large());
                    }
                    sizes.push(size);
                }
                // Surface rows that do not fit the schema now, not mid-stream.
                for chunk in kept.chunks(MAX_READ_BATCH_ROWS) {
                    encoder.record_batch(chunk)?;
                }
                SnapshotRows::Arrow {
                    schema: ArrowSchema { serialized_schema },
                    encoder,
                    rows: kept,
                    sizes,
                }
            }
            _ => {
                let encoder = AvroEncoder::new(columns);
                let schema = encoder.schema();
                if schema.schema.len() > MAX_SCHEMA_BYTES {
                    return Err(Status::resource_exhausted("Avro schema exceeds size limit"));
                }
                let rows = kept
                    .iter()
                    .map(|row| {
                        let encoded = encoder.encode(row)?;
                        snapshot_bytes += encoded.len() + std::mem::size_of::<Vec<u8>>();
                        if encoded.len() > MAX_READ_BATCH_BYTES
                            || snapshot_bytes > MAX_SNAPSHOT_BYTES
                        {
                            return Err(too_large());
                        }
                        Ok(encoded)
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                SnapshotRows::Avro { schema, rows }
            }
        };

        let location = if resource.location.is_empty() {
            state.server.default_location()
        } else {
            &resource.location
        };
        let now = now_unix_nanos() / 1_000_000_000;
        let expires = now + SESSION_SECONDS;
        let mut streams = state.streams.lock().unwrap();
        streams.retain(|_, stream| stream.expires > now);
        if streams.len() >= MAX_SESSIONS {
            return Err(Status::resource_exhausted("too many local read sessions"));
        }
        let id = state.next_id();
        session.name = format!("projects/{project}/locations/{location}/sessions/{id}");
        let name = format!("{}/streams/0", session.name);
        session.streams = vec![ReadStream { name: name.clone() }];
        session.expire_time = Some(prost_types::Timestamp {
            seconds: expires,
            nanos: 0,
        });
        session.schema = Some(match &rows {
            SnapshotRows::Avro { schema, .. } => {
                proto::read_session::Schema::AvroSchema(schema.clone())
            }
            SnapshotRows::Arrow { schema, .. } => {
                proto::read_session::Schema::ArrowSchema(schema.clone())
            }
        });
        let snapshot = Snapshot {
            project: project.to_string(),
            rows,
            expires,
        };
        session.estimated_row_count = snapshot.len() as i64;
        streams.insert(name, Arc::new(snapshot));
        Ok(Response::new(session))
    }

    async fn read_rows(
        &self,
        request: Request<ReadRowsRequest>,
    ) -> Result<Response<Self::ReadRowsStream>, Status> {
        let state = &self.0;
        let caller = state.caller(&request)?;
        let request = request.into_inner();
        if request.arrow_serialization_options.is_some() {
            return Err(Status::unimplemented(
                "per-stream Arrow serialization options are unsupported",
            ));
        }
        let mut offset = usize::try_from(request.offset)
            .map_err(|_| Status::invalid_argument("invalid read offset"))?;
        let snapshot = {
            let now = now_unix_nanos() / 1_000_000_000;
            let mut streams = state.streams.lock().unwrap();
            streams.retain(|_, stream| stream.expires > now);
            streams
                .get(&request.read_stream)
                .cloned()
                .ok_or_else(|| Status::not_found("read stream not found"))?
        };
        state.require(
            &caller,
            Target::Project(snapshot.project.clone()),
            "bigquery.readsessions.getData",
        )?;
        if offset > snapshot.len() {
            return Err(Status::out_of_range(
                "read offset is beyond the end of the stream",
            ));
        }
        let shutdown = state.shutdown.clone();
        let stream = async_stream::stream! {
            let mut first = true;
            while offset < snapshot.len() && !*shutdown.borrow() {
                let start = offset;
                let response = match &snapshot.rows {
                    SnapshotRows::Avro { schema, rows } => {
                        let mut serialized_binary_rows = Vec::new();
                        for row in rows[start..].iter().take(MAX_READ_BATCH_ROWS) {
                            if serialized_binary_rows.len() + row.len() > MAX_READ_BATCH_BYTES {
                                break;
                            }
                            serialized_binary_rows.extend_from_slice(row);
                            offset += 1;
                        }
                        let row_count = (offset - start) as i64;
                        ReadRowsResponse {
                            rows: Some(proto::read_rows_response::Rows::AvroRows(AvroRows {
                                serialized_binary_rows,
                                row_count,
                            })),
                            row_count,
                            schema: first.then(|| {
                                proto::read_rows_response::Schema::AvroSchema(schema.clone())
                            }),
                        }
                    }
                    SnapshotRows::Arrow { schema, encoder, rows, sizes } => {
                        let mut bytes = 0;
                        for size in sizes[start..].iter().take(MAX_READ_BATCH_ROWS) {
                            if offset > start && bytes + size > MAX_READ_BATCH_BYTES {
                                break;
                            }
                            bytes += size;
                            offset += 1;
                        }
                        let batch = match encoder.record_batch(&rows[start..offset]) {
                            Ok(batch) => batch,
                            Err(status) => {
                                yield Err(status);
                                break;
                            }
                        };
                        let row_count = (offset - start) as i64;
                        ReadRowsResponse {
                            rows: Some(proto::read_rows_response::Rows::ArrowRecordBatch(
                                ArrowRecordBatch {
                                    serialized_record_batch: batch,
                                    row_count,
                                },
                            )),
                            row_count,
                            schema: first.then(|| {
                                proto::read_rows_response::Schema::ArrowSchema(schema.clone())
                            }),
                        }
                    }
                };
                first = false;
                yield Ok(response);
            }
        };
        Ok(Response::new(Box::pin(stream)))
    }
}

/// Parses `row_restriction` with the local SQL expression evaluator.
/// User-defined functions are not allowed in read filters.
fn parse_restriction(restriction: &str) -> Result<Option<SqlExpr>, Status> {
    if restriction.trim().is_empty() {
        return Ok(None);
    }
    let expr = parse_expr(restriction)
        .map_err(|err| Status::invalid_argument(format!("invalid row restriction: {err}")))?;
    if calls_routine(&expr) {
        return Err(Status::invalid_argument(
            "row restrictions cannot call user-defined functions",
        ));
    }
    Ok(Some(expr))
}

fn calls_routine(expr: &SqlExpr) -> bool {
    match expr {
        SqlExpr::FunctionCall { name_parts, args } => {
            name_parts.len() > 1 || args.iter().any(calls_routine)
        }
        SqlExpr::UnaryOp { expr, .. }
        | SqlExpr::IsNull { expr, .. }
        | SqlExpr::IsBool { expr, .. }
        | SqlExpr::Cast { expr, .. } => calls_routine(expr),
        SqlExpr::BinaryOp { left, right, .. } => calls_routine(left) || calls_routine(right),
        SqlExpr::Case {
            operand,
            when_clauses,
            else_expr,
        } => {
            operand.as_deref().is_some_and(calls_routine)
                || else_expr.as_deref().is_some_and(calls_routine)
                || when_clauses
                    .iter()
                    .any(|(when, then)| calls_routine(when) || calls_routine(then))
        }
        _ => false,
    }
}

fn row_matches(
    server: &Server,
    project: &str,
    resource: &TableResource,
    row: &StoredRow,
    expr: &SqlExpr,
) -> Result<bool, Status> {
    let resolve = |name: &str| -> Option<SqlValue> {
        let field = find_schema_field(&resource.schema, name)?;
        if field.mode.eq_ignore_ascii_case("REPEATED") || !field.fields.is_empty() {
            return None;
        }
        let raw = row.json.get(&field.name).map_or("", |raw| raw.get());
        Some(SqlValue::from_raw_json(raw, Some(field)))
    };
    match eval_expr(server, project, expr, &resolve, 0) {
        Ok(SqlValue::Bool(keep)) => Ok(keep),
        Ok(SqlValue::Null) => Ok(false),
        Ok(_) => Err(Status::invalid_argument(
            "row restriction must evaluate to BOOL",
        )),
        Err(err) => Err(Status::invalid_argument(format!(
            "invalid row restriction: {err}"
        ))),
    }
}

fn check_snapshot_time(
    server: &Server,
    table: &TableName,
    resource: &TableResource,
    time: i64,
) -> Result<(), Status> {
    let now = now_unix_nanos();
    if time > now {
        return Err(Status::invalid_argument("snapshot time is in the future"));
    }
    if time < now - TIME_TRAVEL_NANOS {
        return Err(Status::invalid_argument(
            "snapshot time is outside the time travel window",
        ));
    }
    let created = resource
        .creation_time
        .parse::<i64>()
        .unwrap_or_default()
        .saturating_mul(1_000_000);
    if time < created {
        return Err(Status::invalid_argument(
            "snapshot time is before the table was created",
        ));
    }
    if server
        .rows_history_reset(table.project, table.dataset, table.table)
        .is_some_and(|reset| time < reset)
    {
        return Err(Status::failed_precondition(
            "local table history is unavailable before the last truncate, copy, or query write",
        ));
    }
    Ok(())
}

/// Parses the `insertedAt` form written by `format_rfc3339_nanos`.
fn parse_rfc3339_nanos(text: &str) -> Option<i64> {
    let text = text.strip_suffix('Z')?;
    let (date, time) = text.split_once('T')?;
    let mut date = date.splitn(3, '-').map(|p| p.parse::<i64>().ok());
    let (year, month, day) = (date.next()??, date.next()??, date.next()??);
    let (clock, fraction) = time.split_once('.').unwrap_or((time, ""));
    let mut clock = clock.splitn(3, ':').map(|p| p.parse::<i64>().ok());
    let (hour, minute, second) = (clock.next()??, clock.next()??, clock.next()??);
    if fraction.len() > 9 || !fraction.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let nanos = if fraction.is_empty() {
        0
    } else {
        format!("{fraction:0<9}").parse::<i64>().ok()?
    };
    // Howard Hinnant's days_from_civil.
    let y = if month <= 2 { year - 1 } else { year };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let mp = (month + 9) % 12;
    let doy = (153 * mp + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146_097 + doe - 719_468;
    Some(((days * 86_400 + hour * 3_600 + minute * 60 + second) * 1_000_000_000) + nanos)
}

pub(crate) struct TableName<'a> {
    project: &'a str,
    dataset: &'a str,
    table: &'a str,
}

impl<'a> TableName<'a> {
    fn parse(name: &'a str) -> Result<Self, Status> {
        let parts: Vec<_> = name.split('/').collect();
        let ["projects", project, "datasets", dataset, "tables", table] = parts.as_slice() else {
            return Err(Status::invalid_argument("expected table resource name"));
        };
        for (id, kind) in [
            (*project, "project"),
            (*dataset, "dataset"),
            (*table, "table"),
        ] {
            validate_resource_id(id, kind).map_err(Status::invalid_argument)?;
        }
        Ok(Self {
            project,
            dataset,
            table,
        })
    }

    fn target(&self) -> Target {
        Target::Table(
            self.project.to_string(),
            self.dataset.to_string(),
            self.table.to_string(),
        )
    }

    fn load(&self, server: &Server) -> Result<TableResource, Status> {
        let table = server
            .read_table(self.project, self.dataset, self.table)
            .map_err(storage_error)?
            .ok_or_else(|| Status::not_found("table not found"))?;
        if !table.table_type.is_empty() && !table.table_type.eq_ignore_ascii_case("TABLE") {
            return Err(Status::unimplemented("only base tables are supported"));
        }
        Ok(table)
    }
}

fn storage_error(_: crate::storage::StorageError) -> Status {
    Status::internal("BigQuery storage operation failed")
}

#[cfg(test)]
mod tests {
    use super::parse_rfc3339_nanos;
    use crate::tabledata_handlers::format_rfc3339_nanos;

    #[test]
    fn rfc3339_parse_round_trips_insert_times() {
        for nanos in [
            0,
            1_700_000_000_123_450_000,
            1_700_000_000_000_000_000,
            951_782_400_000_000_001,
        ] {
            assert_eq!(
                parse_rfc3339_nanos(&format_rfc3339_nanos(nanos)),
                Some(nanos)
            );
        }
        assert_eq!(parse_rfc3339_nanos(""), None);
        assert_eq!(parse_rfc3339_nanos("2023-11-14T22:13:20"), None);
    }
}
