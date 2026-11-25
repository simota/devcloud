//! Storage Write API: the table's `_default` stream plus managed streams.
//!
//! - `COMMITTED`: appended rows are visible immediately.
//! - `PENDING`: rows stay in the stream until `FinalizeWriteStream` and
//!   `BatchCommitWriteStreams`, which commits every listed stream atomically.
//! - `BUFFERED`: rows become visible up to the offset passed to `FlushRows`.
//!
//! Managed streams accept `offset` (exactly-once appends: an offset already
//! written is `ALREADY_EXISTS`, one past the end is `OUT_OF_RANGE`). They live
//! in memory: unflushed or uncommitted rows are lost on restart.

use std::collections::HashSet;
use std::sync::Arc;

use tonic::{Request, Response, Status, Streaming};

use crate::model::{StoredRow, TableResource};
use crate::server::{now_unix_nanos, Server};
use crate::strict_auth::Caller;
use crate::tabledata_handlers::{format_rfc3339_nanos, validate_row_json};

use super::codec::ProtoDecoder;
use super::proto::append_rows_request::Rows;
use super::proto::append_rows_response::{AppendResult, Error, Response as AppendOutcome};
use super::proto::big_query_write_server::BigQueryWrite;
use super::proto::storage_error::StorageErrorCode;
use super::proto::write_stream::{Type as StreamType, WriteMode};
use super::proto::{
    self, AppendRowsRequest, AppendRowsResponse, BatchCommitWriteStreamsRequest,
    BatchCommitWriteStreamsResponse, CreateWriteStreamRequest, FinalizeWriteStreamRequest,
    FinalizeWriteStreamResponse, FlushRowsRequest, FlushRowsResponse, GetWriteStreamRequest,
    RowError, StorageError, WriteStream,
};
use super::{storage_error, GrpcStream, State, StorageApi, TableName, MAX_SNAPSHOT_BYTES};

const MAX_WRITE_STREAMS: usize = 64;
const DEFAULT_STREAM: &str = "_default";

pub(crate) struct ManagedStream {
    kind: StreamType,
    table: String,
    location: String,
    create_time: i64,
    commit_time: Option<i64>,
    finalized: bool,
    /// Appended rows not yet in the table (PENDING / BUFFERED).
    rows: Vec<StoredRow>,
    bytes: usize,
    row_count: i64,
    /// BUFFERED: rows `[0, flushed)` are in the table.
    flushed: i64,
}

impl ManagedStream {
    fn done(&self) -> bool {
        self.commit_time.is_some() || (self.finalized && self.rows.is_empty())
    }

    fn resource(&self, name: &str) -> WriteStream {
        WriteStream {
            name: name.to_string(),
            r#type: self.kind.into(),
            create_time: Some(timestamp(self.create_time)),
            commit_time: self.commit_time.map(timestamp),
            write_mode: WriteMode::Insert.into(),
            location: self.location.clone(),
        }
    }
}

fn timestamp(nanos: i64) -> prost_types::Timestamp {
    prost_types::Timestamp {
        seconds: nanos.div_euclid(1_000_000_000),
        nanos: nanos.rem_euclid(1_000_000_000) as i32,
    }
}

/// `projects/../tables/t/streams/s` → (table name, stream id).
fn split_stream(name: &str) -> Result<(TableName<'_>, &str), Status> {
    let (table, id) = name
        .rsplit_once("/streams/")
        .ok_or_else(|| Status::invalid_argument("write stream is required"))?;
    Ok((TableName::parse(table)?, id))
}

fn location(server: &Server, resource: &TableResource) -> String {
    if resource.location.is_empty() {
        server.default_location().to_string()
    } else {
        resource.location.clone()
    }
}

/// Appends `rows` to the table; the caller holds the table lock.
fn commit_locked(
    server: &Server,
    resource: &TableResource,
    mut rows: Vec<StoredRow>,
) -> Result<(), Status> {
    if rows.is_empty() {
        return Ok(());
    }
    let inserted_at = format_rfc3339_nanos(now_unix_nanos());
    for row in &mut rows {
        row.inserted_at.clone_from(&inserted_at);
    }
    let reference = &resource.table_reference;
    let existing = server
        .read_rows(
            &reference.project_id,
            &reference.dataset_id,
            &reference.table_id,
        )
        .map_err(storage_error)?;
    if (existing.len() + rows.len()) as i64 > server.max_rows_per_table() {
        return Err(Status::resource_exhausted("table row limit exceeded"));
    }
    server
        .commit_storage_rows(resource, &existing, &rows)
        .map_err(storage_error)
}

fn row_bytes(rows: &[StoredRow]) -> usize {
    rows.iter()
        .map(|row| row.json.values().map(|v| v.get().len()).sum::<usize>() + 16)
        .sum()
}

#[tonic::async_trait]
impl BigQueryWrite for StorageApi {
    type AppendRowsStream = GrpcStream<AppendRowsResponse>;

    async fn create_write_stream(
        &self,
        request: Request<CreateWriteStreamRequest>,
    ) -> Result<Response<WriteStream>, Status> {
        let state = &self.0;
        let caller = state.caller(&request)?;
        let request = request.into_inner();
        let table = TableName::parse(&request.parent)?;
        state.require(&caller, table.target(), "bigquery.tables.updateData")?;
        let kind = request
            .write_stream
            .map(|stream| StreamType::try_from(stream.r#type).unwrap_or_default())
            .unwrap_or_default();
        if kind == StreamType::Unspecified {
            return Err(Status::invalid_argument(
                "write stream type must be COMMITTED, PENDING, or BUFFERED",
            ));
        }
        let resource = table.load(&state.server)?;
        let mut streams = state.write_streams.lock().unwrap();
        if streams.len() >= MAX_WRITE_STREAMS {
            streams.retain(|_, stream| !stream.done());
        }
        if streams.len() >= MAX_WRITE_STREAMS {
            return Err(Status::resource_exhausted("too many local write streams"));
        }
        let name = format!("{}/streams/{}", request.parent, state.next_id());
        let stream = ManagedStream {
            kind,
            table: request.parent,
            location: location(&state.server, &resource),
            create_time: now_unix_nanos(),
            commit_time: None,
            finalized: false,
            rows: Vec::new(),
            bytes: 0,
            row_count: 0,
            flushed: 0,
        };
        let response = stream.resource(&name);
        streams.insert(name, stream);
        Ok(Response::new(response))
    }

    async fn append_rows(
        &self,
        request: Request<Streaming<AppendRowsRequest>>,
    ) -> Result<Response<Self::AppendRowsStream>, Status> {
        let state = self.0.clone();
        let caller = state.caller(&request)?;
        let mut incoming = request.into_inner();
        let mut shutdown = state.shutdown.clone();
        let stream = async_stream::try_stream! {
            let mut connection = WriteConnection {
                caller,
                stream: String::new(),
                decoder: None,
            };
            loop {
                let next = tokio::select! {
                    biased;
                    _ = shutdown.wait_for(|closed| *closed) => break,
                    message = incoming.message() => message,
                };
                let Some(request) = next? else { break };
                yield connection.append(&state, request);
            }
        };
        Ok(Response::new(Box::pin(stream)))
    }

    async fn get_write_stream(
        &self,
        request: Request<GetWriteStreamRequest>,
    ) -> Result<Response<WriteStream>, Status> {
        let state = &self.0;
        let caller = state.caller(&request)?;
        let name = request.into_inner().name;
        let (table, id) = split_stream(&name)?;
        state.require(&caller, table.target(), "bigquery.tables.updateData")?;
        if id == DEFAULT_STREAM {
            let resource = table.load(&state.server)?;
            let created = resource
                .creation_time
                .parse::<i64>()
                .unwrap_or_default()
                .saturating_mul(1_000_000);
            return Ok(Response::new(WriteStream {
                name,
                r#type: StreamType::Committed.into(),
                create_time: Some(timestamp(created)),
                commit_time: None,
                write_mode: WriteMode::Insert.into(),
                location: location(&state.server, &resource),
            }));
        }
        let streams = state.write_streams.lock().unwrap();
        let stream = streams
            .get(&name)
            .ok_or_else(|| Status::not_found("write stream not found"))?;
        Ok(Response::new(stream.resource(&name)))
    }

    async fn finalize_write_stream(
        &self,
        request: Request<FinalizeWriteStreamRequest>,
    ) -> Result<Response<FinalizeWriteStreamResponse>, Status> {
        let state = &self.0;
        let caller = state.caller(&request)?;
        let name = request.into_inner().name;
        let (table, id) = split_stream(&name)?;
        state.require(&caller, table.target(), "bigquery.tables.updateData")?;
        if id == DEFAULT_STREAM {
            return Err(Status::invalid_argument(
                "the default stream cannot be finalized",
            ));
        }
        let mut streams = state.write_streams.lock().unwrap();
        let stream = streams
            .get_mut(&name)
            .ok_or_else(|| Status::not_found("write stream not found"))?;
        stream.finalized = true;
        Ok(Response::new(FinalizeWriteStreamResponse {
            row_count: stream.row_count,
        }))
    }

    async fn batch_commit_write_streams(
        &self,
        request: Request<BatchCommitWriteStreamsRequest>,
    ) -> Result<Response<BatchCommitWriteStreamsResponse>, Status> {
        let state = &self.0;
        let caller = state.caller(&request)?;
        let request = request.into_inner();
        let table = TableName::parse(&request.parent)?;
        state.require(&caller, table.target(), "bigquery.tables.updateData")?;
        if request.write_streams.is_empty() {
            return Err(Status::invalid_argument("write streams are required"));
        }
        let mut seen = HashSet::new();
        let names: Vec<_> = request
            .write_streams
            .into_iter()
            .filter(|name| seen.insert(name.clone()))
            .collect();

        let mut streams = state.write_streams.lock().unwrap();
        let error = |code: StorageErrorCode, entity: &str, message: &str| StorageError {
            code: code.into(),
            entity: entity.to_string(),
            error_message: message.to_string(),
        };
        let stream_errors: Vec<_> = names
            .iter()
            .filter_map(|name| match streams.get(name) {
                None => Some(error(
                    StorageErrorCode::StreamNotFound,
                    name,
                    "stream not found",
                )),
                Some(stream) if stream.table != request.parent => Some(error(
                    StorageErrorCode::InvalidStreamState,
                    name,
                    "stream belongs to a different table",
                )),
                Some(stream) if stream.kind != StreamType::Pending => Some(error(
                    StorageErrorCode::InvalidStreamType,
                    name,
                    "only PENDING streams can be committed",
                )),
                Some(stream) if stream.commit_time.is_some() => Some(error(
                    StorageErrorCode::StreamAlreadyCommitted,
                    name,
                    "stream is already committed",
                )),
                Some(stream) if !stream.finalized => Some(error(
                    StorageErrorCode::InvalidStreamState,
                    name,
                    "stream is not finalized",
                )),
                Some(_) => None,
            })
            .collect();
        if !stream_errors.is_empty() {
            return Ok(Response::new(BatchCommitWriteStreamsResponse {
                commit_time: None,
                stream_errors,
            }));
        }

        let table_lock = state
            .server
            .table_lock(table.project, table.dataset, table.table);
        let _guard = table_lock.lock().unwrap();
        let resource = table.load(&state.server)?;
        let rows = names
            .iter()
            .flat_map(|name| streams[name].rows.iter().cloned())
            .collect();
        commit_locked(&state.server, &resource, rows)?;
        let now = now_unix_nanos();
        for name in &names {
            let stream = streams.get_mut(name).unwrap();
            stream.commit_time = Some(now);
            stream.rows.clear();
            stream.bytes = 0;
        }
        Ok(Response::new(BatchCommitWriteStreamsResponse {
            commit_time: Some(timestamp(now)),
            stream_errors: Vec::new(),
        }))
    }

    async fn flush_rows(
        &self,
        request: Request<FlushRowsRequest>,
    ) -> Result<Response<FlushRowsResponse>, Status> {
        let state = &self.0;
        let caller = state.caller(&request)?;
        let request = request.into_inner();
        let (table, _) = split_stream(&request.write_stream)?;
        state.require(&caller, table.target(), "bigquery.tables.updateData")?;
        let offset = request
            .offset
            .ok_or_else(|| Status::invalid_argument("flush offset is required"))?;
        let mut streams = state.write_streams.lock().unwrap();
        let stream = streams
            .get_mut(&request.write_stream)
            .ok_or_else(|| Status::not_found("write stream not found"))?;
        if stream.kind != StreamType::Buffered {
            return Err(Status::invalid_argument(
                "FlushRows is only supported on BUFFERED streams",
            ));
        }
        if offset < 0 || offset >= stream.row_count {
            return Err(Status::out_of_range(
                "flush offset is beyond the end of the stream",
            ));
        }
        if offset >= stream.flushed {
            let count = (offset + 1 - stream.flushed) as usize;
            let table_lock = state
                .server
                .table_lock(table.project, table.dataset, table.table);
            let _guard = table_lock.lock().unwrap();
            let resource = table.load(&state.server)?;
            commit_locked(&state.server, &resource, stream.rows[..count].to_vec())?;
            stream.bytes -= row_bytes(&stream.rows[..count]);
            stream.rows.drain(..count);
            stream.flushed = offset + 1;
        }
        Ok(Response::new(FlushRowsResponse { offset }))
    }
}

struct WriteConnection {
    caller: Option<Caller>,
    stream: String,
    decoder: Option<ProtoDecoder>,
}

impl WriteConnection {
    fn append(&mut self, state: &Arc<State>, request: AppendRowsRequest) -> AppendRowsResponse {
        let write_stream = if request.write_stream.is_empty() {
            self.stream.clone()
        } else {
            request.write_stream.clone()
        };
        let mut row_errors = Vec::new();
        let outcome = match self.append_batch(state, &write_stream, request, &mut row_errors) {
            Ok(offset) => AppendOutcome::AppendResult(AppendResult { offset }),
            Err(error) => AppendOutcome::Error(Error {
                code: error.code() as i32,
                message: error.message().to_string(),
            }),
        };
        AppendRowsResponse {
            response: Some(outcome),
            write_stream,
            row_errors,
        }
    }

    /// Returns the offset of the batch's first row for managed streams.
    fn append_batch(
        &mut self,
        state: &State,
        stream: &str,
        request: AppendRowsRequest,
        row_errors: &mut Vec<RowError>,
    ) -> Result<Option<i64>, Status> {
        let server = &state.server;
        let (table, id) = split_stream(stream)?;
        state.require(&self.caller, table.target(), "bigquery.tables.updateData")?;
        let managed = id != DEFAULT_STREAM;
        if request.offset.is_some() && !managed {
            return Err(Status::invalid_argument(
                "offsets are unsupported on the default stream",
            ));
        }
        if !matches!(request.default_missing_value_interpretation, 0 | 1)
            || request
                .missing_value_interpretations
                .values()
                .any(|&value| value != 1)
        {
            return Err(Status::unimplemented(
                "only NULL missing value interpretation is supported",
            ));
        }
        let data = match request.rows {
            Some(Rows::ProtoRows(data)) => data,
            Some(Rows::ArrowRows(_)) => {
                return Err(Status::unimplemented("only Protobuf writes are supported"))
            }
            None => return Err(Status::invalid_argument("append rows are required")),
        };
        let new_decoder =
            data.writer_schema
                .map(|schema| {
                    ProtoDecoder::new(schema.proto_descriptor.ok_or_else(|| {
                        Status::invalid_argument("Protobuf descriptor is required")
                    })?)
                })
                .transpose()?;
        let decoder = new_decoder
            .as_ref()
            .or(self.decoder.as_ref().filter(|_| stream == self.stream))
            .ok_or_else(|| Status::invalid_argument("writer schema is required for this stream"))?;

        let mut streams = managed.then(|| state.write_streams.lock().unwrap());
        let managed_stream = match streams.as_mut() {
            Some(streams) => {
                let entry = streams
                    .get_mut(stream)
                    .ok_or_else(|| Status::not_found("write stream not found"))?;
                if entry.finalized || entry.commit_time.is_some() {
                    return Err(Status::invalid_argument("write stream is finalized"));
                }
                if let Some(offset) = request.offset {
                    if offset < entry.row_count {
                        return Err(Status::already_exists("offset has already been written"));
                    }
                    if offset > entry.row_count {
                        return Err(Status::out_of_range(
                            "offset is beyond the end of the stream",
                        ));
                    }
                }
                Some(entry)
            }
            None => None,
        };

        let table_lock = server.table_lock(table.project, table.dataset, table.table);
        let _guard = table_lock.lock().unwrap();
        let resource = table.load(server)?;
        decoder.check_table(&resource.schema.fields)?;

        let rows = data
            .rows
            .ok_or_else(|| Status::invalid_argument("append rows are required"))?;
        let mut accepted = Vec::with_capacity(rows.serialized_rows.len());
        for (index, bytes) in rows.serialized_rows.iter().enumerate() {
            let values = decoder.decode(bytes).and_then(|row| {
                let (values, errors) = validate_row_json(row, &resource.schema, false);
                if errors.is_empty() {
                    Ok(values)
                } else {
                    Err("row does not match the table schema")
                }
            });
            match values {
                Ok(json) => accepted.push(StoredRow {
                    insert_id: String::new(),
                    json,
                    inserted_at: String::new(),
                }),
                Err(message) => row_errors.push(RowError {
                    index: index as i64,
                    code: proto::row_error::RowErrorCode::FieldsError.into(),
                    message: message.into(),
                }),
            }
        }
        if let Some(decoder) = new_decoder {
            self.decoder = Some(decoder);
        }
        self.stream = stream.to_owned();
        if !row_errors.is_empty() {
            return Err(Status::invalid_argument(
                "append batch contains invalid rows",
            ));
        }
        let Some(entry) = managed_stream else {
            commit_locked(server, &resource, accepted)?;
            return Ok(None);
        };
        let start = entry.row_count;
        let count = accepted.len() as i64;
        if entry.kind == StreamType::Committed {
            commit_locked(server, &resource, accepted)?;
        } else {
            let bytes = row_bytes(&accepted);
            let existing = server
                .read_rows(table.project, table.dataset, table.table)
                .map_err(storage_error)?;
            if (existing.len() + entry.rows.len() + accepted.len()) as i64
                > server.max_rows_per_table()
            {
                return Err(Status::resource_exhausted("table row limit exceeded"));
            }
            if entry.bytes + bytes > MAX_SNAPSHOT_BYTES {
                return Err(Status::resource_exhausted(
                    "local write stream buffer exceeds size limit",
                ));
            }
            entry.bytes += bytes;
            entry.rows.extend(accepted);
        }
        entry.row_count += count;
        Ok(Some(start))
    }
}
