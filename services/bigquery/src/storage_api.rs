use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use tokio::{net::TcpListener, sync::watch};
use tokio_stream::{wrappers::TcpListenerStream, Stream};
use tonic::{Request, Response, Status, Streaming};

use crate::model::{StoredRow, TableResource};
use crate::routes::authorize;
use crate::server::{now_unix_nanos, Server};
use crate::tabledata_handlers::{format_rfc3339_nanos, validate_row_json};
use crate::validation::validate_resource_id;

mod codec;

pub mod proto {
    tonic::include_proto!("google.cloud.bigquery.storage.v1");
}

use codec::{AvroEncoder, ProtoDecoder};
use proto::append_rows_request::Rows;
use proto::append_rows_response::{AppendResult, Error, Response as AppendOutcome};
use proto::big_query_read_server::{BigQueryRead, BigQueryReadServer};
use proto::big_query_write_server::{BigQueryWrite, BigQueryWriteServer};
use proto::{
    AppendRowsRequest, AppendRowsResponse, AvroRows, AvroSchema, CreateReadSessionRequest,
    DataFormat, ReadRowsRequest, ReadRowsResponse, ReadSession, ReadStream, RowError,
};

const MAX_SESSIONS: usize = 16;
const MAX_SNAPSHOT_BYTES: usize = 16 * 1024 * 1024;
const MAX_READ_BATCH_BYTES: usize = 3 * 1024 * 1024;
const MAX_READ_BATCH_ROWS: usize = 256;
const MAX_SCHEMA_BYTES: usize = 512 * 1024;
const SESSION_SECONDS: i64 = 6 * 60 * 60;

type GrpcStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send>>;

struct Snapshot {
    schema: AvroSchema,
    rows: Vec<Vec<u8>>,
    expires: i64,
}

struct StorageApi {
    server: Arc<Server>,
    streams: Mutex<HashMap<String, Arc<Snapshot>>>,
    next_session: AtomicU64,
    shutdown: watch::Receiver<bool>,
}

pub async fn serve(
    listener: TcpListener,
    server: Arc<Server>,
    shutdown: impl Future<Output = ()>,
) -> Result<(), tonic::transport::Error> {
    let (stop, stopped) = watch::channel(false);
    let max_request_bytes = server.max_request_bytes() as usize;
    let api = Arc::new(StorageApi {
        server,
        streams: Mutex::new(HashMap::new()),
        next_session: AtomicU64::new(now_unix_nanos() as u64),
        shutdown: stopped,
    });
    tonic::transport::Server::builder()
        .add_service(
            BigQueryReadServer::from_arc(api.clone()).max_decoding_message_size(max_request_bytes),
        )
        .add_service(
            BigQueryWriteServer::from_arc(api).max_decoding_message_size(max_request_bytes),
        )
        .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async move {
            shutdown.await;
            let _ = stop.send(true);
        })
        .await
}

impl StorageApi {
    fn authorize<T>(&self, request: &Request<T>) -> Result<(), Status> {
        let authorization = request
            .metadata()
            .get("authorization")
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default();
        if authorize(&self.server, authorization) {
            Ok(())
        } else {
            Err(Status::unauthenticated(
                "invalid authentication credentials",
            ))
        }
    }
}

#[tonic::async_trait]
impl BigQueryRead for StorageApi {
    type ReadRowsStream = GrpcStream<ReadRowsResponse>;

    async fn create_read_session(
        &self,
        request: Request<CreateReadSessionRequest>,
    ) -> Result<Response<ReadSession>, Status> {
        self.authorize(&request)?;
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
        match DataFormat::try_from(session.data_format) {
            Ok(DataFormat::Avro) => {}
            Ok(DataFormat::Arrow) => {
                return Err(Status::unimplemented("only AVRO reads are supported"))
            }
            _ => return Err(Status::invalid_argument("AVRO data format is required")),
        }
        let options = session.read_options.as_ref();
        if session
            .table_modifiers
            .as_ref()
            .is_some_and(|m| m.snapshot_time.is_some())
            || options.is_some_and(|o| {
                !o.row_restriction.is_empty()
                    || o.output_format_serialization_options.is_some()
                    || o.sample_percentage.is_some()
                    || o.response_compression_codec.unwrap_or_default() != 0
            })
        {
            return Err(Status::unimplemented("unsupported read options"));
        }
        let table = TableName::parse(&session.table)?;
        let table_lock = self
            .server
            .table_lock(table.project, table.dataset, table.table);
        let guard = table_lock.lock().unwrap();
        let resource = table.load(&self.server)?;
        let selected = options.map_or(&[][..], |o| o.selected_fields.as_slice());
        let fields = &resource.schema.fields;
        if selected
            .iter()
            .any(|name| !fields.iter().any(|field| &field.name == name))
        {
            return Err(Status::invalid_argument("unknown selected column"));
        }
        let encoder = AvroEncoder::new(
            fields
                .iter()
                .filter(|field| selected.is_empty() || selected.contains(&field.name))
                .collect(),
        )?;
        let schema = encoder.schema();
        if schema.schema.len() > MAX_SCHEMA_BYTES {
            return Err(Status::resource_exhausted("Avro schema exceeds size limit"));
        }
        let rows = self
            .server
            .read_rows(table.project, table.dataset, table.table)
            .map_err(storage_error)?;
        drop(guard);

        let mut snapshot_bytes = 0;
        let rows = rows
            .iter()
            .map(|row| {
                let encoded = encoder.encode(row)?;
                snapshot_bytes += encoded.len() + std::mem::size_of::<Vec<u8>>();
                if encoded.len() > MAX_READ_BATCH_BYTES || snapshot_bytes > MAX_SNAPSHOT_BYTES {
                    return Err(Status::resource_exhausted(
                        "local read snapshot exceeds size limit",
                    ));
                }
                Ok(encoded)
            })
            .collect::<Result<Vec<_>, _>>()?;

        let location = if resource.location.is_empty() {
            self.server.default_location()
        } else {
            &resource.location
        };
        let now = now_unix_nanos() / 1_000_000_000;
        let expires = now + SESSION_SECONDS;
        let mut streams = self.streams.lock().unwrap();
        streams.retain(|_, stream| stream.expires > now);
        if streams.len() >= MAX_SESSIONS {
            return Err(Status::resource_exhausted("too many local read sessions"));
        }
        let id = self.next_session.fetch_add(1, Ordering::Relaxed);
        session.name = format!("projects/{project}/locations/{location}/sessions/{id}");
        let name = format!("{}/streams/0", session.name);
        session.streams = vec![ReadStream { name: name.clone() }];
        session.expire_time = Some(prost_types::Timestamp {
            seconds: expires,
            nanos: 0,
        });
        session.schema = Some(proto::read_session::Schema::AvroSchema(schema.clone()));
        session.estimated_row_count = rows.len() as i64;
        streams.insert(
            name,
            Arc::new(Snapshot {
                schema,
                rows,
                expires,
            }),
        );
        Ok(Response::new(session))
    }

    async fn read_rows(
        &self,
        request: Request<ReadRowsRequest>,
    ) -> Result<Response<Self::ReadRowsStream>, Status> {
        self.authorize(&request)?;
        let request = request.into_inner();
        if request.arrow_serialization_options.is_some() {
            return Err(Status::unimplemented("only AVRO reads are supported"));
        }
        let mut offset = usize::try_from(request.offset)
            .map_err(|_| Status::invalid_argument("invalid read offset"))?;
        let snapshot = {
            let now = now_unix_nanos() / 1_000_000_000;
            let mut streams = self.streams.lock().unwrap();
            streams.retain(|_, stream| stream.expires > now);
            streams
                .get(&request.read_stream)
                .cloned()
                .ok_or_else(|| Status::not_found("read stream not found"))?
        };
        if offset > snapshot.rows.len() {
            return Err(Status::out_of_range(
                "read offset is beyond the end of the stream",
            ));
        }
        let shutdown = self.shutdown.clone();
        let stream = async_stream::stream! {
            let mut schema = Some(proto::read_rows_response::Schema::AvroSchema(snapshot.schema.clone()));
            while offset < snapshot.rows.len() && !*shutdown.borrow() {
                let start = offset;
                let mut serialized_binary_rows = Vec::new();
                for row in snapshot.rows[start..].iter().take(MAX_READ_BATCH_ROWS) {
                    if serialized_binary_rows.len() + row.len() > MAX_READ_BATCH_BYTES {
                        break;
                    }
                    serialized_binary_rows.extend_from_slice(row);
                    offset += 1;
                }
                let row_count = (offset - start) as i64;
                yield Ok(ReadRowsResponse {
                    rows: Some(proto::read_rows_response::Rows::AvroRows(AvroRows {
                        serialized_binary_rows,
                        row_count,
                    })),
                    row_count,
                    schema: schema.take(),
                });
            }
        };
        Ok(Response::new(Box::pin(stream)))
    }
}

#[tonic::async_trait]
impl BigQueryWrite for StorageApi {
    type AppendRowsStream = GrpcStream<AppendRowsResponse>;

    async fn append_rows(
        &self,
        request: Request<Streaming<AppendRowsRequest>>,
    ) -> Result<Response<Self::AppendRowsStream>, Status> {
        self.authorize(&request)?;
        let server = self.server.clone();
        let mut incoming = request.into_inner();
        let mut shutdown = self.shutdown.clone();
        let stream = async_stream::try_stream! {
            let mut connection = WriteConnection::default();
            loop {
                let next = tokio::select! {
                    biased;
                    _ = shutdown.wait_for(|closed| *closed) => break,
                    message = incoming.message() => message,
                };
                let Some(request) = next? else { break };
                yield connection.append(&server, request);
            }
        };
        Ok(Response::new(Box::pin(stream)))
    }
}

#[derive(Default)]
struct WriteConnection {
    stream: String,
    decoder: Option<ProtoDecoder>,
}

impl WriteConnection {
    fn append(&mut self, server: &Server, request: AppendRowsRequest) -> AppendRowsResponse {
        let write_stream = if request.write_stream.is_empty() {
            self.stream.clone()
        } else {
            request.write_stream.clone()
        };
        let mut row_errors = Vec::new();
        let outcome = match self.append_batch(server, &write_stream, request, &mut row_errors) {
            Ok(()) => AppendOutcome::AppendResult(AppendResult { offset: None }),
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

    fn append_batch(
        &mut self,
        server: &Server,
        stream: &str,
        request: AppendRowsRequest,
        row_errors: &mut Vec<RowError>,
    ) -> Result<(), Status> {
        if request.offset.is_some() {
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
        let (name, suffix) = stream
            .rsplit_once("/streams/")
            .ok_or_else(|| Status::invalid_argument("write stream is required"))?;
        if suffix != "_default" {
            return Err(Status::unimplemented(
                "only the default write stream is supported",
            ));
        }
        let table = TableName::parse(name)?;
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
        let table_lock = server.table_lock(table.project, table.dataset, table.table);
        let _guard = table_lock.lock().unwrap();
        let resource = table.load(server)?;
        decoder.check_table(&resource.schema.fields)?;

        let rows = data
            .rows
            .ok_or_else(|| Status::invalid_argument("append rows are required"))?;
        let inserted_at = format_rfc3339_nanos(now_unix_nanos());
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
                    inserted_at: inserted_at.clone(),
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
        if !accepted.is_empty() {
            let existing = server
                .read_rows(table.project, table.dataset, table.table)
                .map_err(storage_error)?;
            if (existing.len() + accepted.len()) as i64 > server.max_rows_per_table() {
                return Err(Status::resource_exhausted("table row limit exceeded"));
            }
            server
                .commit_storage_rows(&resource, &existing, &accepted)
                .map_err(storage_error)?;
        }
        Ok(())
    }
}

struct TableName<'a> {
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
