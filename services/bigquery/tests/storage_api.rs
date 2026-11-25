use std::sync::Arc;
use std::time::Duration;

use devcloud_bigquery::routes::{handle, Request as RestRequest};
use devcloud_bigquery::server::{Config, Server};
use devcloud_bigquery::storage_api::proto::append_rows_request::{ProtoData, Rows};
use devcloud_bigquery::storage_api::proto::append_rows_response::Response as AppendOutcome;
use devcloud_bigquery::storage_api::proto::big_query_read_client::BigQueryReadClient;
use devcloud_bigquery::storage_api::proto::big_query_write_client::BigQueryWriteClient;
use devcloud_bigquery::storage_api::proto::read_session::TableModifiers;
use devcloud_bigquery::storage_api::proto::read_session::{Schema, TableReadOptions};
use devcloud_bigquery::storage_api::proto::write_stream::Type as StreamType;
use devcloud_bigquery::storage_api::proto::*;
use devcloud_bigquery::{storage_api, validation::Query};
use prost::Message;
use prost_types::{
    field_descriptor_proto::{Label, Type},
    DescriptorProto, FieldDescriptorProto,
};
use serde_json::{json, Value};
use tokio::net::TcpListener;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::{Channel, Endpoint};
use tonic::{Code, Request};

const TABLE: &str = "projects/demo/datasets/test/tables/items";
const WRITE_STREAM: &str = "projects/demo/datasets/test/tables/items/streams/_default";

struct Fixture {
    _storage: tempfile::TempDir,
    server: Arc<Server>,
    channel: Channel,
    stop: oneshot::Sender<()>,
    task: JoinHandle<Result<(), tonic::transport::Error>>,
}

impl Fixture {
    async fn start(mode: &str, max_rows_per_table: i64) -> Self {
        Self::start_with(mode, max_rows_per_table, &[]).await
    }

    async fn start_with(mode: &str, max_rows_per_table: i64, principals: &[(&str, &str)]) -> Self {
        let storage = tempfile::tempdir().unwrap();
        let mut server = Server::new(Config {
            auth_mode: mode.into(),
            bearer_token: "local-test-token".into(),
            storage_path: storage.path().to_str().unwrap().into(),
            max_rows_per_table,
            ..Default::default()
        });
        for (token, member) in principals {
            server.add_principal(*token, *member);
        }
        let server = Arc::new(server);
        assert_eq!(
            server
                .create_dataset("demo", br#"{"datasetReference":{"datasetId":"test"}}"#)
                .status,
            200
        );
        assert_eq!(
            server
                .create_table(
                    "demo",
                    "test",
                    br#"{
                "tableReference":{"tableId":"items"},
                "schema":{"fields":[
                    {"name":"id","type":"INT64","mode":"REQUIRED"},
                    {"name":"name","type":"STRING"},
                    {"name":"active","type":"BOOL"},
                    {"name":"score","type":"FLOAT64"},
                    {"name":"data","type":"BYTES"}
                ]}
            }"#
                )
                .status,
            200
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (stop, stopped) = oneshot::channel();
        let task = tokio::spawn(storage_api::serve(listener, server.clone(), async {
            let _ = stopped.await;
        }));
        let channel = Endpoint::from_shared(format!("http://{addr}"))
            .unwrap()
            .timeout(Duration::from_secs(5))
            .connect()
            .await
            .unwrap();
        Self {
            _storage: storage,
            server,
            channel,
            stop,
            task,
        }
    }

    async fn shutdown(self) {
        self.stop.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(5), self.task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }

    fn insert_all(&self, rows: Vec<Value>) {
        let response = self.server.insert_rows(
            "demo",
            "test",
            "items",
            &serde_json::to_vec(&json!({"rows": rows})).unwrap(),
        );
        assert_eq!(response.status, 200);
        assert!(serde_json::from_slice::<Value>(&response.body)
            .unwrap()
            .get("insertErrors")
            .is_none());
    }

    fn list_rest_rows(&self) -> Value {
        let response = self
            .server
            .list_rows("demo", "test", "items", &Query::default());
        assert_eq!(response.status, 200);
        serde_json::from_slice(&response.body).unwrap()
    }
}

fn read_session(selected: &[&str]) -> CreateReadSessionRequest {
    CreateReadSessionRequest {
        parent: "projects/demo".into(),
        read_session: Some(ReadSession {
            table: TABLE.into(),
            data_format: DataFormat::Avro.into(),
            read_options: Some(TableReadOptions {
                selected_fields: selected.iter().map(|name| name.to_string()).collect(),
                ..Default::default()
            }),
            ..Default::default()
        }),
        max_stream_count: 3,
        ..Default::default()
    }
}

#[tokio::test]
async fn read_rows_streams_projected_avro_snapshot_and_resumes_at_offset() {
    let fixture = Fixture::start("relaxed", 0).await;
    fixture.insert_all(
        (0..257)
            .map(|id| json!({"json": {"id": id, "name": if id == 256 { Some("z") } else { None }}}))
            .collect(),
    );
    let mut client = BigQueryReadClient::new(fixture.channel.clone());
    let session = client
        .create_read_session(read_session(&["name", "id"]))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(session.streams.len(), 1);
    assert_eq!(session.estimated_row_count, 257);
    assert!(session.expire_time.is_some());
    let Some(Schema::AvroSchema(schema)) = session.schema else {
        panic!("missing Avro schema")
    };
    assert_eq!(
        serde_json::from_str::<Value>(&schema.schema).unwrap()["fields"],
        json!([
            {"name": "id", "type": "long"},
            {"name": "name", "type": ["null", "string"], "default": null}
        ])
    );
    fixture.insert_all(vec![json!({"json": {"id": 257}})]);

    let request = ReadRowsRequest {
        read_stream: session.streams[0].name.clone(),
        ..Default::default()
    };
    let mut stream = client
        .read_rows(request.clone())
        .await
        .unwrap()
        .into_inner();
    let first = stream.message().await.unwrap().unwrap();
    assert_eq!(first.row_count, 256);
    assert_eq!(
        first.schema,
        Some(read_rows_response::Schema::AvroSchema(schema))
    );
    let Some(read_rows_response::Rows::AvroRows(rows)) = first.rows else {
        panic!("missing Avro rows")
    };
    assert_eq!(&rows.serialized_binary_rows[..6], &[0, 0, 2, 0, 4, 0]);
    let last = stream.message().await.unwrap().unwrap();
    assert_eq!(last.row_count, 1);
    assert!(last.schema.is_none());
    let Some(read_rows_response::Rows::AvroRows(rows)) = last.rows else {
        panic!("missing Avro rows")
    };
    assert_eq!(rows.serialized_binary_rows, [0x80, 0x04, 2, 2, b'z']);
    assert!(stream.message().await.unwrap().is_none());

    let mut resumed = client
        .read_rows(ReadRowsRequest {
            offset: 256,
            ..request.clone()
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(resumed.message().await.unwrap().unwrap().row_count, 1);
    assert!(resumed.message().await.unwrap().is_none());
    for (offset, code) in [(-1, Code::InvalidArgument), (258, Code::OutOfRange)] {
        assert_eq!(
            client
                .read_rows(ReadRowsRequest {
                    offset,
                    ..request.clone()
                })
                .await
                .unwrap_err()
                .code(),
            code
        );
    }
    assert_eq!(
        client
            .read_rows(ReadRowsRequest {
                read_stream: "unknown".into(),
                ..Default::default()
            })
            .await
            .unwrap_err()
            .code(),
        Code::NotFound
    );
    fixture.shutdown().await;
}

#[derive(Clone, PartialEq, Message)]
struct TestRow {
    #[prost(int64, optional, tag = "12")]
    id: Option<i64>,
    #[prost(string, optional, tag = "2")]
    name: Option<String>,
    #[prost(bool, optional, tag = "3")]
    active: Option<bool>,
    #[prost(double, optional, tag = "9")]
    score: Option<f64>,
    #[prost(bytes = "vec", optional, tag = "10")]
    data: Option<Vec<u8>>,
}

fn writer_schema() -> ProtoSchema {
    ProtoSchema {
        proto_descriptor: Some(DescriptorProto {
            name: Some("TestRow".into()),
            field: [
                ("name", 2, Type::String),
                ("id", 12, Type::Int64),
                ("active", 3, Type::Bool),
                ("score", 9, Type::Double),
                ("data", 10, Type::Bytes),
            ]
            .into_iter()
            .map(|(name, number, field_type)| FieldDescriptorProto {
                name: Some(name.into()),
                number: Some(number),
                label: Some(Label::Optional as i32),
                r#type: Some(field_type as i32),
                ..Default::default()
            })
            .collect(),
            ..Default::default()
        }),
    }
}

fn append_request(first: bool, serialized_rows: Vec<Vec<u8>>) -> AppendRowsRequest {
    AppendRowsRequest {
        write_stream: if first {
            WRITE_STREAM.into()
        } else {
            String::new()
        },
        rows: Some(Rows::ProtoRows(ProtoData {
            writer_schema: first.then(writer_schema),
            rows: Some(ProtoRows { serialized_rows }),
        })),
        ..Default::default()
    }
}

#[tokio::test]
async fn append_rows_reuses_schema_rejects_entire_invalid_batch_and_updates_rest() {
    let fixture = Fixture::start("relaxed", 2).await;
    let mut client = BigQueryWriteClient::new(fixture.channel.clone());
    let (tx, rx) = mpsc::channel(1);
    let first = TestRow {
        id: Some(-3),
        name: Some(String::new()),
        active: Some(false),
        score: Some(1.25),
        data: Some(vec![0, 255]),
    }
    .encode_to_vec();
    tx.send(append_request(true, vec![first.clone()]))
        .await
        .unwrap();
    let mut replies = client
        .append_rows(ReceiverStream::new(rx))
        .await
        .unwrap()
        .into_inner();
    let reply = replies.message().await.unwrap().unwrap();
    assert!(
        matches!(reply.response, Some(AppendOutcome::AppendResult(result)) if result.offset.is_none())
    );
    assert_eq!(reply.write_stream, WRITE_STREAM);
    let rest = fixture.list_rest_rows();
    assert_eq!(rest["totalRows"], "1");
    assert_eq!(
        rest["rows"][0]["f"],
        json!([
        {"v":"-3"}, {"v":""}, {"v":"false"}, {"v":1.25}, {"v":"AP8="}
        ])
    );

    let second = TestRow {
        id: Some(0),
        ..Default::default()
    }
    .encode_to_vec();
    tx.send(append_request(
        false,
        vec![second.clone(), vec![], vec![0xff]],
    ))
    .await
    .unwrap();
    let reply = replies.message().await.unwrap().unwrap();
    assert!(
        matches!(reply.response, Some(AppendOutcome::Error(error)) if error.code == Code::InvalidArgument as i32)
    );
    assert_eq!(
        reply
            .row_errors
            .iter()
            .map(|error| error.index)
            .collect::<Vec<_>>(),
        [1, 2]
    );
    assert_eq!(fixture.list_rest_rows()["totalRows"], "1");

    tx.send(append_request(false, vec![second.clone()]))
        .await
        .unwrap();
    assert!(matches!(
        replies.message().await.unwrap().unwrap().response,
        Some(AppendOutcome::AppendResult(_))
    ));
    let mut offset_request = append_request(false, vec![second]);
    offset_request.offset = Some(0);
    tx.send(offset_request).await.unwrap();
    assert!(matches!(replies.message().await.unwrap().unwrap().response,
        Some(AppendOutcome::Error(error)) if error.code == Code::InvalidArgument as i32));
    tx.send(append_request(false, vec![first])).await.unwrap();
    assert!(matches!(replies.message().await.unwrap().unwrap().response,
        Some(AppendOutcome::Error(error)) if error.code == Code::ResourceExhausted as i32));
    assert_eq!(fixture.list_rest_rows()["totalRows"], "2");
    let table = fixture.server.get_table("demo", "test", "items");
    assert_eq!(
        serde_json::from_slice::<Value>(&table.body).unwrap()["numRows"],
        "2"
    );
    let query = fixture.server.query_rows(
        "demo",
        br#"{"query":"SELECT id FROM test.items ORDER BY id","useLegacySql":false}"#,
    );
    assert_eq!(query.status, 200);
    let query: Value = serde_json::from_slice(&query.body).unwrap();
    assert_eq!(
        query["rows"],
        json!([{"f":[{"v":"-3"}]}, {"f":[{"v":"0"}]}])
    );

    let mut reader = BigQueryReadClient::new(fixture.channel.clone());
    let session = reader
        .create_read_session(read_session(&[]))
        .await
        .unwrap()
        .into_inner();
    let mut rows = reader
        .read_rows(ReadRowsRequest {
            read_stream: session.streams[0].name.clone(),
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    let Some(read_rows_response::Rows::AvroRows(rows)) =
        rows.message().await.unwrap().unwrap().rows
    else {
        panic!("missing Avro rows")
    };
    let mut expected = vec![5, 2, 0, 2, 0, 2];
    expected.extend(1.25_f64.to_le_bytes());
    expected.extend([2, 4, 0, 255, 0, 0, 0, 0, 0]);
    assert_eq!(rows.serialized_binary_rows, expected);
    fixture.shutdown().await;
    assert!(replies.message().await.unwrap().is_none());
}

#[tokio::test]
async fn storage_uses_existing_auth_modes_and_rejects_unsupported_requests() {
    let fixture = Fixture::start("strict", 0).await;
    let mut reader = BigQueryReadClient::new(fixture.channel.clone());
    let mut writer = BigQueryWriteClient::new(fixture.channel.clone());
    assert_eq!(
        reader
            .create_read_session(read_session(&[]))
            .await
            .unwrap_err()
            .code(),
        Code::Unauthenticated
    );
    assert_eq!(
        reader
            .read_rows(ReadRowsRequest::default())
            .await
            .unwrap_err()
            .code(),
        Code::Unauthenticated
    );
    assert_eq!(
        writer
            .append_rows(tokio_stream::empty())
            .await
            .unwrap_err()
            .code(),
        Code::Unauthenticated
    );

    fn authorized<T>(request: T) -> Request<T> {
        let mut request = Request::new(request);
        request
            .metadata_mut()
            .insert("authorization", "Bearer local-test-token".parse().unwrap());
        request
    }

    let mut sampled = read_session(&[]);
    sampled
        .read_session
        .as_mut()
        .unwrap()
        .read_options
        .as_mut()
        .unwrap()
        .sample_percentage = Some(10.0);
    assert_eq!(
        reader
            .create_read_session(authorized(sampled))
            .await
            .unwrap_err()
            .code(),
        Code::Unimplemented
    );
    assert_eq!(
        reader
            .create_read_session(authorized(read_session(&["unknown"])))
            .await
            .unwrap_err()
            .code(),
        Code::InvalidArgument
    );
    let session = reader
        .create_read_session(authorized(read_session(&[])))
        .await
        .unwrap()
        .into_inner();
    let mut empty = reader
        .read_rows(authorized(ReadRowsRequest {
            read_stream: session.streams[0].name.clone(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(empty.message().await.unwrap().is_none());

    let mut append = append_request(true, vec![]);
    append.write_stream = format!("{TABLE}/streams/custom");
    let mut replies = writer
        .append_rows(authorized(tokio_stream::iter([append])))
        .await
        .unwrap()
        .into_inner();
    assert!(matches!(replies.message().await.unwrap().unwrap().response,
        Some(AppendOutcome::Error(error)) if error.code == Code::NotFound as i32));
    assert!(replies.message().await.unwrap().is_none());
    fixture.shutdown().await;
}

#[tokio::test]
async fn append_rows_rolls_back_if_metadata_cannot_be_updated() {
    let fixture = Fixture::start("relaxed", 0).await;
    fixture.insert_all(vec![json!({"json": {"id": 1}})]);
    let metadata_temp = fixture
        .server
        .table_path("demo", "test", "items")
        .with_extension("json.tmp");
    std::fs::create_dir(&metadata_temp).unwrap();

    let mut writer = BigQueryWriteClient::new(fixture.channel.clone());
    let append = append_request(
        true,
        vec![TestRow {
            id: Some(2),
            ..Default::default()
        }
        .encode_to_vec()],
    );
    let mut replies = writer
        .append_rows(tokio_stream::iter([append.clone()]))
        .await
        .unwrap()
        .into_inner();
    assert!(matches!(replies.message().await.unwrap().unwrap().response,
        Some(AppendOutcome::Error(error)) if error.code == Code::Internal as i32));
    assert!(replies.message().await.unwrap().is_none());
    assert_eq!(fixture.list_rest_rows()["totalRows"], "1");

    std::fs::remove_dir(metadata_temp).unwrap();
    let mut replies = writer
        .append_rows(tokio_stream::iter([append]))
        .await
        .unwrap()
        .into_inner();
    assert!(matches!(
        replies.message().await.unwrap().unwrap().response,
        Some(AppendOutcome::AppendResult(_))
    ));
    assert!(replies.message().await.unwrap().is_none());
    assert_eq!(fixture.list_rest_rows()["totalRows"], "2");
    fixture.shutdown().await;
}

const NESTED_TABLE: &str = "projects/demo/datasets/test/tables/n";

fn with_token<T>(token: &str, request: T) -> Request<T> {
    let mut request = Request::new(request);
    request
        .metadata_mut()
        .insert("authorization", format!("Bearer {token}").parse().unwrap());
    request
}

fn session_for(
    table: &str,
    format: DataFormat,
    options: TableReadOptions,
) -> CreateReadSessionRequest {
    CreateReadSessionRequest {
        parent: "projects/demo".into(),
        read_session: Some(ReadSession {
            table: table.into(),
            data_format: format.into(),
            read_options: Some(options),
            ..Default::default()
        }),
        max_stream_count: 1,
        ..Default::default()
    }
}

/// Table `n` with nullable, required, repeated, RECORD and repeated RECORD
/// columns, and three rows exercising nulls and empty arrays.
fn create_nested_table(server: &Server) {
    let response = server.create_table(
        "demo",
        "test",
        br#"{"tableReference":{"tableId":"n"},"schema":{"fields":[
          {"name":"id","type":"INT64","mode":"REQUIRED"},
          {"name":"name","type":"STRING"},
          {"name":"ok","type":"BOOL"},
          {"name":"score","type":"FLOAT64"},
          {"name":"data","type":"BYTES"},
          {"name":"tags","type":"STRING","mode":"REPEATED"},
          {"name":"addr","type":"RECORD","fields":[
            {"name":"city","type":"STRING"},
            {"name":"zip","type":"INT64","mode":"REQUIRED"},
            {"name":"nums","type":"INT64","mode":"REPEATED"}]},
          {"name":"items","type":"RECORD","mode":"REPEATED","fields":[
            {"name":"k","type":"STRING"},{"name":"v","type":"FLOAT64"}]}
        ]}}"#,
    );
    assert_eq!(response.status, 200);
}

fn insert_nested_rows(server: &Server) {
    let response = server.insert_rows(
        "demo",
        "test",
        "n",
        &serde_json::to_vec(&json!({"rows": [
            {"json": {"id": 1, "name": "a", "ok": true, "score": 1.5, "data": "AP8=",
                      "tags": ["x", "y"], "addr": {"city": "T", "zip": "100", "nums": [1, 2]},
                      "items": [{"k": "a", "v": 1.0}, {"k": null, "v": 2.5}]}},
            {"json": {"id": 2, "tags": [], "addr": null}},
            {"json": {"id": "3", "name": "c", "ok": false, "addr": {"zip": 7}}}
        ]}))
        .unwrap(),
    );
    assert_eq!(response.status, 200);
}

async fn first_rows(
    client: &mut BigQueryReadClient<Channel>,
    session: &ReadSession,
) -> ReadRowsResponse {
    client
        .read_rows(ReadRowsRequest {
            read_stream: session.streams[0].name.clone(),
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner()
        .message()
        .await
        .unwrap()
        .unwrap()
}

async fn read_ids(
    client: &mut BigQueryReadClient<Channel>,
    request: CreateReadSessionRequest,
) -> Vec<u8> {
    let session = client
        .create_read_session(request)
        .await
        .unwrap()
        .into_inner();
    let mut stream = client
        .read_rows(ReadRowsRequest {
            read_stream: session.streams[0].name.clone(),
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    let mut out = Vec::new();
    while let Some(message) = stream.message().await.unwrap() {
        let Some(read_rows_response::Rows::AvroRows(rows)) = message.rows else {
            panic!("missing Avro rows")
        };
        out.extend(rows.serialized_binary_rows);
    }
    out
}

/// The Arrow fixtures were produced by this encoder and checked with
/// pyarrow 25.0.1 (`ipc.read_schema`, `ipc.read_record_batch` +
/// `validate(full=True)`, and `ipc.open_stream` over schema + batch); the Avro
/// rows were decoded with fastavro 1.12.2 against the session schema.
#[tokio::test]
async fn arrow_and_avro_reads_encode_nested_and_repeated_columns() {
    let fixture = Fixture::start("relaxed", 0).await;
    create_nested_table(&fixture.server);
    insert_nested_rows(&fixture.server);
    let mut client = BigQueryReadClient::new(fixture.channel.clone());

    let session = client
        .create_read_session(session_for(
            NESTED_TABLE,
            DataFormat::Arrow,
            Default::default(),
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(session.estimated_row_count, 3);
    let Some(Schema::ArrowSchema(schema)) = session.schema.clone() else {
        panic!("missing Arrow schema")
    };
    assert_eq!(
        schema.serialized_schema,
        include_bytes!("fixtures/storage_nested_arrow_schema.bin")
    );
    let first = first_rows(&mut client, &session).await;
    assert_eq!(first.row_count, 3);
    assert_eq!(
        first.schema,
        Some(read_rows_response::Schema::ArrowSchema(schema))
    );
    let Some(read_rows_response::Rows::ArrowRecordBatch(batch)) = first.rows else {
        panic!("missing Arrow record batch")
    };
    assert_eq!(batch.row_count, 3);
    assert_eq!(
        batch.serialized_record_batch,
        include_bytes!("fixtures/storage_nested_arrow_batch.bin")
    );

    let session = client
        .create_read_session(session_for(
            NESTED_TABLE,
            DataFormat::Avro,
            Default::default(),
        ))
        .await
        .unwrap()
        .into_inner();
    let Some(Schema::AvroSchema(schema)) = session.schema.clone() else {
        panic!("missing Avro schema")
    };
    let schema: Value = serde_json::from_str(&schema.schema).unwrap();
    assert_eq!(
        schema["fields"][5],
        json!({"name": "tags", "type": {"type": "array", "items": "string"}})
    );
    assert_eq!(
        schema["fields"][6]["type"][1]["fields"][2],
        json!({"name": "nums", "type": {"type": "array", "items": "long"}})
    );
    assert_eq!(schema["fields"][7]["type"]["items"]["type"], "record");
    let Some(read_rows_response::Rows::AvroRows(rows)) =
        first_rows(&mut client, &session).await.rows
    else {
        panic!("missing Avro rows")
    };
    assert_eq!(
        rows.serialized_binary_rows,
        include_bytes!("fixtures/storage_nested_avro_rows.bin")
    );

    // Selecting a nested column keeps its whole subtree.
    let arrow_only_addr = TableReadOptions {
        selected_fields: vec!["addr".into()],
        ..Default::default()
    };
    let session = client
        .create_read_session(session_for(
            NESTED_TABLE,
            DataFormat::Arrow,
            arrow_only_addr,
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(first_rows(&mut client, &session).await.row_count, 3);

    let mut compressed = session_for(NESTED_TABLE, DataFormat::Arrow, TableReadOptions {
        output_format_serialization_options: Some(
            read_session::table_read_options::OutputFormatSerializationOptions::ArrowSerializationOptions(
                ArrowSerializationOptions { buffer_compression: 1 },
            ),
        ),
        ..Default::default()
    });
    assert_eq!(
        client
            .create_read_session(compressed.clone())
            .await
            .unwrap_err()
            .code(),
        Code::Unimplemented
    );
    compressed.read_session.as_mut().unwrap().read_options = Some(TableReadOptions {
        output_format_serialization_options: Some(
            read_session::table_read_options::OutputFormatSerializationOptions::ArrowSerializationOptions(
                ArrowSerializationOptions { buffer_compression: 0 },
            ),
        ),
        ..Default::default()
    });
    assert!(client.create_read_session(compressed).await.is_ok());
    fixture.shutdown().await;
}

fn restricted(filter: &str) -> CreateReadSessionRequest {
    session_for(
        TABLE,
        DataFormat::Avro,
        TableReadOptions {
            selected_fields: vec!["id".into()],
            row_restriction: filter.into(),
            ..Default::default()
        },
    )
}

#[tokio::test]
async fn row_restriction_filters_rows_and_rejects_invalid_filters() {
    let fixture = Fixture::start("relaxed", 0).await;
    fixture.insert_all(vec![
        json!({"json": {"id": 1, "name": "a", "score": 0.5}}),
        json!({"json": {"id": 2, "name": "b", "score": 2.0}}),
        json!({"json": {"id": 3, "score": 9.0}}),
        json!({"json": {"id": 4, "name": "d", "active": true}}),
    ]);
    let mut client = BigQueryReadClient::new(fixture.channel.clone());
    // Avro `long` zig-zag: 2 → 4, 4 → 8.
    assert_eq!(
        read_ids(&mut client, restricted("id >= 2 AND name IS NOT NULL")).await,
        [4, 8]
    );
    assert_eq!(
        read_ids(&mut client, restricted("score > 1.0 OR active")).await,
        [4, 6, 8]
    );
    assert_eq!(
        read_ids(&mut client, restricted("UPPER(name) = 'A'")).await,
        [2]
    );
    assert!(read_ids(&mut client, restricted("id > 100"))
        .await
        .is_empty());
    for filter in ["id >", "missing = 1", "test.fn(id) = 1", "name"] {
        assert_eq!(
            client
                .create_read_session(restricted(filter))
                .await
                .unwrap_err()
                .code(),
            Code::InvalidArgument,
            "{filter}"
        );
    }
    let session = client
        .create_read_session(restricted("id = 3"))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(session.estimated_row_count, 1);
    fixture.shutdown().await;
}

fn now_nanos() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as i64
}

fn as_of(nanos: i64) -> CreateReadSessionRequest {
    let mut request = read_session(&["id"]);
    request.read_session.as_mut().unwrap().table_modifiers = Some(TableModifiers {
        snapshot_time: Some(prost_types::Timestamp {
            seconds: nanos.div_euclid(1_000_000_000),
            nanos: nanos.rem_euclid(1_000_000_000) as i32,
        }),
    });
    request
}

#[tokio::test]
async fn snapshot_time_reads_rows_as_of_their_insert_time() {
    let fixture = Fixture::start("relaxed", 0).await;
    fixture.insert_all(vec![json!({"json": {"id": 1}})]);
    tokio::time::sleep(Duration::from_millis(5)).await;
    let between = now_nanos();
    tokio::time::sleep(Duration::from_millis(5)).await;
    fixture.insert_all(vec![json!({"json": {"id": 2}})]);
    let mut client = BigQueryReadClient::new(fixture.channel.clone());

    assert_eq!(read_ids(&mut client, as_of(between)).await, [2]);
    assert_eq!(read_ids(&mut client, as_of(now_nanos())).await, [2, 4]);
    let session = client
        .create_read_session(as_of(between))
        .await
        .unwrap()
        .into_inner();
    assert!(session.table_modifiers.unwrap().snapshot_time.is_some());
    for (time, code) in [
        (now_nanos() + 60_000_000_000, Code::InvalidArgument),
        (now_nanos() - 60_000_000_000, Code::InvalidArgument),
        (
            now_nanos() - 8 * 24 * 3_600_000_000_000,
            Code::InvalidArgument,
        ),
    ] {
        assert_eq!(
            client
                .create_read_session(as_of(time))
                .await
                .unwrap_err()
                .code(),
            code
        );
    }

    // Copied rows keep their source insert times, so a copy into the table
    // ends the reliable local history: older snapshots are refused.
    let source = fixture.server.create_table(
        "demo",
        "test",
        br#"{"tableReference":{"tableId":"src"},"schema":{"fields":[
            {"name":"id","type":"INT64","mode":"REQUIRED"},{"name":"name","type":"STRING"},
            {"name":"active","type":"BOOL"},{"name":"score","type":"FLOAT64"},
            {"name":"data","type":"BYTES"}]}}"#,
    );
    assert_eq!(source.status, 200);
    let mut job = RestRequest::new(
        "POST",
        "/bigquery/v2/projects/demo/jobs",
        br#"{"configuration":{"copy":{"sourceTable":{"projectId":"demo","datasetId":"test","tableId":"src"},"destinationTable":{"projectId":"demo","datasetId":"test","tableId":"items"},"writeDisposition":"WRITE_APPEND"}}}"#,
    );
    job.authorization = "Bearer local-test-token".into();
    let response = handle(&fixture.server, &job);
    assert_eq!(response.status, 200, "{}", response.body_str());
    assert_eq!(
        client
            .create_read_session(as_of(between))
            .await
            .unwrap_err()
            .code(),
        Code::FailedPrecondition
    );
    tokio::time::sleep(Duration::from_millis(5)).await;
    assert_eq!(read_ids(&mut client, as_of(now_nanos())).await, [2, 4]);
    fixture.shutdown().await;
}

async fn create_stream(writer: &mut BigQueryWriteClient<Channel>, kind: StreamType) -> WriteStream {
    writer
        .create_write_stream(CreateWriteStreamRequest {
            parent: TABLE.into(),
            write_stream: Some(WriteStream {
                r#type: kind.into(),
                ..Default::default()
            }),
        })
        .await
        .unwrap()
        .into_inner()
}

fn row(id: i64) -> Vec<u8> {
    TestRow {
        id: Some(id),
        ..Default::default()
    }
    .encode_to_vec()
}

/// Sends one append on a fresh connection and returns the reply.
async fn append_once(
    writer: &mut BigQueryWriteClient<Channel>,
    stream: &str,
    offset: Option<i64>,
    rows: Vec<Vec<u8>>,
) -> AppendRowsResponse {
    let mut request = append_request(true, rows);
    request.write_stream = stream.into();
    request.offset = offset;
    writer
        .append_rows(tokio_stream::iter([request]))
        .await
        .unwrap()
        .into_inner()
        .message()
        .await
        .unwrap()
        .unwrap()
}

fn appended_at(reply: &AppendRowsResponse) -> Option<i64> {
    match &reply.response {
        Some(AppendOutcome::AppendResult(result)) => result.offset,
        other => panic!("append failed: {other:?}"),
    }
}

fn append_error(reply: &AppendRowsResponse) -> i32 {
    match &reply.response {
        Some(AppendOutcome::Error(error)) => error.code,
        other => panic!("append succeeded: {other:?}"),
    }
}

#[tokio::test]
async fn managed_write_streams_follow_committed_pending_and_buffered_semantics() {
    let fixture = Fixture::start("relaxed", 0).await;
    let mut writer = BigQueryWriteClient::new(fixture.channel.clone());
    let total = |fixture: &Fixture| fixture.list_rest_rows()["totalRows"].clone();

    // COMMITTED: visible at once, offsets give exactly-once appends.
    let committed = create_stream(&mut writer, StreamType::Committed).await;
    assert!(committed.name.starts_with(&format!("{TABLE}/streams/")));
    assert_eq!(committed.r#type, StreamType::Committed as i32);
    assert_eq!(committed.location, "US");
    let reply = append_once(&mut writer, &committed.name, Some(0), vec![row(1), row(2)]).await;
    assert_eq!(appended_at(&reply), Some(0));
    assert_eq!(total(&fixture), "2");
    let duplicate = append_once(&mut writer, &committed.name, Some(0), vec![row(1)]).await;
    assert_eq!(append_error(&duplicate), Code::AlreadyExists as i32);
    let gap = append_once(&mut writer, &committed.name, Some(5), vec![row(9)]).await;
    assert_eq!(append_error(&gap), Code::OutOfRange as i32);
    let reply = append_once(&mut writer, &committed.name, None, vec![row(3)]).await;
    assert_eq!(appended_at(&reply), Some(2));
    assert_eq!(total(&fixture), "3");

    // PENDING: invisible until finalized and batch-committed.
    let pending = create_stream(&mut writer, StreamType::Pending).await;
    append_once(&mut writer, &pending.name, Some(0), vec![row(10), row(11)]).await;
    assert_eq!(total(&fixture), "3");
    let commit = |streams: Vec<String>| BatchCommitWriteStreamsRequest {
        parent: TABLE.into(),
        write_streams: streams,
    };
    let early = writer
        .batch_commit_write_streams(commit(vec![pending.name.clone()]))
        .await
        .unwrap()
        .into_inner();
    assert!(early.commit_time.is_none());
    assert_eq!(
        early.stream_errors[0].code,
        storage_error::StorageErrorCode::InvalidStreamState as i32
    );
    let finalized = writer
        .finalize_write_stream(FinalizeWriteStreamRequest {
            name: pending.name.clone(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(finalized.row_count, 2);
    let late = append_once(&mut writer, &pending.name, None, vec![row(12)]).await;
    assert_eq!(append_error(&late), Code::InvalidArgument as i32);
    let mixed = writer
        .batch_commit_write_streams(commit(vec![
            pending.name.clone(),
            committed.name.clone(),
            format!("{TABLE}/streams/404"),
        ]))
        .await
        .unwrap()
        .into_inner();
    assert!(mixed.commit_time.is_none());
    assert_eq!(
        mixed
            .stream_errors
            .iter()
            .map(|e| e.code)
            .collect::<Vec<_>>(),
        [
            storage_error::StorageErrorCode::InvalidStreamType as i32,
            storage_error::StorageErrorCode::StreamNotFound as i32,
        ]
    );
    assert_eq!(total(&fixture), "3");
    let done = writer
        .batch_commit_write_streams(commit(vec![pending.name.clone()]))
        .await
        .unwrap()
        .into_inner();
    assert!(done.commit_time.is_some() && done.stream_errors.is_empty());
    assert_eq!(total(&fixture), "5");
    let again = writer
        .batch_commit_write_streams(commit(vec![pending.name.clone()]))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        again.stream_errors[0].code,
        storage_error::StorageErrorCode::StreamAlreadyCommitted as i32
    );
    let fetched = writer
        .get_write_stream(GetWriteStreamRequest {
            name: pending.name.clone(),
            view: 0,
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(fetched.r#type, StreamType::Pending as i32);
    assert!(fetched.commit_time.is_some());

    // BUFFERED: rows become visible up to the flushed offset.
    let buffered = create_stream(&mut writer, StreamType::Buffered).await;
    append_once(
        &mut writer,
        &buffered.name,
        Some(0),
        vec![row(20), row(21), row(22)],
    )
    .await;
    assert_eq!(total(&fixture), "5");
    let flush = |offset: Option<i64>| FlushRowsRequest {
        write_stream: buffered.name.clone(),
        offset,
    };
    assert_eq!(
        writer
            .flush_rows(flush(Some(1)))
            .await
            .unwrap()
            .into_inner()
            .offset,
        1
    );
    assert_eq!(total(&fixture), "7");
    assert_eq!(
        writer
            .flush_rows(flush(Some(0)))
            .await
            .unwrap()
            .into_inner()
            .offset,
        0
    );
    assert_eq!(total(&fixture), "7");
    for (offset, code) in [(Some(3), Code::OutOfRange), (None, Code::InvalidArgument)] {
        assert_eq!(
            writer.flush_rows(flush(offset)).await.unwrap_err().code(),
            code
        );
    }
    assert_eq!(
        writer
            .flush_rows(flush(Some(2)))
            .await
            .unwrap()
            .into_inner()
            .offset,
        2
    );
    assert_eq!(total(&fixture), "8");
    let not_buffered = FlushRowsRequest {
        write_stream: committed.name.clone(),
        offset: Some(0),
    };
    assert_eq!(
        writer.flush_rows(not_buffered).await.unwrap_err().code(),
        Code::InvalidArgument
    );

    // The default stream is COMMITTED and cannot be finalized.
    let default = writer
        .get_write_stream(GetWriteStreamRequest {
            name: WRITE_STREAM.into(),
            view: 0,
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(default.r#type, StreamType::Committed as i32);
    assert_eq!(
        writer
            .finalize_write_stream(FinalizeWriteStreamRequest {
                name: WRITE_STREAM.into()
            })
            .await
            .unwrap_err()
            .code(),
        Code::InvalidArgument
    );
    assert_eq!(
        writer
            .create_write_stream(CreateWriteStreamRequest {
                parent: TABLE.into(),
                write_stream: None
            })
            .await
            .unwrap_err()
            .code(),
        Code::InvalidArgument
    );
    assert_eq!(
        writer
            .create_write_stream(CreateWriteStreamRequest {
                parent: "projects/demo/datasets/test/tables/missing".into(),
                write_stream: Some(WriteStream {
                    r#type: StreamType::Committed.into(),
                    ..Default::default()
                }),
            })
            .await
            .unwrap_err()
            .code(),
        Code::NotFound
    );
    assert_eq!(
        writer
            .get_write_stream(GetWriteStreamRequest {
                name: format!("{TABLE}/streams/404"),
                view: 0
            })
            .await
            .unwrap_err()
            .code(),
        Code::NotFound
    );
    fixture.shutdown().await;
}

#[derive(Clone, PartialEq, Message)]
struct Addr {
    #[prost(string, optional, tag = "1")]
    city: Option<String>,
    #[prost(int64, optional, tag = "2")]
    zip: Option<i64>,
    #[prost(int64, repeated, tag = "3")]
    nums: Vec<i64>,
}

#[derive(Clone, PartialEq, Message)]
struct Item {
    #[prost(string, optional, tag = "1")]
    k: Option<String>,
    #[prost(double, optional, tag = "2")]
    v: Option<f64>,
}

#[derive(Clone, PartialEq, Message)]
struct NestedRow {
    #[prost(int64, optional, tag = "1")]
    id: Option<i64>,
    #[prost(string, repeated, tag = "6")]
    tags: Vec<String>,
    #[prost(message, optional, tag = "7")]
    addr: Option<Addr>,
    #[prost(message, repeated, tag = "8")]
    items: Vec<Item>,
}

fn proto_field(
    name: &str,
    number: i32,
    label: Label,
    field_type: Type,
    type_name: Option<&str>,
) -> FieldDescriptorProto {
    FieldDescriptorProto {
        name: Some(name.into()),
        number: Some(number),
        label: Some(label as i32),
        r#type: Some(field_type as i32),
        type_name: type_name.map(Into::into),
        ..Default::default()
    }
}

fn nested_writer_schema() -> ProtoSchema {
    ProtoSchema {
        proto_descriptor: Some(DescriptorProto {
            name: Some("NestedRow".into()),
            field: vec![
                proto_field("id", 1, Label::Optional, Type::Int64, None),
                proto_field("tags", 6, Label::Repeated, Type::String, None),
                proto_field(
                    "addr",
                    7,
                    Label::Optional,
                    Type::Message,
                    Some(".NestedRow.Addr"),
                ),
                proto_field("items", 8, Label::Repeated, Type::Message, Some("Item")),
            ],
            nested_type: vec![
                DescriptorProto {
                    name: Some("Addr".into()),
                    field: vec![
                        proto_field("city", 1, Label::Optional, Type::String, None),
                        proto_field("zip", 2, Label::Optional, Type::Int64, None),
                        proto_field("nums", 3, Label::Repeated, Type::Int64, None),
                    ],
                    ..Default::default()
                },
                DescriptorProto {
                    name: Some("Item".into()),
                    field: vec![
                        proto_field("k", 1, Label::Optional, Type::String, None),
                        proto_field("v", 2, Label::Optional, Type::Double, None),
                    ],
                    ..Default::default()
                },
            ],
            ..Default::default()
        }),
    }
}

#[tokio::test]
async fn proto_appends_decode_nested_messages_and_repeated_fields() {
    let fixture = Fixture::start("relaxed", 0).await;
    create_nested_table(&fixture.server);
    let mut writer = BigQueryWriteClient::new(fixture.channel.clone());
    let rows = vec![
        NestedRow {
            id: Some(1),
            tags: vec!["x".into(), "y".into()],
            addr: Some(Addr {
                city: Some("T".into()),
                zip: Some(100),
                nums: vec![1, 2],
            }),
            items: vec![
                Item {
                    k: Some("a".into()),
                    v: Some(1.0),
                },
                Item {
                    k: None,
                    v: Some(2.5),
                },
            ],
        }
        .encode_to_vec(),
        NestedRow {
            id: Some(2),
            ..Default::default()
        }
        .encode_to_vec(),
        // Missing the REQUIRED nested `zip`.
        NestedRow {
            id: Some(3),
            addr: Some(Addr::default()),
            ..Default::default()
        }
        .encode_to_vec(),
    ];
    let request = |rows: Vec<Vec<u8>>| AppendRowsRequest {
        write_stream: format!("{NESTED_TABLE}/streams/_default"),
        rows: Some(Rows::ProtoRows(ProtoData {
            writer_schema: Some(nested_writer_schema()),
            rows: Some(ProtoRows {
                serialized_rows: rows,
            }),
        })),
        ..Default::default()
    };
    let reply = writer
        .append_rows(tokio_stream::iter([request(rows.clone())]))
        .await
        .unwrap()
        .into_inner()
        .message()
        .await
        .unwrap()
        .unwrap();
    assert_eq!(append_error(&reply), Code::InvalidArgument as i32);
    assert_eq!(
        reply.row_errors.iter().map(|e| e.index).collect::<Vec<_>>(),
        [2]
    );
    let reply = writer
        .append_rows(tokio_stream::iter([request(rows[..2].to_vec())]))
        .await
        .unwrap()
        .into_inner()
        .message()
        .await
        .unwrap()
        .unwrap();
    assert_eq!(appended_at(&reply), None);
    let stored: Vec<Value> = fixture
        .server
        .read_rows("demo", "test", "n")
        .unwrap()
        .into_iter()
        .map(|row| serde_json::to_value(row.json).unwrap())
        .collect();
    assert_eq!(
        stored,
        [
            json!({"id": 1, "tags": ["x", "y"], "addr": {"city": "T", "zip": 100, "nums": [1, 2]},
                   "items": [{"k": "a", "v": 1.0}, {"v": 2.5}]}),
            json!({"id": 2}),
        ]
    );

    // A message field against a scalar column is a schema mismatch.
    let mut mismatched = nested_writer_schema();
    let descriptor = mismatched.proto_descriptor.as_mut().unwrap();
    descriptor.field[1] = proto_field("tags", 6, Label::Optional, Type::String, None);
    let mut bad = request(vec![]);
    if let Some(Rows::ProtoRows(data)) = bad.rows.as_mut() {
        data.writer_schema = Some(mismatched);
    }
    let reply = writer
        .append_rows(tokio_stream::iter([bad]))
        .await
        .unwrap()
        .into_inner()
        .message()
        .await
        .unwrap()
        .unwrap();
    assert_eq!(append_error(&reply), Code::InvalidArgument as i32);
    fixture.shutdown().await;
}

fn rest(server: &Server, method: &str, target: &str, body: &str) -> u16 {
    let mut request = RestRequest::new(method, target, body.as_bytes());
    request.authorization = "Bearer local-test-token".into();
    let response = handle(server, &request);
    assert!(response.status < 300, "{}", response.body_str());
    response.status
}

#[tokio::test]
async fn strict_storage_api_authorizes_principals_like_rest() {
    let fixture = Fixture::start_with(
        "strict",
        0,
        &[
            ("alice-token", "user:alice@example.com"),
            ("bob-token", "serviceAccount:bob@p.iam.gserviceaccount.com"),
        ],
    )
    .await;
    fixture.insert_all(vec![json!({"json": {"id": 1}})]);
    let mut reader = BigQueryReadClient::new(fixture.channel.clone());
    let mut writer = BigQueryWriteClient::new(fixture.channel.clone());
    let code =
        |result: Result<tonic::Response<ReadSession>, tonic::Status>| result.unwrap_err().code();

    assert_eq!(
        code(reader.create_read_session(read_session(&[])).await),
        Code::Unauthenticated
    );
    assert_eq!(
        code(
            reader
                .create_read_session(with_token("nobody", read_session(&[])))
                .await
        ),
        Code::Unauthenticated
    );
    assert_eq!(
        code(
            reader
                .create_read_session(with_token("alice-token", read_session(&[])))
                .await
        ),
        Code::PermissionDenied
    );
    // Table data access alone is not enough: sessions need readsessions.create.
    rest(
        &fixture.server,
        "PATCH",
        "/bigquery/v2/projects/demo/datasets/test",
        r#"{"access":[{"role":"READER","userByEmail":"alice@example.com"}]}"#,
    );
    assert_eq!(
        code(
            reader
                .create_read_session(with_token("alice-token", read_session(&[])))
                .await
        ),
        Code::PermissionDenied
    );
    rest(
        &fixture.server,
        "POST",
        "/v1/projects/demo:setIamPolicy",
        r#"{"policy":{"bindings":[{"role":"roles/bigquery.readSessionUser","members":["user:alice@example.com","serviceAccount:bob@p.iam.gserviceaccount.com"]}]}}"#,
    );
    let session = reader
        .create_read_session(with_token("alice-token", read_session(&[])))
        .await
        .unwrap()
        .into_inner();
    let read = |token: &str| {
        with_token(
            token,
            ReadRowsRequest {
                read_stream: session.streams[0].name.clone(),
                ..Default::default()
            },
        )
    };
    let mut rows = reader
        .read_rows(read("alice-token"))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(rows.message().await.unwrap().unwrap().row_count, 1);
    // bob may read sessions but has no table access.
    assert_eq!(
        code(
            reader
                .create_read_session(with_token("bob-token", read_session(&[])))
                .await
        ),
        Code::PermissionDenied
    );
    assert_eq!(
        reader.read_rows(read("")).await.unwrap_err().code(),
        Code::Unauthenticated
    );

    let append = |token: &str| {
        with_token(
            token,
            tokio_stream::iter([append_request(true, vec![row(2)])]),
        )
    };
    let reply = writer
        .append_rows(append("alice-token"))
        .await
        .unwrap()
        .into_inner()
        .message()
        .await
        .unwrap()
        .unwrap();
    assert_eq!(append_error(&reply), Code::PermissionDenied as i32);
    let stream_request = CreateWriteStreamRequest {
        parent: TABLE.into(),
        write_stream: Some(WriteStream {
            r#type: StreamType::Committed.into(),
            ..Default::default()
        }),
    };
    assert_eq!(
        writer
            .create_write_stream(with_token("bob-token", stream_request.clone()))
            .await
            .unwrap_err()
            .code(),
        Code::PermissionDenied
    );
    rest(
        &fixture.server,
        "POST",
        "/bigquery/v2/projects/demo/datasets/test/tables/items:setIamPolicy",
        r#"{"policy":{"bindings":[{"role":"roles/bigquery.dataEditor","members":["serviceAccount:bob@p.iam.gserviceaccount.com"]}]}}"#,
    );
    let reply = writer
        .append_rows(append("bob-token"))
        .await
        .unwrap()
        .into_inner()
        .message()
        .await
        .unwrap()
        .unwrap();
    assert_eq!(appended_at(&reply), None);
    let stream = writer
        .create_write_stream(with_token("bob-token", stream_request))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        writer
            .finalize_write_stream(with_token(
                "alice-token",
                FinalizeWriteStreamRequest {
                    name: stream.name.clone()
                }
            ))
            .await
            .unwrap_err()
            .code(),
        Code::PermissionDenied
    );
    assert_eq!(fixture.list_rest_rows()["totalRows"], "2");
    // The owner token keeps full access.
    assert!(reader
        .create_read_session(with_token("local-test-token", read_session(&[])))
        .await
        .is_ok());
    fixture.shutdown().await;
}
