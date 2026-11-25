use std::sync::Arc;
use std::time::Duration;

use devcloud_bigquery::server::{Config, Server};
use devcloud_bigquery::storage_api::proto::append_rows_request::{ProtoData, Rows};
use devcloud_bigquery::storage_api::proto::append_rows_response::Response as AppendOutcome;
use devcloud_bigquery::storage_api::proto::big_query_read_client::BigQueryReadClient;
use devcloud_bigquery::storage_api::proto::big_query_write_client::BigQueryWriteClient;
use devcloud_bigquery::storage_api::proto::read_session::{Schema, TableReadOptions};
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
        let storage = tempfile::tempdir().unwrap();
        let server = Arc::new(Server::new(Config {
            auth_mode: mode.into(),
            bearer_token: "local-test-token".into(),
            storage_path: storage.path().to_str().unwrap().into(),
            max_rows_per_table,
            ..Default::default()
        }));
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

    let mut arrow = read_session(&[]);
    arrow.read_session.as_mut().unwrap().data_format = DataFormat::Arrow.into();
    assert_eq!(
        reader
            .create_read_session(authorized(arrow))
            .await
            .unwrap_err()
            .code(),
        Code::Unimplemented
    );
    let mut restricted = read_session(&[]);
    restricted
        .read_session
        .as_mut()
        .unwrap()
        .read_options
        .as_mut()
        .unwrap()
        .row_restriction = "id > 1".into();
    assert_eq!(
        reader
            .create_read_session(authorized(restricted))
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
        Some(AppendOutcome::Error(error)) if error.code == Code::Unimplemented as i32));
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
