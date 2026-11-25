use std::sync::{Arc, Mutex};
use std::time::Duration;

use ciborium::value::Value as Cbor;
use devcloud_dynamodb::dax_data;
use devcloud_dynamodb::server::{Config, Server};
use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;

const TABLE: &str = "items";
const GET: i64 = 263_244_906;
const PUT: i64 = -2_106_490_455;
const DELETE: i64 = 1_013_539_361;
const LIST: i64 = 670_678_385;
const LIST_ID: i64 = -1_230_579_644;
const KEY_SCHEMA: i64 = -742_646_399;
const ENDPOINTS: i64 = 455_855_874;

struct Fixture {
    addr: std::net::SocketAddr,
    server: Arc<Mutex<Server>>,
    task: JoinHandle<std::io::Result<()>>,
}

impl Fixture {
    async fn start() -> Self {
        let mut server = Server::new(Config {
            auth_mode: "relaxed".to_owned(),
            ..Default::default()
        });
        server.set_fixed_now(1000);
        let server = Arc::new(Mutex::new(server));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let task = tokio::spawn(dax_data::serve(listener, server.clone()));
        let fixture = Self { addr, server, task };
        fixture.create_table();
        fixture
    }

    fn create_table(&self) {
        self.ddb(
            "CreateTable",
            json!({
                "TableName": TABLE,
                "KeySchema": [{"AttributeName":"id","KeyType":"HASH"}],
                "AttributeDefinitions": [{"AttributeName":"id","AttributeType":"S"}],
                "BillingMode": "PAY_PER_REQUEST"
            }),
        );
    }

    fn ddb(&self, op: &str, body: Value) -> Value {
        let response = self.server.lock().unwrap().dispatch(
            &format!("DynamoDB_20120810.{op}"),
            &serde_json::to_vec(&body).unwrap(),
            "relaxed",
            None,
            1000,
        );
        assert_eq!(response.status, 200, "{op}");
        serde_json::from_slice(&response.body).unwrap()
    }

    fn ddb_put(&self, key: &str, value: &str) {
        self.ddb(
            "PutItem",
            json!({"TableName": TABLE, "Item": {
                "id": {"S": key}, "value": {"S": value}
            }}),
        );
    }

    async fn connect(&self) -> Client {
        let mut stream = TcpStream::connect(self.addr).await.unwrap();
        stream.set_nodelay(true).unwrap();
        // Tube greeting followed by the SDK's one-way AuthorizeConnection.
        let mut init = b"\x67J7yne5G\x00\xf6\xa1\x69UserAgent\x64test\x00".to_vec();
        init.extend(message(
            1_489_122_155,
            [
                "local".into(),
                "placeholder".into(),
                Cbor::Bytes(Vec::new()),
                Cbor::Null,
                "test".into(),
            ],
        ));
        for chunk in init.chunks(3) {
            stream.write_all(chunk).await.unwrap();
        }
        Client(stream)
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        self.task.abort();
    }
}

struct Client(TcpStream);

impl Client {
    async fn request(&mut self, method: i64, args: impl IntoIterator<Item = Cbor>) -> Cbor {
        self.0.write_all(&message(method, args)).await.unwrap();
        let mut buffer = Vec::new();
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                let mut chunk = [0; 4096];
                let read = self.0.read(&mut chunk).await.unwrap();
                assert!(read > 0);
                buffer.extend(&chunk[..read]);
                let mut input = buffer.as_slice();
                if let Ok(status) = ciborium::from_reader::<Cbor, _>(&mut input) {
                    if status == Cbor::Array(Vec::new()) {
                        if let Ok(value) = ciborium::from_reader::<Cbor, _>(&mut input) {
                            assert!(input.is_empty());
                            return value;
                        }
                    } else if ciborium::from_reader::<Cbor, _>(&mut input).is_ok() {
                        if let Ok(details) = ciborium::from_reader::<Cbor, _>(&mut input) {
                            assert!(input.is_empty());
                            return details;
                        }
                    }
                }
            }
        })
        .await
        .unwrap()
    }

    async fn get(&mut self, key: &str, consistent: bool) -> Option<String> {
        let result = self
            .request(
                GET,
                [
                    table(TABLE),
                    table(key),
                    Cbor::Map(vec![(2.into(), consistent.into())]),
                ],
            )
            .await;
        assert!(result.is_map(), "{result:?}");
        if let Some(mut data) = item_data(&result, 0) {
            let id = read(&mut data);
            let value = read(&mut data).into_text().unwrap();
            assert!(data.is_empty());
            assert_eq!(
                self.request(LIST, [id]).await,
                Cbor::Array(vec!["value".into()])
            );
            return Some(value);
        }
        None
    }
}

fn message(method: i64, args: impl IntoIterator<Item = Cbor>) -> Vec<u8> {
    encode([1.into(), method.into()].into_iter().chain(args))
}

fn encode(values: impl IntoIterator<Item = Cbor>) -> Vec<u8> {
    let mut bytes = Vec::new();
    for value in values {
        ciborium::into_writer(&value, &mut bytes).unwrap();
    }
    bytes
}

fn table(text: &str) -> Cbor {
    Cbor::Bytes(text.as_bytes().to_vec())
}

fn read(input: &mut &[u8]) -> Cbor {
    ciborium::from_reader(input).unwrap()
}

fn item_data(result: &Cbor, field: i32) -> Option<&[u8]> {
    result
        .as_map()?
        .iter()
        .find(|(key, _)| *key == Cbor::from(field))?
        .1
        .as_bytes()
        .map(Vec::as_slice)
}

#[tokio::test]
async fn dax_cache_ttl_negative_entries_and_strong_reads_share_dynamodb_storage() {
    let fixture = Fixture::start().await;
    let mut dax = fixture.connect().await;
    let endpoints = dax.request(ENDPOINTS, []).await;
    let endpoint = endpoints.as_array().unwrap()[0].as_array().unwrap();
    assert_eq!(endpoint[2], Cbor::Bytes(vec![127, 0, 0, 1]));
    assert_eq!(endpoint[3], fixture.addr.port().into());
    assert_eq!(
        dax.request(KEY_SCHEMA, [table(TABLE)]).await,
        Cbor::Map(vec![("id".into(), "S".into())])
    );

    assert_eq!(dax.get("a", false).await, None);
    fixture.ddb_put("a", "first");
    assert_eq!(dax.get("a", true).await.as_deref(), Some("first"));
    assert_eq!(dax.get("a", false).await, None);
    fixture.server.lock().unwrap().set_fixed_now(1299);
    assert_eq!(dax.get("a", false).await, None);
    fixture.server.lock().unwrap().set_fixed_now(1300);
    assert_eq!(dax.get("a", false).await.as_deref(), Some("first"));

    fixture.ddb_put("a", "second");
    let mut another = fixture.connect().await;
    assert_eq!(another.get("a", true).await.as_deref(), Some("second"));
    assert_eq!(another.get("a", false).await.as_deref(), Some("first"));
    fixture.ddb("DeleteTable", json!({"TableName": TABLE}));
    fixture.create_table();
    assert_eq!(another.get("a", false).await, None);
}

#[tokio::test]
async fn dax_write_through_and_invalid_options_keep_cache_and_store_consistent() {
    let fixture = Fixture::start().await;
    let mut dax = fixture.connect().await;
    let id = dax
        .request(LIST_ID, [Cbor::Array(vec!["value".into()])])
        .await;
    assert_eq!(id, 2.into());
    assert_eq!(
        dax.request(LIST_ID, [Cbor::Array(Vec::new())]).await,
        1.into()
    );
    assert_eq!(dax.request(LIST, [1.into()]).await, Cbor::Array(Vec::new()));
    assert_eq!(dax.get("a", false).await, None);
    let values = Cbor::Bytes(encode([id.clone(), "new".into()]));
    let put = dax
        .request(PUT, [table(TABLE), table("a"), values, Cbor::Null])
        .await;
    assert_eq!(put, Cbor::Map(vec![]));
    assert_eq!(dax.get("a", false).await.as_deref(), Some("new"));
    assert_eq!(
        fixture.ddb(
            "GetItem",
            json!({"TableName": TABLE, "Key":{"id":{"S":"a"}}})
        )["Item"]["value"]["S"],
        "new"
    );

    let error = dax
        .request(
            PUT,
            [
                table(TABLE),
                table("a"),
                Cbor::Bytes(encode([id.clone(), "rejected".into()])),
                Cbor::Map(vec![(4.into(), Cbor::Bytes(vec![1]))]),
            ],
        )
        .await;
    assert_eq!(error.as_array().unwrap()[1], "ValidationException".into());
    assert_eq!(dax.get("a", true).await.as_deref(), Some("new"));
    assert_eq!(dax.get("a", false).await.as_deref(), Some("new"));

    fixture.ddb_put("a", "direct");
    let deleted = dax
        .request(
            DELETE,
            [
                table(TABLE),
                table("a"),
                Cbor::Map(vec![(7.into(), 2.into())]),
            ],
        )
        .await;
    let mut data = item_data(&deleted, 2).unwrap();
    assert_eq!(read(&mut data), id);
    assert_eq!(read(&mut data), "direct".into());
    assert!(data.is_empty());
    assert_eq!(dax.get("a", false).await, None);
    assert_eq!(dax.get("a", true).await, None);

    let error = dax.request(LIST, [i64::MIN.into()]).await;
    assert_eq!(error.as_array().unwrap()[1], "ValidationException".into());
    let error = dax
        .request(GET, [table(TABLE), table(""), Cbor::Null])
        .await;
    assert_eq!(error.as_array().unwrap()[1], "ValidationException".into());
    let error = dax.request(999, []).await;
    assert_eq!(
        error.as_array().unwrap()[1],
        "UnknownOperationException".into()
    );
    assert_eq!(dax.0.read(&mut [0]).await.unwrap(), 0);
}

#[tokio::test]
async fn dax_composite_keys_and_attribute_values_use_sdk_wire_types() {
    let fixture = Fixture::start().await;
    fixture.ddb("CreateTable", json!({
        "TableName": "composite",
        "KeySchema": [{"AttributeName":"z","KeyType":"HASH"}, {"AttributeName":"a","KeyType":"RANGE"}],
        "AttributeDefinitions": [{"AttributeName":"z","AttributeType":"B"}, {"AttributeName":"a","AttributeType":"N"}],
        "BillingMode": "PAY_PER_REQUEST"
    }));
    let mut dax = fixture.connect().await;
    assert_eq!(
        dax.request(KEY_SCHEMA, [table("composite")]).await,
        Cbor::Map(vec![("z".into(), "B".into()), ("a".into(), "N".into())])
    );

    fixture.ddb(
        "PutItem",
        json!({
            "TableName": "composite", "Item": {
                "z": {"B":"AP8="}, "a": {"N":"-12.345600"}, "value": {"BOOL":false}
            }
        }),
    );
    let mut key = encode([Cbor::Bytes(vec![0, 255])]);
    // LexDecimal bytes emitted by amazon-dax-client 1.2.10 for -12.3456.
    key.extend(hex::decode("3dde22bff4").unwrap());
    let key = Cbor::Bytes(key);
    let got = dax
        .request(GET, [table("composite"), key.clone(), Cbor::Null])
        .await;
    let mut data = item_data(&got, 0).unwrap();
    let id = read(&mut data);
    assert_eq!(read(&mut data), Cbor::Bool(false));
    assert!(data.is_empty());

    let number = Cbor::Tag(
        4,
        Box::new(Cbor::Array(vec![
            (-3).into(),
            12345678901234567890123456789012345678i128.into(),
        ])),
    );
    let nested = Cbor::Map(vec![(
        "list".into(),
        Cbor::Array(vec![
            Cbor::Null,
            number.clone(),
            Cbor::Tag(3323, Box::new(Cbor::Array(vec![Cbor::Bytes(vec![0, 255])]))),
        ]),
    )]);
    let put = dax
        .request(
            PUT,
            [
                table("composite"),
                key.clone(),
                Cbor::Bytes(encode([id.clone(), nested.clone()])),
                Cbor::Map(vec![(7.into(), 2.into())]),
            ],
        )
        .await;
    let mut old = item_data(&put, 2).unwrap();
    assert_eq!(read(&mut old), id);
    assert_eq!(read(&mut old), false.into());
    let got = dax
        .request(GET, [table("composite"), key, Cbor::Null])
        .await;
    let mut data = item_data(&got, 0).unwrap();
    assert_eq!(read(&mut data), id);
    assert_eq!(read(&mut data), nested);
    assert_eq!(
        fixture.ddb(
            "GetItem",
            json!({"TableName":"composite", "Key":{
                "z":{"B":"AP8="},"a":{"N":"-12.345600"}
            }})
        )["Item"]["value"]["M"]["list"]["L"][1]["N"],
        "12345678901234567890123456789012345.678"
    );
}

#[tokio::test]
async fn dax_lru_keeps_recently_read_items() {
    let fixture = Fixture::start().await;
    let mut dax = fixture.connect().await;
    for key in 0..256 {
        assert_eq!(dax.get(&key.to_string(), false).await, None);
    }
    assert_eq!(dax.get("0", false).await, None);
    assert_eq!(dax.get("256", false).await, None);
    fixture.ddb_put("0", "kept");
    fixture.ddb_put("1", "evicted");
    assert_eq!(dax.get("0", false).await, None);
    assert_eq!(dax.get("1", false).await.as_deref(), Some("evicted"));
}

#[tokio::test]
async fn dax_listener_refuses_signed_modes() {
    for mode in ["strict", "signed-relaxed"] {
        let server = Arc::new(Mutex::new(Server::new(Config {
            auth_mode: mode.to_owned(),
            ..Default::default()
        })));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let err = dax_data::serve(listener, server).await.unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::PermissionDenied);
    }
}
