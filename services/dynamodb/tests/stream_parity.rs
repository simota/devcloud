//! Differential-parity tests for DynamoDB Streams (record generation + the
//! ListStreams / DescribeStream / GetShardIterator / GetRecords operations)
//! against golden oracles captured from the legacy service.

use std::collections::BTreeMap;

use devcloud_dynamodb::model::{AttributeDefinition, Item, KeySchemaElement, StreamSpecification};
use devcloud_dynamodb::requests::{
    BatchWriteItemRequest, CreateTableRequest, DeleteItemRequest, DeleteRequest,
    DescribeStreamRequest, GetRecordsRequest, GetShardIteratorRequest, ListStreamsRequest,
    PutItemRequest, PutRequest, TimeToLiveSpecification, TransactDelete, TransactUpdate,
    TransactWriteItem, TransactWriteItemsRequest, UpdateItemRequest, UpdateTimeToLiveRequest,
    WriteRequest,
};
use devcloud_dynamodb::server::{Config, Server};
use serde_json::{json, Value};

const ORACLE_SECS: i64 = 1_780_098_203;
const ORACLE_MILLIS: i64 = 1_780_098_203_314;
const ARN: &str = "arn:aws:dynamodb:us-east-1:000000000000:table/T/stream/2026-05-29T23:43:23.314";

fn item(pairs: &[(&str, Value)]) -> Item {
    let mut m = Item::new();
    for (k, v) in pairs {
        m.insert((*k).to_string(), v.clone());
    }
    m
}

/// Stream-enabled table `T` (NEW_AND_OLD_IMAGES), pinned to the oracle clock,
/// with INSERT/MODIFY/REMOVE already applied.
fn seeded(dir: &std::path::Path) -> Server {
    let mut s = Server::new(Config {
        region: "us-east-1".to_string(),
        auth_mode: "relaxed".to_string(),
        storage_path: dir.to_string_lossy().to_string(),
        ..Default::default()
    });
    s.set_fixed_now(ORACLE_SECS);
    s.set_fixed_now_millis(ORACLE_MILLIS);
    s.create_table(&CreateTableRequest {
        table_name: "T".to_string(),
        attribute_definitions: vec![AttributeDefinition {
            attribute_name: "pk".to_string(),
            attribute_type: "S".to_string(),
        }],
        key_schema: vec![KeySchemaElement {
            attribute_name: "pk".to_string(),
            key_type: "HASH".to_string(),
        }],
        billing_mode: "PAY_PER_REQUEST".to_string(),
        stream_specification: StreamSpecification {
            stream_enabled: true,
            stream_view_type: "NEW_AND_OLD_IMAGES".to_string(),
        },
        ..Default::default()
    })
    .expect("create");
    s.put_item(&PutItemRequest {
        table_name: "T".to_string(),
        item: item(&[("pk", json!({"S": "a"})), ("v", json!({"N": "1"}))]),
        ..Default::default()
    })
    .expect("put");
    s.update_item(&UpdateItemRequest {
        table_name: "T".to_string(),
        key: item(&[("pk", json!({"S": "a"}))]),
        update_expression: "SET v = :v".to_string(),
        expression_attribute_values: {
            let mut m = BTreeMap::new();
            m.insert(":v".to_string(), json!({"N": "2"}));
            m
        },
        ..Default::default()
    })
    .expect("update");
    s.delete_item(&DeleteItemRequest {
        table_name: "T".to_string(),
        key: item(&[("pk", json!({"S": "a"}))]),
        ..Default::default()
    })
    .expect("delete");
    s
}

fn matches(got: &[u8], fixture: &[u8], label: &str) {
    assert_eq!(
        String::from_utf8_lossy(got),
        String::from_utf8_lossy(fixture),
        "{label}"
    );
}

#[test]
fn create_stream_table_matches_oracle() {
    let dir = tempdir();
    let mut s = Server::new(Config {
        region: "us-east-1".to_string(),
        storage_path: dir.to_string_lossy().to_string(),
        ..Default::default()
    });
    s.set_fixed_now(ORACLE_SECS);
    s.set_fixed_now_millis(ORACLE_MILLIS);
    let body = s
        .create_table(&CreateTableRequest {
            table_name: "T".to_string(),
            attribute_definitions: vec![AttributeDefinition {
                attribute_name: "pk".to_string(),
                attribute_type: "S".to_string(),
            }],
            key_schema: vec![KeySchemaElement {
                attribute_name: "pk".to_string(),
                key_type: "HASH".to_string(),
            }],
            billing_mode: "PAY_PER_REQUEST".to_string(),
            stream_specification: StreamSpecification {
                stream_enabled: true,
                stream_view_type: "NEW_AND_OLD_IMAGES".to_string(),
            },
            ..Default::default()
        })
        .expect("create");
    matches(
        &body,
        include_bytes!("fixtures/stream_create.json"),
        "create",
    );
}

#[test]
fn records_persisted_to_state_match_oracle() {
    let dir = tempdir();
    let _s = seeded(&dir);
    let on_disk = std::fs::read(dir.join("state.json")).expect("read state");
    matches(
        &on_disk,
        include_bytes!("fixtures/stream_state.json"),
        "stream_state",
    );
}

#[test]
fn list_streams_matches_oracle() {
    let dir = tempdir();
    let s = seeded(&dir);
    let body = s
        .list_streams(&ListStreamsRequest {
            table_name: "T".to_string(),
            ..Default::default()
        })
        .expect("ls");
    matches(&body, include_bytes!("fixtures/ls.json"), "ls");
}

#[test]
fn describe_stream_matches_oracle() {
    let dir = tempdir();
    let s = seeded(&dir);
    let body = s
        .describe_stream(&DescribeStreamRequest {
            stream_arn: ARN.to_string(),
            ..Default::default()
        })
        .expect("ds");
    matches(&body, include_bytes!("fixtures/ds.json"), "ds");
}

#[test]
fn get_shard_iterator_trim_horizon_matches_oracle() {
    let dir = tempdir();
    let s = seeded(&dir);
    let body = s
        .get_shard_iterator(&GetShardIteratorRequest {
            stream_arn: ARN.to_string(),
            shard_id: "shardId-000000000000".to_string(),
            shard_iterator_type: "TRIM_HORIZON".to_string(),
            ..Default::default()
        })
        .expect("gsi");
    matches(&body, include_bytes!("fixtures/gsi.json"), "gsi");
}

#[test]
fn get_shard_iterator_latest_matches_oracle() {
    let dir = tempdir();
    let s = seeded(&dir);
    let body = s
        .get_shard_iterator(&GetShardIteratorRequest {
            stream_arn: ARN.to_string(),
            shard_id: "shardId-000000000000".to_string(),
            shard_iterator_type: "LATEST".to_string(),
            ..Default::default()
        })
        .expect("gsi latest");
    matches(
        &body,
        include_bytes!("fixtures/gsi_latest.json"),
        "gsi_latest",
    );
}

#[test]
fn get_records_matches_oracle() {
    let dir = tempdir();
    let s = seeded(&dir);
    // TRIM_HORIZON iterator (position 0).
    let it_body = s
        .get_shard_iterator(&GetShardIteratorRequest {
            stream_arn: ARN.to_string(),
            shard_id: "shardId-000000000000".to_string(),
            shard_iterator_type: "TRIM_HORIZON".to_string(),
            ..Default::default()
        })
        .expect("gsi");
    let it: Value = serde_json::from_slice(&it_body).unwrap();
    let iterator = it["ShardIterator"].as_str().unwrap();
    let body = s
        .get_records(&GetRecordsRequest {
            shard_iterator: iterator.to_string(),
            ..Default::default()
        })
        .expect("gr");
    matches(&body, include_bytes!("fixtures/gr.json"), "gr");
}

#[test]
fn get_shard_iterator_validation() {
    let dir = tempdir();
    let s = seeded(&dir);
    assert_eq!(
        s.get_shard_iterator(&GetShardIteratorRequest {
            stream_arn: ARN.to_string(),
            shard_id: "shardId-000000000000".to_string(),
            shard_iterator_type: "BOGUS".to_string(),
            ..Default::default()
        })
        .expect_err("bad type")
        .message,
        "unsupported shard iterator type"
    );
    assert_eq!(
        s.get_shard_iterator(&GetShardIteratorRequest {
            stream_arn: ARN.to_string(),
            shard_id: "shardId-000000000000".to_string(),
            shard_iterator_type: "AT_SEQUENCE_NUMBER".to_string(),
            ..Default::default()
        })
        .expect_err("no seq")
        .message,
        "sequence number is required"
    );
}

#[test]
fn at_sequence_number_iterator() {
    let dir = tempdir();
    let s = seeded(&dir);
    // AT sequence "2" => position 1; AFTER "2" => position 2.
    let at = s
        .get_shard_iterator(&GetShardIteratorRequest {
            stream_arn: ARN.to_string(),
            shard_id: "shardId-000000000000".to_string(),
            shard_iterator_type: "AT_SEQUENCE_NUMBER".to_string(),
            sequence_number: "2".to_string(),
        })
        .expect("at");
    let at: Value = serde_json::from_slice(&at).unwrap();
    let recs = s
        .get_records(&GetRecordsRequest {
            shard_iterator: at["ShardIterator"].as_str().unwrap().to_string(),
            ..Default::default()
        })
        .expect("recs");
    let recs: Value = serde_json::from_slice(&recs).unwrap();
    // From position 1: records 2 and 3.
    assert_eq!(recs["Records"].as_array().unwrap().len(), 2);
    assert_eq!(recs["Records"][0]["dynamodb"]["SequenceNumber"], "2");
}

fn shard_iterator(s: &Server, kind: &str) -> String {
    let body = s
        .get_shard_iterator(&GetShardIteratorRequest {
            stream_arn: ARN.to_string(),
            shard_id: "shardId-000000000000".to_string(),
            shard_iterator_type: kind.to_string(),
            ..Default::default()
        })
        .expect("shard iterator");
    let body: Value = serde_json::from_slice(&body).unwrap();
    body["ShardIterator"].as_str().unwrap().to_string()
}

fn records_from(s: &Server, iterator: &str) -> Vec<Value> {
    let body = s
        .get_records(&GetRecordsRequest {
            shard_iterator: iterator.to_string(),
            ..Default::default()
        })
        .expect("records");
    let body: Value = serde_json::from_slice(&body).unwrap();
    body["Records"].as_array().unwrap().clone()
}

fn event_names(records: &[Value]) -> Vec<String> {
    records
        .iter()
        .map(|r| r["eventName"].as_str().unwrap().to_string())
        .collect()
}

#[test]
fn batch_and_transact_writes_emit_stream_records() {
    let dir = tempdir();
    let mut s = seeded(&dir);
    let latest = shard_iterator(&s, "LATEST");
    let mut request_items = BTreeMap::new();
    request_items.insert(
        "T".to_string(),
        vec![
            WriteRequest {
                put_request: Some(PutRequest {
                    item: item(&[("pk", json!({"S": "b"})), ("v", json!({"N": "1"}))]),
                }),
                ..Default::default()
            },
            WriteRequest {
                put_request: Some(PutRequest {
                    item: item(&[("pk", json!({"S": "c"})), ("v", json!({"N": "1"}))]),
                }),
                ..Default::default()
            },
        ],
    );
    s.batch_write_item(&BatchWriteItemRequest {
        request_items,
        ..Default::default()
    })
    .expect("batch put");
    let mut request_items = BTreeMap::new();
    request_items.insert(
        "T".to_string(),
        vec![WriteRequest {
            delete_request: Some(DeleteRequest {
                key: item(&[("pk", json!({"S": "c"}))]),
            }),
            ..Default::default()
        }],
    );
    s.batch_write_item(&BatchWriteItemRequest {
        request_items,
        ..Default::default()
    })
    .expect("batch delete");
    s.transact_write_items(&TransactWriteItemsRequest {
        transact_items: vec![
            TransactWriteItem {
                update: Some(TransactUpdate {
                    table_name: "T".to_string(),
                    key: item(&[("pk", json!({"S": "b"}))]),
                    update_expression: "SET v = :v".to_string(),
                    expression_attribute_values: {
                        let mut m = BTreeMap::new();
                        m.insert(":v".to_string(), json!({"N": "2"}));
                        m
                    },
                    ..Default::default()
                }),
                ..Default::default()
            },
            TransactWriteItem {
                delete: Some(TransactDelete {
                    table_name: "T".to_string(),
                    key: item(&[("pk", json!({"S": "missing"}))]),
                    ..Default::default()
                }),
                ..Default::default()
            },
        ],
    })
    .expect("transact");
    let records = records_from(&s, &latest);
    assert_eq!(
        event_names(&records),
        ["INSERT", "INSERT", "REMOVE", "MODIFY"]
    );
    assert_eq!(records[3]["dynamodb"]["OldImage"]["v"], json!({"N": "1"}));
    assert_eq!(records[3]["dynamodb"]["NewImage"]["v"], json!({"N": "2"}));
    // Sequence numbers keep counting after the seeded records 1..=3.
    assert_eq!(records[0]["dynamodb"]["SequenceNumber"], "4");
}

#[test]
fn ttl_expiry_emits_remove_records() {
    let dir = tempdir();
    let mut s = seeded(&dir);
    s.update_time_to_live(&UpdateTimeToLiveRequest {
        table_name: "T".to_string(),
        time_to_live_specification: TimeToLiveSpecification {
            attribute_name: "exp".to_string(),
            enabled: true,
        },
    })
    .expect("ttl");
    s.put_item(&PutItemRequest {
        table_name: "T".to_string(),
        item: item(&[("pk", json!({"S": "old"})), ("exp", json!({"N": "1"}))]),
        ..Default::default()
    })
    .expect("put");
    let latest = shard_iterator(&s, "LATEST");
    s.expire_ttl_items(ORACLE_SECS).expect("expire");
    let records = records_from(&s, &latest);
    assert_eq!(event_names(&records), ["REMOVE"]);
    assert_eq!(records[0]["dynamodb"]["Keys"]["pk"], json!({"S": "old"}));
    assert_eq!(records[0]["dynamodb"]["OldImage"]["exp"], json!({"N": "1"}));
}

#[test]
fn stream_records_are_trimmed_and_iterators_survive() {
    let dir = tempdir();
    let mut s = seeded(&dir); // sequence numbers 1..=3
    s.set_max_stream_records(3);
    let horizon = shard_iterator(&s, "TRIM_HORIZON");
    let latest = shard_iterator(&s, "LATEST");
    for n in 0..2 {
        s.put_item(&PutItemRequest {
            table_name: "T".to_string(),
            item: item(&[("pk", json!({"S": format!("k{n}")}))]),
            ..Default::default()
        })
        .expect("put");
    }
    // Only the newest three records (3, 4, 5) are retained, on disk too.
    let state = std::fs::read_to_string(dir.join("state.json")).expect("state");
    let state: Value = serde_json::from_str(&state).unwrap();
    let persisted = state["tables"]["T"]["streamRecords"].as_array().unwrap();
    assert!(persisted.len() <= 4, "{}", persisted.len());
    let records = records_from(&s, &shard_iterator(&s, "TRIM_HORIZON"));
    let sequences: Vec<&str> = records
        .iter()
        .map(|r| r["dynamodb"]["SequenceNumber"].as_str().unwrap())
        .collect();
    assert_eq!(sequences, ["3", "4", "5"]);
    // An iterator taken before the trim still resumes at the right record.
    let resumed = records_from(&s, &latest);
    assert_eq!(resumed[0]["dynamodb"]["SequenceNumber"], "4");
    // One pointing at trimmed records is rejected, as in DynamoDB.
    let err = s
        .get_records(&GetRecordsRequest {
            shard_iterator: horizon,
            ..Default::default()
        })
        .expect_err("trimmed");
    assert_eq!(err.name, "TrimmedDataAccessException");
    // New records keep counting from the last sequence number.
    s.put_item(&PutItemRequest {
        table_name: "T".to_string(),
        item: item(&[("pk", json!({"S": "k9"}))]),
        ..Default::default()
    })
    .expect("put");
    let records = records_from(&s, &shard_iterator(&s, "TRIM_HORIZON"));
    assert_eq!(records[2]["dynamodb"]["SequenceNumber"], "6");
}

// --- minimal tempdir -------------------------------------------------------

fn tempdir() -> std::path::PathBuf {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let mut dir = std::env::temp_dir();
    dir.push(format!("devcloud-ddb-stream-{}-{}", std::process::id(), n));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create tempdir");
    dir
}
