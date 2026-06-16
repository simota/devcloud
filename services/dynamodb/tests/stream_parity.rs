//! Differential-parity tests for DynamoDB Streams (record generation + the
//! ListStreams / DescribeStream / GetShardIterator / GetRecords operations)
//! against golden oracles captured from the legacy service.

use std::collections::BTreeMap;

use devcloud_dynamodb::model::{AttributeDefinition, Item, KeySchemaElement, StreamSpecification};
use devcloud_dynamodb::requests::{
    CreateTableRequest, DeleteItemRequest, DescribeStreamRequest, GetRecordsRequest,
    GetShardIteratorRequest, ListStreamsRequest, PutItemRequest, UpdateItemRequest,
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
