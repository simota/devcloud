//! Differential-parity tests for BatchGetItem / BatchWriteItem /
//! TransactGetItems / TransactWriteItems against golden oracles captured from the
//! Go service.

use std::collections::BTreeMap;

use devcloud_dynamodb::model::{AttributeDefinition, Item, KeySchemaElement};
use devcloud_dynamodb::requests::{
    BatchGetItemRequest, BatchGetTableRequest, BatchWriteItemRequest, CreateTableRequest,
    DeleteRequest, PutItemRequest, PutRequest, TransactConditionCheck, TransactDelete, TransactGet,
    TransactGetItem, TransactGetItemsRequest, TransactPut, TransactUpdate, TransactWriteItem,
    TransactWriteItemsRequest, WriteRequest,
};
use devcloud_dynamodb::server::{Config, Server};
use serde_json::{json, Value};

const ORACLE_NOW: i64 = 1_780_045_243;

fn item(pairs: &[(&str, Value)]) -> Item {
    let mut m = Item::new();
    for (k, v) in pairs {
        m.insert((*k).to_string(), v.clone());
    }
    m
}

fn vals(pairs: &[(&str, Value)]) -> BTreeMap<String, Value> {
    pairs
        .iter()
        .map(|(k, v)| ((*k).to_string(), v.clone()))
        .collect()
}

/// Table `T` (hash `pk`) seeded with a/b/c, matching the Go oracle.
fn seeded(dir: &std::path::Path) -> Server {
    let mut s = Server::new(Config {
        region: "us-east-1".to_string(),
        auth_mode: "relaxed".to_string(),
        storage_path: dir.to_string_lossy().to_string(),
        ..Default::default()
    });
    s.set_fixed_now(ORACLE_NOW);
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
        ..Default::default()
    })
    .expect("create");
    for (k, v) in [("a", "1"), ("b", "2"), ("c", "3")] {
        s.put_item(&PutItemRequest {
            table_name: "T".to_string(),
            item: item(&[("pk", json!({"S": k})), ("v", json!({"N": v}))]),
            ..Default::default()
        })
        .expect("put");
    }
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
fn batch_get_with_missing_and_consumed_capacity() {
    let dir = tempdir();
    let s = seeded(&dir);
    let mut request_items = BTreeMap::new();
    request_items.insert(
        "T".to_string(),
        BatchGetTableRequest {
            keys: vec![
                item(&[("pk", json!({"S": "a"}))]),
                item(&[("pk", json!({"S": "zzz"}))]),
                item(&[("pk", json!({"S": "c"}))]),
            ],
            ..Default::default()
        },
    );
    let body = s
        .batch_get_item(&BatchGetItemRequest {
            request_items,
            return_consumed_capacity: "TOTAL".to_string(),
        })
        .expect("bget");
    matches(&body, include_bytes!("fixtures/bget.json"), "bget");
}

#[test]
fn batch_get_with_projection() {
    let dir = tempdir();
    let s = seeded(&dir);
    let mut request_items = BTreeMap::new();
    request_items.insert(
        "T".to_string(),
        BatchGetTableRequest {
            keys: vec![item(&[("pk", json!({"S": "a"}))])],
            projection_expression: "pk".to_string(),
            ..Default::default()
        },
    );
    let body = s
        .batch_get_item(&BatchGetItemRequest {
            request_items,
            ..Default::default()
        })
        .expect("bget proj");
    matches(
        &body,
        include_bytes!("fixtures/bget_proj.json"),
        "bget_proj",
    );
}

#[test]
fn batch_write_put_and_delete() {
    let dir = tempdir();
    let mut s = seeded(&dir);
    let mut request_items = BTreeMap::new();
    request_items.insert(
        "T".to_string(),
        vec![
            WriteRequest {
                put_request: Some(PutRequest {
                    item: item(&[("pk", json!({"S": "d"})), ("v", json!({"N": "4"}))]),
                }),
                delete_request: None,
            },
            WriteRequest {
                put_request: None,
                delete_request: Some(DeleteRequest {
                    key: item(&[("pk", json!({"S": "b"}))]),
                }),
            },
        ],
    );
    let body = s
        .batch_write_item(&BatchWriteItemRequest {
            request_items,
            ..Default::default()
        })
        .expect("bwrite");
    matches(&body, include_bytes!("fixtures/bwrite.json"), "bwrite");
}

#[test]
fn transact_get_items_with_missing() {
    let dir = tempdir();
    let mut s = seeded(&dir);
    // Seed `d` so the oracle's third Get hits.
    s.put_item(&PutItemRequest {
        table_name: "T".to_string(),
        item: item(&[("pk", json!({"S": "d"})), ("v", json!({"N": "4"}))]),
        ..Default::default()
    })
    .expect("seed d");
    let body = s
        .transact_get_items(&TransactGetItemsRequest {
            transact_items: vec![
                TransactGetItem {
                    get: Some(TransactGet {
                        table_name: "T".to_string(),
                        key: item(&[("pk", json!({"S": "a"}))]),
                        ..Default::default()
                    }),
                },
                TransactGetItem {
                    get: Some(TransactGet {
                        table_name: "T".to_string(),
                        key: item(&[("pk", json!({"S": "missing"}))]),
                        ..Default::default()
                    }),
                },
                TransactGetItem {
                    get: Some(TransactGet {
                        table_name: "T".to_string(),
                        key: item(&[("pk", json!({"S": "d"}))]),
                        ..Default::default()
                    }),
                },
            ],
        })
        .expect("tget");
    matches(&body, include_bytes!("fixtures/tget.json"), "tget");
}

/// Reproduces the full Go oracle sequence (bwrite then twrite) and checks the
/// final state.json byte-for-byte.
#[test]
fn transact_write_scenario_and_final_state() {
    let dir = tempdir();
    let mut s = seeded(&dir);
    // bwrite: put d, delete b.
    let mut bw = BTreeMap::new();
    bw.insert(
        "T".to_string(),
        vec![
            WriteRequest {
                put_request: Some(PutRequest {
                    item: item(&[("pk", json!({"S": "d"})), ("v", json!({"N": "4"}))]),
                }),
                delete_request: None,
            },
            WriteRequest {
                put_request: None,
                delete_request: Some(DeleteRequest {
                    key: item(&[("pk", json!({"S": "b"}))]),
                }),
            },
        ],
    );
    s.batch_write_item(&BatchWriteItemRequest {
        request_items: bw,
        ..Default::default()
    })
    .expect("bwrite");

    // twrite: put e, update a (+10), delete c, conditioncheck d exists.
    let body = s
        .transact_write_items(&TransactWriteItemsRequest {
            transact_items: vec![
                TransactWriteItem {
                    put: Some(TransactPut {
                        table_name: "T".to_string(),
                        item: item(&[("pk", json!({"S": "e"})), ("v", json!({"N": "5"}))]),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
                TransactWriteItem {
                    update: Some(TransactUpdate {
                        table_name: "T".to_string(),
                        key: item(&[("pk", json!({"S": "a"}))]),
                        update_expression: "SET v = v + :one".to_string(),
                        expression_attribute_values: vals(&[(":one", json!({"N": "10"}))]),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
                TransactWriteItem {
                    delete: Some(TransactDelete {
                        table_name: "T".to_string(),
                        key: item(&[("pk", json!({"S": "c"}))]),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
                TransactWriteItem {
                    condition_check: Some(TransactConditionCheck {
                        table_name: "T".to_string(),
                        key: item(&[("pk", json!({"S": "d"}))]),
                        condition_expression: "attribute_exists(pk)".to_string(),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
            ],
        })
        .expect("twrite");
    matches(&body, include_bytes!("fixtures/twrite.json"), "twrite");

    let on_disk = std::fs::read(dir.join("state.json")).expect("read state");
    matches(
        &on_disk,
        include_bytes!("fixtures/batch_state.json"),
        "state",
    );
}

#[test]
fn transact_write_condition_failure_is_cancelled() {
    let dir = tempdir();
    let mut s = seeded(&dir);
    let err = s
        .transact_write_items(&TransactWriteItemsRequest {
            transact_items: vec![TransactWriteItem {
                condition_check: Some(TransactConditionCheck {
                    table_name: "T".to_string(),
                    key: item(&[("pk", json!({"S": "a"}))]),
                    condition_expression: "attribute_not_exists(pk)".to_string(),
                    ..Default::default()
                }),
                ..Default::default()
            }],
        })
        .expect_err("cancelled");
    assert_eq!(err.status, 400);
    assert_eq!(err.name, "TransactionCanceledException");
    assert_eq!(err.message, "transaction cancelled");
    // No mutation persisted (condition check only).
    let on_disk = std::fs::read(dir.join("state.json")).expect("read");
    assert!(String::from_utf8_lossy(&on_disk).contains("\"a\""));
}

#[test]
fn batch_write_unknown_table_errors() {
    let dir = tempdir();
    let mut s = seeded(&dir);
    let mut request_items = BTreeMap::new();
    request_items.insert(
        "Nope".to_string(),
        vec![WriteRequest {
            put_request: Some(PutRequest {
                item: item(&[("pk", json!({"S": "x"}))]),
            }),
            delete_request: None,
        }],
    );
    let err = s
        .batch_write_item(&BatchWriteItemRequest {
            request_items,
            ..Default::default()
        })
        .expect_err("notable");
    assert_eq!(err.name, "ResourceNotFoundException");
}

#[test]
fn batch_write_requires_exactly_one_op() {
    let dir = tempdir();
    let mut s = seeded(&dir);
    let mut request_items = BTreeMap::new();
    request_items.insert(
        "T".to_string(),
        vec![WriteRequest {
            put_request: None,
            delete_request: None,
        }],
    );
    let err = s
        .batch_write_item(&BatchWriteItemRequest {
            request_items,
            ..Default::default()
        })
        .expect_err("noop");
    assert_eq!(
        err.message,
        "each write request must contain exactly one operation"
    );
}

// --- minimal tempdir -------------------------------------------------------

fn tempdir() -> std::path::PathBuf {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let mut dir = std::env::temp_dir();
    dir.push(format!("devcloud-ddb-batch-{}-{}", std::process::id(), n));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create tempdir");
    dir
}
