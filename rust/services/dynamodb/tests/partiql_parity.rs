//! Differential-parity tests for PartiQL (ExecuteStatement /
//! BatchExecuteStatement / ExecuteTransaction) against golden oracles captured
//! from the Go service.

use devcloud_dynamodb::model::{AttributeDefinition, Item, KeySchemaElement};
use devcloud_dynamodb::requests::{
    BatchExecuteStatementRequest, BatchStatementRequest, CreateTableRequest,
    ExecuteStatementRequest, ExecuteTransactionRequest, PutItemRequest,
};
use devcloud_dynamodb::server::{Config, Server};
use serde_json::{json, Value};

fn item(pairs: &[(&str, Value)]) -> Item {
    let mut m = Item::new();
    for (k, v) in pairs {
        m.insert((*k).to_string(), v.clone());
    }
    m
}

/// Table `T` (pk S HASH, sk N RANGE) seeded with the oracle's 3 items.
fn seeded(dir: &std::path::Path) -> Server {
    let mut s = Server::new(Config {
        region: "us-east-1".to_string(),
        auth_mode: "relaxed".to_string(),
        storage_path: dir.to_string_lossy().to_string(),
        ..Default::default()
    });
    s.set_fixed_now(1_780_000_000);
    s.create_table(&CreateTableRequest {
        table_name: "T".to_string(),
        attribute_definitions: vec![
            AttributeDefinition {
                attribute_name: "pk".to_string(),
                attribute_type: "S".to_string(),
            },
            AttributeDefinition {
                attribute_name: "sk".to_string(),
                attribute_type: "N".to_string(),
            },
        ],
        key_schema: vec![
            KeySchemaElement {
                attribute_name: "pk".to_string(),
                key_type: "HASH".to_string(),
            },
            KeySchemaElement {
                attribute_name: "sk".to_string(),
                key_type: "RANGE".to_string(),
            },
        ],
        billing_mode: "PAY_PER_REQUEST".to_string(),
        ..Default::default()
    })
    .expect("create");
    for (pk, sk, name) in [("u1", "1", "Ann"), ("u1", "2", "Bob"), ("u2", "1", "Cy")] {
        s.put_item(&PutItemRequest {
            table_name: "T".to_string(),
            item: item(&[
                ("pk", json!({"S": pk})),
                ("sk", json!({"N": sk})),
                ("name", json!({"S": name})),
            ]),
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

fn es(stmt: &str, params: Vec<Value>, limit: i64, rcc: &str) -> ExecuteStatementRequest {
    ExecuteStatementRequest {
        statement: stmt.to_string(),
        parameters: params,
        limit,
        return_consumed_capacity: rcc.to_string(),
        ..Default::default()
    }
}

#[test]
fn execute_statement_basic() {
    let dir = tempdir();
    let s = seeded(&dir);
    let body = s
        .execute_statement(&es(
            "SELECT * FROM T WHERE pk = ?",
            vec![json!({"S": "u1"})],
            0,
            "",
        ))
        .expect("es");
    matches(&body, include_bytes!("fixtures/es_basic.json"), "es_basic");
}

#[test]
fn execute_statement_projection() {
    let dir = tempdir();
    let s = seeded(&dir);
    let body = s
        .execute_statement(&es(
            "SELECT name, sk FROM \"T\" WHERE pk = ? AND sk = ?",
            vec![json!({"S": "u1"}), json!({"N": "2"})],
            0,
            "",
        ))
        .expect("es");
    matches(&body, include_bytes!("fixtures/es_proj.json"), "es_proj");
}

#[test]
fn execute_statement_all() {
    let dir = tempdir();
    let s = seeded(&dir);
    let body = s
        .execute_statement(&es("SELECT * FROM T", vec![], 0, ""))
        .expect("es");
    matches(&body, include_bytes!("fixtures/es_all.json"), "es_all");
}

#[test]
fn execute_statement_limit_and_consumed_capacity() {
    let dir = tempdir();
    let s = seeded(&dir);
    let body = s
        .execute_statement(&es(
            "SELECT * FROM T WHERE pk = ?",
            vec![json!({"S": "u1"})],
            1,
            "TOTAL",
        ))
        .expect("es");
    matches(&body, include_bytes!("fixtures/es_limit.json"), "es_limit");
}

#[test]
fn execute_statement_errors() {
    let dir = tempdir();
    let s = seeded(&dir);
    assert_eq!(
        s.execute_statement(&es(
            "DELETE FROM T WHERE pk = ?",
            vec![json!({"S": "u1"})],
            0,
            ""
        ))
        .expect_err("delete")
        .message,
        "only SELECT statements are supported"
    );
    assert_eq!(
        s.execute_statement(&es("SELECT * FROM T WHERE pk = ?", vec![], 0, ""))
            .expect_err("noparam")
            .message,
        "missing PartiQL parameter"
    );
}

#[test]
fn batch_execute_statement_mixed() {
    let dir = tempdir();
    let s = seeded(&dir);
    let body = s
        .batch_execute_statement(&BatchExecuteStatementRequest {
            statements: vec![
                BatchStatementRequest {
                    statement: "SELECT * FROM T WHERE pk = ? AND sk = ?".to_string(),
                    parameters: vec![json!({"S": "u1"}), json!({"N": "1"})],
                    ..Default::default()
                },
                BatchStatementRequest {
                    statement: "SELECT * FROM Missing WHERE pk = ?".to_string(),
                    parameters: vec![json!({"S": "x"})],
                    ..Default::default()
                },
                BatchStatementRequest {
                    statement: "SELECT * FROM T WHERE pk = ?".to_string(),
                    parameters: vec![json!({"S": "u1"})],
                    ..Default::default()
                },
            ],
            ..Default::default()
        })
        .expect("bes");
    matches(&body, include_bytes!("fixtures/bes.json"), "bes");
}

#[test]
fn execute_transaction_ok() {
    let dir = tempdir();
    let s = seeded(&dir);
    let body = s
        .execute_transaction(&ExecuteTransactionRequest {
            transact_statements: vec![
                BatchStatementRequest {
                    statement: "SELECT * FROM T WHERE pk = ? AND sk = ?".to_string(),
                    parameters: vec![json!({"S": "u1"}), json!({"N": "1"})],
                    ..Default::default()
                },
                BatchStatementRequest {
                    statement: "SELECT * FROM T WHERE pk = ? AND sk = ?".to_string(),
                    parameters: vec![json!({"S": "u2"}), json!({"N": "1"})],
                    ..Default::default()
                },
            ],
            ..Default::default()
        })
        .expect("etx");
    matches(&body, include_bytes!("fixtures/etx.json"), "etx");
}

#[test]
fn execute_transaction_requires_key_cover() {
    let dir = tempdir();
    let s = seeded(&dir);
    let err = s
        .execute_transaction(&ExecuteTransactionRequest {
            transact_statements: vec![BatchStatementRequest {
                statement: "SELECT * FROM T WHERE pk = ?".to_string(),
                parameters: vec![json!({"S": "u1"})],
                ..Default::default()
            }],
            ..Default::default()
        })
        .expect_err("etx err");
    assert_eq!(
        err.message,
        "SELECT statement must include equality conditions for all key attributes"
    );
}

// --- minimal tempdir -------------------------------------------------------

fn tempdir() -> std::path::PathBuf {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let mut dir = std::env::temp_dir();
    dir.push(format!("devcloud-ddb-partiql-{}-{}", std::process::id(), n));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create tempdir");
    dir
}
