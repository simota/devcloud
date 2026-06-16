//! Differential-parity tests for Query / Scan (key conditions, filters,
//! pagination, Select, index projection) against golden oracles captured from
//! the legacy service.

use std::collections::BTreeMap;

use devcloud_dynamodb::model::{AttributeDefinition, Item, KeySchemaElement};
use devcloud_dynamodb::requests::{
    CreateTableRequest, GlobalSecondaryIndexRequest, IndexProjectionRequest, PutItemRequest,
    QueryRequest, ScanRequest,
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

fn vals(pairs: &[(&str, Value)]) -> BTreeMap<String, Value> {
    pairs
        .iter()
        .map(|(k, v)| ((*k).to_string(), v.clone()))
        .collect()
}

fn names(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
    pairs
        .iter()
        .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
        .collect()
}

fn server(dir: &std::path::Path) -> Server {
    let mut s = Server::new(Config {
        region: "us-east-1".to_string(),
        auth_mode: "relaxed".to_string(),
        storage_path: dir.to_string_lossy().to_string(),
        ..Default::default()
    });
    s.set_fixed_now(1_780_000_000);
    s
}

/// Builds the `T` table + 4 items exactly as the legacy oracle test did.
fn seeded(dir: &std::path::Path) -> Server {
    let mut s = server(dir);
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
            AttributeDefinition {
                attribute_name: "gsiPk".to_string(),
                attribute_type: "S".to_string(),
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
        global_secondary_indexes: vec![GlobalSecondaryIndexRequest {
            index_name: "byGsi".to_string(),
            key_schema: vec![KeySchemaElement {
                attribute_name: "gsiPk".to_string(),
                key_type: "HASH".to_string(),
            }],
            projection: IndexProjectionRequest {
                projection_type: "INCLUDE".to_string(),
                non_key_attributes: vec!["name".to_string()],
            },
        }],
        billing_mode: "PAY_PER_REQUEST".to_string(),
        ..Default::default()
    })
    .expect("create");
    let items = [
        item(&[
            ("pk", json!({"S": "u1"})),
            ("sk", json!({"N": "1"})),
            ("name", json!({"S": "Ann"})),
            ("gsiPk", json!({"S": "g1"})),
            ("extra", json!({"S": "e1"})),
        ]),
        item(&[
            ("pk", json!({"S": "u1"})),
            ("sk", json!({"N": "2"})),
            ("name", json!({"S": "Bob"})),
            ("gsiPk", json!({"S": "g1"})),
            ("extra", json!({"S": "e2"})),
        ]),
        item(&[
            ("pk", json!({"S": "u1"})),
            ("sk", json!({"N": "3"})),
            ("name", json!({"S": "Cy"})),
            ("extra", json!({"S": "e3"})),
        ]),
        item(&[
            ("pk", json!({"S": "u2"})),
            ("sk", json!({"N": "1"})),
            ("name", json!({"S": "Dee"})),
            ("gsiPk", json!({"S": "g2"})),
        ]),
    ];
    for it in items {
        s.put_item(&PutItemRequest {
            table_name: "T".to_string(),
            item: it,
            ..Default::default()
        })
        .expect("put");
    }
    s
}

fn assert_matches(got: &[u8], fixture: &[u8], label: &str) {
    assert_eq!(
        String::from_utf8_lossy(got),
        String::from_utf8_lossy(fixture),
        "{label}"
    );
}

fn q(s: &Server, r: QueryRequest) -> Vec<u8> {
    s.query(&r).expect("query")
}
fn sc(s: &Server, r: ScanRequest) -> Vec<u8> {
    s.scan(&r).expect("scan")
}

fn base_query() -> QueryRequest {
    QueryRequest {
        table_name: "T".to_string(),
        key_condition_expression: "pk = :p".to_string(),
        expression_attribute_values: vals(&[(":p", json!({"S": "u1"}))]),
        ..Default::default()
    }
}

#[test]
fn query_basic() {
    let dir = tempdir();
    let s = seeded(&dir);
    assert_matches(
        &q(&s, base_query()),
        include_bytes!("fixtures/q_basic.json"),
        "q_basic",
    );
}

#[test]
fn query_sk_condition() {
    let dir = tempdir();
    let s = seeded(&dir);
    let r = QueryRequest {
        key_condition_expression: "pk = :p AND sk > :s".to_string(),
        expression_attribute_values: vals(&[(":p", json!({"S": "u1"})), (":s", json!({"N": "1"}))]),
        ..base_query()
    };
    assert_matches(&q(&s, r), include_bytes!("fixtures/q_sk.json"), "q_sk");
}

#[test]
fn query_descending() {
    let dir = tempdir();
    let s = seeded(&dir);
    let r = QueryRequest {
        scan_index_forward: Some(false),
        ..base_query()
    };
    assert_matches(&q(&s, r), include_bytes!("fixtures/q_desc.json"), "q_desc");
}

#[test]
fn query_limit_pagination() {
    let dir = tempdir();
    let s = seeded(&dir);
    let r = QueryRequest {
        limit: 2,
        ..base_query()
    };
    assert_matches(
        &q(&s, r),
        include_bytes!("fixtures/q_limit.json"),
        "q_limit",
    );
}

#[test]
fn query_projection() {
    let dir = tempdir();
    let s = seeded(&dir);
    let r = QueryRequest {
        projection_expression: "sk, #n".to_string(),
        expression_attribute_names: names(&[("#n", "name")]),
        ..base_query()
    };
    assert_matches(&q(&s, r), include_bytes!("fixtures/q_proj.json"), "q_proj");
}

#[test]
fn query_select_count() {
    let dir = tempdir();
    let s = seeded(&dir);
    let r = QueryRequest {
        select: "COUNT".to_string(),
        ..base_query()
    };
    assert_matches(
        &q(&s, r),
        include_bytes!("fixtures/q_count.json"),
        "q_count",
    );
}

#[test]
fn query_gsi_projection() {
    let dir = tempdir();
    let s = seeded(&dir);
    let r = QueryRequest {
        index_name: "byGsi".to_string(),
        key_condition_expression: "gsiPk = :g".to_string(),
        expression_attribute_values: vals(&[(":g", json!({"S": "g1"}))]),
        ..QueryRequest::default()
    };
    let r = QueryRequest {
        table_name: "T".to_string(),
        ..r
    };
    assert_matches(&q(&s, r), include_bytes!("fixtures/q_gsi.json"), "q_gsi");
}

#[test]
fn scan_all() {
    let dir = tempdir();
    let s = seeded(&dir);
    let r = ScanRequest {
        table_name: "T".to_string(),
        ..Default::default()
    };
    assert_matches(
        &sc(&s, r),
        include_bytes!("fixtures/scan_all.json"),
        "scan_all",
    );
}

#[test]
fn scan_filter() {
    let dir = tempdir();
    let s = seeded(&dir);
    let r = ScanRequest {
        table_name: "T".to_string(),
        filter_expression: "begins_with(#n, :pre)".to_string(),
        expression_attribute_names: names(&[("#n", "name")]),
        expression_attribute_values: vals(&[(":pre", json!({"S": "B"}))]),
        ..Default::default()
    };
    assert_matches(
        &sc(&s, r),
        include_bytes!("fixtures/scan_filter.json"),
        "scan_filter",
    );
}

#[test]
fn scan_limit_pagination() {
    let dir = tempdir();
    let s = seeded(&dir);
    let r = ScanRequest {
        table_name: "T".to_string(),
        limit: 2,
        ..Default::default()
    };
    assert_matches(
        &sc(&s, r),
        include_bytes!("fixtures/scan_limit.json"),
        "scan_limit",
    );
}

#[test]
fn scan_gsi() {
    let dir = tempdir();
    let s = seeded(&dir);
    let r = ScanRequest {
        table_name: "T".to_string(),
        index_name: "byGsi".to_string(),
        ..Default::default()
    };
    assert_matches(
        &sc(&s, r),
        include_bytes!("fixtures/scan_gsi.json"),
        "scan_gsi",
    );
}

#[test]
fn query_missing_key_condition_errors() {
    let dir = tempdir();
    let s = seeded(&dir);
    let err = s
        .query(&QueryRequest {
            table_name: "T".to_string(),
            ..Default::default()
        })
        .expect_err("no kce");
    assert_eq!(err.message, "key condition expression is required");
}

#[test]
fn query_unknown_index_errors() {
    let dir = tempdir();
    let s = seeded(&dir);
    let err = s
        .query(&QueryRequest {
            index_name: "nope".to_string(),
            ..base_query()
        })
        .expect_err("bad index");
    assert_eq!(err.message, "index not found");
}

#[test]
fn select_count_with_projection_errors() {
    let dir = tempdir();
    let s = seeded(&dir);
    let err = s
        .query(&QueryRequest {
            select: "COUNT".to_string(),
            projection_expression: "sk".to_string(),
            ..base_query()
        })
        .expect_err("count+proj");
    assert_eq!(
        err.message,
        "select COUNT cannot be used with ProjectionExpression"
    );
}

#[test]
fn query_pagination_continues_from_last_key() {
    let dir = tempdir();
    let s = seeded(&dir);
    // First page (limit 2) yields LastEvaluatedKey {pk:u1, sk:2}.
    let first: Value = serde_json::from_slice(&q(
        &s,
        QueryRequest {
            limit: 2,
            ..base_query()
        },
    ))
    .unwrap();
    let last = first["LastEvaluatedKey"].clone();
    let start: Item = serde_json::from_value(last).unwrap();
    // Continue.
    let second: Value = serde_json::from_slice(&q(
        &s,
        QueryRequest {
            limit: 2,
            exclusive_start_key: start,
            ..base_query()
        },
    ))
    .unwrap();
    assert_eq!(second["Count"], 1);
    assert_eq!(second["Items"][0]["sk"], json!({"N": "3"}));
    assert!(second.get("LastEvaluatedKey").is_none());
}

// --- minimal tempdir -------------------------------------------------------

fn tempdir() -> std::path::PathBuf {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let mut dir = std::env::temp_dir();
    dir.push(format!("devcloud-ddb-query-{}-{}", std::process::id(), n));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create tempdir");
    dir
}
