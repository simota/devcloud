//! Differential-parity tests for the item operations (PutItem / GetItem /
//! DeleteItem) against golden oracles captured from the legacy service.

use std::collections::BTreeMap;

use devcloud_dynamodb::model::{AttributeDefinition, Item, KeySchemaElement};
use devcloud_dynamodb::requests::{
    CreateTableRequest, DeleteItemRequest, GetItemRequest, PutItemRequest,
};
use devcloud_dynamodb::server::{Config, Server};
use serde_json::{json, Value};

/// `CreationDateTime` baked into the item-state oracle fixture.
const ORACLE_NOW: i64 = 1_780_041_909;

fn parse(body: &[u8]) -> Value {
    serde_json::from_slice(body).expect("valid JSON body")
}

fn item(pairs: &[(&str, Value)]) -> Item {
    let mut m = Item::new();
    for (k, v) in pairs {
        m.insert((*k).to_string(), v.clone());
    }
    m
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
    s.set_fixed_now(ORACLE_NOW);
    s
}

/// Creates the `T` table (pk: S HASH, sk: N RANGE).
fn create_t(s: &mut Server) {
    let req = CreateTableRequest {
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
    };
    s.create_table(&req).expect("create T");
}

fn put_req(it: Item) -> PutItemRequest {
    PutItemRequest {
        table_name: "T".to_string(),
        item: it,
        ..Default::default()
    }
}

#[test]
fn put_then_state_matches_legacy_oracle() {
    let dir = tempdir();
    let mut s = server(&dir);
    create_t(&mut s);
    s.put_item(&put_req(item(&[
        ("pk", json!({"S": "u<1>"})),
        ("sk", json!({"N": "7"})),
        ("name", json!({"S": "Ann"})),
        ("tags", json!({"SS": ["x", "y"]})),
    ])))
    .expect("put");
    let on_disk = std::fs::read(dir.join("state.json")).expect("read state");
    let want = include_bytes!("fixtures/item_state.json").to_vec();
    assert_eq!(
        String::from_utf8_lossy(&on_disk),
        String::from_utf8_lossy(&want),
        "state.json after PutItem must match the legacy oracle byte-for-byte"
    );
}

#[test]
fn put_returns_empty_and_all_old() {
    let dir = tempdir();
    let mut s = server(&dir);
    create_t(&mut s);
    // First put: empty body.
    let first = s
        .put_item(&put_req(item(&[
            ("pk", json!({"S": "u<1>"})),
            ("sk", json!({"N": "7"})),
            ("name", json!({"S": "Ann"})),
            ("tags", json!({"SS": ["x", "y"]})),
            ("score", json!({"N": "3.5"})),
            ("meta", json!({"M": {"a": {"BOOL": true}}})),
        ])))
        .expect("put1");
    assert_eq!(String::from_utf8_lossy(&first), "{}\n");

    // Overwrite with ALL_OLD returns the prior item.
    let mut req = put_req(item(&[
        ("pk", json!({"S": "u<1>"})),
        ("sk", json!({"N": "7"})),
        ("name", json!({"S": "Bob"})),
    ]));
    req.return_values = "ALL_OLD".to_string();
    let body = s.put_item(&req).expect("put2");
    let want = include_bytes!("fixtures/item_put_allold.json").to_vec();
    assert_eq!(
        String::from_utf8_lossy(&body),
        String::from_utf8_lossy(&want),
        "PutItem ALL_OLD body must match the legacy oracle"
    );
}

#[test]
fn get_full_projection_and_missing() {
    let dir = tempdir();
    let mut s = server(&dir);
    create_t(&mut s);
    // Seed item == "Bob" state (matches the oracle GetItem fixtures).
    s.put_item(&put_req(item(&[
        ("pk", json!({"S": "u<1>"})),
        ("sk", json!({"N": "7"})),
        ("name", json!({"S": "Bob"})),
    ])))
    .expect("seed");

    // Full get.
    let full = s
        .get_item(&GetItemRequest {
            table_name: "T".to_string(),
            key: item(&[("pk", json!({"S": "u<1>"})), ("sk", json!({"N": "7"}))]),
            ..Default::default()
        })
        .expect("get");
    let want = include_bytes!("fixtures/item_get.json").to_vec();
    assert_eq!(
        String::from_utf8_lossy(&full),
        String::from_utf8_lossy(&want),
        "GetItem body must match the legacy oracle"
    );

    // Projection + ConsumedCapacity.
    let proj = s
        .get_item(&GetItemRequest {
            table_name: "T".to_string(),
            key: item(&[("pk", json!({"S": "u<1>"})), ("sk", json!({"N": "7"}))]),
            projection_expression: "#n, sk".to_string(),
            expression_attribute_names: names(&[("#n", "name")]),
            return_consumed_capacity: "TOTAL".to_string(),
            ..Default::default()
        })
        .expect("get proj");
    let want_proj = include_bytes!("fixtures/item_get_proj.json").to_vec();
    assert_eq!(
        String::from_utf8_lossy(&proj),
        String::from_utf8_lossy(&want_proj),
        "GetItem projection body must match the legacy oracle"
    );

    // Missing key => empty body.
    let missing = s
        .get_item(&GetItemRequest {
            table_name: "T".to_string(),
            key: item(&[("pk", json!({"S": "zzz"})), ("sk", json!({"N": "0"}))]),
            ..Default::default()
        })
        .expect("get missing");
    assert_eq!(String::from_utf8_lossy(&missing), "{}\n");
}

#[test]
fn delete_condition_failure_and_all_old() {
    let dir = tempdir();
    let mut s = server(&dir);
    create_t(&mut s);
    s.put_item(&put_req(item(&[
        ("pk", json!({"S": "u<1>"})),
        ("sk", json!({"N": "7"})),
        ("name", json!({"S": "Bob"})),
    ])))
    .expect("seed");

    // Failing condition: attribute_not_exists(pk) when pk exists.
    let err = s
        .delete_item(&DeleteItemRequest {
            table_name: "T".to_string(),
            key: item(&[("pk", json!({"S": "u<1>"})), ("sk", json!({"N": "7"}))]),
            condition_expression: "attribute_not_exists(pk)".to_string(),
            ..Default::default()
        })
        .expect_err("cond");
    assert_eq!(err.name, "ConditionalCheckFailedException");
    assert_eq!(err.message, "condition check failed");

    // Successful delete with ALL_OLD.
    let body = s
        .delete_item(&DeleteItemRequest {
            table_name: "T".to_string(),
            key: item(&[("pk", json!({"S": "u<1>"})), ("sk", json!({"N": "7"}))]),
            return_values: "ALL_OLD".to_string(),
            ..Default::default()
        })
        .expect("delete");
    let want = include_bytes!("fixtures/item_del_allold.json").to_vec();
    assert_eq!(
        String::from_utf8_lossy(&body),
        String::from_utf8_lossy(&want),
        "DeleteItem ALL_OLD body must match the legacy oracle"
    );
    // Table is empty again.
    let on_disk = std::fs::read(dir.join("state.json")).expect("read");
    assert!(String::from_utf8_lossy(&on_disk).contains("\"items\":{}"));
}

#[test]
fn put_validation_errors_match_legacy() {
    let dir = tempdir();
    let mut s = server(&dir);
    create_t(&mut s);

    // Empty item.
    let err = s.put_item(&put_req(Item::new())).expect_err("empty");
    assert_eq!(err.message, "item is required");

    // Missing key attribute.
    let err = s
        .put_item(&put_req(item(&[("pk", json!({"S": "x"}))])))
        .expect_err("nokey");
    assert_eq!(err.message, "missing key attribute sk");

    // Invalid attribute value (two types).
    let err = s
        .put_item(&put_req(item(&[
            ("pk", json!({"S": "x"})),
            ("sk", json!({"N": "1"})),
            ("bad", json!({"S": "a", "N": "1"})),
        ])))
        .expect_err("bad");
    assert_eq!(
        err.message,
        "attribute bad must contain exactly one AttributeValue type"
    );

    // Unknown table.
    let mut req = put_req(item(&[
        ("pk", json!({"S": "x"})),
        ("sk", json!({"N": "1"})),
    ]));
    req.table_name = "Nope".to_string();
    assert_eq!(
        s.put_item(&req).expect_err("notable").name,
        "ResourceNotFoundException"
    );
}

#[test]
fn condition_failure_all_old_carries_item() {
    let dir = tempdir();
    let mut s = server(&dir);
    create_t(&mut s);
    s.put_item(&put_req(item(&[
        ("pk", json!({"S": "u<1>"})),
        ("sk", json!({"N": "7"})),
        ("name", json!({"S": "Bob"})),
    ])))
    .expect("seed");
    let err = s
        .delete_item(&DeleteItemRequest {
            table_name: "T".to_string(),
            key: item(&[("pk", json!({"S": "u<1>"})), ("sk", json!({"N": "7"}))]),
            condition_expression: "attribute_not_exists(pk)".to_string(),
            return_values_on_condition_check_failure: "ALL_OLD".to_string(),
            ..Default::default()
        })
        .expect_err("cond");
    let body = parse(&err.body_bytes());
    assert_eq!(body["Item"]["name"], json!({"S": "Bob"}));
    assert_eq!(
        body["__type"],
        "com.amazonaws.dynamodb.v20120810#ConditionalCheckFailedException"
    );
}

// --- minimal tempdir -------------------------------------------------------

fn tempdir() -> std::path::PathBuf {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let mut dir = std::env::temp_dir();
    dir.push(format!("devcloud-ddb-item-{}-{}", std::process::id(), n));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create tempdir");
    dir
}
