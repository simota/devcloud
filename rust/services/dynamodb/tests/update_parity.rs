//! Differential-parity tests for UpdateItem (SET/REMOVE/ADD/DELETE, arithmetic,
//! if_not_exists, list_append) against golden oracles captured from the Go
//! service.

use std::collections::BTreeMap;

use devcloud_dynamodb::model::{AttributeDefinition, Item, KeySchemaElement};
use devcloud_dynamodb::requests::{CreateTableRequest, PutItemRequest, UpdateItemRequest};
use devcloud_dynamodb::server::{Config, Server};
use serde_json::{json, Value};

/// `CreationDateTime` baked into the update-state oracle fixture.
const ORACLE_NOW: i64 = 1_780_043_554;

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

/// Table `T` with hash key `pk` (S).
fn create_t(s: &mut Server) {
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
    .expect("create T");
}

fn put(s: &mut Server, it: Item) {
    s.put_item(&PutItemRequest {
        table_name: "T".to_string(),
        item: it,
        ..Default::default()
    })
    .expect("put");
}

fn update(s: &mut Server, key: Item, expr: &str, v: BTreeMap<String, Value>, ret: &str) -> Vec<u8> {
    s.update_item(&UpdateItemRequest {
        table_name: "T".to_string(),
        key,
        update_expression: expr.to_string(),
        expression_attribute_values: v,
        return_values: ret.to_string(),
        ..Default::default()
    })
    .expect("update")
}

/// Reproduces the Go oracle scenario end-to-end and checks every response body
/// plus the final state.json, all byte-for-byte.
#[test]
fn update_scenario_matches_go_oracle() {
    let dir = tempdir();
    let mut s = server(&dir);
    create_t(&mut s);
    put(
        &mut s,
        item(&[
            ("pk", json!({"S": "k"})),
            ("count", json!({"N": "10"})),
            ("tags", json!({"SS": ["a", "b"]})),
            ("list", json!({"L": [{"S": "x"}]})),
        ]),
    );

    // SET arithmetic + if_not_exists + list_append, REMOVE, ADD — ALL_NEW.
    let all_new = update(
        &mut s,
        item(&[("pk", json!({"S": "k"}))]),
        "SET count = count + :inc, label = if_not_exists(label, :def), list = list_append(list, :more) REMOVE tags ADD score :s",
        vals(&[
            (":inc", json!({"N": "5"})),
            (":def", json!({"S": "new"})),
            (":more", json!({"L": [{"S": "y"}]})),
            (":s", json!({"N": "3"})),
        ]),
        "ALL_NEW",
    );
    assert_eq!(
        String::from_utf8_lossy(&all_new),
        String::from_utf8_lossy(&include_bytes!("fixtures/upd_allnew.json")[..]),
        "ALL_NEW body"
    );

    // SET subtraction — UPDATED_NEW returns only `count`.
    let updated_new = update(
        &mut s,
        item(&[("pk", json!({"S": "k"}))]),
        "SET count = count - :d",
        vals(&[(":d", json!({"N": "2.5"}))]),
        "UPDATED_NEW",
    );
    assert_eq!(
        String::from_utf8_lossy(&updated_new),
        String::from_utf8_lossy(&include_bytes!("fixtures/upd_updnew.json")[..]),
        "UPDATED_NEW body"
    );

    // SET label — UPDATED_OLD returns the prior label.
    let updated_old = update(
        &mut s,
        item(&[("pk", json!({"S": "k"}))]),
        "SET label = :l",
        vals(&[(":l", json!({"S": "changed"}))]),
        "UPDATED_OLD",
    );
    assert_eq!(
        String::from_utf8_lossy(&updated_old),
        String::from_utf8_lossy(&include_bytes!("fixtures/upd_updold.json")[..]),
        "UPDATED_OLD body"
    );

    // DELETE from a set — ALL_NEW.
    put(
        &mut s,
        item(&[
            ("pk", json!({"S": "k2"})),
            ("colors", json!({"SS": ["red", "green", "blue"]})),
        ]),
    );
    let del_set = update(
        &mut s,
        item(&[("pk", json!({"S": "k2"}))]),
        "DELETE colors :c",
        vals(&[(":c", json!({"SS": ["green"]}))]),
        "ALL_NEW",
    );
    assert_eq!(
        String::from_utf8_lossy(&del_set),
        String::from_utf8_lossy(&include_bytes!("fixtures/upd_delset.json")[..]),
        "DELETE ALL_NEW body"
    );

    // Final state.json.
    let on_disk = std::fs::read(dir.join("state.json")).expect("read state");
    assert_eq!(
        String::from_utf8_lossy(&on_disk),
        String::from_utf8_lossy(&include_bytes!("fixtures/upd_state.json")[..]),
        "final state.json"
    );
}

#[test]
fn update_on_absent_item_creates_from_key() {
    let dir = tempdir();
    let mut s = server(&dir);
    create_t(&mut s);
    let body = update(
        &mut s,
        item(&[("pk", json!({"S": "fresh"}))]),
        "SET n = :v",
        vals(&[(":v", json!({"N": "1"}))]),
        "ALL_NEW",
    );
    let parsed: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(parsed["Attributes"]["pk"], json!({"S": "fresh"}));
    assert_eq!(parsed["Attributes"]["n"], json!({"N": "1"}));
}

#[test]
fn update_return_values_validation() {
    let dir = tempdir();
    let mut s = server(&dir);
    create_t(&mut s);
    let err = s
        .update_item(&UpdateItemRequest {
            table_name: "T".to_string(),
            key: item(&[("pk", json!({"S": "k"}))]),
            update_expression: "SET a = :v".to_string(),
            expression_attribute_values: vals(&[(":v", json!({"N": "1"}))]),
            return_values: "BOGUS".to_string(),
            ..Default::default()
        })
        .expect_err("bad return");
    assert_eq!(
        err.message,
        "return values must be NONE, ALL_OLD, UPDATED_OLD, ALL_NEW, or UPDATED_NEW"
    );
}

#[test]
fn update_condition_failure() {
    let dir = tempdir();
    let mut s = server(&dir);
    create_t(&mut s);
    put(
        &mut s,
        item(&[("pk", json!({"S": "k"})), ("n", json!({"N": "1"}))]),
    );
    let err = s
        .update_item(&UpdateItemRequest {
            table_name: "T".to_string(),
            key: item(&[("pk", json!({"S": "k"}))]),
            condition_expression: "attribute_not_exists(pk)".to_string(),
            update_expression: "SET n = :v".to_string(),
            expression_attribute_values: vals(&[(":v", json!({"N": "2"}))]),
            ..Default::default()
        })
        .expect_err("cond");
    assert_eq!(err.name, "ConditionalCheckFailedException");
}

// --- minimal tempdir -------------------------------------------------------

fn tempdir() -> std::path::PathBuf {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let mut dir = std::env::temp_dir();
    dir.push(format!("devcloud-ddb-upd-{}-{}", std::process::id(), n));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create tempdir");
    dir
}
