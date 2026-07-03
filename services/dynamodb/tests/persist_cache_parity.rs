//! Regression tests for `Server::persist`'s per-table dirty-tracking cache
//! (PERF-3): only tables mutated since the last successful persist are
//! re-encoded, every other table reuses its cached encoded bytes, and a
//! failed persist must never leave a stale cache/dirty entry that later
//! writes the wrong (un-reverted) data to disk.

use devcloud_dynamodb::model::{AttributeDefinition, Item, KeySchemaElement};
use devcloud_dynamodb::persistence::PersistedState;
use devcloud_dynamodb::requests::{CreateTableRequest, GetItemRequest, PutItemRequest};
use devcloud_dynamodb::server::{Config, Server};
use serde_json::json;

const ORACLE_NOW: i64 = 1_780_050_000;

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

/// A single-attribute (`pk` S HASH) table, kept minimal since these tests
/// exercise the persistence cache, not table shape.
fn create_table(s: &mut Server, name: &str) {
    let req = CreateTableRequest {
        table_name: name.to_string(),
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
    };
    s.create_table(&req).expect("create table");
}

fn key(pk: &str) -> Item {
    let mut m = Item::new();
    m.insert("pk".to_string(), json!({"S": pk}));
    m
}

fn item(pk: &str, value: &str) -> Item {
    let mut m = key(pk);
    m.insert("val".to_string(), json!({"S": value}));
    m
}

fn put(
    s: &mut Server,
    table: &str,
    it: Item,
) -> Result<Vec<u8>, devcloud_dynamodb::errors::ApiError> {
    s.put_item(&PutItemRequest {
        table_name: table.to_string(),
        item: it,
        ..Default::default()
    })
}

fn get_val(s: &Server, table: &str, pk: &str) -> Option<String> {
    let body = s
        .get_item(&GetItemRequest {
            table_name: table.to_string(),
            key: key(pk),
            ..Default::default()
        })
        .expect("get");
    let value: serde_json::Value = serde_json::from_slice(&body).expect("valid json");
    value
        .get("Item")?
        .get("val")?
        .get("S")?
        .as_str()
        .map(str::to_string)
}

fn read_state(dir: &std::path::Path) -> PersistedState {
    let bytes = std::fs::read(dir.join("state.json")).expect("read state.json");
    PersistedState::from_slice(&bytes).expect("decode state.json")
}

/// mutate table A, persist, mutate table B, persist → state.json must
/// contain both tables' latest data (B's persist must not lose or stale-cache
/// A's already-committed content, and vice versa).
#[test]
fn dirty_tracking_persists_multiple_tables_independently() {
    let dir = tempdir();
    let mut s = server(&dir);
    create_table(&mut s, "A");
    create_table(&mut s, "B");

    put(&mut s, "A", item("a1", "from-a")).expect("put a1");
    put(&mut s, "B", item("b1", "from-b")).expect("put b1");

    let state = read_state(&dir);
    assert_eq!(state.tables.len(), 2);
    let a_values: Vec<_> = state.tables["A"]
        .items
        .values()
        .map(|it| &it["val"])
        .collect();
    let b_values: Vec<_> = state.tables["B"]
        .items
        .values()
        .map(|it| &it["val"])
        .collect();
    assert_eq!(a_values, vec![&json!({"S": "from-a"})]);
    assert_eq!(b_values, vec![&json!({"S": "from-b"})]);

    // A second, unrelated write to A (persisted via the now-warm cache for B)
    // must not disturb B's already-persisted state.
    put(&mut s, "A", item("a2", "from-a-2")).expect("put a2");
    let state = read_state(&dir);
    let a_values: Vec<_> = state.tables["A"]
        .items
        .values()
        .map(|it| &it["val"])
        .collect();
    let b_values: Vec<_> = state.tables["B"]
        .items
        .values()
        .map(|it| &it["val"])
        .collect();
    assert_eq!(
        a_values,
        vec![&json!({"S": "from-a"}), &json!({"S": "from-a-2"})]
    );
    assert_eq!(
        b_values,
        vec![&json!({"S": "from-b"})],
        "B's cached bytes must be reused untouched while only A is dirty"
    );
}

/// persist-failure rollback → subsequent persist writes pre-mutation data:
/// a mutation whose persist fails must never leave the per-table cache (or
/// dirty flag) in a state that lets a later, unrelated persist write the
/// failed mutation's (rolled-back) data to disk.
#[cfg(unix)]
#[test]
fn persist_failure_rollback_then_next_persist_writes_pre_mutation_state() {
    use std::os::unix::fs::PermissionsExt;

    let dir = tempdir();
    let mut s = server(&dir);
    create_table(&mut s, "A");
    put(&mut s, "A", item("k", "before")).expect("initial put");

    // Force the next persist to fail: creating a new `state.json.tmp` entry
    // requires write permission on the directory itself.
    std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o555))
        .expect("make dir read-only");

    let err = put(&mut s, "A", item("k", "after-failed"))
        .expect_err("persist must fail when the directory is read-only");
    assert_eq!(err.name, "InternalServerError");

    // Caller-side rollback (pre-existing behavior) must already have
    // restored the in-memory item.
    assert_eq!(get_val(&s, "A", "k").as_deref(), Some("before"));

    // Restore permissions and drive a real persist via an unrelated write to
    // the same table, so the fix under test — not leaving a stale cache/dirty
    // entry from the failed attempt — is what's exercised.
    std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o755))
        .expect("restore dir permissions");
    put(&mut s, "A", item("k2", "unrelated")).expect("put after recovery");

    let after = read_state(&dir);
    let values: Vec<_> = after.tables["A"]
        .items
        .values()
        .map(|it| &it["val"])
        .collect();
    assert!(
        values.contains(&&json!({"S": "before"})),
        "state.json must still hold the pre-mutation value for the item touched by the failed persist, got {values:?}"
    );
    assert!(
        !values.contains(&&json!({"S": "after-failed"})),
        "state.json must never contain data from a mutation whose persist failed, got {values:?}"
    );
    assert!(values.contains(&&json!({"S": "unrelated"})));
}

// --- minimal tempdir (no external crate) -----------------------------------

fn tempdir() -> std::path::PathBuf {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let mut dir = std::env::temp_dir();
    dir.push(format!(
        "devcloud-ddb-persist-cache-test-{}-{}",
        std::process::id(),
        n
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create tempdir");
    dir
}
