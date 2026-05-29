//! Differential-parity tests for the table-management operations against golden
//! oracles captured from the Go service (`internal/services/dynamodb`).

use devcloud_dynamodb::go_json;
use devcloud_dynamodb::requests::{
    CreateTableRequest, GlobalSecondaryIndexRequest, IndexProjectionRequest, ListTablesRequest,
    LocalSecondaryIndexRequest, UpdateTableRequest,
};
use devcloud_dynamodb::server::{Config, Server};
use serde_json::Value;

/// Parses an operation's encoded body back into a `Value` for field assertions.
fn parse(body: &[u8]) -> Value {
    serde_json::from_slice(body).expect("valid JSON body")
}

/// `CreationDateTime` baked into the captured oracles.
const ORACLE_NOW: i64 = 1_780_039_784;

fn attr_def(name: &str, ty: &str) -> devcloud_dynamodb::model::AttributeDefinition {
    devcloud_dynamodb::model::AttributeDefinition {
        attribute_name: name.to_string(),
        attribute_type: ty.to_string(),
    }
}

fn key(name: &str, kt: &str) -> devcloud_dynamodb::model::KeySchemaElement {
    devcloud_dynamodb::model::KeySchemaElement {
        attribute_name: name.to_string(),
        key_type: kt.to_string(),
    }
}

/// Builds the same `Orders` table the Go oracle test created.
fn orders_request() -> CreateTableRequest {
    CreateTableRequest {
        table_name: "Orders".to_string(),
        attribute_definitions: vec![
            attr_def("pk", "S"),
            attr_def("sk", "N"),
            attr_def("gsiPk", "S"),
        ],
        key_schema: vec![key("pk", "HASH"), key("sk", "RANGE")],
        global_secondary_indexes: vec![GlobalSecondaryIndexRequest {
            index_name: "byGsi".to_string(),
            key_schema: vec![key("gsiPk", "HASH")],
            projection: IndexProjectionRequest {
                projection_type: "INCLUDE".to_string(),
                non_key_attributes: vec!["a".to_string(), "b".to_string()],
            },
        }],
        local_secondary_indexes: vec![LocalSecondaryIndexRequest {
            index_name: "byLsi".to_string(),
            key_schema: vec![key("pk", "HASH"), key("sk", "RANGE")],
            projection: IndexProjectionRequest {
                projection_type: "KEYS_ONLY".to_string(),
                non_key_attributes: vec![],
            },
        }],
        billing_mode: "PROVISIONED".to_string(),
        stream_specification: Default::default(),
    }
}

fn server_with_storage(dir: &std::path::Path) -> Server {
    let mut s = Server::new(Config {
        region: "us-east-1".to_string(),
        auth_mode: "relaxed".to_string(),
        storage_path: dir.to_string_lossy().to_string(),
        ..Default::default()
    });
    s.set_fixed_now(ORACLE_NOW);
    s
}

#[test]
fn create_table_response_matches_go_oracle() {
    let dir = tempdir();
    let mut s = server_with_storage(&dir);
    let got = s.create_table(&orders_request()).expect("create");
    let want = include_bytes!("fixtures/create_table_resp.json").to_vec();
    assert_eq!(
        String::from_utf8_lossy(&got),
        String::from_utf8_lossy(&want),
        "CreateTable response body must match the Go oracle byte-for-byte"
    );
}

#[test]
fn create_table_persists_byte_compatible_state() {
    let dir = tempdir();
    let mut s = server_with_storage(&dir);
    s.create_table(&orders_request()).expect("create");
    let on_disk = std::fs::read(dir.join("state.json")).expect("read state.json");
    let want = include_bytes!("fixtures/state_table.json").to_vec();
    assert_eq!(
        String::from_utf8_lossy(&on_disk),
        String::from_utf8_lossy(&want),
        "state.json must match the Go oracle byte-for-byte"
    );
}

#[test]
fn duplicate_create_table_is_resource_in_use() {
    let dir = tempdir();
    let mut s = server_with_storage(&dir);
    s.create_table(&orders_request()).expect("first create");
    let err = s.create_table(&orders_request()).expect_err("dup");
    assert_eq!(err.status, 400);
    assert_eq!(err.name, "ResourceInUseException");
    assert_eq!(err.message, "table already exists");
    let body = go_json::to_vec(&err.body());
    assert_eq!(
        String::from_utf8_lossy(&body),
        "{\"__type\":\"com.amazonaws.dynamodb.v20120810#ResourceInUseException\",\"message\":\"table already exists\"}\n"
    );
}

#[test]
fn describe_and_delete_table() {
    let dir = tempdir();
    let mut s = server_with_storage(&dir);
    s.create_table(&orders_request()).expect("create");

    let described = parse(&s.describe_table("Orders").expect("describe"));
    assert_eq!(described["Table"]["TableName"], "Orders");

    assert_eq!(
        s.describe_table("Missing").expect_err("missing").name,
        "ResourceNotFoundException"
    );

    let deleted = parse(&s.delete_table("Orders").expect("delete"));
    assert_eq!(deleted["TableDescription"]["TableName"], "Orders");
    assert_eq!(
        s.describe_table("Orders").expect_err("gone").name,
        "ResourceNotFoundException"
    );
    // state.json now holds zero tables.
    let on_disk = std::fs::read(dir.join("state.json")).expect("read");
    assert_eq!(String::from_utf8_lossy(&on_disk), "{\"tables\":{}}\n");
}

#[test]
fn list_tables_sorts_and_paginates() {
    let dir = tempdir();
    let mut s = server_with_storage(&dir);
    for name in ["c", "a", "b"] {
        let mut req = orders_request();
        req.table_name = name.to_string();
        s.create_table(&req).expect("create");
    }
    // No limit: all three, sorted.
    let all = parse(&s.list_tables(&ListTablesRequest::default()).expect("list"));
    assert_eq!(all["TableNames"], serde_json::json!(["a", "b", "c"]));
    assert!(all.get("LastEvaluatedTableName").is_none());

    // Limit 2: first page + LastEvaluatedTableName.
    let page = parse(
        &s.list_tables(&ListTablesRequest {
            exclusive_start_table_name: String::new(),
            limit: 2,
        })
        .expect("page"),
    );
    assert_eq!(page["TableNames"], serde_json::json!(["a", "b"]));
    assert_eq!(page["LastEvaluatedTableName"], "b");

    // Continue from "b".
    let rest = parse(
        &s.list_tables(&ListTablesRequest {
            exclusive_start_table_name: "b".to_string(),
            limit: 2,
        })
        .expect("rest"),
    );
    assert_eq!(rest["TableNames"], serde_json::json!(["c"]));
}

#[test]
fn list_tables_rejects_out_of_range_limit() {
    let dir = tempdir();
    let s = server_with_storage(&dir);
    let err = s
        .list_tables(&ListTablesRequest {
            exclusive_start_table_name: String::new(),
            limit: 101,
        })
        .expect_err("limit");
    assert_eq!(err.name, "ValidationException");
    assert_eq!(err.message, "limit must be between 1 and 100");
}

#[test]
fn create_table_validation_messages_match_go() {
    let dir = tempdir();
    let mut s = server_with_storage(&dir);

    let mut req = orders_request();
    req.table_name = String::new();
    assert_eq!(
        s.create_table(&req).expect_err("noname").message,
        "table name is required"
    );

    let mut req = orders_request();
    req.attribute_definitions.push(attr_def("bad", "X"));
    assert_eq!(
        s.create_table(&req).expect_err("badtype").message,
        "attribute type must be S, N, or B"
    );

    let mut req = orders_request();
    req.key_schema = vec![key("missing", "HASH")];
    assert_eq!(
        s.create_table(&req).expect_err("undef").message,
        "key schema attributes must be defined"
    );
}

#[test]
fn update_table_billing_mode_and_gsi_delete() {
    let dir = tempdir();
    let mut s = server_with_storage(&dir);
    s.create_table(&orders_request()).expect("create");

    // Switch billing mode.
    let updated = parse(
        &s.update_table(&UpdateTableRequest {
            table_name: "Orders".to_string(),
            billing_mode: "PAY_PER_REQUEST".to_string(),
            ..Default::default()
        })
        .expect("update billing"),
    );
    assert_eq!(
        updated["TableDescription"]["BillingModeSummary"]["BillingMode"],
        "PAY_PER_REQUEST"
    );

    // Delete the GSI.
    let deleted = parse(
        &s.update_table(&UpdateTableRequest {
            table_name: "Orders".to_string(),
            global_secondary_index_updates: vec![
                devcloud_dynamodb::requests::GlobalSecondaryIndexUpdate {
                    delete: Some(devcloud_dynamodb::requests::DeleteGlobalSecondaryIndex {
                        index_name: "byGsi".to_string(),
                    }),
                    ..Default::default()
                },
            ],
            ..Default::default()
        })
        .expect("delete gsi"),
    );
    let gsis = deleted["TableDescription"]["GlobalSecondaryIndexes"].as_array();
    assert!(gsis.is_none() || gsis.unwrap().is_empty());
}

#[test]
fn update_table_throughput_update_is_unsupported() {
    let dir = tempdir();
    let mut s = server_with_storage(&dir);
    s.create_table(&orders_request()).expect("create");
    let err = s
        .update_table(&UpdateTableRequest {
            table_name: "Orders".to_string(),
            global_secondary_index_updates: vec![
                devcloud_dynamodb::requests::GlobalSecondaryIndexUpdate {
                    update: Some(devcloud_dynamodb::requests::UpdateGlobalSecondaryIndex {
                        index_name: "byGsi".to_string(),
                    }),
                    ..Default::default()
                },
            ],
            ..Default::default()
        })
        .expect_err("update");
    assert_eq!(
        err.message,
        "global secondary index throughput updates are not supported"
    );
}

#[test]
fn state_reloads_across_server_instances() {
    let dir = tempdir();
    {
        let mut s = server_with_storage(&dir);
        s.create_table(&orders_request()).expect("create");
    }
    let s2 = server_with_storage(&dir);
    let reloaded = parse(&s2.describe_table("Orders").expect("reload"));
    assert_eq!(reloaded["Table"]["TableName"], "Orders");
}

#[test]
fn describe_limits_and_endpoints_are_static() {
    let dir = tempdir();
    let s = server_with_storage(&dir);
    let limits = parse(&s.describe_limits());
    assert_eq!(limits["AccountMaxReadCapacityUnits"], 80000);
    let endpoints = parse(&s.describe_endpoints());
    assert_eq!(endpoints["Endpoints"][0]["CachePeriodInMinutes"], 1440);
}

// --- minimal tempdir (no external crate) -----------------------------------

fn tempdir() -> std::path::PathBuf {
    // Unique-enough within a single test binary: PID + an atomic counter. No
    // Date/random APIs needed.
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let mut dir = std::env::temp_dir();
    dir.push(format!("devcloud-ddb-test-{}-{}", std::process::id(), n));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create tempdir");
    dir
}
