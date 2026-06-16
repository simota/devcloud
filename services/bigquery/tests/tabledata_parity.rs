//! 1:1 port of `internal/services/bigquery/tabledata_test.rs`.
//!
//! The legacy tests drive `server.routes().ServeHTTP`; the routing layer arrives
//! in part 4, so these call the insertAll/list handlers with the
//! already-routed parameters — same scenarios, same expected statuses and
//! JSON. The legacy `TableSnapshot` assertion (dashboard.rs, part 4) is checked
//! through the storage layer the snapshot reads from.

use devcloud_bigquery::model::{InsertAllResponse, TableDataListResponse, TableResource};
use devcloud_bigquery::server::{Config, Server};
use devcloud_bigquery::validation::Query;
use serde_json::json;

fn new_server(dir: &std::path::Path) -> Server {
    Server::new(Config {
        project: "local-project".to_string(),
        storage_path: dir.to_string_lossy().into_owned(),
        ..Default::default()
    })
}

fn create_dataset_for_test(server: &Server, project_id: &str, dataset_id: &str) {
    let body = format!("{{\"datasetReference\":{{\"datasetId\":\"{dataset_id}\"}}}}");
    let response = server.create_dataset(project_id, body.as_bytes());
    assert_eq!(
        response.status,
        200,
        "create dataset status = {}, body = {}",
        response.status,
        response.body_str()
    );
}

/// legacy `createTableForTest` (server_test.rs).
fn create_table_for_test(server: &Server, project_id: &str, dataset_id: &str, table_id: &str) {
    let body = format!(
        "{{\"tableReference\":{{\"tableId\":\"{table_id}\"}},\"schema\":{{\"fields\":[{{\"name\":\"id\",\"type\":\"STRING\",\"mode\":\"REQUIRED\"}},{{\"name\":\"name\",\"type\":\"STRING\"}},{{\"name\":\"age\",\"type\":\"INTEGER\"}},{{\"name\":\"active\",\"type\":\"BOOLEAN\"}}]}}}}"
    );
    let response = server.create_table(project_id, dataset_id, body.as_bytes());
    assert_eq!(
        response.status,
        200,
        "create table status = {}, body = {}",
        response.status,
        response.body_str()
    );
}

// legacy: TestTableDataInsertAllPersistsRowsAndListReturnsBigQueryShape
#[test]
fn tabledata_insert_all_persists_rows_and_list_returns_bigquery_shape() {
    let dir = tempdir();
    let server = new_server(&dir);
    create_dataset_for_test(&server, "local-project", "analytics");
    create_table_for_test(&server, "local-project", "analytics", "people");

    let insert_body = r#"{"rows":[{"insertId":"row-1","json":{"id":"1","name":"Ada","age":37,"active":true}},{"insertId":"row-2","json":{"id":"2","name":"Grace","age":"31","active":false}}]}"#;
    let insert = server.insert_rows(
        "local-project",
        "analytics",
        "people",
        insert_body.as_bytes(),
    );
    assert_eq!(
        insert.status,
        200,
        "insert status = {}, body = {}",
        insert.status,
        insert.body_str()
    );
    let insert_response: InsertAllResponse =
        serde_json::from_slice(&insert.body).expect("decode insert response");
    assert!(
        insert_response.kind == "bigquery#tableDataInsertAllResponse"
            && insert_response.insert_errors.is_empty(),
        "insert response = {insert_response:?}"
    );

    let list = server.list_rows(
        "local-project",
        "analytics",
        "people",
        &Query::parse("maxResults=1"),
    );
    assert_eq!(
        list.status,
        200,
        "list status = {}, body = {}",
        list.status,
        list.body_str()
    );
    let list_response: TableDataListResponse =
        serde_json::from_slice(&list.body).expect("decode list response");
    assert!(
        list_response.kind == "bigquery#tableDataList"
            && list_response.total_rows == "2"
            && list_response.page_token == "1"
            && list_response.rows.len() == 1,
        "list response = {list_response:?}"
    );
    assert_eq!(
        list_response.rows[0].f[1].v,
        json!("Ada"),
        "first row name = {:?}",
        list_response.rows[0].f[1].v
    );

    let next = server.list_rows(
        "local-project",
        "analytics",
        "people",
        &Query::parse("pageToken=1&selectedFields=name"),
    );
    assert_eq!(
        next.status,
        200,
        "next status = {}, body = {}",
        next.status,
        next.body_str()
    );
    let next_response: TableDataListResponse =
        serde_json::from_slice(&next.body).expect("decode next response");
    assert!(
        next_response.rows.len() == 1
            && next_response.rows[0].f.len() == 1
            && next_response.rows[0].f[0].v == json!("Grace"),
        "selected next response = {next_response:?}"
    );

    let table_rec = server.get_table("local-project", "analytics", "people");
    let table: TableResource = serde_json::from_slice(&table_rec.body).expect("decode table");
    assert!(
        table.num_rows == "2" && table.num_bytes != "0",
        "table stats = {table:?}"
    );
}

// legacy: TestTableDataInsertAllReturnsPartialErrorsAndHonorsSkipInvalidRows
#[test]
fn tabledata_insert_all_returns_partial_errors_and_honors_skip_invalid_rows() {
    let dir = tempdir();
    let server = new_server(&dir);
    create_dataset_for_test(&server, "local-project", "analytics");
    create_table_for_test(&server, "local-project", "analytics", "people");

    let insert_body = r#"{"skipInvalidRows":true,"ignoreUnknownValues":true,"rows":[{"insertId":"row-1","json":{"id":"1","name":"Ada","extra":"ignored"}},{"insertId":"row-2","json":{"name":"Missing ID","age":"old"}}]}"#;
    let insert = server.insert_rows(
        "local-project",
        "analytics",
        "people",
        insert_body.as_bytes(),
    );
    assert_eq!(
        insert.status,
        200,
        "insert status = {}, body = {}",
        insert.status,
        insert.body_str()
    );
    let response: InsertAllResponse =
        serde_json::from_slice(&insert.body).expect("decode response");
    assert!(
        response.insert_errors.len() == 1 && response.insert_errors[0].index == 1,
        "insert errors = {:?}",
        response.insert_errors
    );

    let list = server.list_rows("local-project", "analytics", "people", &Query::parse(""));
    let list_response: TableDataListResponse =
        serde_json::from_slice(&list.body).expect("decode list");
    assert!(
        list_response.total_rows == "1"
            && list_response.rows.len() == 1
            && list_response.rows[0].f[1].v == json!("Ada"),
        "list response after partial insert = {list_response:?}"
    );

    let blocked_body = r#"{"rows":[{"insertId":"row-3","json":{"id":"3","name":"Katherine"}},{"insertId":"row-4","json":{"name":"Invalid"}}]}"#;
    let blocked = server.insert_rows(
        "local-project",
        "analytics",
        "people",
        blocked_body.as_bytes(),
    );
    assert_eq!(
        blocked.status,
        200,
        "blocked status = {}, body = {}",
        blocked.status,
        blocked.body_str()
    );
    let after_blocked = server.list_rows("local-project", "analytics", "people", &Query::parse(""));
    let after_blocked_response: TableDataListResponse =
        serde_json::from_slice(&after_blocked.body).expect("decode after blocked");
    assert_eq!(
        after_blocked_response.total_rows, "1",
        "skipInvalidRows=false persisted valid rows from invalid request: {after_blocked_response:?}"
    );
}

// legacy: TestTableDataInsertAllBestEffortDeduplicatesInsertIDs
#[test]
fn tabledata_insert_all_best_effort_deduplicates_insert_ids() {
    let dir = tempdir();
    let server = new_server(&dir);
    create_dataset_for_test(&server, "local-project", "analytics");
    create_table_for_test(&server, "local-project", "analytics", "people");

    let first_body =
        r#"{"rows":[{"insertId":"row-1","json":{"id":"1","name":"Ada","age":37,"active":true}}]}"#;
    let first = server.insert_rows(
        "local-project",
        "analytics",
        "people",
        first_body.as_bytes(),
    );
    assert_eq!(
        first.status,
        200,
        "first insert status = {}, body = {}",
        first.status,
        first.body_str()
    );

    let duplicate_body = r#"{"rows":[
		{"insertId":"row-1","json":{"id":"1-duplicate","name":"Duplicate","age":99,"active":false}},
		{"insertId":"row-2","json":{"id":"2","name":"Grace","age":31,"active":true}},
		{"insertId":"row-2","json":{"id":"2-duplicate","name":"Duplicate Grace","age":32,"active":false}}
	]}"#;
    let duplicate = server.insert_rows(
        "local-project",
        "analytics",
        "people",
        duplicate_body.as_bytes(),
    );
    assert_eq!(
        duplicate.status,
        200,
        "duplicate insert status = {}, body = {}",
        duplicate.status,
        duplicate.body_str()
    );
    let insert_response: InsertAllResponse =
        serde_json::from_slice(&duplicate.body).expect("decode duplicate insert response");
    assert!(
        insert_response.insert_errors.is_empty(),
        "duplicate insert errors = {:?}",
        insert_response.insert_errors
    );

    let list = server.list_rows("local-project", "analytics", "people", &Query::parse(""));
    let list_response: TableDataListResponse =
        serde_json::from_slice(&list.body).expect("decode list");
    assert!(
        list_response.total_rows == "2" && list_response.rows.len() == 2,
        "deduplicated rows = {list_response:?}"
    );
    assert!(
        list_response.rows[0].f[1].v == json!("Ada")
            && list_response.rows[1].f[1].v == json!("Grace"),
        "duplicate insertId rows were persisted: {:?}",
        list_response.rows
    );
}

// legacy: TestTableDataInsertAllHonorsConfiguredRequestAndRowLimits
//
// The legacy test mutates server.config.MaxRequestBytes between requests; Server
// owns its config here, so each phase uses a server with the same storage
// path and the limit that phase expects.
#[test]
fn tabledata_insert_all_honors_configured_request_and_row_limits() {
    let dir = tempdir();
    let server_with = |max_request_bytes: i64| {
        Server::new(Config {
            project: "local-project".to_string(),
            storage_path: dir.to_string_lossy().into_owned(),
            max_rows_per_table: 1,
            max_request_bytes,
            ..Default::default()
        })
    };
    let setup = server_with(0);
    create_dataset_for_test(&setup, "local-project", "analytics");
    create_table_for_test(&setup, "local-project", "analytics", "people");

    let small = server_with(64);
    let too_large_body =
        r#"{"rows":[{"insertId":"row-1","json":{"id":"1","name":"Ada","age":37,"active":true}}]}"#;
    let too_large = small.insert_rows(
        "local-project",
        "analytics",
        "people",
        too_large_body.as_bytes(),
    );
    assert_eq!(
        too_large.status,
        400,
        "large request status = {}, body = {}",
        too_large.status,
        too_large.body_str()
    );
    assert!(
        !too_large.body_str().contains("Ada"),
        "large request error leaked row payload: {}",
        too_large.body_str()
    );

    let server = server_with(1024);
    let first_body =
        r#"{"rows":[{"insertId":"row-1","json":{"id":"1","name":"Ada","age":37,"active":true}}]}"#;
    let first = server.insert_rows(
        "local-project",
        "analytics",
        "people",
        first_body.as_bytes(),
    );
    assert_eq!(
        first.status,
        200,
        "first insert status = {}, body = {}",
        first.status,
        first.body_str()
    );

    let over_limit_body = r#"{"rows":[{"insertId":"row-2","json":{"id":"2","name":"Grace","age":31,"active":false}}]}"#;
    let over_limit = server.insert_rows(
        "local-project",
        "analytics",
        "people",
        over_limit_body.as_bytes(),
    );
    assert_eq!(
        over_limit.status,
        400,
        "row limit status = {}, body = {}",
        over_limit.status,
        over_limit.body_str()
    );
    assert!(
        !over_limit.body_str().contains("Grace"),
        "row limit error leaked row payload: {}",
        over_limit.body_str()
    );

    let list = server.list_rows("local-project", "analytics", "people", &Query::parse(""));
    let list_response: TableDataListResponse =
        serde_json::from_slice(&list.body).expect("decode list");
    assert!(
        list_response.total_rows == "1"
            && list_response.rows.len() == 1
            && list_response.rows[0].f[1].v == json!("Ada"),
        "rows after limit rejection = {list_response:?}"
    );
}

// legacy: TestTableDataInsertAllValidatesNestedRepeatedAndFloatFields
#[test]
fn tabledata_insert_all_validates_nested_repeated_and_float_fields() {
    let dir = tempdir();
    let server = new_server(&dir);
    create_dataset_for_test(&server, "local-project", "analytics");
    let body = r#"{"tableReference":{"tableId":"metrics"},"schema":{"fields":[{"name":"id","type":"STRING","mode":"REQUIRED"},{"name":"score","type":"FLOAT"},{"name":"tags","type":"STRING","mode":"REPEATED"},{"name":"meta","type":"RECORD","fields":[{"name":"active","type":"BOOLEAN","mode":"REQUIRED"}]},{"name":"payload","type":"JSON"}]}}"#;
    let rec = server.create_table("local-project", "analytics", body.as_bytes());
    assert_eq!(
        rec.status,
        200,
        "create metrics table status = {}, body = {}",
        rec.status,
        rec.body_str()
    );

    let insert_body = r#"{"skipInvalidRows":true,"rows":[{"insertId":"ok","json":{"id":"1","score":"12.5","tags":["red","blue"],"meta":{"active":true},"payload":{"nested":1}}},{"insertId":"bad","json":{"id":"2","score":{"bad":true},"tags":"red","meta":{"active":"yes"}}}]}"#;
    let insert = server.insert_rows(
        "local-project",
        "analytics",
        "metrics",
        insert_body.as_bytes(),
    );
    assert_eq!(
        insert.status,
        200,
        "insert metrics rows status = {}, body = {}",
        insert.status,
        insert.body_str()
    );
    let response: InsertAllResponse =
        serde_json::from_slice(&insert.body).expect("decode insert response");
    assert!(
        response.insert_errors.len() == 1 && response.insert_errors[0].errors.len() == 3,
        "insert errors = {:?}",
        response.insert_errors
    );
    // legacy asserts via server.TableSnapshot (dashboard seam, part 4):
    // 1 persisted row whose decoded JSON["score"] is "12.5".
    let rows = server
        .read_rows("local-project", "analytics", "metrics")
        .expect("read rows");
    assert!(
        rows.len() == 1 && rows[0].json.get("score").map(|raw| raw.get()) == Some("\"12.5\""),
        "metrics table rows = {rows:?}"
    );
}

// --- minimal tempdir --------------------------------------------------------

fn tempdir() -> std::path::PathBuf {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let mut dir = std::env::temp_dir();
    dir.push(format!("devcloud-bq-td-{}-{}", std::process::id(), n));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create tempdir");
    dir
}
