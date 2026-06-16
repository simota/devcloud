//! 1:1 port of `internal/services/bigquery/query_sql_test.rs`.
//!
//! The legacy tests drive the `/queries` endpoint through `server.routes()`; the
//! HTTP layer and the table/tabledata handlers arrive in parts 3–4, so these
//! call `Server::create_query_job` with the exact arguments the legacy `queryRows`
//! handler passes (`maxResults` 0, `includeConfiguration` false, dry-run
//! false, effective legacy SQL false) and seed the same fixtures through the
//! storage layer that `createTableForTest`/`insertRowsForTest` produce over
//! HTTP. Assertions compare the same response fields; cell values are
//! compared as JSON values (the legacy tests decode the response body, so its
//! `"2"` / `float64(31)` expectations are the decoded forms of the same JSON).

use std::collections::BTreeMap;

use devcloud_bigquery::model::{
    JobReference, QueryJobConfiguration, QueryJobRecord, RawJson, StoredRow, TableFieldSchema,
    TableReference, TableResource, TableSchema,
};
use devcloud_bigquery::server::{Config, Server};
use serde_json::json;

fn new_server(dir: &std::path::Path) -> Server {
    Server::new(Config {
        project: "local-project".to_string(),
        location: "US".to_string(),
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

/// legacy `createTableForTest` writes the table over HTTP; the table handlers are
/// part 3, so the equivalent resource goes straight through the storage layer.
fn create_table_for_test(server: &Server, project_id: &str, dataset_id: &str, table_id: &str) {
    let field = |name: &str, field_type: &str, mode: &str| TableFieldSchema {
        name: name.to_string(),
        field_type: field_type.to_string(),
        mode: mode.to_string(),
        ..Default::default()
    };
    let table = TableResource {
        kind: "bigquery#table".to_string(),
        id: format!("{project_id}:{dataset_id}.{table_id}"),
        table_reference: TableReference {
            project_id: project_id.to_string(),
            dataset_id: dataset_id.to_string(),
            table_id: table_id.to_string(),
        },
        table_type: "TABLE".to_string(),
        schema: TableSchema {
            fields: vec![
                field("id", "STRING", "REQUIRED"),
                field("name", "STRING", ""),
                field("age", "INTEGER", ""),
                field("active", "BOOLEAN", ""),
            ],
        },
        ..Default::default()
    };
    server.write_table(&table).expect("write table");
}

fn raw(value: &str) -> RawJson {
    serde_json::value::RawValue::from_string(value.to_string()).expect("valid raw json")
}

fn stored_row(insert_id: &str, values: &[(&str, &str)]) -> StoredRow {
    let mut json: BTreeMap<String, RawJson> = BTreeMap::new();
    for (key, value) in values {
        json.insert((*key).to_string(), raw(value));
    }
    StoredRow {
        insert_id: insert_id.to_string(),
        json,
        inserted_at: String::new(),
    }
}

/// legacy `insertRowsForTest` (insertAll over HTTP) stores exactly these raw
/// values in the streaming buffer.
fn insert_rows_for_test(server: &Server, project_id: &str, dataset_id: &str, table_id: &str) {
    server
        .append_rows(
            project_id,
            dataset_id,
            table_id,
            &[
                stored_row(
                    "row-1",
                    &[
                        ("id", "\"1\""),
                        ("name", "\"Ada\""),
                        ("age", "37"),
                        ("active", "true"),
                    ],
                ),
                stored_row(
                    "row-2",
                    &[
                        ("id", "\"2\""),
                        ("name", "\"Grace\""),
                        ("age", "31"),
                        ("active", "true"),
                    ],
                ),
            ],
        )
        .expect("insert rows");
}

/// The legacy tests POST `{"query": ..., "useLegacySql": false}` to `/queries`;
/// the handler calls `createQueryJob(project, ref{}, config{Query}, 0, false,
/// false, false)` and returns `job.Response`.
fn run_query(server: &Server, query: &str) -> Result<QueryJobRecord, String> {
    server.create_query_job(
        "local-project",
        &JobReference::default(),
        QueryJobConfiguration {
            query: query.to_string(),
            use_legacy_sql: Some(false),
            ..Default::default()
        },
        0,
        false,
        false,
        false,
    )
}

// legacy: TestJobsQuerySupportsLimitOffset
#[test]
fn jobs_query_supports_limit_offset() {
    let dir = tempdir();
    let server = new_server(&dir);
    create_dataset_for_test(&server, "local-project", "analytics");
    create_table_for_test(&server, "local-project", "analytics", "people");
    insert_rows_for_test(&server, "local-project", "analytics", "people");

    let response = run_query(
        &server,
        "SELECT id, name FROM `local-project.analytics.people` ORDER BY id LIMIT 1 OFFSET 1",
    )
    .expect("query")
    .response;
    assert!(
        response.total_rows == "1" && response.rows.len() == 1,
        "response rows = {response:?}"
    );
    assert!(
        response.rows[0].f[0].v == json!("2") && response.rows[0].f[1].v == json!("Grace"),
        "offset row = {:?}",
        response.rows[0]
    );
}

// legacy: TestJobsQuerySupportsOrderByDesc
#[test]
fn jobs_query_supports_order_by_desc() {
    let dir = tempdir();
    let server = new_server(&dir);
    create_dataset_for_test(&server, "local-project", "analytics");
    create_table_for_test(&server, "local-project", "analytics", "people");
    insert_rows_for_test(&server, "local-project", "analytics", "people");

    let response = run_query(
        &server,
        "SELECT id, age FROM `local-project.analytics.people` ORDER BY age DESC",
    )
    .expect("query")
    .response;
    assert!(
        response.total_rows == "2" && response.rows.len() == 2,
        "response rows = {response:?}"
    );
    assert!(
        response.rows[0].f[0].v == json!("1") && response.rows[0].f[1].v == json!("37"),
        "desc first row = {:?}",
        response.rows[0]
    );
    assert!(
        response.rows[1].f[0].v == json!("2") && response.rows[1].f[1].v == json!("31"),
        "desc second row = {:?}",
        response.rows[1]
    );
}

// legacy: TestJobsQuerySupportsWhereAndComparisons
#[test]
fn jobs_query_supports_where_and_comparisons() {
    let dir = tempdir();
    let server = new_server(&dir);
    create_dataset_for_test(&server, "local-project", "analytics");
    create_table_for_test(&server, "local-project", "analytics", "people");
    insert_rows_for_test(&server, "local-project", "analytics", "people");

    let response = run_query(
        &server,
        "SELECT id, name FROM `local-project.analytics.people` WHERE age >= 30 AND active = true AND id != '2'",
    )
    .expect("query")
    .response;
    assert!(
        response.total_rows == "1" && response.rows.len() == 1,
        "response rows = {response:?}"
    );
    assert!(
        response.rows[0].f[0].v == json!("1") && response.rows[0].f[1].v == json!("Ada"),
        "filtered row = {:?}",
        response.rows[0]
    );
}

// legacy: TestJobsQuerySupportsOrWhere
#[test]
fn jobs_query_supports_or_where() {
    let dir = tempdir();
    let server = new_server(&dir);
    create_dataset_for_test(&server, "local-project", "analytics");
    create_table_for_test(&server, "local-project", "analytics", "people");
    insert_rows_for_test(&server, "local-project", "analytics", "people");

    let response = run_query(
        &server,
        "SELECT id, name FROM `local-project.analytics.people` WHERE name = 'Ada' OR age < 35 ORDER BY id",
    )
    .expect("query")
    .response;
    assert!(
        response.total_rows == "2" && response.rows.len() == 2,
        "response rows = {response:?}"
    );
    assert!(
        response.rows[0].f[0].v == json!("1") && response.rows[0].f[1].v == json!("Ada"),
        "first OR row = {:?}",
        response.rows[0]
    );
    assert!(
        response.rows[1].f[0].v == json!("2") && response.rows[1].f[1].v == json!("Grace"),
        "second OR row = {:?}",
        response.rows[1]
    );
}

// legacy: TestJobsQuerySupportsNotWhere
#[test]
fn jobs_query_supports_not_where() {
    let dir = tempdir();
    let server = new_server(&dir);
    create_dataset_for_test(&server, "local-project", "analytics");
    create_table_for_test(&server, "local-project", "analytics", "people");
    insert_rows_for_test(&server, "local-project", "analytics", "people");

    let response = run_query(
        &server,
        "SELECT id, name FROM `local-project.analytics.people` WHERE NOT name = 'Grace' AND NOT age < 35",
    )
    .expect("query")
    .response;
    assert!(
        response.total_rows == "1" && response.rows.len() == 1,
        "response rows = {response:?}"
    );
    assert!(
        response.rows[0].f[0].v == json!("1") && response.rows[0].f[1].v == json!("Ada"),
        "NOT filtered row = {:?}",
        response.rows[0]
    );
}

// legacy: TestJobsQueryRejectsMalformedNotWhereWithoutLeakingQueryText
#[test]
fn jobs_query_rejects_malformed_not_where_without_leaking_query_text() {
    let dir = tempdir();
    let server = new_server(&dir);
    create_dataset_for_test(&server, "local-project", "analytics");
    create_table_for_test(&server, "local-project", "analytics", "people");

    let err = run_query(
        &server,
        "SELECT id FROM `local-project.analytics.people` WHERE NOT secret_value",
    )
    .expect_err("malformed NOT WHERE must fail");
    assert!(
        !err.contains("secret_value") && !err.contains("local-project.analytics.people"),
        "query error leaked query text: {err}"
    );
}

// legacy: TestJobsQueryRejectsMalformedOrWhereWithoutLeakingQueryText
#[test]
fn jobs_query_rejects_malformed_or_where_without_leaking_query_text() {
    let dir = tempdir();
    let server = new_server(&dir);
    create_dataset_for_test(&server, "local-project", "analytics");
    create_table_for_test(&server, "local-project", "analytics", "people");

    let err = run_query(
        &server,
        "SELECT id FROM `local-project.analytics.people` WHERE name = 'secret-value' OR",
    )
    .expect_err("malformed OR WHERE must fail");
    assert!(
        !err.contains("secret-value") && !err.contains("local-project.analytics.people"),
        "query error leaked query text: {err}"
    );
}

// legacy: TestJobsQuerySupportsCountAggregate
#[test]
fn jobs_query_supports_count_aggregate() {
    let dir = tempdir();
    let server = new_server(&dir);
    create_dataset_for_test(&server, "local-project", "analytics");
    create_table_for_test(&server, "local-project", "analytics", "people");
    insert_rows_for_test(&server, "local-project", "analytics", "people");

    let response = run_query(
        &server,
        "SELECT COUNT(*) AS total FROM `local-project.analytics.people` WHERE active = true",
    )
    .expect("query")
    .response;
    assert!(
        response.total_rows == "1" && response.rows.len() == 1,
        "response rows = {response:?}"
    );
    assert!(
        response.schema.fields.len() == 1
            && response.schema.fields[0].name == "total"
            && response.schema.fields[0].field_type == "INTEGER",
        "schema = {:?}",
        response.schema
    );
    assert!(
        response.rows[0].f[0].v == json!("2"),
        "count value = {:?}",
        response.rows[0].f[0].v
    );
}

// legacy: TestJobsQuerySupportsCountFieldAggregate
#[test]
fn jobs_query_supports_count_field_aggregate() {
    let dir = tempdir();
    let server = new_server(&dir);
    create_dataset_for_test(&server, "local-project", "analytics");
    create_table_for_test(&server, "local-project", "analytics", "people");
    insert_rows_for_test(&server, "local-project", "analytics", "people");
    // legacy inserts {"id":"3","name":"No Age","active":true} via insertAll.
    server
        .append_rows(
            "local-project",
            "analytics",
            "people",
            &[stored_row(
                "row-3",
                &[("id", "\"3\""), ("name", "\"No Age\""), ("active", "true")],
            )],
        )
        .expect("insert row-3");

    let response = run_query(
        &server,
        "SELECT COUNT(age) FROM `local-project.analytics.people`",
    )
    .expect("query")
    .response;
    assert!(
        response.schema.fields.len() == 1 && response.schema.fields[0].name == "f0_",
        "schema = {:?}",
        response.schema
    );
    assert!(
        response.rows.len() == 1 && response.rows[0].f[0].v == json!("2"),
        "count field response = {response:?}"
    );
}

// legacy: TestJobsQuerySupportsNumericAggregates
#[test]
fn jobs_query_supports_numeric_aggregates() {
    let dir = tempdir();
    let server = new_server(&dir);
    create_dataset_for_test(&server, "local-project", "analytics");
    create_table_for_test(&server, "local-project", "analytics", "people");
    insert_rows_for_test(&server, "local-project", "analytics", "people");

    // (name, selector, field_name, field_type, value). The legacy test decodes the
    // HTTP body, so its "68"/float64(31) expectations are the decoded forms of
    // the JSON string/number cells asserted here.
    let tests: [(&str, &str, &str, &str, serde_json::Value); 4] = [
        (
            "sum",
            "SUM(age) AS total_age",
            "total_age",
            "INTEGER",
            json!("68"),
        ),
        (
            "avg",
            "AVG(age) AS average_age",
            "average_age",
            "FLOAT",
            json!("34"),
        ),
        (
            "min",
            "MIN(age) AS youngest",
            "youngest",
            "INTEGER",
            json!(31),
        ),
        ("max", "MAX(age) AS oldest", "oldest", "INTEGER", json!(37)),
    ];
    for (name, selector, field_name, field_type, value) in tests {
        let response = run_query(
            &server,
            &format!("SELECT {selector} FROM `local-project.analytics.people`"),
        )
        .unwrap_or_else(|err| panic!("{name}: query error = {err}"))
        .response;
        assert!(
            response.schema.fields.len() == 1
                && response.schema.fields[0].name == field_name
                && response.schema.fields[0].field_type == field_type,
            "{name}: schema = {:?}",
            response.schema
        );
        assert!(
            response.rows.len() == 1 && response.rows[0].f[0].v == value,
            "{name}: aggregate response = {response:?}"
        );
    }
}

// legacy: TestJobsQuerySupportsGroupedCountAggregate
#[test]
fn jobs_query_supports_grouped_count_aggregate() {
    let dir = tempdir();
    let server = new_server(&dir);
    create_dataset_for_test(&server, "local-project", "analytics");
    create_table_for_test(&server, "local-project", "analytics", "people");
    insert_rows_for_test(&server, "local-project", "analytics", "people");
    // legacy inserts {"id":"3","name":"Margaret","age":29,"active":false}.
    server
        .append_rows(
            "local-project",
            "analytics",
            "people",
            &[stored_row(
                "row-3",
                &[
                    ("id", "\"3\""),
                    ("name", "\"Margaret\""),
                    ("age", "29"),
                    ("active", "false"),
                ],
            )],
        )
        .expect("insert row-3");

    let response = run_query(
        &server,
        "SELECT active, COUNT(*) AS total FROM `local-project.analytics.people` GROUP BY active ORDER BY active",
    )
    .expect("query")
    .response;
    assert!(
        response.total_rows == "2" && response.rows.len() == 2,
        "response rows = {response:?}"
    );
    assert!(
        response.schema.fields.len() == 2
            && response.schema.fields[0].name == "active"
            && response.schema.fields[1].name == "total",
        "schema = {:?}",
        response.schema
    );
    assert!(
        response.rows[0].f[0].v == json!("false") && response.rows[0].f[1].v == json!("1"),
        "false group = {:?}",
        response.rows[0]
    );
    assert!(
        response.rows[1].f[0].v == json!("true") && response.rows[1].f[1].v == json!("2"),
        "true group = {:?}",
        response.rows[1]
    );
}

// legacy: TestJobsQuerySupportsGroupedOrderByDesc
#[test]
fn jobs_query_supports_grouped_order_by_desc() {
    let dir = tempdir();
    let server = new_server(&dir);
    create_dataset_for_test(&server, "local-project", "analytics");
    create_table_for_test(&server, "local-project", "analytics", "people");
    insert_rows_for_test(&server, "local-project", "analytics", "people");
    server
        .append_rows(
            "local-project",
            "analytics",
            "people",
            &[stored_row(
                "row-3",
                &[
                    ("id", "\"3\""),
                    ("name", "\"Margaret\""),
                    ("age", "29"),
                    ("active", "false"),
                ],
            )],
        )
        .expect("insert row-3");

    let response = run_query(
        &server,
        "SELECT active, COUNT(*) AS total FROM `local-project.analytics.people` GROUP BY active ORDER BY active DESC",
    )
    .expect("query")
    .response;
    assert!(
        response.total_rows == "2" && response.rows.len() == 2,
        "response rows = {response:?}"
    );
    assert!(
        response.rows[0].f[0].v == json!("true") && response.rows[0].f[1].v == json!("2"),
        "true group first = {:?}",
        response.rows[0]
    );
    assert!(
        response.rows[1].f[0].v == json!("false") && response.rows[1].f[1].v == json!("1"),
        "false group second = {:?}",
        response.rows[1]
    );
}

// legacy: TestJobsQueryRejectsGroupByFieldMismatch
#[test]
fn jobs_query_rejects_group_by_field_mismatch() {
    let dir = tempdir();
    let server = new_server(&dir);
    create_dataset_for_test(&server, "local-project", "analytics");
    create_table_for_test(&server, "local-project", "analytics", "people");

    let err = run_query(
        &server,
        "SELECT active, COUNT(*) AS total FROM `local-project.analytics.people` GROUP BY name",
    )
    .expect_err("GROUP BY field mismatch must fail");
    assert_eq!(err, "GROUP BY field must be selected");
}

// --- minimal tempdir --------------------------------------------------------

fn tempdir() -> std::path::PathBuf {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let mut dir = std::env::temp_dir();
    dir.push(format!("devcloud-bq-sql-{}-{}", std::process::id(), n));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create tempdir");
    dir
}
