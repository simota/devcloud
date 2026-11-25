//! Tests for BigQuery SQL routine execution:
//! - Scalar SQL UDFs (`SCALAR_FUNCTION`) in SELECT (with/without FROM) and WHERE
//! - Nested UDF calls, built-in expressions, CAST, CASE, NULL propagation, depth limit
//! - SQL Table-Valued Functions (`TABLE_VALUED_FUNCTION`) in FROM
//! - SQL Stored Procedures (`PROCEDURE`) via CALL
//! - Fail-closed handling for non-SQL routines, arity mismatches, and unsupported scripts

use devcloud_bigquery::model::{QueryResponse, TableDataListResponse};
use devcloud_bigquery::routes::{handle, Request};
use devcloud_bigquery::server::{Config, Server};
use devcloud_bigquery::validation::Query;
use serde_json::json;

fn new_server(dir: &std::path::Path) -> Server {
    Server::new(Config {
        project: "local-project".to_string(),
        location: "US".to_string(),
        auth_mode: "relaxed".to_string(),
        storage_path: dir.to_string_lossy().into_owned(),
        ..Default::default()
    })
}

fn setup_people(server: &Server) {
    let ds = server.create_dataset(
        "local-project",
        br#"{"datasetReference":{"datasetId":"analytics"}}"#,
    );
    assert_eq!(ds.status, 200, "create dataset: {}", ds.body_str());

    let tbl = server.create_table(
        "local-project",
        "analytics",
        br#"{
            "tableReference":{"tableId":"people"},
            "schema":{"fields":[
                {"name":"id","type":"STRING","mode":"REQUIRED"},
                {"name":"name","type":"STRING"},
                {"name":"age","type":"INTEGER"},
                {"name":"score","type":"FLOAT"},
                {"name":"active","type":"BOOLEAN"}
            ]}
        }"#,
    );
    assert_eq!(tbl.status, 200, "create table: {}", tbl.body_str());

    let ins = server.insert_rows(
        "local-project",
        "analytics",
        "people",
        br#"{"rows":[
            {"insertId":"r1","json":{"id":"1","name":"  Ada Lovelace ","age":37,"score":92.5,"active":true}},
            {"insertId":"r2","json":{"id":"2","name":"Grace Hopper","age":31,"score":88.0,"active":true}},
            {"insertId":"r3","json":{"id":"3","name":"Alan Turing","age":17,"score":null,"active":false}}
        ]}"#,
    );
    assert_eq!(ins.status, 200, "insert rows: {}", ins.body_str());
}

fn create_routine(server: &Server, body: &str) {
    let res = server.create_routine("local-project", "analytics", body.as_bytes());
    assert_eq!(res.status, 200, "create routine: {}", res.body_str());
}

fn run_query(server: &Server, sql: &str) -> QueryResponse {
    let body = serde_json::json!({
        "query": sql,
        "useLegacySql": false
    })
    .to_string();
    let res = handle(
        server,
        &Request::new(
            "POST",
            "/bigquery/v2/projects/local-project/queries",
            body.as_bytes(),
        ),
    );
    assert_eq!(
        res.status,
        200,
        "query failed ({}): {}",
        res.status,
        res.body_str()
    );
    serde_json::from_slice(&res.body).expect("decode QueryResponse")
}

#[test]
fn scalar_sql_udf_runs_in_select_and_where_and_dry_run() {
    let dir = tempdir();
    let server = new_server(&dir);
    setup_people(&server);

    create_routine(
        &server,
        r#"{
            "routineReference":{"routineId":"normalize_name"},
            "routineType":"SCALAR_FUNCTION",
            "language":"SQL",
            "arguments":[{"name":"raw_name","dataType":{"typeKind":"STRING"}}],
            "returnType":{"typeKind":"STRING"},
            "definitionBody":"LOWER(TRIM(raw_name))"
        }"#,
    );
    create_routine(
        &server,
        r#"{
            "routineReference":{"routineId":"is_adult"},
            "routineType":"SCALAR_FUNCTION",
            "language":"SQL",
            "arguments":[{"name":"years","dataType":{"typeKind":"INT64"}}],
            "returnType":{"typeKind":"BOOL"},
            "definitionBody":"years >= 18"
        }"#,
    );
    create_routine(
        &server,
        r#"{
            "routineReference":{"routineId":"age_band"},
            "routineType":"SCALAR_FUNCTION",
            "language":"SQL",
            "arguments":[{"name":"years","dataType":{"typeKind":"INT64"}}],
            "returnType":{"typeKind":"STRING"},
            "definitionBody":"CASE WHEN years >= 35 THEN 'senior' WHEN years >= 18 THEN 'adult' ELSE 'minor' END"
        }"#,
    );

    let res = run_query(
        &server,
        "SELECT id, analytics.normalize_name(name) AS norm_name, `local-project.analytics.age_band`(age) AS band FROM `local-project.analytics.people` WHERE analytics.is_adult(age) ORDER BY norm_name",
    );
    assert_eq!(res.total_rows, "2");
    assert_eq!(res.schema.fields.len(), 3);
    assert_eq!(res.schema.fields[1].name, "norm_name");
    assert_eq!(res.schema.fields[1].field_type, "STRING");
    assert_eq!(res.schema.fields[2].name, "band");
    assert_eq!(res.rows[0].f[0].v, json!("1"));
    assert_eq!(res.rows[0].f[1].v, json!("ada lovelace"));
    assert_eq!(res.rows[0].f[2].v, json!("senior"));
    assert_eq!(res.rows[1].f[0].v, json!("2"));
    assert_eq!(res.rows[1].f[1].v, json!("grace hopper"));
    assert_eq!(res.rows[1].f[2].v, json!("adult"));

    // WHERE comparing scalar UDF result to literal + NOT boolean UDF
    let minor = run_query(
        &server,
        "SELECT id FROM `local-project.analytics.people` WHERE NOT analytics.is_adult(age)",
    );
    assert_eq!(minor.total_rows, "1");
    assert_eq!(minor.rows[0].f[0].v, json!("3"));

    let by_name = run_query(
        &server,
        "SELECT id FROM `local-project.analytics.people` WHERE analytics.normalize_name(name) = 'ada lovelace'",
    );
    assert_eq!(by_name.total_rows, "1");
    assert_eq!(by_name.rows[0].f[0].v, json!("1"));

    // Dry run validates routine and returns output schema with 0 rows.
    let dry_body = serde_json::json!({
        "query": "SELECT id, analytics.normalize_name(name) AS norm_name, analytics.is_adult(age) AS adult FROM `local-project.analytics.people` WHERE analytics.is_adult(age)",
        "useLegacySql": false,
        "dryRun": true
    })
    .to_string();
    let dry = server.query_rows("local-project", dry_body.as_bytes());
    assert_eq!(dry.status, 200, "dry run failed: {}", dry.body_str());
    let dry_res: QueryResponse = serde_json::from_slice(&dry.body).expect("decode dry run");
    assert_eq!(dry_res.total_rows, "0");
    assert!(dry_res.rows.is_empty());
    assert_eq!(dry_res.schema.fields[1].field_type, "STRING");
    assert_eq!(dry_res.schema.fields[2].field_type, "BOOLEAN");
}

#[test]
fn scalar_sql_udf_works_without_from_and_with_nested_udfs_and_nulls() {
    let dir = tempdir();
    let server = new_server(&dir);
    setup_people(&server);

    create_routine(
        &server,
        r#"{
            "routineReference":{"routineId":"double_val"},
            "routineType":"SCALAR_FUNCTION",
            "language":"SQL",
            "arguments":[{"name":"x","dataType":{"typeKind":"INT64"}}],
            "returnType":{"typeKind":"INT64"},
            "definitionBody":"x * 2"
        }"#,
    );
    create_routine(
        &server,
        r#"{
            "routineReference":{"routineId":"double_plus"},
            "routineType":"SCALAR_FUNCTION",
            "language":"SQL",
            "arguments":[
                {"name":"x","dataType":{"typeKind":"INT64"}},
                {"name":"delta","dataType":{"typeKind":"INT64"}}
            ],
            "returnType":{"typeKind":"INT64"},
            "definitionBody":"analytics.double_val(x) + delta"
        }"#,
    );
    create_routine(
        &server,
        r#"{
            "routineReference":{"routineId":"safe_ratio"},
            "routineType":"SCALAR_FUNCTION",
            "language":"SQL",
            "arguments":[
                {"name":"num","dataType":{"typeKind":"FLOAT64"}},
                {"name":"den","dataType":{"typeKind":"FLOAT64"}}
            ],
            "returnType":{"typeKind":"FLOAT64"},
            "definitionBody":"COALESCE(SAFE_DIVIDE(num, den), -1.0)"
        }"#,
    );

    // Scalar SELECT without FROM
    let res = run_query(
        &server,
        "SELECT analytics.double_plus(10, 5) AS result, analytics.safe_ratio(10.0, 0.0) AS fallback",
    );
    assert_eq!(res.total_rows, "1");
    assert_eq!(res.schema.fields[0].name, "result");
    assert_eq!(res.schema.fields[0].field_type, "INTEGER");
    assert_eq!(res.rows[0].f[0].v, json!("25"));
    assert_eq!(res.rows[0].f[1].v, json!(-1.0));

    // NULL propagation from table column
    let null_res = run_query(
        &server,
        "SELECT id, analytics.safe_ratio(score, 10.0) AS scaled FROM `local-project.analytics.people` ORDER BY id",
    );
    assert_eq!(null_res.total_rows, "3");
    assert_eq!(null_res.rows[0].f[1].v, json!(9.25));
    assert_eq!(null_res.rows[2].f[1].v, json!(-1.0));
}

#[test]
fn scalar_sql_udf_works_with_query_params_views_and_destination_table() {
    let dir = tempdir();
    let server = new_server(&dir);
    setup_people(&server);

    create_routine(
        &server,
        r#"{
            "routineReference":{"routineId":"add_bonus"},
            "routineType":"SCALAR_FUNCTION",
            "language":"SQL",
            "arguments":[
                {"name":"base","dataType":{"typeKind":"INT64"}},
                {"name":"bonus","dataType":{"typeKind":"INT64"}}
            ],
            "returnType":{"typeKind":"INT64"},
            "definitionBody":"base + bonus"
        }"#,
    );

    // Parameterized query calling a UDF
    let param_body = r#"{
        "query":"SELECT id, analytics.add_bonus(age, @bonus) AS adjusted FROM `local-project.analytics.people` WHERE id = '1'",
        "useLegacySql":false,
        "queryParameters":[{"name":"bonus","parameterType":{"type":"INT64"},"parameterValue":{"value":"3"}}]
    }"#;
    let param_res = server.query_rows("local-project", param_body.as_bytes());
    assert_eq!(
        param_res.status,
        200,
        "param query: {}",
        param_res.body_str()
    );
    let decoded: QueryResponse = serde_json::from_slice(&param_res.body).expect("decode");
    assert_eq!(decoded.rows[0].f[1].v, json!("40"));

    // View calling a scalar SQL UDF
    let view_body = r#"{
        "tableReference":{"tableId":"bonus_view"},
        "type":"VIEW",
        "view":{
            "query":"SELECT id, analytics.add_bonus(age, 5) AS age_plus_five FROM `local-project.analytics.people` WHERE active = true",
            "useLegacySql":false
        }
    }"#;
    let view = server.create_table("local-project", "analytics", view_body.as_bytes());
    assert_eq!(view.status, 200, "create view: {}", view.body_str());
    let view_res = run_query(
        &server,
        "SELECT id, age_plus_five FROM `local-project.analytics.bonus_view` ORDER BY id",
    );
    assert_eq!(view_res.total_rows, "2");
    assert_eq!(view_res.rows[0].f[1].v, json!("42"));
    assert_eq!(view_res.rows[1].f[1].v, json!("36"));

    // Built-in expressions inside UDF body (IF, CAST, CONCAT, SUBSTR, ROUND, ||)
    create_routine(
        &server,
        r#"{
            "routineReference":{"routineId":"format_badge"},
            "routineType":"SCALAR_FUNCTION",
            "language":"SQL",
            "arguments":[
                {"name":"raw_name","dataType":{"typeKind":"STRING"}},
                {"name":"raw_score","dataType":{"typeKind":"FLOAT64"}}
            ],
            "returnType":{"typeKind":"STRING"},
            "definitionBody":"UPPER(SUBSTR(TRIM(raw_name), 1, 3)) || ':' || IF(raw_score IS NULL, 'NA', CAST(ROUND(raw_score, 0) AS STRING))"
        }"#,
    );
    let badge_res = run_query(
        &server,
        "SELECT id, analytics.format_badge(name, score) AS badge FROM `local-project.analytics.people` ORDER BY id",
    );
    assert_eq!(badge_res.total_rows, "3");
    assert_eq!(badge_res.rows[0].f[1].v, json!("ADA:93"));
    assert_eq!(badge_res.rows[1].f[1].v, json!("GRA:88"));
    assert_eq!(badge_res.rows[2].f[1].v, json!("ALA:NA"));

    // Query job with destinationTable
    let dest_job = r#"{
        "jobReference":{"jobId":"udf_to_table"},
        "configuration":{"query":{
            "query":"SELECT id, analytics.add_bonus(age, 10) AS plus_ten FROM `local-project.analytics.people` ORDER BY id LIMIT 2",
            "useLegacySql":false,
            "destinationTable":{"datasetId":"analytics","tableId":"people_bonus"},
            "writeDisposition":"WRITE_TRUNCATE"
        }}
    }"#;
    let job_res = server.insert_job("local-project", dest_job.as_bytes());
    assert_eq!(job_res.status, 200, "dest job: {}", job_res.body_str());

    let listed = server.list_rows(
        "local-project",
        "analytics",
        "people_bonus",
        &Query::parse(""),
    );
    let rows: TableDataListResponse = serde_json::from_slice(&listed.body).expect("decode rows");
    assert_eq!(rows.total_rows, "2");
    assert_eq!(rows.rows[0].f[1].v, json!("47"));
    assert_eq!(rows.rows[1].f[1].v, json!("41"));
}

#[test]
fn table_valued_function_and_sql_procedure_execute() {
    let dir = tempdir();
    let server = new_server(&dir);
    setup_people(&server);

    create_routine(
        &server,
        r#"{
            "routineReference":{"routineId":"people_min_age"},
            "routineType":"TABLE_VALUED_FUNCTION",
            "language":"SQL",
            "arguments":[{"name":"min_age","dataType":{"typeKind":"INT64"}}],
            "definitionBody":"SELECT id, name, age FROM `local-project.analytics.people` WHERE age >= min_age"
        }"#,
    );

    let tvf_res = run_query(
        &server,
        "SELECT id, age FROM analytics.people_min_age(35) ORDER BY id",
    );
    assert_eq!(tvf_res.total_rows, "1");
    assert_eq!(tvf_res.rows[0].f[0].v, json!("1"));
    assert_eq!(tvf_res.rows[0].f[1].v, json!("37"));

    create_routine(
        &server,
        r#"{
            "routineReference":{"routineId":"get_person_by_id"},
            "routineType":"PROCEDURE",
            "language":"SQL",
            "arguments":[{"name":"target_id","dataType":{"typeKind":"STRING"}}],
            "definitionBody":"BEGIN SELECT id, age FROM `local-project.analytics.people` WHERE id = target_id; END;"
        }"#,
    );

    let call_res = run_query(&server, "CALL analytics.get_person_by_id('2')");
    assert_eq!(call_res.total_rows, "1");
    assert_eq!(call_res.rows[0].f[0].v, json!("2"));
    assert_eq!(call_res.rows[0].f[1].v, json!("31"));
}

#[test]
fn routine_execution_fails_closed_for_js_cycles_arity_and_scripts() {
    let dir = tempdir();
    let server = new_server(&dir);
    setup_people(&server);

    // JavaScript UDF must fail closed
    create_routine(
        &server,
        r#"{
            "routineReference":{"routineId":"js_upper"},
            "routineType":"SCALAR_FUNCTION",
            "language":"JAVASCRIPT",
            "arguments":[{"name":"s","dataType":{"typeKind":"STRING"}}],
            "returnType":{"typeKind":"STRING"},
            "definitionBody":"return s.toUpperCase();"
        }"#,
    );
    let js_res = server.query_rows(
        "local-project",
        br#"{"query":"SELECT analytics.js_upper('secret-val')","useLegacySql":false}"#,
    );
    assert_eq!(js_res.status, 400);
    assert!(js_res
        .body_str()
        .contains("only SQL routines are supported"));
    assert!(!js_res.body_str().contains("secret-val"));

    // Recursive UDF must hit depth bound
    create_routine(
        &server,
        r#"{
            "routineReference":{"routineId":"loop_fn"},
            "routineType":"SCALAR_FUNCTION",
            "language":"SQL",
            "arguments":[{"name":"x","dataType":{"typeKind":"INT64"}}],
            "returnType":{"typeKind":"INT64"},
            "definitionBody":"analytics.loop_fn(x + 1)"
        }"#,
    );
    let cycle_res = server.query_rows(
        "local-project",
        br#"{"query":"SELECT analytics.loop_fn(1)","useLegacySql":false}"#,
    );
    assert_eq!(cycle_res.status, 400);
    assert!(cycle_res.body_str().contains("routine call depth exceeded"));

    // Arity mismatch
    let arity_res = server.query_rows(
        "local-project",
        br#"{"query":"SELECT analytics.loop_fn(1, 2)","useLegacySql":false}"#,
    );
    assert_eq!(arity_res.status, 400);
    assert!(arity_res.body_str().contains("expects 1 argument"));

    // Missing routine
    let missing_res = server.query_rows(
        "local-project",
        br#"{"query":"SELECT analytics.no_such_udf(1)","useLegacySql":false}"#,
    );
    assert_eq!(missing_res.status, 400);
    assert!(missing_res.body_str().contains("not found: routine"));

    // Multi-statement / scripting procedure must fail closed
    create_routine(
        &server,
        r#"{
            "routineReference":{"routineId":"script_proc"},
            "routineType":"PROCEDURE",
            "language":"SQL",
            "definitionBody":"BEGIN DECLARE x INT64; SET x = 1; END;"
        }"#,
    );
    let proc_res = server.query_rows(
        "local-project",
        br#"{"query":"CALL analytics.script_proc()","useLegacySql":false}"#,
    );
    assert_eq!(proc_res.status, 400);
}

fn tempdir() -> std::path::PathBuf {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let mut dir = std::env::temp_dir();
    dir.push(format!("devcloud-bq-routine-{}-{}", std::process::id(), n));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create tempdir");
    dir
}
