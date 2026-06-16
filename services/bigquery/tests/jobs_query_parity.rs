//! 1:1 port of `internal/services/bigquery/jobs_query_test.rs` (all 10 tests,
//! deferred from part 2 until the `/queries` handlers existed).
//!
//! The legacy tests drive `server.routes().ServeHTTP`; the routing layer arrives
//! in part 4, so these call `query_rows`/`get_query_results`/`get_job` with
//! the already-routed parameters — same scenarios, same expected statuses
//! and JSON.

use devcloud_bigquery::model::{JobResource, QueryResponse};
use devcloud_bigquery::server::{Config, Server};
use devcloud_bigquery::validation::Query;
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

fn insert_rows_for_test(server: &Server, project_id: &str, dataset_id: &str, table_id: &str) {
    let body = r#"{"rows":[{"insertId":"row-1","json":{"id":"1","name":"Ada","age":37,"active":true}},{"insertId":"row-2","json":{"id":"2","name":"Grace","age":31,"active":true}}]}"#;
    let response = server.insert_rows(project_id, dataset_id, table_id, body.as_bytes());
    assert_eq!(
        response.status,
        200,
        "insert rows status = {}, body = {}",
        response.status,
        response.body_str()
    );
}

// legacy: TestJobsQueryPersistsResultsAndGetQueryResultsReturnsBigQueryShape
#[test]
fn jobs_query_persists_results_and_get_query_results_returns_bigquery_shape() {
    let dir = tempdir();
    let server = new_server(&dir);
    create_dataset_for_test(&server, "local-project", "analytics");
    create_table_for_test(&server, "local-project", "analytics", "people");
    insert_rows_for_test(&server, "local-project", "analytics", "people");

    let body = "{\"query\":\"SELECT id, age FROM `local-project.analytics.people` WHERE age >= 30 ORDER BY id\",\"useLegacySql\":false,\"location\":\"US\"}";
    let query = server.query_rows("local-project", body.as_bytes());
    assert_eq!(
        query.status,
        200,
        "query status = {}, body = {}",
        query.status,
        query.body_str()
    );
    let response: QueryResponse =
        serde_json::from_slice(&query.body).expect("decode query response");
    assert!(
        response.kind == "bigquery#queryResponse"
            && response.job_complete
            && response.total_rows == "2",
        "query response = {response:?}"
    );
    assert!(
        response.job_reference.project_id == "local-project"
            && !response.job_reference.job_id.is_empty()
            && response.job_reference.location == "US",
        "job reference = {:?}",
        response.job_reference
    );
    assert!(
        response.schema.fields.len() == 2
            && response.schema.fields[0].name == "id"
            && response.schema.fields[1].name == "age",
        "schema = {:?}",
        response.schema
    );
    assert!(
        response.rows.len() == 2 && response.rows[0].f[0].v == json!("1"),
        "rows = {:?}",
        response.rows
    );

    // GET /queries/{jobId}?maxResults=1 ...
    let results = server.get_query_results(
        "local-project",
        &response.job_reference.job_id,
        &Query::parse("maxResults=1"),
    );
    assert_eq!(
        results.status,
        200,
        "results status = {}, body = {}",
        results.status,
        results.body_str()
    );
    let results_response: QueryResponse =
        serde_json::from_slice(&results.body).expect("decode results response");
    assert!(
        results_response.job_complete
            && results_response.total_rows == "2"
            && results_response.page_token == "1"
            && results_response.rows.len() == 1,
        "results response = {results_response:?}"
    );

    // ... and the canonical GET /jobs/{jobId}/getQueryResults?maxResults=1
    // (same legacy handler mounted on both paths).
    let canonical_results = server.get_query_results(
        "local-project",
        &response.job_reference.job_id,
        &Query::parse("maxResults=1"),
    );
    assert_eq!(
        canonical_results.status,
        200,
        "canonical results status = {}, body = {}",
        canonical_results.status,
        canonical_results.body_str()
    );
    let canonical_response: QueryResponse =
        serde_json::from_slice(&canonical_results.body).expect("decode canonical results response");
    assert!(
        canonical_response.job_complete
            && canonical_response.total_rows == "2"
            && canonical_response.page_token == "1"
            && canonical_response.rows.len() == 1,
        "canonical results response = {canonical_response:?}"
    );

    let job = server.get_job("local-project", &response.job_reference.job_id);
    assert!(
        job.status == 200 && job.body_str().contains("\"state\":\"DONE\""),
        "job status/body = {} {}",
        job.status,
        job.body_str()
    );
}

// legacy: TestJobsQueryCanReadViewMetadataQuery
#[test]
fn jobs_query_can_read_view_metadata_query() {
    let dir = tempdir();
    let server = new_server(&dir);
    create_dataset_for_test(&server, "local-project", "analytics");
    create_table_for_test(&server, "local-project", "analytics", "people");
    insert_rows_for_test(&server, "local-project", "analytics", "people");

    let view_body = "{
		\"tableReference\":{\"tableId\":\"active_people\"},
		\"type\":\"VIEW\",
		\"view\":{\"query\":\"SELECT id, name, age FROM `local-project.analytics.people` WHERE active = TRUE\",\"useLegacySql\":false}
	}";
    let create_view = server.create_table("local-project", "analytics", view_body.as_bytes());
    assert_eq!(
        create_view.status,
        200,
        "create view status = {}, body = {}",
        create_view.status,
        create_view.body_str()
    );

    let body = "{\"query\":\"SELECT id, name FROM `local-project.analytics.active_people` WHERE age >= 35 ORDER BY id\",\"useLegacySql\":false,\"location\":\"US\"}";
    let query = server.query_rows("local-project", body.as_bytes());
    assert_eq!(
        query.status,
        200,
        "query view status = {}, body = {}",
        query.status,
        query.body_str()
    );
    let response: QueryResponse =
        serde_json::from_slice(&query.body).expect("decode view query response");
    assert!(
        response.total_rows == "1" && response.rows.len() == 1,
        "view query response = {response:?}"
    );
    assert!(
        response.schema.fields.len() == 2
            && response.schema.fields[0].name == "id"
            && response.schema.fields[1].name == "name",
        "view query schema = {:?}",
        response.schema
    );
    assert!(
        response.rows[0].f[0].v == json!("1") && response.rows[0].f[1].v == json!("Ada"),
        "view query rows = {:?}",
        response.rows
    );

    let dry_run_body = "{\"query\":\"SELECT id, name FROM `local-project.analytics.active_people`\",\"useLegacySql\":false,\"dryRun\":true}";
    let dry_run = server.query_rows("local-project", dry_run_body.as_bytes());
    assert_eq!(
        dry_run.status,
        200,
        "dry run view status = {}, body = {}",
        dry_run.status,
        dry_run.body_str()
    );
    let dry_run_response: QueryResponse =
        serde_json::from_slice(&dry_run.body).expect("decode dry run view response");
    assert!(
        dry_run_response.total_rows == "0" && dry_run_response.schema.fields.len() == 2,
        "dry run view response = {dry_run_response:?}"
    );
}

// legacy: TestJobsQueryRejectsLegacySQLWithoutLeakingQueryText
#[test]
fn jobs_query_rejects_legacy_sql_without_leaking_query_text() {
    let dir = tempdir();
    let server = new_server(&dir);
    create_dataset_for_test(&server, "local-project", "analytics");
    create_table_for_test(&server, "local-project", "analytics", "people");

    let body = r#"{"query":"SELECT token FROM secret_sensitive_table","useLegacySql":true}"#;
    let query = server.query_rows("local-project", body.as_bytes());
    assert_eq!(
        query.status,
        400,
        "legacy query status = {}, body = {}",
        query.status,
        query.body_str()
    );
    assert!(
        query.body_str().contains("legacy SQL is not supported"),
        "legacy query error missing reason: {}",
        query.body_str()
    );
    assert!(
        !query.body_str().contains("secret_sensitive_table") && !query.body_str().contains("token"),
        "legacy query error leaked query text: {}",
        query.body_str()
    );
}

// legacy: TestJobsQueryUsesConfiguredDefaultLegacySQLWhenUseLegacySQLIsMissing
#[test]
fn jobs_query_uses_configured_default_legacy_sql_when_use_legacy_sql_is_missing() {
    let dir = tempdir();
    let server = Server::new(Config {
        project: "local-project".to_string(),
        location: "US".to_string(),
        storage_path: dir.to_string_lossy().into_owned(),
        default_legacy_sql: true,
        ..Default::default()
    });
    create_dataset_for_test(&server, "local-project", "analytics");
    create_table_for_test(&server, "local-project", "analytics", "people");

    let body = "{\"query\":\"SELECT id FROM `local-project.analytics.people`\"}";
    let query = server.query_rows("local-project", body.as_bytes());
    assert_eq!(
        query.status,
        400,
        "default legacy query status = {}, body = {}",
        query.status,
        query.body_str()
    );
    assert!(
        !query.body_str().contains("local-project.analytics.people"),
        "default legacy query error leaked query text: {}",
        query.body_str()
    );
}

// legacy: TestJobsQueryMaxResultsPagesResponseWithoutTruncatingPersistedResults
#[test]
fn jobs_query_max_results_pages_response_without_truncating_persisted_results() {
    let dir = tempdir();
    let server = new_server(&dir);
    create_dataset_for_test(&server, "local-project", "analytics");
    create_table_for_test(&server, "local-project", "analytics", "people");
    insert_rows_for_test(&server, "local-project", "analytics", "people");

    let body = "{\"query\":\"SELECT id, name FROM `local-project.analytics.people` ORDER BY id\",\"useLegacySql\":false,\"maxResults\":1}";
    let query = server.query_rows("local-project", body.as_bytes());
    assert_eq!(
        query.status,
        200,
        "query status = {}, body = {}",
        query.status,
        query.body_str()
    );
    let response: QueryResponse =
        serde_json::from_slice(&query.body).expect("decode query response");
    assert!(
        response.total_rows == "2" && response.page_token == "1" && response.rows.len() == 1,
        "paged query response = {response:?}"
    );

    let results = server.get_query_results(
        "local-project",
        &response.job_reference.job_id,
        &Query::parse(""),
    );
    assert_eq!(
        results.status,
        200,
        "results status = {}, body = {}",
        results.status,
        results.body_str()
    );
    let results_response: QueryResponse =
        serde_json::from_slice(&results.body).expect("decode results response");
    assert!(
        results_response.total_rows == "2"
            && results_response.rows.len() == 2
            && results_response.rows[1].f[1].v == json!("Grace"),
        "persisted results were truncated: {results_response:?}"
    );
}

// legacy: TestJobsQueryHonorsConfiguredMaxResultRows
#[test]
fn jobs_query_honors_configured_max_result_rows() {
    let dir = tempdir();
    let server = Server::new(Config {
        project: "local-project".to_string(),
        location: "US".to_string(),
        storage_path: dir.to_string_lossy().into_owned(),
        max_result_rows: 1,
        ..Default::default()
    });
    create_dataset_for_test(&server, "local-project", "analytics");
    create_table_for_test(&server, "local-project", "analytics", "people");
    insert_rows_for_test(&server, "local-project", "analytics", "people");

    let body = "{\"query\":\"SELECT id, name FROM `local-project.analytics.people` ORDER BY id\",\"useLegacySql\":false}";
    let query = server.query_rows("local-project", body.as_bytes());
    assert_eq!(
        query.status,
        200,
        "query status = {}, body = {}",
        query.status,
        query.body_str()
    );
    let response: QueryResponse =
        serde_json::from_slice(&query.body).expect("decode query response");
    assert!(
        response.total_rows == "2" && response.page_token == "1" && response.rows.len() == 1,
        "configured page response = {response:?}"
    );
}

// legacy: TestJobsQueryDryRunValidatesAndReturnsSchemaWithoutRows
#[test]
fn jobs_query_dry_run_validates_and_returns_schema_without_rows() {
    let dir = tempdir();
    let server = new_server(&dir);
    create_dataset_for_test(&server, "local-project", "analytics");
    create_table_for_test(&server, "local-project", "analytics", "people");
    insert_rows_for_test(&server, "local-project", "analytics", "people");

    let body = "{\"query\":\"SELECT id, age FROM `local-project.analytics.people` WHERE age >= 30\",\"useLegacySql\":false,\"dryRun\":true}";
    let query = server.query_rows("local-project", body.as_bytes());
    assert_eq!(
        query.status,
        200,
        "dry run status = {}, body = {}",
        query.status,
        query.body_str()
    );
    let response: QueryResponse =
        serde_json::from_slice(&query.body).expect("decode dry run response");
    assert!(
        response.kind == "bigquery#queryResponse"
            && response.job_complete
            && response.total_rows == "0"
            && response.rows.is_empty(),
        "dry run response = {response:?}"
    );
    assert!(
        response.schema.fields.len() == 2
            && response.schema.fields[0].name == "id"
            && response.schema.fields[1].name == "age",
        "dry run schema = {:?}",
        response.schema
    );

    let job = server.get_job("local-project", &response.job_reference.job_id);
    assert_eq!(
        job.status,
        200,
        "dry run job status = {}, body = {}",
        job.status,
        job.body_str()
    );
    let resource: JobResource = serde_json::from_slice(&job.body).expect("decode dry run job");
    assert!(
        resource.statistics.query.dry_run && resource.statistics.query.total_rows == "0",
        "dry run job statistics = {:?}",
        resource.statistics.query
    );
}

// legacy: TestJobsQuerySupportsNamedScalarQueryParameters
#[test]
fn jobs_query_supports_named_scalar_query_parameters() {
    let dir = tempdir();
    let server = new_server(&dir);
    create_dataset_for_test(&server, "local-project", "analytics");
    create_table_for_test(&server, "local-project", "analytics", "people");
    insert_rows_for_test(&server, "local-project", "analytics", "people");

    let body = "{
		\"query\":\"SELECT id, name FROM `local-project.analytics.people` WHERE age >= @min_age AND active = @active ORDER BY id\",
		\"useLegacySql\":false,
		\"queryParameters\":[
			{\"name\":\"min_age\",\"parameterType\":{\"type\":\"INT64\"},\"parameterValue\":{\"value\":\"35\"}},
			{\"name\":\"active\",\"parameterType\":{\"type\":\"BOOL\"},\"parameterValue\":{\"value\":\"true\"}}
		]
	}";
    let query = server.query_rows("local-project", body.as_bytes());
    assert_eq!(
        query.status,
        200,
        "query status = {}, body = {}",
        query.status,
        query.body_str()
    );
    let response: QueryResponse =
        serde_json::from_slice(&query.body).expect("decode query response");
    assert!(
        response.total_rows == "1" && response.rows.len() == 1,
        "parameter query response = {response:?}"
    );
    assert!(
        response.rows[0].f[0].v == json!("1") && response.rows[0].f[1].v == json!("Ada"),
        "parameter filtered row = {:?}",
        response.rows[0]
    );
}

// legacy: TestJobsQueryRejectsMissingParameterWithoutLeakingQueryText
#[test]
fn jobs_query_rejects_missing_parameter_without_leaking_query_text() {
    let dir = tempdir();
    let server = new_server(&dir);
    create_dataset_for_test(&server, "local-project", "analytics");
    create_table_for_test(&server, "local-project", "analytics", "people");

    let body = "{\"query\":\"SELECT id FROM `local-project.analytics.people` WHERE name = @secret_name\",\"useLegacySql\":false}";
    let query = server.query_rows("local-project", body.as_bytes());
    assert_eq!(
        query.status,
        400,
        "query status = {}, body = {}",
        query.status,
        query.body_str()
    );
    assert!(
        !query.body_str().contains("local-project.analytics.people"),
        "parameter error leaked query text: {}",
        query.body_str()
    );
}

// legacy: TestJobsQueryRejectsUnsupportedQueryWithoutLeakingQueryText
#[test]
fn jobs_query_rejects_unsupported_query_without_leaking_query_text() {
    let dir = tempdir();
    let server = Server::new(Config {
        project: "local-project".to_string(),
        storage_path: dir.to_string_lossy().into_owned(),
        ..Default::default()
    });

    let rec = server.query_rows(
        "local-project",
        br#"{"query":"DELETE FROM secret.dataset.table"}"#,
    );
    assert_eq!(
        rec.status,
        400,
        "status = {}, body = {}",
        rec.status,
        rec.body_str()
    );
    assert!(
        !rec.body_str().contains("secret.dataset.table"),
        "error response leaked query text: {}",
        rec.body_str()
    );
}

// --- minimal tempdir --------------------------------------------------------

fn tempdir() -> std::path::PathBuf {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let mut dir = std::env::temp_dir();
    dir.push(format!("devcloud-bq-jq-{}-{}", std::process::id(), n));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create tempdir");
    dir
}
