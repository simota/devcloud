//! 1:1 port of the job tests in `internal/services/bigquery/jobs_test.rs`,
//! including the copy-job tests (`TestJobsInsertCopyJob*`) that exercise
//! `job_load_extract.rs`.
//!
//! The legacy tests drive `server.routes().ServeHTTP`; these call the job
//! handlers with the already-routed parameters — same scenarios, same
//! expected statuses and JSON. The routing layer itself is covered by
//! `server_parity.rs` / `iam_parity.rs`.

use devcloud_bigquery::model::{
    JobCancelResponse, JobResource, JobsListResponse, QueryResponse, TableDataListResponse,
    TableResource,
};
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

fn insert_query_job_for_test(server: &Server, project_id: &str, job_id: &str) {
    let body = format!(
        "{{\"jobReference\":{{\"jobId\":\"{job_id}\",\"location\":\"US\"}},\"configuration\":{{\"query\":{{\"query\":\"SELECT id, age FROM `{project_id}.analytics.people` WHERE age >= 30 ORDER BY id\",\"useLegacySql\":false}}}}}}"
    );
    let response = server.insert_job(project_id, body.as_bytes());
    assert_eq!(
        response.status,
        200,
        "insert query job status = {}, body = {}",
        response.status,
        response.body_str()
    );
}

// legacy: TestJobsInsertQueryJobCanOverrideDefaultLegacySQL
#[test]
fn jobs_insert_query_job_can_override_default_legacy_sql() {
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
    insert_rows_for_test(&server, "local-project", "analytics", "people");

    let body = "{\"jobReference\":{\"jobId\":\"standard_sql_job\",\"location\":\"US\"},\"configuration\":{\"query\":{\"query\":\"SELECT id FROM `local-project.analytics.people` ORDER BY id\",\"useLegacySql\":false}}}";
    let insert = server.insert_job("local-project", body.as_bytes());
    assert_eq!(
        insert.status,
        200,
        "insert status = {}, body = {}",
        insert.status,
        insert.body_str()
    );
    let raw_job: serde_json::Value = serde_json::from_slice(&insert.body).expect("decode raw job");
    for field in ["creationTime", "startTime", "endTime"] {
        assert!(
            raw_job["statistics"][field].is_string(),
            "statistics.{field} = {:?}, want JSON string",
            raw_job["statistics"][field]
        );
    }
    let job: JobResource = serde_json::from_slice(&insert.body).expect("decode job");
    assert!(
        job.job_reference.job_id == "standard_sql_job"
            && job.configuration.query.use_legacy_sql == Some(false),
        "job query configuration = {:?}",
        job.configuration.query
    );
}

// legacy: TestJobsInsertQueryJobPersistsNamedQueryParameters
#[test]
fn jobs_insert_query_job_persists_named_query_parameters() {
    let dir = tempdir();
    let server = new_server(&dir);
    create_dataset_for_test(&server, "local-project", "analytics");
    create_table_for_test(&server, "local-project", "analytics", "people");
    insert_rows_for_test(&server, "local-project", "analytics", "people");

    let body = "{
		\"jobReference\":{\"jobId\":\"parameterized_query\",\"location\":\"US\"},
		\"configuration\":{\"query\":{
			\"query\":\"SELECT id FROM `local-project.analytics.people` WHERE name = @name\",
			\"useLegacySql\":false,
			\"queryParameters\":[{\"name\":\"name\",\"parameterType\":{\"type\":\"STRING\"},\"parameterValue\":{\"value\":\"Grace\"}}]
		}}
	}";
    let insert = server.insert_job("local-project", body.as_bytes());
    assert_eq!(
        insert.status,
        200,
        "insert status = {}, body = {}",
        insert.status,
        insert.body_str()
    );
    let job: JobResource = serde_json::from_slice(&insert.body).expect("decode job");
    assert!(
        job.configuration.query.query_parameters.len() == 1
            && job.configuration.query.query_parameters[0].name == "name",
        "query parameters were not persisted: {:?}",
        job.configuration.query.query_parameters
    );

    let results =
        server.get_query_results("local-project", "parameterized_query", &Query::parse(""));
    assert_eq!(
        results.status,
        200,
        "results status = {}, body = {}",
        results.status,
        results.body_str()
    );
    let response: QueryResponse = serde_json::from_slice(&results.body).expect("decode results");
    assert!(
        response.total_rows == "1"
            && response.rows.len() == 1
            && response.rows[0].f[0].v == json!("2"),
        "parameterized job results = {response:?}"
    );
}

// legacy: TestJobsInsertQueryJobPersistsResults
#[test]
fn jobs_insert_query_job_persists_results() {
    let dir = tempdir();
    let server = new_server(&dir);
    create_dataset_for_test(&server, "local-project", "analytics");
    create_table_for_test(&server, "local-project", "analytics", "people");
    insert_rows_for_test(&server, "local-project", "analytics", "people");

    let body = "{\"jobReference\":{\"jobId\":\"job_123\",\"location\":\"US\"},\"configuration\":{\"query\":{\"query\":\"SELECT id, age FROM `local-project.analytics.people` WHERE age >= 30 ORDER BY id\",\"useLegacySql\":false}}}";
    let insert = server.insert_job("local-project", body.as_bytes());
    assert_eq!(
        insert.status,
        200,
        "insert status = {}, body = {}",
        insert.status,
        insert.body_str()
    );
    let job: JobResource = serde_json::from_slice(&insert.body).expect("decode job");
    assert!(
        job.kind == "bigquery#job"
            && job.job_reference.job_id == "job_123"
            && job.status.state == "DONE",
        "job = {job:?}"
    );
    assert_eq!(
        job.statistics.query.total_rows, "2",
        "job statistics = {:?}",
        job.statistics
    );

    let get = server.get_job("local-project", "job_123");
    assert!(
        get.status == 200 && get.body_str().contains("\"jobId\":\"job_123\""),
        "get status/body = {} {}",
        get.status,
        get.body_str()
    );

    let results = server.get_query_results("local-project", "job_123", &Query::parse(""));
    assert_eq!(
        results.status,
        200,
        "results status = {}, body = {}",
        results.status,
        results.body_str()
    );
    let response: QueryResponse = serde_json::from_slice(&results.body).expect("decode results");
    assert!(
        response.total_rows == "2" && response.rows.len() == 2,
        "results response = {response:?}"
    );
}

// legacy: TestJobsInsertQueryJobDryRunPersistsSchemaOnly
#[test]
fn jobs_insert_query_job_dry_run_persists_schema_only() {
    let dir = tempdir();
    let server = new_server(&dir);
    create_dataset_for_test(&server, "local-project", "analytics");
    create_table_for_test(&server, "local-project", "analytics", "people");
    insert_rows_for_test(&server, "local-project", "analytics", "people");

    let body = "{\"jobReference\":{\"jobId\":\"dry_run_job\",\"location\":\"US\"},\"configuration\":{\"dryRun\":true,\"query\":{\"query\":\"SELECT COUNT(*) AS total FROM `local-project.analytics.people`\",\"useLegacySql\":false}}}";
    let insert = server.insert_job("local-project", body.as_bytes());
    assert_eq!(
        insert.status,
        200,
        "dry run insert status = {}, body = {}",
        insert.status,
        insert.body_str()
    );
    let job: JobResource = serde_json::from_slice(&insert.body).expect("decode dry run job");
    assert!(
        job.configuration.dry_run
            && job.statistics.query.dry_run
            && job.statistics.query.total_rows == "0",
        "dry run job = {job:?}"
    );

    let results = server.get_query_results("local-project", "dry_run_job", &Query::parse(""));
    assert_eq!(
        results.status,
        200,
        "dry run results status = {}, body = {}",
        results.status,
        results.body_str()
    );
    let response: QueryResponse =
        serde_json::from_slice(&results.body).expect("decode dry run results");
    assert!(
        response.total_rows == "0"
            && response.rows.is_empty()
            && response.schema.fields.len() == 1
            && response.schema.fields[0].name == "total",
        "dry run results = {response:?}"
    );
}

// legacy: TestJobsInsertQueryJobWritesDestinationTable
#[test]
fn jobs_insert_query_job_writes_destination_table() {
    let dir = tempdir();
    let server = new_server(&dir);
    create_dataset_for_test(&server, "local-project", "analytics");
    create_table_for_test(&server, "local-project", "analytics", "people");
    insert_rows_for_test(&server, "local-project", "analytics", "people");

    let body = "{\"jobReference\":{\"jobId\":\"query_to_table\",\"location\":\"US\"},\"configuration\":{\"query\":{\"query\":\"SELECT id, age FROM `local-project.analytics.people` WHERE age >= 30 ORDER BY id\",\"useLegacySql\":false,\"destinationTable\":{\"datasetId\":\"analytics\",\"tableId\":\"people_query\"},\"createDisposition\":\"CREATE_IF_NEEDED\",\"writeDisposition\":\"WRITE_TRUNCATE\"}}}";
    let insert = server.insert_job("local-project", body.as_bytes());
    assert_eq!(
        insert.status,
        200,
        "query destination status = {}, body = {}",
        insert.status,
        insert.body_str()
    );
    let job: JobResource =
        serde_json::from_slice(&insert.body).expect("decode query destination job");
    assert!(
        job.configuration.query.destination_table.table_id == "people_query"
            && job.statistics.query.total_rows == "2",
        "query destination job = {job:?}"
    );

    let table_rec = server.get_table("local-project", "analytics", "people_query");
    assert_eq!(
        table_rec.status,
        200,
        "destination table status = {}, body = {}",
        table_rec.status,
        table_rec.body_str()
    );
    let table: TableResource =
        serde_json::from_slice(&table_rec.body).expect("decode destination table");
    assert!(
        table.num_rows == "2"
            && table.schema.fields.len() == 2
            && table.schema.fields[0].name == "id"
            && table.schema.fields[1].name == "age",
        "destination table = {table:?}"
    );

    let rows = server.list_rows(
        "local-project",
        "analytics",
        "people_query",
        &Query::parse(""),
    );
    let row_list: TableDataListResponse =
        serde_json::from_slice(&rows.body).expect("decode query destination rows");
    assert!(
        row_list.total_rows == "2"
            && row_list.rows.len() == 2
            && row_list.rows[0].f[0].v == json!("1")
            && row_list.rows[0].f[1].v == json!("37"),
        "query destination rows = {row_list:?}"
    );
}

// legacy: TestJobsInsertQueryJobHonorsDestinationDispositions
#[test]
fn jobs_insert_query_job_honors_destination_dispositions() {
    let dir = tempdir();
    let server = new_server(&dir);
    create_dataset_for_test(&server, "local-project", "analytics");
    create_table_for_test(&server, "local-project", "analytics", "people");
    insert_rows_for_test(&server, "local-project", "analytics", "people");

    let create_never_body = "{\"configuration\":{\"query\":{\"query\":\"SELECT id FROM `local-project.analytics.people`\",\"useLegacySql\":false,\"destinationTable\":{\"datasetId\":\"analytics\",\"tableId\":\"missing\"},\"createDisposition\":\"CREATE_NEVER\"}}}";
    let create_never = server.insert_job("local-project", create_never_body.as_bytes());
    assert_eq!(
        create_never.status,
        400,
        "CREATE_NEVER status = {}, body = {}",
        create_never.status,
        create_never.body_str()
    );

    let first_body = "{\"jobReference\":{\"jobId\":\"query_write_empty_first\"},\"configuration\":{\"query\":{\"query\":\"SELECT id, age FROM `local-project.analytics.people`\",\"useLegacySql\":false,\"destinationTable\":{\"datasetId\":\"analytics\",\"tableId\":\"query_disposition\"},\"writeDisposition\":\"WRITE_EMPTY\"}}}";
    let first = server.insert_job("local-project", first_body.as_bytes());
    assert_eq!(
        first.status,
        200,
        "first WRITE_EMPTY status = {}, body = {}",
        first.status,
        first.body_str()
    );

    let write_empty_again_body = "{\"configuration\":{\"query\":{\"query\":\"SELECT id, age FROM `local-project.analytics.people`\",\"useLegacySql\":false,\"destinationTable\":{\"datasetId\":\"analytics\",\"tableId\":\"query_disposition\"},\"writeDisposition\":\"WRITE_EMPTY\"}}}";
    let write_empty_again = server.insert_job("local-project", write_empty_again_body.as_bytes());
    assert_eq!(
        write_empty_again.status,
        400,
        "second WRITE_EMPTY status = {}, body = {}",
        write_empty_again.status,
        write_empty_again.body_str()
    );

    let append_body = "{\"jobReference\":{\"jobId\":\"query_append\"},\"configuration\":{\"query\":{\"query\":\"SELECT id, age FROM `local-project.analytics.people`\",\"useLegacySql\":false,\"destinationTable\":{\"datasetId\":\"analytics\",\"tableId\":\"query_disposition\"},\"writeDisposition\":\"WRITE_APPEND\"}}}";
    let append_rec = server.insert_job("local-project", append_body.as_bytes());
    assert_eq!(
        append_rec.status,
        200,
        "WRITE_APPEND status = {}, body = {}",
        append_rec.status,
        append_rec.body_str()
    );

    let rows = server.list_rows(
        "local-project",
        "analytics",
        "query_disposition",
        &Query::parse(""),
    );
    let row_list: TableDataListResponse =
        serde_json::from_slice(&rows.body).expect("decode disposition rows");
    assert!(
        row_list.total_rows == "4" && row_list.rows.len() == 4,
        "disposition rows = {row_list:?}"
    );
}

// legacy: TestJobsListCancelAndDeleteMetadata
#[test]
fn jobs_list_cancel_and_delete_metadata() {
    let dir = tempdir();
    let server = new_server(&dir);
    create_dataset_for_test(&server, "local-project", "analytics");
    create_table_for_test(&server, "local-project", "analytics", "people");
    insert_rows_for_test(&server, "local-project", "analytics", "people");
    insert_query_job_for_test(&server, "local-project", "job_a");
    insert_query_job_for_test(&server, "local-project", "job_b");

    let list = server.list_jobs("local-project", &Query::parse("maxResults=1"));
    assert_eq!(
        list.status,
        200,
        "list status = {}, body = {}",
        list.status,
        list.body_str()
    );
    let list_response: JobsListResponse = serde_json::from_slice(&list.body).expect("decode list");
    assert!(
        list_response.kind == "bigquery#jobList"
            && list_response.next_page_token == "1"
            && list_response.jobs.len() == 1
            && list_response.jobs[0].job_reference.job_id == "job_a",
        "list response = {list_response:?}"
    );

    let cancel = server.cancel_job("local-project", "job_a");
    assert_eq!(
        cancel.status,
        200,
        "cancel status = {}, body = {}",
        cancel.status,
        cancel.body_str()
    );
    let cancel_response: JobCancelResponse =
        serde_json::from_slice(&cancel.body).expect("decode cancel");
    assert!(
        cancel_response.kind == "bigquery#jobCancelResponse"
            && cancel_response.job.status.state == "DONE",
        "cancel response = {cancel_response:?}"
    );

    // legacy DELETEs /jobs/job_a/delete (custom verb) ...
    let delete = server.delete_job_metadata("local-project", "job_a");
    assert_eq!(
        delete.status,
        204,
        "delete status = {}, body = {}",
        delete.status,
        delete.body_str()
    );
    let missing = server.get_job("local-project", "job_a");
    assert_eq!(
        missing.status,
        404,
        "missing status = {}, body = {}",
        missing.status,
        missing.body_str()
    );

    // ... and the standard DELETE /jobs/job_b; both route to the same handler.
    let delete_standard = server.delete_job_metadata("local-project", "job_b");
    assert_eq!(
        delete_standard.status,
        204,
        "standard delete status = {}, body = {}",
        delete_standard.status,
        delete_standard.body_str()
    );
    let missing_standard = server.get_job("local-project", "job_b");
    assert_eq!(
        missing_standard.status,
        404,
        "standard missing status = {}, body = {}",
        missing_standard.status,
        missing_standard.body_str()
    );
}

// legacy: TestJobsInsertCopyJobCopiesTableMetadataAndRows
#[test]
fn jobs_insert_copy_job_copies_table_metadata_and_rows() {
    let dir = tempdir();
    let server = new_server(&dir);
    create_dataset_for_test(&server, "local-project", "analytics");
    create_table_for_test(&server, "local-project", "analytics", "people");
    insert_rows_for_test(&server, "local-project", "analytics", "people");

    let body = r#"{"jobReference":{"jobId":"copy_people","location":"US"},"configuration":{"copy":{"sourceTable":{"projectId":"local-project","datasetId":"analytics","tableId":"people"},"destinationTable":{"projectId":"local-project","datasetId":"analytics","tableId":"people_copy"},"createDisposition":"CREATE_IF_NEEDED","writeDisposition":"WRITE_TRUNCATE"}}}"#;
    let insert = server.insert_job("local-project", body.as_bytes());
    assert_eq!(
        insert.status,
        200,
        "copy job status = {}, body = {}",
        insert.status,
        insert.body_str()
    );
    let job: JobResource = serde_json::from_slice(&insert.body).expect("decode copy job");
    assert!(
        job.kind == "bigquery#job"
            && job.job_reference.job_id == "copy_people"
            && job.status.state == "DONE",
        "copy job = {job:?}"
    );
    assert_eq!(
        job.configuration.copy.destination_table.table_id, "people_copy",
        "copy configuration = {:?}",
        job.configuration.copy
    );

    let table_rec = server.get_table("local-project", "analytics", "people_copy");
    assert_eq!(
        table_rec.status,
        200,
        "copied table status = {}, body = {}",
        table_rec.status,
        table_rec.body_str()
    );
    let table: TableResource =
        serde_json::from_slice(&table_rec.body).expect("decode copied table");
    assert!(
        table.table_reference.table_id == "people_copy"
            && table.num_rows == "2"
            && table.schema.fields.len() == 4,
        "copied table = {table:?}"
    );

    let rows = server.list_rows(
        "local-project",
        "analytics",
        "people_copy",
        &Query::parse(""),
    );
    let row_list: TableDataListResponse =
        serde_json::from_slice(&rows.body).expect("decode copied rows");
    assert!(
        row_list.total_rows == "2"
            && row_list.rows.len() == 2
            && row_list.rows[0].f[1].v == json!("Ada"),
        "copied rows = {row_list:?}"
    );
}

// legacy: TestJobsInsertCopyJobCopiesMultipleSourceTables
#[test]
fn jobs_insert_copy_job_copies_multiple_source_tables() {
    let dir = tempdir();
    let server = new_server(&dir);
    create_dataset_for_test(&server, "local-project", "analytics");
    create_table_for_test(&server, "local-project", "analytics", "people_a");
    create_table_for_test(&server, "local-project", "analytics", "people_b");
    insert_rows_for_test(&server, "local-project", "analytics", "people_a");
    insert_rows_for_test(&server, "local-project", "analytics", "people_b");

    let body = r#"{"jobReference":{"jobId":"copy_many_people","location":"US"},"configuration":{"copy":{"sourceTables":[{"projectId":"local-project","datasetId":"analytics","tableId":"people_a"},{"projectId":"local-project","datasetId":"analytics","tableId":"people_b"}],"destinationTable":{"projectId":"local-project","datasetId":"analytics","tableId":"people_many"},"createDisposition":"CREATE_IF_NEEDED","writeDisposition":"WRITE_TRUNCATE"}}}"#;
    let insert = server.insert_job("local-project", body.as_bytes());
    assert_eq!(
        insert.status,
        200,
        "copy job status = {}, body = {}",
        insert.status,
        insert.body_str()
    );
    let job: JobResource = serde_json::from_slice(&insert.body).expect("decode copy job");
    assert!(
        job.configuration.copy.source_tables.len() == 2
            && job.configuration.copy.destination_table.table_id == "people_many",
        "copy configuration = {:?}",
        job.configuration.copy
    );

    let rows = server.list_rows(
        "local-project",
        "analytics",
        "people_many",
        &Query::parse(""),
    );
    let row_list: TableDataListResponse =
        serde_json::from_slice(&rows.body).expect("decode copied rows");
    assert!(
        row_list.total_rows == "4" && row_list.rows.len() == 4,
        "copied rows = {row_list:?}"
    );
}

// legacy: TestJobsInsertCopyJobHonorsCreateNeverAndWriteEmpty
#[test]
fn jobs_insert_copy_job_honors_create_never_and_write_empty() {
    let dir = tempdir();
    let server = new_server(&dir);
    create_dataset_for_test(&server, "local-project", "analytics");
    create_table_for_test(&server, "local-project", "analytics", "people");
    insert_rows_for_test(&server, "local-project", "analytics", "people");

    let create_never_body = r#"{"configuration":{"copy":{"sourceTable":{"datasetId":"analytics","tableId":"people"},"destinationTable":{"datasetId":"analytics","tableId":"missing"},"createDisposition":"CREATE_NEVER"}}}"#;
    let create_never = server.insert_job("local-project", create_never_body.as_bytes());
    assert_eq!(
        create_never.status,
        400,
        "CREATE_NEVER status = {}, body = {}",
        create_never.status,
        create_never.body_str()
    );

    create_table_for_test(&server, "local-project", "analytics", "occupied");
    insert_rows_for_test(&server, "local-project", "analytics", "occupied");
    let write_empty_body = r#"{"configuration":{"copy":{"sourceTable":{"datasetId":"analytics","tableId":"people"},"destinationTable":{"datasetId":"analytics","tableId":"occupied"},"writeDisposition":"WRITE_EMPTY"}}}"#;
    let write_empty = server.insert_job("local-project", write_empty_body.as_bytes());
    assert_eq!(
        write_empty.status,
        400,
        "WRITE_EMPTY status = {}, body = {}",
        write_empty.status,
        write_empty.body_str()
    );
}

// --- minimal tempdir --------------------------------------------------------

fn tempdir() -> std::path::PathBuf {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let mut dir = std::env::temp_dir();
    dir.push(format!("devcloud-bq-jobs-{}-{}", std::process::id(), n));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create tempdir");
    dir
}
