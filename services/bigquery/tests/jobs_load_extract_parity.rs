//! 1:1 port of `internal/services/bigquery/jobs_load_extract_test.rs`.
//!
//! Load/extract jobs read/write `gs://` URIs through the shared S3
//! `FileBucketStore` (the same on-disk layout the legacy `s3svc.BucketStore`
//! uses). The upload-load test drives the multipart route through
//! `routes::handle` exactly like legacy drives `/upload/bigquery/v2/...`.

use devcloud_bigquery::model::{JobResource, TableDataListResponse, TableResource};
use devcloud_bigquery::routes::{self, Request};
use devcloud_bigquery::server::{Config, Server};
use devcloud_bigquery::validation::Query;
use devcloud_s3::objops::PutObjectInput;
use devcloud_s3::store::FileBucketStore;

fn new_server_with_store(dir: &std::path::Path, store: FileBucketStore) -> Server {
    Server::new(Config {
        project: "local-project".to_string(),
        location: "US".to_string(),
        storage_path: dir.to_string_lossy().into_owned(),
        ..Default::default()
    })
    .with_object_store(store)
}

fn new_object_store(dir: &std::path::Path, bucket: &str) -> FileBucketStore {
    let store = FileBucketStore::new(dir.to_string_lossy().into_owned());
    let (_, created) = store.create_bucket(bucket).expect("create fixture bucket");
    assert!(created, "create fixture bucket: created={created}");
    store
}

fn put_fixture_object(store: &FileBucketStore, bucket: &str, key: &str, body: &str, ct: &str) {
    store
        .put_object(PutObjectInput {
            bucket: bucket.to_string(),
            key: key.to_string(),
            body: body.as_bytes().to_vec(),
            content_type: ct.to_string(),
            ..Default::default()
        })
        .expect("put fixture object");
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

// legacy: TestJobsInsertLoadJobReadsGCSNDJSON
#[test]
fn jobs_insert_load_job_reads_gcs_ndjson() {
    let store_dir = tempdir();
    let object_store = new_object_store(&store_dir, "bq-fixtures");
    put_fixture_object(
        &object_store,
        "bq-fixtures",
        "people.ndjson",
        "{\"id\":\"3\",\"name\":\"Katherine\",\"age\":42,\"active\":true}\n{\"id\":\"4\",\"name\":\"Edsger\",\"age\":\"51\",\"active\":false}\n",
        "application/x-ndjson",
    );

    let dir = tempdir();
    let server = new_server_with_store(&dir, object_store);
    create_dataset_for_test(&server, "local-project", "analytics");
    create_table_for_test(&server, "local-project", "analytics", "people");

    let body = r#"{"jobReference":{"jobId":"load_people","location":"US"},"configuration":{"load":{"sourceUris":["gs://bq-fixtures/people.ndjson"],"destinationTable":{"datasetId":"analytics","tableId":"people"},"sourceFormat":"NEWLINE_DELIMITED_JSON","writeDisposition":"WRITE_APPEND"}}}"#;
    let insert = server.insert_job("local-project", body.as_bytes());
    assert_eq!(
        insert.status,
        200,
        "load job status = {}, body = {}",
        insert.status,
        insert.body_str()
    );
    let job: JobResource = serde_json::from_slice(&insert.body).expect("decode load job");
    assert!(
        job.kind == "bigquery#job"
            && job.job_reference.job_id == "load_people"
            && job.status.state == "DONE",
        "load job = {job:?}"
    );
    assert_eq!(
        job.configuration.load.destination_table.table_id, "people",
        "load configuration = {:?}",
        job.configuration.load
    );

    let rows = server.list_rows("local-project", "analytics", "people", &Query::parse(""));
    let row_list: TableDataListResponse =
        serde_json::from_slice(&rows.body).expect("decode loaded rows");
    assert!(
        row_list.total_rows == "2"
            && row_list.rows.len() == 2
            && row_list.rows[0].f[1].v == serde_json::json!("Katherine"),
        "loaded rows = {row_list:?}"
    );
}

// legacy: TestJobsInsertLoadJobCanCreateDestinationTableWithSchema
#[test]
fn jobs_insert_load_job_can_create_destination_table_with_schema() {
    let store_dir = tempdir();
    let object_store = new_object_store(&store_dir, "bq-fixtures");
    put_fixture_object(
        &object_store,
        "bq-fixtures",
        "new-people.ndjson",
        "{\"id\":\"5\",\"name\":\"Dorothy\",\"age\":36,\"active\":true}\n",
        "application/x-ndjson",
    );

    let dir = tempdir();
    let server = new_server_with_store(&dir, object_store);
    create_dataset_for_test(&server, "local-project", "analytics");

    let body = r#"{"jobReference":{"jobId":"load_new_people","location":"US"},"configuration":{"load":{"sourceUris":["gs://bq-fixtures/new-people.ndjson"],"destinationTable":{"datasetId":"analytics","tableId":"new_people"},"schema":{"fields":[{"name":"id","type":"STRING","mode":"REQUIRED"},{"name":"name","type":"STRING"},{"name":"age","type":"INTEGER"},{"name":"active","type":"BOOLEAN"}]},"sourceFormat":"NEWLINE_DELIMITED_JSON","createDisposition":"CREATE_IF_NEEDED","writeDisposition":"WRITE_APPEND"}}}"#;
    let insert = server.insert_job("local-project", body.as_bytes());
    assert_eq!(
        insert.status,
        200,
        "load job status = {}, body = {}",
        insert.status,
        insert.body_str()
    );

    let table_rec = server.get_table("local-project", "analytics", "new_people");
    assert_eq!(
        table_rec.status,
        200,
        "created table status = {}, body = {}",
        table_rec.status,
        table_rec.body_str()
    );
    let table: TableResource =
        serde_json::from_slice(&table_rec.body).expect("decode created table");
    assert!(
        table.table_reference.table_id == "new_people"
            && table.num_rows == "1"
            && table.schema.fields.len() == 4,
        "created load table = {table:?}"
    );

    let create_never_body = r#"{"configuration":{"load":{"sourceUris":["gs://bq-fixtures/new-people.ndjson"],"destinationTable":{"datasetId":"analytics","tableId":"missing"},"schema":{"fields":[{"name":"id","type":"STRING"}]},"sourceFormat":"NEWLINE_DELIMITED_JSON","createDisposition":"CREATE_NEVER"}}}"#;
    let create_never = server.insert_job("local-project", create_never_body.as_bytes());
    assert_eq!(
        create_never.status,
        400,
        "CREATE_NEVER status = {}, body = {}",
        create_never.status,
        create_never.body_str()
    );
}

// legacy: TestJobsInsertLoadJobReadsGCSCSV
#[test]
fn jobs_insert_load_job_reads_gcs_csv() {
    let store_dir = tempdir();
    let object_store = new_object_store(&store_dir, "bq-fixtures");
    put_fixture_object(
        &object_store,
        "bq-fixtures",
        "people.csv",
        "6,Barbara,39,true\n7,Donald,44,false\n",
        "text/csv",
    );

    let dir = tempdir();
    let server = new_server_with_store(&dir, object_store);
    create_dataset_for_test(&server, "local-project", "analytics");
    create_table_for_test(&server, "local-project", "analytics", "people");

    let body = r#"{"jobReference":{"jobId":"load_people_csv","location":"US"},"configuration":{"load":{"sourceUris":["gs://bq-fixtures/people.csv"],"destinationTable":{"datasetId":"analytics","tableId":"people"},"sourceFormat":"CSV","writeDisposition":"WRITE_APPEND"}}}"#;
    let insert = server.insert_job("local-project", body.as_bytes());
    assert_eq!(
        insert.status,
        200,
        "CSV load job status = {}, body = {}",
        insert.status,
        insert.body_str()
    );
    let job: JobResource = serde_json::from_slice(&insert.body).expect("decode CSV load job");
    assert!(
        job.kind == "bigquery#job"
            && job.job_reference.job_id == "load_people_csv"
            && job.status.state == "DONE",
        "CSV load job = {job:?}"
    );

    let rows = server.list_rows("local-project", "analytics", "people", &Query::parse(""));
    let row_list: TableDataListResponse =
        serde_json::from_slice(&rows.body).expect("decode CSV loaded rows");
    assert!(
        row_list.total_rows == "2"
            && row_list.rows.len() == 2
            && row_list.rows[0].f[1].v == serde_json::json!("Barbara")
            && row_list.rows[0].f[2].v == serde_json::json!("39"),
        "CSV loaded rows = {row_list:?}"
    );
}

// legacy: TestJobsInsertLoadJobCSVSkipsLeadingRows
#[test]
fn jobs_insert_load_job_csv_skips_leading_rows() {
    let store_dir = tempdir();
    let object_store = new_object_store(&store_dir, "bq-fixtures");
    put_fixture_object(
        &object_store,
        "bq-fixtures",
        "people-with-header.csv",
        "id,name,age,active\n8,Joan,41,true\n",
        "text/csv",
    );

    let dir = tempdir();
    let server = new_server_with_store(&dir, object_store);
    create_dataset_for_test(&server, "local-project", "analytics");
    create_table_for_test(&server, "local-project", "analytics", "people");

    let body = r#"{"jobReference":{"jobId":"load_people_csv_header","location":"US"},"configuration":{"load":{"sourceUris":["gs://bq-fixtures/people-with-header.csv"],"destinationTable":{"datasetId":"analytics","tableId":"people"},"sourceFormat":"CSV","skipLeadingRows":1,"writeDisposition":"WRITE_APPEND"}}}"#;
    let insert = server.insert_job("local-project", body.as_bytes());
    assert_eq!(
        insert.status,
        200,
        "CSV load job status = {}, body = {}",
        insert.status,
        insert.body_str()
    );
    let job: JobResource = serde_json::from_slice(&insert.body).expect("decode CSV load job");
    assert_eq!(
        job.configuration.load.skip_leading_rows, 1,
        "skipLeadingRows = {}",
        job.configuration.load.skip_leading_rows
    );

    let rows = server.list_rows("local-project", "analytics", "people", &Query::parse(""));
    let row_list: TableDataListResponse =
        serde_json::from_slice(&rows.body).expect("decode CSV loaded rows");
    assert!(
        row_list.total_rows == "1"
            && row_list.rows.len() == 1
            && row_list.rows[0].f[0].v == serde_json::json!("8")
            && row_list.rows[0].f[1].v == serde_json::json!("Joan"),
        "CSV loaded rows = {row_list:?}"
    );
}

// legacy: TestJobsInsertUploadLoadJobReadsMultipartNDJSON
#[test]
fn jobs_insert_upload_load_job_reads_multipart_ndjson() {
    let dir = tempdir();
    let server = Server::new(Config {
        project: "local-project".to_string(),
        location: "US".to_string(),
        storage_path: dir.to_string_lossy().into_owned(),
        ..Default::default()
    });
    create_dataset_for_test(&server, "local-project", "analytics");
    create_table_for_test(&server, "local-project", "analytics", "people");

    // The same wire bytes legacy mime/multipart writer produces.
    let boundary = "11dee1fab14b3cf66ba1677d2c0d";
    let metadata = r#"{"jobReference":{"jobId":"upload_load_people","location":"US"},"configuration":{"load":{"destinationTable":{"datasetId":"analytics","tableId":"people"},"sourceFormat":"NEWLINE_DELIMITED_JSON","writeDisposition":"WRITE_APPEND"}}}"#;
    let media = "{\"id\":\"5\",\"name\":\"Dorothy\",\"age\":36,\"active\":true}\n";
    let body = format!(
        "--{boundary}\r\nContent-Type: application/json; charset=UTF-8\r\n\r\n{metadata}\r\n--{boundary}\r\nContent-Type: application/octet-stream\r\n\r\n{media}\r\n--{boundary}--\r\n"
    );

    let mut request = Request::new(
        "POST",
        "/upload/bigquery/v2/projects/local-project/jobs?uploadType=multipart",
        body.as_bytes(),
    );
    request.content_type = format!("multipart/form-data; boundary={boundary}");
    let insert = routes::handle(&server, &request);
    assert_eq!(
        insert.status,
        200,
        "upload load job status = {}, body = {}",
        insert.status,
        insert.body_str()
    );
    let job: JobResource = serde_json::from_slice(&insert.body).expect("decode upload load job");
    assert!(
        job.kind == "bigquery#job"
            && job.job_reference.job_id == "upload_load_people"
            && job.status.state == "DONE",
        "upload load job = {job:?}"
    );

    let rows = server.list_rows("local-project", "analytics", "people", &Query::parse(""));
    let row_list: TableDataListResponse =
        serde_json::from_slice(&rows.body).expect("decode uploaded rows");
    assert!(
        row_list.total_rows == "1"
            && row_list.rows.len() == 1
            && row_list.rows[0].f[1].v == serde_json::json!("Dorothy"),
        "uploaded rows = {row_list:?}"
    );
}

// legacy: TestJobsInsertExtractJobWritesGCSNDJSON
#[test]
fn jobs_insert_extract_job_writes_gcs_ndjson() {
    let store_dir = tempdir();
    let object_store = new_object_store(&store_dir, "bq-exports");
    let dir = tempdir();
    let server = new_server_with_store(&dir, object_store);
    create_dataset_for_test(&server, "local-project", "analytics");
    create_table_for_test(&server, "local-project", "analytics", "people");
    insert_rows_for_test(&server, "local-project", "analytics", "people");

    let body = r#"{"jobReference":{"jobId":"extract_people","location":"US"},"configuration":{"extract":{"sourceTable":{"datasetId":"analytics","tableId":"people"},"destinationUris":["gs://bq-exports/people.ndjson"],"destinationFormat":"NEWLINE_DELIMITED_JSON"}}}"#;
    let insert = server.insert_job("local-project", body.as_bytes());
    assert_eq!(
        insert.status,
        200,
        "extract job status = {}, body = {}",
        insert.status,
        insert.body_str()
    );
    let job: JobResource = serde_json::from_slice(&insert.body).expect("decode extract job");
    assert!(
        job.kind == "bigquery#job"
            && job.job_reference.job_id == "extract_people"
            && job.status.state == "DONE",
        "extract job = {job:?}"
    );

    let verify_store = FileBucketStore::new(store_dir.to_string_lossy().into_owned());
    let (object, body_bytes) = verify_store
        .get_object("bq-exports", "people.ndjson")
        .expect("get exported object")
        .expect("exported object found");
    assert_eq!(
        object.content_type, "application/x-ndjson",
        "export content type = {:?}",
        object.content_type
    );
    let body_string = String::from_utf8(body_bytes).expect("utf-8 export");
    assert!(
        body_string.contains(r#""name":"Ada""#) && body_string.contains(r#""name":"Grace""#),
        "export body missing expected rows: {body_string}"
    );
}

// legacy: TestJobsInsertExtractJobWritesGCSCSV
#[test]
fn jobs_insert_extract_job_writes_gcs_csv() {
    let store_dir = tempdir();
    let object_store = new_object_store(&store_dir, "bq-exports");
    let dir = tempdir();
    let server = new_server_with_store(&dir, object_store);
    create_dataset_for_test(&server, "local-project", "analytics");
    create_table_for_test(&server, "local-project", "analytics", "people");
    insert_rows_for_test(&server, "local-project", "analytics", "people");

    let body = r#"{"jobReference":{"jobId":"extract_people_csv","location":"US"},"configuration":{"extract":{"sourceTable":{"datasetId":"analytics","tableId":"people"},"destinationUris":["gs://bq-exports/people.csv"],"destinationFormat":"CSV"}}}"#;
    let insert = server.insert_job("local-project", body.as_bytes());
    assert_eq!(
        insert.status,
        200,
        "CSV extract job status = {}, body = {}",
        insert.status,
        insert.body_str()
    );
    let job: JobResource = serde_json::from_slice(&insert.body).expect("decode CSV extract job");
    assert!(
        job.kind == "bigquery#job"
            && job.job_reference.job_id == "extract_people_csv"
            && job.status.state == "DONE",
        "CSV extract job = {job:?}"
    );

    let verify_store = FileBucketStore::new(store_dir.to_string_lossy().into_owned());
    let (object, body_bytes) = verify_store
        .get_object("bq-exports", "people.csv")
        .expect("get CSV exported object")
        .expect("CSV exported object found");
    assert_eq!(
        object.content_type, "text/csv",
        "CSV export content type = {:?}",
        object.content_type
    );
    let body_string = String::from_utf8(body_bytes).expect("utf-8 export");
    assert!(
        body_string.contains("1,Ada,37,true") && body_string.contains("2,Grace,31,true"),
        "CSV export body missing expected rows: {body_string}"
    );
}

// --- minimal tempdir --------------------------------------------------------

fn tempdir() -> std::path::PathBuf {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let mut dir = std::env::temp_dir();
    dir.push(format!("devcloud-bq-loadex-{}-{}", std::process::id(), n));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create tempdir");
    dir
}
