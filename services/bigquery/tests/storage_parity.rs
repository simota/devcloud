//! Byte-compatibility tests for the persistence layer and response encoding
//! against golden oracles captured from the legacy bigquery service (the fixtures
//! under `tests/fixtures/` are verbatim files/bodies produced by
//! `internal/services/bigquery` driven over ServeHTTP).

use devcloud_bigquery::model::{DatasetResource, IamPolicy};
use devcloud_bigquery::responses::ApiResponse;
use devcloud_bigquery::server::{Config, Server};
use devcloud_bigquery::wire_json;

const DATASET_JSON: &[u8] = include_bytes!("fixtures/dataset.json");
const IAM_POLICY_JSON: &[u8] = include_bytes!("fixtures/iam_policy.json");
const TABLE_JSON: &[u8] = include_bytes!("fixtures/table.json");
const STREAMING_BUFFER_JSONL: &[u8] = include_bytes!("fixtures/streaming_buffer.jsonl");
const JOB_JSON: &[u8] = include_bytes!("fixtures/job.json");
const RESPONSE_CREATE_DATASET: &[u8] = include_bytes!("fixtures/response_create_dataset.json");
const RESPONSE_SET_IAM: &[u8] = include_bytes!("fixtures/response_set_iam.json");
const RESPONSE_MISSING_DATASET: &[u8] = include_bytes!("fixtures/response_missing_dataset.json");

fn new_server(dir: &std::path::Path) -> Server {
    Server::new(Config {
        project: "local-project".to_string(),
        location: "EU".to_string(),
        storage_path: dir.to_string_lossy().into_owned(),
        ..Default::default()
    })
}

fn plant(path: &std::path::Path, data: &[u8]) {
    std::fs::create_dir_all(path.parent().expect("parent")).expect("mkdir");
    std::fs::write(path, data).expect("write fixture");
}

fn assert_bytes_eq(got: &[u8], want: &[u8], label: &str) {
    assert_eq!(
        String::from_utf8_lossy(got),
        String::from_utf8_lossy(want),
        "{label}: re-encoded bytes must match the legacy oracle byte-for-byte"
    );
}

#[test]
fn dataset_json_round_trips_byte_for_byte() {
    let dir = tempdir();
    let server = new_server(&dir);
    let path = server.dataset_path("local-project", "analytics");
    plant(&path, DATASET_JSON);

    let dataset = server
        .read_dataset("local-project", "analytics")
        .expect("read dataset")
        .expect("dataset present");
    assert_eq!(dataset.dataset_reference.dataset_id, "analytics");
    assert_eq!(dataset.location, "EU");

    server.write_dataset(&dataset).expect("write dataset");
    let rewritten = std::fs::read(&path).expect("reread dataset.json");
    assert_bytes_eq(&rewritten, DATASET_JSON, "dataset.json");
}

#[test]
fn iam_policy_round_trips_byte_for_byte() {
    let dir = tempdir();
    let server = new_server(&dir);
    let path = server.dataset_iam_policy_path("local-project", "analytics");
    plant(&path, IAM_POLICY_JSON);

    let policy = server.read_iam_policy(&path).expect("read policy");
    assert_eq!(policy.version, 1);
    assert_eq!(policy.bindings.len(), 1);

    server
        .write_iam_policy(&path, &policy)
        .expect("write policy");
    let rewritten = std::fs::read(&path).expect("reread iam-policy.json");
    assert_bytes_eq(&rewritten, IAM_POLICY_JSON, "iam-policy.json");

    // The legacy setIamPolicy handler responds with the same struct through
    // writeJSON (compact + newline).
    assert_bytes_eq(
        &wire_json::to_vec(&policy),
        RESPONSE_SET_IAM,
        "setIamPolicy body",
    );
}

#[test]
fn missing_iam_policy_reads_as_default() {
    let dir = tempdir();
    let server = new_server(&dir);
    let path = server.dataset_iam_policy_path("local-project", "nope");
    let policy: IamPolicy = server.read_iam_policy(&path).expect("default policy");
    assert_eq!(policy.version, 1);
    assert_eq!(policy.etag, "\"0\"");
    assert!(policy.bindings.is_empty());
}

#[test]
fn table_json_round_trips_byte_for_byte() {
    let dir = tempdir();
    let server = new_server(&dir);
    let path = server.table_path("local-project", "analytics", "people");
    plant(&path, TABLE_JSON);

    let table = server
        .read_table("local-project", "analytics", "people")
        .expect("read table")
        .expect("table present");
    assert_eq!(table.table_reference.table_id, "people");
    assert_eq!(table.schema.fields.len(), 4);
    assert_eq!(table.num_rows, "2");

    server.write_table(&table).expect("write table");
    let rewritten = std::fs::read(&path).expect("reread table.json");
    assert_bytes_eq(&rewritten, TABLE_JSON, "table.json");
}

#[test]
fn streaming_buffer_round_trips_byte_for_byte() {
    let dir = tempdir();
    let server = new_server(&dir);
    let path = server.rows_path("local-project", "analytics", "people");
    plant(&path, STREAMING_BUFFER_JSONL);

    let rows = server
        .read_rows("local-project", "analytics", "people")
        .expect("read rows");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].insert_id, "row-1");
    // legacy preserves the raw number literal "1.50".
    assert_eq!(rows[0].json.get("score").expect("score").get(), "1.50");
    assert_eq!(rows[1].insert_id, "");

    // Re-appending the same rows into a fresh store must reproduce the file.
    let dir2 = tempdir();
    let server2 = new_server(&dir2);
    server2
        .append_rows("local-project", "analytics", "people", &rows)
        .expect("append rows");
    let rewritten = std::fs::read(server2.rows_path("local-project", "analytics", "people"))
        .expect("reread streaming buffer");
    assert_bytes_eq(&rewritten, STREAMING_BUFFER_JSONL, "streaming-buffer.jsonl");
}

#[test]
fn query_job_round_trips_byte_for_byte() {
    let dir = tempdir();
    let server = new_server(&dir);
    let path = server.query_job_path("local-project", "job-1");
    plant(&path, JOB_JSON);

    let job = server
        .read_query_job("local-project", "job-1")
        .expect("read job")
        .expect("job present");
    assert_eq!(job.job.job_reference.job_id, "job-1");
    assert_eq!(job.job.status.state, "DONE");
    assert_eq!(job.response.total_rows, "2");

    server
        .write_query_job("local-project", "job-1", &job)
        .expect("write job");
    let rewritten = std::fs::read(&path).expect("reread job json");
    assert_bytes_eq(&rewritten, JOB_JSON, "jobs/job-1.json");

    let records = server
        .read_query_job_records("local-project")
        .expect("read job records");
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].job.job_reference.job_id, "job-1");
}

#[test]
fn create_dataset_response_encodes_like_legacy() {
    // The captured legacy HTTP body re-encodes byte-for-byte through the model +
    // wire_json (declaration-ordered fields, sorted labels, HTML escaping,
    // compact + trailing newline).
    let created: DatasetResource =
        serde_json::from_slice(RESPONSE_CREATE_DATASET).expect("decode create body");
    assert_bytes_eq(
        &wire_json::to_vec(&created),
        RESPONSE_CREATE_DATASET,
        "createDataset body",
    );
}

#[test]
fn error_envelope_matches_legacy_bytes() {
    let response = ApiResponse::error(404, "notFound", "Not found: Dataset local-project:missing");
    assert_eq!(response.status, 404);
    assert_bytes_eq(&response.body, RESPONSE_MISSING_DATASET, "error envelope");
}

#[test]
fn dataset_files_created_by_handlers_land_in_legacy_layout() {
    let dir = tempdir();
    let server = new_server(&dir);
    let create = server.create_dataset(
        "local-project",
        br#"{"datasetReference":{"datasetId":"analytics"}}"#,
    );
    assert_eq!(create.status, 200);
    assert!(dir
        .join("projects/local-project/datasets/analytics/dataset.json")
        .is_file());
}

// --- minimal tempdir --------------------------------------------------------

fn tempdir() -> std::path::PathBuf {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let mut dir = std::env::temp_dir();
    dir.push(format!("devcloud-bq-st-{}-{}", std::process::id(), n));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create tempdir");
    dir
}
