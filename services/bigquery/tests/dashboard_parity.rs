//! 1:1 port of `internal/services/bigquery/dashboard_test.rs`.

use devcloud_bigquery::server::{Config, Server};

// legacy: TestSnapshotsExposeDatasetsTablesRowsAndJobs
#[test]
fn snapshots_expose_datasets_tables_rows_and_jobs() {
    let dir = tempdir();
    let server = Server::new(Config {
        project: "local-project".to_string(),
        location: "US".to_string(),
        storage_path: dir.to_string_lossy().into_owned(),
        ..Default::default()
    });
    let created = server.create_dataset(
        "local-project",
        br#"{"datasetReference":{"datasetId":"analytics"}}"#,
    );
    assert_eq!(
        created.status,
        200,
        "create dataset: {}",
        created.body_str()
    );
    let table = server.create_table(
        "local-project",
        "analytics",
        br#"{"tableReference":{"tableId":"people"},"schema":{"fields":[{"name":"id","type":"STRING","mode":"REQUIRED"},{"name":"name","type":"STRING"},{"name":"age","type":"INTEGER"},{"name":"active","type":"BOOLEAN"}]}}"#,
    );
    assert_eq!(table.status, 200, "create table: {}", table.body_str());
    let rows = server.insert_rows(
        "local-project",
        "analytics",
        "people",
        br#"{"rows":[{"insertId":"row-1","json":{"id":"1","name":"Ada","age":37,"active":true}},{"insertId":"row-2","json":{"id":"2","name":"Grace","age":31,"active":true}}]}"#,
    );
    assert_eq!(rows.status, 200, "insert rows: {}", rows.body_str());
    let job = server.insert_job(
        "local-project",
        br#"{"jobReference":{"jobId":"snapshot_job","location":"US"},"configuration":{"query":{"query":"SELECT id, age FROM `local-project.analytics.people` WHERE age >= 30 ORDER BY id","useLegacySql":false}}}"#,
    );
    assert_eq!(job.status, 200, "insert query job: {}", job.body_str());

    let snapshot = server.snapshot();
    assert!(
        snapshot.running
            && snapshot.datasets.len() == 1
            && snapshot.datasets[0].tables.len() == 1
            && snapshot.jobs.len() == 1,
        "snapshot = {snapshot:?}"
    );

    let dataset = server
        .dataset_snapshot("local-project", "analytics")
        .expect("dataset snapshot found");
    assert!(
        dataset.dataset_id == "analytics" && dataset.tables.len() == 1,
        "dataset snapshot = {dataset:?}"
    );
    let table = server
        .table_snapshot_for("local-project", "analytics", "people", 1)
        .expect("table snapshot found");
    assert!(
        table.table_id == "people"
            && table.rows.len() == 1
            && table.rows[0].json["name"] == serde_json::json!("Ada"),
        "table snapshot = {table:?}"
    );
    let job = server
        .job_snapshot("local-project", "snapshot_job")
        .expect("job snapshot found");
    assert!(
        job.job_id == "snapshot_job" && job.state == "DONE",
        "job snapshot = {job:?}"
    );
    assert!(
        server
            .dataset_snapshot("local-project", "missing")
            .is_none(),
        "missing dataset snapshot found"
    );
}

// --- minimal tempdir --------------------------------------------------------

fn tempdir() -> std::path::PathBuf {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let mut dir = std::env::temp_dir();
    dir.push(format!("devcloud-bq-dash-{}-{}", std::process::id(), n));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create tempdir");
    dir
}
