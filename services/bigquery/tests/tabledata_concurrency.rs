//! OMEN-8 regression coverage: `insertAll`'s insertId dedup and
//! `maxRowsPerTable` quota check must serialize with the append, or two
//! concurrent requests against the same table can both snapshot the same
//! pre-mutation row set, both pass the check, and both append — duplicating
//! an insertId or overshooting the row quota.
//!
//! These drive `Server::insert_rows` from real OS threads (the same
//! entry point `services/bigquery/src/http.rs` calls per connection) with a
//! `Barrier` to line the requests up on the same instant, so the race window
//! is exercised on every run rather than left to scheduler luck.

use std::sync::{Arc, Barrier};
use std::thread;

use devcloud_bigquery::model::{InsertAllResponse, TableResource};
use devcloud_bigquery::server::{Config, Server};

fn tempdir() -> std::path::PathBuf {
    let mut dir = std::env::temp_dir();
    dir.push(format!(
        "devcloud-bigquery-tabledata-concurrency-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).expect("create temp dir");
    dir
}

fn new_server(dir: &std::path::Path, max_rows_per_table: i64) -> Arc<Server> {
    Arc::new(Server::new(Config {
        project: "local-project".to_string(),
        storage_path: dir.to_string_lossy().into_owned(),
        max_rows_per_table,
        ..Default::default()
    }))
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
        "{{\"tableReference\":{{\"tableId\":\"{table_id}\"}},\"schema\":{{\"fields\":[{{\"name\":\"id\",\"type\":\"STRING\"}}]}}}}"
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

fn get_table_for_test(
    server: &Server,
    project_id: &str,
    dataset_id: &str,
    table_id: &str,
) -> TableResource {
    let response = server.get_table(project_id, dataset_id, table_id);
    assert_eq!(
        response.status,
        200,
        "get table status = {}, body = {}",
        response.status,
        response.body_str()
    );
    serde_json::from_slice(&response.body).expect("decode table")
}

#[test]
fn concurrent_insert_all_dedups_shared_insert_id_across_requests() {
    let dir = tempdir();
    let server = new_server(&dir, 0);
    create_dataset_for_test(&server, "local-project", "analytics");
    create_table_for_test(&server, "local-project", "analytics", "people");

    const THREADS: usize = 16;
    let barrier = Arc::new(Barrier::new(THREADS));
    let handles: Vec<_> = (0..THREADS)
        .map(|i| {
            let server = Arc::clone(&server);
            let barrier = Arc::clone(&barrier);
            thread::spawn(move || {
                let body =
                    format!(r#"{{"rows":[{{"insertId":"shared-id","json":{{"id":"{i}"}}}}]}}"#);
                barrier.wait();
                server.insert_rows("local-project", "analytics", "people", body.as_bytes())
            })
        })
        .collect();

    for handle in handles {
        let response = handle.join().expect("insertAll thread panicked");
        assert_eq!(
            response.status,
            200,
            "insertAll status = {}, body = {}",
            response.status,
            response.body_str()
        );
        let decoded: InsertAllResponse =
            serde_json::from_slice(&response.body).expect("decode insertAll response");
        assert!(
            decoded.insert_errors.is_empty(),
            "insertId dedup is silent, not an error: {decoded:?}"
        );
    }

    let table = get_table_for_test(&server, "local-project", "analytics", "people");
    assert_eq!(
        table.num_rows, "1",
        "16 concurrent requests sharing one insertId must dedup to exactly one row, table = {table:?}"
    );
}

#[test]
fn concurrent_insert_all_never_exceeds_row_quota() {
    let dir = tempdir();
    const CAP: i64 = 5;
    const THREADS: usize = 20;
    let server = new_server(&dir, CAP);
    create_dataset_for_test(&server, "local-project", "analytics");
    create_table_for_test(&server, "local-project", "analytics", "people");

    let barrier = Arc::new(Barrier::new(THREADS));
    let handles: Vec<_> = (0..THREADS)
        .map(|i| {
            let server = Arc::clone(&server);
            let barrier = Arc::clone(&barrier);
            thread::spawn(move || {
                let body = format!(r#"{{"rows":[{{"insertId":"id-{i}","json":{{"id":"{i}"}}}}]}}"#);
                barrier.wait();
                server.insert_rows("local-project", "analytics", "people", body.as_bytes())
            })
        })
        .collect();

    let mut accepted = 0;
    let mut quota_exceeded = 0;
    for handle in handles {
        let response = handle.join().expect("insertAll thread panicked");
        match response.status {
            200 => accepted += 1,
            400 => quota_exceeded += 1,
            other => panic!(
                "unexpected insertAll status {other}, body = {}",
                response.body_str()
            ),
        }
    }
    assert_eq!(
        accepted, CAP as usize,
        "exactly the quota cap should be accepted under concurrent load"
    );
    assert_eq!(quota_exceeded, THREADS - CAP as usize);

    let table = get_table_for_test(&server, "local-project", "analytics", "people");
    assert_eq!(
        table.num_rows,
        CAP.to_string(),
        "row quota must never be overshot by concurrent requests, table = {table:?}"
    );
}
