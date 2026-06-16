//! 1:1 port of `internal/services/bigquery/iam_test.rs`, driven through the
//! routing layer (the legacy test exercises the `:getIamPolicy` / `:setIamPolicy`
//! / `:testIamPermissions` action suffixes via `server.routes()`).

use devcloud_bigquery::model::IamPolicy;
use devcloud_bigquery::routes::{handle, Request};
use devcloud_bigquery::server::{Config, Server};

fn post(server: &Server, target: &str, body: &str) -> devcloud_bigquery::responses::ApiResponse {
    handle(server, &Request::new("POST", target, body.as_bytes()))
}

// legacy: TestDatasetAndTableIAMPolicyCompatibilityStubs
#[test]
fn dataset_and_table_iam_policy_compatibility_stubs() {
    let dir = tempdir();
    let server = Server::new(Config {
        project: "local-project".to_string(),
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

    let get_default = post(
        &server,
        "/bigquery/v2/projects/local-project/datasets/analytics:getIamPolicy",
        "{}",
    );
    assert_eq!(
        get_default.status,
        200,
        "default dataset policy status = {}, body = {}",
        get_default.status,
        get_default.body_str()
    );
    let default_policy: IamPolicy =
        serde_json::from_slice(&get_default.body).expect("decode default policy");
    assert!(
        default_policy.version == 1
            && default_policy.bindings.is_empty()
            && !default_policy.etag.is_empty(),
        "default policy = {default_policy:?}"
    );

    let set_body = r#"{"policy":{"version":1,"bindings":[{"role":"roles/bigquery.dataViewer","members":["user:local@example.com"]}]}}"#;
    let set_table = post(
        &server,
        "/bigquery/v2/projects/local-project/datasets/analytics/tables/people:setIamPolicy",
        set_body,
    );
    assert_eq!(
        set_table.status,
        200,
        "set table policy status = {}, body = {}",
        set_table.status,
        set_table.body_str()
    );
    let set_policy: IamPolicy = serde_json::from_slice(&set_table.body).expect("decode set policy");
    assert!(
        set_policy.bindings.len() == 1
            && set_policy.bindings[0].role == "roles/bigquery.dataViewer"
            && !set_policy.etag.is_empty(),
        "set policy = {set_policy:?}"
    );

    let get_table = post(
        &server,
        "/bigquery/v2/projects/local-project/datasets/analytics/tables/people:getIamPolicy",
        "{}",
    );
    assert!(
        get_table.status == 200 && get_table.body_str().contains("roles/bigquery.dataViewer"),
        "persisted table policy status/body = {} {}",
        get_table.status,
        get_table.body_str()
    );

    let permissions_body = r#"{"permissions":["bigquery.tables.get","bigquery.tables.update"]}"#;
    let test_permissions = post(
        &server,
        "/bigquery/v2/projects/local-project/datasets/analytics/tables/people:testIamPermissions",
        permissions_body,
    );
    assert!(
        test_permissions.status == 200
            && test_permissions
                .body_str()
                .contains("bigquery.tables.update"),
        "test permissions status/body = {} {}",
        test_permissions.status,
        test_permissions.body_str()
    );
}

// --- minimal tempdir --------------------------------------------------------

fn tempdir() -> std::path::PathBuf {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let mut dir = std::env::temp_dir();
    dir.push(format!("devcloud-bq-iam-{}-{}", std::process::id(), n));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create tempdir");
    dir
}
