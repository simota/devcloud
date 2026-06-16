//! 1:1 port of `internal/services/bigquery/dataset_test.rs`.
//!
//! The legacy tests drive `server.routes().ServeHTTP`; the routing layer arrives
//! in part 4, so these call the dataset handlers with the already-routed
//! parameters — same scenarios, same expected statuses and JSON.

use devcloud_bigquery::model::{DatasetResource, DatasetsListResponse};
use devcloud_bigquery::responses::ApiResponse;
use devcloud_bigquery::server::{Config, Server};
use devcloud_bigquery::validation::Query;

fn new_server(dir: &std::path::Path, location: &str) -> Server {
    Server::new(Config {
        project: "local-project".to_string(),
        location: location.to_string(),
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

// legacy: TestDatasetCatalogCRUDPersistsBigQueryShape
#[test]
fn dataset_catalog_crud_persists_bigquery_shape() {
    let dir = tempdir();
    let server = new_server(&dir, "EU");

    let create_body = r#"{"datasetReference":{"datasetId":"analytics"},"friendlyName":"Analytics","labels":{"env":"test"}}"#;
    let create: ApiResponse = server.create_dataset("local-project", create_body.as_bytes());
    assert_eq!(
        create.status,
        200,
        "create status = {}, body = {}",
        create.status,
        create.body_str()
    );
    let created: DatasetResource =
        serde_json::from_slice(&create.body).expect("decode created dataset");
    assert!(
        created.kind == "bigquery#dataset"
            && created.dataset_reference.project_id == "local-project"
            && created.dataset_reference.dataset_id == "analytics",
        "created dataset = {created:?}"
    );
    assert!(
        created.location == "EU"
            && created.friendly_name == "Analytics"
            && created
                .labels
                .as_ref()
                .and_then(|labels| labels.get("env"))
                .map(String::as_str)
                == Some("test"),
        "created metadata = {created:?}"
    );
    assert!(
        !created.creation_time.is_empty()
            && !created.last_modified_time.is_empty()
            && !created.etag.is_empty(),
        "created timestamps/etag missing = {created:?}"
    );
    assert_eq!(
        created.self_link,
        "/bigquery/v2/projects/local-project/datasets/analytics"
    );
    assert_eq!(create.location.as_deref(), Some(created.self_link.as_str()));

    let get = server.get_dataset("local-project", "analytics");
    assert!(
        get.status == 200 && get.body_str().contains("\"datasetId\":\"analytics\""),
        "get status/body = {} {}",
        get.status,
        get.body_str()
    );

    let patch = server.patch_dataset(
        "local-project",
        "analytics",
        false,
        br#"{"description":"patched"}"#,
    );
    assert!(
        patch.status == 200 && patch.body_str().contains("\"description\":\"patched\""),
        "patch status/body = {} {}",
        patch.status,
        patch.body_str()
    );

    let list = server.list_datasets("local-project", &Query::parse(""));
    assert_eq!(
        list.status,
        200,
        "list status = {}, body = {}",
        list.status,
        list.body_str()
    );
    let listed: DatasetsListResponse = serde_json::from_slice(&list.body).expect("decode list");
    assert!(
        listed.kind == "bigquery#datasetList"
            && listed.total_items == 1
            && listed.datasets.len() == 1,
        "listed datasets = {listed:?}"
    );
    assert_eq!(
        listed.datasets[0].dataset_reference.dataset_id, "analytics",
        "listed item = {:?}",
        listed.datasets[0]
    );

    let delete = server.delete_dataset("local-project", "analytics", &Query::parse(""));
    assert_eq!(
        delete.status,
        204,
        "delete status = {}, body = {}",
        delete.status,
        delete.body_str()
    );
    let missing = server.get_dataset("local-project", "analytics");
    assert_eq!(
        missing.status,
        404,
        "missing status = {}, body = {}",
        missing.status,
        missing.body_str()
    );
}

// legacy: TestDatasetCreateRejectsDuplicateAndUnsafeIDs
#[test]
fn dataset_create_rejects_duplicate_and_unsafe_ids() {
    let dir = tempdir();
    let server = new_server(&dir, "");

    let body = br#"{"datasetReference":{"datasetId":"analytics"}}"#;
    let first = server.create_dataset("local-project", body);
    assert_eq!(
        first.status,
        200,
        "first create status = {}, body = {}",
        first.status,
        first.body_str()
    );

    let duplicate = server.create_dataset("local-project", body);
    assert!(
        duplicate.status == 409 && !duplicate.body_str().contains(std::path::MAIN_SEPARATOR),
        "duplicate status/body = {} {}",
        duplicate.status,
        duplicate.body_str()
    );

    let unsafe_id = server.create_dataset(
        "local-project",
        br#"{"datasetReference":{"datasetId":"../secret"}}"#,
    );
    assert_eq!(
        unsafe_id.status,
        400,
        "unsafe status = {}, body = {}",
        unsafe_id.status,
        unsafe_id.body_str()
    );
}

// legacy: TestDatasetListPaginatesWithNextPageToken
#[test]
fn dataset_list_paginates_with_next_page_token() {
    let dir = tempdir();
    let server = new_server(&dir, "");
    for dataset_id in ["alpha", "beta", "gamma"] {
        create_dataset_for_test(&server, "local-project", dataset_id);
    }

    let first_page = server.list_datasets("local-project", &Query::parse("maxResults=2"));
    assert_eq!(
        first_page.status,
        200,
        "first page status = {}, body = {}",
        first_page.status,
        first_page.body_str()
    );
    let first: DatasetsListResponse =
        serde_json::from_slice(&first_page.body).expect("decode first page");
    assert!(
        first.total_items == 3
            && first.next_page_token == "2"
            && first.datasets.len() == 2
            && first.datasets[0].dataset_reference.dataset_id == "alpha"
            && first.datasets[1].dataset_reference.dataset_id == "beta",
        "first page = {first:?}"
    );

    let second_page = server.list_datasets(
        "local-project",
        &Query::parse(&format!("pageToken={}&maxResults=2", first.next_page_token)),
    );
    assert_eq!(
        second_page.status,
        200,
        "second page status = {}, body = {}",
        second_page.status,
        second_page.body_str()
    );
    let second: DatasetsListResponse =
        serde_json::from_slice(&second_page.body).expect("decode second page");
    assert!(
        second.total_items == 3
            && second.next_page_token.is_empty()
            && second.datasets.len() == 1
            && second.datasets[0].dataset_reference.dataset_id == "gamma",
        "second page = {second:?}"
    );
}

// --- minimal tempdir --------------------------------------------------------

fn tempdir() -> std::path::PathBuf {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let mut dir = std::env::temp_dir();
    dir.push(format!("devcloud-bq-ds-{}-{}", std::process::id(), n));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create tempdir");
    dir
}
