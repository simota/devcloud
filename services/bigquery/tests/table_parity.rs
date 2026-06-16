//! 1:1 port of `internal/services/bigquery/table_test.rs`.
//!
//! The legacy tests drive `server.routes().ServeHTTP`; the routing layer arrives
//! in part 4, so these call the table/routine handlers with the already-routed
//! parameters — same scenarios, same expected statuses and JSON. The legacy
//! `TableSnapshot`/`DatasetSnapshot` assertions (dashboard.rs, part 4) are
//! checked through the storage layer the snapshots read from.

use devcloud_bigquery::model::{
    RoutineResource, RoutinesListResponse, TableResource, TablesListResponse,
};
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

/// legacy `createTableForTest` (server_test.rs).
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

// legacy: TestTableCatalogCRUDPersistsBigQueryShape
#[test]
fn table_catalog_crud_persists_bigquery_shape() {
    let dir = tempdir();
    let server = new_server(&dir, "US");
    create_dataset_for_test(&server, "local-project", "analytics");

    let create_body = r#"{"tableReference":{"tableId":"people"},"schema":{"fields":[{"name":"id","type":"STRING","mode":"REQUIRED"},{"name":"age","type":"INTEGER"}]},"friendlyName":"People"}"#;
    let create = server.create_table("local-project", "analytics", create_body.as_bytes());
    assert_eq!(
        create.status,
        200,
        "create status = {}, body = {}",
        create.status,
        create.body_str()
    );
    let created: TableResource =
        serde_json::from_slice(&create.body).expect("decode created table");
    assert!(
        created.kind == "bigquery#table"
            && created.table_reference.project_id == "local-project"
            && created.table_reference.dataset_id == "analytics"
            && created.table_reference.table_id == "people",
        "created table = {created:?}"
    );
    assert!(
        created.table_type == "TABLE"
            && created.num_rows == "0"
            && created.num_bytes == "0"
            && created.schema.fields.len() == 2,
        "created table metadata = {created:?}"
    );
    assert_eq!(
        created.self_link,
        "/bigquery/v2/projects/local-project/datasets/analytics/tables/people"
    );

    let get = server.get_table("local-project", "analytics", "people");
    assert!(
        get.status == 200 && get.body_str().contains("\"tableId\":\"people\""),
        "get status/body = {} {}",
        get.status,
        get.body_str()
    );

    let patch = server.patch_table(
        "local-project",
        "analytics",
        "people",
        false,
        br#"{"description":"patched table"}"#,
    );
    assert!(
        patch.status == 200
            && patch
                .body_str()
                .contains("\"description\":\"patched table\""),
        "patch status/body = {} {}",
        patch.status,
        patch.body_str()
    );

    let list = server.list_tables("local-project", "analytics", &Query::parse(""));
    assert_eq!(
        list.status,
        200,
        "list status = {}, body = {}",
        list.status,
        list.body_str()
    );
    let listed: TablesListResponse = serde_json::from_slice(&list.body).expect("decode list");
    assert!(
        listed.kind == "bigquery#tableList" && listed.total_items == 1 && listed.tables.len() == 1,
        "listed tables = {listed:?}"
    );
    assert_eq!(
        listed.tables[0].table_reference.table_id, "people",
        "listed item = {:?}",
        listed.tables[0]
    );

    let delete = server.delete_table("local-project", "analytics", "people");
    assert_eq!(
        delete.status,
        204,
        "delete status = {}, body = {}",
        delete.status,
        delete.body_str()
    );
    let missing = server.get_table("local-project", "analytics", "people");
    assert_eq!(
        missing.status,
        404,
        "missing status = {}, body = {}",
        missing.status,
        missing.body_str()
    );
}

// legacy: TestTableListPaginatesWithNextPageToken
#[test]
fn table_list_paginates_with_next_page_token() {
    let dir = tempdir();
    let server = new_server(&dir, "");
    create_dataset_for_test(&server, "local-project", "analytics");
    for table_id in ["events", "people", "sessions"] {
        create_table_for_test(&server, "local-project", "analytics", table_id);
    }

    let first_page =
        server.list_tables("local-project", "analytics", &Query::parse("maxResults=2"));
    assert_eq!(
        first_page.status,
        200,
        "first page status = {}, body = {}",
        first_page.status,
        first_page.body_str()
    );
    let first: TablesListResponse =
        serde_json::from_slice(&first_page.body).expect("decode first page");
    assert!(
        first.total_items == 3
            && first.next_page_token == "2"
            && first.tables.len() == 2
            && first.tables[0].table_reference.table_id == "events"
            && first.tables[1].table_reference.table_id == "people",
        "first page = {first:?}"
    );

    let second_page = server.list_tables(
        "local-project",
        "analytics",
        &Query::parse(&format!("pageToken={}&maxResults=2", first.next_page_token)),
    );
    assert_eq!(
        second_page.status,
        200,
        "second page status = {}, body = {}",
        second_page.status,
        second_page.body_str()
    );
    let second: TablesListResponse =
        serde_json::from_slice(&second_page.body).expect("decode second page");
    assert!(
        second.total_items == 3
            && second.next_page_token.is_empty()
            && second.tables.len() == 1
            && second.tables[0].table_reference.table_id == "sessions",
        "second page = {second:?}"
    );
}

// legacy: TestTablePartitioningAndClusteringMetadataPersists
#[test]
fn table_partitioning_and_clustering_metadata_persists() {
    let dir = tempdir();
    let server = new_server(&dir, "");
    create_dataset_for_test(&server, "local-project", "analytics");

    let create_body = r#"{
		"tableReference":{"tableId":"events"},
		"schema":{"fields":[
			{"name":"event_date","type":"DATE"},
			{"name":"tenant_id","type":"STRING"},
			{"name":"event_id","type":"STRING"}
		]},
		"timePartitioning":{"type":"DAY","field":"event_date","expirationMs":"86400000","requirePartitionFilter":true},
		"clustering":{"fields":["tenant_id","event_id"]}
	}"#;
    let create = server.create_table("local-project", "analytics", create_body.as_bytes());
    assert_eq!(
        create.status,
        200,
        "create status = {}, body = {}",
        create.status,
        create.body_str()
    );
    let created: TableResource =
        serde_json::from_slice(&create.body).expect("decode created table");
    let time_partitioning = created
        .time_partitioning
        .as_ref()
        .unwrap_or_else(|| panic!("timePartitioning = {:?}", created.time_partitioning));
    assert!(
        time_partitioning.partition_type == "DAY" && time_partitioning.require_filter,
        "timePartitioning = {time_partitioning:?}"
    );
    let clustering = created
        .clustering
        .as_ref()
        .unwrap_or_else(|| panic!("clustering = {:?}", created.clustering));
    assert_eq!(
        clustering.fields.join(","),
        "tenant_id,event_id",
        "clustering = {clustering:?}"
    );

    let patch = server.patch_table(
        "local-project",
        "analytics",
        "events",
        false,
        br#"{
		"rangePartitioning":{"field":"event_id","range":{"start":"1","end":"100","interval":"10"}},
		"clustering":{"fields":["tenant_id"]}
	}"#,
    );
    assert_eq!(
        patch.status,
        200,
        "patch status = {}, body = {}",
        patch.status,
        patch.body_str()
    );
    let patched: TableResource = serde_json::from_slice(&patch.body).expect("decode patched table");
    assert!(
        patched
            .time_partitioning
            .as_ref()
            .map(|tp| tp.field == "event_date")
            .unwrap_or(false),
        "patch dropped timePartitioning = {:?}",
        patched.time_partitioning
    );
    assert!(
        patched
            .range_partitioning
            .as_ref()
            .map(|rp| rp.range.interval == "10")
            .unwrap_or(false),
        "rangePartitioning = {:?}",
        patched.range_partitioning
    );
    assert!(
        patched
            .clustering
            .as_ref()
            .map(|c| c.fields.join(",") == "tenant_id")
            .unwrap_or(false),
        "patched clustering = {:?}",
        patched.clustering
    );

    let list = server.list_tables("local-project", "analytics", &Query::parse(""));
    assert_eq!(
        list.status,
        200,
        "list status = {}, body = {}",
        list.status,
        list.body_str()
    );
    let listed: TablesListResponse = serde_json::from_slice(&list.body).expect("decode list");
    assert!(
        listed.tables.len() == 1
            && listed.tables[0].time_partitioning.is_some()
            && listed.tables[0].clustering.is_some(),
        "listed table metadata = {:?}",
        listed.tables
    );

    // legacy asserts via server.TableSnapshot (dashboard seam, part 4); the
    // snapshot reads the same persisted table resource.
    let snapshot = server
        .read_table("local-project", "analytics", "events")
        .expect("read table")
        .expect("table snapshot missing");
    assert!(
        snapshot.time_partitioning.is_some()
            && snapshot.range_partitioning.is_some()
            && snapshot.clustering.is_some(),
        "snapshot metadata = {snapshot:?}"
    );
}

// legacy: TestTableViewAndRoutineMetadataCatalogPersists
#[test]
fn table_view_and_routine_metadata_catalog_persists() {
    let dir = tempdir();
    let server = new_server(&dir, "");
    create_dataset_for_test(&server, "local-project", "analytics");

    let view_body = "{
		\"tableReference\":{\"tableId\":\"active_people\"},
		\"type\":\"VIEW\",
		\"view\":{\"query\":\"SELECT id FROM `local-project.analytics.people` WHERE active = TRUE\",\"useLegacySql\":false}
	}";
    let create_view = server.create_table("local-project", "analytics", view_body.as_bytes());
    assert_eq!(
        create_view.status,
        200,
        "create view status = {}, body = {}",
        create_view.status,
        create_view.body_str()
    );
    let create_view_response = create_view.body_str().to_string();
    let view: TableResource = serde_json::from_str(&create_view_response).expect("decode view");
    assert!(
        view.table_type == "VIEW"
            && view
                .view
                .as_ref()
                .map(|v| v.query.contains("active = TRUE"))
                .unwrap_or(false),
        "view metadata = {view:?}"
    );
    assert!(
        create_view_response.contains("\"useLegacySql\":false"),
        "view response omitted useLegacySql=false: {create_view_response}"
    );

    let create_routine_body = r#"{
		"routineReference":{"routineId":"normalize_name"},
		"routineType":"SCALAR_FUNCTION",
		"language":"SQL",
		"arguments":[{"name":"name","dataType":{"typeKind":"STRING"}}],
		"returnType":{"typeKind":"STRING"},
		"definitionBody":"LOWER(name)",
		"description":"Normalize display names",
		"determinismLevel":"DETERMINISTIC"
	}"#;
    let create_routine =
        server.create_routine("local-project", "analytics", create_routine_body.as_bytes());
    assert_eq!(
        create_routine.status,
        200,
        "create routine status = {}, body = {}",
        create_routine.status,
        create_routine.body_str()
    );
    let routine: RoutineResource =
        serde_json::from_slice(&create_routine.body).expect("decode routine");
    assert!(
        routine.kind == "bigquery#routine"
            && routine.routine_reference.routine_id == "normalize_name"
            && routine
                .return_type
                .as_ref()
                .map(|t| t.type_kind == "STRING")
                .unwrap_or(false),
        "routine metadata = {routine:?}"
    );
    assert_eq!(
        routine.self_link,
        "/bigquery/v2/projects/local-project/datasets/analytics/routines/normalize_name"
    );

    let patch_routine = server.patch_routine(
        "local-project",
        "analytics",
        "normalize_name",
        false,
        br#"{"description":"patched routine"}"#,
    );
    assert_eq!(
        patch_routine.status,
        200,
        "patch routine status = {}, body = {}",
        patch_routine.status,
        patch_routine.body_str()
    );
    let patched: RoutineResource =
        serde_json::from_slice(&patch_routine.body).expect("decode patched routine");
    assert!(
        patched.description == "patched routine" && patched.definition_body == "LOWER(name)",
        "patched routine = {patched:?}"
    );

    let list_routines =
        server.list_routines("local-project", "analytics", &Query::parse("maxResults=1"));
    assert_eq!(
        list_routines.status,
        200,
        "list routines status = {}, body = {}",
        list_routines.status,
        list_routines.body_str()
    );
    let listed: RoutinesListResponse =
        serde_json::from_slice(&list_routines.body).expect("decode routines");
    assert!(
        listed.kind == "bigquery#routineList"
            && listed.total_items == 1
            && listed.routines.len() == 1,
        "listed routines = {listed:?}"
    );

    // legacy asserts via server.DatasetSnapshot (dashboard seam, part 4); the
    // snapshot reads the same persisted tables/routines.
    let snapshot_routines = server
        .read_routines("local-project", "analytics")
        .expect("read routines");
    assert!(
        snapshot_routines.len() == 1
            && snapshot_routines[0].routine_reference.routine_id == "normalize_name",
        "snapshot routines = {snapshot_routines:?}"
    );
    let snapshot_tables = server
        .read_tables("local-project", "analytics")
        .expect("read tables");
    assert!(
        snapshot_tables.len() == 1 && snapshot_tables[0].view.is_some(),
        "snapshot tables = {snapshot_tables:?}"
    );

    let delete_routine = server.delete_routine("local-project", "analytics", "normalize_name");
    assert_eq!(
        delete_routine.status,
        204,
        "delete routine status = {}, body = {}",
        delete_routine.status,
        delete_routine.body_str()
    );
}

// legacy: TestTableCreateRejectsMissingDatasetDuplicateAndInvalidSchema
#[test]
fn table_create_rejects_missing_dataset_duplicate_and_invalid_schema() {
    let dir = tempdir();
    let server = new_server(&dir, "");
    let body =
        br#"{"tableReference":{"tableId":"people"},"schema":{"fields":[{"name":"id","type":"STRING"}]}}"#;

    let missing_dataset = server.create_table("local-project", "missing", body);
    assert_eq!(
        missing_dataset.status,
        404,
        "missing dataset status = {}, body = {}",
        missing_dataset.status,
        missing_dataset.body_str()
    );

    create_dataset_for_test(&server, "local-project", "analytics");
    let first = server.create_table("local-project", "analytics", body);
    assert_eq!(
        first.status,
        200,
        "first create status = {}, body = {}",
        first.status,
        first.body_str()
    );
    let duplicate = server.create_table("local-project", "analytics", body);
    assert_eq!(
        duplicate.status,
        409,
        "duplicate status = {}, body = {}",
        duplicate.status,
        duplicate.body_str()
    );

    let invalid_schema = server.create_table(
        "local-project",
        "analytics",
        br#"{"tableReference":{"tableId":"bad"},"schema":{"fields":[{"name":"payload","type":"UNSUPPORTED"}]}}"#,
    );
    assert_eq!(
        invalid_schema.status,
        400,
        "invalid schema status = {}, body = {}",
        invalid_schema.status,
        invalid_schema.body_str()
    );
}

// --- minimal tempdir --------------------------------------------------------

fn tempdir() -> std::path::PathBuf {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let mut dir = std::env::temp_dir();
    dir.push(format!("devcloud-bq-tbl-{}-{}", std::process::id(), n));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create tempdir");
    dir
}
