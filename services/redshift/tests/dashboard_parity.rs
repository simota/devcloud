//! 1:1 port of `internal/services/redshift/dashboard_test.rs`: the dashboard
//! catalog/statement snapshots and the `ExecuteDashboardSQL` runner (redacted
//! history, oversize rejection without storing the SQL).

mod common;

use common::data_api_request;
use devcloud_redshift::{Config, Server};
use serde_json::Value;

/// Mirrors `TestCatalogAndStatementSnapshotsExposeDashboardMetadata`.
#[test]
fn catalog_and_statement_snapshots_expose_dashboard_metadata() {
    let server = Server::new(Config {
        cluster_identifier: "devcloud".to_string(),
        database: "dev".to_string(),
        user: "dev".to_string(),
        ..Config::default()
    });
    for statement in [
        "create schema if not exists loop",
        "create table loop.events(id integer encode raw, payload varchar(64)) diststyle key distkey(id) sortkey(id)",
        "insert into loop.events values (1, 'created')",
    ] {
        server
            .execute_sql(statement)
            .unwrap_or_else(|err| panic!("execute {statement:?}: {err}"));
    }
    let exec = data_api_request(
        &server,
        "ExecuteStatement",
        r#"{
            "ClusterIdentifier":"devcloud",
            "Database":"dev",
            "DbUser":"dev",
            "Sql":"copy loop.events from 's3://bucket/events.csv' iam_role 'secret-role' csv"
        }"#,
    );
    assert_eq!(exec.status, 200, "body = {}", exec.body);

    let catalog = server.catalog_snapshot();
    assert_eq!(catalog.database, "dev");
    assert!(catalog.schemas.len() >= 2, "catalog = {catalog:?}");
    assert_eq!(catalog.tables.len(), 1, "tables = {:?}", catalog.tables);
    assert_eq!(catalog.tables[0].schema, "loop");
    assert_eq!(catalog.tables[0].name, "events");
    assert_eq!(catalog.tables[0].row_count, 1);
    assert_eq!(catalog.tables[0].dist_style, "key");
    assert_eq!(catalog.tables[0].dist_key, "id");
    assert_eq!(catalog.tables[0].sort_keys, vec!["id".to_string()]);
    assert_eq!(catalog.columns.len(), 2, "columns = {:?}", catalog.columns);
    assert_eq!(catalog.columns[0].name, "id");
    assert_eq!(catalog.columns[0].encoding, "raw");

    let statements = server.statement_snapshots();
    assert_eq!(statements.len(), 1, "statements = {statements:?}");
    assert!(statements[0].query_redacted);
    assert_eq!(statements[0].query_preview, "[redacted]");
    assert_eq!(statements[0].result_rows, 0);
    assert_ne!(statements[0].redshift_query_id, 0);
}

/// Mirrors `TestDashboardSQLRejectsOversizeStatementWithoutStoringSQL`.
#[test]
fn dashboard_sql_rejects_oversize_statement_without_storing_sql() {
    let server = Server::new(Config {
        max_statement_bytes: 8,
        ..Config::default()
    });

    let err = server
        .execute_dashboard_sql("select 123456789", 10)
        .expect_err("ExecuteDashboardSQL should fail");
    assert!(
        err.error.to_string().contains("maxStatementBytes"),
        "ExecuteDashboardSQL error = {}",
        err.error
    );
    assert_eq!(err.statement.status, "FAILED");
    assert_eq!(
        err.statement.query_preview,
        "[statement exceeds maxStatementBytes]"
    );

    let statements = server.statement_snapshots();
    assert_eq!(statements.len(), 1, "statement history = {statements:?}");
    assert!(
        !statements[0].query_preview.contains("123456789"),
        "oversize statement leaked into preview: {:?}",
        statements[0]
    );
}

#[test]
fn dashboard_http_introspection_and_control_routes() {
    let server = Server::new(Config {
        cluster_identifier: "devcloud".to_string(),
        database: "dev".to_string(),
        user: "dev".to_string(),
        backend_kind: "memory".to_string(),
        backend_mode: "memory".to_string(),
        ..Config::default()
    });

    let clusters =
        server.dispatch_http("GET", "/_introspect/clusters", "", &Default::default(), &[]);
    assert_eq!(clusters.status, 200, "body = {}", clusters.body);
    let clusters: Value = serde_json::from_str(&clusters.body).unwrap();
    assert_eq!(clusters["status"], "running");
    assert_eq!(clusters["backendKind"], "memory");
    assert_eq!(clusters["backendMode"], "memory");
    assert_eq!(clusters["clusters"][0]["clusterIdentifier"], "devcloud");

    let query = server.dispatch_http(
        "POST",
        "/_control/query",
        "",
        &Default::default(),
        br#"{"sql":"select 1","maxRows":5}"#,
    );
    assert_eq!(query.status, 200, "body = {}", query.body);
    let query: Value = serde_json::from_str(&query.body).unwrap();
    assert_eq!(query["result"]["commandTag"], "SELECT 1");
    assert_eq!(query["result"]["rowCount"], 1);

    let statements = server.dispatch_http(
        "GET",
        "/_introspect/statements",
        "",
        &Default::default(),
        &[],
    );
    assert_eq!(statements.status, 200, "body = {}", statements.body);
    let statements: Value = serde_json::from_str(&statements.body).unwrap();
    assert_eq!(statements.as_array().map(Vec::len), Some(1));
}
