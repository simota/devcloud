//! 1:1 port of `internal/services/redshift/sql_test.rs`.

mod common;

use common::result_contains_row;
use devcloud_redshift::pg_types::{PG_TYPE_INT4_OID, PG_TYPE_VARCHAR_OID};
use devcloud_redshift::{Config, Server};

fn execute_all(server: &Server, statements: &[&str]) {
    for statement in statements {
        server
            .execute_sql(statement)
            .unwrap_or_else(|err| panic!("execute {statement:?}: {err}"));
    }
}

#[test]
fn sql_core_create_insert_select_workflow() {
    let server = Server::new(Config::default());

    execute_all(
        &server,
        &[
            "create schema if not exists loop",
            "drop table if exists loop.events",
            "create table loop.events(\n\t\t\tid integer encode raw,\n\t\t\tpayload varchar(64)\n\t\t)\n\t\tdiststyle key\n\t\tdistkey(id)\n\t\tsortkey(id)",
            "insert into loop.events values (1, 'created')",
        ],
    );

    let result = server
        .execute_sql("select id, payload from loop.events where id = 1 limit 1")
        .expect("select");
    assert_eq!(result.tag, "SELECT 1");
    assert_eq!(result.fields.len(), 2, "fields = {:?}", result.fields);
    assert_eq!(result.fields[0].name, "id");
    assert_eq!(result.fields[1].name, "payload");
    assert_eq!(
        result.rows,
        vec![vec!["1".to_string(), "created".to_string()]]
    );
}

#[test]
fn sql_core_select_literal_projection() {
    let server = Server::new(Config::default());

    let result = server
        .execute_sql("select 1 as id, 'created' payload")
        .expect("select literals");
    assert_eq!(result.tag, "SELECT 1");
    assert_eq!(result.fields.len(), 2, "fields = {:?}", result.fields);
    assert_eq!(result.fields[0].name, "id");
    assert_eq!(result.fields[0].type_oid, PG_TYPE_INT4_OID);
    assert_eq!(result.fields[1].name, "payload");
    assert_eq!(result.fields[1].type_oid, PG_TYPE_VARCHAR_OID);
    assert_eq!(
        result.rows,
        vec![vec!["1".to_string(), "created".to_string()]]
    );
}

#[test]
fn sql_client_introspection_functions_and_show() {
    let server = Server::new(Config {
        user: "analyst".to_string(),
        password: "local-password".to_string(),
        ..Config::default()
    });

    for (statement, field, value) in [
        ("select current_user", "current_user", "analyst"),
        ("select session_user()", "session_user", "analyst"),
        ("select pg_backend_pid()", "pg_backend_pid", "1"),
        ("show search_path", "search_path", "public"),
        (
            "show transaction isolation level",
            "transaction isolation level",
            "read committed",
        ),
        (
            "show standard_conforming_strings",
            "standard_conforming_strings",
            "on",
        ),
    ] {
        let result = server
            .execute_sql(statement)
            .unwrap_or_else(|err| panic!("execute {statement:?}: {err}"));
        assert_eq!(
            result.fields.len(),
            1,
            "{statement:?} fields = {:?}",
            result.fields
        );
        assert_eq!(
            result.fields[0].name, field,
            "{statement:?} fields = {:?}",
            result.fields
        );
        assert_eq!(
            result.rows,
            vec![vec![value.to_string()]],
            "{statement:?} rows = {:?}",
            result.rows
        );
        assert!(
            !result.rows[0][0].contains("local-password"),
            "{statement:?} leaked password in result: {:?}",
            result.rows
        );
    }
}

#[test]
fn sql_core_insert_column_list_defaults_and_identity() {
    let server = Server::new(Config::default());
    execute_all(
        &server,
        &[
            "create table public.events(id integer identity, payload varchar(64) default 'new', status varchar(16))",
            "insert into public.events(payload, status) values ('created', 'open')",
            "insert into public.events(status, payload, id) values ('closed', default, default)",
        ],
    );

    let result = server
        .execute_sql("select id, payload, status from public.events order by id")
        .expect("select inserted rows");
    assert_eq!(result.rows.len(), 2, "rows = {:?}", result.rows);
    assert_eq!(result.rows[0], vec!["1", "created", "open"]);
    assert_eq!(result.rows[1], vec!["2", "new", "closed"]);
}

#[test]
fn sql_core_insert_multiple_values_rows() {
    let server = Server::new(Config::default());
    execute_all(
        &server,
        &[
            "create table public.events(id integer identity, payload varchar(64) default 'new', status varchar(16))",
            "insert into public.events(payload, status) values ('created', 'open'), ('queued', 'open'), (default, 'closed')",
        ],
    );

    let result = server
        .execute_sql("select id, payload, status from public.events order by id")
        .expect("select inserted rows");
    let want = vec![
        vec!["1".to_string(), "created".to_string(), "open".to_string()],
        vec!["2".to_string(), "queued".to_string(), "open".to_string()],
        vec!["3".to_string(), "new".to_string(), "closed".to_string()],
    ];
    assert_eq!(result.rows, want);
}

#[test]
fn sql_core_update_and_delete_workflow() {
    let server = Server::new(Config::default());
    execute_all(
        &server,
        &[
            "create table public.events(id integer, payload varchar(64), status varchar(16))",
            "insert into public.events values (1, 'created', 'open')",
            "insert into public.events values (2, 'queued', 'open')",
        ],
    );

    let update_result = server
        .execute_sql(
            "update public.events set payload = 'processed', status = 'closed' where id = 2",
        )
        .expect("update");
    assert_eq!(update_result.tag, "UPDATE 1");
    let updated = server
        .execute_sql("select id, payload, status from public.events where id = 2")
        .expect("select updated row");
    assert_eq!(updated.rows.len(), 1, "updated rows = {:?}", updated.rows);
    assert_eq!(updated.rows[0][1], "processed");
    assert_eq!(updated.rows[0][2], "closed");

    let delete_result = server
        .execute_sql("delete from public.events where status = 'open'")
        .expect("delete");
    assert_eq!(delete_result.tag, "DELETE 1");
    let remaining = server
        .execute_sql("select id, payload from public.events order by id")
        .expect("select remaining rows");
    assert_eq!(
        remaining.rows,
        vec![vec!["2".to_string(), "processed".to_string()]],
        "remaining rows = {:?}",
        remaining.rows
    );
}

#[test]
fn sql_core_where_comparison_operators() {
    let server = Server::new(Config::default());
    execute_all(
        &server,
        &[
            "create table public.events(id integer, payload varchar(64), status varchar(16))",
            "insert into public.events values (1, 'alpha', 'open')",
            "insert into public.events values (2, 'bravo', 'open')",
            "insert into public.events values (10, 'charlie', 'closed')",
        ],
    );

    let selected = server
        .execute_sql("select id, payload from public.events where id >= 2 order by id")
        .expect("select comparison");
    assert_eq!(
        selected.rows,
        vec![
            vec!["2".to_string(), "bravo".to_string()],
            vec!["10".to_string(), "charlie".to_string()],
        ]
    );

    let updated = server
        .execute_sql("update public.events set status = 'archived' where payload <> 'alpha'")
        .expect("update comparison");
    assert_eq!(updated.tag, "UPDATE 2");

    let deleted = server
        .execute_sql("delete from public.events where id < 10")
        .expect("delete comparison");
    assert_eq!(deleted.tag, "DELETE 2");

    let remaining = server
        .execute_sql("select id, status from public.events")
        .expect("select remaining");
    assert_eq!(
        remaining.rows,
        vec![vec!["10".to_string(), "archived".to_string()]]
    );
}

#[test]
fn sql_core_select_count_from_table() {
    let server = Server::new(Config::default());
    execute_all(
        &server,
        &[
            "create table public.events(id integer, payload varchar(64), status varchar(16))",
            "insert into public.events values (1, 'alpha', 'open')",
            "insert into public.events values (2, 'bravo', 'open')",
            "insert into public.events values (3, 'charlie', 'closed')",
        ],
    );

    let result = server
        .execute_sql("select count(*) as total from public.events where status = 'open'")
        .expect("select count");
    assert_eq!(result.tag, "SELECT 1");
    assert_eq!(result.fields.len(), 1, "fields = {:?}", result.fields);
    assert_eq!(result.fields[0].name, "total");
    assert_eq!(result.fields[0].type_oid, PG_TYPE_INT4_OID);
    assert_eq!(result.rows, vec![vec!["2".to_string()]]);

    let column_count = server
        .execute_sql("select count(id) row_count from public.events")
        .expect("select count column");
    assert_eq!(
        column_count.rows.len(),
        1,
        "column count = {column_count:?}"
    );
    assert_eq!(column_count.rows[0][0], "3");
    assert_eq!(column_count.fields[0].name, "row_count");
}

#[test]
fn sql_core_create_select_drop_view() {
    let server = Server::new(Config {
        database: "dev".to_string(),
        user: "dev".to_string(),
        ..Config::default()
    });
    execute_all(
        &server,
        &[
            "create schema if not exists analytics",
            "create table analytics.events(id integer, payload varchar(64), status varchar(16))",
            "insert into analytics.events values (1, 'alpha', 'open')",
            "insert into analytics.events values (2, 'bravo', 'closed')",
            "create view analytics.open_events as select id, payload from analytics.events where status = 'open'",
        ],
    );

    let selected = server
        .execute_sql("select payload from analytics.open_events where id = 1")
        .expect("select view");
    assert_eq!(
        selected.fields.len(),
        1,
        "view fields = {:?}",
        selected.fields
    );
    assert_eq!(selected.fields[0].name, "payload");
    assert_eq!(selected.rows, vec![vec!["alpha".to_string()]]);

    let tables = server
        .execute_sql("select table_schema, table_name, table_type from information_schema.tables where table_name = 'open_events'")
        .expect("information_schema view row");
    assert_eq!(
        tables.rows,
        vec![vec![
            "analytics".to_string(),
            "open_events".to_string(),
            "VIEW".to_string(),
        ]]
    );

    let pg_class = server
        .execute_sql(
            "select relname, relkind from pg_catalog.pg_class where relname = 'open_events'",
        )
        .expect("pg_class view row");
    assert_eq!(
        pg_class.rows,
        vec![vec!["open_events".to_string(), "v".to_string()]]
    );

    server
        .execute_sql("drop view if exists analytics.open_events")
        .expect("drop view");
    assert!(
        server
            .execute_sql("select * from analytics.open_events")
            .is_err(),
        "select from dropped view succeeded"
    );
}

#[test]
fn sql_core_create_table_as_select_workflow() {
    let server = Server::new(Config {
        database: "dev".to_string(),
        user: "dev".to_string(),
        ..Config::default()
    });
    execute_all(
        &server,
        &[
            "create schema if not exists analytics",
            "create table analytics.events(id integer, payload varchar(64), status varchar(16))",
            "insert into analytics.events values (1, 'alpha', 'open')",
            "insert into analytics.events values (2, 'bravo', 'closed')",
            "create table analytics.open_events diststyle key distkey(id) sortkey(id) as select id, payload from analytics.events where status = 'open'",
        ],
    );

    let selected = server
        .execute_sql("select id, payload from analytics.open_events")
        .expect("select CTAS table");
    assert_eq!(
        selected.rows,
        vec![vec!["1".to_string(), "alpha".to_string()]]
    );

    let table_info = server
        .execute_sql("select * from svv_table_info")
        .expect("svv_table_info");
    assert!(
        result_contains_row(
            &table_info,
            &["analytics", "open_events", "key", "id", "id", "1"]
        ),
        "svv_table_info rows = {:?}",
        table_info.rows
    );
}

#[test]
fn sql_core_create_select_drop_materialized_view() {
    let server = Server::new(Config {
        database: "dev".to_string(),
        user: "dev".to_string(),
        ..Config::default()
    });
    execute_all(
        &server,
        &[
            "create schema if not exists analytics",
            "create table analytics.events(id integer, payload varchar(64), status varchar(16))",
            "insert into analytics.events values (1, 'alpha', 'open')",
            "insert into analytics.events values (2, 'bravo', 'closed')",
            "create materialized view analytics.open_event_mv diststyle key distkey(id) sortkey(id) as select id, payload from analytics.events where status = 'open'",
            "insert into analytics.events values (3, 'charlie', 'open')",
        ],
    );

    let selected = server
        .execute_sql("select id, payload from analytics.open_event_mv order by id")
        .expect("select materialized view");
    assert_eq!(
        selected.rows,
        vec![vec!["1".to_string(), "alpha".to_string()]]
    );

    let tables = server
        .execute_sql("select table_schema, table_name, table_type from information_schema.tables where table_name = 'open_event_mv'")
        .expect("information_schema materialized view row");
    assert_eq!(
        tables.rows,
        vec![vec![
            "analytics".to_string(),
            "open_event_mv".to_string(),
            "MATERIALIZED VIEW".to_string(),
        ]]
    );

    let pg_class = server
        .execute_sql(
            "select relname, relkind from pg_catalog.pg_class where relname = 'open_event_mv'",
        )
        .expect("pg_class materialized view row");
    assert_eq!(
        pg_class.rows,
        vec![vec!["open_event_mv".to_string(), "m".to_string()]]
    );

    let mv_info = server
        .execute_sql(
            "select schema, name, state, is_stale from svv_mv_info where name = 'open_event_mv'",
        )
        .expect("svv_mv_info");
    assert_eq!(
        mv_info.rows,
        vec![vec![
            "analytics".to_string(),
            "open_event_mv".to_string(),
            "1".to_string(),
            "false".to_string(),
        ]]
    );

    let catalog = server.catalog_snapshot();
    assert_eq!(
        catalog.tables.len(),
        2,
        "catalog tables = {:?}",
        catalog.tables
    );
    let materialized_view = catalog
        .tables
        .iter()
        .find(|table| table.name == "open_event_mv")
        .expect("open_event_mv snapshot");
    assert_eq!(materialized_view.table_type, "MATERIALIZED_VIEW");
    assert_eq!(materialized_view.row_count, 1);
    assert_eq!(materialized_view.dist_key, "id");

    server
        .execute_sql("drop materialized view if exists analytics.open_event_mv")
        .expect("drop materialized view");
    assert!(
        server
            .execute_sql("select * from analytics.open_event_mv")
            .is_err(),
        "select from dropped materialized view succeeded"
    );
}

#[test]
fn sql_core_drop_schema_removes_tables_and_preserves_public() {
    let server = Server::new(Config::default());
    execute_all(
        &server,
        &[
            "create schema if not exists scratch",
            "create table scratch.events(id integer, payload varchar(64))",
            "drop schema if exists scratch cascade",
        ],
    );

    let schemas = server
        .execute_sql("select * from information_schema.schemata")
        .expect("information_schema.schemata");
    assert!(
        !result_contains_row(&schemas, &["scratch"]),
        "scratch schema should be removed: {:?}",
        schemas.rows
    );
    assert!(
        result_contains_row(&schemas, &["public"]),
        "public schema should be preserved: {:?}",
        schemas.rows
    );

    assert!(
        server.execute_sql("select * from scratch.events").is_err(),
        "select from dropped schema table succeeded"
    );
}

#[test]
fn simple_query_records_redacted_query_history() {
    let server = Server::new(Config {
        cluster_identifier: "devcloud".to_string(),
        database: "dev".to_string(),
        user: "dev".to_string(),
        ..Config::default()
    });
    let mut wire = Vec::new();

    server.handle_simple_query(
        &mut wire,
        "copy public.missing from 's3://bucket/events.csv' iam_role 'secret-role' csv;",
    );

    let statements = server.statement_snapshots();
    assert_eq!(statements.len(), 1, "statements = {statements:?}");
    assert!(
        statements[0].status == "FAILED"
            && statements[0].query_redacted
            && statements[0].query_preview == "[redacted]",
        "statement history = {:?}",
        statements[0]
    );

    let stl_query = server
        .execute_sql("select * from stl_query")
        .expect("stl_query");
    assert!(
        result_contains_row(&stl_query, &["[redacted]", "FAILED"]),
        "stl_query should expose redacted preview only: {:?}",
        stl_query.rows
    );
    for row in &stl_query.rows {
        for value in row {
            assert!(
                !value.contains("secret-role") && !value.contains("s3://bucket/events.csv"),
                "stl_query leaked sensitive SQL text: {:?}",
                stl_query.rows
            );
        }
    }
}
