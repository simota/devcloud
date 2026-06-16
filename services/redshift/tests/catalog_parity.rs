//! 1:1 port of `internal/services/redshift/catalog_test.rs`.

mod common;

use common::{column_snapshot_has, result_contains_row};
use devcloud_redshift::{Config, Server};

fn execute_all(server: &Server, statements: &[&str]) {
    for statement in statements {
        server
            .execute_sql(statement)
            .unwrap_or_else(|err| panic!("execute {statement:?}: {err}"));
    }
}

#[test]
fn catalog_views_expose_schemas_tables_columns_and_redshift_metadata() {
    let server = Server::new(Config {
        database: "dev".to_string(),
        user: "dev".to_string(),
        ..Config::default()
    });
    execute_all(
        &server,
        &[
            "create schema if not exists analytics",
            "create table analytics.events(\n\t\t\tid integer encode raw,\n\t\t\tpayload varchar(64) default 'unknown'\n\t\t)\n\t\tdiststyle key\n\t\tdistkey(id)\n\t\tsortkey(id)",
            "insert into analytics.events values (1, 'created')",
        ],
    );

    let tables = server
        .execute_sql("select * from information_schema.tables")
        .expect("information_schema.tables");
    assert!(
        result_contains_row(&tables, &["analytics", "events", "BASE TABLE"]),
        "tables rows = {:?}",
        tables.rows
    );

    let columns = server
        .execute_sql("select * from information_schema.columns")
        .expect("information_schema.columns");
    assert!(
        result_contains_row(&columns, &["events", "id", "1", "", "integer", "raw"]),
        "columns rows = {:?}",
        columns.rows
    );

    let pg_tables = server
        .execute_sql("select * from pg_catalog.pg_tables")
        .expect("pg_catalog.pg_tables");
    assert!(
        result_contains_row(&pg_tables, &["analytics", "events", "dev"]),
        "pg_tables rows = {:?}",
        pg_tables.rows
    );

    let table_info = server
        .execute_sql("select * from svv_table_info")
        .expect("svv_table_info");
    assert!(
        result_contains_row(
            &table_info,
            &["analytics", "events", "key", "id", "id", "1"]
        ),
        "svv_table_info rows = {:?}",
        table_info.rows
    );

    let svv_columns = server
        .execute_sql("select * from svv_columns")
        .expect("svv_columns");
    assert!(
        result_contains_row(
            &svv_columns,
            &[
                "dev",
                "analytics",
                "events",
                "id",
                "1",
                "",
                "integer",
                "raw"
            ]
        ),
        "svv_columns rows = {:?}",
        svv_columns.rows
    );

    let table_def = server
        .execute_sql("select * from pg_table_def")
        .expect("pg_table_def");
    assert!(
        result_contains_row(
            &table_def,
            &[
                "analytics",
                "events",
                "id",
                "integer",
                "raw",
                "true",
                "1",
                "false"
            ]
        ),
        "pg_table_def rows = {:?}",
        table_def.rows
    );
}

#[test]
fn catalog_select_supports_projection_filter_order_limit_and_count() {
    let server = Server::new(Config {
        database: "dev".to_string(),
        user: "dev".to_string(),
        ..Config::default()
    });
    execute_all(
        &server,
        &[
            "create schema if not exists analytics",
            "create table analytics.events(id integer, payload varchar(64))",
            "create table analytics.logs(id integer, message varchar(64))",
            "create table public.events(id integer)",
        ],
    );

    let tables = server
        .execute_sql("select t.table_name from information_schema.tables t where t.table_schema = 'analytics' order by t.table_name limit 1")
        .expect("filtered information_schema.tables");
    assert_eq!(
        tables.fields.len(),
        1,
        "projected table fields = {:?}",
        tables.fields
    );
    assert_eq!(tables.fields[0].name, "table_name");
    assert_eq!(tables.rows, vec![vec!["events".to_string()]]);

    let count = server
        .execute_sql("select count(t.table_name) as table_count from information_schema.tables t where t.table_schema = 'analytics'")
        .expect("catalog count");
    assert_eq!(
        count.fields.len(),
        1,
        "catalog count fields = {:?}",
        count.fields
    );
    assert_eq!(count.fields[0].name, "table_count");
    assert_eq!(count.rows, vec![vec!["2".to_string()]]);
}

#[test]
fn catalog_views_expose_driver_introspection_metadata_without_secrets() {
    let server = Server::new(Config {
        database: "warehouse".to_string(),
        user: "analyst".to_string(),
        password: "local-password".to_string(),
        ..Config::default()
    });

    let databases = server
        .execute_sql("select * from pg_catalog.pg_database")
        .expect("pg_catalog.pg_database");
    assert!(
        result_contains_row(&databases, &["warehouse", "10", "6", "false", "true"]),
        "pg_database rows = {:?}",
        databases.rows
    );

    let users = server
        .execute_sql("select * from pg_catalog.pg_user")
        .expect("pg_catalog.pg_user");
    assert!(
        result_contains_row(&users, &["analyst", "10", "true", "true", "********"]),
        "pg_user rows = {:?}",
        users.rows
    );
    for row in &users.rows {
        for value in row {
            assert!(
                !value.contains("local-password"),
                "pg_user leaked password: {:?}",
                users.rows
            );
        }
    }

    let types = server
        .execute_sql("select * from pg_catalog.pg_type")
        .expect("pg_catalog.pg_type");
    assert!(
        result_contains_row(&types, &["23", "int4", "4", "N"])
            && result_contains_row(&types, &["1043", "varchar", "-1", "S"]),
        "pg_type rows = {:?}",
        types.rows
    );
}

#[test]
fn create_table_accepts_column_level_redshift_attributes() {
    let server = Server::new(Config {
        database: "dev".to_string(),
        user: "dev".to_string(),
        ..Config::default()
    });
    execute_all(
        &server,
        &[
            "create schema if not exists analytics",
            "create table analytics.column_attrs(\n\t\t\tid integer identity(1,1) distkey sortkey encode raw,\n\t\t\tgenerated_id integer generated by default as identity,\n\t\t\tpayload varchar(64) default 'unknown'\n\t\t)",
        ],
    );

    let catalog = server.catalog_snapshot();
    assert_eq!(catalog.tables.len(), 1, "tables = {:?}", catalog.tables);
    assert_eq!(catalog.schemas.len(), 2, "schemas = {:?}", catalog.schemas);
    for schema in &catalog.schemas {
        match schema.name.as_str() {
            "analytics" => assert_eq!(schema.table_count, 1, "analytics tableCount"),
            "public" => assert_eq!(schema.table_count, 0, "public tableCount"),
            _ => {}
        }
    }
    let table = &catalog.tables[0];
    assert_eq!(
        table.column_count, 3,
        "columnCount = {}",
        table.column_count
    );
    assert_eq!(table.dist_style, "key", "table attributes = {table:?}");
    assert_eq!(table.dist_key, "id", "table attributes = {table:?}");
    assert_eq!(
        table.sort_keys,
        vec!["id".to_string()],
        "table attributes = {table:?}"
    );
    assert!(
        column_snapshot_has(&catalog.columns, "id", "raw", "", true),
        "id column metadata = {:?}",
        catalog.columns
    );
    assert!(
        column_snapshot_has(&catalog.columns, "generated_id", "", "", true),
        "generated identity metadata = {:?}",
        catalog.columns
    );
    assert!(
        column_snapshot_has(&catalog.columns, "payload", "", "'unknown'", false),
        "default metadata = {:?}",
        catalog.columns
    );
}
