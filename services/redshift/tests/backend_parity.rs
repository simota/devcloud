//! 1:1 port of `internal/services/redshift/backend_test.rs` (memory backend
//! plus the translator seam tests deferred from part 1).

mod common;

use std::sync::{Arc, Mutex};

use common::column_snapshot_has;
use devcloud_redshift::backend::{CatalogSnapshot, ExecResult, SqlTransaction};
use devcloud_redshift::translator::{
    RedshiftTranslator, Session, SideEffect, SideEffectKind, TranslationResult,
};
use devcloud_redshift::{Config, MemoryBackend, RedshiftToPostgres, Server, SqlBackend, SqlError};

#[test]
fn sql_backend_interface_defaults_to_memory_fallback() {
    let server = Server::new(Config {
        database: "dev".to_string(),
        user: "dev".to_string(),
        ..Config::default()
    });

    let result = server.backend().exec("select 1").expect("Exec(select 1)");
    assert_eq!(result.tag, "SELECT 1", "result = {result:?}");
    assert_eq!(
        result.rows,
        vec![vec!["1".to_string()]],
        "result = {result:?}"
    );
}

#[test]
fn memory_backend_catalog_reflects_redshift_metadata() {
    let server = Server::new(Config {
        database: "dev".to_string(),
        user: "dev".to_string(),
        ..Config::default()
    });
    server
        .execute_sql("create table events(id int, payload varchar(64)) diststyle key distkey(id) sortkey(id)")
        .expect("create table");

    let catalog = server.backend().catalog().expect("Catalog()");
    let table = catalog
        .find_table("public", "events")
        .unwrap_or_else(|| panic!("catalog missing public.events: {catalog:?}"));
    assert_eq!(table.columns.len(), 2, "columns = {:?}", table.columns);
    assert_eq!(table.columns[0].name, "id");
    assert_eq!(table.columns[1].name, "payload");
}

#[test]
fn memory_backend_rejects_exec_after_close() {
    let backend = MemoryBackend::new(
        Some(Box::new(|_statement| {
            Ok(ExecResult {
                tag: "SELECT 1".to_string(),
                ..ExecResult::default()
            })
        })),
        None,
    );
    backend.close().expect("Close()");

    assert!(
        backend.exec("select 1").is_err(),
        "Exec after Close() error = nil"
    );
}

#[test]
fn sql_backend_can_be_injected() {
    struct FailingBackend {
        err: SqlError,
    }
    impl SqlBackend for FailingBackend {
        fn exec(&self, _statement: &str) -> Result<ExecResult, SqlError> {
            Err(self.err.clone())
        }
        fn begin(&self) -> Result<Box<dyn SqlTransaction>, SqlError> {
            Err(self.err.clone())
        }
        fn catalog(&self) -> Result<CatalogSnapshot, SqlError> {
            Err(self.err.clone())
        }
        fn close(&self) -> Result<(), SqlError> {
            Ok(())
        }
    }

    let expected = SqlError::new("backend unavailable");
    let server = Server::new(Config {
        sql_backend: Some(Arc::new(FailingBackend {
            err: expected.clone(),
        })),
        ..Config::default()
    });

    let err = server
        .execute_sql("select 1")
        .expect_err("executeSQL with failing backend");
    assert_eq!(err, expected);
}

#[test]
fn redshift_translator_defaults_to_passthrough() {
    let server = Server::new(Config {
        database: "dev".to_string(),
        user: "dev".to_string(),
        ..Config::default()
    });

    let result = server
        .execute_sql("select 1")
        .expect("executeSQL(select 1)");
    assert_eq!(result.tag, "SELECT 1", "result = {result:?}");
    assert_eq!(
        result.rows,
        vec![vec!["1".to_string()]],
        "result = {result:?}"
    );
}

/// Mirrors legacy `rewriteTranslator` test double.
struct RewriteTranslator {
    backend_sql: String,
    side_effects: Vec<SideEffect>,
}

impl RedshiftTranslator for RewriteTranslator {
    fn translate(&self, _session: &Session, _sql: &str) -> Result<TranslationResult, SqlError> {
        Ok(TranslationResult {
            backend_sql: self.backend_sql.clone(),
            side_effects: self.side_effects.clone(),
            ..TranslationResult::default()
        })
    }
}

#[test]
fn redshift_translator_can_rewrite_backend_sql() {
    let server = Server::new(Config {
        database: "dev".to_string(),
        user: "dev".to_string(),
        translator: Some(Arc::new(RewriteTranslator {
            backend_sql: "select 2".to_string(),
            side_effects: Vec::new(),
        })),
        ..Config::default()
    });

    let result = server
        .execute_sql("select 1")
        .expect("executeSQL(select 1)");
    assert_eq!(
        result.rows,
        vec![vec!["2".to_string()]],
        "rows = {:?}",
        result.rows
    );
}

#[test]
fn redshift_translator_rejects_unwired_side_effects() {
    let server = Server::new(Config {
        translator: Some(Arc::new(RewriteTranslator {
            backend_sql: "select 1".to_string(),
            side_effects: vec![SideEffect {
                kind: SideEffectKind::Copy,
                source: "s3://bucket/key".to_string(),
                target: "public.events".to_string(),
            }],
        })),
        ..Config::default()
    });

    assert!(
        server
            .execute_sql("copy public.events from 's3://bucket/key' csv")
            .is_err(),
        "executeSQL with side effects error = nil"
    );
}

#[test]
fn redshift_translator_extracts_table_attributes_for_postgres_backend() {
    /// Mirrors legacy `recordingBackend`.
    struct RecordingBackend {
        statement: Mutex<String>,
    }
    impl SqlBackend for RecordingBackend {
        fn exec(&self, statement: &str) -> Result<ExecResult, SqlError> {
            *self.statement.lock().unwrap() = statement.to_string();
            Ok(ExecResult {
                tag: "CREATE TABLE".to_string(),
                ..ExecResult::default()
            })
        }
        fn begin(&self) -> Result<Box<dyn SqlTransaction>, SqlError> {
            Err(SqlError::new(
                "transaction is not implemented in recording backend",
            ))
        }
        fn catalog(&self) -> Result<CatalogSnapshot, SqlError> {
            Ok(CatalogSnapshot::default())
        }
        fn close(&self) -> Result<(), SqlError> {
            Ok(())
        }
    }

    let recording = Arc::new(RecordingBackend {
        statement: Mutex::new(String::new()),
    });
    let server = Server::new(Config {
        sql_backend: Some(Arc::clone(&recording) as Arc<dyn SqlBackend>),
        translator: Some(Arc::new(RedshiftToPostgres)),
        ..Config::default()
    });

    server
        .execute_sql(
            "create table analytics.events(\n\t\tid integer identity(1,1) distkey sortkey encode raw,\n\t\tpayload varchar(64) default 'unknown'\n\t) diststyle key distkey(id) sortkey(id) backup no",
        )
        .expect("executeSQL(create table)");
    let statement = recording.statement.lock().unwrap().clone();
    let lower = statement.to_lowercase();
    assert!(
        !lower.contains("diststyle")
            && !lower.contains("distkey")
            && !lower.contains("sortkey")
            && !lower.contains("encode")
            && !lower.contains("backup"),
        "backend SQL still contains Redshift-only attributes: {statement}"
    );
    assert!(
        lower.contains("generated by default as identity"),
        "backend SQL did not rewrite identity: {statement}"
    );

    let catalog = server.catalog_snapshot();
    assert!(
        catalog.tables.len() == 1
            && catalog.tables[0].schema == "analytics"
            && catalog.tables[0].dist_style == "key"
            && catalog.tables[0].dist_key == "id",
        "table metadata = {:?}",
        catalog.tables
    );
    assert_eq!(
        catalog.tables[0].sort_keys,
        vec!["id".to_string()],
        "sort keys = {:?}",
        catalog.tables[0].sort_keys
    );
    assert!(
        column_snapshot_has(&catalog.columns, "id", "raw", "", true),
        "id column metadata = {:?}",
        catalog.columns
    );
    assert!(
        column_snapshot_has(&catalog.columns, "payload", "", "'unknown'", false),
        "payload column metadata = {:?}",
        catalog.columns
    );
}
