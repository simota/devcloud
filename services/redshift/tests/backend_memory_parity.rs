//! 1:1 port of `internal/services/redshift/backend/memory/memory_test.rs`.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use devcloud_redshift::backend::{CatalogSnapshot, ExecResult, Schema, SqlBackend};
use devcloud_redshift::{MemoryBackend, SqlError};

#[test]
fn backend_exec_catalog_and_transaction() {
    let exec_calls = Arc::new(AtomicUsize::new(0));
    let calls = Arc::clone(&exec_calls);
    let backend = MemoryBackend::new(
        Some(Box::new(move |statement| {
            calls.fetch_add(1, Ordering::SeqCst);
            assert_eq!(statement, "select 1", "statement = {statement:?}");
            Ok(ExecResult {
                tag: "SELECT 1".to_string(),
                rows: vec![vec!["1".to_string()]],
                ..ExecResult::default()
            })
        })),
        Some(Box::new(|| {
            Ok(CatalogSnapshot {
                schemas: vec![Schema {
                    name: "public".to_string(),
                    ..Schema::default()
                }],
            })
        })),
    );

    let result = backend.exec("select 1").expect("Exec()");
    assert_eq!(result.tag, "SELECT 1", "result = {result:?}");
    assert_eq!(
        result.rows,
        vec![vec!["1".to_string()]],
        "result = {result:?}"
    );

    let mut tx = backend.begin().expect("Begin()");
    tx.exec("select 1").expect("transaction Exec()");
    tx.commit().expect("Commit()");
    assert!(
        tx.exec("select 1").is_err(),
        "transaction Exec() after Commit error = nil"
    );

    let mut tx = backend.begin().expect("second Begin()");
    tx.rollback().expect("Rollback()");
    tx.rollback().expect("second Rollback()");

    let catalog = backend.catalog().expect("Catalog()");
    assert_eq!(catalog.schemas.len(), 1, "catalog = {catalog:?}");
    assert_eq!(catalog.schemas[0].name, "public", "catalog = {catalog:?}");
    assert_eq!(exec_calls.load(Ordering::SeqCst), 2, "execCalls");
}

#[test]
fn backend_errors() {
    let backend = MemoryBackend::new(None, None);
    assert!(
        backend.exec("select 1").is_err(),
        "Exec() without executor error = nil"
    );
    let catalog = backend
        .catalog()
        .expect("Catalog() without catalog function");
    assert_eq!(catalog.schemas.len(), 0, "catalog = {catalog:?}");

    let expected = SqlError::new("boom");
    let failing = expected.clone();
    let backend = MemoryBackend::new(Some(Box::new(move |_| Err(failing.clone()))), None);
    let err = backend.exec("select 1").expect_err("Exec() error");
    assert_eq!(err, expected, "Exec() error = {err}, want {expected}");
    backend.close().expect("Close()");
    assert!(backend.begin().is_err(), "Begin() after Close error = nil");
    assert!(
        backend.catalog().is_err(),
        "Catalog() after Close error = nil"
    );
}
