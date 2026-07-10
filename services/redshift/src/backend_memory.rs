//! Closure-backed in-memory `SqlBackend`.
//!
//! Parity: `internal/services/redshift/backend/memory/memory.rs` — the memory
//! backend delegates execution and catalog snapshots to functions supplied by
//! the server (which owns the actual memory SQL engine), tracks a closed flag,
//! and provides pass-through transactions whose Commit/Rollback only flip a
//! closed bit.

use std::sync::{Arc, Mutex};

use crate::backend::{CatalogSnapshot, ExecResult, SqlBackend, SqlTransaction};
use crate::errors::SqlError;
use crate::model::Database;

pub type ExecFn = dyn Fn(&str) -> Result<ExecResult, SqlError> + Send + Sync;
pub type CatalogFn = dyn Fn() -> Result<CatalogSnapshot, SqlError> + Send + Sync;
/// Captures the state a transaction can restore on rollback.
pub type SnapshotFn = dyn Fn() -> Database + Send + Sync;
/// Restores state previously captured by a [`SnapshotFn`].
pub type RestoreFn = dyn Fn(Database) + Send + Sync;

pub struct MemoryBackend {
    inner: Arc<Inner>,
}

struct Inner {
    closed: Mutex<bool>,
    exec: Option<Box<ExecFn>>,
    catalog: Option<Box<CatalogFn>>,
    snapshot: Option<Box<SnapshotFn>>,
    restore: Option<Box<RestoreFn>>,
}

impl MemoryBackend {
    /// Mirrors `memory.New`; either function may be absent (legacy passes nil).
    /// Transactions opened via `begin()` stay pass-through (no snapshot/restore
    /// hooks); use [`MemoryBackend::with_transaction_hooks`] for real rollback
    /// semantics.
    pub fn new(exec: Option<Box<ExecFn>>, catalog: Option<Box<CatalogFn>>) -> MemoryBackend {
        MemoryBackend::with_transaction_hooks(exec, catalog, None, None)
    }

    /// Like [`MemoryBackend::new`], but also wires up snapshot/restore hooks
    /// so a transaction opened via `begin()` can actually undo its statements
    /// on `rollback()`.
    pub fn with_transaction_hooks(
        exec: Option<Box<ExecFn>>,
        catalog: Option<Box<CatalogFn>>,
        snapshot: Option<Box<SnapshotFn>>,
        restore: Option<Box<RestoreFn>>,
    ) -> MemoryBackend {
        MemoryBackend {
            inner: Arc::new(Inner {
                closed: Mutex::new(false),
                exec,
                catalog,
                snapshot,
                restore,
            }),
        }
    }
}

impl Inner {
    fn ready(&self) -> Result<(), SqlError> {
        if *self.closed.lock().unwrap() {
            return Err(SqlError::new("memory redshift backend is closed"));
        }
        Ok(())
    }

    fn exec(&self, statement: &str) -> Result<ExecResult, SqlError> {
        self.ready()?;
        let exec = self
            .exec
            .as_ref()
            .ok_or_else(|| SqlError::new("memory redshift backend has no executor"))?;
        exec(statement)
    }
}

impl SqlBackend for MemoryBackend {
    fn exec(&self, statement: &str) -> Result<ExecResult, SqlError> {
        self.inner.exec(statement)
    }

    fn begin(&self) -> Result<Box<dyn SqlTransaction>, SqlError> {
        self.inner.ready()?;
        let snapshot = self.inner.snapshot.as_ref().map(|snapshot| snapshot());
        Ok(Box::new(MemoryTransaction {
            inner: Arc::clone(&self.inner),
            closed: false,
            snapshot,
        }))
    }

    fn catalog(&self) -> Result<CatalogSnapshot, SqlError> {
        self.inner.ready()?;
        match &self.inner.catalog {
            None => Ok(CatalogSnapshot::default()),
            Some(catalog) => catalog(),
        }
    }

    fn close(&self) -> Result<(), SqlError> {
        *self.inner.closed.lock().unwrap() = true;
        Ok(())
    }
}

struct MemoryTransaction {
    inner: Arc<Inner>,
    closed: bool,
    /// State captured at `begin()` time, restored by `rollback()`. `None`
    /// when the backend has no transaction hooks configured (pass-through).
    snapshot: Option<Database>,
}

impl SqlTransaction for MemoryTransaction {
    fn exec(&mut self, statement: &str) -> Result<ExecResult, SqlError> {
        if self.closed {
            return Err(SqlError::new("memory redshift transaction is closed"));
        }
        self.inner.exec(statement)
    }

    fn commit(&mut self) -> Result<(), SqlError> {
        if self.closed {
            return Err(SqlError::new("memory redshift transaction is closed"));
        }
        self.closed = true;
        Ok(())
    }

    /// Restores the pre-transaction snapshot when the backend was configured
    /// with transaction hooks; idempotent like Postgres's own ROLLBACK (a
    /// second call on an already-closed transaction is a no-op success).
    fn rollback(&mut self) -> Result<(), SqlError> {
        if self.closed {
            return Ok(());
        }
        if let (Some(restore), Some(snapshot)) = (&self.inner.restore, self.snapshot.take()) {
            restore(snapshot);
        }
        self.closed = true;
        Ok(())
    }
}
