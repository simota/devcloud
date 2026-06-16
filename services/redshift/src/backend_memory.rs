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

pub type ExecFn = dyn Fn(&str) -> Result<ExecResult, SqlError> + Send + Sync;
pub type CatalogFn = dyn Fn() -> Result<CatalogSnapshot, SqlError> + Send + Sync;

pub struct MemoryBackend {
    inner: Arc<Inner>,
}

struct Inner {
    closed: Mutex<bool>,
    exec: Option<Box<ExecFn>>,
    catalog: Option<Box<CatalogFn>>,
}

impl MemoryBackend {
    /// Mirrors `memory.New`; either function may be absent (legacy passes nil).
    pub fn new(exec: Option<Box<ExecFn>>, catalog: Option<Box<CatalogFn>>) -> MemoryBackend {
        MemoryBackend {
            inner: Arc::new(Inner {
                closed: Mutex::new(false),
                exec,
                catalog,
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
        Ok(Box::new(MemoryTransaction {
            inner: Arc::clone(&self.inner),
            closed: false,
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

    fn rollback(&mut self) -> Result<(), SqlError> {
        self.closed = true;
        Ok(())
    }
}
