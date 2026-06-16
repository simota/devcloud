//! The SQL execution boundary between Redshift compatibility code and the
//! engine that owns SQL execution.
//!
//! Parity: `internal/services/redshift/backend/backend.rs`. Later increments
//! plug pgwire (part 3), the Data API (part 4), and the managed Postgres
//! backend (part 5) into this trait.

use crate::errors::SqlError;

pub trait SqlBackend: Send + Sync {
    fn exec(&self, statement: &str) -> Result<ExecResult, SqlError>;
    fn begin(&self) -> Result<Box<dyn SqlTransaction>, SqlError>;
    fn catalog(&self) -> Result<CatalogSnapshot, SqlError>;
    fn close(&self) -> Result<(), SqlError>;
}

pub trait SqlTransaction: Send {
    fn exec(&mut self, statement: &str) -> Result<ExecResult, SqlError>;
    fn commit(&mut self) -> Result<(), SqlError>;
    fn rollback(&mut self) -> Result<(), SqlError>;
}

/// Mirrors `backend.Result`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ExecResult {
    pub fields: Vec<Field>,
    pub rows: Vec<Vec<String>>,
    pub tag: String,
}

/// Mirrors `backend.Field`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Field {
    pub name: String,
    pub type_oid: i32,
    pub type_size: i16,
}

/// Mirrors `backend.CatalogSnapshot`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CatalogSnapshot {
    pub schemas: Vec<Schema>,
}

/// Mirrors `backend.Schema`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Schema {
    pub name: String,
    pub tables: Vec<Table>,
}

/// Mirrors `backend.Table`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Table {
    pub schema: String,
    pub name: String,
    pub kind: String,
    pub columns: Vec<Column>,
}

/// Mirrors `backend.Column`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Column {
    pub name: String,
    pub data_type: String,
}

impl CatalogSnapshot {
    /// Convenience lookup used by parity tests (legacy `findBackendTable`).
    pub fn find_table(&self, schema_name: &str, table_name: &str) -> Option<&Table> {
        self.schemas
            .iter()
            .filter(|schema| schema.name == schema_name)
            .flat_map(|schema| schema.tables.iter())
            .find(|table| table.name == table_name)
    }
}
