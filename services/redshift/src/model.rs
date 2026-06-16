//! In-memory database model: schemas, tables, columns.
//!
//! Parity: `internal/services/redshift/types.rs`. legacy stores schemas/tables in
//! `map[string]*...` and sorts the names whenever catalog output is produced
//! (`sortedSchemaNames` / `sortedTableNames`, byte-wise `sort.Strings`);
//! `BTreeMap<String, _>` gives the same byte-wise ordering directly.

use std::collections::BTreeMap;

use crate::pg_types::{pg_field_type_name, PgField};

#[derive(Debug, Clone, Default)]
pub struct Database {
    pub schemas: BTreeMap<String, Schema>,
}

#[derive(Debug, Clone, Default)]
pub struct Schema {
    pub tables: BTreeMap<String, Table>,
}

#[derive(Debug, Clone, Default)]
pub struct Table {
    pub name: QualifiedName,
    pub columns: Vec<Column>,
    pub rows: Vec<Vec<String>>,
    /// "" (base table), "VIEW", or "MATERIALIZED VIEW".
    pub kind: String,
    pub view_sql: String,
    pub dist_style: String,
    pub dist_key: String,
    pub sort_keys: Vec<String>,
}

#[derive(Debug, Clone, Default)]
pub struct Column {
    pub name: String,
    pub data_type: String,
    pub encoding: String,
    pub default_value: String,
    pub identity: bool,
    pub dist_key: bool,
    pub sort_key: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct QualifiedName {
    pub schema: String,
    pub table: String,
}

impl Table {
    pub fn is_view(&self) -> bool {
        self.kind.eq_ignore_ascii_case("VIEW")
    }

    pub fn is_materialized_view(&self) -> bool {
        self.kind.eq_ignore_ascii_case("MATERIALIZED VIEW")
    }

    pub fn is_read_only_relation(&self) -> bool {
        self.is_view() || self.is_materialized_view()
    }
}

/// Mirrors `tableSnapshotType` (dashboard snapshot type strings).
pub fn table_snapshot_type(table: &Table) -> &'static str {
    if table.is_materialized_view() {
        return "MATERIALIZED_VIEW";
    }
    if table.is_view() {
        return "VIEW";
    }
    "TABLE"
}

/// Mirrors `informationSchemaTableType`.
pub fn information_schema_table_type(table: &Table) -> &'static str {
    if table.is_materialized_view() {
        return "MATERIALIZED VIEW";
    }
    if table.is_view() {
        return "VIEW";
    }
    "BASE TABLE"
}

/// Mirrors `pgClassRelKind`.
pub fn pg_class_rel_kind(table: &Table) -> &'static str {
    if table.is_materialized_view() {
        return "m";
    }
    if table.is_view() {
        return "v";
    }
    "r"
}

/// Mirrors `ensurePublicSchema` (storage.rs): `public` always exists.
pub fn ensure_public_schema(db: &mut Database) {
    db.schemas.entry("public".to_string()).or_default();
}

/// Mirrors `columnsFromFields` (types.rs): result fields become columns whose
/// declared type is the short PG type name ("int4", "varchar", ...).
pub fn columns_from_fields(fields: &[PgField]) -> Vec<Column> {
    fields
        .iter()
        .map(|field| Column {
            name: field.name.clone(),
            data_type: pg_field_type_name(field).to_string(),
            ..Column::default()
        })
        .collect()
}

pub fn lookup_table<'a>(db: &'a Database, name: &QualifiedName) -> Option<&'a Table> {
    db.schemas.get(&name.schema)?.tables.get(&name.table)
}

pub fn lookup_table_mut<'a>(db: &'a mut Database, name: &QualifiedName) -> Option<&'a mut Table> {
    db.schemas
        .get_mut(&name.schema)?
        .tables
        .get_mut(&name.table)
}
