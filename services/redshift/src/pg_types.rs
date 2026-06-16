//! PostgreSQL wire type constants and the SQL value/type inference rules.
//!
//! Parity: `internal/services/redshift/pgwire.rs` (the type OID constants) and
//! `internal/services/redshift/sql_types.rs`.

pub const PG_TYPE_BOOL_OID: i32 = 16;
pub const PG_TYPE_INT4_OID: i32 = 23;
pub const PG_TYPE_VARCHAR_OID: i32 = 1043;
pub const PG_TYPE_FLOAT8_OID: i32 = 701;
pub const PG_DEFAULT_BACKEND_PID: i32 = 1;

/// One result-column descriptor (name + PG type OID + type size), as carried in
/// a pgwire RowDescription. Mirrors legacy `pgField`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PgField {
    pub name: String,
    pub type_oid: i32,
    pub type_size: i16,
}

/// Mirrors `inferLiteralPGType`: integers → int4, true/false → bool, values
/// containing `.`/`e`/`E` that parse as floats → float8, everything else varchar.
pub fn infer_literal_pg_type(value: &str) -> (i32, i16) {
    if value.parse::<i64>().is_ok() {
        return (PG_TYPE_INT4_OID, 4);
    }
    if value.eq_ignore_ascii_case("true") || value.eq_ignore_ascii_case("false") {
        return (PG_TYPE_BOOL_OID, 1);
    }
    if value.contains(['.', 'e', 'E']) && value.parse::<f64>().is_ok() {
        return (PG_TYPE_FLOAT8_OID, 8);
    }
    (PG_TYPE_VARCHAR_OID, -1)
}

/// Mirrors `pgTypeOID`: substring-based mapping of a declared column type to a
/// PG type OID (any type containing "int" is int4, etc.).
pub fn pg_type_oid(data_type: &str) -> i32 {
    let normalized = data_type.to_lowercase();
    if normalized.contains("int") {
        return PG_TYPE_INT4_OID;
    }
    if normalized == "bool" || normalized == "boolean" {
        return PG_TYPE_BOOL_OID;
    }
    if normalized.contains("double") || normalized.contains("float") || normalized == "real" {
        return PG_TYPE_FLOAT8_OID;
    }
    PG_TYPE_VARCHAR_OID
}

/// Mirrors `pgTypeSize`.
pub fn pg_type_size(data_type: &str) -> i16 {
    match pg_type_oid(data_type) {
        PG_TYPE_INT4_OID => 4,
        PG_TYPE_BOOL_OID => 1,
        PG_TYPE_FLOAT8_OID => 8,
        _ => -1,
    }
}

/// Mirrors `pgFieldTypeName` (snapshot.rs): OID → short PG type name.
pub fn pg_field_type_name(field: &PgField) -> &'static str {
    match field.type_oid {
        PG_TYPE_INT4_OID => "int4",
        PG_TYPE_BOOL_OID => "bool",
        PG_TYPE_FLOAT8_OID => "float8",
        _ => "varchar",
    }
}
