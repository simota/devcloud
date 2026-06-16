//! Catalog emulation: information_schema, pg_catalog, SVV/STL/STV views.
//!
//! Parity: `internal/services/redshift/catalog.rs` — substring-based dispatch
//! over the statement, fixed synthetic result shapes, and a post-shaping pass
//! (`shapeCatalogResult`) that applies projection/WHERE/ORDER BY/LIMIT/COUNT
//! to the synthetic rows.

use std::sync::Arc;

use crate::engine::{parse_count_projection, QueryResult};
use crate::errors::SqlError;
use crate::model::{
    columns_from_fields, information_schema_table_type, pg_class_rel_kind, Database, Table,
};
use crate::pg_types::{
    pg_type_oid, PgField, PG_DEFAULT_BACKEND_PID, PG_TYPE_INT4_OID, PG_TYPE_VARCHAR_OID,
};
use crate::server::{default_str, ServerShared, ServerState, SharedConfig};
use crate::snapshot::{redshift_query_id, safe_sql_preview};
use crate::sql_parse::split_clause;
use crate::sql_predicates::{
    parse_limit, parse_order_by, parse_where_predicate, selected_columns,
    sort_rows_by_source_column,
};

/// Mirrors `isCatalogSelect`.
pub(crate) fn is_catalog_select(lower: &str) -> bool {
    lower.starts_with("select ")
        && (lower.contains(" information_schema.")
            || lower.contains(" pg_catalog.")
            || lower.contains(" svv_")
            || lower.contains(" stl_")
            || lower.contains(" stv_")
            || lower.contains(" pg_table_def"))
}

impl ServerShared {
    /// Mirrors `selectCatalog`: dispatch by substring, then shape the result.
    pub(crate) fn select_catalog(
        self: &Arc<Self>,
        statement: &str,
    ) -> Result<QueryResult, SqlError> {
        let lower = statement.to_ascii_lowercase();
        let result = {
            let state = self.lock_state();
            let config = &self.config;
            if lower.contains("information_schema.schemata") {
                catalog_schemata(config, &state.db)
            } else if lower.contains("information_schema.tables") {
                catalog_tables(config, &state.db)
            } else if lower.contains("information_schema.columns") {
                catalog_columns(config, &state.db)
            } else if lower.contains("pg_catalog.pg_namespace") {
                catalog_pg_namespace(&state.db)
            } else if lower.contains("pg_catalog.pg_database") {
                catalog_pg_database(config)
            } else if lower.contains("pg_catalog.pg_class") {
                catalog_pg_class(&state.db)
            } else if lower.contains("pg_catalog.pg_attribute") {
                catalog_pg_attribute(&state.db)
            } else if lower.contains("pg_catalog.pg_tables") {
                catalog_pg_tables(config, &state.db)
            } else if lower.contains("pg_catalog.pg_type") {
                catalog_pg_type()
            } else if lower.contains("pg_catalog.pg_user") {
                catalog_pg_user(config)
            } else if lower.contains("pg_table_def") {
                catalog_pg_table_def(&state.db)
            } else if lower.contains("svv_columns") {
                catalog_svv_columns(config, &state.db)
            } else if lower.contains("svv_mv_info") {
                catalog_svv_mv_info(config, &state.db)
            } else if lower.contains("svv_table_info") {
                catalog_svv_table_info(&state.db)
            } else if lower.contains("stl_query") {
                catalog_stl_query(&state)
            } else if lower.contains("stv_recents") {
                catalog_stv_recents()
            } else {
                return Err(SqlError::new(
                    "unsupported Redshift catalog query in local MVP",
                ));
            }
        };
        shape_catalog_result(statement, result)
    }
}

/// Mirrors `shapeCatalogResult`.
fn shape_catalog_result(statement: &str, result: QueryResult) -> Result<QueryResult, SqlError> {
    let lower = statement.to_ascii_lowercase();
    let Some(from_index) = lower.find(" from ") else {
        return Ok(result);
    };
    let column_part = statement["select".len()..from_index].trim();
    let rest = statement[from_index + " from ".len()..].trim();
    let (_, clause_part) = split_catalog_from_clause(rest);
    let table_state = table_from_query_result(&result);

    let (where_part, order_part, limit_part) = split_select_clauses(&clause_part);
    let where_predicate = parse_where_predicate(&table_state, &where_part)?;
    if let Some(count_alias) = parse_count_projection(&table_state, column_part)? {
        let count = result
            .rows
            .iter()
            .filter(|row| where_predicate.matches(row))
            .count();
        return Ok(QueryResult {
            fields: vec![PgField {
                name: count_alias,
                type_oid: PG_TYPE_INT4_OID,
                type_size: 4,
            }],
            rows: vec![vec![count.to_string()]],
            tag: "SELECT 1".to_string(),
        });
    }
    let (selected_indexes, fields) = selected_columns(&table_state, column_part)?;
    let order_index = parse_order_by(&table_state, &order_part)?;
    let limit = parse_limit(&limit_part)?;

    let mut rows: Vec<Vec<String>> = Vec::with_capacity(result.rows.len());
    for source_row in &result.rows {
        if !where_predicate.matches(source_row) {
            continue;
        }
        rows.push(
            selected_indexes
                .iter()
                .map(|&index| source_row[index].clone())
                .collect(),
        );
    }
    if let Some(order_index) = order_index {
        sort_rows_by_source_column(&mut rows, &selected_indexes, order_index);
    }
    if let Some(limit) = limit {
        if rows.len() > limit {
            rows.truncate(limit);
        }
    }
    let tag = format!("SELECT {}", rows.len());
    Ok(QueryResult { fields, rows, tag })
}

/// Mirrors `splitCatalogFromClause`.
fn split_catalog_from_clause(rest: &str) -> (String, String) {
    for separator in [" where ", " order by ", " limit "] {
        let (table_part, clause_part) = split_clause(rest, separator);
        if !clause_part.is_empty() {
            return (
                first_catalog_token(&table_part),
                format!("{separator}{clause_part}"),
            );
        }
    }
    (first_catalog_token(rest), String::new())
}

/// Mirrors `firstCatalogToken`.
fn first_catalog_token(value: &str) -> String {
    value
        .split_whitespace()
        .next()
        .unwrap_or_default()
        .to_string()
}

/// Mirrors `splitSelectClauses`, quirks included (a trailing `order by ... limit`
/// split can reset an already-found LIMIT, exactly like legacy).
fn split_select_clauses(value: &str) -> (String, String, String) {
    let clause_part = value.trim();
    let lower = clause_part.to_ascii_lowercase();
    let mut where_part = String::new();
    let mut order_part = String::new();
    let mut limit_part = String::new();
    if lower.starts_with("where ") {
        let trimmed = clause_part["where ".len()..].trim();
        let (next_where, next_order) = split_clause(trimmed, " order by ");
        order_part = next_order;
        let (next_where, next_limit) = split_clause(&next_where, " limit ");
        where_part = next_where;
        limit_part = next_limit;
    }
    if lower.starts_with("order by ") {
        order_part = clause_part["order by ".len()..].trim().to_string();
    }
    if lower.starts_with("limit ") {
        limit_part = clause_part["limit ".len()..].trim().to_string();
    }
    if !order_part.is_empty() {
        let (next_order, next_limit) = split_clause(&order_part, " limit ");
        order_part = next_order;
        limit_part = next_limit;
    }
    (where_part, order_part, limit_part)
}

/// Mirrors `tableFromQueryResult`: a synthetic table whose columns mirror the
/// result fields (used to evaluate WHERE/ORDER BY against catalog rows).
pub(crate) fn table_from_query_result(result: &QueryResult) -> Table {
    Table {
        columns: columns_from_fields(&result.fields),
        ..Table::default()
    }
}

fn varchar_field(name: &str) -> PgField {
    PgField {
        name: name.to_string(),
        type_oid: PG_TYPE_VARCHAR_OID,
        type_size: -1,
    }
}

fn int4_field(name: &str) -> PgField {
    PgField {
        name: name.to_string(),
        type_oid: PG_TYPE_INT4_OID,
        type_size: 4,
    }
}

fn select_tag(rows: &[Vec<String>]) -> String {
    format!("SELECT {}", rows.len())
}

/// Mirrors `catalogSchemata`.
fn catalog_schemata(config: &SharedConfig, db: &Database) -> QueryResult {
    let rows: Vec<Vec<String>> = db
        .schemas
        .keys()
        .map(|schema_name| {
            vec![
                default_str(&config.database, "dev"),
                schema_name.clone(),
                default_str(&config.user, "dev"),
            ]
        })
        .collect();
    QueryResult {
        fields: vec![
            varchar_field("catalog_name"),
            varchar_field("schema_name"),
            varchar_field("schema_owner"),
        ],
        tag: select_tag(&rows),
        rows,
    }
}

/// Mirrors `catalogTables`.
fn catalog_tables(config: &SharedConfig, db: &Database) -> QueryResult {
    let rows = catalog_table_rows(config, db);
    QueryResult {
        fields: vec![
            varchar_field("table_catalog"),
            varchar_field("table_schema"),
            varchar_field("table_name"),
            varchar_field("table_type"),
        ],
        tag: select_tag(&rows),
        rows,
    }
}

/// Mirrors `catalogColumns`.
fn catalog_columns(config: &SharedConfig, db: &Database) -> QueryResult {
    let rows = catalog_column_rows(config, db);
    QueryResult {
        fields: vec![
            varchar_field("table_catalog"),
            varchar_field("table_schema"),
            varchar_field("table_name"),
            varchar_field("column_name"),
            int4_field("ordinal_position"),
            varchar_field("column_default"),
            varchar_field("data_type"),
            varchar_field("encoding"),
        ],
        tag: select_tag(&rows),
        rows,
    }
}

/// Mirrors `catalogPGNamespace`.
fn catalog_pg_namespace(db: &Database) -> QueryResult {
    let rows: Vec<Vec<String>> = db
        .schemas
        .keys()
        .enumerate()
        .map(|(i, schema_name)| vec![(2200 + i).to_string(), schema_name.clone()])
        .collect();
    QueryResult {
        fields: vec![int4_field("oid"), varchar_field("nspname")],
        tag: select_tag(&rows),
        rows,
    }
}

/// Mirrors `catalogPGDatabase`.
fn catalog_pg_database(config: &SharedConfig) -> QueryResult {
    QueryResult {
        fields: vec![
            int4_field("oid"),
            varchar_field("datname"),
            int4_field("datdba"),
            int4_field("encoding"),
            varchar_field("datistemplate"),
            varchar_field("datallowconn"),
        ],
        rows: vec![vec![
            "1".to_string(),
            default_str(&config.database, "dev"),
            "10".to_string(),
            "6".to_string(),
            "false".to_string(),
            "true".to_string(),
        ]],
        tag: "SELECT 1".to_string(),
    }
}

/// Mirrors `catalogPGClass`.
fn catalog_pg_class(db: &Database) -> QueryResult {
    let mut rows = Vec::new();
    for (schema_name, schema_state) in &db.schemas {
        for (table_name, table_state) in &schema_state.tables {
            rows.push(vec![
                catalog_table_oid(schema_name, table_name),
                table_name.clone(),
                pg_class_rel_kind(table_state).to_string(),
            ]);
        }
    }
    QueryResult {
        fields: vec![
            int4_field("oid"),
            varchar_field("relname"),
            varchar_field("relkind"),
        ],
        tag: select_tag(&rows),
        rows,
    }
}

/// Mirrors `catalogPGAttribute`.
fn catalog_pg_attribute(db: &Database) -> QueryResult {
    let mut rows = Vec::new();
    for (schema_name, schema_state) in &db.schemas {
        for (table_name, table_state) in &schema_state.tables {
            for (i, column) in table_state.columns.iter().enumerate() {
                rows.push(vec![
                    catalog_table_oid(schema_name, table_name),
                    (i + 1).to_string(),
                    column.name.clone(),
                    pg_type_oid(&column.data_type).to_string(),
                ]);
            }
        }
    }
    QueryResult {
        fields: vec![
            int4_field("attrelid"),
            int4_field("attnum"),
            varchar_field("attname"),
            int4_field("atttypid"),
        ],
        tag: select_tag(&rows),
        rows,
    }
}

/// Mirrors `catalogPGTables`.
fn catalog_pg_tables(config: &SharedConfig, db: &Database) -> QueryResult {
    let mut rows = Vec::new();
    for row in catalog_table_rows(config, db) {
        if row[3] != "BASE TABLE" {
            continue;
        }
        rows.push(vec![
            row[1].clone(),
            row[2].clone(),
            default_str(&config.user, "dev"),
        ]);
    }
    QueryResult {
        fields: vec![
            varchar_field("schemaname"),
            varchar_field("tablename"),
            varchar_field("tableowner"),
        ],
        tag: select_tag(&rows),
        rows,
    }
}

/// Mirrors `catalogPGType`.
fn catalog_pg_type() -> QueryResult {
    let rows = vec![
        vec![
            PG_TYPE_INT4_OID.to_string(),
            "int4".to_string(),
            "4".to_string(),
            "N".to_string(),
        ],
        vec![
            PG_TYPE_VARCHAR_OID.to_string(),
            "varchar".to_string(),
            "-1".to_string(),
            "S".to_string(),
        ],
        vec![
            "25".to_string(),
            "text".to_string(),
            "-1".to_string(),
            "S".to_string(),
        ],
        vec![
            "16".to_string(),
            "bool".to_string(),
            "1".to_string(),
            "B".to_string(),
        ],
    ];
    QueryResult {
        fields: vec![
            int4_field("oid"),
            varchar_field("typname"),
            int4_field("typlen"),
            varchar_field("typcategory"),
        ],
        tag: select_tag(&rows),
        rows,
    }
}

/// Mirrors `catalogPGUser`.
fn catalog_pg_user(config: &SharedConfig) -> QueryResult {
    QueryResult {
        fields: vec![
            varchar_field("usename"),
            int4_field("usesysid"),
            varchar_field("usecreatedb"),
            varchar_field("usesuper"),
            varchar_field("passwd"),
        ],
        rows: vec![vec![
            default_str(&config.user, "dev"),
            "10".to_string(),
            "true".to_string(),
            "true".to_string(),
            "********".to_string(),
        ]],
        tag: "SELECT 1".to_string(),
    }
}

/// Mirrors `catalogSVVTableInfo` (skips plain views, includes MVs).
fn catalog_svv_table_info(db: &Database) -> QueryResult {
    let mut rows = Vec::new();
    for (schema_name, schema_state) in &db.schemas {
        for (table_name, table_state) in &schema_state.tables {
            if table_state.is_view() {
                continue;
            }
            rows.push(vec![
                schema_name.clone(),
                table_name.clone(),
                default_str(&table_state.dist_style, "even"),
                table_state.dist_key.clone(),
                table_state.sort_keys.join(","),
                table_state.rows.len().to_string(),
            ]);
        }
    }
    QueryResult {
        fields: vec![
            varchar_field("schema"),
            varchar_field("table"),
            varchar_field("diststyle"),
            varchar_field("distkey"),
            varchar_field("sortkey1"),
            int4_field("tbl_rows"),
        ],
        tag: select_tag(&rows),
        rows,
    }
}

/// Mirrors `catalogSVVColumns`.
fn catalog_svv_columns(config: &SharedConfig, db: &Database) -> QueryResult {
    let rows = catalog_column_rows(config, db);
    QueryResult {
        fields: vec![
            varchar_field("table_catalog"),
            varchar_field("table_schema"),
            varchar_field("table_name"),
            varchar_field("column_name"),
            int4_field("ordinal_position"),
            varchar_field("column_default"),
            varchar_field("data_type"),
            varchar_field("encoding"),
        ],
        tag: select_tag(&rows),
        rows,
    }
}

/// Mirrors `catalogSVVMVInfo`.
fn catalog_svv_mv_info(config: &SharedConfig, db: &Database) -> QueryResult {
    let mut rows = Vec::new();
    for (schema_name, schema_state) in &db.schemas {
        for (table_name, table_state) in &schema_state.tables {
            if !table_state.is_materialized_view() {
                continue;
            }
            rows.push(vec![
                schema_name.clone(),
                table_name.clone(),
                default_str(&config.user, "dev"),
                "1".to_string(),
                "false".to_string(),
                "false".to_string(),
            ]);
        }
    }
    QueryResult {
        fields: vec![
            varchar_field("schema"),
            varchar_field("name"),
            varchar_field("owner_user_name"),
            int4_field("state"),
            varchar_field("autorefresh"),
            varchar_field("is_stale"),
        ],
        tag: select_tag(&rows),
        rows,
    }
}

/// Mirrors `catalogPGTableDef` (skips plain views, includes MVs).
fn catalog_pg_table_def(db: &Database) -> QueryResult {
    let mut rows = Vec::new();
    for (schema_name, schema_state) in &db.schemas {
        for (table_name, table_state) in &schema_state.tables {
            if table_state.is_view() {
                continue;
            }
            for column in &table_state.columns {
                let dist_key = (column.name == table_state.dist_key).to_string();
                let mut sort_key = "0".to_string();
                for (sort_index, sort_column) in table_state.sort_keys.iter().enumerate() {
                    if &column.name == sort_column {
                        sort_key = (sort_index + 1).to_string();
                        break;
                    }
                }
                rows.push(vec![
                    schema_name.clone(),
                    table_name.clone(),
                    column.name.clone(),
                    column.data_type.clone(),
                    column.encoding.clone(),
                    dist_key,
                    sort_key,
                    "false".to_string(),
                ]);
            }
        }
    }
    QueryResult {
        fields: vec![
            varchar_field("schemaname"),
            varchar_field("tablename"),
            varchar_field("column"),
            varchar_field("type"),
            varchar_field("encoding"),
            varchar_field("distkey"),
            int4_field("sortkey"),
            varchar_field("notnull"),
        ],
        tag: select_tag(&rows),
        rows,
    }
}

/// Mirrors `catalogSTLQuery` (statement history; populated by parts 3-4).
fn catalog_stl_query(state: &ServerState) -> QueryResult {
    let rows: Vec<Vec<String>> = state
        .statements
        .values()
        .map(|stmt| {
            let (preview, _, _) = safe_sql_preview(&stmt.query_string, 200);
            vec![
                redshift_query_id(&stmt.id).to_string(),
                preview,
                stmt.status.clone(),
            ]
        })
        .collect();
    QueryResult {
        fields: vec![
            int4_field("query"),
            varchar_field("querytxt"),
            varchar_field("status"),
        ],
        tag: select_tag(&rows),
        rows,
    }
}

/// Mirrors `catalogSTVRecents`.
fn catalog_stv_recents() -> QueryResult {
    QueryResult {
        fields: vec![int4_field("pid"), varchar_field("status")],
        rows: vec![vec![PG_DEFAULT_BACKEND_PID.to_string(), "Idle".to_string()]],
        tag: "SELECT 1".to_string(),
    }
}

/// Mirrors `catalogTableRowsLocked`.
fn catalog_table_rows(config: &SharedConfig, db: &Database) -> Vec<Vec<String>> {
    let mut rows = Vec::new();
    for (schema_name, schema_state) in &db.schemas {
        for (table_name, table_state) in &schema_state.tables {
            rows.push(vec![
                default_str(&config.database, "dev"),
                schema_name.clone(),
                table_name.clone(),
                information_schema_table_type(table_state).to_string(),
            ]);
        }
    }
    rows
}

/// Mirrors `catalogColumnRowsLocked`.
fn catalog_column_rows(config: &SharedConfig, db: &Database) -> Vec<Vec<String>> {
    let mut rows = Vec::new();
    for (schema_name, schema_state) in &db.schemas {
        for (table_name, table_state) in &schema_state.tables {
            for (i, column) in table_state.columns.iter().enumerate() {
                rows.push(vec![
                    default_str(&config.database, "dev"),
                    schema_name.clone(),
                    table_name.clone(),
                    column.name.clone(),
                    (i + 1).to_string(),
                    column.default_value.clone(),
                    column.data_type.clone(),
                    column.encoding.clone(),
                ]);
            }
        }
    }
    rows
}

/// Mirrors `catalogTableOID`: a deterministic hash of `schema.table` with
/// legacy exact wrapping/negation arithmetic.
fn catalog_table_oid(schema_name: &str, table_name: &str) -> String {
    let mut value: i64 = 10000;
    for ch in format!("{schema_name}.{table_name}").chars() {
        value = value.wrapping_mul(31).wrapping_add(ch as i64);
        if value < 0 {
            value = value.wrapping_neg();
        }
    }
    (value % 1_000_000_000).to_string()
}
