//! The in-memory SQL engine.
//!
//! Parity: `internal/services/redshift/sql_memory.rs` plus the SQL-foundation
//! helpers from `sql_execute.rs` (`showParameter`, `stringFunctionResult`).
//! This is deliberately a tiny statement-prefix dispatcher, not a SQL engine —
//! it supports exactly what the legacy local MVP supports, quirks included.

use std::sync::Arc;

use crate::catalog::{is_catalog_select, table_from_query_result};
use crate::errors::SqlError;
use crate::model::{
    columns_from_fields, ensure_public_schema, lookup_table, lookup_table_mut, Table,
};
use crate::pg_types::{
    infer_literal_pg_type, PgField, PG_DEFAULT_BACKEND_PID, PG_TYPE_INT4_OID, PG_TYPE_VARCHAR_OID,
};
use crate::server::{default_str, ServerShared};
use crate::sql_parse::{
    apply_column_table_attributes, clean_column_identifier, clean_identifier,
    first_identifier_token, matching_paren, parse_columns, parse_qualified_name,
    parse_select_alias, parse_select_literal, parse_table_attributes, parse_values_tuples,
    split_clause, split_comma_separated, split_top_level_clause,
};
use crate::sql_predicates::{
    build_insert_row, column_index, parse_assignments, parse_limit, parse_order_by,
    parse_where_predicate, selected_columns, sort_rows_by_source_column,
};

/// Mirrors legacy `queryResult`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct QueryResult {
    pub fields: Vec<PgField>,
    pub rows: Vec<Vec<String>>,
    pub tag: String,
}

impl QueryResult {
    pub fn tag_only(tag: &str) -> QueryResult {
        QueryResult {
            tag: tag.to_string(),
            ..QueryResult::default()
        }
    }
}

/// Mirrors `stringFunctionResult`.
fn string_function_result(name: &str, value: &str) -> QueryResult {
    QueryResult {
        fields: vec![PgField {
            name: name.to_string(),
            type_oid: PG_TYPE_VARCHAR_OID,
            type_size: -1,
        }],
        rows: vec![vec![value.to_string()]],
        tag: "SELECT 1".to_string(),
    }
}

impl ServerShared {
    /// Mirrors `executeSQLMemory`: the statement dispatcher. Case order is
    /// load-bearing (e.g. catalog SELECTs are matched before generic SELECTs).
    pub(crate) fn execute_sql_memory(
        self: &Arc<Self>,
        statement: &str,
    ) -> Result<QueryResult, SqlError> {
        let normalized = statement.trim_end_matches(';').trim();
        if normalized.is_empty() {
            return Ok(QueryResult::default());
        }
        self.validate_statement_size(normalized)?;
        let lower = normalized.to_ascii_lowercase();
        if normalized.eq_ignore_ascii_case("select 1") {
            return Ok(QueryResult {
                fields: vec![PgField {
                    name: "?column?".to_string(),
                    type_oid: PG_TYPE_INT4_OID,
                    type_size: 4,
                }],
                rows: vec![vec!["1".to_string()]],
                tag: "SELECT 1".to_string(),
            });
        }
        if let Some(result) = self.builtin_scalar_select(normalized) {
            return Ok(result);
        }
        if normalized.eq_ignore_ascii_case("select pg_backend_pid()") {
            return Ok(QueryResult {
                fields: vec![PgField {
                    name: "pg_backend_pid".to_string(),
                    type_oid: PG_TYPE_INT4_OID,
                    type_size: 4,
                }],
                rows: vec![vec![PG_DEFAULT_BACKEND_PID.to_string()]],
                tag: "SELECT 1".to_string(),
            });
        }
        if lower.starts_with("set ") {
            return Ok(QueryResult::tag_only("SET"));
        }
        if lower.starts_with("show ") {
            return self.show_parameter(normalized);
        }
        if lower == "begin" || lower == "begin transaction" || lower == "start transaction" {
            return Ok(QueryResult::tag_only("BEGIN"));
        }
        if lower == "commit" || lower == "end" {
            return Ok(QueryResult::tag_only("COMMIT"));
        }
        if lower == "rollback" {
            return Ok(QueryResult::tag_only("ROLLBACK"));
        }
        if lower.starts_with("create schema") {
            return self.create_schema(normalized);
        }
        if lower.starts_with("drop schema") {
            return self.drop_schema(normalized);
        }
        if lower.starts_with("drop materialized view") {
            return self.drop_materialized_view(normalized);
        }
        if lower.starts_with("drop view") {
            return self.drop_view(normalized);
        }
        if lower.starts_with("drop table") {
            return self.drop_table(normalized);
        }
        if lower.starts_with("create materialized view") {
            return self.create_materialized_view(normalized);
        }
        if lower.starts_with("create view") || lower.starts_with("create or replace view") {
            return self.create_view(normalized);
        }
        if lower.starts_with("create table") {
            return self.create_table(normalized);
        }
        if lower.starts_with("insert into") {
            return self.insert_into(normalized);
        }
        if lower.starts_with("update ") {
            return self.update_table(normalized);
        }
        if lower.starts_with("delete from ") {
            return self.delete_from(normalized);
        }
        if is_catalog_select(&lower) {
            return self.select_catalog(normalized);
        }
        if lower.starts_with("select ") {
            if !lower.contains(" from ") {
                return select_literals(normalized);
            }
            return self.select_from_table(normalized);
        }
        if lower.starts_with("copy ") {
            return self.copy_from_local_csv(normalized);
        }
        if lower.starts_with("unload ") {
            return self.unload_to_local_csv(normalized);
        }
        Err(SqlError::new("unsupported Redshift SQL in local MVP"))
    }

    /// Recursion seam for nested SELECTs (views, CTAS, materialized views).
    /// legacy routes these through `Server.executeSQL` (translator + backend);
    /// with part 1's passthrough translator + memory backend that path is
    /// exactly the memory engine. Part 2 rewires this through the translator.
    fn execute_sql_nested(self: &Arc<Self>, statement: &str) -> Result<QueryResult, SqlError> {
        self.execute_sql_memory(statement)
    }

    /// Returns a `string_function_result` for the handful of no-arg scalar
    /// SELECTs that share an identical response shape.  Returns `None` for
    /// any statement that is not one of these builtins (falls through to the
    /// rest of the dispatcher).
    fn builtin_scalar_select(&self, normalized: &str) -> Option<QueryResult> {
        if normalized.eq_ignore_ascii_case("select current_database()") {
            return Some(string_function_result(
                "current_database",
                &default_str(&self.config.database, "dev"),
            ));
        }
        if normalized.eq_ignore_ascii_case("select current_schema()") {
            return Some(string_function_result("current_schema", "public"));
        }
        if normalized.eq_ignore_ascii_case("select current_user")
            || normalized.eq_ignore_ascii_case("select current_user()")
        {
            return Some(string_function_result(
                "current_user",
                &default_str(&self.config.user, "dev"),
            ));
        }
        if normalized.eq_ignore_ascii_case("select session_user")
            || normalized.eq_ignore_ascii_case("select session_user()")
        {
            return Some(string_function_result(
                "session_user",
                &default_str(&self.config.user, "dev"),
            ));
        }
        if normalized.eq_ignore_ascii_case("select version()") {
            return Some(string_function_result(
                "version",
                "PostgreSQL 8.0.2 on devcloud Redshift-compatible local server",
            ));
        }
        None
    }

    /// Mirrors `showParameter`.
    fn show_parameter(&self, statement: &str) -> Result<QueryResult, SqlError> {
        let name = statement["show ".len()..].trim();
        if name.is_empty() {
            return Err(SqlError::new("SHOW requires a parameter name"));
        }
        let normalized = name.trim_matches('"').to_lowercase();
        let value = match normalized.as_str() {
            "application_name" => String::new(),
            "client_encoding" => "UTF8".to_string(),
            "datestyle" => "ISO, MDY".to_string(),
            "integer_datetimes" => "on".to_string(),
            "is_superuser" => "on".to_string(),
            "search_path" => "public".to_string(),
            "server_encoding" => "UTF8".to_string(),
            "server_version" => "8.0.2".to_string(),
            "session_authorization" => default_str(&self.config.user, "dev"),
            "standard_conforming_strings" => "on".to_string(),
            "transaction isolation level" => "read committed".to_string(),
            _ => {
                return Err(SqlError::new(format!("unsupported SHOW parameter: {name}")));
            }
        };
        Ok(QueryResult {
            fields: vec![PgField {
                name: normalized,
                type_oid: PG_TYPE_VARCHAR_OID,
                type_size: -1,
            }],
            rows: vec![vec![value]],
            tag: "SHOW".to_string(),
        })
    }

    /// Mirrors `createSchema`.
    fn create_schema(&self, statement: &str) -> Result<QueryResult, SqlError> {
        let mut rest = statement["create schema".len()..].trim();
        if rest.to_ascii_lowercase().starts_with("if not exists ") {
            rest = rest["if not exists ".len()..].trim();
        }
        let name = rest.trim_matches('"');
        if name.is_empty() {
            return Err(SqlError::new("CREATE SCHEMA requires a schema name"));
        }
        let mut state = self.lock_state();
        state.db.schemas.entry(name.to_string()).or_default();
        self.persist_locked(&state)?;
        Ok(QueryResult::tag_only("CREATE SCHEMA"))
    }

    /// Mirrors `dropSchema`.
    fn drop_schema(&self, statement: &str) -> Result<QueryResult, SqlError> {
        let mut rest = statement["drop schema".len()..].trim();
        if rest.to_ascii_lowercase().starts_with("if exists ") {
            rest = rest["if exists ".len()..].trim();
        }
        let first = rest
            .split_whitespace()
            .next()
            .ok_or_else(|| SqlError::new("DROP SCHEMA requires a schema name"))?;
        let name = clean_identifier(first);
        if name.is_empty() {
            return Err(SqlError::new("DROP SCHEMA requires a schema name"));
        }
        let mut state = self.lock_state();
        state.db.schemas.remove(&name);
        ensure_public_schema(&mut state.db);
        self.persist_locked(&state)?;
        Ok(QueryResult::tag_only("DROP SCHEMA"))
    }

    /// Mirrors `dropTable`.
    fn drop_table(&self, statement: &str) -> Result<QueryResult, SqlError> {
        let mut rest = statement["drop table".len()..].trim();
        if rest.to_ascii_lowercase().starts_with("if exists ") {
            rest = rest["if exists ".len()..].trim();
        }
        let name = parse_qualified_name(rest);
        let mut state = self.lock_state();
        if let Some(schema) = state.db.schemas.get_mut(&name.schema) {
            schema.tables.remove(&name.table);
        }
        self.persist_locked(&state)?;
        Ok(QueryResult::tag_only("DROP TABLE"))
    }

    /// Mirrors `createTable`.
    fn create_table(self: &Arc<Self>, statement: &str) -> Result<QueryResult, SqlError> {
        if let Some(result) = self.create_table_as(statement)? {
            return Ok(result);
        }
        let open = statement.find('(');
        let close = open.and_then(|open| matching_paren(statement, open));
        let (Some(open), Some(close)) = (open, close) else {
            return Err(SqlError::new("CREATE TABLE requires a column list"));
        };
        let mut name_part = statement["create table".len()..open].trim();
        if name_part.to_lowercase().starts_with("if not exists ") {
            name_part = name_part["if not exists ".len()..].trim();
        }
        let name = parse_qualified_name(name_part);
        let columns = parse_columns(&statement[open + 1..close])?;
        let (mut dist_style, mut dist_key, mut sort_keys) =
            parse_table_attributes(&statement[close + 1..]);
        apply_column_table_attributes(&columns, &mut dist_style, &mut dist_key, &mut sort_keys);
        let mut state = self.lock_state();
        let schema_state = state.db.schemas.entry(name.schema.clone()).or_default();
        schema_state.tables.insert(
            name.table.clone(),
            Table {
                name,
                columns,
                dist_style,
                dist_key,
                sort_keys,
                ..Table::default()
            },
        );
        self.persist_locked(&state)?;
        Ok(QueryResult::tag_only("CREATE TABLE"))
    }

    /// Mirrors `createTableAs`: returns Ok(None) when the statement is not a
    /// CREATE TABLE AS (legacy `ok == false`).
    fn create_table_as(self: &Arc<Self>, statement: &str) -> Result<Option<QueryResult>, SqlError> {
        let mut rest = statement["create table".len()..].trim();
        if rest.to_lowercase().starts_with("if not exists ") {
            rest = rest["if not exists ".len()..].trim();
        }
        let (name_part, query_part) = split_top_level_clause(rest, " as ");
        if query_part.is_empty() {
            return Ok(None);
        }
        if !query_part.trim().to_lowercase().starts_with("select ") {
            return Err(SqlError::new("CREATE TABLE AS requires SELECT"));
        }
        let name_token = first_identifier_token(&name_part);
        if name_token.trim().is_empty() {
            return Err(SqlError::new("CREATE TABLE AS requires a table name"));
        }
        let name = parse_qualified_name(name_token);
        let result = self.execute_sql_nested(&query_part)?;
        let columns = columns_from_fields(&result.fields);
        if columns.is_empty() {
            return Err(SqlError::new("CREATE TABLE AS SELECT must return columns"));
        }
        let attributes = name_part
            .trim()
            .strip_prefix(name_token)
            .unwrap_or(name_part.trim())
            .trim()
            .to_string();
        let (dist_style, dist_key, sort_keys) = parse_table_attributes(&attributes);

        let mut state = self.lock_state();
        let schema_state = state.db.schemas.entry(name.schema.clone()).or_default();
        let row_count = result.rows.len();
        schema_state.tables.insert(
            name.table.clone(),
            Table {
                name,
                columns,
                rows: result.rows,
                dist_style,
                dist_key,
                sort_keys,
                ..Table::default()
            },
        );
        self.persist_locked(&state)?;
        Ok(Some(QueryResult::tag_only(&format!("SELECT {row_count}"))))
    }

    /// Mirrors `createView`.
    fn create_view(self: &Arc<Self>, statement: &str) -> Result<QueryResult, SqlError> {
        let mut rest = statement["create ".len()..].trim();
        let mut or_replace = false;
        if rest.to_lowercase().starts_with("or replace ") {
            or_replace = true;
            rest = rest["or replace ".len()..].trim();
        }
        if !rest.to_lowercase().starts_with("view ") {
            return Err(SqlError::new("CREATE VIEW requires VIEW"));
        }
        let rest = rest["view ".len()..].trim();
        let (name_part, query_part) = split_clause(rest, " as ");
        if name_part.is_empty() || query_part.is_empty() {
            return Err(SqlError::new("CREATE VIEW requires name and AS SELECT"));
        }
        let name = parse_qualified_name(&name_part);
        if !query_part.trim().to_lowercase().starts_with("select ") {
            return Err(SqlError::new("CREATE VIEW requires SELECT"));
        }
        let result = self.execute_sql_nested(&query_part)?;
        let columns = columns_from_fields(&result.fields);
        if columns.is_empty() {
            return Err(SqlError::new("CREATE VIEW SELECT must return columns"));
        }

        let mut state = self.lock_state();
        let schema_state = state.db.schemas.entry(name.schema.clone()).or_default();
        if let Some(existing) = schema_state.tables.get(&name.table) {
            if !existing.is_view() {
                return Err(SqlError::new(format!(
                    "relation {}.{} already exists",
                    name.schema, name.table
                )));
            }
            if !or_replace {
                return Err(SqlError::new(format!(
                    "view {}.{} already exists",
                    name.schema, name.table
                )));
            }
        }
        schema_state.tables.insert(
            name.table.clone(),
            Table {
                name,
                columns,
                kind: "VIEW".to_string(),
                view_sql: query_part.trim().to_string(),
                ..Table::default()
            },
        );
        self.persist_locked(&state)?;
        Ok(QueryResult::tag_only("CREATE VIEW"))
    }

    /// Mirrors `createMaterializedView`.
    fn create_materialized_view(
        self: &Arc<Self>,
        statement: &str,
    ) -> Result<QueryResult, SqlError> {
        let mut rest = statement["create materialized view".len()..].trim();
        if rest.to_lowercase().starts_with("if not exists ") {
            rest = rest["if not exists ".len()..].trim();
        }
        let (name_part, query_part) = split_top_level_clause(rest, " as ");
        if name_part.is_empty() || query_part.is_empty() {
            return Err(SqlError::new(
                "CREATE MATERIALIZED VIEW requires name and AS SELECT",
            ));
        }
        if !query_part.trim().to_lowercase().starts_with("select ") {
            return Err(SqlError::new("CREATE MATERIALIZED VIEW requires SELECT"));
        }
        let name_token = first_identifier_token(&name_part);
        if name_token.is_empty() {
            return Err(SqlError::new(
                "CREATE MATERIALIZED VIEW requires a view name",
            ));
        }
        let name = parse_qualified_name(name_token);
        let attributes = name_part
            .trim()
            .strip_prefix(name_token)
            .unwrap_or(name_part.trim())
            .trim()
            .to_string();
        let (dist_style, dist_key, sort_keys) = parse_table_attributes(&attributes);
        let result = self.execute_sql_nested(&query_part)?;
        let columns = columns_from_fields(&result.fields);
        if columns.is_empty() {
            return Err(SqlError::new(
                "CREATE MATERIALIZED VIEW SELECT must return columns",
            ));
        }

        let mut state = self.lock_state();
        let schema_state = state.db.schemas.entry(name.schema.clone()).or_default();
        if let Some(existing) = schema_state.tables.get(&name.table) {
            if !existing.is_materialized_view() {
                return Err(SqlError::new(format!(
                    "relation {}.{} already exists",
                    name.schema, name.table
                )));
            }
        }
        schema_state.tables.insert(
            name.table.clone(),
            Table {
                name,
                columns,
                rows: result.rows,
                kind: "MATERIALIZED VIEW".to_string(),
                view_sql: query_part.trim().to_string(),
                dist_style,
                dist_key,
                sort_keys,
            },
        );
        self.persist_locked(&state)?;
        Ok(QueryResult::tag_only("CREATE MATERIALIZED VIEW"))
    }

    /// Mirrors `dropView`.
    fn drop_view(&self, statement: &str) -> Result<QueryResult, SqlError> {
        let mut rest = statement["drop view".len()..].trim();
        if rest.to_ascii_lowercase().starts_with("if exists ") {
            rest = rest["if exists ".len()..].trim();
        }
        let name = parse_qualified_name(rest);
        let mut state = self.lock_state();
        if let Some(schema) = state.db.schemas.get_mut(&name.schema) {
            if schema.tables.get(&name.table).is_some_and(Table::is_view) {
                schema.tables.remove(&name.table);
            }
        }
        self.persist_locked(&state)?;
        Ok(QueryResult::tag_only("DROP VIEW"))
    }

    /// Mirrors `dropMaterializedView`.
    fn drop_materialized_view(&self, statement: &str) -> Result<QueryResult, SqlError> {
        let mut rest = statement["drop materialized view".len()..].trim();
        if rest.to_ascii_lowercase().starts_with("if exists ") {
            rest = rest["if exists ".len()..].trim();
        }
        let name = parse_qualified_name(rest);
        let mut state = self.lock_state();
        if let Some(schema) = state.db.schemas.get_mut(&name.schema) {
            if schema
                .tables
                .get(&name.table)
                .is_some_and(Table::is_materialized_view)
            {
                schema.tables.remove(&name.table);
            }
        }
        self.persist_locked(&state)?;
        Ok(QueryResult::tag_only("DROP MATERIALIZED VIEW"))
    }

    /// Mirrors `insertInto`.
    fn insert_into(&self, statement: &str) -> Result<QueryResult, SqlError> {
        let lower = statement.to_ascii_lowercase();
        let values_index = lower
            .find(" values ")
            .ok_or_else(|| SqlError::new("INSERT requires VALUES"))?;
        let mut name_part = statement["insert into".len()..values_index]
            .trim()
            .to_string();
        let mut insert_columns: Vec<String> = Vec::new();
        if let Some(column_list_index) = name_part.find('(') {
            let close = matching_paren(&name_part, column_list_index)
                .ok_or_else(|| SqlError::new("INSERT column list is unterminated"))?;
            for column_name in split_comma_separated(&name_part[column_list_index + 1..close]) {
                let cleaned = clean_identifier(&column_name);
                if cleaned.is_empty() {
                    return Err(SqlError::new("INSERT column list contains an empty column"));
                }
                insert_columns.push(cleaned);
            }
            name_part = name_part[..column_list_index].trim().to_string();
        }
        let name = parse_qualified_name(&name_part);
        let value_rows = parse_values_tuples(&statement[values_index + " values ".len()..])?;
        let mut state = self.lock_state();
        let Some(table) = lookup_table_mut(&mut state.db, &name) else {
            return Err(SqlError::new(format!(
                "table {}.{} does not exist",
                name.schema, name.table
            )));
        };
        if table.is_read_only_relation() {
            return Err(SqlError::new(format!(
                "cannot insert into view {}.{}",
                name.schema, name.table
            )));
        }
        let mut inserted = 0;
        for values in &value_rows {
            let row = build_insert_row(table, &insert_columns, values)?;
            table.rows.push(row);
            inserted += 1;
        }
        self.persist_locked(&state)?;
        Ok(QueryResult::tag_only(&format!("INSERT 0 {inserted}")))
    }

    /// Mirrors `updateTable`.
    fn update_table(&self, statement: &str) -> Result<QueryResult, SqlError> {
        let lower = statement.to_ascii_lowercase();
        let set_index = lower
            .find(" set ")
            .ok_or_else(|| SqlError::new("UPDATE requires SET"))?;
        let name = parse_qualified_name(&statement["update ".len()..set_index]);
        let (assignments_part, where_part) =
            split_clause(&statement[set_index + " set ".len()..], " where ");

        let mut state = self.lock_state();
        let Some(table) = lookup_table_mut(&mut state.db, &name) else {
            return Err(SqlError::new(format!(
                "table {}.{} does not exist",
                name.schema, name.table
            )));
        };
        if table.is_read_only_relation() {
            return Err(SqlError::new(format!(
                "cannot update view {}.{}",
                name.schema, name.table
            )));
        }
        let assignments = parse_assignments(table, &assignments_part)?;
        let where_predicate = parse_where_predicate(table, &where_part)?;
        let mut updated = 0;
        for stored in &mut table.rows {
            if !where_predicate.matches(stored) {
                continue;
            }
            for assignment in &assignments {
                stored[assignment.index] = assignment.value.clone();
            }
            updated += 1;
        }
        self.persist_locked(&state)?;
        Ok(QueryResult::tag_only(&format!("UPDATE {updated}")))
    }

    /// Mirrors `deleteFrom`.
    fn delete_from(&self, statement: &str) -> Result<QueryResult, SqlError> {
        let rest = statement["delete from ".len()..].trim();
        let (table_part, where_part) = split_clause(rest, " where ");
        let name = parse_qualified_name(&table_part);

        let mut state = self.lock_state();
        let Some(table) = lookup_table_mut(&mut state.db, &name) else {
            return Err(SqlError::new(format!(
                "table {}.{} does not exist",
                name.schema, name.table
            )));
        };
        if table.is_read_only_relation() {
            return Err(SqlError::new(format!(
                "cannot delete from view {}.{}",
                name.schema, name.table
            )));
        }
        let where_predicate = parse_where_predicate(table, &where_part)?;
        let before = table.rows.len();
        table.rows.retain(|stored| !where_predicate.matches(stored));
        let deleted = before - table.rows.len();
        self.persist_locked(&state)?;
        Ok(QueryResult::tag_only(&format!("DELETE {deleted}")))
    }

    /// Mirrors `selectFromTable` (including view resolution via the nested
    /// execution seam, run outside the state lock exactly like legacy).
    fn select_from_table(self: &Arc<Self>, statement: &str) -> Result<QueryResult, SqlError> {
        let lower = statement.to_ascii_lowercase();
        let from_index = lower
            .find(" from ")
            .ok_or_else(|| SqlError::new("SELECT requires FROM"))?;
        let column_part = statement["select".len()..from_index].trim().to_string();
        let rest = statement[from_index + " from ".len()..].trim();
        let (table_part, where_part, order_part, limit_part) = split_select_clauses(rest);
        let name = parse_qualified_name(&table_part);

        enum Source {
            View(String),
            Table(Table),
        }
        let source = {
            let state = self.lock_state();
            let Some(table_state) = lookup_table(&state.db, &name) else {
                return Err(SqlError::new(format!(
                    "table {}.{} does not exist",
                    name.schema, name.table
                )));
            };
            if table_state.is_view() {
                Source::View(table_state.view_sql.clone())
            } else {
                Source::Table(table_state.clone())
            }
        };
        match source {
            Source::View(view_sql) => {
                let result = self.execute_sql_nested(&view_sql)?;
                let mut view_table = table_from_query_result(&result);
                view_table.rows = result.rows;
                select_from_resolved_table(
                    &view_table,
                    &column_part,
                    &where_part,
                    &order_part,
                    &limit_part,
                )
            }
            Source::Table(table) => select_from_resolved_table(
                &table,
                &column_part,
                &where_part,
                &order_part,
                &limit_part,
            ),
        }
    }
}

/// Splits the tail of a SELECT statement (everything after `FROM <table>`)
/// into the four clause parts: (table_part, where_part, order_part, limit_part).
///
/// The splitting order is load-bearing — it mirrors the exact precedence used
/// by the legacy `selectFromTable` implementation:
///   1. Split on WHERE first (so ORDER BY / LIMIT inside WHERE are handled).
///   2. Strip ORDER BY and LIMIT from the WHERE clause if present.
///   3. Strip LIMIT from the ORDER BY clause if present.
fn split_select_clauses(rest: &str) -> (String, String, String, String) {
    let (table_part, where_part) = split_clause(rest, " where ");
    let (table_part, mut order_part) = split_clause(&table_part, " order by ");
    let (table_part, mut limit_part) = split_clause(&table_part, " limit ");
    let mut where_part = where_part;
    if !where_part.is_empty() {
        let (next_where, next_order) = split_clause(&where_part, " order by ");
        where_part = next_where;
        order_part = next_order;
        let (next_where, next_limit) = split_clause(&where_part, " limit ");
        where_part = next_where;
        limit_part = next_limit;
    }
    if !order_part.is_empty() {
        let (next_order, next_limit) = split_clause(&order_part, " limit ");
        order_part = next_order;
        limit_part = next_limit;
    }
    (table_part, where_part, order_part, limit_part)
}

/// Mirrors `selectLiterals` (`SELECT 1 AS id, 'x' label` with no FROM).
fn select_literals(statement: &str) -> Result<QueryResult, SqlError> {
    let column_part = statement["select".len()..].trim();
    if column_part.is_empty() {
        return Err(SqlError::new("SELECT requires at least one expression"));
    }
    let expressions = split_comma_separated(column_part);
    let mut fields = Vec::with_capacity(expressions.len());
    let mut row = Vec::with_capacity(expressions.len());
    for (i, expression) in expressions.iter().enumerate() {
        let (value, alias) = parse_select_literal(expression, i + 1)?;
        let (type_oid, type_size) = infer_literal_pg_type(&value);
        fields.push(PgField {
            name: alias,
            type_oid,
            type_size,
        });
        row.push(value);
    }
    Ok(QueryResult {
        fields,
        rows: vec![row],
        tag: "SELECT 1".to_string(),
    })
}

/// Mirrors `selectFromResolvedTable`: applies count/projection, WHERE,
/// ORDER BY, and LIMIT to a fully resolved table.
pub(crate) fn select_from_resolved_table(
    table: &Table,
    column_part: &str,
    where_part: &str,
    order_part: &str,
    limit_part: &str,
) -> Result<QueryResult, SqlError> {
    let where_predicate = parse_where_predicate(table, where_part)?;
    if let Some(count_alias) = parse_count_projection(table, column_part)? {
        let count = table
            .rows
            .iter()
            .filter(|stored| where_predicate.matches(stored))
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
    let (selected_indexes, fields) = selected_columns(table, column_part)?;
    let limit = parse_limit(limit_part)?;
    let order_index = parse_order_by(table, order_part)?;
    let mut rows: Vec<Vec<String>> = Vec::new();
    for stored in &table.rows {
        if !where_predicate.matches(stored) {
            continue;
        }
        rows.push(
            selected_indexes
                .iter()
                .map(|&index| stored[index].clone())
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

/// Mirrors `parseCountProjection`: detects a leading `count(...)` projection
/// and returns its alias; `Ok(None)` means "not a count projection".
pub(crate) fn parse_count_projection(
    table: &Table,
    column_part: &str,
) -> Result<Option<String>, SqlError> {
    let expression = column_part.trim();
    if expression.is_empty() {
        return Ok(None);
    }
    let fields: Vec<&str> = expression.split_whitespace().collect();
    if fields.is_empty() {
        return Ok(None);
    }
    let count_expr = fields[0];
    let lower = count_expr.to_lowercase();
    if !lower.starts_with("count(") || !lower.ends_with(')') {
        return Ok(None);
    }
    let argument = count_expr["count(".len()..count_expr.len() - 1].trim();
    let counts_all = argument == "*" || argument == "1";
    if !counts_all && column_index(table, &clean_column_identifier(argument)).is_none() {
        return Err(SqlError::new(format!("column {argument} does not exist")));
    }
    let mut alias = "count".to_string();
    if fields.len() > 1 {
        let rest = expression[count_expr.len()..].trim();
        let parsed_alias = parse_select_alias(rest);
        if parsed_alias.is_empty() {
            return Err(SqlError::new(format!(
                "unsupported SELECT count alias syntax: {rest}"
            )));
        }
        alias = parsed_alias;
    }
    Ok(Some(alias))
}
