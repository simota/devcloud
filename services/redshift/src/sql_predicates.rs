//! Predicate / expression evaluation for the memory engine.
//!
//! Parity: `internal/services/redshift/sql_predicates.rs` — single-comparison
//! WHERE predicates, INSERT row building (defaults + identity), UPDATE
//! assignments, projection resolution, ORDER BY / LIMIT.

use std::cmp::Ordering;

use crate::errors::SqlError;
use crate::model::Table;
use crate::pg_types::{pg_type_oid, pg_type_size, PgField};
use crate::sql_parse::{
    clean_column_identifier, clean_identifier, parse_literal, split_comma_separated,
};

#[derive(Debug, Clone)]
pub struct ColumnAssignment {
    pub index: usize,
    pub value: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompareOp {
    Eq,
    Ne,
    Gt,
    Ge,
    Lt,
    Le,
}

impl CompareOp {
    fn from_symbol(op: &str) -> Option<CompareOp> {
        match op {
            "=" => Some(CompareOp::Eq),
            "!=" | "<>" => Some(CompareOp::Ne),
            ">" => Some(CompareOp::Gt),
            ">=" => Some(CompareOp::Ge),
            "<" => Some(CompareOp::Lt),
            "<=" => Some(CompareOp::Le),
            _ => None,
        }
    }
}

/// Mirrors legacy `wherePredicate`; `index: None` is legacy `index: -1` match-all.
#[derive(Debug, Clone)]
pub struct WherePredicate {
    pub index: Option<usize>,
    pub op: CompareOp,
    pub value: String,
}

impl WherePredicate {
    pub fn match_all() -> WherePredicate {
        WherePredicate {
            index: None,
            op: CompareOp::Eq,
            value: String::new(),
        }
    }

    /// Mirrors `wherePredicate.matches`.
    pub fn matches(&self, row: &[String]) -> bool {
        let Some(index) = self.index else {
            return true;
        };
        if index >= row.len() {
            return false;
        }
        let left = &row[index];
        match self.op {
            CompareOp::Eq => left == &self.value,
            CompareOp::Ne => left != &self.value,
            CompareOp::Gt => compare_sql_values(left, &self.value) == Ordering::Greater,
            CompareOp::Ge => compare_sql_values(left, &self.value) != Ordering::Less,
            CompareOp::Lt => compare_sql_values(left, &self.value) == Ordering::Less,
            CompareOp::Le => compare_sql_values(left, &self.value) != Ordering::Greater,
        }
    }
}

/// Mirrors `buildInsertRow`: maps a VALUES tuple onto the table columns,
/// resolving DEFAULT keywords, column defaults, and identity values.
pub fn build_insert_row(
    table: &Table,
    insert_columns: &[String],
    values: &[String],
) -> Result<Vec<String>, SqlError> {
    if insert_columns.is_empty() {
        if values.len() != table.columns.len() {
            return Err(SqlError::new(format!(
                "INSERT has {} values for {} columns",
                values.len(),
                table.columns.len()
            )));
        }
        let mut row = Vec::with_capacity(values.len());
        for (index, value) in values.iter().enumerate() {
            row.push(resolve_insert_value(table, index, value));
        }
        return Ok(row);
    }
    if values.len() != insert_columns.len() {
        return Err(SqlError::new(format!(
            "INSERT has {} values for {} target columns",
            values.len(),
            insert_columns.len()
        )));
    }
    let mut row = vec![String::new(); table.columns.len()];
    let mut assigned = vec![false; table.columns.len()];
    for (value_index, column_name) in insert_columns.iter().enumerate() {
        let column_index = column_index(table, column_name)
            .ok_or_else(|| SqlError::new(format!("column {column_name} does not exist")))?;
        if assigned[column_index] {
            return Err(SqlError::new(format!(
                "column {column_name} specified more than once"
            )));
        }
        row[column_index] = resolve_insert_value(table, column_index, &values[value_index]);
        assigned[column_index] = true;
    }
    for index in 0..table.columns.len() {
        if assigned[index] {
            continue;
        }
        row[index] = default_insert_value(table, index);
    }
    Ok(row)
}

/// Mirrors `resolveInsertValue` (legacy error return is always nil).
fn resolve_insert_value(table: &Table, column_index: usize, value: &str) -> String {
    if value.trim().eq_ignore_ascii_case("default") {
        return default_insert_value(table, column_index);
    }
    value.to_string()
}

/// Mirrors `defaultInsertValue`.
fn default_insert_value(table: &Table, column_index: usize) -> String {
    let column = &table.columns[column_index];
    if !column.default_value.is_empty() {
        return column.default_value.trim_matches('\'').to_string();
    }
    if column.identity {
        return next_identity_value(table, column_index).to_string();
    }
    String::new()
}

/// Mirrors `nextIdentityValue`: max(existing numeric values) + 1, min 1.
fn next_identity_value(table: &Table, column_index: usize) -> i64 {
    let mut next: i64 = 1;
    for row in &table.rows {
        if column_index >= row.len() {
            continue;
        }
        if let Ok(value) = row[column_index].parse::<i64>() {
            if value >= next {
                next = value + 1;
            }
        }
    }
    next
}

/// Mirrors `parseAssignments` (UPDATE ... SET column = literal, ...).
pub fn parse_assignments(
    table: &Table,
    assignments_part: &str,
) -> Result<Vec<ColumnAssignment>, SqlError> {
    let parts = split_comma_separated(assignments_part);
    if parts.is_empty() {
        return Err(SqlError::new("UPDATE requires at least one assignment"));
    }
    let mut assignments = Vec::with_capacity(parts.len());
    for part in &parts {
        let Some((name_part, value_part)) = part.split_once('=') else {
            return Err(SqlError::new(
                "UPDATE assignments must use column = literal",
            ));
        };
        let name = clean_identifier(name_part);
        let index = column_index(table, &name)
            .ok_or_else(|| SqlError::new(format!("column {name} does not exist")))?;
        let value = parse_literal(value_part)?;
        assignments.push(ColumnAssignment { index, value });
    }
    Ok(assignments)
}

/// Mirrors `selectedColumns`: resolves a projection list (or `*`) to source
/// column indexes and result field descriptors.
pub fn selected_columns(
    table: &Table,
    column_part: &str,
) -> Result<(Vec<usize>, Vec<PgField>), SqlError> {
    if column_part.trim() == "*" {
        let mut indexes = Vec::with_capacity(table.columns.len());
        let mut fields = Vec::with_capacity(table.columns.len());
        for (i, column) in table.columns.iter().enumerate() {
            indexes.push(i);
            fields.push(PgField {
                name: column.name.clone(),
                type_oid: pg_type_oid(&column.data_type),
                type_size: pg_type_size(&column.data_type),
            });
        }
        return Ok((indexes, fields));
    }
    let names = split_comma_separated(column_part);
    let mut indexes = Vec::with_capacity(names.len());
    let mut fields = Vec::with_capacity(names.len());
    for name in &names {
        let cleaned = clean_column_identifier(name);
        let index = column_index(table, &cleaned)
            .ok_or_else(|| SqlError::new(format!("column {cleaned} does not exist")))?;
        let column = &table.columns[index];
        indexes.push(index);
        fields.push(PgField {
            name: column.name.clone(),
            type_oid: pg_type_oid(&column.data_type),
            type_size: pg_type_size(&column.data_type),
        });
    }
    Ok((indexes, fields))
}

/// Mirrors `parseWherePredicate`.
pub fn parse_where_predicate(table: &Table, where_part: &str) -> Result<WherePredicate, SqlError> {
    if where_part.is_empty() {
        return Ok(WherePredicate::match_all());
    }
    let (left, op, right) = split_where_comparison(where_part)
        .ok_or_else(|| SqlError::new("only simple WHERE comparison is supported"))?;
    let name = clean_column_identifier(&left);
    let index = column_index(table, &name)
        .ok_or_else(|| SqlError::new(format!("column {name} does not exist")))?;
    let value = parse_literal(&right)?;
    Ok(WherePredicate {
        index: Some(index),
        op,
        value,
    })
}

/// Mirrors `splitWhereComparison`: finds the first comparison operator outside
/// string literals and stops there (an empty side at the first operator is a
/// failure, not a reason to keep scanning).
pub fn split_where_comparison(where_part: &str) -> Option<(String, CompareOp, String)> {
    const OPERATORS: [&str; 7] = [">=", "<=", "!=", "<>", "=", ">", "<"];
    let bytes = where_part.as_bytes();
    let mut in_string = false;
    let mut i = 0usize;
    while i < bytes.len() {
        let ch = bytes[i];
        if ch == b'\'' {
            if in_string && i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                i += 2;
                continue;
            }
            in_string = !in_string;
            i += 1;
            continue;
        }
        if in_string {
            i += 1;
            continue;
        }
        for op in OPERATORS {
            if bytes[i..].starts_with(op.as_bytes()) {
                let left = where_part[..i].trim();
                let right = where_part[i + op.len()..].trim();
                if left.is_empty() || right.is_empty() {
                    return None;
                }
                return Some((
                    left.to_string(),
                    CompareOp::from_symbol(op).expect("operator table is exhaustive"),
                    right.to_string(),
                ));
            }
        }
        i += 1;
    }
    None
}

/// Mirrors `compareSQLValues`: numeric comparison when both sides parse as
/// int64, byte-wise string comparison otherwise.
pub fn compare_sql_values(left: &str, right: &str) -> Ordering {
    if let (Ok(left_int), Ok(right_int)) = (left.parse::<i64>(), right.parse::<i64>()) {
        return left_int.cmp(&right_int);
    }
    left.cmp(right)
}

/// Mirrors `parseOrderBy`: only `ORDER BY <column> [ASC]` is supported.
pub fn parse_order_by(table: &Table, order_part: &str) -> Result<Option<usize>, SqlError> {
    if order_part.is_empty() {
        return Ok(None);
    }
    let fields: Vec<&str> = order_part.split_whitespace().collect();
    if fields.is_empty() {
        return Ok(None);
    }
    let index = column_index(table, &clean_column_identifier(fields[0]))
        .ok_or_else(|| SqlError::new(format!("column {} does not exist", fields[0])))?;
    if fields.len() > 1 && !fields[1].eq_ignore_ascii_case("asc") {
        return Err(SqlError::new("only ORDER BY column ASC is supported"));
    }
    Ok(Some(index))
}

/// Mirrors `sortRowsBySourceColumn`: stable sort of the projected rows by the
/// projected position of the ORDER BY source column (no-op when the ORDER BY
/// column was not selected).
pub fn sort_rows_by_source_column(
    rows: &mut [Vec<String>],
    selected_indexes: &[usize],
    order_index: usize,
) {
    let Some(selected_index) = selected_indexes
        .iter()
        .position(|&source_index| source_index == order_index)
    else {
        return;
    };
    rows.sort_by(|a, b| {
        let left = &a[selected_index];
        let right = &b[selected_index];
        if let (Ok(left_int), Ok(right_int)) = (left.parse::<i64>(), right.parse::<i64>()) {
            return left_int.cmp(&right_int);
        }
        left.cmp(right)
    });
}

/// Mirrors `parseLimit`: empty → no limit; otherwise a non-negative integer.
pub fn parse_limit(value: &str) -> Result<Option<usize>, SqlError> {
    if value.is_empty() {
        return Ok(None);
    }
    match value.trim().parse::<i64>() {
        Ok(limit) if limit >= 0 => Ok(Some(limit as usize)),
        _ => Err(SqlError::new("LIMIT must be a non-negative integer")),
    }
}

/// Mirrors `columnIndex` (case-insensitive column lookup).
pub fn column_index(table: &Table, name: &str) -> Option<usize> {
    table
        .columns
        .iter()
        .position(|column| column.name.eq_ignore_ascii_case(name))
}
