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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{Column, QualifiedName, Table};

    fn make_table(col_names: &[&str]) -> Table {
        Table {
            name: QualifiedName {
                schema: "public".to_string(),
                table: "t".to_string(),
            },
            columns: col_names
                .iter()
                .map(|n| Column {
                    name: n.to_string(),
                    data_type: "text".to_string(),
                    ..Column::default()
                })
                .collect(),
            ..Table::default()
        }
    }

    // ---- split_where_comparison ----

    #[test]
    fn split_where_comparison_eq() {
        let (left, op, right) = split_where_comparison("id = 1").unwrap();
        assert_eq!(left, "id");
        assert_eq!(op, CompareOp::Eq);
        assert_eq!(right, "1");
    }

    #[test]
    fn split_where_comparison_ne_bang() {
        let (_, op, _) = split_where_comparison("id != 1").unwrap();
        assert_eq!(op, CompareOp::Ne);
    }

    #[test]
    fn split_where_comparison_ne_ansi() {
        let (_, op, _) = split_where_comparison("id <> 1").unwrap();
        assert_eq!(op, CompareOp::Ne);
    }

    #[test]
    fn split_where_comparison_ge() {
        let (_, op, _) = split_where_comparison("age >= 18").unwrap();
        assert_eq!(op, CompareOp::Ge);
    }

    #[test]
    fn split_where_comparison_le() {
        let (_, op, _) = split_where_comparison("age <= 100").unwrap();
        assert_eq!(op, CompareOp::Le);
    }

    #[test]
    fn split_where_comparison_gt() {
        let (_, op, _) = split_where_comparison("age > 18").unwrap();
        assert_eq!(op, CompareOp::Gt);
    }

    #[test]
    fn split_where_comparison_lt() {
        let (_, op, _) = split_where_comparison("age < 18").unwrap();
        assert_eq!(op, CompareOp::Lt);
    }

    #[test]
    fn split_where_comparison_no_operator_returns_none() {
        assert!(split_where_comparison("just_a_column").is_none());
    }

    #[test]
    fn split_where_comparison_empty_left_returns_none() {
        // operator at the start — no left side
        assert!(split_where_comparison("= 1").is_none());
    }

    #[test]
    fn split_where_comparison_empty_right_returns_none() {
        assert!(split_where_comparison("id =").is_none());
    }

    #[test]
    fn split_where_comparison_string_value() {
        let (left, op, right) = split_where_comparison("name = 'alice'").unwrap();
        assert_eq!(left, "name");
        assert_eq!(op, CompareOp::Eq);
        assert_eq!(right, "'alice'");
    }

    // ---- compare_sql_values ----

    #[test]
    fn compare_sql_values_numeric_ordering() {
        use std::cmp::Ordering;
        // Numeric: 9 < 10 (would be reversed in lexical order: "9" > "10")
        assert_eq!(compare_sql_values("9", "10"), Ordering::Less);
        assert_eq!(compare_sql_values("10", "9"), Ordering::Greater);
        assert_eq!(compare_sql_values("5", "5"), Ordering::Equal);
    }

    #[test]
    fn compare_sql_values_negative_numbers() {
        use std::cmp::Ordering;
        assert_eq!(compare_sql_values("-1", "1"), Ordering::Less);
        assert_eq!(compare_sql_values("0", "-0"), Ordering::Equal);
    }

    #[test]
    fn compare_sql_values_string_fallback() {
        use std::cmp::Ordering;
        // Non-numeric: falls back to lexical byte-wise comparison
        assert_eq!(compare_sql_values("abc", "abd"), Ordering::Less);
        assert_eq!(compare_sql_values("z", "a"), Ordering::Greater);
    }

    #[test]
    fn compare_sql_values_mixed_not_both_numeric_falls_back_to_string() {
        use std::cmp::Ordering;
        // one is numeric, other is not: both parse as i64 required for numeric path
        assert_eq!(compare_sql_values("42", "abc"), Ordering::Less); // "42" < "abc" lexically
    }

    // ---- WherePredicate::matches ----

    #[test]
    fn where_predicate_match_all_always_true() {
        let pred = WherePredicate::match_all();
        assert!(pred.matches(&["a".to_string(), "b".to_string()]));
        assert!(pred.matches(&[]));
    }

    #[test]
    fn where_predicate_eq_match() {
        let pred = WherePredicate {
            index: Some(0),
            op: CompareOp::Eq,
            value: "alice".to_string(),
        };
        assert!(pred.matches(&["alice".to_string()]));
        assert!(!pred.matches(&["bob".to_string()]));
    }

    #[test]
    fn where_predicate_ne_match() {
        let pred = WherePredicate {
            index: Some(0),
            op: CompareOp::Ne,
            value: "alice".to_string(),
        };
        assert!(pred.matches(&["bob".to_string()]));
        assert!(!pred.matches(&["alice".to_string()]));
    }

    #[test]
    fn where_predicate_gt_numeric() {
        let pred = WherePredicate {
            index: Some(0),
            op: CompareOp::Gt,
            value: "5".to_string(),
        };
        assert!(pred.matches(&["10".to_string()]));
        assert!(!pred.matches(&["5".to_string()]));
        assert!(!pred.matches(&["3".to_string()]));
    }

    #[test]
    fn where_predicate_ge_numeric() {
        let pred = WherePredicate {
            index: Some(0),
            op: CompareOp::Ge,
            value: "5".to_string(),
        };
        assert!(pred.matches(&["5".to_string()]));
        assert!(pred.matches(&["6".to_string()]));
        assert!(!pred.matches(&["4".to_string()]));
    }

    #[test]
    fn where_predicate_lt_le() {
        let lt = WherePredicate {
            index: Some(0),
            op: CompareOp::Lt,
            value: "5".to_string(),
        };
        assert!(lt.matches(&["3".to_string()]));
        assert!(!lt.matches(&["5".to_string()]));

        let le = WherePredicate {
            index: Some(0),
            op: CompareOp::Le,
            value: "5".to_string(),
        };
        assert!(le.matches(&["5".to_string()]));
        assert!(!le.matches(&["6".to_string()]));
    }

    #[test]
    fn where_predicate_index_out_of_bounds_returns_false() {
        let pred = WherePredicate {
            index: Some(99),
            op: CompareOp::Eq,
            value: "x".to_string(),
        };
        assert!(!pred.matches(&["a".to_string()]));
    }

    // ---- parse_where_predicate ----

    #[test]
    fn parse_where_predicate_empty_is_match_all() {
        let table = make_table(&["id"]);
        let pred = parse_where_predicate(&table, "").unwrap();
        assert!(pred.index.is_none());
    }

    #[test]
    fn parse_where_predicate_simple_eq() {
        let table = make_table(&["id", "name"]);
        let pred = parse_where_predicate(&table, "name = 'alice'").unwrap();
        assert_eq!(pred.index, Some(1));
        assert_eq!(pred.op, CompareOp::Eq);
        assert_eq!(pred.value, "alice");
    }

    #[test]
    fn parse_where_predicate_unknown_column_is_err() {
        let table = make_table(&["id"]);
        assert!(parse_where_predicate(&table, "unknown = 1").is_err());
    }

    #[test]
    fn parse_where_predicate_no_operator_is_err() {
        let table = make_table(&["id"]);
        assert!(parse_where_predicate(&table, "id").is_err());
    }

    // ---- parse_order_by / sort_rows_by_source_column ----

    #[test]
    fn parse_order_by_empty_returns_none() {
        let table = make_table(&["id"]);
        assert_eq!(parse_order_by(&table, "").unwrap(), None);
    }

    #[test]
    fn parse_order_by_asc() {
        let table = make_table(&["id", "name"]);
        assert_eq!(parse_order_by(&table, "id ASC").unwrap(), Some(0));
    }

    #[test]
    fn parse_order_by_bare_column() {
        let table = make_table(&["id", "name"]);
        assert_eq!(parse_order_by(&table, "name").unwrap(), Some(1));
    }

    #[test]
    fn parse_order_by_desc_is_err() {
        // Only ASC is supported
        let table = make_table(&["id"]);
        assert!(parse_order_by(&table, "id DESC").is_err());
    }

    #[test]
    fn parse_order_by_unknown_column_is_err() {
        let table = make_table(&["id"]);
        assert!(parse_order_by(&table, "unknown").is_err());
    }

    #[test]
    fn sort_rows_by_source_column_numeric() {
        let mut rows = vec![
            vec!["10".to_string()],
            vec!["2".to_string()],
            vec!["5".to_string()],
        ];
        sort_rows_by_source_column(&mut rows, &[0], 0);
        assert_eq!(rows[0][0], "2");
        assert_eq!(rows[1][0], "5");
        assert_eq!(rows[2][0], "10");
    }

    #[test]
    fn sort_rows_by_source_column_order_index_not_selected_is_noop() {
        // order_index=1 not in selected_indexes=[0] — should be a no-op
        let mut rows = vec![vec!["z".to_string()], vec!["a".to_string()]];
        sort_rows_by_source_column(&mut rows, &[0], 1);
        assert_eq!(rows[0][0], "z"); // unchanged
    }

    // ---- parse_assignments ----

    #[test]
    fn parse_assignments_single() {
        let table = make_table(&["id", "name"]);
        let assignments = parse_assignments(&table, "name = 'alice'").unwrap();
        assert_eq!(assignments.len(), 1);
        assert_eq!(assignments[0].index, 1);
        assert_eq!(assignments[0].value, "alice");
    }

    #[test]
    fn parse_assignments_multiple() {
        let table = make_table(&["id", "name", "age"]);
        let assignments = parse_assignments(&table, "name = 'bob', age = '30'").unwrap();
        assert_eq!(assignments.len(), 2);
    }

    #[test]
    fn parse_assignments_unknown_column_is_err() {
        let table = make_table(&["id"]);
        assert!(parse_assignments(&table, "missing = 1").is_err());
    }

    #[test]
    fn parse_assignments_empty_is_err() {
        let table = make_table(&["id"]);
        assert!(parse_assignments(&table, "").is_err());
    }

    #[test]
    fn parse_assignments_no_equals_is_err() {
        let table = make_table(&["id"]);
        assert!(parse_assignments(&table, "id").is_err());
    }

    // ---- selected_columns ----

    #[test]
    fn selected_columns_star_returns_all() {
        let table = make_table(&["id", "name", "age"]);
        let (indexes, fields) = selected_columns(&table, "*").unwrap();
        assert_eq!(indexes, vec![0, 1, 2]);
        assert_eq!(fields.len(), 3);
    }

    #[test]
    fn selected_columns_subset() {
        let table = make_table(&["id", "name", "age"]);
        let (indexes, fields) = selected_columns(&table, "name, id").unwrap();
        assert_eq!(indexes, vec![1, 0]);
        assert_eq!(fields[0].name, "name");
        assert_eq!(fields[1].name, "id");
    }

    #[test]
    fn selected_columns_unknown_is_err() {
        let table = make_table(&["id"]);
        assert!(selected_columns(&table, "missing").is_err());
    }

    // ---- build_insert_row ----

    #[test]
    fn build_insert_row_positional_values() {
        let table = make_table(&["id", "name"]);
        let row = build_insert_row(&table, &[], &["1".to_string(), "alice".to_string()]).unwrap();
        assert_eq!(row, vec!["1", "alice"]);
    }

    #[test]
    fn build_insert_row_positional_count_mismatch_is_err() {
        let table = make_table(&["id", "name"]);
        assert!(build_insert_row(&table, &[], &["1".to_string()]).is_err());
    }

    #[test]
    fn build_insert_row_named_columns() {
        let table = make_table(&["id", "name"]);
        let cols = vec!["name".to_string(), "id".to_string()];
        let vals = vec!["alice".to_string(), "42".to_string()];
        let row = build_insert_row(&table, &cols, &vals).unwrap();
        assert_eq!(row[0], "42"); // id at index 0
        assert_eq!(row[1], "alice"); // name at index 1
    }

    #[test]
    fn build_insert_row_named_count_mismatch_is_err() {
        let table = make_table(&["id", "name"]);
        let cols = vec!["id".to_string()];
        let vals = vec!["1".to_string(), "extra".to_string()];
        assert!(build_insert_row(&table, &cols, &vals).is_err());
    }

    #[test]
    fn build_insert_row_unknown_column_is_err() {
        let table = make_table(&["id"]);
        let cols = vec!["missing".to_string()];
        let vals = vec!["1".to_string()];
        assert!(build_insert_row(&table, &cols, &vals).is_err());
    }

    #[test]
    fn build_insert_row_default_keyword_uses_column_default() {
        let mut table = make_table(&["id", "status"]);
        table.columns[1].default_value = "'active'".to_string();
        let row = build_insert_row(&table, &[], &["1".to_string(), "DEFAULT".to_string()]).unwrap();
        assert_eq!(row[1], "active");
    }

    #[test]
    fn build_insert_row_omitted_column_gets_empty_default() {
        let table = make_table(&["id", "name"]);
        let cols = vec!["id".to_string()];
        let vals = vec!["1".to_string()];
        let row = build_insert_row(&table, &cols, &vals).unwrap();
        assert_eq!(row[0], "1");
        assert_eq!(row[1], ""); // omitted, no default
    }

    #[test]
    fn build_insert_row_identity_column_auto_increments() {
        let mut table = make_table(&["id", "name"]);
        table.columns[0].identity = true;
        // existing row with id=5
        table.rows = vec![vec!["5".to_string(), "prev".to_string()]];
        let cols = vec!["name".to_string()];
        let vals = vec!["new".to_string()];
        let row = build_insert_row(&table, &cols, &vals).unwrap();
        assert_eq!(row[0], "6"); // max(5) + 1
    }

    #[test]
    fn build_insert_row_duplicate_column_is_err() {
        let table = make_table(&["id", "name"]);
        let cols = vec!["id".to_string(), "id".to_string()];
        let vals = vec!["1".to_string(), "2".to_string()];
        assert!(build_insert_row(&table, &cols, &vals).is_err());
    }
}
