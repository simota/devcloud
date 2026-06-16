//! Expression/predicate evaluation and aggregates — port of
//! `internal/services/bigquery/sql_eval.rs`, plus the raw-JSON cell helpers
//! from `tabledata_handlers.rs` (`isJSONNull`, `formatRowValues`,
//! `rawValueForResponse`, `rawValueForFieldResponse`) that the engine shares
//! with the tabledata handlers (part 3).
//!
//! legacy decodes raw JSON with `UseNumber`, so cell values carry `json.Number`
//! and re-encode as the original literal. `serde_json::Value` (without
//! `arbitrary_precision`) normalizes the number text instead (`1.50` → `1.5`,
//! `1e2` → `100.0`); integers and canonically-formatted floats — everything
//! the legacy test suite exercises — round-trip identically.

use std::cmp::Ordering;
use std::collections::BTreeMap;

use serde_json::Value;

use crate::model::{RawJson, StoredRow, TableCell, TableDataRow, TableFieldSchema, TableSchema};
use crate::responses::default_string;
use crate::sql_parser::{
    aggregate_field, fields_for_query, grouped_aggregate_dry_run_fields, AggregateFunction,
    AggregateSelection, ComparisonOp, SimpleSelectQuery, WhereCondition,
};
use crate::wire_json;

/// legacy `queryExecutionResult`.
#[derive(Debug, Clone, Default)]
pub struct QueryExecutionResult {
    pub fields: Vec<TableFieldSchema>,
    pub rows: Vec<TableDataRow>,
}

/// legacy `executeParsedQuery`.
pub fn execute_parsed_query(
    schema: &TableSchema,
    rows: &[StoredRow],
    query: &SimpleSelectQuery,
) -> Result<QueryExecutionResult, String> {
    let selected_fields = fields_for_query(schema, &query.selected_fields)?;
    let mut filtered: Vec<&StoredRow> = rows
        .iter()
        .filter(|row| row_matches_query(row, query))
        .collect();
    if let Some(aggregate) = &query.aggregate {
        if !query.group_by.is_empty() {
            return execute_grouped_aggregate_query(&filtered, schema, query, aggregate);
        }
        return execute_aggregate_query(&filtered, schema, aggregate);
    }
    if !query.order_by.is_empty() {
        filtered.sort_by(|a, b| {
            let cmp =
                compare_raw_values(raw_field(a, &query.order_by), raw_field(b, &query.order_by));
            if query.order_desc {
                cmp.reverse()
            } else {
                cmp
            }
        });
    }
    if query.offset > 0 {
        if query.offset >= filtered.len() as i64 {
            filtered.clear();
        } else {
            filtered.drain(..query.offset as usize);
        }
    }
    if query.limit >= 0 && query.limit < filtered.len() as i64 {
        filtered.truncate(query.limit as usize);
    }
    let response_rows = filtered
        .iter()
        .map(|row| TableDataRow {
            f: format_row_values(&row.json, &selected_fields),
        })
        .collect();
    Ok(QueryExecutionResult {
        fields: selected_fields,
        rows: response_rows,
    })
}

/// Missing key ≙ legacy nil `json.RawMessage` (empty bytes): every decode fails,
/// numeric comparison falls back to the empty string.
fn raw_field<'a>(row: &'a StoredRow, name: &str) -> &'a str {
    row.json.get(name).map(|raw| raw.get()).unwrap_or("")
}

/// legacy `queryResultRowsToStoredRows`.
pub fn query_result_rows_to_stored_rows(result: &QueryExecutionResult) -> Vec<StoredRow> {
    result
        .rows
        .iter()
        .map(|row| {
            let mut values: BTreeMap<String, RawJson> = BTreeMap::new();
            for (i, field) in result.fields.iter().enumerate() {
                let value = row
                    .f
                    .get(i)
                    .map(|cell| cell.v.clone())
                    .unwrap_or(Value::Null);
                let raw =
                    String::from_utf8(wire_json::marshal(&value)).expect("marshaled json is utf-8");
                let raw = serde_json::value::RawValue::from_string(raw)
                    .expect("marshaled json is a valid raw value");
                values.insert(field.name.clone(), raw);
            }
            StoredRow {
                insert_id: String::new(),
                json: values,
                inserted_at: String::new(),
            }
        })
        .collect()
}

/// legacy `executeAggregateQuery`.
pub(crate) fn execute_aggregate_query(
    rows: &[&StoredRow],
    schema: &TableSchema,
    aggregate: &AggregateSelection,
) -> Result<QueryExecutionResult, String> {
    let field_name = if aggregate.alias.is_empty() {
        "f0_"
    } else {
        &aggregate.alias
    };

    let field = aggregate_field(schema, aggregate);

    if aggregate.field == "*" {
        if aggregate.function != AggregateFunction::Count {
            return Err(format!("{} requires a field", aggregate.function.name()));
        }
        return Ok(single_aggregate_result(
            field_name,
            "INTEGER",
            &rows.len().to_string(),
        ));
    }
    let Some(field) = field else {
        return Err(format!(
            "aggregate field {:?} does not exist",
            aggregate.field
        ));
    };

    match aggregate.function {
        AggregateFunction::Count => {
            let count = rows
                .iter()
                .filter(|row| {
                    row.json
                        .get(&aggregate.field)
                        .map(|raw| !is_json_null(raw.get()))
                        .unwrap_or(false)
                })
                .count();
            Ok(single_aggregate_result(
                field_name,
                "INTEGER",
                &count.to_string(),
            ))
        }
        AggregateFunction::Sum => {
            if !is_numeric_field(field) {
                return Err("SUM requires a numeric field".to_string());
            }
            let (sum, count) = sum_aggregate(rows, &aggregate.field, is_integer_field(field))?;
            if count == 0 {
                return Ok(single_aggregate_null_result(
                    field_name,
                    &aggregate_numeric_type(field),
                ));
            }
            Ok(single_aggregate_result(
                field_name,
                &aggregate_numeric_type(field),
                &sum,
            ))
        }
        AggregateFunction::Avg => {
            if !is_numeric_field(field) {
                return Err("AVG requires a numeric field".to_string());
            }
            let (sum, count) = float_aggregate(rows, &aggregate.field)?;
            if count == 0 {
                return Ok(single_aggregate_null_result(field_name, "FLOAT"));
            }
            Ok(single_aggregate_result(
                field_name,
                "FLOAT",
                &format_float_legacy(sum / count as f64),
            ))
        }
        AggregateFunction::Min | AggregateFunction::Max => {
            let field_type = default_string(field.field_type.clone(), "STRING");
            let Some(raw) = min_max_aggregate(rows, &aggregate.field, aggregate.function) else {
                return Ok(single_aggregate_null_result(field_name, &field_type));
            };
            Ok(QueryExecutionResult {
                fields: vec![TableFieldSchema {
                    name: field_name.to_string(),
                    field_type,
                    mode: "NULLABLE".to_string(),
                    ..Default::default()
                }],
                rows: vec![TableDataRow {
                    f: vec![TableCell {
                        v: raw_value_for_response(raw),
                    }],
                }],
            })
        }
    }
}

/// legacy `executeGroupedAggregateQuery`.
fn execute_grouped_aggregate_query(
    rows: &[&StoredRow],
    schema: &TableSchema,
    query: &SimpleSelectQuery,
    aggregate: &AggregateSelection,
) -> Result<QueryExecutionResult, String> {
    let fields = grouped_aggregate_dry_run_fields(schema, &query.group_by, aggregate)?;
    // Groups are keyed by the raw JSON text of the grouped value (missing →
    // "null"), like legacy `string(raw)` map key. legacy sorts the keys with a
    // non-stable sort over random map iteration order; keys are unique, so the
    // only ties `compareRawValues` can report are numerically-equal distinct
    // literals, where legacy order is unspecified — first-seen stable order here.
    let mut keys: Vec<String> = Vec::new();
    let mut groups: BTreeMap<String, Vec<&StoredRow>> = BTreeMap::new();
    for row in rows {
        let raw = row
            .json
            .get(&query.group_by)
            .map(|raw| raw.get())
            .unwrap_or("null");
        if !groups.contains_key(raw) {
            keys.push(raw.to_string());
        }
        groups.entry(raw.to_string()).or_default().push(row);
    }
    keys.sort_by(|a, b| {
        let cmp = compare_raw_values(a, b);
        if query.order_desc {
            cmp.reverse()
        } else {
            cmp
        }
    });
    if !query.order_by.is_empty() && query.order_by != query.group_by {
        return Err("ORDER BY supports grouped field only for GROUP BY queries".to_string());
    }
    if query.offset > 0 {
        if query.offset >= keys.len() as i64 {
            keys.clear();
        } else {
            keys.drain(..query.offset as usize);
        }
    }
    if query.limit >= 0 && query.limit < keys.len() as i64 {
        keys.truncate(query.limit as usize);
    }

    let mut response_rows = Vec::with_capacity(keys.len());
    for key in &keys {
        let aggregate_result = execute_aggregate_query(&groups[key], schema, aggregate)?;
        let cell = TableCell {
            v: if is_json_null(key) {
                Value::Null
            } else {
                raw_value_for_field_response(key, &fields[0])
            },
        };
        let Some(aggregate_cell) = aggregate_result
            .rows
            .first()
            .and_then(|row| row.f.first())
            .cloned()
        else {
            return Err("aggregate result is empty".to_string());
        };
        response_rows.push(TableDataRow {
            f: vec![cell, aggregate_cell],
        });
    }
    Ok(QueryExecutionResult {
        fields,
        rows: response_rows,
    })
}

/// legacy `singleAggregateResult` (the value is always rendered as a JSON string
/// cell; legacy marshals then re-decodes it, which is the identity).
fn single_aggregate_result(name: &str, field_type: &str, value: &str) -> QueryExecutionResult {
    QueryExecutionResult {
        fields: vec![TableFieldSchema {
            name: name.to_string(),
            field_type: field_type.to_string(),
            mode: "NULLABLE".to_string(),
            ..Default::default()
        }],
        rows: vec![TableDataRow {
            f: vec![TableCell {
                v: Value::String(value.to_string()),
            }],
        }],
    }
}

/// legacy `singleAggregateNullResult`.
fn single_aggregate_null_result(name: &str, field_type: &str) -> QueryExecutionResult {
    QueryExecutionResult {
        fields: vec![TableFieldSchema {
            name: name.to_string(),
            field_type: field_type.to_string(),
            mode: "NULLABLE".to_string(),
            ..Default::default()
        }],
        rows: vec![TableDataRow {
            f: vec![TableCell { v: Value::Null }],
        }],
    }
}

/// legacy `sumAggregate`. Integer sums wrap on overflow like legacy `int64 +=`.
fn sum_aggregate(
    rows: &[&StoredRow],
    field_name: &str,
    integer: bool,
) -> Result<(String, usize), String> {
    if integer {
        let mut sum: i64 = 0;
        let mut count = 0;
        for row in rows {
            let Some(raw) = row.json.get(field_name) else {
                continue;
            };
            if is_json_null(raw.get()) {
                continue;
            }
            let Some(value) = raw_int(raw.get()) else {
                return Err("SUM field contains a non-integer value".to_string());
            };
            sum = sum.wrapping_add(value);
            count += 1;
        }
        return Ok((sum.to_string(), count));
    }
    let (sum, count) = float_aggregate(rows, field_name)?;
    Ok((format_float_legacy(sum), count))
}

/// legacy `floatAggregate`.
fn float_aggregate(rows: &[&StoredRow], field_name: &str) -> Result<(f64, usize), String> {
    let mut sum = 0.0;
    let mut count = 0;
    for row in rows {
        let Some(raw) = row.json.get(field_name) else {
            continue;
        };
        if is_json_null(raw.get()) {
            continue;
        }
        let Some(value) = raw_float(raw.get()) else {
            return Err("aggregate field contains a non-numeric value".to_string());
        };
        sum += value;
        count += 1;
    }
    Ok((sum, count))
}

/// legacy `minMaxAggregate`.
fn min_max_aggregate<'a>(
    rows: &[&'a StoredRow],
    field_name: &str,
    function: AggregateFunction,
) -> Option<&'a str> {
    let mut selected: Option<&str> = None;
    for row in rows {
        let Some(raw) = row.json.get(field_name) else {
            continue;
        };
        let raw = raw.get();
        if is_json_null(raw) {
            continue;
        }
        let Some(current) = selected else {
            selected = Some(raw);
            continue;
        };
        let cmp = compare_raw_values(raw, current);
        if function == AggregateFunction::Min && cmp == Ordering::Less
            || function == AggregateFunction::Max && cmp == Ordering::Greater
        {
            selected = Some(raw);
        }
    }
    selected
}

/// legacy `isNumericField`.
pub(crate) fn is_numeric_field(field: &TableFieldSchema) -> bool {
    matches!(
        default_string(field.field_type.clone(), "STRING")
            .to_uppercase()
            .as_str(),
        "INTEGER" | "INT64" | "FLOAT" | "FLOAT64" | "NUMERIC" | "BIGNUMERIC"
    )
}

/// legacy `isIntegerField`.
pub(crate) fn is_integer_field(field: &TableFieldSchema) -> bool {
    matches!(
        default_string(field.field_type.clone(), "STRING")
            .to_uppercase()
            .as_str(),
        "INTEGER" | "INT64"
    )
}

/// legacy `aggregateNumericType`.
pub(crate) fn aggregate_numeric_type(field: &TableFieldSchema) -> String {
    if is_integer_field(field) {
        return "INTEGER".to_string();
    }
    let field_type = default_string(field.field_type.clone(), "FLOAT").to_uppercase();
    if field_type == "FLOAT64" {
        "FLOAT".to_string()
    } else {
        field_type
    }
}

/// legacy `rowMatchesQuery`. The legacy fallbacks that rebuild the groups from the
/// flattened `WhereConditions` / legacy single-condition fields are
/// unreachable from the parser (it always fills the groups), so only the
/// groups are consulted.
fn row_matches_query(row: &StoredRow, query: &SimpleSelectQuery) -> bool {
    if query.where_condition_groups.is_empty() {
        return true;
    }
    query
        .where_condition_groups
        .iter()
        .any(|group| row_matches_all_conditions(row, group))
}

/// legacy `rowMatchesAllConditions`.
fn row_matches_all_conditions(row: &StoredRow, conditions: &[WhereCondition]) -> bool {
    conditions.iter().all(|condition| {
        let Some(raw) = row.json.get(&condition.field) else {
            return false;
        };
        let raw = raw.get();
        if is_json_null(raw) {
            return false;
        }
        let cmp = compare_raw_values(raw, &condition.value_raw);
        let matches = match condition.op {
            ComparisonOp::Eq => cmp == Ordering::Equal,
            ComparisonOp::NotEq => cmp != Ordering::Equal,
            ComparisonOp::Gt => cmp == Ordering::Greater,
            ComparisonOp::Ge => cmp != Ordering::Less,
            ComparisonOp::Lt => cmp == Ordering::Less,
            ComparisonOp::Le => cmp != Ordering::Greater,
        };
        // legacy spells every `NOT <op>` case out; each is the exact negation.
        matches != condition.negated
    })
}

/// legacy `compareRawValues`: numeric comparison when both sides parse as floats
/// (NaN compares equal, like legacy `<`/`>` chain), otherwise byte-wise string
/// comparison of the `fmt.Sprint`-rendered decoded values.
pub(crate) fn compare_raw_values(left: &str, right: &str) -> Ordering {
    if let (Some(left_number), Some(right_number)) = (raw_float(left), raw_float(right)) {
        return left_number
            .partial_cmp(&right_number)
            .unwrap_or(Ordering::Equal);
    }
    legacy_sprint(&raw_value_for_response(left)).cmp(&legacy_sprint(&raw_value_for_response(right)))
}

/// legacy `rawFloat`: a JSON string parsed with `ParseFloat`, or a JSON number.
pub(crate) fn raw_float(raw: &str) -> Option<f64> {
    if let Ok(as_string) = serde_json::from_str::<String>(raw) {
        return as_string.parse::<f64>().ok();
    }
    match first_json_value(raw) {
        Some(Value::Number(number)) => number.as_f64(),
        _ => None,
    }
}

/// legacy `rawInt`.
pub(crate) fn raw_int(raw: &str) -> Option<i64> {
    if let Ok(as_string) = serde_json::from_str::<String>(raw) {
        return as_string.parse::<i64>().ok();
    }
    match first_json_value(raw) {
        Some(Value::Number(number)) => number.as_i64(),
        _ => None,
    }
}

/// legacy `isJSONNull` (tabledata_handlers.rs).
pub(crate) fn is_json_null(raw: &str) -> bool {
    raw.trim().eq_ignore_ascii_case("null")
}

/// legacy `formatRowValues` (tabledata_handlers.rs).
pub(crate) fn format_row_values(
    row: &BTreeMap<String, RawJson>,
    fields: &[TableFieldSchema],
) -> Vec<TableCell> {
    fields
        .iter()
        .map(|field| match row.get(&field.name) {
            Some(raw) if !is_json_null(raw.get()) => TableCell {
                v: raw_value_for_field_response(raw.get(), field),
            },
            _ => TableCell { v: Value::Null },
        })
        .collect()
}

/// legacy `rawValueForFieldResponse` (tabledata_handlers.rs): INTEGER/BOOLEAN
/// (and NUMERIC) cells render as strings, everything else decodes as-is.
pub(crate) fn raw_value_for_field_response(raw: &str, field: &TableFieldSchema) -> Value {
    if field.mode.eq_ignore_ascii_case("REPEATED") {
        return raw_value_for_response(raw);
    }
    match default_string(field.field_type.clone(), "STRING")
        .to_uppercase()
        .as_str()
    {
        "INTEGER" | "INT64" => {
            if let Some(value) = raw_int(raw) {
                return Value::String(value.to_string());
            }
        }
        "BOOLEAN" | "BOOL" => {
            if let Ok(value) = serde_json::from_str::<bool>(raw) {
                return Value::String(value.to_string());
            }
        }
        "NUMERIC" | "BIGNUMERIC" => {
            if let Ok(value) = serde_json::from_str::<String>(raw) {
                return Value::String(value);
            }
            if let Some(Value::Number(number)) = first_json_value(raw) {
                return Value::String(number.to_string());
            }
        }
        _ => {}
    }
    raw_value_for_response(raw)
}

/// legacy `rawValueForResponse` (tabledata_handlers.rs): decode the first JSON
/// value (trailing bytes ignored, like a single `Decoder.Decode` call);
/// undecodable bytes come back as their literal text.
pub(crate) fn raw_value_for_response(raw: &str) -> Value {
    match first_json_value(raw) {
        Some(value) => value,
        None => Value::String(raw.to_string()),
    }
}

fn first_json_value(raw: &str) -> Option<Value> {
    match serde_json::Deserializer::from_str(raw)
        .into_iter::<Value>()
        .next()
    {
        Some(Ok(value)) => Some(value),
        _ => None,
    }
}

/// legacy `fmt.Sprint` over a `UseNumber`-decoded JSON value: `nil` → `<nil>`,
/// slices space-joined in brackets, maps as `map[k:v ...]` with sorted keys
/// (serde's `Value` object map is a `BTreeMap`, matching legacy sorted `Sprint`).
pub(crate) fn legacy_sprint(value: &Value) -> String {
    match value {
        Value::Null => "<nil>".to_string(),
        Value::Bool(b) => b.to_string(),
        Value::Number(n) => n.to_string(),
        Value::String(s) => s.clone(),
        Value::Array(items) => {
            let parts: Vec<String> = items.iter().map(legacy_sprint).collect();
            format!("[{}]", parts.join(" "))
        }
        Value::Object(map) => {
            let parts: Vec<String> = map
                .iter()
                .map(|(key, value)| format!("{key}:{}", legacy_sprint(value)))
                .collect();
            format!("map[{}]", parts.join(" "))
        }
    }
}

/// legacy `strconv.FormatFloat(value, 'f', -1, 64)`: shortest round-trip decimal,
/// never scientific notation — Rust's `Display` for `f64` matches, except for
/// the non-finite spellings.
fn format_float_legacy(value: f64) -> String {
    if value.is_nan() {
        return "NaN".to_string();
    }
    if value.is_infinite() {
        return if value > 0.0 { "+Inf" } else { "-Inf" }.to_string();
    }
    format!("{value}")
}
