//! SQL parsing — port of `internal/services/bigquery/sql_parser.rs` plus the
//! engine-internal structs from `types.rs` (`simpleSelectQuery`,
//! `aggregateSelection`, `whereCondition`).
//!
//! The legacy parser keeps the aggregate function and comparison operators as
//! strings (`""` meaning "no aggregate", `"NOT >="` for negated operators);
//! here they are enums (`Option<AggregateSelection>`, [`ComparisonOp`] +
//! `negated`) with exhaustive matches, so legacy "unsupported aggregate
//! function" / "unsupported WHERE operator" defensive branches are
//! unrepresentable. legacy `simpleSelectQuery.WhereConditions` /
//! `WhereField`/`WhereOperator`/`WhereValueRaw` mirrors of the first parsed
//! condition are write-only outside a fallback in `rowMatchesQuery` that the
//! parser can never trigger (it always fills the groups), so only
//! `where_condition_groups` is kept.
//!
//! Keyword scanning uses ASCII case folding over byte indices. legacy uppercases
//! with `strings.ToUpper` and slices the *original* query at indices found in
//! the uppercased copy — identical for all-ASCII queries; for the exotic
//! non-ASCII characters whose uppercase changes byte length legacy silently
//! produces garbled slices, which is not behavior worth reproducing.

use crate::model::{QueryParameter, TableFieldSchema, TableSchema};
use crate::responses::default_string;
use crate::sql_eval::{aggregate_numeric_type, is_numeric_field};
use crate::validation::validate_resource_id;
use crate::wire_json;

/// legacy `simpleSelectQuery`.
#[derive(Debug, Clone, Default)]
pub struct SimpleSelectQuery {
    pub project_id: String,
    pub dataset_id: String,
    pub table_id: String,
    pub selected_fields: Vec<String>,
    pub aggregate: Option<AggregateSelection>,
    pub where_condition_groups: Vec<Vec<WhereCondition>>,
    pub group_by: String,
    pub order_by: String,
    pub order_desc: bool,
    /// `-1` ≙ no LIMIT clause (legacy initializes `Limit: -1`).
    pub limit: i64,
    pub offset: i64,
}

/// legacy `aggregateSelection` (`Function: ""` ≙ `Option::None` at the use site).
#[derive(Debug, Clone)]
pub struct AggregateSelection {
    pub function: AggregateFunction,
    pub field: String,
    pub alias: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggregateFunction {
    Count,
    Sum,
    Avg,
    Min,
    Max,
}

impl AggregateFunction {
    pub(crate) fn name(self) -> &'static str {
        match self {
            AggregateFunction::Count => "COUNT",
            AggregateFunction::Sum => "SUM",
            AggregateFunction::Avg => "AVG",
            AggregateFunction::Min => "MIN",
            AggregateFunction::Max => "MAX",
        }
    }
}

const AGGREGATE_FUNCTIONS: [AggregateFunction; 5] = [
    AggregateFunction::Count,
    AggregateFunction::Sum,
    AggregateFunction::Avg,
    AggregateFunction::Min,
    AggregateFunction::Max,
];

/// legacy `whereCondition`. The operator string (`"NOT <op>"` when negated) is
/// split into [`ComparisonOp`] + `negated`; `value_raw` is the JSON literal
/// text (legacy `json.RawMessage`).
#[derive(Debug, Clone)]
pub struct WhereCondition {
    pub field: String,
    pub op: ComparisonOp,
    pub negated: bool,
    pub value_raw: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ComparisonOp {
    Eq,
    NotEq,
    Gt,
    Ge,
    Lt,
    Le,
}

/// legacy `parseSimpleSelect`.
pub fn parse_simple_select(
    raw_query: &str,
    default_project_id: &str,
) -> Result<SimpleSelectQuery, String> {
    // legacy: TrimSpace(TrimSuffix(rawQuery, ";")) — the semicolon is stripped
    // only when it is the literal last byte, *before* trimming whitespace.
    let query = raw_query.strip_suffix(';').unwrap_or(raw_query).trim();
    if query.is_empty() {
        return Err("query is required".to_string());
    }
    let upper = query.to_ascii_uppercase();
    if !upper.starts_with("SELECT ") {
        return Err("only SELECT queries are supported".to_string());
    }
    let Some(from_index) = upper.find(" FROM ") else {
        return Err("SELECT query requires FROM".to_string());
    };
    let selected = query["SELECT ".len()..from_index].trim();
    if selected.is_empty() {
        return Err("SELECT list is required".to_string());
    }
    let rest = query[from_index + " FROM ".len()..].trim();
    let (table_expr, rest) = next_query_token(rest);
    let (project_id, dataset_id, table_id) =
        parse_table_identifier(table_expr, default_project_id)?;
    let mut parsed = SimpleSelectQuery {
        project_id,
        dataset_id,
        table_id,
        selected_fields: parse_selected_fields(selected),
        limit: -1,
        ..Default::default()
    };
    if parsed.selected_fields.is_empty() {
        return Err("SELECT list is required".to_string());
    }
    if let Some(aggregate) = parse_aggregate_selection(selected)? {
        parsed.aggregate = Some(aggregate);
        parsed.selected_fields = Vec::new();
    } else if let Some((group_field, aggregate)) = parse_grouped_aggregate_selection(selected)? {
        parsed.aggregate = Some(aggregate);
        parsed.selected_fields = vec![group_field];
    }
    let mut rest = rest.trim();
    while !rest.is_empty() {
        let upper_rest = rest.to_ascii_uppercase();
        if upper_rest.starts_with("WHERE ") {
            let mut condition_end = rest.len();
            for marker in [" GROUP BY ", " ORDER BY ", " LIMIT ", " OFFSET "] {
                if let Some(idx) = upper_rest.find(marker) {
                    if idx < condition_end {
                        condition_end = idx;
                    }
                }
            }
            let condition = rest["WHERE ".len()..condition_end].trim();
            parsed.where_condition_groups = parse_simple_condition_groups(condition)?;
            rest = rest[condition_end..].trim();
        } else if upper_rest.starts_with("GROUP BY ") {
            let value = rest["GROUP BY ".len()..].trim();
            let (field, suffix) = next_query_token(value);
            if field.is_empty() {
                return Err("GROUP BY field is required".to_string());
            }
            parsed.group_by = field.trim_matches('`').to_string();
            rest = suffix.trim();
        } else if upper_rest.starts_with("ORDER BY ") {
            if parsed.aggregate.is_some() && parsed.group_by.is_empty() {
                return Err("ORDER BY is not supported for aggregate queries".to_string());
            }
            let value = rest["ORDER BY ".len()..].trim();
            let (field, suffix) = next_query_token(value);
            if field.is_empty() {
                return Err("ORDER BY field is required".to_string());
            }
            parsed.order_by = field.trim_matches('`').to_string();
            rest = suffix.trim();
            let (direction, direction_suffix) = next_query_token(rest);
            match direction.to_ascii_uppercase().as_str() {
                "ASC" => rest = direction_suffix.trim(),
                "DESC" => {
                    parsed.order_desc = true;
                    rest = direction_suffix.trim();
                }
                _ => {}
            }
        } else if upper_rest.starts_with("LIMIT ") {
            let (value, suffix) = next_query_token(rest["LIMIT ".len()..].trim());
            match value.parse::<i64>() {
                Ok(limit) if limit >= 0 => parsed.limit = limit,
                _ => return Err("LIMIT must be a non-negative integer".to_string()),
            }
            rest = suffix.trim();
        } else if upper_rest.starts_with("OFFSET ") {
            let (value, suffix) = next_query_token(rest["OFFSET ".len()..].trim());
            match value.parse::<i64>() {
                Ok(offset) if offset >= 0 => parsed.offset = offset,
                _ => return Err("OFFSET must be a non-negative integer".to_string()),
            }
            rest = suffix.trim();
        } else {
            return Err("unsupported query clause".to_string());
        }
    }
    if !parsed.group_by.is_empty() {
        if parsed.aggregate.is_none() {
            return Err("GROUP BY requires an aggregate selection".to_string());
        }
        if parsed.selected_fields.len() != 1 || parsed.selected_fields[0] != parsed.group_by {
            return Err("GROUP BY field must be selected".to_string());
        }
    }
    Ok(parsed)
}

/// legacy `bindQueryParameters`.
pub fn bind_query_parameters(
    raw_query: &str,
    parameters: &[QueryParameter],
) -> Result<String, String> {
    if parameters.is_empty() {
        return Ok(raw_query.to_string());
    }
    let mut replacements: Vec<(String, String)> = Vec::with_capacity(parameters.len());
    for parameter in parameters {
        let name = parameter.name.trim();
        if name.is_empty() {
            return Err("named query parameter name is required".to_string());
        }
        validate_resource_id(name, "query parameter")?;
        let value = parameter_sql_literal(parameter)?;
        replacements.push((name.to_string(), value));
    }
    let (bound, used) = replace_named_parameters(raw_query, &replacements)?;
    for (name, _) in &replacements {
        if !used.contains(name) {
            return Err(format!("query parameter {name:?} was not used"));
        }
    }
    Ok(bound)
}

/// legacy `replaceNamedParameters`: byte scan with single/double-quote tracking;
/// `@name` outside quotes is replaced, unknown names error.
fn replace_named_parameters(
    query: &str,
    replacements: &[(String, String)],
) -> Result<(String, Vec<String>), String> {
    let bytes = query.as_bytes();
    let mut out: Vec<u8> = Vec::with_capacity(bytes.len());
    let mut used: Vec<String> = Vec::new();
    let mut in_single_quote = false;
    let mut in_double_quote = false;
    let mut i = 0;
    while i < bytes.len() {
        let ch = bytes[i];
        match ch {
            b'\'' => {
                if !in_double_quote {
                    in_single_quote = !in_single_quote;
                }
                out.push(ch);
                i += 1;
            }
            b'"' => {
                if !in_single_quote {
                    in_double_quote = !in_double_quote;
                }
                out.push(ch);
                i += 1;
            }
            b'@' => {
                if in_single_quote || in_double_quote {
                    out.push(ch);
                    i += 1;
                    continue;
                }
                let mut end = i + 1;
                while end < bytes.len() && is_parameter_name_byte(bytes[end]) {
                    end += 1;
                }
                if end == i + 1 {
                    out.push(ch);
                    i += 1;
                    continue;
                }
                let name = &query[i + 1..end];
                let Some((_, value)) = replacements.iter().find(|(n, _)| n == name) else {
                    return Err(format!("query parameter {name:?} was not provided"));
                };
                if !used.iter().any(|n| n == name) {
                    used.push(name.to_string());
                }
                out.extend_from_slice(value.as_bytes());
                i = end;
            }
            _ => {
                out.push(ch);
                i += 1;
            }
        }
    }
    Ok((
        String::from_utf8(out).expect("byte-wise rewrite preserves utf-8"),
        used,
    ))
}

fn is_parameter_name_byte(ch: u8) -> bool {
    ch.is_ascii_alphanumeric() || ch == b'_'
}

/// legacy `parameterSQLLiteral`.
fn parameter_sql_literal(parameter: &QueryParameter) -> Result<String, String> {
    let value = &parameter.parameter_value.value;
    let field_type =
        default_string(parameter.parameter_type.param_type.clone(), "STRING").to_uppercase();
    match field_type.as_str() {
        "STRING" | "BYTES" | "NUMERIC" | "BIGNUMERIC" | "TIMESTAMP" | "DATE" | "TIME"
        | "DATETIME" | "GEOGRAPHY" | "JSON" => {
            Ok(String::from_utf8(wire_json::marshal(value)).expect("json string literal is utf-8"))
        }
        "INTEGER" | "INT64" => {
            if value.parse::<i64>().is_err() {
                return Err(format!(
                    "query parameter {:?} must be an integer",
                    parameter.name
                ));
            }
            Ok(value.clone())
        }
        "FLOAT" | "FLOAT64" => {
            if value.parse::<f64>().is_err() {
                return Err(format!(
                    "query parameter {:?} must be a number",
                    parameter.name
                ));
            }
            Ok(value.clone())
        }
        "BOOLEAN" | "BOOL" => match legacy_parse_bool(value) {
            Some(true) => Ok("true".to_string()),
            Some(false) => Ok("false".to_string()),
            None => Err(format!(
                "query parameter {:?} must be a boolean",
                parameter.name
            )),
        },
        _ => Err(format!(
            "unsupported query parameter type {:?}",
            parameter.parameter_type.param_type
        )),
    }
}

/// legacy `strconv.ParseBool`.
fn legacy_parse_bool(value: &str) -> Option<bool> {
    match value {
        "1" | "t" | "T" | "TRUE" | "true" | "True" => Some(true),
        "0" | "f" | "F" | "FALSE" | "false" | "False" => Some(false),
        _ => None,
    }
}

/// legacy `parseAggregateSelection`: `Ok(None)` ≙ "not an aggregate" (legacy
/// `ok == false`), `Err` ≙ malformed aggregate.
pub(crate) fn parse_aggregate_selection(
    selected: &str,
) -> Result<Option<AggregateSelection>, String> {
    let mut expr = selected.trim();
    if expr.contains(',') {
        return Ok(None);
    }
    let mut alias = String::new();
    // legacy checks the exact markers " AS " and " as " only (no "As"/"aS").
    for marker in [" AS ", " as "] {
        if let Some((left, right)) = expr.split_once(marker) {
            expr = left.trim();
            alias = right.trim().trim_matches('`').to_string();
            if alias.is_empty() {
                return Err("aggregate alias is empty".to_string());
            }
            break;
        }
    }
    let upper = expr.to_ascii_uppercase();
    let function = AGGREGATE_FUNCTIONS
        .into_iter()
        .find(|candidate| upper.starts_with(&format!("{}(", candidate.name())));
    let function = match function {
        Some(function) if expr.ends_with(')') => function,
        _ => {
            if upper.contains('(') || upper.contains(')') {
                return Err("unsupported aggregate expression".to_string());
            }
            return Ok(None);
        }
    };
    let field = expr[function.name().len() + 1..expr.len() - 1]
        .trim()
        .trim_matches('`');
    if field.is_empty() {
        return Err(format!("{} requires a field or *", function.name()));
    }
    if field == "*" && function != AggregateFunction::Count {
        return Err(format!("{} requires a field", function.name()));
    }
    Ok(Some(AggregateSelection {
        function,
        field: field.to_string(),
        alias,
    }))
}

/// legacy `parseGroupedAggregateSelection`: `Ok(Some((group_field, aggregate)))`
/// for a two-part `field, AGG(...)` selection.
pub(crate) fn parse_grouped_aggregate_selection(
    selected: &str,
) -> Result<Option<(String, AggregateSelection)>, String> {
    let parts: Vec<&str> = selected.split(',').collect();
    if parts.len() != 2 {
        return Ok(None);
    }
    let mut group_field = String::new();
    let mut aggregate: Option<AggregateSelection> = None;
    let mut group_field_count = 0;
    for part in parts {
        let expr = part.trim();
        if let Some(parsed_aggregate) = parse_aggregate_selection(expr)? {
            if aggregate.is_some() {
                return Err("GROUP BY supports one aggregate expression".to_string());
            }
            aggregate = Some(parsed_aggregate);
            continue;
        }
        if expr.contains(['(', ')']) {
            return Err("unsupported grouped SELECT expression".to_string());
        }
        if group_field_count > 0 && aggregate.is_some() {
            return Err("GROUP BY supports one selected field".to_string());
        }
        group_field = expr.trim_matches('`').to_string();
        group_field_count += 1;
    }
    let Some(aggregate) = aggregate else {
        return Ok(None);
    };
    if group_field_count != 1 {
        return Err("GROUP BY supports one selected field".to_string());
    }
    if group_field.is_empty() {
        return Ok(None);
    }
    Ok(Some((group_field, aggregate)))
}

/// legacy `aggregateField`.
pub(crate) fn aggregate_field<'a>(
    schema: &'a TableSchema,
    aggregate: &AggregateSelection,
) -> Option<&'a TableFieldSchema> {
    if aggregate.field == "*" {
        return None;
    }
    schema.fields.iter().find(|f| f.name == aggregate.field)
}

/// legacy `aggregateDryRunFields`.
pub(crate) fn aggregate_dry_run_fields(
    schema: &TableSchema,
    aggregate: &AggregateSelection,
) -> Result<Vec<TableFieldSchema>, String> {
    let field_name = if aggregate.alias.is_empty() {
        "f0_".to_string()
    } else {
        aggregate.alias.clone()
    };
    let single = |field_type: String| -> Vec<TableFieldSchema> {
        vec![TableFieldSchema {
            name: field_name.clone(),
            field_type,
            mode: "NULLABLE".to_string(),
            ..Default::default()
        }]
    };
    if aggregate.field == "*" {
        if aggregate.function != AggregateFunction::Count {
            return Err(format!("{} requires a field", aggregate.function.name()));
        }
        return Ok(single("INTEGER".to_string()));
    }
    let Some(field) = aggregate_field(schema, aggregate) else {
        return Err(format!(
            "aggregate field {:?} does not exist",
            aggregate.field
        ));
    };
    match aggregate.function {
        AggregateFunction::Count => Ok(single("INTEGER".to_string())),
        AggregateFunction::Sum => {
            if !is_numeric_field(field) {
                return Err("SUM requires a numeric field".to_string());
            }
            Ok(single(aggregate_numeric_type(field)))
        }
        AggregateFunction::Avg => {
            if !is_numeric_field(field) {
                return Err("AVG requires a numeric field".to_string());
            }
            Ok(single("FLOAT".to_string()))
        }
        AggregateFunction::Min | AggregateFunction::Max => {
            Ok(single(default_string(field.field_type.clone(), "STRING")))
        }
    }
}

/// legacy `groupedAggregateDryRunFields`.
pub(crate) fn grouped_aggregate_dry_run_fields(
    schema: &TableSchema,
    group_by: &str,
    aggregate: &AggregateSelection,
) -> Result<Vec<TableFieldSchema>, String> {
    let mut fields = fields_for_query(schema, &[group_by.to_string()])?;
    fields.extend(aggregate_dry_run_fields(schema, aggregate)?);
    Ok(fields)
}

/// legacy `nextQueryToken`: one whitespace-delimited token (backquoted tokens kept
/// intact) plus the trimmed remainder.
fn next_query_token(value: &str) -> (&str, &str) {
    let value = value.trim();
    if value.starts_with('`') {
        if let Some(end) = value[1..].find('`') {
            let token_end = end + 2;
            return (&value[..token_end], value[token_end..].trim());
        }
    }
    match value.find([' ', '\t', '\n', '\r']) {
        None => (value, ""),
        Some(index) => (&value[..index], value[index..].trim()),
    }
}

/// legacy `parseTableIdentifier`.
fn parse_table_identifier(
    identifier: &str,
    default_project_id: &str,
) -> Result<(String, String, String), String> {
    let trimmed = identifier.trim().trim_matches('`');
    let parts: Vec<&str> = trimmed.split('.').collect();
    match parts.as_slice() {
        [dataset, table] => {
            let project = if default_project_id.is_empty() {
                "devcloud"
            } else {
                default_project_id
            };
            Ok((project.to_string(), dataset.to_string(), table.to_string()))
        }
        [project, dataset, table] => {
            Ok((project.to_string(), dataset.to_string(), table.to_string()))
        }
        _ => Err("FROM table must be dataset.table or project.dataset.table".to_string()),
    }
}

/// legacy `parseSelectedFields`.
fn parse_selected_fields(selected: &str) -> Vec<String> {
    if selected.trim() == "*" {
        return vec!["*".to_string()];
    }
    selected
        .split(',')
        .map(|part| part.trim().trim_matches('`'))
        .filter(|field| !field.is_empty())
        .map(str::to_string)
        .collect()
}

/// legacy `parseSimpleConditions`.
fn parse_simple_conditions(condition: &str) -> Result<Vec<WhereCondition>, String> {
    let parts = split_and_conditions(condition);
    if parts.is_empty() {
        return Err("WHERE condition is required".to_string());
    }
    parts
        .iter()
        .map(|part| parse_simple_condition(part))
        .collect()
}

/// legacy `parseSimpleConditionGroups` (groups are OR-ed, members AND-ed).
fn parse_simple_condition_groups(condition: &str) -> Result<Vec<Vec<WhereCondition>>, String> {
    let groups = split_or_condition_groups(condition);
    if groups.is_empty() {
        return Err("WHERE condition is required".to_string());
    }
    groups
        .iter()
        .map(|group| parse_simple_conditions(group))
        .collect()
}

/// legacy `splitORConditionGroups`: whitespace-tokenized split on case-insensitive
/// `OR`; a leading/trailing/double `OR` yields no groups (legacy returns nil).
fn split_or_condition_groups(condition: &str) -> Vec<String> {
    split_on_keyword(condition, "OR")
}

/// legacy `splitANDConditions`.
fn split_and_conditions(condition: &str) -> Vec<String> {
    split_on_keyword(condition, "AND")
}

fn split_on_keyword(condition: &str, keyword: &str) -> Vec<String> {
    let parts: Vec<&str> = condition.split_whitespace().collect();
    if parts.is_empty() {
        return Vec::new();
    }
    let mut groups: Vec<String> = Vec::new();
    let mut current: Vec<&str> = Vec::new();
    for part in parts {
        if part.eq_ignore_ascii_case(keyword) {
            if current.is_empty() {
                return Vec::new();
            }
            groups.push(current.join(" "));
            current = Vec::new();
            continue;
        }
        current.push(part);
    }
    if current.is_empty() {
        return Vec::new();
    }
    groups.push(current.join(" "));
    groups
}

/// legacy `parseSimpleCondition`.
fn parse_simple_condition(condition: &str) -> Result<WhereCondition, String> {
    let mut trimmed = condition.trim();
    let mut negated = false;
    if trimmed.to_ascii_uppercase().starts_with("NOT ") {
        negated = true;
        trimmed = trimmed["NOT ".len()..].trim();
    }
    for (op_text, op) in [
        (">=", ComparisonOp::Ge),
        ("<=", ComparisonOp::Le),
        ("!=", ComparisonOp::NotEq),
        ("=", ComparisonOp::Eq),
        (">", ComparisonOp::Gt),
        ("<", ComparisonOp::Lt),
    ] {
        if let Some(idx) = trimmed.find(op_text) {
            let field = trimmed[..idx].trim().trim_matches('`');
            let value = trimmed[idx + op_text.len()..].trim();
            if field.is_empty() || value.is_empty() {
                return Err("WHERE condition must compare a field to a literal".to_string());
            }
            let value_raw = raw_json_literal(value)?;
            return Ok(WhereCondition {
                field: field.to_string(),
                op,
                negated,
                value_raw,
            });
        }
    }
    Err("WHERE supports simple comparisons only".to_string())
}

/// legacy `rawJSONLiteral`: SQL literal text → raw JSON value text.
fn raw_json_literal(value: &str) -> Result<String, String> {
    let trimmed = value.trim();
    if trimmed.starts_with('\'') && trimmed.ends_with('\'') && trimmed.len() >= 2 {
        // legacy: json.Marshal(strings.Trim(trimmed, "'")) — *all* leading and
        // trailing single quotes are stripped.
        return Ok(
            String::from_utf8(wire_json::marshal(&trimmed.trim_matches('\'')))
                .expect("json string literal is utf-8"),
        );
    }
    if trimmed.starts_with('"') && trimmed.ends_with('"') {
        return match serde_json::from_str::<String>(trimmed) {
            Ok(_) => Ok(trimmed.to_string()),
            Err(_) => Err("invalid string literal".to_string()),
        };
    }
    match trimmed.to_ascii_uppercase().as_str() {
        "TRUE" => return Ok("true".to_string()),
        "FALSE" => return Ok("false".to_string()),
        "NULL" => return Ok("null".to_string()),
        _ => {}
    }
    // legacy decodes one JSON value and ignores trailing bytes (a single
    // `Decoder.Decode` call), then returns the *whole* trimmed text.
    match serde_json::Deserializer::from_str(trimmed)
        .into_iter::<serde_json::Value>()
        .next()
    {
        Some(Ok(_)) => Ok(trimmed.to_string()),
        _ => Err("WHERE literal must be a number, boolean, null, or quoted string".to_string()),
    }
}

/// legacy `fieldsForQuery`.
pub(crate) fn fields_for_query(
    schema: &TableSchema,
    selected: &[String],
) -> Result<Vec<TableFieldSchema>, String> {
    if selected.len() == 1 && selected[0] == "*" {
        return Ok(schema.fields.clone());
    }
    let mut fields = Vec::with_capacity(selected.len());
    for name in selected {
        // legacy builds a name→field map first, so for duplicate names the *last*
        // schema entry wins (unlike `aggregateField`, which scans and returns
        // the first).
        let Some(field) = schema.fields.iter().rev().find(|field| &field.name == name) else {
            return Err(format!("selected field {name:?} does not exist"));
        };
        fields.push(field.clone());
    }
    Ok(fields)
}
