//! PartiQL `SELECT` parsing and evaluation.
//!
//! Mirrors `internal/services/dynamodb/partiql.go`. Only `SELECT` is supported,
//! with positional (`?`) parameters and equality predicates joined by `AND`
//! (the subset the Go service implements). Used by ExecuteStatement,
//! BatchExecuteStatement, and ExecuteTransaction.

use serde_json::Value;

use crate::model::{Item, TableDescription};

/// A parsed `SELECT` statement.
#[derive(Debug, Clone)]
pub struct PartiQLSelect {
    pub table_name: String,
    /// `None` projections == `SELECT *`.
    pub projections: Vec<String>,
    pub conditions: Vec<PartiQLCondition>,
}

#[derive(Debug, Clone)]
pub struct PartiQLCondition {
    pub attribute: String,
    pub value: Value,
}

/// Parses a `SELECT` statement with positional parameters, mirroring
/// `parsePartiQLSelect`.
pub fn parse_select(statement: &str, parameters: &[Value]) -> Result<PartiQLSelect, String> {
    let statement = statement.trim();
    let statement = statement.strip_suffix(';').unwrap_or(statement).trim();
    if statement.is_empty() {
        return Err("statement is required".to_string());
    }
    let upper = statement.to_uppercase();
    if !upper.starts_with("SELECT ") {
        return Err("only SELECT statements are supported".to_string());
    }
    let from_index = upper
        .find(" FROM ")
        .ok_or_else(|| "SELECT statement must include FROM".to_string())?;
    let projection_part = statement["SELECT ".len()..from_index].trim();
    let after_from = statement[from_index + " FROM ".len()..].trim();
    let (table_name_raw, where_part) = match after_from.to_uppercase().find(" WHERE ") {
        Some(where_index) => (
            after_from[..where_index].trim(),
            after_from[where_index + " WHERE ".len()..].trim(),
        ),
        None => (after_from, ""),
    };
    let table_name = trim_identifier(table_name_raw);
    if table_name.is_empty() {
        return Err("table name is required".to_string());
    }
    let projections = parse_projections(projection_part)?;
    let conditions = parse_where(where_part, parameters)?;
    Ok(PartiQLSelect {
        table_name,
        projections,
        conditions,
    })
}

fn parse_projections(value: &str) -> Result<Vec<String>, String> {
    let value = value.trim();
    if value.is_empty() {
        return Err("SELECT projection is required".to_string());
    }
    if value == "*" {
        return Ok(Vec::new());
    }
    let mut projections = Vec::new();
    for token in value.split(',') {
        let name = trim_identifier(token.trim());
        if name.is_empty() {
            return Err("invalid SELECT projection".to_string());
        }
        projections.push(name);
    }
    Ok(projections)
}

fn parse_where(value: &str, parameters: &[Value]) -> Result<Vec<PartiQLCondition>, String> {
    let value = value.trim();
    if value.is_empty() {
        if !parameters.is_empty() {
            return Err("too many PartiQL parameters".to_string());
        }
        return Ok(Vec::new());
    }
    let parts = split_and(value);
    let mut conditions = Vec::with_capacity(parts.len());
    let mut param_index = 0;
    for part in parts {
        let (left, right) = part
            .split_once('=')
            .ok_or_else(|| "WHERE supports equality predicates only".to_string())?;
        let attribute = trim_identifier(left.trim());
        if attribute.is_empty() {
            return Err("invalid WHERE attribute".to_string());
        }
        if right.trim() != "?" {
            return Err("WHERE predicates must use positional parameters".to_string());
        }
        if param_index >= parameters.len() {
            return Err("missing PartiQL parameter".to_string());
        }
        conditions.push(PartiQLCondition {
            attribute,
            value: parameters[param_index].clone(),
        });
        param_index += 1;
    }
    if param_index != parameters.len() {
        return Err("too many PartiQL parameters".to_string());
    }
    Ok(conditions)
}

fn split_and(value: &str) -> Vec<String> {
    let mut parts = Vec::new();
    let mut current: Vec<&str> = Vec::new();
    for field in value.split_whitespace() {
        if field.eq_ignore_ascii_case("AND") {
            parts.push(current.join(" "));
            current.clear();
            continue;
        }
        current.push(field);
    }
    if !current.is_empty() {
        parts.push(current.join(" "));
    }
    parts
}

/// Strips a single matching pair of `"`, `'`, or backtick quotes, mirroring
/// `trimPartiQLIdentifier`.
fn trim_identifier(value: &str) -> String {
    let value = value.trim();
    let bytes = value.as_bytes();
    if bytes.len() >= 2 {
        let first = bytes[0];
        let last = bytes[bytes.len() - 1];
        if (first == b'"' && last == b'"')
            || (first == b'\'' && last == b'\'')
            || (first == b'`' && last == b'`')
        {
            return value[1..value.len() - 1].to_string();
        }
    }
    value.to_string()
}

/// True when every condition matches the item exactly (deep equality), mirroring
/// `partiQLConditionsMatch`.
pub fn conditions_match(value: &Item, conditions: &[PartiQLCondition]) -> bool {
    conditions.iter().all(|condition| {
        value
            .get(&condition.attribute)
            .map(|actual| actual == &condition.value)
            .unwrap_or(false)
    })
}

/// Projects an item to the named attributes (empty projections == whole item),
/// mirroring `projectPartiQLItem`.
pub fn project_item(value: &Item, projections: &[String]) -> Item {
    if projections.is_empty() {
        return value.clone();
    }
    let mut projected = Item::new();
    for name in projections {
        if let Some(attr) = value.get(name) {
            projected.insert(name.clone(), attr.clone());
        }
    }
    projected
}

/// True when the conditions include an equality on every key attribute,
/// mirroring `partiQLConditionsCoverKey`.
pub fn conditions_cover_key(
    description: &TableDescription,
    conditions: &[PartiQLCondition],
) -> bool {
    let attrs: std::collections::BTreeSet<&str> =
        conditions.iter().map(|c| c.attribute.as_str()).collect();
    description
        .key_schema
        .iter()
        .all(|element| attrs.contains(element.attribute_name.as_str()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn params(vs: &[Value]) -> Vec<Value> {
        vs.to_vec()
    }

    #[test]
    fn parses_select_star_with_where() {
        let s = parse_select(
            "SELECT * FROM T WHERE pk = ?",
            &params(&[json!({"S": "u1"})]),
        )
        .unwrap();
        assert_eq!(s.table_name, "T");
        assert!(s.projections.is_empty());
        assert_eq!(s.conditions.len(), 1);
        assert_eq!(s.conditions[0].attribute, "pk");
    }

    #[test]
    fn parses_quoted_table_and_projections() {
        let s = parse_select(
            "SELECT name, sk FROM \"T\" WHERE pk = ? AND sk = ?",
            &params(&[json!({"S": "u1"}), json!({"N": "2"})]),
        )
        .unwrap();
        assert_eq!(s.table_name, "T");
        assert_eq!(s.projections, vec!["name", "sk"]);
        assert_eq!(s.conditions.len(), 2);
    }

    #[test]
    fn rejects_non_select() {
        assert_eq!(
            parse_select("DELETE FROM T WHERE pk = ?", &params(&[json!({"S": "u1"})])).unwrap_err(),
            "only SELECT statements are supported"
        );
    }

    #[test]
    fn rejects_missing_param() {
        assert_eq!(
            parse_select("SELECT * FROM T WHERE pk = ?", &[]).unwrap_err(),
            "missing PartiQL parameter"
        );
    }

    #[test]
    fn rejects_too_many_params() {
        assert_eq!(
            parse_select("SELECT * FROM T", &params(&[json!({"S": "x"})])).unwrap_err(),
            "too many PartiQL parameters"
        );
    }

    #[test]
    fn conditions_match_and_cover() {
        let mut it = Item::new();
        it.insert("pk".to_string(), json!({"S": "u1"}));
        it.insert("sk".to_string(), json!({"N": "1"}));
        let conds = vec![PartiQLCondition {
            attribute: "pk".to_string(),
            value: json!({"S": "u1"}),
        }];
        assert!(conditions_match(&it, &conds));
    }
}
