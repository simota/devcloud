//! Condition / filter / key-condition expression evaluation.
//!
//! Mirrors `internal/services/dynamodb/{expressions,expression_tokens,
//! expression_attributes}.rs`. Update expressions (SET/REMOVE/ADD/DELETE) land
//! in a later part; this part covers the read-side predicate language used by
//! ConditionExpression, FilterExpression, and KeyConditionExpression.

use std::cmp::Ordering;
use std::collections::BTreeMap;

use serde_json::{json, Value};

use crate::attribute::{
    attribute_type_name, attribute_values_equal, compare_attribute_values, resolve_attribute_name,
};
use crate::model::Item;

type Names = BTreeMap<String, String>;
type Values = BTreeMap<String, Value>;

/// Evaluates a ConditionExpression against the current item, mirroring
/// `checkCondition`. An empty expression always passes; a non-match yields the
/// legacy "condition check failed" error.
pub fn check_condition(
    expression: &str,
    names: &Names,
    values: &Values,
    existing: Option<&Item>,
) -> Result<(), String> {
    let expression = expression.trim();
    if expression.is_empty() {
        return Ok(());
    }
    let empty = Item::new();
    let candidate = existing.unwrap_or(&empty);
    if match_conjunctive_expression(expression, names, values, candidate)? {
        Ok(())
    } else {
        Err("condition check failed".to_string())
    }
}

/// Evaluates a FilterExpression (empty == always true), mirroring `matchFilter`.
pub fn match_filter(
    expression: &str,
    names: &Names,
    values: &Values,
    candidate: &Item,
) -> Result<bool, String> {
    let expression = expression.trim();
    if expression.is_empty() {
        return Ok(true);
    }
    match_conjunctive_expression(expression, names, values, candidate)
}

/// Evaluates a KeyConditionExpression: a pure AND of predicates, mirroring
/// `matchKeyCondition`.
pub fn match_key_condition(
    expression: &str,
    names: &Names,
    values: &Values,
    candidate: &Item,
) -> Result<bool, String> {
    for part in split_conjunctive_predicates(expression)? {
        if !match_predicate(part.trim(), names, values, candidate)? {
            return Ok(false);
        }
    }
    Ok(true)
}

fn match_conjunctive_expression(
    expression: &str,
    names: &Names,
    values: &Values,
    candidate: &Item,
) -> Result<bool, String> {
    for disjunct in split_disjunctive_predicates(expression)? {
        let parts = split_conjunctive_predicates(&disjunct)?;
        let mut matched_all = true;
        for part in parts {
            if !match_predicate(part.trim(), names, values, candidate)? {
                matched_all = false;
                break;
            }
        }
        if matched_all {
            return Ok(true);
        }
    }
    Ok(false)
}

fn match_predicate(
    expression: &str,
    names: &Names,
    values: &Values,
    candidate: &Item,
) -> Result<bool, String> {
    if expression.to_uppercase().starts_with("NOT ") {
        let inner = expression[4..].trim();
        return Ok(!match_predicate(inner, names, values, candidate)?);
    }
    if let Some(result) = evaluate_existence_predicate(expression, names, candidate) {
        return Ok(result);
    }
    if let Some(result) = evaluate_binary_function_predicate(expression, names, values, candidate)?
    {
        return Ok(result);
    }
    if let Some((attr, lower, upper)) = split_between_expression(expression) {
        return evaluate_between_predicate(&attr, &lower, &upper, names, values, candidate);
    }
    if let Some((attr, value_tokens)) = split_in_expression(expression) {
        return evaluate_in_predicate(&attr, &value_tokens, names, values, candidate);
    }
    evaluate_comparison_predicate(expression, names, values, candidate)
}

fn evaluate_existence_predicate(expression: &str, names: &Names, candidate: &Item) -> Option<bool> {
    if let Some(body) = parse_function_call(expression, "attribute_exists") {
        let attr = resolve_attribute_name(body.trim(), names);
        return Some(candidate.contains_key(&attr));
    }
    if let Some(body) = parse_function_call(expression, "attribute_not_exists") {
        let attr = resolve_attribute_name(body.trim(), names);
        return Some(!candidate.contains_key(&attr));
    }
    None
}

fn evaluate_binary_function_predicate(
    expression: &str,
    names: &Names,
    values: &Values,
    candidate: &Item,
) -> Result<Option<bool>, String> {
    const FUNCS: [&str; 3] = ["begins_with", "contains", "attribute_type"];
    for name in FUNCS {
        let Some(body) = parse_function_call(expression, name) else {
            continue;
        };
        let args = split_comma_separated(&body);
        if args.len() != 2 {
            return Err(format!("invalid {name} expression"));
        }
        let attr = resolve_attribute_name(args[0].trim(), names);
        let value_token = args[1].trim();
        let expected = values
            .get(value_token)
            .ok_or_else(|| format!("missing expression attribute value {value_token}"))?;
        let actual = candidate.get(&attr);
        let result = match name {
            "begins_with" => attribute_begins_with(actual, expected),
            "contains" => attribute_contains(actual, expected),
            "attribute_type" => attribute_has_type(actual, expected),
            _ => unreachable!(),
        };
        return Ok(Some(result));
    }
    Ok(None)
}

fn evaluate_between_predicate(
    attr_token: &str,
    lower_token: &str,
    upper_token: &str,
    names: &Names,
    values: &Values,
    candidate: &Item,
) -> Result<bool, String> {
    let attr = resolve_attribute_name(attr_token.trim(), names);
    let lower = values
        .get(lower_token.trim())
        .ok_or_else(|| format!("missing expression attribute value {}", lower_token.trim()))?;
    let upper = values
        .get(upper_token.trim())
        .ok_or_else(|| format!("missing expression attribute value {}", upper_token.trim()))?;
    let Some(actual) = candidate.get(&attr) else {
        return Ok(false);
    };
    Ok(compare_attribute_values(actual, lower) != Ordering::Less
        && compare_attribute_values(actual, upper) != Ordering::Greater)
}

fn evaluate_in_predicate(
    attr_token: &str,
    value_tokens: &[String],
    names: &Names,
    values: &Values,
    candidate: &Item,
) -> Result<bool, String> {
    let attr = resolve_attribute_name(attr_token.trim(), names);
    let Some(actual) = candidate.get(&attr) else {
        return Ok(false);
    };
    if value_tokens.is_empty() {
        return Err("IN expression requires at least one value".to_string());
    }
    for token in value_tokens {
        let token = token.trim();
        let expected = values
            .get(token)
            .ok_or_else(|| format!("missing expression attribute value {token}"))?;
        if attribute_values_equal(actual, expected) {
            return Ok(true);
        }
    }
    Ok(false)
}

fn evaluate_comparison_predicate(
    expression: &str,
    names: &Names,
    values: &Values,
    candidate: &Item,
) -> Result<bool, String> {
    let (name_token, operator, value_token) = split_comparison_expression(expression)
        .ok_or_else(|| "unsupported expression predicate".to_string())?;

    // size(attr) operand.
    if let Some(actual_size) = evaluate_size_operand(name_token.trim(), names, candidate)? {
        let value_token = value_token.trim();
        let expected = values
            .get(value_token)
            .ok_or_else(|| format!("missing expression attribute value {value_token}"))?;
        let comparison = compare_attribute_values(&json!({"N": actual_size.to_string()}), expected);
        return compare_with_operator(comparison, &operator);
    }

    let attr = resolve_attribute_name(name_token.trim(), names);
    let value_token = value_token.trim();
    let expected = values
        .get(value_token)
        .ok_or_else(|| format!("missing expression attribute value {value_token}"))?;
    let Some(actual) = candidate.get(&attr) else {
        return Ok(false);
    };
    match operator.as_str() {
        "=" => Ok(attribute_values_equal(actual, expected)),
        "<>" => Ok(!attribute_values_equal(actual, expected)),
        _ => compare_with_operator(compare_attribute_values(actual, expected), &operator),
    }
}

fn compare_with_operator(comparison: Ordering, operator: &str) -> Result<bool, String> {
    Ok(match operator {
        "=" => comparison == Ordering::Equal,
        "<>" => comparison != Ordering::Equal,
        "<" => comparison == Ordering::Less,
        "<=" => comparison != Ordering::Greater,
        ">" => comparison == Ordering::Greater,
        ">=" => comparison != Ordering::Less,
        _ => return Err(format!("unsupported comparison operator {operator}")),
    })
}

/// `size(attr)` operand: `Ok(Some(n))` when the token is a size() call (n=0 if
/// the attribute is absent); `Ok(None)` when it is not a size() call. Mirrors
/// `evaluateSizeOperand`.
fn evaluate_size_operand(
    expression: &str,
    names: &Names,
    candidate: &Item,
) -> Result<Option<i64>, String> {
    let Some(body) = parse_function_call(expression, "size") else {
        return Ok(None);
    };
    let attr = resolve_attribute_name(body.trim(), names);
    let Some(value) = candidate.get(&attr) else {
        return Ok(Some(0));
    };
    match attribute_size(value) {
        Some(size) => Ok(Some(size)),
        None => Err(format!("size is not supported for attribute {attr}")),
    }
}

/// The DynamoDB `size()` of a value, mirroring `attributeSize`.
fn attribute_size(value: &Value) -> Option<i64> {
    let obj = value.as_object()?;
    if let Some(s) = obj.get("S").and_then(Value::as_str) {
        return Some(s.len() as i64);
    }
    if let Some(b) = obj.get("B").and_then(Value::as_str) {
        return Some(b.len() as i64);
    }
    for set in ["SS", "NS", "BS"] {
        if let Some(arr) = obj.get(set).and_then(Value::as_array) {
            return Some(arr.len() as i64);
        }
    }
    if let Some(arr) = obj.get("L").and_then(Value::as_array) {
        return Some(arr.len() as i64);
    }
    if let Some(map) = obj.get("M").and_then(Value::as_object) {
        return Some(map.len() as i64);
    }
    None
}

// --- binary function evaluators --------------------------------------------

fn attribute_has_type(actual: Option<&Value>, expected: &Value) -> bool {
    let Some(expected_type) = expected
        .as_object()
        .and_then(|o| o.get("S"))
        .and_then(Value::as_str)
    else {
        return false;
    };
    let actual = actual.unwrap_or(&Value::Null);
    attribute_type_name(actual) == expected_type
}

fn attribute_begins_with(actual: Option<&Value>, expected: &Value) -> bool {
    let actual_string = actual
        .and_then(Value::as_object)
        .and_then(|o| o.get("S"))
        .and_then(Value::as_str);
    let expected_string = expected
        .as_object()
        .and_then(|o| o.get("S"))
        .and_then(Value::as_str);
    match (actual_string, expected_string) {
        (Some(a), Some(b)) => a.starts_with(b),
        _ => false,
    }
}

fn attribute_contains(actual: Option<&Value>, expected: &Value) -> bool {
    let Some(actual) = actual else { return false };
    let actual_obj = actual.as_object();
    if let Some(actual_string) = actual_obj.and_then(|o| o.get("S")).and_then(Value::as_str) {
        if let Some(expected_string) = expected
            .as_object()
            .and_then(|o| o.get("S"))
            .and_then(Value::as_str)
        {
            return actual_string.contains(expected_string);
        }
        return false;
    }
    for set in ["SS", "NS", "BS"] {
        let Some(actual_values) = actual_obj
            .and_then(|o| o.get(set))
            .and_then(Value::as_array)
        else {
            continue;
        };
        let actual_strings: Vec<&str> = actual_values.iter().filter_map(Value::as_str).collect();
        // expected as a one-element set of the same type, or a scalar element.
        if let Some(expected_values) = expected
            .as_object()
            .and_then(|o| o.get(set))
            .and_then(Value::as_array)
        {
            if expected_values.len() == 1 {
                if let Some(want) = expected_values[0].as_str() {
                    return actual_strings.contains(&want);
                }
            }
            return false;
        }
        let scalar_type = match set {
            "SS" => "S",
            "NS" => "N",
            "BS" => "B",
            _ => "",
        };
        if let Some(scalar) = expected
            .as_object()
            .and_then(|o| o.get(scalar_type))
            .and_then(Value::as_str)
        {
            return actual_strings.contains(&scalar);
        }
        return false;
    }
    if let Some(list) = actual_obj
        .and_then(|o| o.get("L"))
        .and_then(Value::as_array)
    {
        return list
            .iter()
            .any(|entry| attribute_values_equal(entry, expected));
    }
    false
}

// --- tokenizers (mirror expression_tokens.rs) ------------------------------

fn split_disjunctive_predicates(expression: &str) -> Result<Vec<String>, String> {
    let fields: Vec<&str> = expression.split_whitespace().collect();
    if fields.is_empty() {
        return Err("empty expression".to_string());
    }
    let mut parts = Vec::new();
    let mut current: Vec<&str> = Vec::new();
    for field in fields {
        if field.eq_ignore_ascii_case("OR") {
            if current.is_empty() {
                return Err("invalid OR expression".to_string());
            }
            parts.push(current.join(" "));
            current.clear();
            continue;
        }
        current.push(field);
    }
    if current.is_empty() {
        return Err("invalid OR expression".to_string());
    }
    parts.push(current.join(" "));
    Ok(parts)
}

fn split_conjunctive_predicates(expression: &str) -> Result<Vec<String>, String> {
    let fields: Vec<&str> = expression.split_whitespace().collect();
    if fields.is_empty() {
        return Err("empty expression".to_string());
    }
    let mut parts = Vec::new();
    let mut current: Vec<&str> = Vec::new();
    let mut between_needs_and = false;
    for field in fields {
        let upper = field.to_uppercase();
        if upper == "BETWEEN" {
            between_needs_and = true;
            current.push(field);
            continue;
        }
        if upper == "AND" && !between_needs_and {
            if current.is_empty() {
                return Err("invalid AND expression".to_string());
            }
            parts.push(current.join(" "));
            current.clear();
            continue;
        }
        if upper == "AND" && between_needs_and {
            between_needs_and = false;
        }
        current.push(field);
    }
    if current.is_empty() {
        return Err("invalid AND expression".to_string());
    }
    parts.push(current.join(" "));
    Ok(parts)
}

fn split_between_expression(expression: &str) -> Option<(String, String, String)> {
    let fields: Vec<&str> = expression.split_whitespace().collect();
    if fields.len() != 5
        || !fields[1].eq_ignore_ascii_case("BETWEEN")
        || !fields[3].eq_ignore_ascii_case("AND")
    {
        return None;
    }
    Some((
        fields[0].to_string(),
        fields[2].to_string(),
        fields[4].to_string(),
    ))
}

fn split_in_expression(expression: &str) -> Option<(String, Vec<String>)> {
    let (left, right) = cut(expression, " IN ").or_else(|| cut(expression, " in "))?;
    let right = right.trim();
    if !right.starts_with('(') || !right.ends_with(')') {
        return None;
    }
    let inner = &right[1..right.len() - 1];
    Some((left.trim().to_string(), split_comma_separated(inner)))
}

fn split_comparison_expression(expression: &str) -> Option<(String, String, String)> {
    for op in ["<=", ">=", "<>", "=", "<", ">"] {
        if let Some((left, right)) = cut(expression, op) {
            return Some((left, op.to_string(), right));
        }
    }
    None
}

fn split_comma_separated(value: &str) -> Vec<String> {
    let mut parts = Vec::new();
    let mut depth = 0i32;
    let mut start = 0usize;
    let bytes = value.as_bytes();
    for (i, &c) in bytes.iter().enumerate() {
        match c {
            b'(' => depth += 1,
            b')' => {
                if depth > 0 {
                    depth -= 1;
                }
            }
            b',' if depth == 0 => {
                parts.push(value[start..i].trim().to_string());
                start = i + 1;
            }
            _ => {}
        }
    }
    parts.push(value[start..].trim().to_string());
    parts
}

fn parse_function_call(expression: &str, name: &str) -> Option<String> {
    let prefix = format!("{name}(");
    if expression.starts_with(&prefix) && expression.ends_with(')') {
        Some(expression[prefix.len()..expression.len() - 1].to_string())
    } else {
        None
    }
}

/// legacy `strings.Cut`: split on the first occurrence of `sep`.
fn cut(s: &str, sep: &str) -> Option<(String, String)> {
    s.find(sep)
        .map(|idx| (s[..idx].to_string(), s[idx + sep.len()..].to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn names() -> Names {
        Names::new()
    }
    fn item(pairs: &[(&str, Value)]) -> Item {
        let mut m = Item::new();
        for (k, v) in pairs {
            m.insert((*k).to_string(), v.clone());
        }
        m
    }

    #[test]
    fn condition_attribute_exists() {
        let it = item(&[("pk", json!({"S": "x"}))]);
        check_condition("attribute_exists(pk)", &names(), &Values::new(), Some(&it)).unwrap();
        assert!(check_condition(
            "attribute_not_exists(pk)",
            &names(),
            &Values::new(),
            Some(&it)
        )
        .is_err());
    }

    #[test]
    fn condition_on_missing_item_uses_empty_candidate() {
        // attribute_not_exists should pass when the item does not exist.
        check_condition("attribute_not_exists(pk)", &names(), &Values::new(), None).unwrap();
    }

    #[test]
    fn comparison_numeric_and_string() {
        let it = item(&[("n", json!({"N": "5"})), ("s", json!({"S": "abc"}))]);
        let mut vals = Values::new();
        vals.insert(":big".to_string(), json!({"N": "10"}));
        vals.insert(":abc".to_string(), json!({"S": "abc"}));
        assert!(check_condition("n < :big", &names(), &vals, Some(&it)).is_ok());
        assert!(check_condition("s = :abc", &names(), &vals, Some(&it)).is_ok());
    }

    #[test]
    fn between_and_in_and_begins_with() {
        let it = item(&[("n", json!({"N": "5"})), ("s", json!({"S": "hello"}))]);
        let mut vals = Values::new();
        vals.insert(":lo".to_string(), json!({"N": "1"}));
        vals.insert(":hi".to_string(), json!({"N": "9"}));
        vals.insert(":a".to_string(), json!({"S": "zzz"}));
        vals.insert(":b".to_string(), json!({"S": "hello"}));
        vals.insert(":pre".to_string(), json!({"S": "he"}));
        check_condition("n BETWEEN :lo AND :hi", &names(), &vals, Some(&it)).unwrap();
        check_condition("s IN (:a, :b)", &names(), &vals, Some(&it)).unwrap();
        check_condition("begins_with(s, :pre)", &names(), &vals, Some(&it)).unwrap();
    }

    #[test]
    fn size_operand() {
        let it = item(&[("s", json!({"S": "hello"}))]);
        let mut vals = Values::new();
        vals.insert(":five".to_string(), json!({"N": "5"}));
        check_condition("size(s) = :five", &names(), &vals, Some(&it)).unwrap();
    }

    #[test]
    fn and_or_not_combinations() {
        let it = item(&[("a", json!({"S": "x"})), ("b", json!({"N": "2"}))]);
        let mut vals = Values::new();
        vals.insert(":x".to_string(), json!({"S": "x"}));
        vals.insert(":one".to_string(), json!({"N": "1"}));
        check_condition("a = :x AND b > :one", &names(), &vals, Some(&it)).unwrap();
        check_condition("a = :x OR b < :one", &names(), &vals, Some(&it)).unwrap();
        assert!(check_condition("NOT a = :x", &names(), &vals, Some(&it)).is_err());
    }

    #[test]
    fn missing_value_placeholder_errors() {
        let it = item(&[("n", json!({"N": "5"}))]);
        let err = check_condition("n < :missing", &names(), &Values::new(), Some(&it)).unwrap_err();
        assert_eq!(err, "missing expression attribute value :missing");
    }

    #[test]
    fn name_placeholders_resolve() {
        let it = item(&[("real", json!({"S": "v"}))]);
        let mut nm = names();
        nm.insert("#alias".to_string(), "real".to_string());
        let mut vals = Values::new();
        vals.insert(":v".to_string(), json!({"S": "v"}));
        check_condition("#alias = :v", &nm, &vals, Some(&it)).unwrap();
    }
}
