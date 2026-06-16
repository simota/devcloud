//! UpdateExpression evaluation: SET / REMOVE / ADD / DELETE.
//!
//! Mirrors `internal/services/dynamodb/{update_expressions,expression_tokens}.rs`.
//! Operates in place on a mutable item (the merged key + existing attributes),
//! exactly as legacy `applyUpdateExpression` does. Number arithmetic delegates to
//! `crate::number` (legacy `big.Rat` semantics).

use std::collections::BTreeMap;

use serde_json::{json, Value};

use crate::attribute::resolve_attribute_name;
use crate::model::Item;
use crate::number;

type Names = BTreeMap<String, String>;
type Values = BTreeMap<String, Value>;

/// Applies an UpdateExpression to `target` in place. Mirrors
/// `applyUpdateExpression`.
pub fn apply_update_expression(
    target: &mut Item,
    expression: &str,
    names: &Names,
    values: &Values,
) -> Result<(), String> {
    let expression = expression.trim();
    if expression.is_empty() {
        return Err("update expression is required".to_string());
    }
    for clause in split_update_clauses(expression)? {
        match clause.keyword.as_str() {
            "SET" => {
                for assignment in split_comma_separated(&clause.body) {
                    let (name_token, value_token) = cut(assignment.trim(), "=")
                        .ok_or_else(|| "invalid SET assignment".to_string())?;
                    let attr = resolve_attribute_name(name_token.trim(), names);
                    let value = evaluate_update_value(target, value_token.trim(), names, values)?;
                    target.insert(attr, value);
                }
            }
            "REMOVE" => {
                for removal in split_comma_separated(&clause.body) {
                    let attr = resolve_attribute_name(removal.trim(), names);
                    if attr.is_empty() {
                        return Err("invalid REMOVE path".to_string());
                    }
                    target.remove(&attr);
                }
            }
            "ADD" => {
                for addition in split_comma_separated(&clause.body) {
                    let fields: Vec<&str> = addition.split_whitespace().collect();
                    if fields.len() != 2 {
                        return Err("invalid ADD assignment".to_string());
                    }
                    let attr = resolve_attribute_name(fields[0], names);
                    if attr.is_empty() {
                        return Err("invalid ADD path".to_string());
                    }
                    let value = values.get(fields[1]).ok_or_else(|| {
                        format!("missing expression attribute value {}", fields[1])
                    })?;
                    let updated = add_attribute_value(target.get(&attr), value)?;
                    target.insert(attr, updated);
                }
            }
            "DELETE" => {
                for deletion in split_comma_separated(&clause.body) {
                    let fields: Vec<&str> = deletion.split_whitespace().collect();
                    if fields.len() != 2 {
                        return Err("invalid DELETE assignment".to_string());
                    }
                    let attr = resolve_attribute_name(fields[0], names);
                    if attr.is_empty() {
                        return Err("invalid DELETE path".to_string());
                    }
                    let value = values.get(fields[1]).ok_or_else(|| {
                        format!("missing expression attribute value {}", fields[1])
                    })?;
                    let (updated, remove) = delete_attribute_value(target.get(&attr), value)?;
                    if remove {
                        target.remove(&attr);
                    } else if let Some(v) = updated {
                        target.insert(attr, v);
                    }
                }
            }
            other => return Err(format!("unsupported update expression clause {other}")),
        }
    }
    Ok(())
}

fn evaluate_update_value(
    target: &Item,
    expression: &str,
    names: &Names,
    values: &Values,
) -> Result<Value, String> {
    if let Some((left, operator, right)) = split_arithmetic_update_expression(expression) {
        let left_value = evaluate_update_value(target, &left, names, values)?;
        let mut right_value = evaluate_update_value(target, &right, names, values)?;
        if operator == "-" {
            right_value = negate_number_attribute(&right_value)?;
        }
        return add_attribute_value(Some(&left_value), &right_value);
    }
    if let Some(body) = parse_function_call(expression, "if_not_exists") {
        let args = split_comma_separated(&body);
        if args.len() != 2 {
            return Err("invalid if_not_exists expression".to_string());
        }
        let attr = resolve_attribute_name(args[0].trim(), names);
        if let Some(current) = target.get(&attr) {
            return Ok(current.clone());
        }
        return evaluate_update_value(target, args[1].trim(), names, values);
    }
    if let Some(body) = parse_function_call(expression, "list_append") {
        let args = split_comma_separated(&body);
        if args.len() != 2 {
            return Err("invalid list_append expression".to_string());
        }
        let left_value = evaluate_update_value(target, args[0].trim(), names, values)?;
        let right_value = evaluate_update_value(target, args[1].trim(), names, values)?;
        return append_list_attribute_values(&left_value, &right_value);
    }
    if let Some(value) = values.get(expression) {
        return Ok(value.clone());
    }
    let attr = resolve_attribute_name(expression, names);
    if let Some(current) = target.get(&attr) {
        return Ok(current.clone());
    }
    Err(format!("missing expression attribute value {expression}"))
}

/// `addAttributeValue`: numeric add, or set union for SS/NS/BS. `current == None`
/// means the attribute is absent.
pub fn add_attribute_value(current: Option<&Value>, increment: &Value) -> Result<Value, String> {
    let inc_obj = increment.as_object();
    if let Some(number) = inc_obj.and_then(|o| o.get("N")).and_then(Value::as_str) {
        let Some(current) = current else {
            return Ok(increment.clone());
        };
        let current_number = current
            .as_object()
            .and_then(|o| o.get("N"))
            .and_then(Value::as_str)
            .ok_or_else(|| "ADD number requires existing number attribute".to_string())?;
        let sum = number::add_number_strings(current_number, number)?;
        return Ok(json!({ "N": sum }));
    }
    for set_type in ["SS", "NS", "BS"] {
        let Some(values_to_add) = string_slice(increment, set_type) else {
            continue;
        };
        let Some(current) = current else {
            return Ok(increment.clone());
        };
        let current_values = string_slice(current, set_type)
            .ok_or_else(|| format!("ADD {set_type} requires existing {set_type} attribute"))?;
        let union = union_strings(&current_values, &values_to_add);
        return Ok(json!({ set_type: union }));
    }
    Err("ADD supports N, SS, NS, and BS values".to_string())
}

fn negate_number_attribute(value: &Value) -> Result<Value, String> {
    let number = value
        .as_object()
        .and_then(|o| o.get("N"))
        .and_then(Value::as_str)
        .ok_or_else(|| "subtraction requires number attributes".to_string())?;
    let negated = number::negate_number_string(number)?;
    Ok(json!({ "N": negated }))
}

/// `deleteAttributeValue`: removes set members. Returns `(updated, remove)`:
/// `remove == true` means delete the whole attribute; `updated == None &&
/// !remove` means leave the attribute unchanged (absent current).
pub fn delete_attribute_value(
    current: Option<&Value>,
    decrement: &Value,
) -> Result<(Option<Value>, bool), String> {
    for set_type in ["SS", "NS", "BS"] {
        let Some(values_to_delete) = string_slice(decrement, set_type) else {
            continue;
        };
        if values_to_delete.is_empty() {
            return Err("DELETE set value must not be empty".to_string());
        }
        let Some(current) = current else {
            return Ok((None, false));
        };
        let current_values = string_slice(current, set_type)
            .ok_or_else(|| format!("DELETE {set_type} requires existing {set_type} attribute"))?;
        let remaining = subtract_strings(&current_values, &values_to_delete);
        if remaining.is_empty() {
            return Ok((None, true));
        }
        return Ok((Some(json!({ set_type: remaining })), false));
    }
    Err("DELETE supports SS, NS, and BS values".to_string())
}

fn append_list_attribute_values(left: &Value, right: &Value) -> Result<Value, String> {
    let left_entries = left
        .as_object()
        .and_then(|o| o.get("L"))
        .and_then(Value::as_array);
    let right_entries = right
        .as_object()
        .and_then(|o| o.get("L"))
        .and_then(Value::as_array);
    match (left_entries, right_entries) {
        (Some(l), Some(r)) => {
            let mut combined = Vec::with_capacity(l.len() + r.len());
            combined.extend(l.iter().cloned());
            combined.extend(r.iter().cloned());
            Ok(json!({ "L": combined }))
        }
        _ => Err("list_append requires list attributes".to_string()),
    }
}

// --- string-set helpers ----------------------------------------------------

fn string_slice(value: &Value, key: &str) -> Option<Vec<String>> {
    let arr = value.as_object()?.get(key)?.as_array()?;
    let mut out = Vec::with_capacity(arr.len());
    for entry in arr {
        out.push(entry.as_str()?.to_string());
    }
    Some(out)
}

fn union_strings(left: &[String], right: &[String]) -> Vec<String> {
    let mut seen = std::collections::BTreeSet::new();
    let mut result = Vec::with_capacity(left.len() + right.len());
    for value in left.iter().chain(right.iter()) {
        if seen.insert(value.clone()) {
            result.push(value.clone());
        }
    }
    result
}

fn subtract_strings(left: &[String], right: &[String]) -> Vec<String> {
    let remove: std::collections::BTreeSet<&String> = right.iter().collect();
    left.iter()
        .filter(|v| !remove.contains(v))
        .cloned()
        .collect()
}

// --- tokenizers (mirror expression_tokens.rs) ------------------------------

struct UpdateClause {
    keyword: String,
    body: String,
}

fn split_update_clauses(expression: &str) -> Result<Vec<UpdateClause>, String> {
    let upper = expression.to_uppercase();
    let mut starts: Vec<(String, usize)> = Vec::new();
    for keyword in ["SET", "REMOVE", "ADD", "DELETE"] {
        let mut offset = 0;
        while let Some(rel) = upper[offset..].find(keyword) {
            let absolute = offset + rel;
            if is_update_clause_boundary(&upper, absolute, keyword.len()) {
                starts.push((keyword.to_string(), absolute));
            }
            offset = absolute + keyword.len();
        }
    }
    if starts.is_empty() {
        return Err("update expression must include SET, REMOVE, ADD, or DELETE".to_string());
    }
    starts.sort_by_key(|(_, index)| *index);
    let mut clauses = Vec::with_capacity(starts.len());
    for i in 0..starts.len() {
        let (keyword, index) = &starts[i];
        let next = if i + 1 < starts.len() {
            starts[i + 1].1
        } else {
            expression.len()
        };
        let body = expression[index + keyword.len()..next].trim().to_string();
        if body.is_empty() {
            return Err(format!("{keyword} update expression clause is empty"));
        }
        clauses.push(UpdateClause {
            keyword: keyword.clone(),
            body,
        });
    }
    Ok(clauses)
}

fn is_update_clause_boundary(expression: &str, index: usize, keyword_len: usize) -> bool {
    let bytes = expression.as_bytes();
    let before_ok = index == 0 || bytes[index - 1] == b' ';
    let after = index + keyword_len;
    let after_ok = after < bytes.len() && bytes[after] == b' ';
    before_ok && after_ok
}

fn split_arithmetic_update_expression(expression: &str) -> Option<(String, String, String)> {
    let mut depth = 0i32;
    for (index, ch) in expression.char_indices() {
        match ch {
            '(' => depth += 1,
            ')' => {
                if depth > 0 {
                    depth -= 1;
                }
            }
            '+' | '-' if depth == 0 => {
                return Some((
                    expression[..index].trim().to_string(),
                    ch.to_string(),
                    expression[index + 1..].trim().to_string(),
                ));
            }
            _ => {}
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

fn cut(s: &str, sep: &str) -> Option<(String, String)> {
    s.find(sep)
        .map(|idx| (s[..idx].to_string(), s[idx + sep.len()..].to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

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
    fn set_with_arithmetic_and_value() {
        let mut target = item(&[("count", json!({"N": "10"}))]);
        let mut vals = Values::new();
        vals.insert(":inc".to_string(), json!({"N": "5"}));
        apply_update_expression(&mut target, "SET count = count + :inc", &names(), &vals).unwrap();
        assert_eq!(target["count"], json!({"N": "15"}));
    }

    #[test]
    fn set_if_not_exists_and_list_append() {
        let mut target = item(&[("list", json!({"L": [{"S": "x"}]}))]);
        let mut vals = Values::new();
        vals.insert(":def".to_string(), json!({"S": "new"}));
        vals.insert(":more".to_string(), json!({"L": [{"S": "y"}]}));
        apply_update_expression(
            &mut target,
            "SET label = if_not_exists(label, :def), list = list_append(list, :more)",
            &names(),
            &vals,
        )
        .unwrap();
        assert_eq!(target["label"], json!({"S": "new"}));
        assert_eq!(target["list"], json!({"L": [{"S": "x"}, {"S": "y"}]}));
    }

    #[test]
    fn remove_clause() {
        let mut target = item(&[("a", json!({"S": "x"})), ("b", json!({"N": "1"}))]);
        apply_update_expression(&mut target, "REMOVE b", &names(), &Values::new()).unwrap();
        assert!(!target.contains_key("b"));
    }

    #[test]
    fn add_number_to_absent_and_existing() {
        let mut target = Item::new();
        let mut vals = Values::new();
        vals.insert(":s".to_string(), json!({"N": "3"}));
        apply_update_expression(&mut target, "ADD score :s", &names(), &vals).unwrap();
        assert_eq!(target["score"], json!({"N": "3"}));
        apply_update_expression(&mut target, "ADD score :s", &names(), &vals).unwrap();
        assert_eq!(target["score"], json!({"N": "6"}));
    }

    #[test]
    fn add_to_set_unions() {
        let mut target = item(&[("tags", json!({"SS": ["a", "b"]}))]);
        let mut vals = Values::new();
        vals.insert(":t".to_string(), json!({"SS": ["b", "c"]}));
        apply_update_expression(&mut target, "ADD tags :t", &names(), &vals).unwrap();
        assert_eq!(target["tags"], json!({"SS": ["a", "b", "c"]}));
    }

    #[test]
    fn delete_from_set_and_whole_attribute() {
        let mut target = item(&[("colors", json!({"SS": ["red", "green", "blue"]}))]);
        let mut vals = Values::new();
        vals.insert(":c".to_string(), json!({"SS": ["green"]}));
        apply_update_expression(&mut target, "DELETE colors :c", &names(), &vals).unwrap();
        assert_eq!(target["colors"], json!({"SS": ["red", "blue"]}));

        // Deleting all members removes the attribute.
        vals.insert(":all".to_string(), json!({"SS": ["red", "blue"]}));
        apply_update_expression(&mut target, "DELETE colors :all", &names(), &vals).unwrap();
        assert!(!target.contains_key("colors"));
    }

    #[test]
    fn subtraction_operator() {
        let mut target = item(&[("count", json!({"N": "10"}))]);
        let mut vals = Values::new();
        vals.insert(":d".to_string(), json!({"N": "2.5"}));
        apply_update_expression(&mut target, "SET count = count - :d", &names(), &vals).unwrap();
        assert_eq!(target["count"], json!({"N": "7.5"}));
    }

    #[test]
    fn multi_clause_expression() {
        let mut target = item(&[("count", json!({"N": "1"})), ("old", json!({"S": "x"}))]);
        let mut vals = Values::new();
        vals.insert(":inc".to_string(), json!({"N": "2"}));
        apply_update_expression(
            &mut target,
            "SET count = count + :inc REMOVE old",
            &names(),
            &vals,
        )
        .unwrap();
        assert_eq!(target["count"], json!({"N": "3"}));
        assert!(!target.contains_key("old"));
    }

    #[test]
    fn empty_expression_errors() {
        let mut target = Item::new();
        assert_eq!(
            apply_update_expression(&mut target, "  ", &names(), &Values::new()).unwrap_err(),
            "update expression is required"
        );
    }

    #[test]
    fn name_placeholders_resolve() {
        let mut target = item(&[("real", json!({"N": "1"}))]);
        let mut nm = names();
        nm.insert("#a".to_string(), "real".to_string());
        let mut vals = Values::new();
        vals.insert(":one".to_string(), json!({"N": "1"}));
        apply_update_expression(&mut target, "SET #a = #a + :one", &nm, &vals).unwrap();
        assert_eq!(target["real"], json!({"N": "2"}));
    }
}
