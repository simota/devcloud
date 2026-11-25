//! Event source mapping `FilterCriteria` evaluation (EventBridge-style
//! content filtering, as Lambda applies it to polled records).
//!
//! A record is kept when any filter's pattern matches. A pattern is a JSON
//! object whose keys address record fields; a leaf is an array of matchers,
//! any of which may match: a literal (string, number, boolean, `null`),
//! `{"prefix": s}`, `{"suffix": s}`, `{"equals-ignore-case": s}`,
//! `{"anything-but": v | [v, ...]}`, `{"exists": bool}`, or
//! `{"numeric": [op, n (, op, n)]}`. An array field in the record matches
//! when any of its elements does. For SQS the message `body` is matched as
//! JSON when it parses as a JSON object, and as a plain string otherwise.

use serde_json::Value;

/// Parsed `FilterCriteria.Filters[].Pattern` documents (already validated
/// as JSON objects when the mapping was stored).
pub(crate) fn patterns(filter_criteria: Option<&Value>) -> Vec<Value> {
    filter_criteria
        .and_then(|fc| fc.get("Filters"))
        .and_then(Value::as_array)
        .map(|filters| {
            filters
                .iter()
                .filter_map(|f| f.get("Pattern").and_then(Value::as_str))
                .filter_map(|p| serde_json::from_str::<Value>(p).ok())
                .collect()
        })
        .unwrap_or_default()
}

/// Whether `record` passes the filters (no filters: everything passes).
pub(crate) fn passes(patterns: &[Value], record: &Value) -> bool {
    patterns.is_empty() || patterns.iter().any(|p| matches_object(p, Some(record)))
}

fn matches_object(pattern: &Value, value: Option<&Value>) -> bool {
    let Some(fields) = pattern.as_object() else {
        return false;
    };
    fields.iter().all(|(key, sub)| {
        let field = value.and_then(|v| v.get(key));
        match sub {
            Value::Object(_) => matches_object(sub, field),
            Value::Array(matchers) => matchers.iter().any(|m| matches_leaf(m, field)),
            _ => false,
        }
    })
}

/// One matcher against a (possibly missing, possibly array) field.
fn matches_leaf(matcher: &Value, field: Option<&Value>) -> bool {
    if let Some(exists) = matcher.get("exists").and_then(Value::as_bool) {
        return field.is_some() == exists;
    }
    let Some(field) = field else {
        return false;
    };
    match field {
        Value::Array(items) => items.iter().any(|item| matches_scalar(matcher, item)),
        _ => matches_scalar(matcher, field),
    }
}

fn matches_scalar(matcher: &Value, value: &Value) -> bool {
    match matcher {
        Value::Object(m) => {
            if let Some(p) = m.get("prefix").and_then(Value::as_str) {
                return value.as_str().is_some_and(|s| s.starts_with(p));
            }
            if let Some(p) = m.get("suffix").and_then(Value::as_str) {
                return value.as_str().is_some_and(|s| s.ends_with(p));
            }
            if let Some(p) = m.get("equals-ignore-case").and_then(Value::as_str) {
                return value.as_str().is_some_and(|s| s.eq_ignore_ascii_case(p));
            }
            if let Some(not) = m.get("anything-but") {
                return match not {
                    Value::Array(list) => !list.iter().any(|v| literal_eq(v, value)),
                    Value::Object(inner) => match inner.get("prefix").and_then(Value::as_str) {
                        Some(p) => value.as_str().is_some_and(|s| !s.starts_with(p)),
                        None => false,
                    },
                    other => !literal_eq(other, value),
                };
            }
            if let Some(cmp) = m.get("numeric").and_then(Value::as_array) {
                return value.as_f64().is_some_and(|n| numeric_matches(cmp, n));
            }
            false
        }
        literal => literal_eq(literal, value),
    }
}

fn literal_eq(literal: &Value, value: &Value) -> bool {
    match (literal, value) {
        (Value::Number(a), Value::Number(b)) => a.as_f64() == b.as_f64(),
        _ => literal == value,
    }
}

/// `["<", 10]`, `[">=", 1, "<", 5]`, `["=", 3]`.
fn numeric_matches(cmp: &[Value], n: f64) -> bool {
    if cmp.is_empty() || !cmp.len().is_multiple_of(2) {
        return false;
    }
    cmp.chunks(2).all(|pair| {
        let (Some(op), Some(rhs)) = (pair[0].as_str(), pair[1].as_f64()) else {
            return false;
        };
        match op {
            "<" => n < rhs,
            "<=" => n <= rhs,
            ">" => n > rhs,
            ">=" => n >= rhs,
            "=" => n == rhs,
            _ => false,
        }
    })
}

/// The view of an SQS record that patterns are matched against: `body`
/// replaced by its parsed JSON object when it is one.
pub(crate) fn sqs_match_view(record: &Value) -> Value {
    let mut view = record.clone();
    if let Some(body) = record.get("body").and_then(Value::as_str) {
        if let Ok(parsed @ Value::Object(_)) = serde_json::from_str::<Value>(body) {
            view["body"] = parsed;
        }
    }
    view
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn pat(p: Value) -> Vec<Value> {
        patterns(Some(&json!({ "Filters": [{ "Pattern": p.to_string() }] })))
    }

    #[test]
    fn no_filters_pass_everything() {
        assert!(passes(&patterns(None), &json!({})));
        assert!(passes(&patterns(Some(&json!({}))), &json!({})));
    }

    #[test]
    fn literals_nesting_and_arrays() {
        let p = pat(json!({ "body": { "kind": ["order", "refund"], "n": [3] } }));
        assert!(passes(
            &p,
            &json!({ "body": { "kind": "order", "n": 3.0 } })
        ));
        assert!(!passes(&p, &json!({ "body": { "kind": "other", "n": 3 } })));
        assert!(!passes(&p, &json!({ "body": { "kind": "order" } })));
        // An array field matches when any element does.
        let tags = pat(json!({ "tags": ["red"] }));
        assert!(passes(&tags, &json!({ "tags": ["blue", "red"] })));
        assert!(!passes(&tags, &json!({ "tags": ["blue"] })));
    }

    #[test]
    fn operators() {
        let p = pat(json!({ "k": [{ "prefix": "ab" }] }));
        assert!(passes(&p, &json!({ "k": "abc" })));
        assert!(!passes(&p, &json!({ "k": "xabc" })));
        let p = pat(json!({ "k": [{ "suffix": ".png" }] }));
        assert!(passes(&p, &json!({ "k": "a.png" })));
        let p = pat(json!({ "k": [{ "equals-ignore-case": "YES" }] }));
        assert!(passes(&p, &json!({ "k": "yes" })));
        let p = pat(json!({ "k": [{ "anything-but": ["a", "b"] }] }));
        assert!(passes(&p, &json!({ "k": "c" })));
        assert!(!passes(&p, &json!({ "k": "a" })));
        let p = pat(json!({ "k": [{ "anything-but": { "prefix": "tmp" } }] }));
        assert!(passes(&p, &json!({ "k": "final" })));
        assert!(!passes(&p, &json!({ "k": "tmp1" })));
        let p = pat(json!({ "k": [{ "numeric": [">=", 10, "<", 20] }] }));
        assert!(passes(&p, &json!({ "k": 10 })));
        assert!(!passes(&p, &json!({ "k": 20 })));
        assert!(!passes(&p, &json!({ "k": "15" })));
    }

    #[test]
    fn exists() {
        let yes = pat(json!({ "k": [{ "exists": true }] }));
        assert!(passes(&yes, &json!({ "k": 1 })));
        assert!(!passes(&yes, &json!({})));
        let no = pat(json!({ "k": [{ "exists": false }] }));
        assert!(passes(&no, &json!({})));
        assert!(!passes(&no, &json!({ "k": 1 })));
    }

    #[test]
    fn any_filter_may_match() {
        let p = patterns(Some(&json!({ "Filters": [
            { "Pattern": "{\"a\":[1]}" },
            { "Pattern": "{\"b\":[2]}" },
        ]})));
        assert!(passes(&p, &json!({ "b": 2 })));
        assert!(!passes(&p, &json!({ "a": 2, "b": 1 })));
    }

    #[test]
    fn sqs_bodies_match_as_json_when_they_are_objects() {
        let p = pat(json!({ "body": { "ok": [true] } }));
        let rec = json!({ "body": "{\"ok\":true}" });
        assert!(passes(&p, &sqs_match_view(&rec)));
        let plain = pat(json!({ "body": [{ "prefix": "hello" }] }));
        assert!(passes(
            &plain,
            &sqs_match_view(&json!({ "body": "hello world" }))
        ));
    }
}
