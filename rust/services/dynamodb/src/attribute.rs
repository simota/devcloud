//! Attribute-value helpers: validation, type detection, comparison, equality,
//! size, key extraction, and projection.
//!
//! Mirrors the corresponding logic in
//! `internal/services/dynamodb/{item_handlers,expression_attributes,query_scan}.go`.
//! Attribute values are `serde_json::Value` objects (`{"S": "x"}`, `{"N": "1"}`,
//! `{"M": {...}}`, …). Numbers (`N`) compare as arbitrary-precision rationals to
//! match Go's `big.Rat`.

use serde_json::Value;

use crate::model::{Item, TableDescription};

/// Validates a single attribute value, mirroring `validateAttributeValue`. The
/// `path` is woven into error messages exactly as Go does.
pub fn validate_attribute_value(value: &Value, path: &str) -> Result<(), String> {
    let obj = match value.as_object() {
        Some(obj) if obj.len() == 1 => obj,
        _ => {
            return Err(format!(
                "attribute {path} must contain exactly one AttributeValue type"
            ))
        }
    };
    let (kind, raw) = obj.iter().next().unwrap();
    match kind.as_str() {
        "S" => {
            if !raw.is_string() {
                return Err(format!("attribute {path} {kind} value must be a string"));
            }
        }
        "B" => {
            let binary = raw
                .as_str()
                .ok_or_else(|| format!("attribute {path} B value must be a string"))?;
            if base64_decode(binary).is_none() {
                return Err(format!("attribute {path} B value must be base64 encoded"));
            }
        }
        "N" => {
            let number = raw
                .as_str()
                .ok_or_else(|| format!("attribute {path} N value must be a string"))?;
            if !crate::number::is_valid_number(number) {
                return Err(format!("attribute {path} N value must be a valid number"));
            }
        }
        "BOOL" => {
            if !raw.is_boolean() {
                return Err(format!("attribute {path} BOOL value must be a boolean"));
            }
        }
        "NULL" => {
            if raw.as_bool() != Some(true) {
                return Err(format!("attribute {path} NULL value must be true"));
            }
        }
        "M" => {
            let entries = raw
                .as_object()
                .ok_or_else(|| format!("attribute {path} M value must be a map"))?;
            for (name, nested) in entries {
                if !nested.is_object() {
                    return Err(format!(
                        "attribute {path}.{name} must be an AttributeValue object"
                    ));
                }
                validate_attribute_value(nested, &format!("{path}.{name}"))?;
            }
        }
        "L" => {
            let entries = raw
                .as_array()
                .ok_or_else(|| format!("attribute {path} L value must be a list"))?;
            for (index, nested) in entries.iter().enumerate() {
                if !nested.is_object() {
                    return Err(format!(
                        "attribute {path}[{index}] must be an AttributeValue object"
                    ));
                }
                validate_attribute_value(nested, &format!("{path}[{index}]"))?;
            }
        }
        "SS" | "BS" => {
            let values = string_slice(raw)
                .ok_or_else(|| format!("attribute {path} {kind} value must be a string list"))?;
            if values.is_empty() {
                return Err(format!("attribute {path} {kind} value must not be empty"));
            }
            if has_duplicate(&values) {
                return Err(format!(
                    "attribute {path} {kind} value must not contain duplicates"
                ));
            }
            if kind == "BS" {
                for binary in &values {
                    if base64_decode(binary).is_none() {
                        return Err(format!(
                            "attribute {path} BS value must contain base64 encoded strings"
                        ));
                    }
                }
            }
        }
        "NS" => {
            let values = string_slice(raw)
                .ok_or_else(|| format!("attribute {path} NS value must be a string list"))?;
            if values.is_empty() {
                return Err(format!("attribute {path} NS value must not be empty"));
            }
            if has_duplicate(&values) {
                return Err(format!(
                    "attribute {path} NS value must not contain duplicates"
                ));
            }
            for number in &values {
                if !crate::number::is_valid_number(number) {
                    return Err(format!(
                        "attribute {path} NS value must contain valid numbers"
                    ));
                }
            }
        }
        other => {
            return Err(format!(
                "attribute {path} has unsupported AttributeValue type {other}"
            ))
        }
    }
    Ok(())
}

/// Validates every attribute in an item, mirroring `validateItemAttributeValues`.
pub fn validate_item_attribute_values(item: &Item) -> Result<(), String> {
    for (name, attr) in item {
        if name.is_empty() {
            return Err("attribute name is required".to_string());
        }
        validate_attribute_value(attr, name)?;
    }
    Ok(())
}

/// The DynamoDB type name of a value (first matching key), mirroring
/// `attributeTypeName`.
pub fn attribute_type_name(value: &Value) -> &'static str {
    const ORDER: [&str; 10] = ["S", "N", "B", "BOOL", "NULL", "M", "L", "SS", "NS", "BS"];
    if let Some(obj) = value.as_object() {
        for name in ORDER {
            if obj.contains_key(name) {
                return name;
            }
        }
    }
    ""
}

/// Builds the internal item-key string: `json.Marshal` of the key attribute
/// values in key-schema order. Mirrors `itemKey`.
pub fn item_key(description: &TableDescription, values: &Item) -> Result<String, String> {
    let mut key_values: Vec<&Value> = Vec::with_capacity(description.key_schema.len());
    for element in &description.key_schema {
        let value = values
            .get(&element.attribute_name)
            .ok_or_else(|| format!("missing key attribute {}", element.attribute_name))?;
        validate_attribute_value(value, &element.attribute_name)?;
        key_values.push(value);
    }
    Ok(crate::go_json::marshal_string(&key_values))
}

/// Extracts the primary-key attributes from an item, mirroring `extractKey`.
pub fn extract_key(description: &TableDescription, value: &Item) -> Result<Item, String> {
    let mut key = Item::new();
    for element in &description.key_schema {
        let attr = value
            .get(&element.attribute_name)
            .ok_or_else(|| format!("missing key attribute {}", element.attribute_name))?;
        key.insert(element.attribute_name.clone(), attr.clone());
    }
    Ok(key)
}

/// Applies a `ProjectionExpression` to an item, mirroring `projectItem`.
pub fn project_item(
    value: &Item,
    expression: &str,
    names: &std::collections::BTreeMap<String, String>,
) -> Item {
    let expression = expression.trim();
    if expression.is_empty() {
        return value.clone();
    }
    let mut projected = Item::new();
    for token in expression.split(',') {
        let name = resolve_attribute_name(token.trim(), names);
        if let Some(attr) = value.get(&name) {
            projected.insert(name, attr.clone());
        }
    }
    projected
}

/// Resolves a `#name` placeholder against the expression-attribute-names map,
/// mirroring `resolveAttributeName`.
pub fn resolve_attribute_name(
    token: &str,
    names: &std::collections::BTreeMap<String, String>,
) -> String {
    if token.starts_with('#') {
        if let Some(value) = names.get(token) {
            return value.clone();
        }
    }
    token.to_string()
}

/// Deep value equality used by `=`/`<>` and IN, mirroring
/// `attributeValuesEqual` (a structural compare; `serde_json::Value` equality
/// already matches Go's `reflect.DeepEqual` + JSON fallback for these shapes).
pub fn attribute_values_equal(left: &Value, right: &Value) -> bool {
    left == right
}

/// Ordered comparison of two attribute values, mirroring `compareAttributeValues`
/// (numbers as rationals; strings/binary lexicographically; otherwise by JSON).
pub fn compare_attribute_values(left: &Value, right: &Value) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    let (lo, ro) = (left.as_object(), right.as_object());
    if let (Some(lo), Some(ro)) = (lo, ro) {
        if let Some(ln) = lo.get("N").and_then(Value::as_str) {
            return match ro.get("N").and_then(Value::as_str) {
                Some(rn) => crate::number::compare_number_strings(ln, rn),
                None => attribute_type_name(left).cmp(attribute_type_name(right)),
            };
        }
        if let Some(ls) = lo.get("S").and_then(Value::as_str) {
            return match ro.get("S").and_then(Value::as_str) {
                Some(rs) => ls.cmp(rs),
                None => attribute_type_name(left).cmp(attribute_type_name(right)),
            };
        }
        if let Some(lb) = lo.get("B").and_then(Value::as_str) {
            return match ro.get("B").and_then(Value::as_str) {
                Some(rb) => lb.cmp(rb),
                None => attribute_type_name(left).cmp(attribute_type_name(right)),
            };
        }
    }
    let lj = crate::go_json::marshal(left);
    let rj = crate::go_json::marshal(right);
    lj.cmp(&rj).then(Ordering::Equal)
}

// --- helpers ---------------------------------------------------------------

fn string_slice(raw: &Value) -> Option<Vec<String>> {
    let arr = raw.as_array()?;
    let mut out = Vec::with_capacity(arr.len());
    for entry in arr {
        out.push(entry.as_str()?.to_string());
    }
    Some(out)
}

fn has_duplicate(values: &[String]) -> bool {
    let mut seen = std::collections::BTreeSet::new();
    for v in values {
        if !seen.insert(v) {
            return true;
        }
    }
    false
}

/// Validates standard base64 (the encoding Go's `base64.StdEncoding` accepts),
/// returning the decoded bytes on success. Implemented locally to avoid a
/// dependency.
fn base64_decode(input: &str) -> Option<Vec<u8>> {
    const PAD: u8 = b'=';
    let bytes = input.as_bytes();
    if !bytes.len().is_multiple_of(4) {
        return None;
    }
    let mut out = Vec::with_capacity(bytes.len() / 4 * 3);
    let mut i = 0;
    while i < bytes.len() {
        let chunk = &bytes[i..i + 4];
        let pads = chunk.iter().rev().take_while(|&&b| b == PAD).count();
        if pads > 2 {
            return None;
        }
        let mut acc = 0u32;
        for (j, &c) in chunk.iter().enumerate() {
            let v = if c == PAD {
                if j < 4 - pads {
                    return None;
                }
                0
            } else {
                base64_value(c)?
            };
            acc = (acc << 6) | v as u32;
        }
        out.push((acc >> 16) as u8);
        if pads < 2 {
            out.push((acc >> 8) as u8);
        }
        if pads < 1 {
            out.push(acc as u8);
        }
        i += 4;
    }
    Some(out)
}

fn base64_value(c: u8) -> Option<u8> {
    match c {
        b'A'..=b'Z' => Some(c - b'A'),
        b'a'..=b'z' => Some(c - b'a' + 26),
        b'0'..=b'9' => Some(c - b'0' + 52),
        b'+' => Some(62),
        b'/' => Some(63),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::KeySchemaElement;
    use serde_json::json;

    fn names() -> std::collections::BTreeMap<String, String> {
        std::collections::BTreeMap::new()
    }

    #[test]
    fn compare_numbers_uses_rational_order() {
        use std::cmp::Ordering;
        assert_eq!(
            compare_attribute_values(&json!({"N": "9"}), &json!({"N": "10"})),
            Ordering::Less
        );
    }

    #[test]
    fn type_name_follows_go_order() {
        assert_eq!(attribute_type_name(&json!({"S": "x"})), "S");
        assert_eq!(attribute_type_name(&json!({"BOOL": true})), "BOOL");
    }

    #[test]
    fn validate_rejects_multi_key() {
        let err = validate_attribute_value(&json!({"S": "a", "N": "1"}), "x").unwrap_err();
        assert_eq!(
            err,
            "attribute x must contain exactly one AttributeValue type"
        );
    }

    #[test]
    fn validate_accepts_nested_and_sets() {
        validate_attribute_value(&json!({"M": {"a": {"BOOL": true}}}), "m").unwrap();
        validate_attribute_value(&json!({"SS": ["x", "y"]}), "s").unwrap();
        validate_attribute_value(&json!({"L": [{"S": "x"}, {"N": "1"}]}), "l").unwrap();
    }

    #[test]
    fn validate_rejects_set_duplicates_and_bad_numbers() {
        assert_eq!(
            validate_attribute_value(&json!({"SS": ["x", "x"]}), "s").unwrap_err(),
            "attribute s SS value must not contain duplicates"
        );
        assert_eq!(
            validate_attribute_value(&json!({"NS": ["1", "z"]}), "n").unwrap_err(),
            "attribute n NS value must contain valid numbers"
        );
    }

    #[test]
    fn base64_validation_matches_go() {
        validate_attribute_value(&json!({"B": "aGVsbG8="}), "b").unwrap();
        assert_eq!(
            validate_attribute_value(&json!({"B": "not base64!"}), "b").unwrap_err(),
            "attribute b B value must be base64 encoded"
        );
    }

    #[test]
    fn item_key_marshals_key_schema_order() {
        let mut desc = TableDescription {
            ..test_description()
        };
        desc.key_schema = vec![
            KeySchemaElement {
                attribute_name: "pk".to_string(),
                key_type: "HASH".to_string(),
            },
            KeySchemaElement {
                attribute_name: "sk".to_string(),
                key_type: "RANGE".to_string(),
            },
        ];
        let mut item = Item::new();
        item.insert("pk".to_string(), json!({"S": "u<1>"}));
        item.insert("sk".to_string(), json!({"N": "7"}));
        item.insert("other".to_string(), json!({"S": "ignored"}));
        let key = item_key(&desc, &item).unwrap();
        // Go's json.Marshal HTML-escapes `<`/`>` even in the internal key string.
        assert_eq!(key, "[{\"S\":\"u\\u003c1\\u003e\"},{\"N\":\"7\"}]");
    }

    #[test]
    fn project_item_selects_named_attributes() {
        let mut item = Item::new();
        item.insert("name".to_string(), json!({"S": "Ann"}));
        item.insert("sk".to_string(), json!({"N": "7"}));
        item.insert("pk".to_string(), json!({"S": "x"}));
        let mut nm = names();
        nm.insert("#n".to_string(), "name".to_string());
        let projected = project_item(&item, "#n, sk", &nm);
        assert_eq!(projected.len(), 2);
        assert!(projected.contains_key("name") && projected.contains_key("sk"));
    }

    fn test_description() -> TableDescription {
        TableDescription {
            attribute_definitions: vec![],
            billing_mode_summary: None,
            creation_date_time: 0,
            global_secondary_indexes: vec![],
            item_count: 0,
            key_schema: vec![],
            latest_stream_arn: String::new(),
            latest_stream_label: String::new(),
            local_secondary_indexes: vec![],
            stream_specification: None,
            table_arn: String::new(),
            table_name: String::new(),
            table_size_bytes: 0,
            table_status: String::new(),
            time_to_live_description: None,
        }
    }
}
