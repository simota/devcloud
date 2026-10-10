//! Attribute-value helpers: validation, type detection, comparison, equality,
//! size, key extraction, and projection.
//!
//! Mirrors the corresponding logic in
//! `internal/services/dynamodb/{item_handlers,expression_attributes,query_scan}.rs`.
//! Attribute values are `serde_json::Value` objects (`{"S": "x"}`, `{"N": "1"}`,
//! `{"M": {...}}`, …). Numbers (`N`) compare as arbitrary-precision rationals to
//! match legacy `big.Rat`.

use serde_json::Value;

use crate::model::{Item, TableDescription};

/// Validates a single attribute value, mirroring `validateAttributeValue`. The
/// `path` is woven into error messages exactly as legacy does.
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
/// values in key-schema order. Mirrors `itemKey`, except that `N` values use
/// their canonical spelling so numerically equal keys (`"1"`, `"1.0"`) address
/// the same item.
pub fn item_key(description: &TableDescription, values: &Item) -> Result<String, String> {
    let mut key_values: Vec<Value> = Vec::with_capacity(description.key_schema.len());
    for element in &description.key_schema {
        let value = values
            .get(&element.attribute_name)
            .ok_or_else(|| format!("missing key attribute {}", element.attribute_name))?;
        validate_attribute_value(value, &element.attribute_name)?;
        key_values.push(canonical_key_value(value));
    }
    Ok(crate::wire_json::marshal_string(&key_values))
}

/// A key attribute value with any `N` number in canonical spelling.
fn canonical_key_value(value: &Value) -> Value {
    if let Some(number) = value.get("N").and_then(Value::as_str) {
        if let Some(canonical) = crate::number::canonical_number_string(number) {
            return serde_json::json!({ "N": canonical });
        }
    }
    value.clone()
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

/// Applies an index projection then a `ProjectionExpression`, mirroring
/// `projectResultItem`.
pub fn project_result_item(
    description: &TableDescription,
    index_name: &str,
    value: &Item,
    expression: &str,
    names: &std::collections::BTreeMap<String, String>,
) -> Item {
    let projected = project_index_item(description, index_name, value);
    project_item(&projected, expression, names)
}

/// Restricts an item to the attributes an index projects, mirroring
/// `projectIndexItem`. A missing index, or an `ALL`/empty projection type,
/// returns the item unchanged.
pub fn project_index_item(description: &TableDescription, index_name: &str, value: &Item) -> Item {
    let Some((projection, schema)) = index_projection_for_name(description, index_name) else {
        return value.clone();
    };
    if projection.projection_type.is_empty() || projection.projection_type == "ALL" {
        return value.clone();
    }
    let mut allowed: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
    for element in &description.key_schema {
        allowed.insert(element.attribute_name.clone());
    }
    for element in &schema {
        allowed.insert(element.attribute_name.clone());
    }
    if projection.projection_type == "INCLUDE" {
        for name in &projection.non_key_attributes {
            allowed.insert(name.clone());
        }
    }
    let mut projected = Item::new();
    for name in &allowed {
        if let Some(attr) = value.get(name) {
            projected.insert(name.clone(), attr.clone());
        }
    }
    projected
}

/// Returns the projection + key schema for a named index, mirroring
/// `indexProjectionForName`.
fn index_projection_for_name(
    description: &TableDescription,
    index_name: &str,
) -> Option<(
    crate::model::IndexProjection,
    Vec<crate::model::KeySchemaElement>,
)> {
    if index_name.is_empty() {
        return None;
    }
    for index in &description.global_secondary_indexes {
        if index.index_name == index_name {
            return Some((index.projection.clone(), index.key_schema.clone()));
        }
    }
    for index in &description.local_secondary_indexes {
        if index.index_name == index_name {
            return Some((index.projection.clone(), index.key_schema.clone()));
        }
    }
    None
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

/// Deep value equality used by `=`/`<>`, IN and `contains`. Values of
/// different types are never equal; numbers (`N`, and `NS` members) compare
/// numerically so `"1"` equals `"1.0"`; sets compare as unordered sets; lists
/// and maps recurse.
pub fn attribute_values_equal(left: &Value, right: &Value) -> bool {
    let (Some(lo), Some(ro)) = (left.as_object(), right.as_object()) else {
        return left == right;
    };
    let (Some((lt, lv)), Some((rt, rv))) = (single_entry(lo), single_entry(ro)) else {
        return left == right;
    };
    if lt != rt {
        return false;
    }
    match lt.as_str() {
        "N" => match (lv.as_str(), rv.as_str()) {
            (Some(a), Some(b)) => numbers_equal(a, b),
            _ => lv == rv,
        },
        "NS" => sets_equal(lv, rv, numbers_equal),
        "SS" | "BS" => sets_equal(lv, rv, |a, b| a == b),
        "L" => match (lv.as_array(), rv.as_array()) {
            (Some(a), Some(b)) => {
                a.len() == b.len() && a.iter().zip(b).all(|(x, y)| attribute_values_equal(x, y))
            }
            _ => lv == rv,
        },
        "M" => match (lv.as_object(), rv.as_object()) {
            (Some(a), Some(b)) => {
                a.len() == b.len()
                    && a.iter()
                        .all(|(name, x)| b.get(name).is_some_and(|y| attribute_values_equal(x, y)))
            }
            _ => lv == rv,
        },
        _ => lv == rv,
    }
}

fn single_entry(obj: &serde_json::Map<String, Value>) -> Option<(&String, &Value)> {
    if obj.len() == 1 {
        obj.iter().next()
    } else {
        None
    }
}

/// Numeric equality of two number strings.
pub fn numbers_equal(left: &str, right: &str) -> bool {
    crate::number::compare_number_strings(left, right) == std::cmp::Ordering::Equal
}

/// Unordered equality of two string-set payloads under `eq`.
fn sets_equal(left: &Value, right: &Value, eq: impl Fn(&str, &str) -> bool) -> bool {
    let (Some(a), Some(b)) = (string_slice(left), string_slice(right)) else {
        return left == right;
    };
    a.len() == b.len()
        && a.iter().all(|x| b.iter().any(|y| eq(x, y)))
        && b.iter().all(|y| a.iter().any(|x| eq(x, y)))
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
                Some(rb) => compare_binary(lb, rb),
                None => attribute_type_name(left).cmp(attribute_type_name(right)),
            };
        }
    }
    let lj = crate::wire_json::marshal(left);
    let rj = crate::wire_json::marshal(right);
    lj.cmp(&rj).then(Ordering::Equal)
}

/// The DynamoDB size of an item in bytes: each attribute name's UTF-8 length
/// plus its value size (see [`attribute_value_size`]). Checked against the
/// 400 KB item limit.
pub fn item_size(item: &Item) -> usize {
    item.iter()
        .map(|(name, value)| name.len() + attribute_value_size(value))
        .sum()
}

/// The DynamoDB size of one attribute value: S as UTF-8 bytes, B as decoded
/// bytes, N as about one byte per two significant digits plus one, BOOL/NULL
/// as one byte, sets as the sum of their members, and lists/maps as 3 bytes
/// of overhead plus one byte per element and the elements (map entries also
/// count their names).
pub fn attribute_value_size(value: &Value) -> usize {
    let Some((kind, raw)) = value.as_object().and_then(single_entry) else {
        return crate::wire_json::marshal(value).len();
    };
    let strings = || {
        raw.as_array()
            .into_iter()
            .flatten()
            .filter_map(Value::as_str)
    };
    match kind.as_str() {
        "S" => raw.as_str().map_or(0, str::len),
        "B" => raw.as_str().map_or(0, binary_len),
        "N" => raw.as_str().map_or(0, number_size),
        "BOOL" | "NULL" => 1,
        "SS" => strings().map(str::len).sum(),
        "BS" => strings().map(binary_len).sum(),
        "NS" => strings().map(number_size).sum(),
        "L" => {
            3 + raw
                .as_array()
                .into_iter()
                .flatten()
                .map(|entry| 1 + attribute_value_size(entry))
                .sum::<usize>()
        }
        "M" => {
            3 + raw
                .as_object()
                .into_iter()
                .flatten()
                .map(|(name, entry)| 1 + name.len() + attribute_value_size(entry))
                .sum::<usize>()
        }
        _ => crate::wire_json::marshal(value).len(),
    }
}

fn number_size(number: &str) -> usize {
    crate::number::significant_digits(number).map_or(number.len(), |digits| digits.div_ceil(2) + 1)
}

/// Ordered comparison for the `<`/`<=`/`>`/`>=`/BETWEEN operators: defined
/// only between two values of the same scalar type (N numerically, S by
/// UTF-8 bytes, B by decoded bytes). `None` for mismatched or non-scalar
/// types, which DynamoDB evaluates as false.
pub fn compare_same_type(left: &Value, right: &Value) -> Option<std::cmp::Ordering> {
    let (lo, ro) = (left.as_object()?, right.as_object()?);
    let pair = |kind: &str| Some((lo.get(kind)?.as_str()?, ro.get(kind)?.as_str()?));
    if let Some((l, r)) = pair("N") {
        return Some(crate::number::compare_number_strings(l, r));
    }
    if let Some((l, r)) = pair("S") {
        return Some(l.cmp(r));
    }
    if let Some((l, r)) = pair("B") {
        return Some(compare_binary(l, r));
    }
    None
}

/// Compares two base64 `B` payloads by their decoded bytes (falling back to
/// the text when either is not valid base64).
fn compare_binary(left: &str, right: &str) -> std::cmp::Ordering {
    match (base64_decode(left), base64_decode(right)) {
        (Some(l), Some(r)) => l.cmp(&r),
        _ => left.cmp(right),
    }
}

/// The decoded byte length of a base64 `B` payload (the text length when it
/// is not valid base64).
pub fn binary_len(encoded: &str) -> usize {
    base64_decode(encoded).map_or(encoded.len(), |bytes| bytes.len())
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

/// Validates standard base64 (the encoding legacy `base64.StdEncoding` accepts),
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
    fn base64_validation_matches_legacy() {
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
        // legacy json.Marshal HTML-escapes `<`/`>` even in the internal key string.
        assert_eq!(key, "[{\"S\":\"u\\u003c1\\u003e\"},{\"N\":\"7\"}]");
    }

    #[test]
    fn numbers_are_equal_by_value() {
        assert!(attribute_values_equal(
            &json!({"N": "1"}),
            &json!({"N": "1.0"})
        ));
        assert!(attribute_values_equal(
            &json!({"N": "100"}),
            &json!({"N": "1e2"})
        ));
        assert!(!attribute_values_equal(
            &json!({"N": "1"}),
            &json!({"N": "1.5"})
        ));
        assert!(!attribute_values_equal(
            &json!({"N": "1"}),
            &json!({"S": "1"})
        ));
        assert!(attribute_values_equal(
            &json!({"NS": ["1", "2.50"]}),
            &json!({"NS": ["2.5", "1.0"]})
        ));
        assert!(attribute_values_equal(
            &json!({"SS": ["a", "b"]}),
            &json!({"SS": ["b", "a"]})
        ));
        assert!(attribute_values_equal(
            &json!({"M": {"n": {"N": "2"}}}),
            &json!({"M": {"n": {"N": "2.0"}}})
        ));
        assert!(attribute_values_equal(
            &json!({"L": [{"N": "3"}]}),
            &json!({"L": [{"N": "3.00"}]})
        ));
    }

    #[test]
    fn item_size_follows_dynamodb_rules() {
        let mut item = Item::new();
        item.insert("s".to_string(), json!({"S": "a<é"})); // 1 + 4
        item.insert("b".to_string(), json!({"B": "AAAA"})); // 1 + 3 decoded
        item.insert("n".to_string(), json!({"N": "12300"})); // 1 + (3 digits → 3)
        item.insert("t".to_string(), json!({"BOOL": true})); // 1 + 1
        item.insert("l".to_string(), json!({"L": [{"S": "xy"}]})); // 1 + 3 + 1 + 2
        item.insert("m".to_string(), json!({"M": {"k": {"NULL": true}}})); // 1 + 3 + 1 + 1 + 1
        item.insert("ss".to_string(), json!({"SS": ["ab", "c"]})); // 2 + 3
        assert_eq!(item_size(&item), 5 + 4 + 4 + 2 + 7 + 7 + 5);
    }

    #[test]
    fn item_key_uses_canonical_numbers() {
        let mut desc = test_description();
        desc.key_schema = vec![KeySchemaElement {
            attribute_name: "pk".to_string(),
            key_type: "HASH".to_string(),
        }];
        let key_of = |n: &str| {
            let mut item = Item::new();
            item.insert("pk".to_string(), json!({ "N": n }));
            item_key(&desc, &item).unwrap()
        };
        assert_eq!(key_of("1"), key_of("1.0"));
        assert_eq!(key_of("1"), key_of("1e0"));
        assert_eq!(key_of("1"), "[{\"N\":\"1\"}]");
        assert_ne!(key_of("1"), key_of("1.5"));
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
