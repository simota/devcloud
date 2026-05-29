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
            if parse_rational(number).is_none() {
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
                if parse_rational(number).is_none() {
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
                Some(rn) => match (parse_rational(ln), parse_rational(rn)) {
                    (Some(a), Some(b)) => a.cmp(&b),
                    _ => ln.cmp(rn),
                },
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

/// An arbitrary-precision rational, matching Go's `big.Rat.SetString` semantics
/// for the number formats DynamoDB accepts (decimal, optional sign, optional
/// fraction, optional exponent). Returns `None` when Go would reject the string.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Rational {
    negative: bool,
    /// numerator / denominator in lowest terms is not required for comparison;
    /// we compare via cross-multiplication on `i128`-free big integers.
    num: BigUint,
    den: BigUint,
}

impl Rational {
    fn cmp(&self, other: &Rational) -> std::cmp::Ordering {
        use std::cmp::Ordering;
        let lz = self.num.is_zero();
        let rz = other.num.is_zero();
        if lz && rz {
            return Ordering::Equal;
        }
        match (self.negative, other.negative) {
            (false, true) => return Ordering::Greater,
            (true, false) => return Ordering::Less,
            _ => {}
        }
        // Same sign (treat zero as positive): compare magnitudes a/b vs c/d via
        // a*d vs c*b.
        let left = self.num.mul(&other.den);
        let right = other.num.mul(&self.den);
        let mag = left.cmp(&right);
        if self.negative {
            mag.reverse()
        } else {
            mag
        }
    }
}

/// Parses a number string the way Go's `big.Rat.SetString` does (the subset
/// DynamoDB uses): optional sign, integer/decimal digits, optional `.fraction`,
/// optional `e`/`E` exponent. Pure base-10. Returns `None` on anything Go would
/// reject.
pub fn parse_rational(s: &str) -> Option<Rational> {
    let s = s.trim();
    if s.is_empty() {
        return None;
    }
    let bytes = s.as_bytes();
    let mut idx = 0;
    let mut negative = false;
    if bytes[idx] == b'+' || bytes[idx] == b'-' {
        negative = bytes[idx] == b'-';
        idx += 1;
    }
    let mut int_digits = String::new();
    while idx < bytes.len() && bytes[idx].is_ascii_digit() {
        int_digits.push(bytes[idx] as char);
        idx += 1;
    }
    let mut frac_digits = String::new();
    if idx < bytes.len() && bytes[idx] == b'.' {
        idx += 1;
        while idx < bytes.len() && bytes[idx].is_ascii_digit() {
            frac_digits.push(bytes[idx] as char);
            idx += 1;
        }
    }
    if int_digits.is_empty() && frac_digits.is_empty() {
        return None;
    }
    let mut exp: i64 = 0;
    if idx < bytes.len() && (bytes[idx] == b'e' || bytes[idx] == b'E') {
        idx += 1;
        let mut exp_neg = false;
        if idx < bytes.len() && (bytes[idx] == b'+' || bytes[idx] == b'-') {
            exp_neg = bytes[idx] == b'-';
            idx += 1;
        }
        let mut exp_digits = String::new();
        while idx < bytes.len() && bytes[idx].is_ascii_digit() {
            exp_digits.push(bytes[idx] as char);
            idx += 1;
        }
        if exp_digits.is_empty() {
            return None;
        }
        exp = exp_digits.parse::<i64>().ok()?;
        if exp_neg {
            exp = -exp;
        }
    }
    if idx != bytes.len() {
        return None; // trailing garbage — Go rejects
    }

    // Value = (int_digits frac_digits) * 10^(exp - frac_len).
    let mut mantissa = int_digits;
    let frac_len = frac_digits.len() as i64;
    mantissa.push_str(&frac_digits);
    let mantissa = mantissa.trim_start_matches('0');
    let num_digits = if mantissa.is_empty() { "0" } else { mantissa };
    let total_exp = exp - frac_len;

    let mut num = BigUint::from_decimal(num_digits)?;
    let mut den = BigUint::one();
    if total_exp >= 0 {
        num = num.mul(&BigUint::pow10(total_exp as u64));
    } else {
        den = BigUint::pow10((-total_exp) as u64);
    }
    Some(Rational {
        negative: negative && !num.is_zero(),
        num,
        den,
    })
}

// --- minimal big unsigned integer (base 1e9 limbs) -------------------------

/// A tiny arbitrary-precision unsigned integer, just enough for `N` comparison.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BigUint {
    /// Little-endian base-1_000_000_000 limbs; empty == zero.
    limbs: Vec<u32>,
}

const LIMB_BASE: u64 = 1_000_000_000;

impl BigUint {
    fn zero() -> Self {
        BigUint { limbs: Vec::new() }
    }
    fn one() -> Self {
        BigUint { limbs: vec![1] }
    }
    fn is_zero(&self) -> bool {
        self.limbs.iter().all(|&l| l == 0)
    }
    fn normalize(mut self) -> Self {
        while self.limbs.last() == Some(&0) {
            self.limbs.pop();
        }
        self
    }

    fn from_decimal(s: &str) -> Option<Self> {
        if s.is_empty() || !s.bytes().all(|b| b.is_ascii_digit()) {
            return None;
        }
        let mut n = BigUint::zero();
        let ten = BigUint { limbs: vec![10] };
        for b in s.bytes() {
            n = n.mul(&ten).add_small(u32::from(b - b'0'));
        }
        Some(n.normalize())
    }

    fn pow10(mut e: u64) -> Self {
        let mut result = BigUint::one();
        let ten = BigUint { limbs: vec![10] };
        let mut base = ten;
        while e > 0 {
            if e & 1 == 1 {
                result = result.mul(&base);
            }
            e >>= 1;
            if e > 0 {
                base = base.mul(&base);
            }
        }
        result.normalize()
    }

    fn add_small(&self, add: u32) -> Self {
        let mut limbs = self.limbs.clone();
        let mut carry = add as u64;
        let mut i = 0;
        while carry > 0 {
            if i == limbs.len() {
                limbs.push(0);
            }
            let cur = limbs[i] as u64 + carry;
            limbs[i] = (cur % LIMB_BASE) as u32;
            carry = cur / LIMB_BASE;
            i += 1;
        }
        BigUint { limbs }.normalize()
    }

    fn mul(&self, other: &BigUint) -> Self {
        if self.is_zero() || other.is_zero() {
            return BigUint::zero();
        }
        let mut out = vec![0u64; self.limbs.len() + other.limbs.len()];
        for (i, &a) in self.limbs.iter().enumerate() {
            let mut carry = 0u64;
            for (j, &b) in other.limbs.iter().enumerate() {
                let cur = out[i + j] + a as u64 * b as u64 + carry;
                out[i + j] = cur % LIMB_BASE;
                carry = cur / LIMB_BASE;
            }
            out[i + other.limbs.len()] += carry;
        }
        BigUint {
            limbs: out.into_iter().map(|l| l as u32).collect(),
        }
        .normalize()
    }

    fn cmp(&self, other: &BigUint) -> std::cmp::Ordering {
        use std::cmp::Ordering;
        let a = self.clone().normalize();
        let b = other.clone().normalize();
        match a.limbs.len().cmp(&b.limbs.len()) {
            Ordering::Equal => {}
            ord => return ord,
        }
        for i in (0..a.limbs.len()).rev() {
            match a.limbs[i].cmp(&b.limbs[i]) {
                Ordering::Equal => {}
                ord => return ord,
            }
        }
        Ordering::Equal
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
    fn rational_compares_like_big_rat() {
        use std::cmp::Ordering;
        assert_eq!(
            parse_rational("3.5")
                .unwrap()
                .cmp(&parse_rational("3.50").unwrap()),
            Ordering::Equal
        );
        assert_eq!(
            parse_rational("10")
                .unwrap()
                .cmp(&parse_rational("9").unwrap()),
            Ordering::Greater
        );
        assert_eq!(
            parse_rational("-1")
                .unwrap()
                .cmp(&parse_rational("0").unwrap()),
            Ordering::Less
        );
        assert_eq!(
            parse_rational("1e2")
                .unwrap()
                .cmp(&parse_rational("100").unwrap()),
            Ordering::Equal
        );
        assert_eq!(
            parse_rational("0.1")
                .unwrap()
                .cmp(&parse_rational("0.2").unwrap()),
            Ordering::Less
        );
    }

    #[test]
    fn rational_rejects_garbage() {
        assert!(parse_rational("abc").is_none());
        assert!(parse_rational("1.2.3").is_none());
        assert!(parse_rational("").is_none());
        assert!(parse_rational("1e").is_none());
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
