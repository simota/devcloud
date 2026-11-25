use base64::{engine::general_purpose::STANDARD, Engine};
use ciborium::value::Value as Cbor;
use serde_json::{json, Value};

use super::{invalid, read_cbor, CborResult};

pub(super) fn decode(value: &Cbor) -> CborResult<Value> {
    Ok(match value {
        Cbor::Text(text) => json!({"S": text}),
        Cbor::Bytes(bytes) => json!({"B": STANDARD.encode(bytes)}),
        Cbor::Bool(flag) => json!({"BOOL": flag}),
        Cbor::Null => json!({"NULL": true}),
        Cbor::Array(values) => {
            json!({"L": values.iter().map(decode).collect::<CborResult<Vec<_>>>()?})
        }
        Cbor::Map(entries) => {
            let mut fields = serde_json::Map::new();
            for (name, field) in entries {
                let name = name.as_text().ok_or_else(invalid)?;
                if fields.insert(name.to_owned(), decode(field)?).is_some() {
                    return Err(invalid());
                }
            }
            json!({"M": fields})
        }
        Cbor::Tag(tag @ 3321..=3323, values) => {
            let (set, element) = set_types(*tag);
            let values = values.as_array().ok_or_else(invalid)?;
            let values = values
                .iter()
                .map(|value| decode(value)?.get(element).cloned().ok_or_else(invalid))
                .collect::<CborResult<Vec<_>>>()?;
            json!({set: values})
        }
        _ => json!({"N": decode_number(value)?}),
    })
}

pub(super) fn encode(value: &Value) -> CborResult<Cbor> {
    let (kind, raw) = value
        .as_object()
        .and_then(|obj| obj.iter().next())
        .ok_or_else(invalid)?;
    match kind.as_str() {
        "S" => Ok(Cbor::Text(text(raw)?.to_owned())),
        "B" => Ok(Cbor::Bytes(binary(text(raw)?)?)),
        "N" => encode_number(text(raw)?),
        "BOOL" => Ok(Cbor::Bool(raw.as_bool().ok_or_else(invalid)?)),
        "NULL" => Ok(Cbor::Null),
        "L" => Ok(Cbor::Array(
            raw.as_array()
                .ok_or_else(invalid)?
                .iter()
                .map(encode)
                .collect::<CborResult<_>>()?,
        )),
        "M" => Ok(Cbor::Map(
            raw.as_object()
                .ok_or_else(invalid)?
                .iter()
                .map(|(name, value)| Ok((Cbor::Text(name.clone()), encode(value)?)))
                .collect::<CborResult<_>>()?,
        )),
        "SS" | "NS" | "BS" => {
            let tag = match kind.as_str() {
                "SS" => 3321,
                "NS" => 3322,
                _ => 3323,
            };
            let (_, element) = set_types(tag);
            let values = raw
                .as_array()
                .ok_or_else(invalid)?
                .iter()
                .map(|raw| match element {
                    "S" => Ok(Cbor::Text(text(raw)?.to_owned())),
                    "N" => encode_number(text(raw)?),
                    _ => Ok(Cbor::Bytes(binary(text(raw)?)?)),
                })
                .collect::<CborResult<_>>()?;
            Ok(Cbor::Tag(tag, Box::new(Cbor::Array(values))))
        }
        _ => Err(invalid()),
    }
}

fn set_types(tag: u64) -> (&'static str, &'static str) {
    match tag {
        3321 => ("SS", "S"),
        3322 => ("NS", "N"),
        _ => ("BS", "B"),
    }
}

fn text(value: &Value) -> CborResult<&str> {
    value.as_str().ok_or_else(invalid)
}

fn binary(text: &str) -> CborResult<Vec<u8>> {
    STANDARD
        .decode(text.replace(['\r', '\n'], ""))
        .map_err(|_| invalid())
}

fn decode_integer(value: &Cbor) -> CborResult<i128> {
    if let Some(integer) = value.as_integer() {
        return Ok(i128::from(integer));
    }
    let (tag @ (2 | 3), Cbor::Bytes(bytes)) = value.as_tag().ok_or_else(invalid)? else {
        return Err(invalid());
    };
    let magnitude = bytes
        .iter()
        .try_fold(0i128, |magnitude, &byte| {
            magnitude.checked_mul(256)?.checked_add(i128::from(byte))
        })
        .ok_or_else(invalid)?;
    Ok(if tag == 2 { magnitude } else { -1 - magnitude })
}

fn decode_number(value: &Cbor) -> CborResult<String> {
    let number = match value {
        Cbor::Float(float) if float.is_finite() => float.to_string(),
        Cbor::Tag(4, parts) => {
            let [exponent, mantissa] = parts.as_array().ok_or_else(invalid)?.as_slice() else {
                return Err(invalid());
            };
            let exponent = i32::try_from(decode_integer(exponent)?).map_err(|_| invalid())?;
            decimal_text(decode_integer(mantissa)?, exponent)?
        }
        _ => decode_integer(value)?.to_string(),
    };
    let (mantissa, exponent) = decimal_parts(&number)?;
    decimal_text(mantissa, exponent)
}

fn encode_number(number: &str) -> CborResult<Cbor> {
    let (mantissa, exponent) = decimal_parts(number)?;
    if exponent == 0 {
        return Ok(Cbor::from(mantissa));
    }
    Ok(Cbor::Tag(
        4,
        Box::new(Cbor::Array(vec![
            Cbor::from(exponent),
            Cbor::from(mantissa),
        ])),
    ))
}

fn decimal_parts(text: &str) -> CborResult<(i128, i32)> {
    let (significand, exponent) = match text.split_once(['e', 'E']) {
        Some((significand, exponent)) => {
            (significand, exponent.parse::<i32>().map_err(|_| invalid())?)
        }
        None => (text, 0),
    };
    let (integer, fraction) = significand.split_once('.').unwrap_or((significand, ""));
    let digits = format!("{integer}{fraction}");
    let exponent = exponent
        .checked_sub(i32::try_from(fraction.len()).map_err(|_| invalid())?)
        .ok_or_else(invalid)?;
    let trimmed = digits.trim_end_matches('0');
    if trimmed.is_empty() || trimmed == "-" || trimmed == "+" {
        if digits.ends_with('0') {
            return Ok((0, 0));
        }
        return Err(invalid());
    }
    let exponent = exponent
        .checked_add(i32::try_from(digits.len() - trimmed.len()).map_err(|_| invalid())?)
        .ok_or_else(invalid)?;
    let mantissa = trimmed.parse::<i128>().map_err(|_| invalid())?;
    let precision = trimmed.trim_start_matches(['-', '+', '0']).len();
    let adjusted = i32::try_from(precision)
        .map_err(|_| invalid())?
        .checked_add(exponent)
        .ok_or_else(invalid)?;
    if precision > 38 || !(-129..=126).contains(&adjusted) {
        return Err(invalid());
    }
    // Prefer a CBOR integer when the whole number fits.
    if exponent > 0 && i32::try_from(precision).map_err(|_| invalid())? + exponent <= 38 {
        return Ok((mantissa * 10i128.pow(exponent as u32), 0));
    }
    Ok((mantissa, exponent))
}

fn decimal_text(mantissa: i128, exponent: i32) -> CborResult<String> {
    if !(-167..=125).contains(&exponent) {
        return Err(invalid());
    }
    let digits = mantissa.unsigned_abs().to_string();
    let mut result = if mantissa < 0 {
        "-".to_owned()
    } else {
        String::new()
    };
    let point = digits.len() as i32 + exponent;
    if exponent >= 0 {
        result.push_str(&digits);
        result.extend(std::iter::repeat_n('0', exponent as usize));
    } else if point > 0 {
        let (integer, fraction) = digits.split_at(point as usize);
        result.push_str(integer);
        result.push('.');
        result.push_str(fraction);
    } else {
        result.push_str("0.");
        result.extend(std::iter::repeat_n('0', (-point) as usize));
        result.push_str(&digits);
    }
    Ok(result)
}

pub(super) fn decode_key(kind: &str, bytes: &mut &[u8], raw: bool) -> CborResult<Value> {
    match (kind, raw) {
        ("S", true) => {
            Ok(json!({"S": std::str::from_utf8(std::mem::take(bytes)).map_err(|_| invalid())?}))
        }
        ("B", true) => Ok(json!({"B": STANDARD.encode(std::mem::take(bytes))})),
        _ => {
            let value = decode(&read_cbor(bytes).map_err(|_| invalid())?)?;
            if value.get(kind).is_none() || value.get(kind).and_then(Value::as_str) == Some("") {
                return Err(invalid());
            }
            Ok(value)
        }
    }
}

pub(super) fn validate_key_size(kind: &str, value: &Value, max: usize) -> CborResult<()> {
    let text = text(&value[kind])?;
    let size = if kind == "B" {
        binary(text)?.len()
    } else {
        text.len()
    };
    if size == 0 || size > max {
        return Err(invalid());
    }
    Ok(())
}

pub(super) fn key_values_equal(stored: &Value, key: &Value) -> bool {
    if let (Some(stored), Some(key)) = (stored["N"].as_str(), key["N"].as_str()) {
        return decimal_parts(stored)
            .is_ok_and(|stored| decimal_parts(key).is_ok_and(|key| stored == key));
    }
    stored == key
}

// DAX stores numeric sort keys in LexDecimal order. An exponent header is
// followed by 10-bit base-1000 digits and a terminator giving the last width.
pub(super) fn decode_sort_number(bytes: &[u8]) -> CborResult<Value> {
    let (&header, mut digits) = bytes.split_first().ok_or_else(invalid)?;
    let negative = header < 0x7f;
    let exponent = match header {
        0 | 0xff => return Err(invalid()),
        0x7f | 0x80 if digits.is_empty() => return Ok(json!({"N": "0"})),
        0x7f | 0x80 => return Err(invalid()),
        1 | 0x7e | 0x81 | 0xfe => {
            let (prefix, rest) = digits.split_at_checked(4).ok_or_else(invalid)?;
            digits = rest;
            let encoded = i32::from_be_bytes(prefix.try_into().map_err(|_| invalid())?);
            encoded ^ if negative { i32::MAX } else { i32::MIN }
        }
        0x82..=0xfd => i32::from(header) - 0xc0,
        _ => 0x3f - i32::from(header),
    };
    if !(-129..=126).contains(&exponent) || digits.len() > 18 {
        return Err(invalid());
    }
    let mut groups = Vec::new();
    let mut bits = 0;
    let mut buffer = 0u32;
    for (index, &byte) in digits.iter().enumerate() {
        buffer = (buffer << 8) | u32::from(byte);
        bits += 8;
        if bits < 10 {
            continue;
        }
        bits -= 10;
        let mut group = (buffer >> bits) & 1023;
        if negative {
            group = 1023 - group;
        }
        if group <= 2 {
            let width = group + 1;
            let padding = 10i128.pow(3 - width);
            let last: i128 = groups.pop().ok_or_else(invalid)?;
            if index + 1 != digits.len() || buffer & ((1 << bits) - 1) != 0 || last % padding != 0 {
                return Err(invalid());
            }
            let precision = groups.len() as i32 * 3 + width as i32;
            if precision > 38 {
                return Err(invalid());
            }
            let prefix = groups
                .into_iter()
                .fold(0i128, |sum, digit| sum * 1000 + digit);
            let mantissa = prefix * 10i128.pow(width) + last / padding;
            if mantissa == 0 {
                return Err(invalid());
            }
            let mantissa = if negative { -mantissa } else { mantissa };
            let (mantissa, exponent) =
                decimal_parts(&decimal_text(mantissa, exponent - precision)?)?;
            return Ok(json!({"N": decimal_text(mantissa, exponent)?}));
        }
        if !(12..=1011).contains(&group) || (groups.is_empty() && group < 112) || groups.len() == 13
        {
            return Err(invalid());
        }
        groups.push(i128::from(group - 12));
    }
    Err(invalid())
}
