//! Request decoding, resource-id validation, query-string parsing, and the
//! list-pagination parameters — ports of the helpers in
//! `internal/services/bigquery/routes.rs` and `tabledata_handlers.rs`.

use serde::de::DeserializeOwned;

use crate::model::{RoutineResource, TableFieldSchema, TableSchema};
use crate::responses::default_string;

/// legacy `validateResourceID`: ASCII letters, digits, `_`, `-` only.
pub fn validate_resource_id(id: &str, kind: &str) -> Result<(), String> {
    if id.is_empty() {
        return Err(format!("{kind} id is required"));
    }
    for c in id.chars() {
        if c.is_ascii_alphanumeric() || c == '_' || c == '-' {
            continue;
        }
        return Err(format!("{kind} id contains unsupported character"));
    }
    Ok(())
}

/// legacy `validateTableSchema` (tabledata_handlers.rs).
pub fn validate_table_schema(schema: &TableSchema) -> Result<(), String> {
    for field in &schema.fields {
        validate_table_field(field)?;
    }
    Ok(())
}

/// legacy `validateTableField`.
fn validate_table_field(field: &TableFieldSchema) -> Result<(), String> {
    validate_resource_id(&field.name, "field")?;
    let field_type = default_string(field.field_type.clone(), "STRING").to_uppercase();
    match field_type.as_str() {
        "STRING" | "BYTES" | "INTEGER" | "INT64" | "FLOAT" | "FLOAT64" | "NUMERIC"
        | "BIGNUMERIC" | "BOOLEAN" | "BOOL" | "TIMESTAMP" | "DATE" | "TIME" | "DATETIME"
        | "GEOGRAPHY" | "JSON" | "RECORD" | "STRUCT" => {}
        _ => return Err(format!("unsupported field type {:?}", field.field_type)),
    }
    let mode = default_string(field.mode.clone(), "NULLABLE").to_uppercase();
    match mode.as_str() {
        "NULLABLE" | "REQUIRED" | "REPEATED" => {}
        _ => return Err(format!("unsupported field mode {:?}", field.mode)),
    }
    if field_type == "RECORD" || field_type == "STRUCT" {
        if field.fields.is_empty() {
            return Err(format!(
                "record field {:?} requires nested fields",
                field.name
            ));
        }
        for nested in &field.fields {
            validate_table_field(nested)?;
        }
    }
    Ok(())
}

/// legacy `validateRoutineResource` (routes.rs).
pub fn validate_routine_resource(routine: &RoutineResource) -> Result<(), String> {
    validate_resource_id(&routine.routine_reference.routine_id, "routine")?;
    if routine.routine_type.trim().is_empty() {
        return Err("routineType is required".to_string());
    }
    match routine.routine_type.to_uppercase().as_str() {
        "SCALAR_FUNCTION" | "PROCEDURE" | "TABLE_VALUED_FUNCTION" => Ok(()),
        _ => Err(format!(
            "unsupported routineType {:?}",
            routine.routine_type
        )),
    }
}

/// Decodes one JSON value the way the legacy handlers do:
/// `json.NewDecoder(http.MaxBytesReader(...)).Decode(&request)` with `io.EOF`
/// ignored — an empty body yields the zero value, trailing garbage after the
/// first value is ignored, and a body larger than `max_bytes` is an error.
pub fn decode_body<T: DeserializeOwned + Default>(body: &[u8], max_bytes: i64) -> Result<T, ()> {
    if body.len() as i64 > max_bytes {
        return Err(());
    }
    let mut stream = serde_json::Deserializer::from_slice(body).into_iter::<T>();
    match stream.next() {
        None => Ok(T::default()), // io.EOF → zero value
        Some(Ok(value)) => Ok(value),
        Some(Err(_)) => Err(()),
    }
}

/// Parsed URL query parameters with legacy `url.Values.Get` semantics (first value
/// wins, missing → "").
#[derive(Debug, Default)]
pub struct Query {
    params: Vec<(String, String)>,
}

impl Query {
    /// Parses a raw query string the way legacy `url.ParseQuery` does: split on
    /// `&`, reject segments containing `;`, percent+plus decode, and keep
    /// going past bad pairs.
    pub fn parse(raw: &str) -> Query {
        let mut params = Vec::new();
        for segment in raw.split('&') {
            if segment.is_empty() || segment.contains(';') {
                continue;
            }
            let (key, value) = match segment.split_once('=') {
                Some((k, v)) => (k, v),
                None => (segment, ""),
            };
            let (Some(key), Some(value)) = (query_unescape(key), query_unescape(value)) else {
                continue;
            };
            params.push((key, value));
        }
        Query { params }
    }

    pub fn get(&self, key: &str) -> &str {
        self.params
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v.as_str())
            .unwrap_or("")
    }
}

/// legacy `url.QueryUnescape`: `+` → space, `%XX` → byte; invalid escapes fail.
fn query_unescape(input: &str) -> Option<String> {
    let bytes = input.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'+' => out.push(b' '),
            b'%' => {
                if i + 2 >= bytes.len() {
                    return None;
                }
                let hi = hex_digit(bytes[i + 1])?;
                let lo = hex_digit(bytes[i + 2])?;
                out.push(hi * 16 + lo);
                i += 2;
            }
            b => out.push(b),
        }
        i += 1;
    }
    String::from_utf8(out).ok()
}

fn hex_digit(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

/// legacy `url.PathEscape`: percent-encodes a path segment. Unescaped set per
/// RFC 3986 §3.3 as implemented by legacy `shouldEscape(c, encodePathSegment)`.
pub fn path_escape(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    for &b in input.as_bytes() {
        let unescaped = b.is_ascii_alphanumeric()
            || matches!(
                b,
                b'-' | b'_' | b'.' | b'~' | b'$' | b'&' | b'+' | b':' | b'=' | b'@'
            );
        if unescaped {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

/// legacy `rowOffsetFromRequest`: `pageToken` (fallback `startIndex`), trimmed;
/// empty → 0; non-integer or negative → "invalid page token".
pub fn row_offset_from_request(query: &Query) -> Result<i64, String> {
    let mut token = query.get("pageToken").trim();
    if token.is_empty() {
        token = query.get("startIndex").trim();
    }
    if token.is_empty() {
        return Ok(0);
    }
    match token.parse::<i64>() {
        Ok(offset) if offset >= 0 => Ok(offset),
        _ => Err("invalid page token".to_string()),
    }
}

/// legacy `maxResultsFromRequest`: empty → 100; non-integer or negative →
/// "maxResults must be positive"; 0 → 100; capped at 10000.
pub fn max_results_from_request(query: &Query) -> Result<i64, String> {
    let raw = query.get("maxResults").trim();
    if raw.is_empty() {
        return Ok(100);
    }
    let max_results = match raw.parse::<i64>() {
        Ok(value) if value >= 0 => value,
        _ => return Err("maxResults must be positive".to_string()),
    };
    match max_results {
        0 => Ok(100),
        v if v > 10_000 => Ok(10_000),
        v => Ok(v),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resource_id_rules_match_legacy() {
        assert!(validate_resource_id("analytics", "dataset").is_ok());
        assert!(validate_resource_id("A-1_b", "dataset").is_ok());
        assert_eq!(
            validate_resource_id("", "dataset").unwrap_err(),
            "dataset id is required"
        );
        assert_eq!(
            validate_resource_id("../secret", "dataset").unwrap_err(),
            "dataset id contains unsupported character"
        );
    }

    #[test]
    fn decode_body_matches_legacy_decoder_semantics() {
        #[derive(Default, serde::Deserialize)]
        struct T {
            #[serde(default)]
            a: i64,
        }
        // Empty body → zero value (io.EOF ignored).
        assert_eq!(decode_body::<T>(b"", 100).unwrap().a, 0);
        // First value decoded; trailing bytes ignored.
        assert_eq!(decode_body::<T>(b"{\"a\":7} trailing", 100).unwrap().a, 7);
        // Invalid JSON → error.
        assert!(decode_body::<T>(b"{", 100).is_err());
        // Over the limit → error.
        assert!(decode_body::<T>(b"{\"a\":7}", 3).is_err());
    }

    #[test]
    fn pagination_params_match_legacy() {
        let q = Query::parse("maxResults=2&pageToken=4");
        assert_eq!(max_results_from_request(&q).unwrap(), 2);
        assert_eq!(row_offset_from_request(&q).unwrap(), 4);

        let q = Query::parse("");
        assert_eq!(max_results_from_request(&q).unwrap(), 100);
        assert_eq!(row_offset_from_request(&q).unwrap(), 0);

        let q = Query::parse("maxResults=0&startIndex=3");
        assert_eq!(max_results_from_request(&q).unwrap(), 100);
        assert_eq!(row_offset_from_request(&q).unwrap(), 3);

        let q = Query::parse("maxResults=20001");
        assert_eq!(max_results_from_request(&q).unwrap(), 10_000);

        let q = Query::parse("maxResults=-1&pageToken=x");
        assert!(max_results_from_request(&q).is_err());
        assert!(row_offset_from_request(&q).is_err());
    }

    #[test]
    fn path_escape_matches_legacy() {
        assert_eq!(path_escape("local-project"), "local-project");
        assert_eq!(path_escape("a b/c"), "a%20b%2Fc");
        assert_eq!(path_escape("a:b@c"), "a:b@c");
    }
}
