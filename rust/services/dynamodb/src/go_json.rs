//! Go-compatible JSON encoding.
//!
//! The Go DynamoDB service encodes JSON with `encoding/json`, which differs from
//! serde_json defaults in ways we must reproduce byte-for-byte:
//!
//!  1. **HTML escaping** — Go escapes `<`, `>`, `&` (and the U+2028 / U+2029 line
//!     separators) inside string values as `<`, `>`, `&`,
//!     ` `, ` `. serde_json leaves them raw. These bytes never appear
//!     structurally in JSON (only inside string literals), so a post-pass byte
//!     rewrite over already-valid JSON is exact.
//!  2. **Trailing newline** — `json.Encoder.Encode` appends a single `\n`.
//!  3. **Key ordering** — Go sorts *map* keys but keeps *struct* fields in
//!     declaration order. serde's derive reproduces both natively: structs keep
//!     field order, and `BTreeMap` (used for every map shape, including
//!     `serde_json::Value::Object`, since `preserve_order` is off) sorts keys.
//!     So plain [`to_vec`] matches Go for both `state.json` (struct-ordered top
//!     level, sorted maps within) and the struct-shaped HTTP responses.

use serde::Serialize;

/// Rewrites `<`, `>`, `&` and the U+2028/U+2029 line separators to their `\uXXXX`
/// escapes, matching Go's `encoding/json` HTML escaping. Operates on valid JSON:
/// these bytes only occur inside string literals, so the rewrite is safe.
fn html_escape(input: Vec<u8>) -> Vec<u8> {
    if !input
        .iter()
        .any(|&b| b == b'<' || b == b'>' || b == b'&' || b == 0xE2)
    {
        return input;
    }
    let mut out = Vec::with_capacity(input.len());
    let mut i = 0;
    while i < input.len() {
        match input[i] {
            b'<' => out.extend_from_slice(b"\\u003c"),
            b'>' => out.extend_from_slice(b"\\u003e"),
            b'&' => out.extend_from_slice(b"\\u0026"),
            0xE2 if i + 2 < input.len() && input[i + 1] == 0x80 && input[i + 2] == 0xA8 => {
                out.extend_from_slice(b"\\u2028");
                i += 3;
                continue;
            }
            0xE2 if i + 2 < input.len() && input[i + 1] == 0x80 && input[i + 2] == 0xA9 => {
                out.extend_from_slice(b"\\u2029");
                i += 3;
                continue;
            }
            b => out.push(b),
        }
        i += 1;
    }
    out
}

/// Compact JSON, Go HTML escaping, trailing `\n`. Struct fields keep declaration
/// order and `BTreeMap` keys are sorted, matching `json.NewEncoder(w).Encode`.
pub fn to_vec<T: Serialize>(value: &T) -> Vec<u8> {
    let mut buf = html_escape(serde_json::to_vec(value).expect("serialize json"));
    buf.push(b'\n');
    buf
}

/// Like [`to_vec`] but **without** the trailing newline — matches Go's
/// `json.Marshal` (used for the internal item-key string and for sizing items,
/// where the encoded bytes must match Go's `json.Marshal(...)` exactly).
pub fn marshal<T: Serialize>(value: &T) -> Vec<u8> {
    html_escape(serde_json::to_vec(value).expect("serialize json"))
}

/// Convenience: [`marshal`] as a `String`.
pub fn marshal_string<T: Serialize>(value: &T) -> String {
    String::from_utf8(marshal(value)).expect("utf-8 json")
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn compact_html_escapes_and_appends_newline() {
        let got = to_vec(&json!({"s": "a<b>&c"}));
        assert_eq!(got, b"{\"s\":\"a\\u003cb\\u003e\\u0026c\"}\n");
    }

    #[test]
    fn line_separators_are_escaped() {
        let got = to_vec(&json!({"s": "x\u{2028}y\u{2029}z"}));
        assert_eq!(got, b"{\"s\":\"x\\u2028y\\u2029z\"}\n");
    }

    #[test]
    fn map_keys_are_sorted() {
        // serde_json::Value uses a BTreeMap, so object keys come out sorted —
        // matching Go's map marshaling.
        let got = to_vec(&json!({"b": {"d": 1, "c": 2}, "a": 3}));
        assert_eq!(got, b"{\"a\":3,\"b\":{\"c\":2,\"d\":1}}\n");
    }
}
