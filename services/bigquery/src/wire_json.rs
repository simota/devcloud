//! legacy-compatible JSON encoding for the BigQuery service.
//!
//! The legacy service writes HTTP responses via `json.NewEncoder(w).Encode`
//! (compact, trailing `\n`), the persisted `dataset.json` / `table.json` /
//! `routine.json` / `iam-policy.json` / job files via a `json.Encoder` with
//! `SetIndent("", "  ")` (2-space indent **and** trailing `\n`), and the
//! streaming-buffer JSONL via a plain `json.Encoder` (one compact value +
//! `\n` per row). All paths apply legacy `encoding/json` HTML escaping (`<`,
//! `>`, `&` and U+2028/U+2029 inside string values). serde leaves those raw,
//! so a post-pass byte rewrite over the already-valid JSON reproduces legacy
//! exactly. Struct fields keep declaration order; `BTreeMap` keys (and
//! `serde_json::Value` objects, with `preserve_order` off) come out sorted,
//! matching legacy map marshaling.

use serde::Serialize;

/// Rewrites `<`, `>`, `&` and the U+2028/U+2029 line separators to their
/// `\uXXXX` escapes, matching legacy `encoding/json` HTML escaping.
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

/// Compact JSON + HTML escaping, **no** trailing newline, matching
/// `json.Marshal` (used for row byte accounting).
pub fn marshal<T: Serialize>(value: &T) -> Vec<u8> {
    html_escape(serde_json::to_vec(value).expect("serialize json"))
}

/// Compact JSON + HTML escaping + trailing `\n`, matching
/// `json.NewEncoder(w).Encode` (HTTP responses and JSONL rows).
pub fn to_vec<T: Serialize>(value: &T) -> Vec<u8> {
    let mut buf = marshal(value);
    buf.push(b'\n');
    buf
}

/// 2-space-indented JSON + HTML escaping + trailing `\n`, matching a
/// `json.Encoder` with `SetIndent("", "  ")` (persisted resource files).
pub fn to_vec_indent<T: Serialize>(value: &T) -> Vec<u8> {
    let mut buf = html_escape(serde_json::to_vec_pretty(value).expect("serialize json"));
    buf.push(b'\n');
    buf
}

/// Removes insignificant whitespace from a JSON document, matching
/// `json.Compact` (which legacy applies to every `json.RawMessage` it re-encodes).
/// String contents — including escape sequences — pass through untouched.
pub fn compact(input: &str) -> String {
    let bytes = input.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut in_string = false;
    let mut escaped = false;
    for &b in bytes {
        if in_string {
            out.push(b);
            if escaped {
                escaped = false;
            } else if b == b'\\' {
                escaped = true;
            } else if b == b'"' {
                in_string = false;
            }
            continue;
        }
        match b {
            b' ' | b'\t' | b'\n' | b'\r' => {}
            b'"' => {
                in_string = true;
                out.push(b);
            }
            _ => out.push(b),
        }
    }
    String::from_utf8(out).expect("compact preserves utf-8")
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn compact_encode_escapes_and_appends_newline() {
        assert_eq!(
            to_vec(&json!({"s": "a<b>&c"})),
            b"{\"s\":\"a\\u003cb\\u003e\\u0026c\"}\n"
        );
    }

    #[test]
    fn indent_encode_appends_newline() {
        // legacy json.Encoder with SetIndent("", "  ") still appends '\n'.
        assert_eq!(to_vec_indent(&json!({"a": 1})), b"{\n  \"a\": 1\n}\n");
    }

    #[test]
    fn map_keys_sorted() {
        assert_eq!(to_vec(&json!({"b": 1, "a": 2})), b"{\"a\":2,\"b\":1}\n");
    }

    #[test]
    fn compact_strips_whitespace_outside_strings() {
        assert_eq!(
            compact("{ \"a\" : [ 1.50 , \"x \\\" y\" ] }"),
            "{\"a\":[1.50,\"x \\\" y\"]}"
        );
    }
}
