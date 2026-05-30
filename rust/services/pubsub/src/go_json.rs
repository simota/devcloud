//! Go-compatible JSON encoding for the Pub/Sub REST service.
//!
//! The Go service writes HTTP responses via `json.NewEncoder(w).Encode`
//! (compact, trailing `\n`) and the persisted `resources.json` / `pubsub.json`
//! via `json.MarshalIndent(v, "", "  ")` (2-space indent, **no** trailing
//! newline). Both apply Go's `encoding/json` HTML escaping (`<`, `>`, `&` and
//! U+2028/U+2029 inside string values). serde leaves those raw, so a post-pass
//! byte rewrite over the already-valid JSON reproduces Go exactly. Struct fields
//! keep declaration order; `BTreeMap` keys (and `serde_json::Value` objects, with
//! `preserve_order` off) come out sorted, matching Go's map marshaling.

use serde::Serialize;

/// Rewrites `<`, `>`, `&` and the U+2028/U+2029 line separators to their `\uXXXX`
/// escapes, matching Go's `encoding/json` HTML escaping.
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

/// Compact JSON + HTML escaping + trailing `\n`, matching
/// `json.NewEncoder(w).Encode` (HTTP responses).
pub fn to_vec<T: Serialize>(value: &T) -> Vec<u8> {
    let mut buf = html_escape(serde_json::to_vec(value).expect("serialize json"));
    buf.push(b'\n');
    buf
}

/// 2-space-indented JSON + HTML escaping, **no** trailing newline, matching
/// `json.MarshalIndent(v, "", "  ")` (state files).
pub fn to_vec_indent<T: Serialize>(value: &T) -> Vec<u8> {
    html_escape(serde_json::to_vec_pretty(value).expect("serialize json"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn compact_escapes_and_newline() {
        assert_eq!(
            to_vec(&json!({"s": "a<b>&c"})),
            b"{\"s\":\"a\\u003cb\\u003e\\u0026c\"}\n"
        );
    }

    #[test]
    fn indent_has_no_trailing_newline() {
        assert_eq!(to_vec_indent(&json!({"a": 1})), b"{\n  \"a\": 1\n}");
    }

    #[test]
    fn map_keys_sorted() {
        assert_eq!(to_vec(&json!({"b": 1, "a": 2})), b"{\"a\":2,\"b\":1}\n");
    }
}
