//! Go-compatible JSON encoding for on-disk metadata.
//!
//! The Go S3 store persists every metadata file (`bucket.json`, `object.json`,
//! `versioning.json`, …) via `writeJSONFile`, which is
//! `json.MarshalIndent(value, "", "  ")` followed by a single trailing `\n`. We
//! reproduce that byte-for-byte:
//!
//!  1. **2-space indentation** — `serde_json::to_vec_pretty` matches Go's
//!     `MarshalIndent(_, "", "  ")` (same `": "` separators, same per-element
//!     lines, same `{}`/`[]` for empties).
//!  2. **HTML escaping** — Go escapes `<`, `>`, `&` and the U+2028/U+2029 line
//!     separators inside string values as `<`, `>`, `&`,
//!     ` `, ` `. serde leaves them raw; a post-pass byte rewrite over
//!     already-valid JSON is exact (these bytes only occur inside string
//!     literals).
//!  3. **Trailing newline** — `writeJSONFile` appends one `\n`.
//!  4. **Key ordering** — structs keep declaration order (serde derive) and
//!     `BTreeMap` keys sort (matching Go's struct-vs-map split).

use serde::Serialize;

/// Rewrites `<`, `>`, `&` and U+2028/U+2029 to their `\uXXXX` escapes, matching
/// Go's `encoding/json` HTML escaping. Operates on valid JSON — these bytes only
/// occur inside string literals, so the rewrite is safe.
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

/// Indented JSON + Go HTML escaping + trailing `\n`, matching the Go store's
/// `writeJSONFile` (`json.MarshalIndent(_, "", "  ")` plus one `\n`).
pub fn to_vec_indent<T: Serialize>(value: &T) -> Vec<u8> {
    let mut buf = html_escape(serde_json::to_vec_pretty(value).expect("serialize json"));
    buf.push(b'\n');
    buf
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn indent_two_spaces_and_trailing_newline() {
        let got = to_vec_indent(&json!({"a": 1, "b": {"c": 2}}));
        assert_eq!(
            String::from_utf8(got).unwrap(),
            "{\n  \"a\": 1,\n  \"b\": {\n    \"c\": 2\n  }\n}\n"
        );
    }

    #[test]
    fn empty_object_renders_braces() {
        let got = to_vec_indent(&json!({"r": {}}));
        assert_eq!(String::from_utf8(got).unwrap(), "{\n  \"r\": {}\n}\n");
    }

    #[test]
    fn html_escaped_in_strings() {
        let got = to_vec_indent(&json!({"s": "a<b>&c"}));
        assert_eq!(
            String::from_utf8(got).unwrap(),
            "{\n  \"s\": \"a\\u003cb\\u003e\\u0026c\"\n}\n"
        );
    }
}
