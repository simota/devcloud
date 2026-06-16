//! AWS percent-encoding, matching the legacy `awsPercentEncode`: bytes in
//! `A-Za-z0-9-_.~` (plus any extra `safe` bytes) pass through; every other byte
//! is `%XX` (uppercase hex). Used by SigV4 canonicalization and URL-encoded
//! listings.

use std::fmt::Write;

pub fn aws_percent_encode(value: &str, safe: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for &c in value.as_bytes() {
        let ch = c as char;
        if ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.' | '~') || safe.contains(ch) {
            out.push(ch);
        } else {
            let _ = write!(out, "%{c:02X}");
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encodes_reserved_bytes() {
        assert_eq!(aws_percent_encode("a b/c~d", "~-_."), "a%20b%2Fc~d");
        assert_eq!(aws_percent_encode("k+e=y", ""), "k%2Be%3Dy");
        // Always-safe set passes through regardless of `safe`.
        assert_eq!(aws_percent_encode("A.z-9_~", ""), "A.z-9_~");
    }
}
