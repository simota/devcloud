//! legacy `time.ParseDuration` reproduction, used to validate `messageRetentionDuration`,
//! `expirationPolicy.ttl`, etc.
//!
//! Mirrors `parseGoogleDuration` in `internal/services/pubsub/validation.rs`,
//! which calls `time.ParseDuration` and rejects negatives. legacy accepts a signed
//! sequence of decimal numbers each with a unit suffix (`ns`, `us`/`µs`, `ms`,
//! `s`, `m`, `h`), e.g. `"3600s"`, `"1h30m"`, `"1.5h"`. We only need accept/reject
//! parity (the parsed value is just checked for validity + non-negativity), but
//! we compute the total nanoseconds to mirror the overflow/negative checks.

/// Parses a legacy duration string, returning the total nanoseconds. Returns `None`
/// for anything `time.ParseDuration` rejects. (The caller additionally rejects
/// negative results, matching `parseGoogleDuration`.)
pub fn parse_go_duration(value: &str) -> Option<i128> {
    let s = value.trim();
    if s.is_empty() {
        return None;
    }
    let bytes = s.as_bytes();
    let mut i = 0;
    let mut neg = false;
    // Optional leading sign.
    if bytes[i] == b'+' || bytes[i] == b'-' {
        neg = bytes[i] == b'-';
        i += 1;
    }
    // Special case: "0" with no unit is allowed.
    if &s[i..] == "0" {
        return Some(0);
    }
    if i >= bytes.len() {
        return None;
    }
    let mut total: i128 = 0;
    while i < bytes.len() {
        // Each segment: a number (int or decimal) followed by a unit.
        let start = i;
        let mut saw_digit = false;
        while i < bytes.len() && bytes[i].is_ascii_digit() {
            i += 1;
            saw_digit = true;
        }
        let mut frac_digits = "";
        if i < bytes.len() && bytes[i] == b'.' {
            i += 1;
            let frac_start = i;
            while i < bytes.len() && bytes[i].is_ascii_digit() {
                i += 1;
                saw_digit = true;
            }
            frac_digits = &s[frac_start..i];
        }
        if !saw_digit {
            return None;
        }
        let int_digits = {
            // integer part is from `start` up to the '.' or current unit.
            let int_end = s[start..i].find('.').map(|p| start + p).unwrap_or(i);
            &s[start..int_end]
        };
        // Unit.
        let unit_start = i;
        while i < bytes.len() {
            let c = bytes[i];
            if c.is_ascii_digit() || c == b'.' {
                break;
            }
            i += 1;
        }
        let unit = &s[unit_start..i];
        let unit_ns: i128 = match unit {
            "ns" => 1,
            "us" | "µs" => 1_000,
            "ms" => 1_000_000,
            "s" => 1_000_000_000,
            "m" => 60_000_000_000,
            "h" => 3_600_000_000_000,
            _ => return None,
        };
        let int_val: i128 = if int_digits.is_empty() {
            0
        } else {
            int_digits.parse().ok()?
        };
        let mut segment = int_val.checked_mul(unit_ns)?;
        if !frac_digits.is_empty() {
            // fractional nanoseconds = 0.<frac> * unit_ns
            let frac_val: i128 = frac_digits.parse().ok()?;
            let scale: i128 = 10i128.checked_pow(frac_digits.len() as u32)?;
            segment = segment.checked_add(frac_val.checked_mul(unit_ns)? / scale)?;
        }
        total = total.checked_add(segment)?;
    }
    Some(if neg { -total } else { total })
}

/// True when `value` is a valid non-negative legacy duration (the
/// `parseGoogleDuration` contract).
pub fn valid_google_duration(value: &str) -> bool {
    matches!(parse_go_duration(value.trim()), Some(n) if n >= 0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_seconds_and_compound() {
        assert!(valid_google_duration("3600s"));
        assert!(valid_google_duration("1h30m"));
        assert!(valid_google_duration("1.5h"));
        assert!(valid_google_duration("0"));
        assert!(valid_google_duration("500ms"));
        assert!(valid_google_duration("100ns"));
    }

    #[test]
    fn rejects_garbage_and_negative() {
        assert!(!valid_google_duration("abc"));
        assert!(!valid_google_duration(""));
        assert!(!valid_google_duration("10"));
        assert!(!valid_google_duration("-5s"));
        assert!(!valid_google_duration("1x"));
    }

    #[test]
    fn computes_nanos() {
        assert_eq!(parse_go_duration("3600s"), Some(3_600_000_000_000));
        assert_eq!(parse_go_duration("1h"), Some(3_600_000_000_000));
        assert_eq!(parse_go_duration("1.5h"), Some(5_400_000_000_000));
    }
}
