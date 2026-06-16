//! Delivery-record time helpers.
//!
//! Mirrors the `time.Time` arithmetic in
//! `internal/services/pubsub/{message_handlers,retention_leases}.rs`. Delivery
//! timestamps are RFC3339 strings; the legacy zero time is `0001-01-01T00:00:00Z`.
//! These helpers compare/add seconds in UNIX space.

use crate::model::ZERO_TIME;

/// True for the legacy zero time (`0001-01-01T00:00:00Z`) or an empty string.
pub fn is_zero(value: &str) -> bool {
    value.is_empty() || value == ZERO_TIME
}

/// UNIX seconds for a delivery timestamp; the zero time sorts oldest.
pub fn unix_secs(value: &str) -> i64 {
    if is_zero(value) {
        return i64::MIN;
    }
    crate::time_fmt::parse_rfc3339(value)
        .map(|(s, _)| s)
        .unwrap_or(i64::MIN)
}

/// True when `value` is strictly after `now_secs` (legacy `t.After(now)`); the
/// zero time is never after.
pub fn after(value: &str, now_secs: i64) -> bool {
    !is_zero(value) && unix_secs(value) > now_secs
}

/// `now + seconds` rendered as an RFC3339 timestamp (whole seconds).
pub fn plus_seconds(now_secs: i64, seconds: i64) -> String {
    crate::time_fmt::rfc3339nano_from_unix(now_secs + seconds, 0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zero_and_after() {
        assert!(is_zero(ZERO_TIME));
        assert!(is_zero(""));
        assert!(!after(ZERO_TIME, 100));
        assert!(after(
            "2026-05-30T12:00:10Z",
            unix_secs("2026-05-30T12:00:00Z")
        ));
        assert!(!after(
            "2026-05-30T12:00:00Z",
            unix_secs("2026-05-30T12:00:10Z")
        ));
    }

    #[test]
    fn plus() {
        let base = unix_secs("2026-05-30T12:00:00Z");
        assert_eq!(plus_seconds(base, 10), "2026-05-30T12:00:10Z");
    }
}
