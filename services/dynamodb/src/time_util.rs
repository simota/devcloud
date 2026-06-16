//! Time helpers, dependency-free.
//!
//! The legacy service stamps `CreationDateTime` with `time.Now().Unix()` (seconds)
//! and stream labels with `time.Now().UTC().Format("2006-01-02T15:04:05.000")`
//! (millisecond precision, always three fractional digits). Both are reproduced
//! here; the server injects a fixed value in tests for byte-exact parity.

use std::time::{SystemTime, UNIX_EPOCH};

/// Current UNIX time in seconds.
pub fn now_unix() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// Current UNIX time in milliseconds.
pub fn now_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// Formats UNIX milliseconds as a DynamoDB stream label
/// (`2006-01-02T15:04:05.000`, UTC, always three fractional digits) — matching
/// legacy `time.Now().UTC().Format("2006-01-02T15:04:05.000")`.
pub fn stream_label(unix_millis: i64) -> String {
    let secs = unix_millis.div_euclid(1000);
    let millis = unix_millis.rem_euclid(1000);
    let days = secs.div_euclid(86_400);
    let secs_of_day = secs.rem_euclid(86_400);
    let (year, month, day) = civil_from_days(days);
    let (hour, minute, second) = (
        secs_of_day / 3600,
        (secs_of_day % 3600) / 60,
        secs_of_day % 60,
    );
    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}.{:03}",
        year, month, day, hour, minute, second, millis
    )
}

/// Hinnant's civil-from-days algorithm: days since the UNIX epoch ->
/// `(year, month, day)`.
fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u64; // [0, 146096]
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365; // [0, 399]
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11]
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32; // [1, 31]
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32; // [1, 12]
    (if m <= 2 { y + 1 } else { y }, m, d)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stream_label_formats_millis() {
        // 1780039784123 ms = 2026-05-29T07:29:44.123Z (verified vs `date -u`).
        assert_eq!(stream_label(1_780_039_784_123), "2026-05-29T07:29:44.123");
    }

    #[test]
    fn stream_label_pads_zero_millis() {
        // Exactly on a second boundary keeps `.000`.
        assert_eq!(stream_label(1_780_039_784_000), "2026-05-29T07:29:44.000");
    }

    #[test]
    fn epoch_is_1970() {
        assert_eq!(stream_label(0), "1970-01-01T00:00:00.000");
    }
}
