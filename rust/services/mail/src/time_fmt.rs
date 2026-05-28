//! Minimal RFC 3339 (UTC) formatting for timestamps, matching the wire form of
//! Go's `time.Time` JSON marshaling — without pulling in a date crate.
//!
//! Go marshals `time.Time` as RFC 3339 with fractional seconds only when
//! nonzero and with trailing zeros trimmed (`time.RFC3339Nano`), suffixed `Z`
//! for UTC. This module reproduces that for the current instant; the store
//! never performs date arithmetic, only formats "now".

use std::time::{SystemTime, UNIX_EPOCH};

/// Current UTC time formatted as RFC 3339 (e.g. `2026-04-30T10:00:00Z`, or with
/// a trimmed fractional part when sub-second precision is present).
pub fn now_rfc3339() -> String {
    let d = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    rfc3339_from_unix(d.as_secs() as i64, d.subsec_nanos())
}

/// Formats a UNIX timestamp (seconds since epoch, plus nanoseconds) as RFC 3339
/// UTC. Uses Howard Hinnant's civil-from-days algorithm for the calendar date.
pub fn rfc3339_from_unix(secs: i64, nanos: u32) -> String {
    let days = secs.div_euclid(86_400);
    let secs_of_day = secs.rem_euclid(86_400);
    let (year, month, day) = civil_from_days(days);
    let hour = secs_of_day / 3_600;
    let minute = (secs_of_day % 3_600) / 60;
    let second = secs_of_day % 60;

    let mut out = format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}",
        year, month, day, hour, minute, second
    );
    if nanos > 0 {
        // Trailing-zero-trimmed fractional seconds, mirroring RFC3339Nano.
        let frac = format!("{:09}", nanos);
        let trimmed = frac.trim_end_matches('0');
        out.push('.');
        out.push_str(trimmed);
    }
    out.push('Z');
    out
}

/// Parses an RFC 3339 UTC timestamp (`YYYY-MM-DDTHH:MM:SS[.fff]Z`) into a
/// `(seconds, nanoseconds)` UNIX key for chronological comparison. Returns
/// `(i64::MIN, 0)` on a malformed input so it sorts oldest (last in a
/// newest-first ordering), avoiding a panic on unexpected data.
pub fn unix_from_rfc3339(s: &str) -> (i64, u32) {
    parse_rfc3339(s).unwrap_or((i64::MIN, 0))
}

fn parse_rfc3339(s: &str) -> Option<(i64, u32)> {
    let (date, rest) = s.split_once('T')?;
    let time = rest.strip_suffix('Z').or_else(|| rest.strip_suffix('z'))?;

    let mut dparts = date.split('-');
    let year: i64 = dparts.next()?.parse().ok()?;
    let month: i64 = dparts.next()?.parse().ok()?;
    let day: i64 = dparts.next()?.parse().ok()?;
    if dparts.next().is_some() {
        return None;
    }

    let (hms, frac) = match time.split_once('.') {
        Some((hms, frac)) => (hms, Some(frac)),
        None => (time, None),
    };
    let mut tparts = hms.split(':');
    let hour: i64 = tparts.next()?.parse().ok()?;
    let minute: i64 = tparts.next()?.parse().ok()?;
    let second: i64 = tparts.next()?.parse().ok()?;
    if tparts.next().is_some() {
        return None;
    }

    let nanos = match frac {
        None => 0,
        Some(f) => {
            if f.is_empty() || !f.bytes().all(|b| b.is_ascii_digit()) {
                return None;
            }
            // Right-pad/truncate the fractional part to 9 digits (nanoseconds).
            let mut digits = f.to_string();
            digits.truncate(9);
            while digits.len() < 9 {
                digits.push('0');
            }
            digits.parse().ok()?
        }
    };

    let days = days_from_civil(year, month, day);
    let secs = days * 86_400 + hour * 3_600 + minute * 60 + second;
    Some((secs, nanos))
}

/// Howard Hinnant's algorithm: (year, month, day) → days since 1970-01-01.
fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400; // [0, 399]
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + d - 1; // [0, 365]
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy; // [0, 146096]
    era * 146_097 + doe - 719_468
}

/// Howard Hinnant's algorithm: days since 1970-01-01 → (year, month, day).
fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097; // [0, 146096]
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365; // [0, 399]
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11]
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32; // [1, 31]
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32; // [1, 12]
    let year = if m <= 2 { y + 1 } else { y };
    (year, m, d)
}

#[cfg(test)]
mod tests {
    use super::{rfc3339_from_unix, unix_from_rfc3339};

    #[test]
    fn formats_known_instants() {
        // 2026-04-30T10:00:00Z — matches the Go golden oracle.
        let secs = 1_777_543_200; // date -u -d 2026-04-30T10:00:00Z +%s
        assert_eq!(rfc3339_from_unix(secs, 0), "2026-04-30T10:00:00Z");
        // Epoch.
        assert_eq!(rfc3339_from_unix(0, 0), "1970-01-01T00:00:00Z");
        // Fractional seconds trimmed.
        assert_eq!(rfc3339_from_unix(0, 500_000_000), "1970-01-01T00:00:00.5Z");
    }

    #[test]
    fn parse_round_trips_and_orders() {
        for (secs, nanos) in [(1_777_543_200, 0u32), (0, 0), (0, 500_000_000)] {
            let s = rfc3339_from_unix(secs, nanos);
            assert_eq!(unix_from_rfc3339(&s), (secs, nanos));
        }
        // Trimmed-zero fractional vs whole second order correctly.
        let half = rfc3339_from_unix(10, 500_000_000); // 00:10.5
        let whole = rfc3339_from_unix(10, 0); // 00:10
        assert!(unix_from_rfc3339(&half) > unix_from_rfc3339(&whole));
        // Malformed sorts oldest.
        assert_eq!(unix_from_rfc3339("not-a-time"), (i64::MIN, 0));
    }
}
