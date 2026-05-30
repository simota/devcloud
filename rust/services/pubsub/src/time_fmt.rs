//! UTC RFC3339Nano formatting, matching Go's
//! `time.Now().UTC().Format(time.RFC3339Nano)` used for `createdAt`/`updatedAt`/
//! `publishTime`. Dependency-free (Hinnant civil-date algorithm). A whole-second
//! instant renders without a fraction (`2026-05-30T12:00:00Z`); otherwise the
//! fraction is trailing-zero-trimmed (Go drops all trailing zero digits).

use std::time::{SystemTime, UNIX_EPOCH};

/// Current UTC time as RFC3339Nano.
pub fn now_rfc3339nano() -> String {
    let d = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    rfc3339nano_from_unix(d.as_secs() as i64, d.subsec_nanos())
}

/// Formats UNIX seconds + nanos as RFC3339Nano UTC.
pub fn rfc3339nano_from_unix(secs: i64, nanos: u32) -> String {
    let days = secs.div_euclid(86_400);
    let secs_of_day = secs.rem_euclid(86_400);
    let (year, month, day) = civil_from_days(days);
    let (hour, minute, second) = (
        secs_of_day / 3600,
        (secs_of_day % 3600) / 60,
        secs_of_day % 60,
    );
    let mut out = format!("{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}");
    if nanos > 0 {
        let frac = format!("{nanos:09}");
        let trimmed = frac.trim_end_matches('0');
        out.push('.');
        out.push_str(trimmed);
    }
    out.push('Z');
    out
}

/// Hinnant civil-from-days: days since the UNIX epoch -> (year, month, day).
fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    (if m <= 2 { y + 1 } else { y }, m, d)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn whole_second_has_no_fraction() {
        // 1780142400 = 2026-05-30T12:00:00Z
        assert_eq!(
            rfc3339nano_from_unix(1_780_142_400, 0),
            "2026-05-30T12:00:00Z"
        );
    }

    #[test]
    fn fraction_trailing_zeros_trimmed() {
        assert_eq!(
            rfc3339nano_from_unix(1_780_142_400, 500_000_000),
            "2026-05-30T12:00:00.5Z"
        );
        assert_eq!(
            rfc3339nano_from_unix(1_780_142_400, 123_000_000),
            "2026-05-30T12:00:00.123Z"
        );
    }

    #[test]
    fn epoch() {
        assert_eq!(rfc3339nano_from_unix(0, 0), "1970-01-01T00:00:00Z");
    }
}
