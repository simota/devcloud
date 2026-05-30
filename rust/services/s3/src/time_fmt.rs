//! UTC RFC3339Nano formatting, matching Go's
//! `time.Now().UTC().Format(time.RFC3339Nano)` used for `createdAt`/`updatedAt`/
//! `publishTime`. Dependency-free (Hinnant civil-date algorithm). A whole-second
//! instant renders without a fraction (`2026-05-30T12:00:00Z`); otherwise the
//! fraction is trailing-zero-trimmed (Go drops all trailing zero digits).

use std::time::{SystemTime, UNIX_EPOCH};

/// Go's `time.Time` zero value, as rendered by `encoding/json`. Object metadata
/// fields left unset (e.g. a delete marker's `createdAt`) serialize to this.
pub const GO_ZERO_TIME: &str = "0001-01-01T00:00:00Z";

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

/// Parses an RFC3339(/Nano) UTC timestamp to `(unix_secs, nanos)`. Accepts an
/// optional fractional part and a trailing `Z`. Returns `None` on malformed
/// input (the limited subset the service produces/consumes).
pub fn parse_rfc3339(value: &str) -> Option<(i64, u32)> {
    let s = value.strip_suffix('Z')?;
    let (date, time) = s.split_once('T')?;
    let mut date_parts = date.split('-');
    let year: i64 = date_parts.next()?.parse().ok()?;
    let month: u32 = date_parts.next()?.parse().ok()?;
    let day: u32 = date_parts.next()?.parse().ok()?;
    if date_parts.next().is_some() {
        return None;
    }
    let (hms, frac) = match time.split_once('.') {
        Some((hms, frac)) => (hms, frac),
        None => (time, ""),
    };
    let mut t = hms.split(':');
    let hour: i64 = t.next()?.parse().ok()?;
    let minute: i64 = t.next()?.parse().ok()?;
    let second: i64 = t.next()?.parse().ok()?;
    if t.next().is_some() {
        return None;
    }
    let days = days_from_civil(year, month, day);
    let secs = days * 86_400 + hour * 3600 + minute * 60 + second;
    let nanos = if frac.is_empty() {
        0
    } else {
        let padded = format!("{frac:0<9}");
        padded[..9].parse().ok()?
    };
    Some((secs, nanos))
}

/// Reports whether RFC3339 time `a` is strictly after `b`, compared numerically
/// (fractional precision varies, so a lexical compare would be wrong).
/// Unparseable inputs sort before any valid time.
pub fn time_after(a: &str, b: &str) -> bool {
    parse_rfc3339(a).unwrap_or((i64::MIN, 0)) > parse_rfc3339(b).unwrap_or((i64::MIN, 0))
}

/// Formats UNIX seconds as RFC3339 (second precision, no fraction), matching
/// Go's `t.Format(time.RFC3339)` for a UTC instant.
pub fn rfc3339_seconds_from_unix(secs: i64) -> String {
    let days = secs.div_euclid(86_400);
    let sod = secs.rem_euclid(86_400);
    let (y, m, d) = civil_from_days(days);
    format!(
        "{y:04}-{m:02}-{d:02}T{:02}:{:02}:{:02}Z",
        sod / 3600,
        (sod % 3600) / 60,
        sod % 60
    )
}

/// Adds `years` calendar years to an RFC3339(/Nano) timestamp, normalizing
/// day overflow like Go's `time.Time.AddDate(years, 0, 0)`, and returns RFC3339
/// (second precision). `None` if `value` does not parse.
pub fn add_calendar_years_rfc3339(value: &str, years: i64) -> Option<String> {
    let (secs, _) = parse_rfc3339(value)?;
    let days = secs.div_euclid(86_400);
    let sod = secs.rem_euclid(86_400);
    let (y, m, d) = civil_from_days(days);
    let serial = days_from_civil(y + years, m, d);
    Some(rfc3339_seconds_from_unix(serial * 86_400 + sod))
}

/// Hinnant days-from-civil: (year, month, day) -> days since the UNIX epoch.
fn days_from_civil(y: i64, m: u32, d: u32) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) as i64 + 2) / 5 + d as i64 - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
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
