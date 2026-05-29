//! RFC 3339 UTC formatting/parsing matching Go's `time.Time` JSON wire form,
//! dependency-free. Shared by the queue model (timestamps) and the computed
//! queue attributes (`CreatedTimestamp` etc. are UNIX seconds).

use std::time::{SystemTime, UNIX_EPOCH};

use crate::model::ZERO_TIME;

/// Current UTC time as RFC 3339 (e.g. `2026-04-30T10:00:00Z`).
pub fn now_rfc3339() -> String {
    let d = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    rfc3339_from_unix(d.as_secs() as i64, d.subsec_nanos())
}

/// Formats a UNIX timestamp as RFC 3339 UTC (trailing-zero-trimmed fraction).
pub fn rfc3339_from_unix(secs: i64, nanos: u32) -> String {
    let days = secs.div_euclid(86_400);
    let secs_of_day = secs.rem_euclid(86_400);
    let (year, month, day) = civil_from_days(days);
    let (hour, minute, second) = (
        secs_of_day / 3600,
        (secs_of_day % 3600) / 60,
        secs_of_day % 60,
    );
    let mut out = format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}",
        year, month, day, hour, minute, second
    );
    if nanos > 0 {
        let frac = format!("{:09}", nanos);
        out.push('.');
        out.push_str(frac.trim_end_matches('0'));
    }
    out.push('Z');
    out
}

/// Parses an RFC 3339 UTC timestamp to UNIX seconds. The Go zero time
/// (`0001-01-01T00:00:00Z`) and unparseable input both yield the Go zero
/// `time.Time.Unix()` value (`-62135596800`), matching `time.Time{}.Unix()`.
pub fn unix_from_rfc3339(s: &str) -> i64 {
    const GO_ZERO_UNIX: i64 = -62_135_596_800;
    if s == ZERO_TIME {
        return GO_ZERO_UNIX;
    }
    parse_rfc3339_secs(s).unwrap_or(GO_ZERO_UNIX)
}

/// Total nanoseconds since the UNIX epoch for an RFC 3339 UTC timestamp, for
/// chronological comparison and arithmetic at full precision. The Go zero time
/// and unparseable input sort oldest (`i128::MIN`).
pub fn unix_nanos_from_rfc3339(s: &str) -> i128 {
    if s.is_empty() || s == ZERO_TIME {
        return i128::MIN;
    }
    parse_rfc3339_nanos(s).unwrap_or(i128::MIN)
}

/// Mirrors Go's `time.Time.UnixMilli()` for an RFC 3339 timestamp.
pub fn unix_millis_from_rfc3339(s: &str) -> i64 {
    (unix_nanos_from_rfc3339(s) / 1_000_000) as i64
}

/// Returns the RFC 3339 timestamp `seconds` after `base` (RFC3339Nano form,
/// trailing-zero-trimmed fraction), mirroring `t.Add(d).UTC()`.
pub fn add_seconds(base: &str, seconds: i64) -> String {
    let nanos = unix_nanos_from_rfc3339(base) + (seconds as i128) * 1_000_000_000;
    let total_secs = (nanos.div_euclid(1_000_000_000)) as i64;
    let frac = (nanos.rem_euclid(1_000_000_000)) as u32;
    rfc3339_from_unix(total_secs, frac)
}

/// `true` when `a` is strictly before `b` (mirrors `a.Before(b)`).
pub fn before(a: &str, b: &str) -> bool {
    unix_nanos_from_rfc3339(a) < unix_nanos_from_rfc3339(b)
}

/// `true` when the RFC 3339 timestamp is the Go zero time (or empty).
pub fn is_zero(s: &str) -> bool {
    s.is_empty() || s == ZERO_TIME
}

fn parse_rfc3339_nanos(s: &str) -> Option<i128> {
    let (date, rest) = s.split_once('T')?;
    let time = rest.strip_suffix('Z').or_else(|| rest.strip_suffix('z'))?;
    let mut dp = date.split('-');
    let year: i64 = dp.next()?.parse().ok()?;
    let month: i64 = dp.next()?.parse().ok()?;
    let day: i64 = dp.next()?.parse().ok()?;
    if dp.next().is_some() {
        return None;
    }
    let (hms, frac) = match time.split_once('.') {
        Some((h, f)) => (h, Some(f)),
        None => (time, None),
    };
    let mut tp = hms.split(':');
    let hour: i64 = tp.next()?.parse().ok()?;
    let minute: i64 = tp.next()?.parse().ok()?;
    let second: i64 = tp.next()?.parse().ok()?;
    let secs = days_from_civil(year, month, day) * 86_400 + hour * 3600 + minute * 60 + second;
    let nanos = match frac {
        None => 0i128,
        Some(f) => {
            if f.is_empty() || !f.bytes().all(|b| b.is_ascii_digit()) {
                return None;
            }
            let mut digits = f.to_string();
            digits.truncate(9);
            while digits.len() < 9 {
                digits.push('0');
            }
            digits.parse::<i128>().ok()?
        }
    };
    Some((secs as i128) * 1_000_000_000 + nanos)
}

fn parse_rfc3339_secs(s: &str) -> Option<i64> {
    let (date, rest) = s.split_once('T')?;
    let time = rest.strip_suffix('Z').or_else(|| rest.strip_suffix('z'))?;
    let mut dp = date.split('-');
    let year: i64 = dp.next()?.parse().ok()?;
    let month: i64 = dp.next()?.parse().ok()?;
    let day: i64 = dp.next()?.parse().ok()?;
    if dp.next().is_some() {
        return None;
    }
    let hms = time.split_once('.').map(|(h, _)| h).unwrap_or(time);
    let mut tp = hms.split(':');
    let hour: i64 = tp.next()?.parse().ok()?;
    let minute: i64 = tp.next()?.parse().ok()?;
    let second: i64 = tp.next()?.parse().ok()?;
    Some(days_from_civil(year, month, day) * 86_400 + hour * 3600 + minute * 60 + second)
}

fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    (if m <= 2 { y + 1 } else { y }, m, d)
}
