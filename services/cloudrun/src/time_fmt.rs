//! Dependency-free RFC 3339 UTC timestamps (`google.protobuf.Timestamp` JSON
//! form: fractional seconds trimmed, `Z` suffix).

use std::time::{SystemTime, UNIX_EPOCH};

/// Current UTC time as RFC 3339 (e.g. `2026-04-30T10:00:00Z`).
pub fn now_rfc3339() -> String {
    let d = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    rfc3339_from_unix(d.as_secs() as i64, d.subsec_nanos())
}

/// A key that orders RFC 3339 UTC timestamps chronologically: the fraction
/// is padded to nine digits so `…:05.1Z` < `…:05.11Z` as strings too.
pub fn sortable(ts: &str) -> String {
    let body = ts.trim_end_matches('Z');
    let (base, frac) = body.split_once('.').unwrap_or((body, ""));
    format!("{base}.{frac:0<9}")
}

fn rfc3339_from_unix(secs: i64, nanos: u32) -> String {
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
        let frac = format!("{:09}", nanos);
        out.push('.');
        out.push_str(frac.trim_end_matches('0'));
    }
    out.push('Z');
    out
}

/// Howard Hinnant's algorithm: days since 1970-01-01 → (year, month, day).
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
    let year = if m <= 2 { y + 1 } else { y };
    (year, m, d)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rfc3339_format() {
        assert_eq!(rfc3339_from_unix(1_700_000_000, 0), "2023-11-14T22:13:20Z");
    }

    #[test]
    fn sortable_orders_trimmed_fractions_chronologically() {
        let mut v = vec![
            "2026-10-06T00:00:05.11Z",
            "2026-10-06T00:00:05.1Z",
            "2026-10-06T00:00:05Z",
            "2026-10-06T00:00:04.999999999Z",
        ];
        v.sort_by_key(|t| sortable(t));
        assert_eq!(
            v,
            [
                "2026-10-06T00:00:04.999999999Z",
                "2026-10-06T00:00:05Z",
                "2026-10-06T00:00:05.1Z",
                "2026-10-06T00:00:05.11Z",
            ]
        );
    }
}
