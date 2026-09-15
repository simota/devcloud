"""Temporary audit helper. Run only after the original-source regressions fail."""
from pathlib import Path
import sys

root = Path(sys.argv[1] if len(sys.argv) > 1 else '.')
p = root / 'orchestrator/src/config.rs'
s = p.read_text()
old = '''/// Replicate legacy strconv.Atoi: signed 64→truncated to int; here legacy `int` is
/// 64-bit on the target platforms but field types are i32/i64 per struct. We
/// parse as i64 then narrow; an out-of-range value mirrors legacy ParseInt error.
fn parse_int(field: &str, value: &str) -> io::Result<i64> {
    value.parse::<i64>().map_err(|_| {'''
new = '''/// Parse directly into the destination integer type so out-of-range values
/// cannot wrap when assigned to a narrower config field.
fn parse_int<T: std::str::FromStr>(field: &str, value: &str) -> io::Result<T> {
    value.parse::<T>().map_err(|_| {'''
assert s.count(old) == 1
s = s.replace(old, new, 1)
assert s.count(' as i32') == 38
s = s.replace(' as i32', '')
assert s.count('parse_int("server.') == 16
s = s.replace('parse_int("server.', 'parse_port("server.')
marker = '/// Replicate legacy strconv.ParseBool:'
port_parser = '''/// Configured listeners need a concrete TCP port, not an ephemeral port (0).
fn parse_port(field: &str, value: &str) -> io::Result<i32> {
    let port: u16 = parse_int(field, value)?;
    if port == 0 {
        return Err(err_positive(field));
    }
    Ok(i32::from(port))
}

'''
assert s.count(marker) == 1
s = s.replace(marker, port_parser + marker, 1)
p.write_text(s)

for service in ('s3', 'pubsub'):
    p = root / f'services/{service}/src/time_fmt.rs'
    s = p.read_text()
    old = '        Some((hms, frac)) => (hms, frac),'
    new = '''        Some((hms, frac)) => {
            // Validate the entire fraction, including digits beyond nanosecond
            // precision, before truncating. Never slice arbitrary UTF-8 bytes.
            if frac.is_empty() || !frac.bytes().all(|b| b.is_ascii_digit()) {
                return None;
            }
            (hms, frac)
        }'''
    assert s.count(old) == 1
    s = s.replace(old, new, 1)
    old = '''    let nanos = if frac.is_empty() {
        0
    } else {
        let padded = format!("{frac:0<9}");
        padded[..9].parse().ok()?
    };'''
    new = '''    // Keep the existing nanosecond truncation for valid higher precision,
    // padding shorter fractions without allocating a copy of the input.
    let mut nanos = 0;
    for digit in frac.bytes().chain(std::iter::repeat(b'0')).take(9) {
        nanos = nanos * 10 + u32::from(digit - b'0');
    }'''
    assert s.count(old) == 1
    s = s.replace(old, new, 1)
    p.write_text(s)

p = root / 'README.md'
s = p.read_text()
marker = 'Configuration lives at `.devcloud/config.yaml`. Runtime data is stored under `.devcloud/data` by default.\n'
assert s.count(marker) == 1
s = s.replace(marker, marker + '\nConfigured server ports must be between `1` and `65535`; port `0` (ephemeral binding) is not supported. Integer settings are checked against their destination type before assignment, while each setting retains its documented positive or non-negative constraint.\n', 1)
p.write_text(s)
