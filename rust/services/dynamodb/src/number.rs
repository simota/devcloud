//! Decimal number handling matching Go's `math/big.Rat` usage in the DynamoDB
//! service (validation, comparison, and the `ADD`/arithmetic update operators).
//!
//! DynamoDB `N` values are decimal strings. Go parses them with
//! `big.Rat.SetString` and formats sums via `FloatString` with a lexical
//! precision (`decimalPlaces`) and trailing-zero trimming. We reproduce that with
//! a sign + decimal-digit-coefficient + scale representation, so arithmetic is
//! exact and formatting matches byte-for-byte.
//!
//! Supported syntax: optional sign, integer/fraction digits, optional `e`/`E`
//! exponent (the realistic subset DynamoDB clients send). Go's `big.Rat` also
//! accepts `a/b` fractions and `0x`/`0b` prefixes; those are a documented gap
//! (no DynamoDB SDK emits them).

use std::cmp::Ordering;

/// A signed decimal: value = (−1)^`negative` · `coeff` / 10^`scale`, where
/// `coeff` is a normalized decimal-digit string (no leading zeros, `"0"` for
/// zero).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Decimal {
    negative: bool,
    coeff: String,
    scale: u32,
}

impl Decimal {
    fn zero() -> Self {
        Decimal {
            negative: false,
            coeff: "0".to_string(),
            scale: 0,
        }
    }

    fn is_zero(&self) -> bool {
        self.coeff == "0"
    }

    /// Coefficient raised to `target` scale by appending fractional zeros.
    fn coeff_at_scale(&self, target: u32) -> String {
        debug_assert!(target >= self.scale);
        let mut digits = self.coeff.clone();
        for _ in 0..(target - self.scale) {
            digits.push('0');
        }
        normalize_mag(&digits)
    }

    /// True when the value is an integer (no nonzero fractional digit).
    fn is_integer(&self) -> bool {
        if self.scale == 0 {
            return true;
        }
        let len = self.coeff.len();
        if (len as u32) <= self.scale {
            // |value| < 1 and nonzero → not an integer unless it is exactly zero.
            return self.coeff == "0";
        }
        self.coeff[len - self.scale as usize..]
            .bytes()
            .all(|b| b == b'0')
    }

    /// The integer rendering (`big.Rat.Num().String()` for integral values):
    /// drops the `scale` fractional digits and applies the sign.
    fn integer_string(&self) -> String {
        let int_digits = if (self.coeff.len() as u32) <= self.scale {
            "0".to_string()
        } else {
            normalize_mag(&self.coeff[..self.coeff.len() - self.scale as usize])
        };
        if self.negative && int_digits != "0" {
            format!("-{int_digits}")
        } else {
            int_digits
        }
    }

    /// `big.Rat.FloatString(precision)` then Go's trailing `0`/`.` trimming, as
    /// used by `addNumberStrings`/`negateNumberAttribute`.
    fn float_string_trimmed(&self, precision: u32) -> String {
        let (rounded, neg) = self.round_to(precision);
        // `rounded` is the coefficient at scale `precision`.
        let mut padded = rounded;
        while (padded.len() as u32) <= precision {
            padded.insert(0, '0');
        }
        let split = padded.len() - precision as usize;
        let int_part = normalize_mag(&padded[..split]);
        let frac_part = &padded[split..];
        let mut out = if precision == 0 {
            int_part
        } else {
            format!("{int_part}.{frac_part}")
        };
        // Trim trailing zeros, then a dangling dot (Go's TrimRight chain).
        if out.contains('.') {
            while out.ends_with('0') {
                out.pop();
            }
            if out.ends_with('.') {
                out.pop();
            }
        }
        if neg && out != "0" {
            format!("-{out}")
        } else {
            out
        }
    }

    /// Rounds |value| to `precision` fractional digits (half away from zero),
    /// returning the coefficient at scale `precision` and the sign.
    fn round_to(&self, precision: u32) -> (String, bool) {
        if precision >= self.scale {
            let mut digits = self.coeff.clone();
            for _ in 0..(precision - self.scale) {
                digits.push('0');
            }
            (normalize_mag(&digits), self.negative)
        } else {
            let drop = (self.scale - precision) as usize;
            let len = self.coeff.len();
            let kept = if drop >= len {
                "0".to_string()
            } else {
                self.coeff[..len - drop].to_string()
            };
            let first_dropped = if drop <= len {
                self.coeff.as_bytes().get(len - drop).copied()
            } else {
                Some(b'0')
            };
            let round_up = matches!(first_dropped, Some(d) if d >= b'5');
            let result = if round_up {
                increment_mag(&normalize_mag(&kept))
            } else {
                normalize_mag(&kept)
            };
            (result, self.negative)
        }
    }
}

/// Parses a Go-`big.Rat` decimal/exponent string. Returns `None` on anything
/// outside the supported subset (which Go's broader parser might still accept).
pub fn parse(s: &str) -> Option<Decimal> {
    let s = s.trim();
    if s.is_empty() {
        return None;
    }
    let bytes = s.as_bytes();
    let mut idx = 0;
    let mut negative = false;
    if bytes[idx] == b'+' || bytes[idx] == b'-' {
        negative = bytes[idx] == b'-';
        idx += 1;
    }
    let mut int_digits = String::new();
    while idx < bytes.len() && bytes[idx].is_ascii_digit() {
        int_digits.push(bytes[idx] as char);
        idx += 1;
    }
    let mut frac_digits = String::new();
    if idx < bytes.len() && bytes[idx] == b'.' {
        idx += 1;
        while idx < bytes.len() && bytes[idx].is_ascii_digit() {
            frac_digits.push(bytes[idx] as char);
            idx += 1;
        }
    }
    if int_digits.is_empty() && frac_digits.is_empty() {
        return None;
    }
    let mut exp: i64 = 0;
    if idx < bytes.len() && (bytes[idx] == b'e' || bytes[idx] == b'E') {
        idx += 1;
        let mut exp_neg = false;
        if idx < bytes.len() && (bytes[idx] == b'+' || bytes[idx] == b'-') {
            exp_neg = bytes[idx] == b'-';
            idx += 1;
        }
        let mut exp_digits = String::new();
        while idx < bytes.len() && bytes[idx].is_ascii_digit() {
            exp_digits.push(bytes[idx] as char);
            idx += 1;
        }
        if exp_digits.is_empty() {
            return None;
        }
        exp = exp_digits.parse::<i64>().ok()?;
        if exp_neg {
            exp = -exp;
        }
    }
    if idx != bytes.len() {
        return None; // trailing garbage
    }

    // Coefficient = int_digits ++ frac_digits, value = coeff · 10^(exp − fraclen).
    let frac_len = frac_digits.len() as i64;
    let mut mantissa = int_digits;
    mantissa.push_str(&frac_digits);
    let mantissa = normalize_mag(&mantissa);
    let total_exp = exp - frac_len;

    let (coeff, scale) = if total_exp >= 0 {
        let mut digits = mantissa;
        for _ in 0..total_exp {
            digits.push('0');
        }
        (normalize_mag(&digits), 0)
    } else {
        (mantissa, (-total_exp) as u32)
    };
    let negative = negative && coeff != "0";
    Some(Decimal {
        negative,
        coeff,
        scale,
    })
}

/// True when `s` is a valid number per [`parse`] (the `big.Rat.SetString` subset).
pub fn is_valid_number(s: &str) -> bool {
    parse(s).is_some()
}

/// Compares two decimal values; mirrors `big.Rat.Cmp`.
pub fn compare(a: &Decimal, b: &Decimal) -> Ordering {
    if a.is_zero() && b.is_zero() {
        return Ordering::Equal;
    }
    match (a.negative, b.negative) {
        (false, true) => return Ordering::Greater,
        (true, false) => return Ordering::Less,
        _ => {}
    }
    let scale = a.scale.max(b.scale);
    let mag = cmp_mag(&a.coeff_at_scale(scale), &b.coeff_at_scale(scale));
    if a.negative {
        mag.reverse()
    } else {
        mag
    }
}

/// Compares two number strings. Mirrors the `N` branch of
/// `compareAttributeValues`: if both parse, compare numerically; otherwise fall
/// back to byte comparison of the raw strings.
pub fn compare_number_strings(left: &str, right: &str) -> Ordering {
    match (parse(left), parse(right)) {
        (Some(a), Some(b)) => compare(&a, &b),
        _ => left.cmp(right),
    }
}

/// Lexical decimal places of a raw number string (digits after `.`), mirroring
/// `decimalPlaces`.
fn decimal_places(value: &str) -> u32 {
    match value.find('.') {
        Some(i) => (value.len() - i - 1) as u32,
        None => 0,
    }
}

/// Adds two decimals, returning a new `Decimal` (exact).
fn add(a: &Decimal, b: &Decimal) -> Decimal {
    let scale = a.scale.max(b.scale);
    let am = a.coeff_at_scale(scale);
    let bm = b.coeff_at_scale(scale);
    if a.negative == b.negative {
        let mag = add_mag(&am, &bm);
        let negative = a.negative && mag != "0";
        Decimal {
            negative,
            coeff: mag,
            scale,
        }
    } else {
        match cmp_mag(&am, &bm) {
            Ordering::Equal => Decimal::zero(),
            Ordering::Greater => {
                let mag = sub_mag(&am, &bm);
                Decimal {
                    negative: a.negative && mag != "0",
                    coeff: mag,
                    scale,
                }
            }
            Ordering::Less => {
                let mag = sub_mag(&bm, &am);
                Decimal {
                    negative: b.negative && mag != "0",
                    coeff: mag,
                    scale,
                }
            }
        }
    }
}

/// `addNumberStrings`: parses both, adds, and formats the way Go does (integer →
/// `Num().String()`; otherwise `FloatString(max lexical places, ≥1)` trimmed).
pub fn add_number_strings(left: &str, right: &str) -> Result<String, String> {
    let a = parse(left).ok_or_else(|| format!("invalid number \"{left}\""))?;
    let b = parse(right).ok_or_else(|| format!("invalid number \"{right}\""))?;
    let sum = add(&a, &b);
    Ok(format_sum(
        &sum,
        decimal_places(left).max(decimal_places(right)),
    ))
}

/// `negateNumberAttribute`: negates and formats like Go (integer →
/// `Num().String()`; otherwise `FloatString(lexical places, ≥1)` trimmed).
pub fn negate_number_string(value: &str) -> Result<String, String> {
    let mut d = parse(value).ok_or_else(|| format!("invalid number \"{value}\""))?;
    if !d.is_zero() {
        d.negative = !d.negative;
    }
    Ok(format_sum(&d, decimal_places(value)))
}

fn format_sum(value: &Decimal, lexical_precision: u32) -> String {
    if value.is_integer() {
        value.integer_string()
    } else {
        value.float_string_trimmed(lexical_precision.max(1))
    }
}

// --- decimal-digit-string magnitude arithmetic -----------------------------

/// Strips leading zeros; empty/all-zero → `"0"`.
fn normalize_mag(s: &str) -> String {
    let trimmed = s.trim_start_matches('0');
    if trimmed.is_empty() {
        "0".to_string()
    } else {
        trimmed.to_string()
    }
}

fn cmp_mag(a: &str, b: &str) -> Ordering {
    let (a, b) = (normalize_mag(a), normalize_mag(b));
    match a.len().cmp(&b.len()) {
        Ordering::Equal => a.cmp(&b),
        ord => ord,
    }
}

fn add_mag(a: &str, b: &str) -> String {
    let a = a.as_bytes();
    let b = b.as_bytes();
    let mut out = Vec::with_capacity(a.len().max(b.len()) + 1);
    let mut carry = 0u8;
    let (mut i, mut j) = (a.len(), b.len());
    while i > 0 || j > 0 || carry > 0 {
        let da = if i > 0 {
            i -= 1;
            a[i] - b'0'
        } else {
            0
        };
        let db = if j > 0 {
            j -= 1;
            b[j] - b'0'
        } else {
            0
        };
        let sum = da + db + carry;
        out.push(b'0' + sum % 10);
        carry = sum / 10;
    }
    out.reverse();
    normalize_mag(&String::from_utf8(out).unwrap())
}

/// `a - b`, requiring `a >= b` (both normalized magnitudes).
fn sub_mag(a: &str, b: &str) -> String {
    let a = a.as_bytes();
    let b = b.as_bytes();
    let mut out = Vec::with_capacity(a.len());
    let mut borrow = 0i16;
    let (mut i, mut j) = (a.len(), b.len());
    while i > 0 {
        i -= 1;
        let da = (a[i] - b'0') as i16;
        let db = if j > 0 {
            j -= 1;
            (b[j] - b'0') as i16
        } else {
            0
        };
        let mut diff = da - db - borrow;
        if diff < 0 {
            diff += 10;
            borrow = 1;
        } else {
            borrow = 0;
        }
        out.push(b'0' + diff as u8);
    }
    out.reverse();
    normalize_mag(&String::from_utf8(out).unwrap())
}

fn increment_mag(a: &str) -> String {
    add_mag(a, "1")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn add_integers() {
        assert_eq!(add_number_strings("1", "2").unwrap(), "3");
        assert_eq!(add_number_strings("10", "-3").unwrap(), "7");
        assert_eq!(add_number_strings("5", "0").unwrap(), "5");
        assert_eq!(
            add_number_strings("1000000000000000000", "1").unwrap(),
            "1000000000000000001"
        );
    }

    #[test]
    fn add_decimals_uses_lexical_precision() {
        assert_eq!(add_number_strings("1.5", "2.25").unwrap(), "3.75");
        assert_eq!(add_number_strings("0.1", "0.2").unwrap(), "0.3");
        assert_eq!(add_number_strings("100", "0.001").unwrap(), "100.001");
        assert_eq!(add_number_strings("3.14159", "2.71828").unwrap(), "5.85987");
        assert_eq!(add_number_strings("-1.5", "-2.5").unwrap(), "-4");
        assert_eq!(
            add_number_strings("0.0000001", "0.0000002").unwrap(),
            "0.0000003"
        );
    }

    #[test]
    fn negate_matches_go() {
        assert_eq!(negate_number_string("5").unwrap(), "-5");
        assert_eq!(negate_number_string("1.5").unwrap(), "-1.5");
        assert_eq!(negate_number_string("-2.25").unwrap(), "2.25");
        assert_eq!(negate_number_string("0").unwrap(), "0");
        assert_eq!(negate_number_string("0.10").unwrap(), "-0.1");
    }

    #[test]
    fn compare_orders_numerically() {
        assert_eq!(compare_number_strings("9", "10"), Ordering::Less);
        assert_eq!(compare_number_strings("3.5", "3.50"), Ordering::Equal);
        assert_eq!(compare_number_strings("-1", "0"), Ordering::Less);
        assert_eq!(compare_number_strings("1e2", "100"), Ordering::Equal);
        assert_eq!(compare_number_strings("0.1", "0.2"), Ordering::Less);
    }

    #[test]
    fn validity_matches_subset() {
        assert!(is_valid_number("1"));
        assert!(is_valid_number("-3.14"));
        assert!(is_valid_number("1e2"));
        assert!(is_valid_number(".5"));
        assert!(!is_valid_number("abc"));
        assert!(!is_valid_number("1.2.3"));
        assert!(!is_valid_number(""));
        assert!(!is_valid_number("1e"));
    }
}
