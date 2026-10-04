//! JSON numbers as exact decimals.
//!
//! A [`Number`] is `±digits × 10^exponent` with no leading or trailing zero
//! in `digits`, so every spelling of one value (`1`, `1.0`, `10e-1`, `1E0`)
//! is the same `Number`, and no digit is lost the way an IEEE double loses
//! the low digits of an integer beyond 2^53.
//!
//! Its canonical text follows ECMAScript's `Number::toString` (which RFC
//! 8785 uses) applied to the exact decimal instead of to the nearest double:
//! plain digits up to 21 integer digits, a decimal point inside that, a
//! `0.000…` prefix down to 10^-7, and exponential form (`1.5e+30`) beyond.
//! For any value a double holds exactly and prints shortest, the text is
//! RFC 8785's.

use std::cmp::Ordering;

/// The most exponent digits (leading zeros aside) a number may carry. Far
/// beyond any real value; it bounds the exponent arithmetic.
pub const MAX_EXPONENT_DIGITS: usize = 30;

/// An exact decimal: `±digits × 10^exponent`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Number {
    negative: bool,
    /// ASCII digits without leading or trailing zeros; empty for zero.
    digits: String,
    exponent: i128,
}

/// The exponent of a number literal has more than [`MAX_EXPONENT_DIGITS`]
/// digits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExponentOutOfRange;

impl Number {
    /// The number a JSON literal's parts spell: `int` and `frac` are its
    /// integer and fraction digits, `exp` its exponent digits (empty when it
    /// has none). The parts must be ASCII digits; the parser guarantees it.
    pub(crate) fn from_literal(
        negative: bool,
        int: &str,
        frac: &str,
        exp_negative: bool,
        exp: &str,
    ) -> Result<Self, ExponentOutOfRange> {
        let exp_digits = exp.trim_start_matches('0');
        if exp_digits.len() > MAX_EXPONENT_DIGITS {
            return Err(ExponentOutOfRange);
        }
        let mut exponent: i128 = 0;
        for digit in exp_digits.bytes() {
            // At most 30 digits: below 10^30, far inside i128.
            exponent = exponent * 10 + i128::from(digit - b'0');
        }
        if exp_negative {
            exponent = -exponent;
        }
        // A literal is at most usize::MAX bytes, far inside i128.
        exponent -= i128::try_from(frac.len()).map_err(|_| ExponentOutOfRange)?;
        let mut digits: String = int.chars().chain(frac.chars()).collect();
        let leading = digits.len() - digits.trim_start_matches('0').len();
        digits.drain(..leading);
        let trimmed = digits.trim_end_matches('0').len();
        let trailing = digits.len() - trimmed;
        digits.truncate(trimmed);
        exponent += i128::try_from(trailing).map_err(|_| ExponentOutOfRange)?;
        if digits.is_empty() {
            return Ok(Self::zero());
        }
        Ok(Self {
            negative,
            digits,
            exponent,
        })
    }

    pub fn zero() -> Self {
        Self {
            negative: false,
            digits: String::new(),
            exponent: 0,
        }
    }

    pub fn is_zero(&self) -> bool {
        self.digits.is_empty()
    }

    /// The value as a `u64`, when it is a non-negative integer that fits.
    pub fn as_u64(&self) -> Option<u64> {
        if self.is_zero() {
            return Some(0);
        }
        if self.negative || self.exponent < 0 {
            return None;
        }
        let zeros = usize::try_from(self.exponent).ok()?;
        if self.digits.len().checked_add(zeros)? > 20 {
            return None;
        }
        let mut text = self.digits.clone();
        text.extend(std::iter::repeat_n('0', zeros));
        text.parse().ok()
    }

    /// The canonical text (module docs).
    pub fn canonical(&self) -> String {
        if self.is_zero() {
            return "0".to_owned();
        }
        let mut out = String::new();
        if self.negative {
            out.push('-');
        }
        let k = self.digits.len();
        // k is a string length, far inside i128.
        let k_wide = i128::try_from(k).unwrap_or(i128::MAX);
        // The decimal point sits after `n` digits: value = 0.digits × 10^n.
        let n = k_wide.saturating_add(self.exponent);
        match (n.cmp(&k_wide), n) {
            (Ordering::Equal | Ordering::Greater, ..=21) => {
                out.push_str(&self.digits);
                let zeros = usize::try_from(n - k_wide).unwrap_or(0);
                out.extend(std::iter::repeat_n('0', zeros));
            }
            (Ordering::Less, 1..=21) => {
                let at = usize::try_from(n).unwrap_or(0);
                out.push_str(&self.digits[..at]);
                out.push('.');
                out.push_str(&self.digits[at..]);
            }
            (_, -5..=0) => {
                out.push_str("0.");
                out.extend(std::iter::repeat_n('0', usize::try_from(-n).unwrap_or(0)));
                out.push_str(&self.digits);
            }
            _ => {
                out.push_str(&self.digits[..1]);
                if k > 1 {
                    out.push('.');
                    out.push_str(&self.digits[1..]);
                }
                let exponent = n - 1;
                out.push('e');
                out.push(if exponent < 0 { '-' } else { '+' });
                out.push_str(&exponent.unsigned_abs().to_string());
            }
        }
        out
    }
}
