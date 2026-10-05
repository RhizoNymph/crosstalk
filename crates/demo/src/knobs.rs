//! Checked knob values shared by the subcommands: inclusive ranges
//! (`A..B` or `A`), fractions in `[0, 1]`, durations (`500ms`, `90s`,
//! `5m`, `1h`, or bare seconds), and the seeded random stream every
//! choice draws from.

use std::fmt;
use std::str::FromStr;
use std::time::Duration;

use crosstalk_spec::ids::{RandomSource, SeededRandom};

/// Why a knob value is refused.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum KnobError {
    #[error("{0:?} is not a number")]
    Number(String),
    #[error("range {0:?}: the start is above the end")]
    Inverted(String),
    #[error("{0:?} is not a fraction between 0 and 1")]
    Fraction(String),
    #[error("{0:?} is not a duration (500ms, 90s, 5m, 1h)")]
    Duration(String),
    #[error("{0:?} must be at least 1")]
    Zero(String),
}

/// An inclusive range `low..=high`, written `A..B` (or `A` for one value).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Span {
    low: u64,
    high: u64,
}

impl Span {
    /// `low..=high`; `None` when `low > high`.
    pub const fn new(low: u64, high: u64) -> Option<Self> {
        if low > high {
            None
        } else {
            Some(Self { low, high })
        }
    }

    /// The range between `a` and `b`, whichever is lower first.
    pub const fn ordered(a: u64, b: u64) -> Self {
        if a <= b {
            Self { low: a, high: b }
        } else {
            Self { low: b, high: a }
        }
    }

    pub const fn low(self) -> u64 {
        self.low
    }

    pub const fn high(self) -> u64 {
        self.high
    }

    /// A value in the range drawn from `rng`.
    pub fn draw(self, rng: &mut Rng) -> u64 {
        self.low + rng.below(self.high - self.low + 1)
    }

    /// As milliseconds, a duration drawn from `rng`.
    pub fn draw_ms(self, rng: &mut Rng) -> Duration {
        Duration::from_millis(self.draw(rng))
    }
}

impl FromStr for Span {
    type Err = KnobError;

    fn from_str(text: &str) -> Result<Self, KnobError> {
        let number = |part: &str| {
            part.trim()
                .parse::<u64>()
                .map_err(|_| KnobError::Number(part.to_owned()))
        };
        let (low, high) = match text.split_once("..") {
            Some((low, high)) => (number(low)?, number(high.trim_start_matches('='))?),
            None => {
                let value = number(text)?;
                (value, value)
            }
        };
        Self::new(low, high).ok_or_else(|| KnobError::Inverted(text.to_owned()))
    }
}

impl fmt::Display for Span {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.low == self.high {
            write!(f, "{}", self.low)
        } else {
            write!(f, "{}..{}", self.low, self.high)
        }
    }
}

/// A [`Span`] that never includes zero.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PositiveSpan(Span);

impl PositiveSpan {
    /// The range between `a` and `b`, with zero raised to one.
    pub const fn ordered(a: u64, b: u64) -> Self {
        let a = if a == 0 { 1 } else { a };
        let b = if b == 0 { 1 } else { b };
        Self(Span::ordered(a, b))
    }

    pub const fn get(self) -> Span {
        self.0
    }
}

impl FromStr for PositiveSpan {
    type Err = KnobError;

    fn from_str(text: &str) -> Result<Self, KnobError> {
        let span: Span = text.parse()?;
        if span.low == 0 {
            return Err(KnobError::Zero(text.to_owned()));
        }
        Ok(Self(span))
    }
}

/// A probability in `[0, 1]`.
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd)]
pub struct Fraction(f64);

impl Fraction {
    pub const ZERO: Fraction = Fraction(0.0);
    pub const ONE: Fraction = Fraction(1.0);

    /// `value` when it lies in `[0, 1]`.
    pub fn new(value: f64) -> Option<Self> {
        (0.0..=1.0).contains(&value).then_some(Self(value))
    }

    pub const fn get(self) -> f64 {
        self.0
    }
}

impl FromStr for Fraction {
    type Err = KnobError;

    fn from_str(text: &str) -> Result<Self, KnobError> {
        text.trim()
            .parse::<f64>()
            .ok()
            .and_then(Self::new)
            .ok_or_else(|| KnobError::Fraction(text.to_owned()))
    }
}

/// Parses `500ms`, `90s`, `5m`, `1h` or bare seconds.
pub fn parse_duration(text: &str) -> Result<Duration, KnobError> {
    let bad = || KnobError::Duration(text.to_owned());
    let trimmed = text.trim();
    let (digits, unit_ms): (&str, u64) = if let Some(n) = trimmed.strip_suffix("ms") {
        (n, 1)
    } else if let Some(n) = trimmed.strip_suffix('s') {
        (n, 1_000)
    } else if let Some(n) = trimmed.strip_suffix('m') {
        (n, 60_000)
    } else if let Some(n) = trimmed.strip_suffix('h') {
        (n, 3_600_000)
    } else {
        (trimmed, 1_000)
    };
    let value: u64 = digits.trim().parse().map_err(|_| bad())?;
    value
        .checked_mul(unit_ms)
        .map(Duration::from_millis)
        .ok_or_else(bad)
}

/// A seeded random stream (the spec's SplitMix64).
#[derive(Debug, Clone)]
pub struct Rng(SeededRandom);

impl Rng {
    pub const fn new(seed: u64) -> Self {
        Self(SeededRandom::new(seed))
    }

    /// A stream keyed by `seed` and `parts`, so independent streams (one per
    /// agent, one per request body) never share draws.
    pub fn derive(seed: u64, parts: &[&[u8]]) -> Self {
        let mut hasher = blake3::Hasher::new();
        hasher.update(&seed.to_le_bytes());
        for part in parts {
            hasher.update(&(part.len() as u64).to_le_bytes());
            hasher.update(part);
        }
        let digest = hasher.finalize();
        let mut first = [0u8; 8];
        first.copy_from_slice(&digest.as_bytes()[..8]);
        Self::new(u64::from_le_bytes(first))
    }

    pub fn next_u64(&mut self) -> u64 {
        self.0.next_u64()
    }

    /// Uniform in `0..bound` (`0` when `bound` is 0).
    pub fn below(&mut self, bound: u64) -> u64 {
        if bound == 0 {
            return 0;
        }
        // Rejection sampling keeps every value equally likely.
        let zone = u64::MAX - (u64::MAX % bound);
        loop {
            let draw = self.next_u64();
            if draw < zone {
                return draw % bound;
            }
        }
    }

    /// Uniform in `0..len` as an index.
    pub fn index(&mut self, len: usize) -> usize {
        usize::try_from(self.below(len as u64)).unwrap_or(0)
    }

    /// Uniform in `[0, 1)`.
    pub fn unit(&mut self) -> f64 {
        // 53 random bits: every value exactly representable.
        (self.next_u64() >> 11) as f64 / (1u64 << 53) as f64
    }

    /// True with probability `p`.
    pub fn chance(&mut self, p: Fraction) -> bool {
        self.unit() < p.get()
    }

    /// One element of a non-empty slice; `None` for an empty one.
    pub fn pick<'a, T>(&mut self, items: &'a [T]) -> Option<&'a T> {
        if items.is_empty() {
            None
        } else {
            items.get(self.index(items.len()))
        }
    }

    /// `n` lowercase hex digits.
    pub fn hex(&mut self, n: usize) -> String {
        const DIGITS: &[u8; 16] = b"0123456789abcdef";
        (0..n).map(|_| char::from(DIGITS[self.index(16)])).collect()
    }

    /// `n` base62 characters, as Anthropic ids use.
    pub fn base62(&mut self, n: usize) -> String {
        const DIGITS: &[u8; 62] = b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
        (0..n).map(|_| char::from(DIGITS[self.index(62)])).collect()
    }
}
