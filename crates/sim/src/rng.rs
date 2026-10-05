//! The seed, the seeded random number generator, and the checked values
//! fault plans are written in ([`Probability`], [`DurationRange`]).
//!
//! Every random choice in a simulation comes from one [`Seed`]: the driver
//! builds the root [`SimRng`] from it, and every component forks its own
//! stream from the root ([`SimRng::fork`]) in the order the scenario builds
//! them, so the whole run is a function of the seed.

use std::fmt;
use std::num::{NonZeroU64, ParseIntError};
use std::str::FromStr;
use std::time::Duration;

use crosstalk_spec::ids::RandomSource;

/// The one number a simulation run is a function of. On the command line
/// and in failure reports, a decimal `u64` (`CROSSTALK_SIM_SEED=<n>`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Seed(u64);

impl Seed {
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u64 {
        self.0
    }
}

impl fmt::Display for Seed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl FromStr for Seed {
    type Err = ParseIntError;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        text.trim().parse().map(Self)
    }
}

/// SplitMix64 (Steele, Lea and Flood, *Fast splittable pseudorandom number
/// generators*): 64 bits of state, full period, and good enough statistics
/// for choosing faults and interleavings. Not for cryptography.
///
/// Deterministic across platforms and toolchains: the same seed yields the
/// same sequence everywhere.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SimRng {
    state: u64,
}

const GOLDEN_GAMMA: u64 = 0x9E37_79B9_7F4A_7C15;
const UNIT_SCALE: f64 = 1.0 / (1u64 << 53) as f64;

impl SimRng {
    pub fn new(seed: Seed) -> Self {
        Self { state: seed.0 }
    }

    pub fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(GOLDEN_GAMMA);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// A uniform value in `0..bound`, without modulo bias (Lemire's
    /// multiply-and-reject).
    pub fn below(&mut self, bound: NonZeroU64) -> u64 {
        let bound = bound.get();
        let mut product = u128::from(self.next_u64()) * u128::from(bound);
        // The low half of the product decides rejection; truncation is the
        // point of the cast.
        let mut low = product as u64;
        if low < bound {
            let threshold = bound.wrapping_neg() % bound;
            while low < threshold {
                product = u128::from(self.next_u64()) * u128::from(bound);
                low = product as u64;
            }
        }
        // The high half is below `bound`, so it fits in a u64.
        (product >> 64) as u64
    }

    /// A uniform index into a slice of `len` elements; `None` when `len` is
    /// zero.
    pub fn index(&mut self, len: usize) -> Option<usize> {
        let bound = NonZeroU64::new(u64::try_from(len).ok()?)?;
        usize::try_from(self.below(bound)).ok()
    }

    /// A uniform value in `[0, 1)` with 53 bits of precision.
    pub fn unit(&mut self) -> f64 {
        (self.next_u64() >> 11) as f64 * UNIT_SCALE
    }

    /// `true` with probability `p`. [`Probability::NEVER`] and
    /// [`Probability::ALWAYS`] consume no randomness, so turning a fault
    /// off does not shift the choices that follow.
    pub fn chance(&mut self, p: Probability) -> bool {
        if p == Probability::NEVER {
            false
        } else if p == Probability::ALWAYS {
            true
        } else {
            self.unit() < p.0
        }
    }

    /// A uniform duration in `range` (inclusive at both ends), to the
    /// nanosecond.
    pub fn duration_in(&mut self, range: DurationRange) -> Duration {
        let span = range.max.saturating_sub(range.min);
        let span_nanos = u64::try_from(span.as_nanos()).unwrap_or(u64::MAX);
        let offset = match NonZeroU64::new(span_nanos.wrapping_add(1)) {
            Some(bound) => self.below(bound),
            // span_nanos == u64::MAX: every u64 is in range.
            None => self.next_u64(),
        };
        range.min.saturating_add(Duration::from_nanos(offset))
    }

    /// A uniformly chosen element; `None` when `items` is empty.
    pub fn pick<'a, T>(&mut self, items: &'a [T]) -> Option<&'a T> {
        self.index(items.len()).and_then(|i| items.get(i))
    }

    /// Fisher-Yates: every permutation equally likely.
    pub fn shuffle<T>(&mut self, items: &mut [T]) {
        for last in (1..items.len()).rev() {
            if let Some(i) = self.index(last + 1) {
                items.swap(i, last);
            }
        }
    }

    /// An independent stream seeded from this one. Forking advances this
    /// stream by one draw, so the forks a scenario makes, in the order it
    /// makes them, are a function of the seed.
    pub fn fork(&mut self) -> SimRng {
        SimRng {
            state: self.next_u64() ^ GOLDEN_GAMMA.rotate_left(17),
        }
    }
}

/// A simulation's random stream is the spec's random source, so code that
/// mints ids (`crosstalk_spec::ids::UlidGenerator`) draws them from the
/// seed under simulation.
impl RandomSource for SimRng {
    fn next_u64(&mut self) -> u64 {
        SimRng::next_u64(self)
    }
}

/// A probability in `[0, 1]`, never NaN.
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd, Default)]
pub struct Probability(f64);

#[derive(Debug, Clone, Copy, PartialEq, thiserror::Error)]
#[error("a probability must be a number in [0, 1], got {got}")]
pub struct InvalidProbability {
    pub got: f64,
}

impl Probability {
    pub const NEVER: Self = Self(0.0);
    pub const ALWAYS: Self = Self(1.0);

    pub fn new(p: f64) -> Result<Self, InvalidProbability> {
        if (0.0..=1.0).contains(&p) {
            Ok(Self(p))
        } else {
            Err(InvalidProbability { got: p })
        }
    }

    /// `percent` per hundred; above 100 is refused.
    pub fn percent(percent: u8) -> Result<Self, InvalidProbability> {
        Self::new(f64::from(percent) / 100.0)
    }

    pub const fn get(self) -> f64 {
        self.0
    }

    /// For the presets' literals, which are in `[0, 1]`.
    pub(crate) const fn literal(p: f64) -> Self {
        Self(p)
    }
}

/// A closed range of durations `[min, max]` with `min <= max`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct DurationRange {
    min: Duration,
    max: Duration,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("a duration range needs min <= max, got {min:?}..={max:?}")]
pub struct InvalidDurationRange {
    pub min: Duration,
    pub max: Duration,
}

impl DurationRange {
    pub fn new(min: Duration, max: Duration) -> Result<Self, InvalidDurationRange> {
        if min <= max {
            Ok(Self { min, max })
        } else {
            Err(InvalidDurationRange { min, max })
        }
    }

    /// For the presets' literals, which have `min <= max`.
    pub(crate) const fn literal(min: Duration, max: Duration) -> Self {
        Self { min, max }
    }

    pub const fn exactly(duration: Duration) -> Self {
        Self {
            min: duration,
            max: duration,
        }
    }

    pub const fn min(self) -> Duration {
        self.min
    }

    pub const fn max(self) -> Duration {
        self.max
    }
}
