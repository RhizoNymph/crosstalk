//! A small seeded generator for synthetic corpora.
//!
//! SplitMix64: one `u64` of state, a fixed output function, no platform or
//! library dependence. The same seed gives the same corpus on every run and
//! every machine, which is all a corpus generator needs; it is not for
//! anything secret.

/// A SplitMix64 stream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SplitMix64 {
    state: u64,
}

impl SplitMix64 {
    pub fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    /// A stream derived from `seed` and a label, so independent choices
    /// (which pair, which payload) do not shift when another one changes.
    pub fn derived(seed: u64, label: &str) -> Self {
        let mut mixed = Self::new(seed);
        for byte in label.bytes() {
            mixed.state ^= u64::from(byte);
            mixed.next_u64();
        }
        Self::new(mixed.next_u64())
    }

    pub fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// A number in `0..bound`; `None` when `bound` is zero. Rejection
    /// sampling keeps it unbiased.
    pub fn below(&mut self, bound: u64) -> Option<u64> {
        if bound == 0 {
            return None;
        }
        let zone = u64::MAX - u64::MAX % bound;
        loop {
            let draw = self.next_u64();
            if draw < zone {
                return Some(draw % bound);
            }
        }
    }

    /// An index into a slice of `len` items; `None` when it is empty.
    pub fn index(&mut self, len: usize) -> Option<usize> {
        let bound = u64::try_from(len).ok()?;
        self.below(bound).and_then(|at| usize::try_from(at).ok())
    }

    /// One item of `items`, uniformly.
    pub fn pick<'a, T>(&mut self, items: &'a [T]) -> Option<&'a T> {
        self.index(items.len()).and_then(|at| items.get(at))
    }

    /// Shuffles `items` in place (Fisher–Yates).
    pub fn shuffle<T>(&mut self, items: &mut [T]) {
        for last in (1..items.len()).rev() {
            if let Some(other) = self.index(last + 1) {
                items.swap(last, other);
            }
        }
    }
}
