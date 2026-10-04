//! A small deterministic PRNG (SplitMix64), so the fixture needs no `rand`
//! dependency and the same seed always yields the same world.

/// SplitMix64: fast, statistically decent, and trivially seedable.
#[derive(Debug, Clone)]
pub struct Rng {
    state: u64,
}

impl Rng {
    pub fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    /// An independent stream derived from this seed and a label, so adding
    /// draws to one part of the generator does not shift every other part.
    pub fn fork(seed: u64, label: &str) -> Self {
        let mut mixed = seed ^ 0x9e37_79b9_7f4a_7c15;
        for byte in label.bytes() {
            mixed = mix(mixed ^ u64::from(byte));
        }
        Self::new(mixed)
    }

    pub fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9e37_79b9_7f4a_7c15);
        mix(self.state)
    }

    /// Uniform in `[0, 1)`.
    pub fn unit(&mut self) -> f64 {
        // 53 random bits into the mantissa.
        (self.next_u64() >> 11) as f64 / (1u64 << 53) as f64
    }

    /// Uniform in `[0, n)`; 0 when `n` is 0.
    pub fn below(&mut self, n: u64) -> u64 {
        if n == 0 {
            return 0;
        }
        // Multiply-shift: unbiased enough for a fixture and branch-free.
        ((u128::from(self.next_u64()) * u128::from(n)) >> 64) as u64
    }

    /// Uniform index into a slice of length `len`; 0 when `len` is 0.
    pub fn index(&mut self, len: usize) -> usize {
        usize::try_from(self.below(len as u64)).unwrap_or(0)
    }

    /// Uniform in `[low, high]`.
    pub fn between(&mut self, low: u64, high: u64) -> u64 {
        if high <= low {
            return low;
        }
        low + self.below(high - low + 1)
    }

    pub fn chance(&mut self, p: f64) -> bool {
        self.unit() < p
    }

    pub fn pick<'a, T>(&mut self, items: &'a [T]) -> Option<&'a T> {
        if items.is_empty() {
            return None;
        }
        items.get(self.index(items.len()))
    }

    /// An index chosen with probability proportional to its weight. `None`
    /// when every weight is zero.
    pub fn weighted(&mut self, weights: &[f64]) -> Option<usize> {
        let total: f64 = weights.iter().filter(|w| **w > 0.0).sum();
        if total <= 0.0 {
            return None;
        }
        let mut target = self.unit() * total;
        let mut last = None;
        for (i, w) in weights.iter().enumerate() {
            if *w <= 0.0 {
                continue;
            }
            last = Some(i);
            if target < *w {
                return Some(i);
            }
            target -= *w;
        }
        last
    }

    /// A standard normal draw (Box-Muller).
    pub fn gaussian(&mut self) -> f64 {
        let u1 = self.unit().max(f64::MIN_POSITIVE);
        let u2 = self.unit();
        (-2.0 * u1.ln()).sqrt() * (std::f64::consts::TAU * u2).cos()
    }

    pub fn bytes32(&mut self) -> [u8; 32] {
        let mut out = [0u8; 32];
        for chunk in out.chunks_mut(8) {
            chunk.copy_from_slice(&self.next_u64().to_le_bytes());
        }
        out
    }

    /// Fisher-Yates shuffle.
    pub fn shuffle<T>(&mut self, items: &mut [T]) {
        for i in (1..items.len()).rev() {
            let j = self.index(i + 1);
            items.swap(i, j);
        }
    }
}

fn mix(mut z: u64) -> u64 {
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn same_seed_same_stream() {
        let mut a = Rng::new(42);
        let mut b = Rng::new(42);
        for _ in 0..100 {
            assert_eq!(a.next_u64(), b.next_u64());
        }
    }

    #[test]
    fn forks_differ_by_label() {
        let mut a = Rng::fork(1, "agents");
        let mut b = Rng::fork(1, "channels");
        assert_ne!(a.next_u64(), b.next_u64());
    }

    #[test]
    fn bounded_draws_stay_in_range() {
        let mut rng = Rng::new(7);
        for _ in 0..1000 {
            assert!(rng.below(10) < 10);
            let u = rng.unit();
            assert!((0.0..1.0).contains(&u));
            let b = rng.between(3, 5);
            assert!((3..=5).contains(&b));
        }
        assert_eq!(rng.below(0), 0);
    }

    #[test]
    fn weighted_skips_zero_weights() {
        let mut rng = Rng::new(9);
        for _ in 0..200 {
            assert_eq!(rng.weighted(&[0.0, 1.0, 0.0]), Some(1));
        }
        assert_eq!(rng.weighted(&[0.0, 0.0]), None);
    }
}
