//! A seeded generator for [`DeliveryOrder::Shuffled`](crate::DeliveryOrder)
//! and the simulation tests: SplitMix64, small and fully determined by its
//! seed. Not for anything that needs unpredictability.

#[derive(Debug, Clone)]
pub(crate) struct SplitMix64(u64);

impl SplitMix64 {
    pub(crate) const fn new(seed: u64) -> Self {
        Self(seed)
    }

    pub(crate) fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// A value in `0..bound`; `0` when `bound` is zero.
    pub(crate) fn below(&mut self, bound: usize) -> usize {
        if bound == 0 {
            return 0;
        }
        // usize is at most 64 bits on every supported target, and the
        // remainder is below `bound`, so both conversions are lossless.
        (self.next_u64() % bound as u64) as usize
    }
}
