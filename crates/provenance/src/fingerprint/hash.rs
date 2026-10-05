//! The k-gram hash: a rolling polynomial hash over Unicode scalar values
//! modulo the Mersenne prime 2^61 − 1, finished by the SplitMix64 mixer.
//!
//! Fingerprints are stored and compared across processes and builds
//! (`provenance.fingerprint.reproducible`), so the hash is a fixed function
//! of the k-gram's normalized characters and `k`: no per-process seed, no
//! `std` hasher. Arithmetic modulo a prime (rather than modulo 2^64) avoids
//! the Thue–Morse collisions every power-of-two polynomial hash has. The
//! mixer is a bijection on `u64`, so it spreads the 61-bit residues over
//! all 64 bits: winnowing's minimum and `Fingerprint::shard`'s modulo both
//! see uniform values. Changing anything here is a migration of every
//! stored fingerprint; the golden vectors in the tests pin it.

/// 2^61 − 1.
const MODULUS: u64 = (1 << 61) - 1;

/// The polynomial's base, a fixed residue.
const BASE: u64 = 0x1F35_A7BD_9C6E_2B41 % MODULUS;

fn reduce(mut value: u64) -> u64 {
    while value >= MODULUS {
        value -= MODULUS;
    }
    value
}

fn mul(a: u64, b: u64) -> u64 {
    let product = u128::from(a) * u128::from(b);
    // The low 61 bits plus the high bits: x = hi * 2^61 + lo ≡ hi + lo.
    let low = u64::try_from(product & u128::from(MODULUS)).unwrap_or(0);
    let high = u64::try_from(product >> 61).unwrap_or(0);
    reduce(reduce(low) + reduce(high))
}

fn add(a: u64, b: u64) -> u64 {
    reduce(a + b)
}

fn sub(a: u64, b: u64) -> u64 {
    reduce(a + MODULUS - b)
}

/// A character's value in the polynomial: its scalar value plus one, so no
/// character is zero.
fn value(ch: char) -> u64 {
    u64::from(u32::from(ch)) + 1
}

/// SplitMix64's finalizer.
fn mix(mut z: u64) -> u64 {
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

/// The hash of every window of `k` consecutive characters of `chars`, in
/// order: `chars.len() - k + 1` values, none when `chars` is shorter.
pub fn rolling(chars: &[char], k: usize) -> Vec<u64> {
    if k == 0 || chars.len() < k {
        return Vec::new();
    }
    let mut top = 1u64;
    for _ in 1..k {
        top = mul(top, BASE);
    }
    let salt = u64::try_from(k).unwrap_or(u64::MAX).rotate_left(56);
    let mut hash = 0u64;
    for ch in &chars[..k] {
        hash = add(mul(hash, BASE), value(*ch));
    }
    let mut out = Vec::with_capacity(chars.len() - k + 1);
    out.push(mix(hash ^ salt));
    for index in k..chars.len() {
        let leaving = value(chars[index - k]);
        hash = add(mul(sub(hash, mul(leaving, top)), BASE), value(chars[index]));
        out.push(mix(hash ^ salt));
    }
    out
}

/// The hash of exactly the `k` characters `chars` (one k-gram), equal to
/// the value [`rolling`] gives that window.
pub fn kgram(chars: &[char]) -> u64 {
    rolling(chars, chars.len()).first().copied().unwrap_or(0)
}

/// Prefix hashes of a character sequence: the hash of any window in O(1),
/// equal to [`kgram`] of that window.
#[derive(Debug, Clone)]
pub struct Prefix {
    /// `prefix[i]` is the polynomial of the first `i` characters.
    prefix: Vec<u64>,
    /// `powers[n]` is `BASE^n`.
    powers: Vec<u64>,
}

impl Prefix {
    pub fn new(chars: &[char]) -> Self {
        let mut prefix = Vec::with_capacity(chars.len() + 1);
        let mut powers = Vec::with_capacity(chars.len() + 1);
        prefix.push(0);
        powers.push(1);
        for ch in chars {
            let last = prefix.last().copied().unwrap_or(0);
            prefix.push(add(mul(last, BASE), value(*ch)));
            let power = powers.last().copied().unwrap_or(1);
            powers.push(mul(power, BASE));
        }
        Self { prefix, powers }
    }

    /// The hash of characters `start..end`, as [`kgram`] gives it; `None`
    /// for an empty or out-of-range window.
    pub fn window(&self, start: usize, end: usize) -> Option<u64> {
        if start >= end || end >= self.prefix.len() {
            return None;
        }
        let high = *self.prefix.get(end)?;
        let low = mul(*self.prefix.get(start)?, *self.powers.get(end - start)?);
        let length = u64::try_from(end - start).unwrap_or(u64::MAX);
        Some(mix(sub(high, low) ^ length.rotate_left(56)))
    }
}

/// Mixed into the hash of a whole short value, so it never equals the
/// k-gram fingerprint of the same characters: a short-span hash names a
/// whole value, a k-gram any window.
const SHORT_DOMAIN: u64 = 0x5348_4F52_545F_5350;

/// The exact hash of a whole short value, from its window hash
/// ([`kgram`] or [`Prefix::window`]).
pub fn short(window: u64) -> u64 {
    mix(window ^ SHORT_DOMAIN)
}
