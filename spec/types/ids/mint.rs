//! Minting entity ids: the ULID generator.
//!
//! A ULID is 128 bits: a 48-bit Unix time in milliseconds, then 80 random
//! bits. A [`UlidGenerator`] reads its time from the spec's [`Clock`] and
//! its randomness from a [`RandomSource`], both handed to it, so under
//! simulation (a virtual clock and a seeded source) its ids are a function
//! of the seed (`canonical.clock.injected`).
//!
//! **Time.** [`UlidGenerator::next_ulid`] stamps an id with the clock's
//! reading; [`UlidGenerator::next_at`] with a time the caller passes, for
//! an id that carries the time of what it names (ingress stamps an
//! exchange id with the exchange's start). Both go through the same
//! monotonic rule below.
//!
//! **Monotonic.** Every id a generator mints is greater than the one before
//! it, whatever its clock reads or the time it is given
//! (`canonical.ids.ulid-monotonic`). When that time is in the same
//! millisecond as the last id, or an earlier one (an NTP step back, a
//! skewed node, an earlier exchange stamped later), the next id is the last plus one: the
//! random part is incremented, never redrawn, and a carry out of it moves
//! the id into the next millisecond. A later millisecond draws fresh
//! randomness. So one generator never repeats an id, and two generators
//! repeat one only if they draw the same 80 random bits in the same
//! millisecond (`canonical.ids.ulid-unique`).
//!
//! **Sources.** [`SeededRandom`] is SplitMix64: [`SeededRandom::new`] for
//! simulation and tests, [`SeededRandom::from_entropy`] for a running
//! gateway, seeded from the operating system's randomness through the
//! standard library.
//!
//! [`Clock`]: crate::support::Clock

use std::collections::hash_map::RandomState;
use std::fmt;
use std::hash::{BuildHasher, Hasher};
use std::sync::Arc;

use super::EntityId;
use crate::support::{Clock, Timestamp};

/// Random 64-bit words: where a [`UlidGenerator`] draws the random part of
/// an id.
pub trait RandomSource: Send {
    fn next_u64(&mut self) -> u64;
}

/// SplitMix64 (Steele, Lea and Flood): 64 bits of state, full period, the
/// same sequence for a seed on every platform. Not for keys or tokens.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SeededRandom {
    state: u64,
}

impl SeededRandom {
    pub const fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    /// A source seeded from the operating system's randomness: the
    /// standard library seeds each thread's `RandomState` keys from it, and
    /// a fresh `RandomState`'s SipHash of a fixed word is a draw keyed by
    /// them. For a running gateway; simulations and tests use
    /// [`SeededRandom::new`].
    pub fn from_entropy() -> Self {
        let mut hasher = RandomState::new().build_hasher();
        hasher.write_u64(0x6372_6f73_7374_616c);
        Self::new(hasher.finish())
    }
}

impl RandomSource for SeededRandom {
    fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }
}

/// The largest time a ULID holds: 2^48 - 1 milliseconds after the epoch
/// (in the year 10889). A later clock reading is taken as this.
pub const MAX_ULID_MILLIS: u64 = (1 << 48) - 1;

const RANDOM_BITS: u32 = 80;

/// The last id was the largest ULID there is, so no greater one exists.
/// Reachable only with a clock past [`MAX_ULID_MILLIS`] and 2^80 ids minted
/// in that millisecond.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UlidExhausted;

impl fmt::Display for UlidExhausted {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("no ULID is greater than the last one minted")
    }
}

impl std::error::Error for UlidExhausted {}

/// Mints ULIDs, monotonic per generator (module docs).
///
/// Mutably borrowed per id: a component owns its generator, or one task
/// owns a node's and hands ids out over a channel.
pub struct UlidGenerator<R> {
    clock: Arc<dyn Clock>,
    random: R,
    last: Option<u128>,
}

impl<R: RandomSource> UlidGenerator<R> {
    pub fn new(clock: Arc<dyn Clock>, random: R) -> Self {
        Self {
            clock,
            random,
            last: None,
        }
    }

    /// The next ULID: greater than every one this generator minted before,
    /// stamped with the clock's reading.
    pub fn next_ulid(&mut self) -> Result<u128, UlidExhausted> {
        let now = self.clock.now();
        self.next_at(now)
    }

    /// The next ULID stamped with `at` instead of the clock's reading: for
    /// an id that carries the time of what it names (an exchange's id
    /// carries the instant the exchange started). Monotonic like
    /// [`UlidGenerator::next_ulid`], with which it shares the last id: when
    /// `at` is in the last id's millisecond or an earlier one, the next id
    /// is the last plus one, so its time can be later than `at`.
    pub fn next_at(&mut self, at: Timestamp) -> Result<u128, UlidExhausted> {
        let millis = (at.as_micros() / 1000).min(MAX_ULID_MILLIS);
        let id = match self.last {
            Some(last) if millis <= ulid_millis(last) => {
                last.checked_add(1).ok_or(UlidExhausted)?
            }
            _ => (u128::from(millis) << RANDOM_BITS) | self.random_part(),
        };
        self.last = Some(id);
        Ok(id)
    }

    /// The next ULID as an entity id of type `I`.
    pub fn mint<I: EntityId>(&mut self) -> Result<I, UlidExhausted> {
        self.next_ulid().map(I::from_ulid)
    }

    /// The next ULID stamped with `at` ([`UlidGenerator::next_at`]) as an
    /// entity id of type `I`.
    pub fn mint_at<I: EntityId>(&mut self, at: Timestamp) -> Result<I, UlidExhausted> {
        self.next_at(at).map(I::from_ulid)
    }

    /// 80 random bits: one whole draw and the top 16 bits of another.
    fn random_part(&mut self) -> u128 {
        let high = u128::from(self.random.next_u64());
        let low = u128::from(self.random.next_u64() >> 48);
        (high << 16) | low
    }
}

/// The millisecond a ULID carries.
pub fn ulid_millis(ulid: u128) -> u64 {
    // The top 48 bits of a u128 always fit a u64.
    u64::try_from(ulid >> RANDOM_BITS).unwrap_or(u64::MAX)
}

impl<R> fmt::Debug for UlidGenerator<R> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("UlidGenerator")
            .field("last", &self.last)
            .finish_non_exhaustive()
    }
}
