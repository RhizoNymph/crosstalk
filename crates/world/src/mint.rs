//! The world's ids: ULIDs from the spec's generator, each minted at the
//! time of the entity it names, so ids sort by time like the gateway's.
//!
//! A [`UlidGenerator`] is monotonic: asked for an id at a time before its
//! last one, it returns the next id after the last, whose time is not the
//! time asked for. The world generates entities out of time order (an
//! agent's sub-agent before the parent's first transmission, transmissions
//! at random times), so [`Mint`] makes a fresh generator for every id,
//! seeded from the world's own stream: each id carries exactly its
//! entity's time, and the same seed mints the same ids.

use std::sync::Arc;

use crosstalk_spec::ids::{EntityId, SeededRandom, UlidGenerator};
use crosstalk_spec::support::{Clock, Timestamp};

use crate::error::WorldError;
use crate::rng::Rng;

/// Mints entity ids from one seeded stream.
pub struct Mint {
    rng: Rng,
    /// The clock a generator needs; never read, every id is minted at an
    /// explicit time.
    clock: Arc<dyn Clock + Send + Sync>,
}

impl std::fmt::Debug for Mint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Mint")
            .field("rng", &self.rng)
            .finish_non_exhaustive()
    }
}

impl Mint {
    /// The mint of stream `label` of `seed`. `clock` is any clock: a
    /// generator requires one, and the mint never reads it.
    pub fn new(seed: u64, label: &str, clock: Arc<dyn Clock + Send + Sync>) -> Self {
        Self {
            rng: Rng::fork(seed, label),
            clock,
        }
    }

    /// A fresh id whose time is `at`.
    pub fn at<I: EntityId>(&mut self, at: Timestamp) -> Result<I, WorldError> {
        let random = SeededRandom::new(self.rng.next_u64());
        let mut generator = UlidGenerator::new(Arc::clone(&self.clock) as Arc<dyn Clock>, random);
        Ok(generator.mint_at(at)?)
    }
}

#[cfg(test)]
mod tests {
    use crosstalk_spec::ids::AgentId;
    use crosstalk_spec::ids::mint::ulid_millis;

    use super::*;
    use crate::clock::{Anchor, HOUR, UI_ANCHOR, WorldClock};

    fn mint(seed: u64) -> Result<Mint, WorldError> {
        let clock = WorldClock::Fixed(Anchor::new(UI_ANCHOR)?);
        Ok(Mint::new(seed, "test", Arc::new(clock)))
    }

    #[test]
    fn ids_carry_their_time_even_out_of_order() -> Result<(), WorldError> {
        let mut mint = mint(1)?;
        let late = crate::clock::minus(UI_ANCHOR, HOUR);
        let early = crate::clock::minus(UI_ANCHOR, 5 * HOUR);
        let a: AgentId = mint.at(late)?;
        let b: AgentId = mint.at(early)?;
        assert_eq!(ulid_millis(a.as_ulid()) * 1000, late.as_micros());
        assert_eq!(ulid_millis(b.as_ulid()) * 1000, early.as_micros());
        assert!(b < a);
        Ok(())
    }

    #[test]
    fn same_seed_same_ids_and_ids_are_distinct() -> Result<(), WorldError> {
        let (mut a, mut b) = (mint(7)?, mint(7)?);
        let mut seen = std::collections::BTreeSet::new();
        for _ in 0..1000 {
            let x: AgentId = a.at(UI_ANCHOR)?;
            let y: AgentId = b.at(UI_ANCHOR)?;
            assert_eq!(x, y);
            assert!(seen.insert(x));
        }
        Ok(())
    }
}
