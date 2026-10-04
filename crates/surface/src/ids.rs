//! The ids the surface mints itself: audit entries, exports, projection
//! jobs and the envelopes it publishes.
//!
//! One [`UlidGenerator`] over the injected clock, behind a mutex: minting is
//! a short synchronous step that never awaits, so the lock is never held
//! across an `.await`, and the generator's monotonicity holds across every
//! task that shares the surface.

use std::sync::{Arc, Mutex, PoisonError};

use crosstalk_spec::ids::EntityId;
use crosstalk_spec::ids::mint::{SeededRandom, UlidExhausted, UlidGenerator};
use crosstalk_spec::support::Clock;

/// Mints ULIDs of any entity type from one monotonic generator.
#[derive(Debug, Clone)]
pub struct IdMinter {
    generator: Arc<Mutex<UlidGenerator<SeededRandom>>>,
}

impl IdMinter {
    /// A minter reading `clock` and drawing from `random`
    /// (`SeededRandom::new` in tests and simulations,
    /// `SeededRandom::from_entropy` in a running gateway).
    pub fn new(clock: Arc<dyn Clock>, random: SeededRandom) -> Self {
        Self {
            generator: Arc::new(Mutex::new(UlidGenerator::new(clock, random))),
        }
    }

    /// The next id. `UlidExhausted` only past the year 10889.
    pub fn mint<I: EntityId>(&self) -> Result<I, UlidExhausted> {
        // A poisoned lock still holds a valid generator: `mint` cannot
        // panic between reading and writing its state.
        self.generator
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .mint()
    }
}
