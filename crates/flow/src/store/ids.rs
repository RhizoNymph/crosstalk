//! Where the registry takes the ids of the channels it declares.
//!
//! `ChannelRegistry::declare` assigns a fresh id, and only an accepted
//! declaration takes one. The id is read inside the serializable
//! transaction, which may run more than once, so a source hands out a
//! pending id ([`ChannelIdSource::pending`]) and is told when a declaration
//! committed with it ([`ChannelIdSource::consume`]).

use std::sync::{Arc, Mutex};

use crosstalk_spec::ids::{ChannelId, RandomSource, UlidExhausted, UlidGenerator};
use crosstalk_spec::support::Timestamp;

/// Why no id could be drawn.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum IdSourceError {
    /// The ULID generator has no greater id left.
    #[error("{0}")]
    Exhausted(UlidExhausted),
    /// The generator's lock was poisoned by a panicking holder.
    #[error("the id generator's lock is poisoned")]
    Poisoned,
}

/// Fresh channel ids for declarations.
pub trait ChannelIdSource: Send + Sync {
    /// The id the next accepted declaration at `at` takes. A source may
    /// return a fresh id on every call (ids are never reused, so an id a
    /// refused or retried declaration saw is simply skipped) or the same one
    /// until it is consumed.
    fn pending(&self, at: Timestamp) -> Result<ChannelId, IdSourceError>;

    /// A declaration committed with `id`.
    fn consume(&self, id: ChannelId);
}

/// ULIDs stamped with the declaration's time, from a generator the source
/// owns. Every `pending` call mints a fresh id.
pub struct UlidChannelIds<R> {
    generator: Mutex<UlidGenerator<R>>,
}

impl<R: RandomSource> UlidChannelIds<R> {
    pub fn new(generator: UlidGenerator<R>) -> Self {
        Self {
            generator: Mutex::new(generator),
        }
    }
}

impl<R> std::fmt::Debug for UlidChannelIds<R> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UlidChannelIds").finish_non_exhaustive()
    }
}

impl<R: RandomSource> ChannelIdSource for UlidChannelIds<R> {
    fn pending(&self, at: Timestamp) -> Result<ChannelId, IdSourceError> {
        let mut generator = self.generator.lock().map_err(|_| IdSourceError::Poisoned)?;
        generator
            .mint_at::<ChannelId>(at)
            .map_err(IdSourceError::Exhausted)
    }

    fn consume(&self, _id: ChannelId) {}
}

impl<T: ChannelIdSource + ?Sized> ChannelIdSource for Arc<T> {
    fn pending(&self, at: Timestamp) -> Result<ChannelId, IdSourceError> {
        (**self).pending(at)
    }

    fn consume(&self, id: ChannelId) {
        (**self).consume(id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crosstalk_spec::ids::SeededRandom;
    use crosstalk_spec::support::Clock;

    #[derive(Debug)]
    struct Fixed;

    impl Clock for Fixed {
        fn now(&self) -> Timestamp {
            Timestamp::from_micros(5_000)
        }
    }

    #[test]
    fn ulid_ids_are_fresh_and_increasing() -> Result<(), IdSourceError> {
        let ids = UlidChannelIds::new(UlidGenerator::new(Arc::new(Fixed), SeededRandom::new(9)));
        let at = Timestamp::from_micros(1_000_000);
        let first = ids.pending(at)?;
        ids.consume(first);
        let second = ids.pending(at)?;
        assert!(second > first);
        Ok(())
    }
}
