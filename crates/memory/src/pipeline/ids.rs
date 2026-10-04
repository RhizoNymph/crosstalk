//! Deterministic ids for the entities a store creates itself.
//!
//! `IdentityResolver::merge` creates a `MergeId` and
//! `ChannelRegistry::declare` a `ChannelId`. A real store mints ULIDs; the
//! in-memory stores take them from an [`IdSequence`], so two stores built
//! from equal sequences, given the same calls, create the same ids. That is
//! what lets the model-based harnesses compare a store under test with the
//! reference without translating ids.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

/// Increasing 128-bit ids: `prefix` in the high 64 bits and a counter in
/// the low 64. Clones share the counter. Ids are time-sortable in the sense
/// the spec needs: a later id is greater.
#[derive(Debug, Clone)]
pub struct IdSequence {
    prefix: u64,
    next: Arc<AtomicU64>,
}

impl IdSequence {
    /// A sequence whose ids start at `prefix << 64 | 1`. Give each kind of
    /// id, and each source of caller-chosen ids, its own prefix so they
    /// never collide.
    pub fn new(prefix: u64) -> Self {
        Self {
            prefix,
            next: Arc::new(AtomicU64::new(1)),
        }
    }

    /// The next id.
    pub fn next_ulid(&self) -> u128 {
        let counter = self.next.fetch_add(1, Ordering::SeqCst);
        (u128::from(self.prefix) << 64) | u128::from(counter)
    }
}

impl Default for IdSequence {
    /// Prefix `0x5EED`: well clear of the small ids tests choose by hand.
    fn default() -> Self {
        Self::new(0x5EED)
    }
}
