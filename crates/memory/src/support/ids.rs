//! Deterministic ids for the entities a store creates itself.
//!
//! `IdentityResolver::merge` creates a `MergeId`, `ChannelRegistry::declare`
//! a `ChannelId`, `AlertRuleStore::create` an `AlertRuleId`, triage an
//! `AlertId`, and a config load its `AuditId`s. A real store mints ULIDs;
//! the in-memory stores take them from an [`IdSequence`], so two stores
//! built from equal sequences, given the same calls, create the same ids.
//! That is what lets the pipeline harnesses compare a store under test with
//! the reference without translating ids.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

/// Increasing 128-bit ids: `base + 1`, `base + 2`, … Clones share the
/// counter, so every clone of a store draws from one sequence. A later id
/// is greater, which is the time order the spec's ids need.
#[derive(Debug, Clone)]
pub struct IdSequence {
    base: u128,
    issued: Arc<AtomicU64>,
}

impl IdSequence {
    /// The default base: the first ULID timestamp millisecond, so no
    /// generated id falls in the range reserved for built-in alert rules (a
    /// zero ULID timestamp), and every generated id is far above the small
    /// ids tests choose by hand.
    pub const DEFAULT_BASE: u128 = 1 << 80;

    /// A sequence whose first id is `base + 1`. Give each source of ids
    /// that share one id type its own base so they never collide.
    pub fn new(base: u128) -> Self {
        Self {
            base,
            issued: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Draw the next id.
    pub fn next_ulid(&self) -> u128 {
        let issued = self.issued.fetch_add(1, Ordering::SeqCst) + 1;
        self.base + u128::from(issued)
    }

    /// The ids the next `n` draws would return, without drawing them: a
    /// store that must draw several ids only if a whole operation is
    /// accepted reads them here and draws them with [`IdSequence::skip`]
    /// once it is.
    pub fn peek(&self, n: usize) -> Vec<u128> {
        let issued = self.issued.load(Ordering::SeqCst);
        (1..=u64::try_from(n).unwrap_or(u64::MAX))
            .map(|k| self.base + u128::from(issued.saturating_add(k)))
            .collect()
    }

    /// Draw `n` ids at once, discarding them (the ones [`IdSequence::peek`]
    /// returned).
    pub fn skip(&self, n: usize) {
        self.issued
            .fetch_add(u64::try_from(n).unwrap_or(u64::MAX), Ordering::SeqCst);
    }
}

impl Default for IdSequence {
    fn default() -> Self {
        Self::new(Self::DEFAULT_BASE)
    }
}
