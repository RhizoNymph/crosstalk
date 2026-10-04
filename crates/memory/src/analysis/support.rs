//! Small pieces the L6–L8 reference stores share: a clock for the trait
//! methods that take no time, deterministic id sequences, the outbox each
//! store publishes into, and the one similarity function every score uses.

use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use crosstalk_spec::aggregates::topic::Embedding;
use crosstalk_spec::events::changed::Changed;
use crosstalk_spec::events::insight::InsightEvent;
use crosstalk_spec::support::{Similarity, Timestamp};

/// Locks `mutex`, recovering the state from a poisoned lock. Every store
/// mutates its state only through methods that leave it consistent before
/// they can panic, so a poisoned lock still guards a valid state.
pub(crate) fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Where a store reads the time for the trait methods that do not take
/// one (`AlertTriage`'s suppressions, `TopicCatalog::unpin`'s retention).
pub trait Clock: Send + Sync {
    fn now(&self) -> Timestamp;
}

/// A clock the test sets. Starts where it is built and never moves on its
/// own.
#[derive(Debug, Clone)]
pub struct ManualClock {
    now: Arc<Mutex<Timestamp>>,
}

impl ManualClock {
    pub fn new(start: Timestamp) -> Self {
        Self {
            now: Arc::new(Mutex::new(start)),
        }
    }

    /// Set the time. A clock may be set backwards; the stores never assume
    /// it is monotone.
    pub fn set(&self, at: Timestamp) {
        *lock(&self.now) = at;
    }
}

impl Clock for ManualClock {
    fn now(&self) -> Timestamp {
        *lock(&self.now)
    }
}

/// A deterministic sequence of 128-bit ULID values for the ids a store
/// assigns: `base + 1`, `base + 2`, … The base puts every value outside the
/// range reserved for built-in alert rules (a zero ULID timestamp), so the
/// same sequence serves every id kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IdSequence {
    base: u128,
    issued: u64,
}

impl IdSequence {
    /// The first ULID timestamp millisecond a generated id uses: 1, so no
    /// generated id is ever in the reserved range.
    pub const DEFAULT_BASE: u128 = 1 << 80;

    pub const fn new(base: u128) -> Self {
        Self { base, issued: 0 }
    }

    /// The next raw ULID.
    pub fn next_raw(&mut self) -> u128 {
        self.issued += 1;
        self.base + u128::from(self.issued)
    }
}

impl Default for IdSequence {
    fn default() -> Self {
        Self::new(Self::DEFAULT_BASE)
    }
}

/// What a store published after a committed change, in commit order.
// A bus event is large next to a `Changed`; the outbox holds a handful at a
// time, so boxing would only add noise at every match.
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, PartialEq)]
pub enum Published {
    /// A bus event (`AlertOpened`, `AlertChanged`, `AlertRuleChanged`,
    /// `TopicVersionDropped`, `WatermarkAdvanced`, `TopicVersionActivated`).
    Insight(InsightEvent),
    /// A live-feed notification.
    Changed(Changed),
}

/// The events a store has published and nobody has drained yet. Appended in
/// the same critical section as the change it announces, so nothing is
/// announced before it is visible.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Outbox {
    events: Vec<Published>,
}

impl Outbox {
    pub fn insight(&mut self, event: InsightEvent) {
        self.events.push(Published::Insight(event));
    }

    pub fn changed(&mut self, changed: Changed) {
        self.events.push(Published::Changed(changed));
    }

    /// Everything published since the last drain, oldest first.
    pub fn drain(&mut self) -> Vec<Published> {
        std::mem::take(&mut self.events)
    }
}

/// The similarity of two embeddings: their cosine (the dot product, both
/// being unit vectors) clamped to `0.0..=1.0`, so anti-correlated vectors
/// score 0. `None` when they come from different models, which are never
/// compared.
pub fn similarity(a: &Embedding, b: &Embedding) -> Option<Similarity> {
    if a.model() != b.model() {
        return None;
    }
    let dot: f32 = a.values().iter().zip(b.values()).map(|(x, y)| x * y).sum();
    // Clamped into the range, so the constructor cannot refuse it. Anything
    // not above zero (negative, -0.0, and NaN, impossible for checked
    // embeddings) becomes +0.0, so equal scores compare equal everywhere.
    let clamped = if dot > 0.0 { dot.min(1.0) } else { 0.0 };
    Similarity::new(clamped).ok()
}
