//! Time for stores whose trait needs a "now" it does not pass in.
//!
//! `ChannelRegistry::declare` stamps a declaration with no time argument,
//! and `FingerprintIndex::frequency` counts observations "within the
//! retention period" of an unstated now. The in-memory stores read both
//! from a [`Clock`], which a test or the simulation drives.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use crosstalk_spec::support::Timestamp;

/// A source of the current time.
pub trait Clock: Send + Sync {
    fn now(&self) -> Timestamp;
}

/// A clock that moves only when told to. Clones share one time.
#[derive(Debug, Clone, Default)]
pub struct ManualClock {
    micros: Arc<AtomicU64>,
}

impl ManualClock {
    pub fn at(now: Timestamp) -> Self {
        Self {
            micros: Arc::new(AtomicU64::new(now.as_micros())),
        }
    }

    /// Set the time. It may move backwards: a test can replay a past.
    pub fn set(&self, now: Timestamp) {
        self.micros.store(now.as_micros(), Ordering::SeqCst);
    }
}

impl Clock for ManualClock {
    fn now(&self) -> Timestamp {
        Timestamp::from_micros(self.micros.load(Ordering::SeqCst))
    }
}
