//! A clock for the computation doubles that read the time.
//!
//! No store reads a clock: every store method takes its time as an argument
//! (`crosstalk_spec::interfaces`, "Time is an argument"). A computation
//! that has to stamp what it returns without a time argument, such as
//! `TopicModel::fit` stamping its topics' `fitted_at`, reads the spec's
//! [`Clock`]; [`ManualClock`] is one a test moves.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use crosstalk_spec::support::{Clock, Timestamp};

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
