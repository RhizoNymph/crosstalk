//! The clock a live process stamps and ticks with.

use std::sync::Arc;

use crosstalk_memory::support::ManualClock;
use crosstalk_spec::support::{Clock, Timestamp};

/// The injected clock: a clock the process only reads (the wall clock, a
/// simulation's clock), or one [`Live::settle`](super::Live::settle)
/// moves (a corpus replay, a test).
#[derive(Clone)]
pub enum LiveClock {
    Read(Arc<dyn Clock>),
    Manual(ManualClock),
}

impl std::fmt::Debug for LiveClock {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Read(clock) => write!(formatter, "Read({:?})", clock.now()),
            Self::Manual(clock) => write!(formatter, "Manual({:?})", clock.now()),
        }
    }
}

impl LiveClock {
    /// The clock as every stage reads it.
    pub fn reader(&self) -> Arc<dyn Clock> {
        match self {
            Self::Read(clock) => Arc::clone(clock),
            Self::Manual(clock) => Arc::new(clock.clone()),
        }
    }

    pub fn now(&self) -> Timestamp {
        match self {
            Self::Read(clock) => clock.now(),
            Self::Manual(clock) => clock.now(),
        }
    }

    /// Move a manual clock forward to `until`; never backwards, and a read
    /// clock not at all. Returns the time it reads afterwards.
    pub fn advance_to(&self, until: Timestamp) -> Timestamp {
        if let Self::Manual(clock) = self
            && clock.now() < until
        {
            clock.set(until);
        }
        self.now()
    }
}
