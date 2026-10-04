//! The virtual wall clock: [`SimClock`] implements the spec's
//! [`Clock`] on top of tokio's paused time.
//!
//! A reading is `epoch + elapsed + offset`: `epoch` is the wall time the
//! run starts at ([`SimConfig::epoch`](crate::SimConfig)), `elapsed` the
//! simulated time since then (`tokio::time::Instant`, which only moves when
//! the runtime advances it), and `offset` what steps and skew added. Steps
//! change the wall clock and nothing else: timers and `tokio::time::Instant`
//! keep running monotonically, as they do when NTP steps a real clock.

use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::Duration;

use crosstalk_spec::support::{Clock, Timestamp};
use tokio::time::Instant;

use crate::trace::{FaultKind, FaultSite, NodeName, Tracer};

/// A step of a wall clock, as NTP makes one.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ClockStep {
    Forward(Duration),
    Back(Duration),
}

impl ClockStep {
    fn signed_micros(self) -> i64 {
        let micros = |d: Duration| i64::try_from(d.as_micros()).unwrap_or(i64::MAX);
        match self {
            Self::Forward(d) => micros(d),
            Self::Back(d) => micros(d).saturating_neg(),
        }
    }
}

/// A node's simulated wall clock. Clones share the clock: a step through
/// one is seen through all. [`SimClock::skewed`] makes another node's
/// clock, which steps independently.
#[derive(Debug, Clone)]
pub struct SimClock {
    epoch: Timestamp,
    start: Instant,
    offset: Arc<AtomicI64>,
    node: NodeName,
    tracer: Tracer,
}

impl SimClock {
    /// Must be called inside the simulation's runtime, so that `start` is
    /// simulated time; the driver does this for [`SimCtx::clock`](crate::SimCtx::clock).
    pub(crate) fn new(epoch: Timestamp, node: NodeName, tracer: Tracer) -> Self {
        Self {
            epoch,
            start: Instant::now(),
            offset: Arc::new(AtomicI64::new(0)),
            node,
            tracer,
        }
    }

    /// Another node's clock, reading this clock's time plus `skew` and
    /// stepping independently of it from now on.
    pub fn skewed(&self, node: &str, skew: ClockStep) -> SimClock {
        let offset = self
            .offset
            .load(Ordering::Relaxed)
            .saturating_add(skew.signed_micros());
        SimClock {
            epoch: self.epoch,
            start: self.start,
            offset: Arc::new(AtomicI64::new(offset)),
            node: NodeName::new(node),
            tracer: self.tracer.clone(),
        }
    }

    /// Steps the wall clock, recorded as a [`FaultKind::ClockStep`]. A
    /// backward step makes the next readings earlier than the last ones,
    /// and repeats them until simulated time catches up.
    pub fn step(&self, step: ClockStep) {
        self.offset
            .try_update(Ordering::Relaxed, Ordering::Relaxed, |offset| {
                Some(offset.saturating_add(step.signed_micros()))
            })
            // The closure always returns `Some`, so the update cannot fail;
            // either arm carries the previous offset.
            .unwrap_or_else(|previous| previous);
        self.tracer
            .fault(FaultKind::ClockStep, &self.node, FaultSite::Clock);
    }

    pub fn node(&self) -> &NodeName {
        &self.node
    }
}

impl Clock for SimClock {
    fn now(&self) -> Timestamp {
        let elapsed = i128::try_from(self.start.elapsed().as_micros()).unwrap_or(i128::MAX);
        let micros = i128::from(self.epoch.as_micros())
            .saturating_add(elapsed)
            .saturating_add(i128::from(self.offset.load(Ordering::Relaxed)));
        let clamped = micros.clamp(0, i128::from(u64::MAX));
        Timestamp::from_micros(u64::try_from(clamped).unwrap_or(u64::MAX))
    }
}
