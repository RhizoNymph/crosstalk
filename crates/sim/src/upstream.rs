//! Upstream faults: what a fake upstream (crosstalk-testkit's) should do to
//! one exchange, drawn from an [`UpstreamFaults`] plan.
//!
//! The kit does not serve HTTP. A fake upstream asks
//! [`UpstreamFaultInjector::next_exchange`] once per exchange it accepts
//! and acts the fault out: refuse the connection, answer with the status,
//! cut the stream after that many chunks, or stop sending for the stall.

use std::sync::{Mutex, PoisonError};
use std::time::Duration;

use crate::node::NodeHandle;
use crate::plan::{ErrorStatus, UpstreamFaults};
use crate::rng::SimRng;
use crate::trace::{FaultKind, FaultSite};

/// The fault one exchange gets.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum UpstreamFault {
    /// Accept no connection: no response head ever arrives.
    Unreachable,
    /// Answer with this status before any content.
    Status(ErrorStatus),
    /// End the response stream after this many chunks, before it finished.
    Truncate { after_chunks: u32 },
    /// Stop sending for this long partway through the stream.
    Stall(Duration),
}

impl UpstreamFault {
    pub const fn kind(self) -> FaultKind {
        match self {
            Self::Unreachable => FaultKind::UpstreamUnreachable,
            Self::Status(_) => FaultKind::UpstreamStatus,
            Self::Truncate { .. } => FaultKind::UpstreamTruncate,
            Self::Stall(_) => FaultKind::UpstreamStall,
        }
    }
}

/// Draws upstream faults for one node's fake upstream.
#[derive(Debug)]
pub struct UpstreamFaultInjector {
    faults: UpstreamFaults,
    rng: Mutex<SimRng>,
    node: NodeHandle,
}

impl UpstreamFaultInjector {
    pub fn new(faults: UpstreamFaults, rng: SimRng, node: NodeHandle) -> Self {
        Self {
            faults,
            rng: Mutex::new(rng),
            node,
        }
    }

    /// The fault for the next exchange, at most one, recorded in the
    /// trace; `None` for a clean exchange.
    pub fn next_exchange(&self) -> Option<UpstreamFault> {
        let fault = self.draw();
        if let Some(fault) = fault {
            self.node.fault(fault.kind(), FaultSite::Upstream);
        }
        fault
    }

    fn draw(&self) -> Option<UpstreamFault> {
        let mut rng = self.rng.lock().unwrap_or_else(PoisonError::into_inner);
        if rng.chance(self.faults.unreachable) {
            return Some(UpstreamFault::Unreachable);
        }
        if let Some(status) = &self.faults.error_status
            && rng.chance(status.chance)
        {
            let statuses: Vec<ErrorStatus> = status.statuses.iter().copied().collect();
            let chosen = rng
                .pick(&statuses)
                .copied()
                .unwrap_or(*status.statuses.first());
            return Some(UpstreamFault::Status(chosen));
        }
        if let Some(truncate) = self.faults.truncate
            && rng.chance(truncate.chance)
        {
            let after_chunks = rng
                .index(
                    usize::try_from(truncate.max_chunks)
                        .unwrap_or(usize::MAX)
                        .saturating_add(1),
                )
                .and_then(|n| u32::try_from(n).ok())
                .unwrap_or(0);
            return Some(UpstreamFault::Truncate { after_chunks });
        }
        if let Some(stall) = self.faults.stall
            && rng.chance(stall.chance)
        {
            return Some(UpstreamFault::Stall(rng.duration_in(stall.within)));
        }
        None
    }
}
