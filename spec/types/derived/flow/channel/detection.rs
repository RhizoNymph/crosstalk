//! Detection state of a channel: what its traffic shows.
//!
//! ```text
//! declared:   AwaitingTraffic ─idle─▶ Unused
//!                   │                   │
//!                   └──── access ───────┴─▶ InUse(Observed)
//!
//! traffic:    Observed ─cross access─▶ Candidate ─confirm─▶ Active ─idle─▶ Dormant
//!                                          ▲                                 │
//!                                          └────────── cross access ─────────┘
//! ```
//!
//! A cross access is a read by an agent other than an earlier writer.
//!
//! A promoted channel keeps the `TrafficDetection` it had when it was
//! discovered and continues on the traffic machine. A superseded channel's
//! detection is frozen: a confirmation of a transmission whose stored route
//! names it is a confirmation on the channel that superseded it, and moves
//! that channel's detection (`confirm` above) instead.

use crate::derived::flow::evidence::CoAccess;
use crate::ids::{AccessId, TransmissionId};
use crate::support::Timestamp;

/// Detection for a channel declared before any traffic.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DeclaredDetection {
    /// No traffic yet, and the idle window has not closed.
    AwaitingTraffic,
    /// No traffic by the time the idle window closed. Flow publishes
    /// `DeclaredChannelUnused` whatever the policy; the `SanctionedUnused`
    /// alert rule (L6) checks whether the policy is sanctioned.
    Unused {
        since: Timestamp,
    },
    InUse(TrafficDetection),
}

/// Detection once a channel has traffic.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TrafficDetection {
    /// Accessed, but not yet written by one agent and read by another.
    Observed { first_access: AccessId },
    /// Written by one agent and read by another; no content match yet.
    Candidate { first_cross_access: CoAccess },
    /// At least one confirmed transmission.
    Active {
        since: Timestamp,
        last_transmission: TransmissionId,
    },
    /// Was active; no confirmed transmission within the idle window.
    Dormant {
        since: Timestamp,
        last_transmission: TransmissionId,
    },
}

/// Which detection state a channel is in, without its data: what a graph
/// node shows.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DetectionKind {
    AwaitingTraffic,
    Unused,
    Observed,
    Candidate,
    Active,
    Dormant,
}

impl DetectionKind {
    pub fn of_traffic(detection: &TrafficDetection) -> Self {
        match detection {
            TrafficDetection::Observed { .. } => Self::Observed,
            TrafficDetection::Candidate { .. } => Self::Candidate,
            TrafficDetection::Active { .. } => Self::Active,
            TrafficDetection::Dormant { .. } => Self::Dormant,
        }
    }

    pub fn of_declared(detection: &DeclaredDetection) -> Self {
        match detection {
            DeclaredDetection::AwaitingTraffic => Self::AwaitingTraffic,
            DeclaredDetection::Unused { .. } => Self::Unused,
            DeclaredDetection::InUse(traffic) => Self::of_traffic(traffic),
        }
    }
}
