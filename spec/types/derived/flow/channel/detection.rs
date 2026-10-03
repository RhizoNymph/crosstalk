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

use crate::derived::flow::evidence::CoAccess;
use crate::ids::{AccessId, TransmissionId};
use crate::support::Timestamp;

/// Detection for a channel declared in config.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DeclaredDetection {
    /// No traffic yet, and the idle window has not closed.
    AwaitingTraffic,
    /// No traffic by the time the idle window closed. Raises
    /// `SanctionedUnused` when the channel's policy is sanctioned.
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
