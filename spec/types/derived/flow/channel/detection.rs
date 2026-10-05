//! Detection state of a channel: what its traffic shows.
//!
//! A channel's traffic is its cross-agent transmissions: transmissions
//! routed through it whose sender and reader are different agents. A
//! co-access opens one (a write by one agent, then a read by another,
//! [`CoAccess`](crate::derived::flow::evidence::CoAccess)), and content
//! evidence may confirm it later. Accesses alone are not traffic: a
//! resource only one agent touches, or one written and never read by
//! anyone else, is a resource, not a channel (`l5_flow`, "Discovery"), and
//! a declared channel whose resources see only such accesses is still
//! awaiting traffic.
//!
//! ```text
//! declared:   AwaitingTraffic ─idle─▶ Unused
//!                   │                   │
//!                   └── cross-agent ────┴─▶ InUse(Active)
//!                       transmission
//!
//! traffic:    Active ─idle─▶ Dormant ─cross-agent transmission─▶ Active
//! ```
//!
//! A discovered channel is created by its first cross-agent transmission,
//! so it starts `Active`: no discovered channel ever has a state without
//! traffic. A promoted channel keeps the `TrafficDetection` it had when it
//! was discovered and continues on the traffic machine. A superseded
//! channel's detection is frozen: a transmission whose stored route names
//! it moves the detection of the channel that superseded it instead.
//!
//! Whether any of that traffic is confirmed is a separate axis, decided at
//! read time from the transmissions themselves, after merged agents
//! resolve ([`confirmation`](super::confirmation)): detection says when a
//! channel carried traffic, confirmation what evidence backs it.

use serde::{Deserialize, Serialize};

use crate::ids::TransmissionId;
use crate::support::Timestamp;

/// Detection for a channel declared before any traffic.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "type",
    content = "data",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum DeclaredDetection {
    /// No cross-agent transmission yet, and the idle window has not closed.
    AwaitingTraffic,
    /// No cross-agent transmission by the time the idle window closed.
    /// Flow publishes `DeclaredChannelUnused` whatever the policy; the
    /// `SanctionedUnused` alert rule (L6) checks whether the policy is
    /// sanctioned.
    Unused {
        since: Timestamp,
    },
    InUse(TrafficDetection),
}

/// Detection once a channel has cross-agent traffic.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "type",
    content = "data",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum TrafficDetection {
    /// A cross-agent transmission was opened or confirmed through it within
    /// the idle window. `since` is when it last became active: its first
    /// cross-agent transmission, or the one that ended a dormant spell.
    Active {
        since: Timestamp,
        last_transmission: TransmissionId,
    },
    /// Was active; no cross-agent transmission within the idle window.
    Dormant {
        since: Timestamp,
        last_transmission: TransmissionId,
    },
}

impl TrafficDetection {
    /// The last cross-agent transmission opened or confirmed through it.
    pub fn last_transmission(&self) -> TransmissionId {
        match self {
            Self::Active {
                last_transmission, ..
            }
            | Self::Dormant {
                last_transmission, ..
            } => *last_transmission,
        }
    }
}

/// Which detection state a channel is in, without its data: what a graph
/// node shows.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DetectionKind {
    AwaitingTraffic,
    Unused,
    Active,
    Dormant,
}

impl DetectionKind {
    pub fn of_traffic(detection: &TrafficDetection) -> Self {
        match detection {
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
