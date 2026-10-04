//! Channel lists, resources and promotion (items 1, 5 and 16).

use crosstalk_spec::derived::flow::channel::detection::{DeclaredDetection, TrafficDetection};
use crosstalk_spec::derived::flow::channel::policy::Policy;
use crosstalk_spec::derived::flow::channel::{Channel, ChannelOrigin};
use crosstalk_spec::derived::flow::resource::Resource;
use crosstalk_spec::ids::{AgentId, ChannelId, OperatorId};
use crosstalk_spec::interfaces::l8_surface::PolicyKind;
use crosstalk_spec::support::Timestamp;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum OriginKind {
    Declared,
    Discovered,
}

/// Every detection state of either origin, flattened for display and
/// filtering.
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
    pub fn of(origin: &ChannelOrigin) -> Self {
        match origin {
            ChannelOrigin::Declared { detection, .. } => match detection {
                DeclaredDetection::AwaitingTraffic => Self::AwaitingTraffic,
                DeclaredDetection::Unused { .. } => Self::Unused,
                DeclaredDetection::InUse(traffic) => Self::of_traffic(traffic),
            },
            ChannelOrigin::Discovered { detection, .. } => Self::of_traffic(detection),
        }
    }

    fn of_traffic(traffic: &TrafficDetection) -> Self {
        match traffic {
            TrafficDetection::Observed { .. } => Self::Observed,
            TrafficDetection::Candidate { .. } => Self::Candidate,
            TrafficDetection::Active { .. } => Self::Active,
            TrafficDetection::Dormant { .. } => Self::Dormant,
        }
    }
}

impl OriginKind {
    pub fn of(origin: &ChannelOrigin) -> Self {
        match origin {
            ChannelOrigin::Declared { .. } => Self::Declared,
            ChannelOrigin::Discovered { .. } => Self::Discovered,
        }
    }
}

pub fn policy_kind(policy: &Policy) -> PolicyKind {
    match policy {
        Policy::Unreviewed(_) => PolicyKind::Unreviewed,
        Policy::Sanctioned(_) => PolicyKind::Sanctioned,
        Policy::Unsanctioned(_) => PolicyKind::Unsanctioned,
    }
}

/// A discovered channel taken over by a declared channel through
/// `PromoteChannel`. A superseded channel accepts no new resources; its id
/// resolves to `into` at read time.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Supersession {
    pub into: ChannelId,
    pub by: OperatorId,
    pub at: Timestamp,
}

/// A row in the channels list, and the head of the channel page.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChannelSummary {
    pub channel: Channel,
    /// The seed resource of a discovered channel, for display.
    pub seed: Option<Resource>,
    pub superseded: Option<Supersession>,
    pub writers: u32,
    pub readers: u32,
    pub transmissions: u64,
    pub last_activity: Option<Timestamp>,
}

/// Restricts the channels list. Empty lists do not restrict.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ChannelListFilter {
    pub origins: Vec<OriginKind>,
    pub detections: Vec<DetectionKind>,
    pub policies: Vec<PolicyKind>,
    pub include_superseded: bool,
}

/// One resource of a channel with who wrote and read it in a window.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResourceUse {
    pub resource: Resource,
    pub writers: Vec<(AgentId, u64)>,
    pub readers: Vec<(AgentId, u64)>,
}
