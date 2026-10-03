//! Events from analysis (L6), topology (L7) and the surface (L8).

use crate::aggregates::alert::Alert;
use crate::aggregates::edge::EdgeKey;
use crate::derived::flow::channel::policy::Policy;
use crate::derived::flow::transmission::{Classification, Route};
use crate::events::Subject;
use crate::ids::{AgentId, ChannelId, TransmissionId};

#[derive(Debug, Clone, PartialEq)]
pub enum InsightEvent {
    TransmissionClassified {
        transmission: TransmissionId,
        from: AgentId,
        to: AgentId,
        route: Route,
        classification: Classification,
    },
    EdgeUpdated(EdgeKey),
    AlertOpened(Alert),
    /// From the surface: an operator or config changed a channel's policy.
    /// Flow detection applies it; alert triage suppresses alerts on newly
    /// sanctioned channels.
    PolicyChanged {
        channel: ChannelId,
        policy: Policy,
    },
}

impl InsightEvent {
    pub fn subject(&self) -> Subject {
        match self {
            Self::TransmissionClassified { .. } => Subject::TransmissionClassified,
            Self::EdgeUpdated(_) => Subject::EdgeUpdated,
            Self::AlertOpened(_) => Subject::AlertOpened,
            Self::PolicyChanged { .. } => Subject::PolicyChanged,
        }
    }
}
