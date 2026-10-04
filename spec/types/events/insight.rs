//! Events from analysis (L6), topology (L7) and the surface (L8).

use crate::aggregates::alert::Alert;
use crate::aggregates::edge::EdgeKey;
use crate::derived::flow::channel::policy::Policy;
use crate::derived::flow::transmission::{Classification, Route};
use crate::events::Subject;
use std::num::NonZeroU64;

use crate::aggregates::topic::TopicModelVersion;
use crate::ids::{AgentId, ChannelId, TransmissionId};
use crate::support::Timestamp;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClassificationCause {
    /// The transmission was just confirmed.
    Confirmation,
    /// A re-fit re-classified an existing transmission.
    Refit,
}

#[derive(Debug, Clone, PartialEq)]
pub enum InsightEvent {
    /// Published once per transmission per topic-model version: on
    /// confirmation under the current version, and again for every
    /// transmission when a re-fit produces a new version. Content alert rules
    /// (WatchedTopic, SemanticQuery) evaluate only `Confirmation`, so a re-fit
    /// never re-raises alerts about history.
    TransmissionClassified {
        cause: ClassificationCause,
        transmission: TransmissionId,
        from: AgentId,
        to: AgentId,
        route: Route,
        at: Timestamp,
        matched_bytes: NonZeroU64,
        classification: Classification,
    },
    /// Every transmission has been classified under `version`; readers may
    /// switch to it once they have applied `transmissions` classifications
    /// under it.
    TopicVersionReady {
        version: TopicModelVersion,
        transmissions: u64,
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
            Self::TopicVersionReady { .. } => Subject::TopicVersionReady,
            Self::EdgeUpdated(_) => Subject::EdgeUpdated,
            Self::AlertOpened(_) => Subject::AlertOpened,
            Self::PolicyChanged { .. } => Subject::PolicyChanged,
        }
    }
}
