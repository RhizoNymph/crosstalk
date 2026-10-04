//! Events from analysis (L6), topology (L7) and the surface (L8).

use crate::aggregates::alert::{Alert, AlertRevision, AlertRuleDef, RuleRevision};
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
    /// From topology (L7): every bucket of `version` is complete, and graph
    /// and series queries now read `version`'s buckets instead of
    /// `previous`'s. Published once `EdgeStore::activate` has switched, never
    /// for a version older than the active one. The topic catalog marks
    /// `version` active and every older version superseded by it.
    TopicVersionActivated {
        version: TopicModelVersion,
        previous: TopicModelVersion,
    },
    EdgeUpdated(EdgeKey),
    /// Revision 1 (`AlertRevision::OPENED`).
    AlertOpened(Alert),
    /// A stored alert changed: triage (L6) folded a draft into it or
    /// suppressed it, or an operator (L8) acknowledged or resolved it.
    /// `alert` is the alert after the change and `revision` its new revision.
    AlertChanged {
        alert: Alert,
        revision: AlertRevision,
    },
    /// A rule was created or changed: by an operator (L8, through
    /// `AlertRuleStore`), or by L6 when it went stale. `rule` is the rule
    /// after the change and `revision` its new revision
    /// ([`RuleRevision::CREATED`] for a new rule).
    AlertRuleChanged {
        rule: AlertRuleDef,
        revision: RuleRevision,
    },
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
            Self::TopicVersionActivated { .. } => Subject::TopicVersionActivated,
            Self::EdgeUpdated(_) => Subject::EdgeUpdated,
            Self::AlertOpened(_) => Subject::AlertOpened,
            Self::AlertChanged { .. } => Subject::AlertChanged,
            Self::AlertRuleChanged { .. } => Subject::AlertRuleChanged,
            Self::PolicyChanged { .. } => Subject::PolicyChanged,
        }
    }
}
