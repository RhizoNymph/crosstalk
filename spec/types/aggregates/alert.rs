//! Alert rules and alerts.
//!
//! A rule evaluation produces an [`AlertDraft`] (the lifecycle's `Fired`).
//! Triage either opens an [`Alert`] or folds the draft into an active (open
//! or acknowledged) alert with the same rule and subject, so `Deduplicated`
//! is a [`TriageOutcome`], not a stored state.
//!
//! Sanctioning a channel suppresses the active alerts whose subject is that
//! channel; alerts about transmissions on it stay, because content can be
//! worth flagging on a sanctioned channel. Disabling a rule suppresses its
//! active alerts.
//!
//! ```text
//! draft ─triage─┬─▶ Open ─acknowledge─▶ Acknowledged ─resolve─▶ Resolved
//!               │     └──────┬──────────────┘
//!               │      channel sanctioned
//!               │            ▼
//!               │       Suppressed
//!               └─▶ deduplicated into an existing alert
//! ```
//!
//! Every stored change to an alert (a deduplicated occurrence, a
//! suppression, an acknowledgement, a resolution) bumps its
//! [`AlertRevision`] by one and publishes `AlertChanged` with the alert after
//! the change. Changes to one alert are compare-and-set on its revision, so
//! revisions are consecutive and a reader that keeps the highest revision it
//! has seen ends with the stored alert, whatever order the events arrive in.

use crate::aggregates::topic::{Embedding, TopicModelVersion};
use crate::ids::{AgentId, AlertId, AlertRuleId, ChannelId, OperatorId, TopicId, TransmissionId};
use std::num::NonZeroU32;

use crate::support::{NonEmpty, Similarity, Timestamp};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AlertRuleKind {
    NewChannel,
    UnreviewedTraffic,
    UnsanctionedTraffic,
    SanctionedUnused,
    SuspectedTransmission,
    WatchedTopic,
    SemanticQuery,
}

#[derive(Debug, Clone, PartialEq)]
pub enum AlertRule {
    /// A channel was discovered that no config declared.
    NewChannel,
    /// Confirmed traffic on a channel whose policy is unreviewed.
    UnreviewedTraffic,
    /// Confirmed traffic on a channel whose policy is unsanctioned.
    UnsanctionedTraffic,
    /// A declared channel whose policy is sanctioned saw no traffic within
    /// its idle window. Flow reports every unused declared channel; this rule
    /// checks the policy.
    SanctionedUnused,
    /// A transmission was left with access-pattern evidence only.
    SuspectedTransmission,
    /// Topic ids only mean something within one topic-model version. When a
    /// new version becomes ready (`TopicVersionReady`), each topic is
    /// remapped to the new version's topic whose centroid is most similar, if
    /// that similarity reaches `remap_threshold`; a rule with any topic left
    /// unmapped becomes [`RuleStatus::Stale`] instead of silently watching the
    /// wrong topics. Remapping at that moment switches the rule in step with
    /// new confirmations' classifications.
    WatchedTopic {
        version: TopicModelVersion,
        topics: NonEmpty<TopicId>,
        remap_threshold: Similarity,
    },
    SemanticQuery {
        query: Embedding,
        threshold: Similarity,
    },
}

impl AlertRule {
    pub fn kind(&self) -> AlertRuleKind {
        match self {
            Self::NewChannel => AlertRuleKind::NewChannel,
            Self::UnreviewedTraffic => AlertRuleKind::UnreviewedTraffic,
            Self::UnsanctionedTraffic => AlertRuleKind::UnsanctionedTraffic,
            Self::SanctionedUnused => AlertRuleKind::SanctionedUnused,
            Self::SuspectedTransmission => AlertRuleKind::SuspectedTransmission,
            Self::WatchedTopic { .. } => AlertRuleKind::WatchedTopic,
            Self::SemanticQuery { .. } => AlertRuleKind::SemanticQuery,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct AlertRuleDef {
    pub id: AlertRuleId,
    pub rule: AlertRule,
    pub status: RuleStatus,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuleStatus {
    Enabled,
    Disabled,
    /// A watched-topic rule that could not be remapped after a re-fit. It
    /// evaluates nothing until an operator updates it. Its active alerts stay
    /// active: they were valid when raised.
    Stale,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AlertSubject {
    Channel(ChannelId),
    Transmission(TransmissionId),
    Agent(AgentId),
}

/// A rule's output, before triage.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlertDraft {
    pub rule: AlertRuleId,
    pub subject: AlertSubject,
    pub raised_at: Timestamp,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TriageOutcome {
    Opened(Alert),
    /// An active (open or acknowledged) alert with the same rule and subject
    /// already exists; its occurrence count goes up instead.
    Deduplicated {
        into: AlertId,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Alert {
    pub id: AlertId,
    pub rule: AlertRuleId,
    pub subject: AlertSubject,
    pub raised_at: Timestamp,
    pub occurrences: u32,
    pub state: AlertState,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AlertState {
    Open,
    Acknowledged {
        by: OperatorId,
        at: Timestamp,
    },
    Resolved {
        by: OperatorId,
        at: Timestamp,
        note: Option<String>,
    },
    /// The condition stopped being alert-worthy, e.g. the channel was
    /// sanctioned.
    Suppressed {
        at: Timestamp,
        reason: SuppressReason,
    },
}

/// How many stored changes an alert has had: 1 when opened, one more per
/// change.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct AlertRevision(NonZeroU32);

impl AlertRevision {
    /// The revision `AlertOpened` carries.
    pub const OPENED: Self = Self(NonZeroU32::MIN);

    pub const fn new(revision: NonZeroU32) -> Self {
        Self(revision)
    }

    pub const fn get(self) -> NonZeroU32 {
        self.0
    }

    /// The revision after one more change. `None` once the counter is
    /// exhausted; the store rejects that change.
    pub fn next(self) -> Option<Self> {
        self.0.checked_add(1).map(Self)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SuppressReason {
    ChannelSanctioned,
    RuleDisabled,
}
