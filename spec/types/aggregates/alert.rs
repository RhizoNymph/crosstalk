//! Alert rules and alerts.
//!
//! A rule evaluation produces an [`AlertDraft`] (the lifecycle's `Fired`).
//! Triage either opens an [`Alert`] or folds the draft into an open alert
//! with the same rule and subject, so `Deduplicated` is a
//! [`TriageOutcome`], not a stored state.
//!
//! ```text
//! draft ─triage─┬─▶ Open ─acknowledge─▶ Acknowledged ─resolve─▶ Resolved
//!               │     └──────┬──────────────┘
//!               │      channel sanctioned
//!               │            ▼
//!               │       Suppressed
//!               └─▶ deduplicated into an existing alert
//! ```

use crate::aggregates::topic::Embedding;
use crate::ids::{AgentId, AlertId, AlertRuleId, ChannelId, OperatorId, TopicId, TransmissionId};
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
    /// A declared, sanctioned channel saw no traffic within its idle window.
    SanctionedUnused,
    /// A transmission was left with access-pattern evidence only.
    SuspectedTransmission,
    WatchedTopic {
        topics: NonEmpty<TopicId>,
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
    pub enabled: bool,
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
    /// An open alert with the same rule and subject already exists; its
    /// occurrence count goes up instead.
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SuppressReason {
    ChannelSanctioned,
    RuleDisabled,
}
