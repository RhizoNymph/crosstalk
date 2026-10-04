//! Alert rules and alerts.
//!
//! A rule evaluation produces an [`AlertDraft`] (the lifecycle's `Fired`).
//! Triage either opens an [`Alert`] or folds the draft into an active (open
//! or acknowledged) alert with the same rule and subject, so `Deduplicated`
//! is a [`TriageOutcome`], not a stored state. A draft whose rule stopped
//! evaluating before triage opens nothing (`RuleInactive`).
//!
//! Sanctioning a channel suppresses the active alerts whose subject is that
//! channel, or a channel it superseded (subjects are compared
//! [`AlertSubject::resolved`]); alerts about transmissions on it stay,
//! because content can be worth flagging on a sanctioned channel. A
//! promotion that sets `Sanctioned` sanctions the promoted channel.
//! Deduplication compares stored subjects, so alerts on a superseded channel
//! stay under its id and later traffic raises alerts on the superseding
//! channel. Disabling a rule suppresses its
//! active alerts. Dismissing a suspected transmission suppresses its
//! `SuspectedTransmission` alerts.
//!
//! Operators create and update content rules ([`ContentRule`]) and enable or
//! disable any rule. A rule keeps its kind for life. Updating a rule leaves
//! its alerts as they are.
//!
//! ```text
//! draft ─triage─┬─▶ Open ─acknowledge─▶ Acknowledged ─resolve─▶ Resolved
//!               │     └──────┬──────────────┘
//!               │   sanctioned, rule disabled
//!               │   or transmission dismissed
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
use crate::aliases::Aliases;
use crate::ids::{AgentId, AlertId, AlertRuleId, ChannelId, OperatorId, TopicId, TransmissionId};
use std::num::NonZeroU32;

use crate::support::{NonBlank, NonEmpty, Similarity, Timestamp};

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
    /// unmapped becomes [`TopicWatch::Stale`] instead of silently watching the
    /// wrong topics. Remapping at that moment switches the rule in step with
    /// new confirmations' classifications.
    ///
    /// The remap is [`TopicLineage::remap`] over the lineage from the rule's
    /// version to the new one, the same lineage the UI shows, so the two
    /// cannot disagree.
    ///
    /// [`TopicLineage::remap`]: crate::aggregates::topic_history::TopicLineage::remap
    WatchedTopic {
        watch: TopicWatch,
        remap_threshold: Similarity,
    },
    SemanticQuery {
        /// What the operator wrote; `query` is its embedding.
        text: NonBlank,
        query: Embedding,
        threshold: Similarity,
    },
}

/// Topics of one topic-model version.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WatchedTopics {
    pub version: TopicModelVersion,
    pub topics: NonEmpty<TopicId>,
}

/// Whether a watched-topic rule still names topics that mean something.
/// Only watched-topic rules can be stale.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TopicWatch {
    Current(WatchedTopics),
    /// A re-fit to `unmapped_in` left a topic without a close counterpart.
    /// The rule evaluates nothing until an operator updates it, which is the
    /// only way back to `Current`. Its active alerts stay active: they were
    /// valid when raised. Produced only by
    /// [`TopicLineage::remap`](crate::aggregates::topic_history::TopicLineage::remap).
    Stale {
        last: WatchedTopics,
        unmapped_in: TopicModelVersion,
        /// The topics of `last` whose best link into `unmapped_in` is absent
        /// or below the rule's threshold, in the rule's order.
        unmapped: NonEmpty<TopicId>,
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

    pub fn is_stale(&self) -> bool {
        matches!(
            self,
            Self::WatchedTopic {
                watch: TopicWatch::Stale { .. },
                ..
            }
        )
    }
}

/// A rule an operator can create and edit: the content rules. The other
/// kinds take no parameters; config provisions one rule of each, which
/// operators can only enable and disable. A watched-topic definition is
/// always current, so updating a stale rule makes it current.
#[derive(Debug, Clone, PartialEq)]
pub enum ContentRule {
    WatchedTopic {
        topics: WatchedTopics,
        remap_threshold: Similarity,
    },
    SemanticQuery {
        text: NonBlank,
        query: Embedding,
        threshold: Similarity,
    },
}

impl From<ContentRule> for AlertRule {
    fn from(rule: ContentRule) -> Self {
        match rule {
            ContentRule::WatchedTopic {
                topics,
                remap_threshold,
            } => Self::WatchedTopic {
                watch: TopicWatch::Current(topics),
                remap_threshold,
            },
            ContentRule::SemanticQuery {
                text,
                query,
                threshold,
            } => Self::SemanticQuery {
                text,
                query,
                threshold,
            },
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct AlertRuleDef {
    pub id: AlertRuleId,
    pub rule: AlertRule,
    pub status: RuleStatus,
}

/// What an operator set. Staleness is separate ([`TopicWatch`]), so a rule
/// can be disabled and stale at once, and enabling a stale rule does not
/// make it evaluate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuleStatus {
    Enabled,
    Disabled,
}

/// An update that would change a rule's kind. A rule keeps its kind for
/// life, so its alerts keep their meaning.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct KindChanged {
    pub from: AlertRuleKind,
    pub to: AlertRuleKind,
}

impl AlertRuleDef {
    /// Whether the rule produces drafts: enabled and not stale.
    pub fn evaluates(&self) -> bool {
        self.status == RuleStatus::Enabled && !self.rule.is_stale()
    }

    /// Replace the definition, keeping id and status. A stale watched-topic
    /// rule becomes current. Rejects a definition of another kind, leaving
    /// the rule unchanged.
    pub fn update(&mut self, definition: ContentRule) -> Result<(), KindChanged> {
        let rule = AlertRule::from(definition);
        if rule.kind() != self.rule.kind() {
            return Err(KindChanged {
                from: self.rule.kind(),
                to: rule.kind(),
            });
        }
        self.rule = rule;
        Ok(())
    }
}

/// What an alert is about. Stored as raised: an alert on a channel that is
/// later superseded, or on an agent that is later merged, keeps that id.
/// Readers that match subjects against a channel or agent (the alert inbox's
/// channel filter, the live feed, sanction suppression) compare
/// [`AlertSubject::resolved`] subjects.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AlertSubject {
    Channel(ChannelId),
    Transmission(TransmissionId),
    Agent(AgentId),
}

impl AlertSubject {
    /// The subject with its channel resolved through supersession and its
    /// agent through merges. A transmission subject is unchanged; its route
    /// resolves separately ([`Route::resolved`]).
    ///
    /// [`Route::resolved`]: crate::derived::flow::transmission::Route::resolved
    pub fn resolved(self, aliases: impl Aliases) -> Self {
        match self {
            Self::Channel(channel) => Self::Channel(aliases.channel(channel)),
            Self::Agent(agent) => Self::Agent(aliases.agent(agent)),
            Self::Transmission(_) => self,
        }
    }
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
    /// The draft's rule no longer evaluates (disabled or stale by the time
    /// the draft was triaged), so nothing was opened. Closes the race between
    /// an evaluation and a disable.
    RuleInactive,
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
    /// The condition stopped being alert-worthy: the channel was sanctioned,
    /// the rule disabled, or the suspected transmission dismissed.
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
    /// An operator dismissed the suspected transmission the alert is about.
    TransmissionDismissed,
}
