//! Alerts, and (in [`rules`], re-exported here) the rules that raise them.
//!
//! See [`rules`] for the built-in and user rules and their staleness.
//!
//! **Alerts.** A rule evaluation produces an [`AlertDraft`] (the lifecycle's
//! `Fired`). Triage either opens an [`Alert`] or folds the draft into an
//! active (open or acknowledged) alert with the same rule and subject, so
//! `Deduplicated` is a [`TriageOutcome`], not a stored state. A draft whose
//! rule stopped evaluating before triage opens nothing (`RuleInactive`).
//! Alerts are delivered to the sinks their rule lists.
//!
//! Sanctioning a channel suppresses the active alerts whose subject is that
//! channel, or a channel it superseded (subjects are compared
//! [`AlertSubject::resolved`]); alerts about transmissions on it stay,
//! because content can be worth flagging on a sanctioned channel. A
//! promotion that sets `Sanctioned` sanctions the promoted channel.
//! Deduplication compares stored subjects, so alerts on a superseded channel
//! stay under its id and later traffic raises alerts on the superseding
//! channel. Disabling a rule suppresses its active alerts.
//!
//! A `FalseDetection` verdict on a transmission suppresses every active
//! alert whose subject is that transmission, whatever its rule, with reason
//! [`SuppressReason::OperatorRejected`], and while it is the transmission's
//! current verdict triage opens nothing about it
//! ([`TriageOutcome::OperatorRejected`]). Withdrawing the verdict, or
//! replacing it with `Genuine`, reopens nothing: later drafts open alerts
//! again. A `Genuine` verdict changes no alert.
//!
//! ```text
//! draft ─triage─┬─▶ Open ─acknowledge─▶ Acknowledged ─resolve─▶ Resolved
//!               │     └──────┬──────────────┘
//!               │   sanctioned, rule disabled, or transmission
//!               │   judged a false detection
//!               │            ▼
//!               │       Suppressed
//!               ├─▶ deduplicated into an existing alert
//!               └─▶ nothing: rule inactive, or subject judged a false detection
//! ```
//!
//! Every stored change to an alert (a deduplicated occurrence, a
//! suppression, an acknowledgement, a resolution) bumps its
//! [`AlertRevision`] by one and publishes `AlertChanged` with the alert after
//! the change. Changes to one alert are compare-and-set on its revision, so
//! revisions are consecutive and a reader that keeps the highest revision it
//! has seen ends with the stored alert, whatever order the events arrive in.
//! Rules follow the same scheme with [`RuleRevision`] and `AlertRuleChanged`.

pub mod rules;

pub use rules::{
    AlertRule, AlertRuleConfig, AlertRuleDef, AlertRuleKind, AlertRuleSet, BuiltinRule,
    ContentRule, InsertError, InvalidRuleDef, NotEditable, QueryWatch, RULE_QUERY_MAX_CHARS,
    ReservedRuleId, RuleDefinition, RuleName, RuleQueryText, RuleRemapError, RuleRevision,
    RuleStatus, SemanticQuery, StaleReason, StaleRule, TopicWatch, UserRule, WatchedTopics,
    is_reserved_rule_id,
};

use std::num::NonZeroU32;

use serde::{Deserialize, Serialize};

use crate::aliases::Aliases;
use crate::ids::{AgentId, AlertId, AlertRuleId, ChannelId, OperatorId, TransmissionId};
use crate::support::Timestamp;

/// What an alert is about. Stored as raised: an alert on a channel that is
/// later superseded, or on an agent that is later merged, keeps that id.
/// Readers that match subjects against a channel or agent (the alert inbox's
/// channel filter, the live feed, sanction suppression) compare
/// [`AlertSubject::resolved`] subjects.
///
/// On the wire, `{"type": "channel", "data": "<ChannelId>"}`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(
    tag = "type",
    content = "data",
    rename_all = "snake_case",
    deny_unknown_fields
)]
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
    /// The draft's subject is a transmission whose current verdict, as
    /// triage holds it, is `FalseDetection`, so nothing was opened. Closes
    /// the race between an evaluation and the verdict's suppression.
    OperatorRejected,
}

/// A response: `QueryApi::alerts` lists it and `QueryApi::alert` returns
/// it. Never a request: its state names who acknowledged or resolved it,
/// which the surface stamps.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct Alert {
    pub id: AlertId,
    pub rule: AlertRuleId,
    pub subject: AlertSubject,
    pub raised_at: Timestamp,
    pub occurrences: u32,
    pub state: AlertState,
}

/// On the wire, `{"type": "open"}` or
/// `{"type": "acknowledged", "data": {"by": .., "at": ..}}`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "type",
    content = "data",
    rename_all = "snake_case",
    deny_unknown_fields
)]
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
    /// the rule disabled, or the transmission judged a false detection.
    Suppressed {
        at: Timestamp,
        reason: SuppressReason,
    },
}

impl AlertState {
    /// The state without its data: what `AlertFilter::states` matches and
    /// the inbox's tabs show. One exhaustive match, so a new state needs a
    /// kind.
    pub fn kind(&self) -> AlertStateKind {
        match self {
            Self::Open => AlertStateKind::Open,
            Self::Acknowledged { .. } => AlertStateKind::Acknowledged,
            Self::Resolved { .. } => AlertStateKind::Resolved,
            Self::Suppressed { .. } => AlertStateKind::Suppressed,
        }
    }

    /// Open or acknowledged: still waiting for someone. Only an active
    /// alert is deduplicated into, suppressed, acknowledged or resolved;
    /// acknowledging or resolving any other is `Conflict(AlertNotActive)`,
    /// and resolving an open one `Conflict(AlertNotAcknowledged)`.
    pub fn is_active(&self) -> bool {
        self.kind().is_active()
    }
}

/// An [`AlertState`] without its data. On the wire, a string:
/// `"acknowledged"` (a request inside `AlertFilter`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AlertStateKind {
    Open,
    Acknowledged,
    Resolved,
    Suppressed,
}

impl AlertStateKind {
    pub const ALL: [Self; 4] = [
        Self::Open,
        Self::Acknowledged,
        Self::Resolved,
        Self::Suppressed,
    ];

    /// Whether alerts in this state are active (open or acknowledged).
    pub fn is_active(self) -> bool {
        match self {
            Self::Open | Self::Acknowledged => true,
            Self::Resolved | Self::Suppressed => false,
        }
    }
}

/// How many stored changes an alert has had: 1 when opened, one more per
/// change.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
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

/// On the wire, a string: `"channel_sanctioned"`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SuppressReason {
    ChannelSanctioned,
    RuleDisabled,
    /// An operator judged the transmission the alert is about a
    /// `FalseDetection`. A later withdrawal does not reopen the alert.
    OperatorRejected,
}
