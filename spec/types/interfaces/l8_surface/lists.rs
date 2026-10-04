//! Filters, requests and pages for the surface's list and search queries.
//!
//! List filters follow [`AlertFilter`](super::AlertFilter): an empty list
//! does not restrict, and each filter's `matches` is its definition.

use crate::aggregates::alert::{AlertRuleDef, RuleStatus};
use crate::aggregates::node::CanonicalOriginKind;
use crate::aggregates::topic::{Topic, TopicModelVersion};
use crate::derived::flow::channel::Channel;
use crate::derived::flow::channel::detection::DetectionKind;
use crate::observed::agent::{Agent, AgentState};
use crate::paging::{Page, TopicList};
use crate::support::{NonBlank, TimeWindow};

use super::PolicyKind;

/// Which channels `QueryApi::channels` lists, and over what window it
/// counts their activity.
///
/// [`ChannelFilter::matches`] is the definition of which channels are
/// listed: `origin`, then `detections` (each channel's own
/// [`ChannelOrigin::detection_kind`], frozen for a superseded one) and
/// `policies` (each channel's own current policy kind; a superseded one
/// takes no decisions, so its last one stays), combined with AND. Empty
/// lists do not restrict. `window` is not part of the match: it changes the
/// counts on each row, never which rows are listed.
///
/// [`ChannelOrigin::detection_kind`]: crate::derived::flow::channel::ChannelOrigin::detection_kind
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ChannelFilter {
    pub origin: OriginFilter,
    pub detections: Vec<DetectionKind>,
    pub policies: Vec<PolicyKind>,
    /// The window writers, readers and transmissions are counted in
    /// (`Access::at`, `Confirmed::at`); `None` counts all of them. Never
    /// restricts the rows, and never changes a row's last activity.
    pub window: Option<TimeWindow>,
}

/// Which origins a channel list keeps, superseded channels included or not.
///
/// One value instead of a list of origins and a separate superseded flag,
/// so "superseded channels only, but exclude superseded channels" cannot be
/// asked. Origin kinds apply to channels in force only: a superseded channel
/// was always discovered, and is selected by its variant alone.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OriginFilter {
    /// Channels in force whose origin kind is listed (every origin when
    /// empty), and no superseded channel. The default.
    InForce(Vec<CanonicalOriginKind>),
    /// The same channels in force, and every superseded channel.
    WithSuperseded(Vec<CanonicalOriginKind>),
    /// Superseded channels only.
    Superseded,
}

impl Default for OriginFilter {
    fn default() -> Self {
        Self::InForce(Vec::new())
    }
}

impl OriginFilter {
    pub fn matches(&self, channel: &Channel) -> bool {
        match (self, CanonicalOriginKind::of(&channel.origin)) {
            (Self::InForce(kinds) | Self::WithSuperseded(kinds), Some(kind)) => {
                kinds.is_empty() || kinds.contains(&kind)
            }
            (Self::Superseded, Some(_)) | (Self::InForce(_), None) => false,
            (Self::WithSuperseded(_) | Self::Superseded, None) => true,
        }
    }
}

impl ChannelFilter {
    pub fn matches(&self, channel: &Channel) -> bool {
        let by_detection = self.detections.is_empty()
            || self.detections.contains(&channel.origin.detection_kind());
        let by_policy = self.policies.is_empty() || self.policies.contains(&channel.policy.kind());
        self.origin.matches(channel) && by_detection && by_policy
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AgentStateKind {
    Registered,
    Provisional,
    Established,
    Merged,
}

impl AgentStateKind {
    pub fn of(state: &AgentState) -> Self {
        match state {
            AgentState::Registered { .. } => Self::Registered,
            AgentState::Provisional { .. } => Self::Provisional,
            AgentState::Established { .. } => Self::Established,
            AgentState::Merged { .. } => Self::Merged,
        }
    }
}

/// Keeps agents whose state kind is listed. A graph legend wants
/// `[Registered, Provisional, Established]`: the canonical agents.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct AgentFilter {
    pub states: Vec<AgentStateKind>,
}

impl AgentFilter {
    pub fn matches(&self, agent: &Agent) -> bool {
        self.states.is_empty() || self.states.contains(&AgentStateKind::of(&agent.state))
    }
}

/// Keeps rules whose operator-set status is listed and, when `stale` is
/// set, whose staleness ([`AlertRule::is_stale`]) equals it. Staleness is
/// separate from status, so "stale rules" is `stale: Some(true)` with any
/// statuses, and "rules that evaluate" is `[Enabled]` with `Some(false)`.
///
/// [`AlertRule::is_stale`]: crate::aggregates::alert::AlertRule::is_stale
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct AlertRuleFilter {
    pub statuses: Vec<RuleStatus>,
    pub stale: Option<bool>,
}

impl AlertRuleFilter {
    pub fn matches(&self, rule: &AlertRuleDef) -> bool {
        let by_status = self.statuses.is_empty() || self.statuses.contains(&rule.status);
        let by_staleness = self
            .stale
            .is_none_or(|stale| rule.rule().is_stale() == stale);
        by_status && by_staleness
    }
}

/// How `QueryApi::search` matches its text.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SearchMode {
    /// Full-text only.
    Text,
    /// The text is embedded with the current model; vector similarity only.
    Semantic,
    /// Both, scored as the mean of the two.
    Hybrid,
}

/// A search as an operator asks for it. The surface embeds the text itself,
/// so a client never sends a vector and never needs to know the model.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchRequest {
    pub mode: SearchMode,
    pub text: NonBlank,
}

/// One page of a version's topics, and that version.
#[derive(Debug, Clone, PartialEq)]
pub struct TopicPage {
    pub version: TopicModelVersion,
    pub page: Page<Topic, TopicList>,
}
