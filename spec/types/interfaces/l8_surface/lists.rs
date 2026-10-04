//! Filters, requests and pages for the surface's list and search queries.
//!
//! List filters follow [`AlertFilter`](super::AlertFilter): an empty list
//! does not restrict, and each filter's `matches` is its definition.

use crate::aggregates::alert::{AlertRuleDef, RuleStatus};
use crate::aggregates::topic::{Topic, TopicModelVersion};
use crate::derived::flow::channel::Channel;
use crate::observed::agent::{Agent, AgentState};
use crate::paging::{Page, TopicList};
use crate::support::NonBlank;

use super::PolicyKind;

/// Keeps channels whose current policy kind is listed.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ChannelFilter {
    pub policies: Vec<PolicyKind>,
}

impl ChannelFilter {
    pub fn matches(&self, channel: &Channel) -> bool {
        self.policies.is_empty() || self.policies.contains(&channel.policy.kind())
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
