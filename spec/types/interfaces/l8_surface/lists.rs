//! Filters and requests for the surface's list and projection queries.
//!
//! List filters follow [`AlertFilter`](super::AlertFilter): an empty list
//! does not restrict, and each filter's `matches` is its definition.

use crate::aggregates::alert::{AlertRuleDef, RuleStatus};
use crate::aggregates::filter::TopologyFilter;
use crate::aggregates::projection::{ProjectionLimit, ProjectionToken};
use crate::derived::flow::channel::Channel;
use crate::observed::agent::{Agent, AgentState};
use crate::support::TimeWindow;

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

/// What `QueryApi::projection` returns points for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectionRequest {
    pub window: TimeWindow,
    pub filter: TopologyFilter,
    pub limit: ProjectionLimit,
    /// The layout of points the client already holds, when it is adding to
    /// them (a wider window, a relaxed filter). `None` for a fresh load.
    pub layout: Option<ProjectionToken>,
}
