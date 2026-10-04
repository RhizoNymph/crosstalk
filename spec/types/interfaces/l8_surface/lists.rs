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
        self.policies.is_empty() || self.policies.contains(&PolicyKind::of(&channel.policy))
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

/// Keeps rules whose status is listed.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct AlertRuleFilter {
    pub statuses: Vec<RuleStatus>,
}

impl AlertRuleFilter {
    pub fn matches(&self, rule: &AlertRuleDef) -> bool {
        self.statuses.is_empty() || self.statuses.contains(&rule.status)
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
