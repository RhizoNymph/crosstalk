//! The mutable half of the fixture: everything an operator action can
//! change. Generation fills it with history; `act` changes it under a write
//! lock.

use std::collections::BTreeMap;

use crosstalk_spec::derived::flow::channel::Channel;
use crosstalk_spec::ids::{AgentId, ChannelId};
use crosstalk_spec::interfaces::l2_transport::DeadLetter;
use crosstalk_spec::support::Timestamp;

use crate::contract::alerts::Alert;
use crate::contract::agents::{Agent, AgentLabel, AgentState, MergeRecord, MergeVeto};
use crate::contract::channels::Supersession;
use crate::contract::research::{AuditEntry, AuditSubject, ProjectionPoints};
use crate::contract::rules::RuleDef;
use crate::contract::verdict::TransmissionVerdict;
use crate::contract::ProjectionId;

use super::clock::Mint;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentRecord {
    pub agent: Agent,
    pub label: Option<AgentLabel>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChannelRecord {
    pub channel: Channel,
    pub superseded: Option<Supersession>,
    /// When the channel was declared or discovered.
    pub created: Timestamp,
}

/// An audit entry with the entities it concerns, for `AuditFilter::subject`.
#[derive(Debug, Clone, PartialEq)]
pub struct AuditRecord {
    pub entry: AuditEntry,
    pub subjects: Vec<AuditSubject>,
}

#[derive(Debug, Clone)]
pub struct State {
    pub agents: BTreeMap<AgentId, AgentRecord>,
    /// Oldest first.
    pub merges: Vec<MergeRecord>,
    pub vetoes: Vec<MergeVeto>,
    pub channels: BTreeMap<ChannelId, ChannelRecord>,
    /// Append-only, oldest first.
    pub verdicts: Vec<TransmissionVerdict>,
    pub alerts: Vec<Alert>,
    pub rules: Vec<RuleDef>,
    /// Append-only, oldest first.
    pub audit: Vec<AuditRecord>,
    pub dead_letters: Vec<DeadLetter>,
    pub projections: Vec<(ProjectionId, ProjectionPoints)>,
    pub mint: Mint,
}

impl State {
    pub fn new(agents: Vec<AgentRecord>, channels: Vec<ChannelRecord>, mint: Mint) -> Self {
        Self {
            agents: agents.into_iter().map(|r| (r.agent.id, r)).collect(),
            merges: Vec::new(),
            vetoes: Vec::new(),
            channels: channels.into_iter().map(|r| (r.channel.id, r)).collect(),
            verdicts: Vec::new(),
            alerts: Vec::new(),
            rules: Vec::new(),
            audit: Vec::new(),
            dead_letters: Vec::new(),
            projections: Vec::new(),
            mint,
        }
    }

    /// Follows merge aliases to the canonical agent. Unknown ids resolve to
    /// themselves.
    pub fn canonical_agent(&self, id: AgentId) -> AgentId {
        let mut current = id;
        // Merges never chain (the target of a merge is never merged), but
        // stay bounded in case a bug makes them.
        for _ in 0..self.agents.len().max(1) {
            match self.agents.get(&current).map(|r| &r.agent.state) {
                Some(AgentState::Merged { into, .. }) if *into != current => current = *into,
                _ => return current,
            }
        }
        current
    }

    /// Follows supersession to the channel in force. Unknown ids resolve to
    /// themselves.
    pub fn canonical_channel(&self, id: ChannelId) -> ChannelId {
        let mut current = id;
        for _ in 0..self.channels.len().max(1) {
            match self.channels.get(&current).and_then(|r| r.superseded) {
                Some(s) if s.into != current => current = s.into,
                _ => return current,
            }
        }
        current
    }

    pub fn is_merged(&self, id: AgentId) -> bool {
        self.agents
            .get(&id)
            .is_some_and(|r| r.agent.state.is_merged())
    }
}
