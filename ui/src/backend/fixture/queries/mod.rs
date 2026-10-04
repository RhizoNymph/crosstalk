//! Reads over the world and the current state.
//!
//! Every query builds a [`Ctx`]: a snapshot of alias and supersession
//! resolution and of the verdicts in force, so each record is resolved the
//! same way within one response.

pub mod content;
pub mod graph;
pub mod lists;
pub mod names;
pub mod page;
pub mod scope;
pub mod summaries;
pub mod transmissions;

use std::collections::{BTreeMap, HashMap};

use crosstalk_spec::aggregates::topic::TopicModelVersion;
use crosstalk_spec::derived::flow::transmission::Route;
use crosstalk_spec::ids::{AgentId, ChannelId, TransmissionId};
use crosstalk_spec::interfaces::l8_surface::{Caller, Permission};

use crate::backend::Result;
use crate::contract::errors::QueryError;
use crate::contract::verdict::Verdict;

use super::store::State;
use super::world::World;

/// Fails with `Forbidden` unless the caller holds `permission`.
pub fn require(caller: &Caller, permission: Permission) -> Result<()> {
    if caller.permissions.contains(&permission) {
        Ok(())
    } else {
        Err(QueryError::Forbidden {
            missing: permission,
        })
    }
}

/// Fails with `VersionNotRetained` unless the world keeps `version`.
pub fn retained(world: &World, version: TopicModelVersion) -> Result<()> {
    if world.topics.retains(version) {
        Ok(())
    } else {
        Err(QueryError::VersionNotRetained { version })
    }
}

/// One query's view of the world.
pub struct Ctx<'a> {
    pub world: &'a World,
    pub state: &'a State,
    agents: HashMap<AgentId, AgentId>,
    channels: HashMap<ChannelId, ChannelId>,
    /// Canonical agent to the agents that resolve to it, itself included.
    members: BTreeMap<AgentId, Vec<AgentId>>,
    verdicts: HashMap<TransmissionId, Verdict>,
}

impl<'a> Ctx<'a> {
    pub fn new(world: &'a World, state: &'a State) -> Self {
        let agents: HashMap<AgentId, AgentId> = state
            .agents
            .keys()
            .map(|id| (*id, state.canonical_agent(*id)))
            .collect();
        let mut members: BTreeMap<AgentId, Vec<AgentId>> = BTreeMap::new();
        for (id, canonical) in &agents {
            members.entry(*canonical).or_default().push(*id);
        }
        for list in members.values_mut() {
            list.sort();
        }
        let channels = state
            .channels
            .keys()
            .map(|id| (*id, state.canonical_channel(*id)))
            .collect();
        // The log is oldest first, so the last entry per transmission wins.
        let mut latest: HashMap<TransmissionId, Option<Verdict>> = HashMap::new();
        for entry in &state.verdicts {
            latest.insert(entry.transmission, entry.verdict);
        }
        let verdicts = latest
            .into_iter()
            .filter_map(|(id, v)| v.map(|v| (id, v)))
            .collect();
        Self {
            world,
            state,
            agents,
            channels,
            members,
            verdicts,
        }
    }

    pub fn agent(&self, id: AgentId) -> AgentId {
        self.agents.get(&id).copied().unwrap_or(id)
    }

    pub fn channel(&self, id: ChannelId) -> ChannelId {
        self.channels.get(&id).copied().unwrap_or(id)
    }

    /// The route with its channel resolved through supersession.
    pub fn route(&self, route: &Route) -> Route {
        match route {
            Route::Channel(id) => Route::Channel(self.channel(*id)),
            other => other.clone(),
        }
    }

    /// The agents that resolve to `canonical`, itself included.
    pub fn members(&self, canonical: AgentId) -> &[AgentId] {
        self.members.get(&canonical).map_or(&[], Vec::as_slice)
    }

    /// Canonical agents, in id order.
    pub fn canonical_agents(&self) -> impl Iterator<Item = AgentId> + '_ {
        self.members.keys().copied()
    }

    /// The raw channel ids whose traffic counts for `channel`: itself and
    /// every channel superseded into it. A superseded channel counts only
    /// its own.
    pub fn channel_members(&self, channel: ChannelId) -> Vec<ChannelId> {
        if self.channel(channel) != channel {
            return vec![channel];
        }
        self.channels
            .iter()
            .filter(|(_, c)| **c == channel)
            .map(|(id, _)| *id)
            .collect()
    }

    /// The verdict in force on a transmission.
    pub fn verdict(&self, id: TransmissionId) -> Option<Verdict> {
        self.verdicts.get(&id).copied()
    }
}

/// A stable order for routes, which have no `Ord`.
pub fn route_key(route: &Route) -> (u8, u128, String) {
    use crosstalk_spec::derived::flow::transmission::{DelegationDirection, DirectCarrier};
    match route {
        Route::Channel(id) => (0, id.as_ulid(), String::new()),
        Route::Delegation(DelegationDirection::ParentToChild) => (1, 0, String::new()),
        Route::Delegation(DelegationDirection::ChildToParent) => (1, 1, String::new()),
        Route::Direct(DirectCarrier::UserTurn) => (2, 0, String::new()),
        Route::Direct(DirectCarrier::SystemPrompt) => (2, 1, String::new()),
        Route::Direct(DirectCarrier::ToolResult(name)) => (2, 2, name.0.clone()),
        Route::Unobserved => (3, 0, String::new()),
    }
}
