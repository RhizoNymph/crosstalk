//! The spec's [`NodeFacts`] as a cache kept current from L3's and L5's
//! events: what a graph node says about each canonical agent and channel.
//!
//! ```text
//! bus: Changed::Agent / Changed::Channel, AgentSeen, AgentRenamed,
//!      ConversationDelta, ChannelDiscovered, ChannelPromoted, PolicyChanged,
//!      DeclaredChannelUnused, AccessRecorded
//!   ─▶ NodeFeeder::apply ─ re-reads the named ids ─▶ AgentReads::cluster, ChannelReads::channel
//!                                                   (with its traffic, so its listing),
//!                                                   ChannelRegistry::resource_use (resources held)
//!      AgentMerged, AgentUnmerged ─▶ the agents, and every listed channel re-read
//!                                    (a merge can hide a channel, an unmerge list it again)
//!   ─▶ NodeCache (one std RwLock) ◀─ NodeFacts::agent / channel / channel_of (sync) ─ the edge store
//! start: NodeFeeder::rebuild ─ every canonical agent (AgentReads::list) and every listed channel
//!        in force (ChannelReads::channels) with the resources it holds ─▶ a fresh table, swapped in
//! ```
//!
//! Events name ids, never facts: the feeder re-reads each id an event
//! names, so a redelivered, reordered or duplicated event leaves the cache
//! as the stores are. A merged agent or a superseded channel is never a
//! node, so its entry is removed when an event names it. A hidden channel
//! is described with `Listing::Hidden` once an event names it, and left out
//! of a rebuild (no listing read returns it): the edge store draws neither.
//!
//! `channel_of` answers from the resources each channel holds: a channel's
//! resources accessed at any time (`resource_use`) and its seed, re-read
//! with the channel, plus the channel each `AccessRecorded` names. A
//! promotion re-reads the promoted channel, which then holds its
//! superseded channels' resources.
//!
//! [`NodeCache`] is what the wiring hands the edge store; [`NodeFeeder`]
//! keeps it current, from the bus ([`NodeFeeder::consume`]) or from events
//! a caller passes ([`NodeFeeder::apply`], or [`NodeFeeder::apply_all`] for
//! a backlog: each id re-read once, leaving what one by one would).

mod feeder;
pub mod summary;

use std::collections::HashMap;
use std::sync::{Arc, PoisonError, RwLock};

use crosstalk_spec::ids::{AgentId, ChannelId, ResourceId};
use crosstalk_spec::interfaces::l7_topology::{AgentFacts, ChannelFacts, NodeFacts};

pub use feeder::{NodeFeedError, NodeFeeder};

/// The facts of every canonical agent and channel the feeder has read.
/// Clones share the cache.
#[derive(Debug, Clone, Default)]
pub struct NodeCache {
    tables: Arc<RwLock<Tables>>,
}

#[derive(Debug, Default)]
pub(crate) struct Tables {
    pub(crate) agents: HashMap<AgentId, AgentFacts>,
    pub(crate) channels: HashMap<ChannelId, ChannelFacts>,
    /// The channel holding each resource, as last read.
    pub(crate) resources: HashMap<ResourceId, ChannelId>,
}

impl NodeCache {
    pub fn new() -> Self {
        Self::default()
    }

    /// How many agents and channels the cache describes.
    pub fn len(&self) -> (usize, usize) {
        self.read(|tables| (tables.agents.len(), tables.channels.len()))
    }

    pub fn is_empty(&self) -> bool {
        self.len() == (0, 0)
    }

    fn read<T>(&self, read: impl FnOnce(&Tables) -> T) -> T {
        // A poisoned lock still guards whole entries: every write replaces
        // or removes one entry, or swaps the table, in one step.
        read(&self.tables.read().unwrap_or_else(PoisonError::into_inner))
    }

    pub(crate) fn write<T>(&self, write: impl FnOnce(&mut Tables) -> T) -> T {
        write(&mut self.tables.write().unwrap_or_else(PoisonError::into_inner))
    }
}

impl NodeFacts for NodeCache {
    fn agent(&self, canonical: AgentId) -> Option<AgentFacts> {
        self.read(|tables| tables.agents.get(&canonical).cloned())
    }

    fn channel(&self, canonical: ChannelId) -> Option<ChannelFacts> {
        self.read(|tables| tables.channels.get(&canonical).cloned())
    }

    fn channel_of(&self, resource: ResourceId) -> Option<ChannelId> {
        self.read(|tables| tables.resources.get(&resource).copied())
    }
}
