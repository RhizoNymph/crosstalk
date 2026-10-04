//! The spec's [`NodeFacts`] as a cache kept current from L3's and L5's
//! events: what a graph node says about each canonical agent and channel.
//!
//! ```text
//! bus: Changed::Agent / Changed::Channel, AgentSeen, AgentMerged,
//!      AgentUnmerged, AgentRenamed, ConversationDelta, ChannelDiscovered,
//!      ChannelPromoted, PolicyChanged, DeclaredChannelUnused
//!   ─▶ NodeFeeder::apply ─ re-reads the named ids ─▶ AgentReads::cluster, ChannelReads::channel,
//!                                                   ChannelRegistry::resource_use (seed and resource count)
//!   ─▶ NodeCache (one std RwLock) ◀─ NodeFacts::agent / channel (sync) ─ the edge store's graphs
//! start: NodeFeeder::rebuild ─ every canonical agent (AgentReads::list) and channel in force
//!        (ChannelReads::channels) ─▶ a fresh table, swapped in whole
//! ```
//!
//! Events name ids, never facts: the feeder re-reads each id an event
//! names, so a redelivered, reordered or duplicated event leaves the cache
//! as the stores are. A merged agent or a superseded channel is never a
//! node, so its entry is removed when an event names it.
//!
//! [`NodeCache`] is what the wiring hands the edge store; [`NodeFeeder`]
//! keeps it current, from the bus ([`NodeFeeder::consume`]) or from events
//! a caller passes ([`NodeFeeder::apply`]).

mod feeder;
pub mod summary;

use std::collections::HashMap;
use std::sync::{Arc, PoisonError, RwLock};

use crosstalk_spec::ids::{AgentId, ChannelId};
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
}
