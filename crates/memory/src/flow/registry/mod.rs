//! The in-memory channel registry: `ChannelRegistry` and `ChannelDirectory`
//! over one table of channels, policy histories, resources and accesses,
//! plus [`SeedChannels`].

pub mod model;
mod seed;
mod store;
mod table;

#[cfg(test)]
mod tests;

use std::sync::Arc;

use crosstalk_spec::ids::{ChannelId, ResourceId};

use crate::pipeline::{Clock, CursorTable, IdSequence, Outbox, State};

pub use seed::{DetectionUpdate, SeedChannels, SeedError};

use table::ChannelTable;

/// The in-memory L5 registry. Agents in `resource_use` are resolved through
/// `agents`. Clones are handles on one registry.
#[derive(Clone)]
pub struct MemoryChannels<D> {
    state: State<ChannelTable>,
    cursors: Arc<CursorTable<ResourceId>>,
    channel_ids: IdSequence,
    clock: Arc<dyn Clock>,
    agents: D,
    outbox: Outbox,
}

impl<D> MemoryChannels<D> {
    /// An empty registry. Declared channels take their ids from
    /// `channel_ids` (one per accepted declaration) and their declaration
    /// time from `clock`; events go to `outbox`.
    pub fn new(agents: D, channel_ids: IdSequence, clock: Arc<dyn Clock>, outbox: Outbox) -> Self {
        Self {
            state: State::new(ChannelTable::default()),
            cursors: Arc::new(CursorTable::new("resources")),
            channel_ids,
            clock,
            agents,
            outbox,
        }
    }

    fn next_channel_id(&self) -> ChannelId {
        ChannelId::from_ulid(self.channel_ids.next_ulid())
    }
}

impl<D> std::fmt::Debug for MemoryChannels<D> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemoryChannels")
            .field("state", &self.state)
            .finish_non_exhaustive()
    }
}
