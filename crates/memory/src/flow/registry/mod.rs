//! The in-memory channel registry: `ChannelRegistry`, `ChannelTraffic`,
//! `ChannelReads` and `ChannelDirectory` over one table of channels, policy
//! histories, resources and accesses.

pub mod model;
mod store;
mod table;

#[cfg(test)]
mod tests;

use std::sync::{Arc, Mutex};

use crosstalk_spec::ids::{ChannelId, ResourceId};
use crosstalk_spec::interfaces::l8_surface::lists::ChannelFilter;

use crate::support::{CursorBook, IdSequence, Outbox, State};

use table::ChannelTable;

/// The cursors the registry's two lists issue. Behind their own lock, since
/// a read takes only a shared lock on the table.
#[derive(Debug, Default)]
struct Cursors {
    /// `resource_use`: bound to the canonical channel and window's JSON.
    resources: CursorBook<String, ResourceId>,
    /// `ChannelReads::channels`: bound to the filter.
    channels: CursorBook<ChannelFilter, ChannelId>,
}

/// The in-memory L5 registry. Agents in `resource_use` are resolved through
/// `agents`. Clones are handles on one registry.
#[derive(Clone)]
pub struct MemoryChannels<D> {
    state: State<ChannelTable>,
    cursors: Arc<Mutex<Cursors>>,
    channel_ids: IdSequence,
    agents: D,
    outbox: Outbox,
}

impl<D> MemoryChannels<D> {
    /// An empty registry. Declared channels take their ids from
    /// `channel_ids` (one per accepted declaration); events go to `outbox`.
    pub fn new(agents: D, channel_ids: IdSequence, outbox: Outbox) -> Self {
        Self {
            state: State::new(ChannelTable::default()),
            cursors: Arc::new(Mutex::new(Cursors::default())),
            channel_ids,
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
