//! L3 reference store: agents, the merge log with exact unmerges and
//! vetoes, harness claims and activity, and the agent reads.
//!
//! [`MemoryAgents`] implements, over one table:
//! - `AgentDirectory` (the merge table every reader resolves through);
//! - `IdentityResolver`: `merge`, `unmerge` and `rename` exactly as the
//!   spec documents them, and `resolve` as the reference lookup in
//!   [`resolve`];
//! - `ClaimStore` and `ActivityStore` (per attributed agent, unioned over
//!   aliases at read time);
//! - `AgentReads` (the agents list, a cluster, batch names);
//! - [`SeedAgents`], the creation and state changes the spec leaves to the
//!   reconstruct consumer and config.
//!
//! [`model`] is the model-based property harness the Postgres store reuses.

pub mod model;
pub mod resolve;
mod seed;
mod store;
mod table;

#[cfg(test)]
mod tests;

use std::sync::Arc;

use crosstalk_spec::ids::{AgentId, MergeId};

use crate::pipeline::{CursorTable, IdSequence, Outbox, State};

pub use seed::{Advance, AgentOrigin, NewAgent, SeedAgents, SeedError};

use table::AgentTable;

/// The in-memory L3 store. Clones are handles on one store.
#[derive(Debug, Clone)]
pub struct MemoryAgents {
    state: State<AgentTable>,
    cursors: Arc<CursorTable<AgentId>>,
    merge_ids: IdSequence,
    outbox: Outbox,
}

impl MemoryAgents {
    /// An empty store that takes merge record ids from `merge_ids` and
    /// publishes its events to `outbox`.
    pub fn new(merge_ids: IdSequence, outbox: Outbox) -> Self {
        Self {
            state: State::new(AgentTable::default()),
            cursors: Arc::new(CursorTable::new("agents")),
            merge_ids,
            outbox,
        }
    }

    fn next_merge_id(&self) -> MergeId {
        MergeId::from_ulid(self.merge_ids.next_ulid())
    }
}

impl Default for MemoryAgents {
    fn default() -> Self {
        Self::new(IdSequence::default(), Outbox::none())
    }
}
