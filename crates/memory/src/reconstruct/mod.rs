//! L3 reference store: agents, the merge log with exact unmerges and
//! vetoes, harness claims and activity, and the agent reads.
//!
//! [`MemoryAgents`] implements, over one table:
//! - `AgentDirectory` (the merge table every reader resolves through);
//! - `IdentityResolver`: `merge`, `unmerge`, `rename` and `resolve` (the
//!   evidence lookup in [`resolve`]) exactly as the spec documents them;
//! - `AgentLifecycle`: creating agents, moving them forward between active
//!   states and attaching new evidence;
//! - `ClaimStore` and `ActivityStore` (per attributed agent, unioned over
//!   aliases at read time);
//! - `AgentReads` (the agents list, a cluster, batch names).
//!
//! [`model`] is the model-based property harness the Postgres store reuses.

pub mod model;
pub mod resolve;
mod store;
mod table;

#[cfg(test)]
mod tests;

use std::sync::{Arc, Mutex};

use crosstalk_spec::ids::{AgentId, MergeId};

use crate::support::{CursorBook, IdSequence, Outbox, State};

use table::AgentTable;

/// The in-memory L3 store. Clones are handles on one store.
#[derive(Debug, Clone)]
pub struct MemoryAgents {
    state: State<AgentTable>,
    /// Cursors bound to the list filter's JSON. Behind its own lock, since
    /// a list takes only a read lock on the table.
    cursors: Arc<Mutex<CursorBook<String, AgentId>>>,
    merge_ids: IdSequence,
    outbox: Outbox,
}

impl MemoryAgents {
    /// An empty store that takes merge record ids from `merge_ids` and
    /// publishes its events to `outbox`.
    pub fn new(merge_ids: IdSequence, outbox: Outbox) -> Self {
        Self {
            state: State::new(AgentTable::default()),
            cursors: Arc::new(Mutex::new(CursorBook::default())),
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
