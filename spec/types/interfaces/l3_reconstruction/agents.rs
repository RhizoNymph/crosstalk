//! L3's agent reads: the agents list, one agent's cluster and batch names,
//! and the activity store behind each row's last-seen time.
//!
//! Each call reads one consistent snapshot of the agent table, the merge
//! log, the vetoes, the claim store and the activity store, so a profile's
//! aliases, claims and last-seen time describe the same cluster. Merges are
//! resolved through the same table `AgentDirectory::canonical` reads. The
//! surface joins these with L7's traffic counts
//! (`EdgeStore::agent_traffic`); see [`crate::aggregates::agents`].

use std::collections::BTreeMap;

use crate::aggregates::agents::filter::AgentFilter;
use crate::aggregates::agents::{AgentCluster, AgentName, AgentProfile};
use crate::batch::IdBatch;
use crate::ids::AgentId;
use crate::paging::{AgentList, Page, PageRequest};
use crate::support::Timestamp;

use super::ResolveError;

/// Reads of agents as the surface shows them: canonical agents only.
pub trait AgentReads {
    /// The profiles of the canonical agents `filter` admits
    /// ([`AgentFilter::matches`], listed parents resolved through the merge
    /// table), newest agent first (`AgentId` descending). A merged agent is
    /// never listed, whatever the filter. Each profile's claims are
    /// `ClaimStore::claims` of the agent and its `last_seen`
    /// `ActivityStore::last_seen`.
    async fn list(
        &self,
        filter: &AgentFilter,
        page: &PageRequest<AgentList>,
    ) -> Result<Page<AgentProfile, AgentList>, AgentReadError>;

    /// The cluster of the canonical agent `id` resolves to, with
    /// `AgentLookup::Canonical` when `id` is that agent and
    /// `Redirected { from: id }` when `id` is merged. `None` for an unknown
    /// id.
    async fn cluster(&self, id: AgentId) -> Result<Option<AgentCluster>, AgentReadError>;

    /// For each id in `ids` that names a stored agent, merged or not, the
    /// [`AgentName`] of the canonical agent it resolves to, keyed by the id
    /// asked for. Unknown ids are left out, not errors.
    async fn names(
        &self,
        ids: &IdBatch<AgentId>,
    ) -> Result<BTreeMap<AgentId, AgentName>, AgentReadError>;
}

/// When each agent was last seen: the start of the latest exchange
/// attributed to it.
///
/// Maintained like the claim store: the reconstruct consumer records every
/// captured exchange against its attributed agent, in the transaction that
/// creates the agent when the exchange is its first, so an agent created
/// from traffic is never listed without a last-seen time. Times stay stored
/// under the attributed agent; a canonical agent's is the latest over it and
/// its aliases, read at query time, so an unmerge splits them again.
pub trait ActivityStore {
    /// Record that an exchange attributed to `agent` started at `at`,
    /// keeping the latest time. Idempotent and order-independent.
    async fn record(&mut self, agent: AgentId, at: Timestamp) -> Result<(), ResolveError>;

    /// The latest time recorded for `agent`'s canonical agent or any agent
    /// that resolves to it. `None` when none was ever recorded.
    async fn last_seen(&self, agent: AgentId) -> Result<Option<Timestamp>, ResolveError>;
}

/// Why an agent read failed. Unknown ids are not errors: `cluster` returns
/// `None` and `names` leaves them out.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AgentReadError {
    Store {
        reason: String,
    },
    /// A cursor this store did not issue, or issued for another filter.
    InvalidCursor,
}
