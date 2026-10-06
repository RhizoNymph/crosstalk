//! L3's agent writes: creating an agent, moving it forward between active
//! states and attaching evidence a later exchange carried.
//!
//! The reconstruct consumer calls these after resolving an exchange
//! (`IdentityResolver::resolve`): `New` creates an agent from traffic,
//! `Known` attaches the evidence the agent lacks and moves a registered
//! agent to provisional on its first exchange; corroborated evidence
//! establishes a provisional agent. Config registers the agents it
//! declares. Merges, unmerges and renames stay on `IdentityResolver`.
//!
//! Each write checks before it changes anything, so a refusal leaves the
//! store as it was and publishes nothing. Each accepted write publishes
//! `Changed::Agent` (see [`super`] for who else is announced). A `create`
//! of an agent from traffic also publishes `AgentSeen` for each item of its
//! evidence, in order, and an `attach_evidence` for the item it attached:
//! the announcement commits with the write, so a delivery redone after the
//! write (which finds the evidence held) never loses it.

use crate::ids::AgentId;
#[cfg(doc)]
use crate::observed::agent::AgentState;
use crate::observed::agent::{AgentLabel, IdentityEvidence};
use crate::support::{NonEmpty, Timestamp};

/// An agent to create, under an id the caller minted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewAgent {
    pub id: AgentId,
    pub evidence: NonEmpty<IdentityEvidence>,
    /// The spawning agent, as stored (resolved through merges at read
    /// time). It need not be stored yet.
    pub parent: Option<AgentId>,
    pub origin: AgentOrigin,
    pub label: Option<AgentLabel>,
}

/// Where a new agent came from, which fixes its first state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AgentOrigin {
    /// Declared in config, before any traffic: [`AgentState::Registered`]
    /// at `at`.
    Config { at: Timestamp },
    /// Created by its first exchange, which started at `first_seen`:
    /// [`AgentState::Provisional`], with that exchange recorded in the
    /// activity store in the same transaction, so the agent is never listed
    /// without a last-seen time.
    Traffic { first_seen: Timestamp },
}

/// A forward move between active states. Only these two exist: the other
/// state changes are merges and unmerges.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Advance {
    /// A registered agent's first exchange, started at `at`: `Registered`
    /// to `Provisional { first_seen: at }`, recording the exchange in the
    /// activity store in the same transaction.
    FirstTraffic { at: Timestamp },
    /// Corroborated evidence, as the resolver decided it:
    /// `Provisional` to `Established { since }`.
    Establish { since: Timestamp },
}

/// Creating agents and changing their active state.
pub trait AgentLifecycle {
    /// Create `agent`. Publishes `Changed::Agent` for it and for its
    /// canonical parent when the parent is stored (its children grew).
    ///
    /// `DuplicateAgent` when the id is taken, changing nothing.
    fn create(
        &mut self,
        agent: NewAgent,
    ) -> impl Future<Output = Result<(), AgentLifecycleError>> + Send;

    /// Move `agent` forward. Publishes `Changed::Agent` for it.
    ///
    /// `UnknownAgent` for an unknown id, and `IllegalTransition` when the
    /// move does not start from the agent's state (a merged agent included),
    /// changing nothing.
    fn advance(
        &mut self,
        agent: AgentId,
        advance: Advance,
    ) -> impl Future<Output = Result<(), AgentLifecycleError>> + Send;

    /// Attach `evidence`, which a later exchange attributed to `agent`
    /// carried, to `agent`'s own record (merged or not: the record keeps
    /// what was seen on it). Publishes `Changed::Agent` for it.
    ///
    /// `UnknownAgent` for an unknown id, and `DuplicateEvidence` when the
    /// record already holds it, changing nothing.
    fn attach_evidence(
        &mut self,
        agent: AgentId,
        evidence: IdentityEvidence,
    ) -> impl Future<Output = Result<(), AgentLifecycleError>> + Send;
}

/// Why an agent write was refused. Nothing changed. Consumer-side only: no
/// surface query or action makes these writes, so none maps to a
/// `QueryError`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AgentLifecycleError {
    Store {
        reason: String,
    },
    DuplicateAgent(AgentId),
    UnknownAgent(AgentId),
    /// Only `Registered` to `Provisional` and `Provisional` to
    /// `Established` are forward moves.
    IllegalTransition {
        agent: AgentId,
    },
    DuplicateEvidence(AgentId),
}
