//! How agents get into the store.
//!
//! The spec has no trait method that creates an agent or moves it between
//! active states: the reconstruct consumer does that when it resolves an
//! exchange (P4.1), and config registers declared agents. These are the
//! store operations behind those steps, with the checks the agent
//! lifecycle needs (`reconstruct.agent-state.legal-transitions`). The
//! model-based harness drives the store under test through the same
//! operations ([`SeedAgents`]).

use crosstalk_spec::ids::AgentId;
use crosstalk_spec::observed::agent::{AgentLabel, IdentityEvidence};
use crosstalk_spec::support::{NonEmpty, Timestamp};

/// An agent to create.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewAgent {
    pub id: AgentId,
    pub evidence: NonEmpty<IdentityEvidence>,
    /// The spawning agent, as stored (resolved through merges at read
    /// time).
    pub parent: Option<AgentId>,
    pub origin: AgentOrigin,
    pub label: Option<AgentLabel>,
}

/// Where a new agent came from, which fixes its first state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AgentOrigin {
    /// Declared in config, before any traffic: `Registered { at }`.
    Config { at: Timestamp },
    /// Created by its first exchange, which started at `first_seen`:
    /// `Provisional { first_seen }`, with the exchange recorded in the
    /// activity store in the same transaction, so the agent is never
    /// listed without a last-seen time.
    Traffic { first_seen: Timestamp },
}

/// A forward move between active states.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Advance {
    /// A registered agent's first exchange, started at `at`: `Registered`
    /// to `Provisional { first_seen: at }`, recording the exchange in the
    /// activity store.
    FirstTraffic { at: Timestamp },
    /// Corroborated evidence: `Provisional` to `Established { since }`.
    Establish { since: Timestamp },
}

/// Why a seeding operation was refused. Nothing changed.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SeedError {
    #[error("agent {0:?} already exists")]
    DuplicateAgent(AgentId),
    #[error("agent {0:?} is unknown")]
    UnknownAgent(AgentId),
    /// The advance does not start from the agent's state (merged agents
    /// included): only `Registered` to `Provisional` and `Provisional` to
    /// `Established` are forward moves.
    #[error("agent {agent:?} cannot make that transition from its state")]
    IllegalTransition { agent: AgentId },
    #[error("agent {0:?} already holds that evidence")]
    DuplicateEvidence(AgentId),
}

/// The operations behind agent creation and state changes, implemented by
/// every agent store the model-based harness checks.
pub trait SeedAgents {
    /// Create `agent`. Publishes `Changed::Agent` for it and for its
    /// canonical parent, when the parent is stored.
    fn create(&mut self, agent: NewAgent) -> impl Future<Output = Result<(), SeedError>> + Send;

    /// Move `agent` forward. Publishes `Changed::Agent` for it.
    fn advance(
        &mut self,
        agent: AgentId,
        advance: Advance,
    ) -> impl Future<Output = Result<(), SeedError>> + Send;

    /// Attach evidence a later exchange carried to `agent` (merged or
    /// not). Publishes `Changed::Agent` for it.
    fn attach(
        &mut self,
        agent: AgentId,
        evidence: IdentityEvidence,
    ) -> impl Future<Output = Result<(), SeedError>> + Send;
}
