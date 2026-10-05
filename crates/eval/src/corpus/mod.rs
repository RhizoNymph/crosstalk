//! The streaming corpus model.
//!
//! A dataset is a [`TraceSource`]: a stream of [`World`]s. A world is a set of
//! agents that only communicate with each other (one SALT trace file), with
//! their exchanges in virtual-time order and the world's ground truth. The
//! stream yields one world at a time, so no dataset is ever held whole; a
//! world is the natural unit because its exchanges and its labels come from
//! one parse of the same records, and a matcher's index can be dropped when
//! the world ends.

pub mod builder;
pub mod client;
pub mod clock;
pub mod delta;
pub mod exchange;

use crosstalk_spec::ids::{AgentId, ExchangeId};
use crosstalk_spec::observed::client::ClientContext;
use serde::{Deserialize, Serialize};

pub use builder::{ExchangeDraft, WorldBuilder};
pub use exchange::{CorpusError, CorpusExchange, Fidelity, HashedMessage};

use crate::keys::{AgentKey, DatasetId, WorldKey};
use crate::truth::{Expectation, Tier};

/// Whether an agent's turns came from a model (and so are exchanges).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Driven {
    Model,
    /// Scripted by the dataset's harness: it makes no exchanges.
    Scripted,
}

/// One agent of a world.
#[derive(Debug, Clone, PartialEq)]
pub struct CorpusAgent {
    pub key: AgentKey,
    pub id: AgentId,
    pub client: ClientContext,
    pub driven: Driven,
    pub model: String,
}

/// How completely a world's truth covers it, which decides what an
/// unlabelled prediction is.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(tag = "coverage", rename_all = "snake_case")]
pub enum Coverage {
    /// Every transmission in the world is labelled at `tier` or stronger: a
    /// prediction that matches no label is a false positive.
    Complete { tier: Tier },
    /// Labels are a sample: a prediction that matches no label is unjudged.
    Partial,
}

/// One world: agents, exchanges in time order, truth.
#[derive(Debug, Clone)]
pub struct World {
    dataset: DatasetId,
    key: WorldKey,
    agents: Vec<CorpusAgent>,
    exchanges: Vec<CorpusExchange>,
    truth: Vec<Expectation>,
    coverage: Coverage,
}

impl World {
    pub fn dataset(&self) -> &DatasetId {
        &self.dataset
    }

    pub fn key(&self) -> &WorldKey {
        &self.key
    }

    pub fn agents(&self) -> &[CorpusAgent] {
        &self.agents
    }

    pub fn agent(&self, key: &AgentKey) -> Option<&CorpusAgent> {
        self.agents.iter().find(|agent| &agent.key == key)
    }

    pub fn agent_by_id(&self, id: AgentId) -> Option<&CorpusAgent> {
        self.agents.iter().find(|agent| agent.id == id)
    }

    /// Exchanges ordered by virtual time.
    pub fn exchanges(&self) -> &[CorpusExchange] {
        &self.exchanges
    }

    pub fn exchange(&self, reference: ExchangeId) -> Option<&CorpusExchange> {
        self.exchanges
            .iter()
            .find(|exchange| exchange.id() == reference)
    }

    pub fn truth(&self) -> &[Expectation] {
        &self.truth
    }

    pub fn coverage(&self) -> Coverage {
        self.coverage
    }
}

/// Why a source could not produce a world.
#[derive(Debug, thiserror::Error)]
pub enum SourceError {
    #[error("reading {path}: {source}")]
    Io {
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("{path} is not the expected JSON: {source}")]
    Json {
        path: String,
        #[source]
        source: serde_json::Error,
    },
    #[error(transparent)]
    Salt(#[from] crate::datasets::salt::SaltError),
    #[error(transparent)]
    Wiki(#[from] Box<crate::datasets::wiki::WikiError>),
    #[error(transparent)]
    Swarm(#[from] Box<crate::datasets::swarm::SwarmError>),
    #[error(transparent)]
    Corpus(#[from] CorpusError),
    #[error(transparent)]
    AiVillage(#[from] crate::datasets::ai_village::AiVillageError),
    #[error(transparent)]
    OpenSwe(#[from] crate::datasets::open_swe::OpenSweError),
    #[error(transparent)]
    Lmcache(#[from] crate::datasets::lmcache::LmcacheError),
    #[error(transparent)]
    Splice(#[from] crate::datasets::swe_splice::SpliceError),
    #[error(transparent)]
    Cipher(#[from] crate::datasets::cipher::CipherError),
    #[error(transparent)]
    AgentDojo(#[from] crate::datasets::agentdojo::AgentDojoError),
    #[error(transparent)]
    Tau2(#[from] crate::datasets::tau2::Tau2Error),
}

/// A dataset as a stream of worlds.
pub trait TraceSource {
    fn id(&self) -> DatasetId;

    /// The worlds, one at a time. An error ends nothing: the next item is the
    /// next world.
    fn worlds(&mut self) -> impl Iterator<Item = Result<World, SourceError>> + '_;
}

/// A source over worlds already built (tests, small fixtures).
pub struct InMemory {
    dataset: DatasetId,
    worlds: Vec<World>,
}

impl InMemory {
    pub fn new(dataset: DatasetId, worlds: Vec<World>) -> Self {
        Self { dataset, worlds }
    }
}

impl TraceSource for InMemory {
    fn id(&self) -> DatasetId {
        self.dataset.clone()
    }

    fn worlds(&mut self) -> impl Iterator<Item = Result<World, SourceError>> + '_ {
        self.worlds.drain(..).map(Ok)
    }
}
