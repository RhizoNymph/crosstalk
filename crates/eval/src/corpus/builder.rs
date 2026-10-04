//! [`WorldBuilder`]: how a converter assembles a world.
//!
//! A converter declares its agents, hands each exchange over as an
//! [`ExchangeDraft`] (messages already hashed, a virtual time, a source
//! reference), adds labels, and finishes the world. The builder derives
//! every id, synthesises each agent's client context, assembles the spec
//! `Exchange` and checks the invariants a corpus relies on.

use std::collections::{BTreeMap, BTreeSet};

use crosstalk_spec::ids::ExchangeId;
use crosstalk_spec::observed::exchange::{
    Continuation, Exchange, ExchangeMeta, ExchangeOutcome, ModelName, StopReason, TokenUsage,
    Transport, WireProtocol,
};
use crosstalk_spec::support::Timestamp;

use super::client::synthetic_client;
use super::exchange::{CorpusError, CorpusExchange, Fidelity, HashedMessage, normalized};
use super::{CorpusAgent, Coverage, Driven, World};
use crate::ids::{agent_id, exchange_id};
use crate::keys::{AgentKey, DatasetId, SourceRef, WorldKey};
use crate::truth::Expectation;

/// Everything a converter knows about one exchange.
#[derive(Debug, Clone)]
pub struct ExchangeDraft {
    pub agent: AgentKey,
    pub at: Timestamp,
    pub protocol: WireProtocol,
    pub model: String,
    /// The request's messages, in order.
    pub request: Vec<HashedMessage>,
    pub response: HashedMessage,
    pub stop: StopReason,
    pub usage: Option<TokenUsage>,
    pub fidelity: Fidelity,
    /// The record the response came from; the exchange id derives from it.
    pub source: SourceRef,
}

pub struct WorldBuilder {
    dataset: DatasetId,
    key: WorldKey,
    agents: BTreeMap<AgentKey, CorpusAgent>,
    last_at: BTreeMap<AgentKey, Timestamp>,
    exchanges: Vec<CorpusExchange>,
    ids: BTreeSet<ExchangeId>,
    truth: Vec<Expectation>,
}

impl WorldBuilder {
    pub fn new(dataset: DatasetId, key: WorldKey) -> Self {
        Self {
            dataset,
            key,
            agents: BTreeMap::new(),
            last_at: BTreeMap::new(),
            exchanges: Vec::new(),
            ids: BTreeSet::new(),
            truth: Vec::new(),
        }
    }

    pub fn dataset(&self) -> &DatasetId {
        &self.dataset
    }

    pub fn key(&self) -> &WorldKey {
        &self.key
    }

    /// Declares agent `name`, driven by `model` (its client context's
    /// upstream follows from it) or scripted.
    pub fn agent(
        &mut self,
        name: &str,
        driven: Driven,
        model: &str,
    ) -> Result<AgentKey, CorpusError> {
        let key = AgentKey::new(self.key.clone(), name);
        if self.agents.contains_key(&key) {
            return Err(CorpusError::DuplicateAgent(key));
        }
        let agent = CorpusAgent {
            id: agent_id(&self.dataset, &key),
            client: synthetic_client(&self.dataset, &key, model),
            key: key.clone(),
            driven,
            model: model.to_owned(),
        };
        self.agents.insert(key.clone(), agent);
        Ok(key)
    }

    /// Adds one exchange. Its agent must be declared and model-driven, and
    /// its time later than the agent's previous exchange.
    pub fn exchange(&mut self, draft: ExchangeDraft) -> Result<ExchangeId, CorpusError> {
        if draft.agent.world != self.key {
            return Err(CorpusError::ForeignAgent {
                agent: draft.agent,
                world: self.key.clone(),
            });
        }
        let agent = self
            .agents
            .get(&draft.agent)
            .ok_or_else(|| CorpusError::UnknownAgent(draft.agent.clone()))?;
        if agent.driven == Driven::Scripted {
            return Err(CorpusError::ScriptedAgent(draft.agent));
        }
        if let Some(last) = self.last_at.get(&draft.agent)
            && *last >= draft.at
        {
            return Err(CorpusError::OutOfOrder {
                agent: draft.agent,
                at: *last,
            });
        }
        let id = exchange_id(&self.dataset, &draft.source, draft.at);
        let reference = id;
        if !self.ids.insert(reference) {
            return Err(CorpusError::DuplicateExchange(reference));
        }
        let exchange = Exchange {
            meta: ExchangeMeta {
                id,
                protocol: draft.protocol,
                transport: Transport::Http,
                model: ModelName(draft.model),
                client: agent.client.clone(),
                started_at: draft.at,
            },
            continuation: Continuation::FullHistory,
            request: draft.request.iter().map(HashedMessage::hash).collect(),
            outcome: ExchangeOutcome::Completed {
                response: draft.response.hash(),
                response_id: None,
                first_chunk_at: draft.at,
                finished_at: draft.at,
                stop: draft.stop,
                usage: draft.usage,
            },
        };
        let mut messages = draft.request;
        messages.push(draft.response);
        let normalized = normalized(exchange, messages)?;
        let exchange = CorpusExchange::new(
            draft.agent.clone(),
            normalized,
            draft.fidelity,
            draft.source,
        )?;
        self.last_at.insert(draft.agent, draft.at);
        self.exchanges.push(exchange);
        Ok(reference)
    }

    pub fn expect(&mut self, expectation: Expectation) {
        self.truth.push(expectation);
    }

    /// The world, with exchanges ordered by time (ties by agent, then id).
    pub fn finish(mut self, coverage: Coverage) -> World {
        self.exchanges
            .sort_by(|a, b| (a.at(), a.agent(), a.id()).cmp(&(b.at(), b.agent(), b.id())));
        World {
            dataset: self.dataset,
            key: self.key,
            agents: self.agents.into_values().collect(),
            exchanges: self.exchanges,
            truth: self.truth,
            coverage,
        }
    }
}
