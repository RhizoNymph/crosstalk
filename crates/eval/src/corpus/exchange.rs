//! One exchange of the corpus: a spec `NormalizedExchange` plus what the eval
//! knows about it (agent, virtual time, fidelity, where it came from).

use std::collections::HashMap;

use crosstalk_spec::ids::{ExchangeId, MessageHash};
use crosstalk_spec::interfaces::l1_canonical::{InvalidNormalizedExchange, NormalizedExchange};
use crosstalk_spec::observed::exchange::{Exchange, ExchangeOutcome};
use crosstalk_spec::observed::message::{Message, MessageBody, encoding};
use crosstalk_spec::support::Timestamp;
use serde::{Deserialize, Serialize};

use crate::keys::{AgentKey, SourceRef};

/// How faithfully an exchange reproduces what crossed the wire.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Fidelity {
    /// Captured request and response bytes, normalized.
    Exact,
    /// Rebuilt from a conversation log whose entries were matched one to one
    /// with the API calls the log records.
    Reconstructed,
    /// Rebuilt without that check passing: the request may not be exactly
    /// what was sent.
    Synthetic,
}

/// A message whose hash is the canonical hash of its body. Only
/// [`HashedMessage::new`] builds one, so a corpus never holds a message under
/// a wrong hash.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HashedMessage(Message);

impl HashedMessage {
    pub fn new(body: MessageBody) -> Self {
        Self(encoding::message(body))
    }

    pub fn hash(&self) -> MessageHash {
        self.0.hash
    }

    pub fn message(&self) -> &Message {
        &self.0
    }

    pub fn into_message(self) -> Message {
        self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum CorpusError {
    #[error("exchange {exchange:?} breaks the normalized-exchange invariants: {reason:?}")]
    Invalid {
        exchange: ExchangeId,
        reason: InvalidNormalizedExchange,
    },
    #[error("exchange {exchange:?} carries two different bodies under one hash")]
    HashCollision { exchange: ExchangeId },
    #[error(
        "exchange {exchange:?} has its first chunk at {first_chunk:?}, before its start {started:?}"
    )]
    ChunkBeforeStart {
        exchange: ExchangeId,
        started: Timestamp,
        first_chunk: Timestamp,
    },
    #[error("agent {0} is not declared in its world")]
    UnknownAgent(AgentKey),
    #[error("agent {0} is scripted and makes no exchanges")]
    ScriptedAgent(AgentKey),
    #[error("agent {agent} already has an exchange at or after {at:?}")]
    OutOfOrder { agent: AgentKey, at: Timestamp },
    #[error("agent {0} is declared twice")]
    DuplicateAgent(AgentKey),
    #[error("agent {agent} belongs to another world than {world}")]
    ForeignAgent {
        agent: AgentKey,
        world: crate::keys::WorldKey,
    },
    #[error("exchange {0:?} is declared twice")]
    DuplicateExchange(ExchangeId),
}

/// The normalized exchange of `exchange` and the messages it names, each
/// kept once. [`CorpusExchange::new`] checks it.
pub fn normalized(
    exchange: Exchange,
    messages: Vec<HashedMessage>,
) -> Result<NormalizedExchange, CorpusError> {
    let id = exchange.meta.id;
    let mut kept: Vec<Message> = Vec::with_capacity(messages.len());
    let mut index: HashMap<MessageHash, usize> = HashMap::with_capacity(messages.len());
    for message in messages {
        let message = message.into_message();
        match index.get(&message.hash) {
            Some(&at) if kept[at].body != message.body => {
                return Err(CorpusError::HashCollision { exchange: id });
            }
            Some(_) => {}
            None => {
                index.insert(message.hash, kept.len());
                kept.push(message);
            }
        }
    }
    Ok(NormalizedExchange {
        exchange,
        messages: kept,
        warnings: Vec::new(),
        media: Vec::new(),
    })
}

/// One exchange of one agent in the corpus.
///
/// Built only through [`CorpusExchange::new`], which checks the normalized
/// exchange ([`NormalizedExchange::check`]) and its times.
#[derive(Debug, Clone, PartialEq)]
pub struct CorpusExchange {
    agent: AgentKey,
    at: Timestamp,
    normalized: NormalizedExchange,
    index: HashMap<MessageHash, usize>,
    fidelity: Fidelity,
    source: SourceRef,
}

impl CorpusExchange {
    pub fn new(
        agent: AgentKey,
        normalized: NormalizedExchange,
        fidelity: Fidelity,
        source: SourceRef,
    ) -> Result<Self, CorpusError> {
        let id = normalized.exchange.meta.id;
        let at = normalized.exchange.meta.started_at;
        normalized.check().map_err(|reason| CorpusError::Invalid {
            exchange: id,
            reason,
        })?;
        let index = normalized
            .messages
            .iter()
            .enumerate()
            .map(|(position, message)| (message.hash, position))
            .collect();
        if let ExchangeOutcome::Completed { first_chunk_at, .. } = &normalized.exchange.outcome
            && *first_chunk_at < at
        {
            return Err(CorpusError::ChunkBeforeStart {
                exchange: id,
                started: at,
                first_chunk: *first_chunk_at,
            });
        }
        Ok(Self {
            agent,
            at,
            normalized,
            index,
            fidelity,
            source,
        })
    }

    pub fn agent(&self) -> &AgentKey {
        &self.agent
    }

    /// The virtual time the exchange started: its `meta.started_at`.
    pub fn at(&self) -> Timestamp {
        self.at
    }

    pub fn id(&self) -> ExchangeId {
        self.normalized.exchange.meta.id
    }

    pub fn exchange(&self) -> &Exchange {
        &self.normalized.exchange
    }

    pub fn normalized(&self) -> &NormalizedExchange {
        &self.normalized
    }

    pub fn fidelity(&self) -> Fidelity {
        self.fidelity
    }

    pub fn source(&self) -> &SourceRef {
        &self.source
    }

    /// The message with this hash, if the exchange carries it.
    pub fn message(&self, hash: MessageHash) -> Option<&Message> {
        self.index
            .get(&hash)
            .and_then(|&at| self.normalized.messages.get(at))
    }

    /// The request's messages, in order.
    pub fn request(&self) -> impl Iterator<Item = &Message> {
        self.normalized
            .exchange
            .request
            .iter()
            .filter_map(|hash| self.message(*hash))
    }

    /// The response message, when the exchange completed.
    pub fn response(&self) -> Option<&Message> {
        match &self.normalized.exchange.outcome {
            ExchangeOutcome::Completed { response, .. } => self.message(*response),
            ExchangeOutcome::Failed {
                partial_response, ..
            } => partial_response.and_then(|hash| self.message(hash)),
        }
    }
}
